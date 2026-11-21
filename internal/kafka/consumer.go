package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
)

// Handler é o que o laço de consumo aciona; implementado por aggregation.Service.
type Handler interface {
	Assign(ctx context.Context, tps []aggregation.TopicPartition) error
	Revoke(ctx context.Context, tps []aggregation.TopicPartition) error
	Drop(tps []aggregation.TopicPartition)
	HandleBatch(ctx context.Context, records []aggregation.Record) error
}

// LoopObserver recebe as métricas do laço de consumo: lag por partição,
// registros consumidos e duração de cada lote.
type LoopObserver interface {
	Lag(tp aggregation.TopicPartition, lag int64)
	Consumed(n int)
	BatchDuration(d time.Duration)
}

// GroupConsumerConfig configura o consumidor em grupo.
type GroupConsumerConfig struct {
	Seeds          []string
	Group          string
	Topic          string
	MaxPollRecords int
	Linger         time.Duration
	Hooks          []kgo.Hook
}

// GroupConsumer consome em grupo com commit manual. Também implementa
// aggregation.Committer, porque o commit precisa da sessão do grupo.
type GroupConsumer struct {
	cfg    GroupConsumerConfig
	client *kgo.Client
	logger *slog.Logger

	mu      sync.Mutex
	handler Handler
	fatal   error
	cancel  context.CancelFunc
}

// NewGroupConsumer cria o cliente. O mesmo cliente produz agregados, alertas,
// DLQ e checkpoints, com as opções de produtor idempotente.
func NewGroupConsumer(cfg GroupConsumerConfig, logger *slog.Logger) (*GroupConsumer, error) {
	g := &GroupConsumer{cfg: cfg, logger: logger}
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(cfg.Seeds...),
		kgo.ClientID(cfg.Group),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.DisableAutoCommit(),
		// Rebalances só acontecem entre lotes: o estado em memória nunca é
		// revogado no meio do processamento.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(g.onAssigned),
		kgo.OnPartitionsRevoked(g.onRevoked),
		kgo.OnPartitionsLost(g.onLost),
		kgo.WithHooks(cfg.Hooks...),
	}, ProducerOptions(cfg.Linger)...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("cliente kafka: %w", err)
	}
	g.client = cl
	return g, nil
}

// Client expõe o cliente para os adaptadores que compartilham a conexão.
func (g *GroupConsumer) Client() *kgo.Client { return g.client }

// Run consome até o contexto acabar ou um erro fatal acontecer. Um erro de
// processamento encerra o laço sem commit: o processo deve terminar e o
// orquestrador reiniciá-lo, retomando do último offset commitado.
func (g *GroupConsumer) Run(ctx context.Context, h Handler, obs LoopObserver) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g.mu.Lock()
	g.handler, g.cancel = h, cancel
	g.mu.Unlock()

	for {
		fetches := g.client.PollRecords(ctx, g.cfg.MaxPollRecords)
		// Todo poll que devolve um fetch (inclusive o de contexto cancelado)
		// bloqueia rebalances até AllowRebalance; sem liberar, o Close e o
		// LeaveGroup ficariam presos esperando.
		if err := g.fatalErr(); err != nil {
			g.client.AllowRebalance()
			return err
		}
		if fetches.IsClientClosed() || ctx.Err() != nil {
			g.client.AllowRebalance()
			return nil
		}
		var fetchErr error
		fetches.EachError(func(topic string, partition int32, err error) {
			// Erros retentáveis (troca de líder ou de coordenador) o próprio
			// cliente resolve; só os definitivos derrubam o processo.
			if kerr.IsRetriable(err) {
				g.logger.Warn("erro retentável no fetch", "topic", topic, "partition", partition, "error", err)
				return
			}
			fetchErr = errors.Join(fetchErr, fmt.Errorf("fetch %s/%d: %w", topic, partition, err))
		})
		if fetchErr != nil {
			g.client.AllowRebalance()
			return fetchErr
		}

		var batch []aggregation.Record
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			tp := aggregation.TopicPartition{Topic: p.Topic, Partition: p.Partition}
			for _, r := range p.Records {
				batch = append(batch, aggregation.Record{
					TopicPartition: tp, Offset: r.Offset, Key: r.Key, Value: r.Value, Timestamp: r.Timestamp,
				})
			}
			if n := len(p.Records); n > 0 && obs != nil {
				obs.Lag(tp, max(p.HighWatermark-(p.Records[n-1].Offset+1), 0))
			}
		})
		if obs != nil {
			obs.Consumed(len(batch))
		}
		start := time.Now()
		if err := h.HandleBatch(ctx, batch); err != nil {
			g.client.AllowRebalance()
			return fmt.Errorf("processar lote: %w", err)
		}
		if obs != nil {
			obs.BatchDuration(time.Since(start))
		}
		g.client.AllowRebalance()
	}
}

// Close sai do grupo e fecha o cliente.
func (g *GroupConsumer) Close() { g.client.Close() }

// Commit implementa aggregation.Committer com commit síncrono.
func (g *GroupConsumer) Commit(ctx context.Context, offsets map[aggregation.TopicPartition]int64) error {
	uncommitted := make(map[string]map[int32]kgo.EpochOffset)
	for tp, off := range offsets {
		if uncommitted[tp.Topic] == nil {
			uncommitted[tp.Topic] = make(map[int32]kgo.EpochOffset)
		}
		uncommitted[tp.Topic][tp.Partition] = kgo.EpochOffset{Epoch: -1, Offset: off}
	}
	var commitErr error
	g.client.CommitOffsetsSync(ctx, uncommitted, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			commitErr = err
			return
		}
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if e := kerr.ErrorForCode(p.ErrorCode); e != nil {
					commitErr = errors.Join(commitErr, fmt.Errorf("commit %s/%d: %w", t.Topic, p.Partition, e))
				}
			}
		}
	})
	return commitErr
}

func (g *GroupConsumer) onAssigned(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
	h := g.currentHandler()
	if h == nil {
		return
	}
	tps := toTopicPartitions(assigned)
	if err := h.Assign(ctx, tps); err != nil {
		g.fail(fmt.Errorf("assign: %w", err))
	}
}

func (g *GroupConsumer) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	h := g.currentHandler()
	if h == nil {
		return
	}
	if err := h.Revoke(ctx, toTopicPartitions(revoked)); err != nil {
		g.logger.Error("commit no revoke falhou; o próximo dono reprocessa a partir do último commit", "error", err)
	}
}

func (g *GroupConsumer) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	if h := g.currentHandler(); h != nil {
		h.Drop(toTopicPartitions(lost))
	}
}

func (g *GroupConsumer) currentHandler() Handler {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.handler
}

func (g *GroupConsumer) fail(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fatal == nil {
		g.fatal = err
	}
	if g.cancel != nil {
		g.cancel()
	}
}

func (g *GroupConsumer) fatalErr() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fatal
}

func toTopicPartitions(m map[string][]int32) []aggregation.TopicPartition {
	var out []aggregation.TopicPartition
	for topic, parts := range m {
		for _, p := range parts {
			out = append(out, aggregation.TopicPartition{Topic: topic, Partition: p})
		}
	}
	return out
}
