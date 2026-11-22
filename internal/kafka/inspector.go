package kafka

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/dlq"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/pipeline"
	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

// readTimeout limita quanto uma consulta espera o broker.
const readTimeout = 5 * time.Second

// Inspector lê a DLQ e mede o lag do grupo usando a API de administração.
// Implementa pipeline.DLQReader e pipeline.HealthReporter.
type Inspector struct {
	seeds      []string
	group      string
	throughput pipeline.Throughput
	client     *kgo.Client
	admin      *kadm.Client
	// opts são as opções de segurança, repassadas aos clientes de leitura.
	opts []kgo.Opt
}

// NewInspector abre um cliente só para consultas, separado do consumidor do
// agregador para que uma consulta lenta nunca atrase o processamento.
func NewInspector(seeds []string, group string, throughput pipeline.Throughput, opts ...kgo.Opt) (*Inspector, error) {
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(seeds...)}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("cliente de inspeção: %w", err)
	}
	return &Inspector{seeds: seeds, group: group, throughput: throughput, client: cl, admin: kadm.NewClient(cl), opts: opts}, nil
}

// Close encerra o cliente.
func (i *Inspector) Close() { i.client.Close() }

// Report devolve lag por partição, tamanho da DLQ e vazão.
func (i *Inspector) Report(ctx context.Context) (pipeline.Health, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()

	lags, err := i.admin.Lag(ctx, i.group)
	if err != nil {
		return pipeline.Health{}, fmt.Errorf("lag do grupo: %w", err)
	}
	gl, ok := lags[i.group]
	if !ok {
		return pipeline.Health{}, fmt.Errorf("grupo %s desconhecido", i.group)
	}
	if err := gl.Error(); err != nil {
		return pipeline.Health{}, fmt.Errorf("lag do grupo: %w", err)
	}
	h := pipeline.Health{Group: i.group, GroupState: gl.State, Topic: schemas.TopicReadings, Partitions: []pipeline.PartitionLag{}}
	for _, p := range gl.Lag.Sorted() {
		if p.Topic != schemas.TopicReadings {
			continue
		}
		lag := max(p.Lag, 0)
		h.Partitions = append(h.Partitions, pipeline.PartitionLag{
			Partition: p.Partition, CommittedOffset: p.Commit.At, EndOffset: p.End.Offset, Lag: lag,
		})
		h.TotalLag += lag
	}
	start, end, err := i.dlqOffsets(ctx)
	if err != nil {
		return pipeline.Health{}, err
	}
	h.DLQMessages = end - start
	h.ThroughputPerS, h.ThroughputSince = i.throughput.PerSecond()
	return h, nil
}

func (i *Inspector) dlqOffsets(ctx context.Context) (start, end int64, err error) {
	starts, err := i.admin.ListStartOffsets(ctx, schemas.TopicReadingsDLQ)
	if err != nil {
		return 0, 0, fmt.Errorf("offsets iniciais da DLQ: %w", err)
	}
	ends, err := i.admin.ListEndOffsets(ctx, schemas.TopicReadingsDLQ)
	if err != nil {
		return 0, 0, fmt.Errorf("offsets finais da DLQ: %w", err)
	}
	if err := starts.Error(); err != nil {
		return 0, 0, err
	}
	if err := ends.Error(); err != nil {
		return 0, 0, err
	}
	// A DLQ tem uma partição só (ver PipelineTopics).
	s, _ := starts.Lookup(schemas.TopicReadingsDLQ, 0)
	e, _ := ends.Lookup(schemas.TopicReadingsDLQ, 0)
	return s.Offset, e.Offset, nil
}

// Recent lê as últimas mensagens da DLQ, da mais nova para a mais antiga.
func (i *Inspector) Recent(ctx context.Context, limit int) ([]pipeline.DeadLetter, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()

	start, end, err := i.dlqOffsets(ctx)
	if err != nil {
		return nil, err
	}
	if end <= start || limit <= 0 {
		return []pipeline.DeadLetter{}, nil
	}
	from := max(start, end-int64(limit))

	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(i.seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			schemas.TopicReadingsDLQ: {0: kgo.NewOffset().At(from)},
		})}, i.opts...)...)
	if err != nil {
		return nil, fmt.Errorf("ler DLQ: %w", err)
	}
	defer cl.Close()

	out := make([]pipeline.DeadLetter, 0, end-from)
	for int64(len(out)) < end-from {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("ler DLQ: %w", err)
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, fmt.Errorf("ler DLQ: %w", errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Offset < end {
				out = append(out, deadLetter(r))
			}
		})
	}
	slices.Reverse(out)
	return out, nil
}

func deadLetter(r *kgo.Record) pipeline.DeadLetter {
	h := map[string]string{}
	for _, hd := range r.Headers {
		h[hd.Key] = string(hd.Value)
	}
	return pipeline.DeadLetter{
		Partition: r.Partition, Offset: r.Offset, Timestamp: r.Timestamp.UTC(), Key: string(r.Key),
		Reason: h[aggregation.HeaderReason], Error: h[aggregation.HeaderError],
		SourceTopic:     h[aggregation.HeaderSourceTopic],
		SourcePartition: int32(parseInt(h[aggregation.HeaderSourcePartition])), SourceOffset: parseInt(h[aggregation.HeaderSourceOffset]),
	}
}

func parseInt(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return -1
	}
	return n
}

// replayReadTimeout cobre a leitura de um bloco para o reprocessamento.
const replayReadTimeout = 20 * time.Second

// Read implementa dlq.Source: lê até limit mensagens da DLQ a partir de from
// (ajustado ao início retido do log) e devolve também o offset final.
func (i *Inspector) Read(ctx context.Context, from int64, limit int) ([]dlq.Entry, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, replayReadTimeout)
	defer cancel()

	start, end, err := i.dlqOffsets(ctx)
	if err != nil {
		return nil, 0, err
	}
	from = max(from, start)
	if from >= end || limit <= 0 {
		return nil, end, nil
	}
	want := min(int64(limit), end-from)

	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(i.seeds...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			schemas.TopicReadingsDLQ: {0: kgo.NewOffset().At(from)},
		})}, i.opts...)...)
	if err != nil {
		return nil, 0, fmt.Errorf("ler DLQ: %w", err)
	}
	defer cl.Close()

	out := make([]dlq.Entry, 0, want)
	for int64(len(out)) < want {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, 0, fmt.Errorf("ler DLQ: %w", err)
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, 0, fmt.Errorf("ler DLQ: %w", errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if int64(len(out)) >= want || r.Offset >= end {
				return
			}
			h := make(map[string]string, len(r.Headers))
			for _, hd := range r.Headers {
				h[hd.Key] = string(hd.Value)
			}
			out = append(out, dlq.Entry{Offset: r.Offset, Timestamp: r.Timestamp.UTC(), Key: r.Key, Value: r.Value, Headers: h})
		})
	}
	return out, end, nil
}

// DefaultSinkGroup é o grupo do conector JDBC que grava os agregados
// (connect-<nome do conector>).
const DefaultSinkGroup = "connect-greenhouse-aggregates-jdbc-sink"

// SinkProof implementa dlq.SinkProof: o sink "passou" quando o grupo dele
// commitou, em toda partição do tópico de agregados, tudo o que foi escrito.
type SinkProof struct {
	inspector *Inspector
	group     string
}

// NewSinkProof usa o cliente de administração do Inspector.
func NewSinkProof(in *Inspector, group string) *SinkProof {
	return &SinkProof{inspector: in, group: group}
}

// CaughtUp devolve false (sem erro) se o grupo não existe ou não commitou
// alguma partição: sem prova, quem chama deve falhar fechado.
func (s *SinkProof) CaughtUp(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	ends, err := s.inspector.admin.ListEndOffsets(ctx, schemas.TopicAggregates)
	if err != nil {
		return false, fmt.Errorf("offsets finais de %s: %w", schemas.TopicAggregates, err)
	}
	if err := ends.Error(); err != nil {
		return false, err
	}
	lags, err := s.inspector.admin.Lag(ctx, s.group)
	if err != nil {
		return false, fmt.Errorf("lag do grupo %s: %w", s.group, err)
	}
	gl, ok := lags[s.group]
	if !ok || gl.Error() != nil {
		return false, nil
	}
	caught := true
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset == 0 {
			return
		}
		l, found := gl.Lag.Lookup(o.Topic, o.Partition)
		if !found || l.Err != nil || l.Commit.At < o.Offset {
			caught = false
		}
	})
	return caught, nil
}
