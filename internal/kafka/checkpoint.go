package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
)

// CheckpointStore guarda os checkpoints do agregador num tópico compactado,
// com uma mensagem por partição de origem (chave "<tópico>/<partição>").
// É o mesmo princípio do changelog do Kafka Streams, reduzido ao mínimo:
// a compactação mantém só o último checkpoint de cada partição.
type CheckpointStore struct {
	client *kgo.Client
	admin  *kadm.Client
	seeds  []string
	topic  string
}

// NewCheckpointStore cria o store. client precisa ter ProducerOptions.
func NewCheckpointStore(client *kgo.Client, seeds []string, topic string) *CheckpointStore {
	return &CheckpointStore{client: client, admin: kadm.NewClient(client), seeds: seeds, topic: topic}
}

type checkpointValue struct {
	Offset      int64 `json:"offset"`
	WatermarkMs int64 `json:"watermark_ms"`
}

// Save grava os checkpoints e espera a confirmação do broker.
func (s *CheckpointStore) Save(ctx context.Context, cps []aggregation.Checkpoint) error {
	records := make([]*kgo.Record, 0, len(cps))
	for _, cp := range cps {
		var wm int64
		if !cp.Watermark.IsZero() {
			wm = cp.Watermark.UnixMilli()
		}
		value, err := json.Marshal(checkpointValue{Offset: cp.Offset, WatermarkMs: wm})
		if err != nil {
			return err
		}
		records = append(records, &kgo.Record{Topic: s.topic, Key: []byte(checkpointKey(cp.TopicPartition)), Value: value})
	}
	return s.client.ProduceSync(ctx, records...).FirstErr()
}

// loadTimeout limita a leitura do tópico de checkpoints: Load roda dentro do
// callback de rebalance, que não pode ficar preso indefinidamente.
const loadTimeout = 30 * time.Second

// Load lê o tópico de checkpoints do início ao fim.
func (s *CheckpointStore) Load(ctx context.Context) (map[aggregation.TopicPartition]aggregation.Checkpoint, error) {
	ctx, cancel := context.WithTimeout(ctx, loadTimeout)
	defer cancel()
	ends, err := s.admin.ListEndOffsets(ctx, s.topic)
	if err != nil {
		return nil, fmt.Errorf("end offsets de %s: %w", s.topic, err)
	}
	if err := ends.Error(); err != nil {
		return nil, fmt.Errorf("end offsets de %s: %w", s.topic, err)
	}
	pending := map[int32]int64{}
	start := map[int32]kgo.Offset{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			pending[o.Partition] = o.Offset
			start[o.Partition] = kgo.NewOffset().AtStart()
		}
	})
	out := make(map[aggregation.TopicPartition]aggregation.Checkpoint)
	if len(pending) == 0 {
		return out, nil
	}

	reader, err := kgo.NewClient(kgo.SeedBrokers(s.seeds...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{s.topic: start}))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	for len(pending) > 0 {
		fetches := reader.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("ler checkpoints: %w", err)
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, fmt.Errorf("ler checkpoints: %w", errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Offset+1 >= pending[r.Partition] {
				delete(pending, r.Partition)
			}
			cp, err := decodeCheckpoint(r.Key, r.Value)
			if err == nil {
				out[cp.TopicPartition] = cp
			}
		})
	}
	return out, nil
}

func checkpointKey(tp aggregation.TopicPartition) string {
	return tp.Topic + "/" + strconv.Itoa(int(tp.Partition))
}

func decodeCheckpoint(key, value []byte) (aggregation.Checkpoint, error) {
	k := string(key)
	i := strings.LastIndex(k, "/")
	if i <= 0 {
		return aggregation.Checkpoint{}, fmt.Errorf("chave de checkpoint inválida: %q", k)
	}
	partition, err := strconv.ParseInt(k[i+1:], 10, 32)
	if err != nil {
		return aggregation.Checkpoint{}, fmt.Errorf("chave de checkpoint inválida: %q", k)
	}
	var v checkpointValue
	if err := json.Unmarshal(value, &v); err != nil {
		return aggregation.Checkpoint{}, fmt.Errorf("valor de checkpoint inválido: %w", err)
	}
	cp := aggregation.Checkpoint{
		TopicPartition: aggregation.TopicPartition{Topic: k[:i], Partition: int32(partition)},
		Offset:         v.Offset,
	}
	if v.WatermarkMs != 0 {
		cp.Watermark = time.UnixMilli(v.WatermarkMs).UTC()
	}
	return cp, nil
}
