package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
)

// Publisher implementa aggregation.Publisher sobre um cliente franz-go.
type Publisher struct {
	client *kgo.Client
}

// NewPublisher cria o publisher. O cliente precisa ter ProducerOptions.
func NewPublisher(client *kgo.Client) *Publisher { return &Publisher{client: client} }

// Publish envia as mensagens e espera a confirmação de todas.
func (p *Publisher) Publish(ctx context.Context, msgs []aggregation.Message) error {
	records := make([]*kgo.Record, len(msgs))
	for i, m := range msgs {
		r := &kgo.Record{Topic: m.Topic, Key: m.Key, Value: m.Value}
		for k, v := range m.Headers {
			r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		records[i] = r
	}
	return p.client.ProduceSync(ctx, records...).FirstErr()
}
