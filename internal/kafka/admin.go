package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

// TopicSpec descreve um tópico do pipeline.
type TopicSpec struct {
	Name       string
	Partitions int32
	Configs    map[string]*string
}

// PipelineTopics devolve os tópicos com as configurações de cada papel.
// partitions vale para os tópicos de dados; o de checkpoint usa uma partição
// só porque é pequeno e lido inteiro a cada rebalance. minInsyncReplicas <= 0
// deixa o padrão do broker (útil em brokers que não aceitam a configuração).
func PipelineTopics(partitions int32, minInsyncReplicas int) []TopicSpec {
	minISR := kadm.StringPtr(fmt.Sprint(minInsyncReplicas))
	specs := []TopicSpec{
		{Name: schemas.TopicReadings, Partitions: partitions, Configs: map[string]*string{
			"retention.ms": kadm.StringPtr("604800000"), "min.insync.replicas": minISR,
		}},
		{Name: schemas.TopicAggregates, Partitions: partitions, Configs: map[string]*string{
			"retention.ms": kadm.StringPtr("2592000000"), "min.insync.replicas": minISR,
		}},
		{Name: schemas.TopicAlerts, Partitions: partitions, Configs: map[string]*string{
			"retention.ms": kadm.StringPtr("2592000000"), "min.insync.replicas": minISR,
		}},
		{Name: schemas.TopicReadingsDLQ, Partitions: 1, Configs: map[string]*string{
			"retention.ms": kadm.StringPtr("2592000000"), "min.insync.replicas": minISR,
		}},
		{Name: schemas.TopicCheckpoints, Partitions: 1, Configs: map[string]*string{
			"cleanup.policy": kadm.StringPtr("compact"), "min.insync.replicas": minISR,
		}},
	}
	if minInsyncReplicas <= 0 {
		for _, s := range specs {
			delete(s.Configs, "min.insync.replicas")
		}
	}
	return specs
}

// EnsureTopics cria os tópicos que ainda não existem. É idempotente.
func EnsureTopics(ctx context.Context, seeds []string, replication int16, specs []TopicSpec) ([]string, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)

	var created []string
	for _, spec := range specs {
		resp, err := admin.CreateTopic(ctx, spec.Partitions, replication, spec.Configs, spec.Name)
		if err == nil {
			err = resp.Err
		}
		switch {
		case err == nil:
			created = append(created, spec.Name)
		case errors.Is(err, kerr.TopicAlreadyExists):
		default:
			return created, fmt.Errorf("criar %s: %w", spec.Name, err)
		}
	}
	return created, nil
}
