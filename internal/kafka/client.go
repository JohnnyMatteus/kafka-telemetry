// Package kafka contém os adaptadores para Kafka e Schema Registry: produtor
// idempotente, consumidor em grupo com commit manual, store de checkpoint em
// tópico compactado, administração de tópicos e cliente do registry.
package kafka

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProducerOptions devolve as opções de produção usadas por todos os
// produtores do pipeline.
//
//   - acks=all: o líder só confirma depois que todas as réplicas em sincronia
//     gravaram (com min.insync.replicas=2 em produção).
//   - Idempotência: ligada por padrão no franz-go (não chamamos
//     DisableIdempotentWrite). O broker descarta retransmissões pelo par
//     producer id + número de sequência, então retries não duplicam nem
//     reordenam mensagens dentro de uma partição.
//   - Particionador por chave (murmur2, compatível com o cliente Java): a
//     mesma estufa sempre cai na mesma partição, o que preserva a ordem.
func ProducerOptions(linger time.Duration) []kgo.Opt {
	return []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.ProducerLinger(linger),
		kgo.ProducerBatchCompression(kgo.Lz4Compression(), kgo.NoCompression()),
		kgo.RecordDeliveryTimeout(30 * time.Second),
	}
}
