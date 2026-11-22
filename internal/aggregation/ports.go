// Package aggregation é o caso de uso do consumidor: decodifica leituras,
// agrega em janelas, publica agregados, alertas e DLQ, grava o checkpoint e
// só então commita os offsets. Não conhece o cliente Kafka; fala com ele por
// portas pequenas, o que permite testar toda a garantia de entrega em memória.
package aggregation

import (
	"context"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

// TopicPartition identifica uma partição.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// Record é uma mensagem consumida do tópico de leituras.
type Record struct {
	TopicPartition
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// Message é uma mensagem a publicar.
type Message struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// Checkpoint é o estado mínimo para retomar uma partição sem perder nem
// duplicar janelas: o offset seguro e o watermark no momento do commit.
type Checkpoint struct {
	TopicPartition
	Offset int64
	// Watermark é o mais avançado entre as chaves; checkpoints antigos só têm
	// este valor.
	Watermark time.Time
	// Watermarks é o watermark de cada chave (estufa, ou "" para a partição
	// e para o piso). Vazio em checkpoints antigos.
	Watermarks map[string]time.Time
}

// Publisher publica mensagens e só retorna depois da confirmação do broker
// (acks=all). Um erro significa que nenhuma garantia pode ser assumida.
type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

// CheckpointStore guarda o checkpoint de cada partição de forma durável.
type CheckpointStore interface {
	Save(ctx context.Context, cps []Checkpoint) error
	Load(ctx context.Context) (map[TopicPartition]Checkpoint, error)
}

// Committer commita offsets do grupo de consumo.
type Committer interface {
	Commit(ctx context.Context, offsets map[TopicPartition]int64) error
}

// Decoder desserializa o valor de uma mensagem no registro de destino.
type Decoder interface {
	Decode(ctx context.Context, b []byte, v any) error
}

// Encoder serializa um registro para publicação.
type Encoder interface {
	Encode(v any) ([]byte, error)
}

// SensorState é o último agregado de uma grandeza de uma estufa e o alerta
// que ele gerou, se houver.
type SensorState struct {
	Aggregate telemetry.Aggregate
	Alert     *telemetry.Alert
}

// StateStore guarda a projeção do último estado de cada estufa para leitura
// rápida. É derivada dos agregados já publicados: o tópico e o Postgres
// continuam sendo a fonte da verdade.
type StateStore interface {
	Update(ctx context.Context, states []SensorState) error
}

// Observer recebe os eventos relevantes para métricas.
type Observer interface {
	RecordProcessed(outcome string)
	DeadLettered(reason string)
	Emitted(aggregates, alerts int)
	EndToEndLatency(d time.Duration)
	PartitionState(tp TopicPartition, openWindows, trackedEvents int, watermark time.Time)
	Committed(tp TopicPartition, offset int64)
	IdleClosed(windows int)
}

// NopObserver descarta todos os eventos.
type NopObserver struct{}

func (NopObserver) RecordProcessed(string)                             {}
func (NopObserver) DeadLettered(string)                                {}
func (NopObserver) Emitted(int, int)                                   {}
func (NopObserver) EndToEndLatency(time.Duration)                      {}
func (NopObserver) PartitionState(TopicPartition, int, int, time.Time) {}
func (NopObserver) Committed(TopicPartition, int64)                    {}
func (NopObserver) IdleClosed(int)                                     {}
