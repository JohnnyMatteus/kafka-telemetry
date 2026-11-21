//go:build integration

// Testes de integração contra um Redpanda real (API Kafka + Schema Registry)
// via testcontainers. Rode com: go test -tags integration ./internal/kafka/...
// Sem Docker disponível, os testes são pulados.
package kafka_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/kafka"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

const redpandaImage = "docker.redpanda.com/redpandadata/redpanda:v24.2.4"

type env struct {
	seeds    []string
	registry *kafka.Registry
}

func startRedpanda(t *testing.T) env {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := redpanda.Run(ctx, redpandaImage)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("subir redpanda: %v", err)
	}
	broker, err := ctr.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	srURL, err := ctr.SchemaRegistryAddress(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := kafka.NewRegistry(srURL)
	if err != nil {
		t.Fatal(err)
	}
	return env{seeds: []string{broker}, registry: reg}
}

func registerAll(t *testing.T, ctx context.Context, e env) map[string]int {
	t.Helper()
	ids := map[string]int{}
	for _, sub := range schemas.Subjects {
		if err := e.registry.SetBackward(ctx, sub.Name); err != nil {
			t.Fatal(err)
		}
		texts, err := sub.Texts()
		if err != nil {
			t.Fatal(err)
		}
		for _, text := range texts {
			ok, msgs, err := e.registry.CheckCompatible(ctx, sub.Name, text)
			if err != nil || !ok {
				t.Fatalf("%s: compatível=%v msgs=%v err=%v", sub.Name, ok, msgs, err)
			}
			if ids[sub.Topic], err = e.registry.Register(ctx, sub.Name, text); err != nil {
				t.Fatal(err)
			}
		}
	}
	return ids
}

// O registry real é a fonte da verdade da compatibilidade: confere que ele
// concorda com o portão local nos casos documentados.
func TestRegistryEnforcesBackwardCompatibility(t *testing.T) {
	e := startRedpanda(t)
	ctx := context.Background()
	registerAll(t, ctx, e)
	subject := schemas.TopicReadings + "-value"

	for name, wantOK := range map[string]bool{
		"sensor_reading.add-required-field.avsc":    false,
		"sensor_reading.change-value-type.avsc":     false,
		"sensor_reading.remove-required-field.avsc": true,
	} {
		b, err := os.ReadFile(filepath.Join("..", "..", "schemas", "testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		ok, msgs, err := e.registry.CheckCompatible(ctx, subject, string(b))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if ok != wantOK {
			t.Errorf("%s: registry disse compatível=%v (%v), quero %v", name, ok, msgs, wantOK)
		}
	}
}

type pipeline struct {
	t        *testing.T
	e        env
	ids      map[string]int
	producer *kgo.Client
	readEnc  *contract.Encoder
}

func newPipeline(t *testing.T) *pipeline {
	t.Helper()
	e := startRedpanda(t)
	ctx := context.Background()
	if _, err := kafka.EnsureTopics(ctx, e.seeds, 1, kafka.PipelineTopics(3, 0)); err != nil {
		t.Fatal(err)
	}
	ids := registerAll(t, ctx, e)
	sub, _ := schemas.Lookup(schemas.TopicReadings)
	text, _ := sub.Latest()
	enc, err := contract.NewEncoder(ids[schemas.TopicReadings], text)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(e.seeds...), kgo.DefaultProduceTopic(schemas.TopicReadings)},
		kafka.ProducerOptions(time.Millisecond)...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	return &pipeline{t: t, e: e, ids: ids, producer: producer, readEnc: enc}
}

func (p *pipeline) send(readings ...telemetry.Reading) {
	p.t.Helper()
	for _, r := range readings {
		v, err := p.readEnc.Encode(contract.NewSensorReadingRecord(r, "1.0.0"))
		if err != nil {
			p.t.Fatal(err)
		}
		p.sendRaw(r.GreenhouseID, v)
	}
}

func (p *pipeline) sendRaw(key string, value []byte) {
	p.t.Helper()
	if err := p.producer.ProduceSync(context.Background(), &kgo.Record{Key: []byte(key), Value: value}).FirstErr(); err != nil {
		p.t.Fatal(err)
	}
}

// startAggregator sobe uma instância do agregador e devolve a função que a
// derruba como um processo encerrado (sai do grupo).
func (p *pipeline) startAggregator() (stop func()) {
	p.t.Helper()
	consumer, err := kafka.NewGroupConsumer(kafka.GroupConsumerConfig{
		Seeds: p.e.seeds, Group: "it-aggregator", Topic: schemas.TopicReadings, MaxPollRecords: 500, Linger: time.Millisecond,
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		p.t.Fatal(err)
	}
	encoder := func(topic string) *contract.Encoder {
		sub, _ := schemas.Lookup(topic)
		text, _ := sub.Latest()
		enc, err := contract.NewEncoder(p.ids[topic], text)
		if err != nil {
			p.t.Fatal(err)
		}
		return enc
	}
	svc, err := aggregation.New(
		telemetry.WindowConfig{Size: time.Minute, AllowedLateness: 5 * time.Second},
		telemetry.DefaultThresholds(),
		aggregation.Topics{Aggregates: schemas.TopicAggregates, Alerts: schemas.TopicAlerts, DLQ: schemas.TopicReadingsDLQ},
		aggregation.Deps{
			Decoder:        contract.NewDecoder(p.e.registry),
			AggregateCodec: encoder(schemas.TopicAggregates),
			AlertCodec:     encoder(schemas.TopicAlerts),
			Publisher:      kafka.NewPublisher(consumer.Client()),
			Checkpoints:    kafka.NewCheckpointStore(consumer.Client(), p.e.seeds, schemas.TopicCheckpoints),
			Committer:      consumer,
		})
	if err != nil {
		p.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx, svc, nil) }()
	return func() {
		cancel()
		if err := <-done; err != nil {
			p.t.Errorf("agregador terminou com erro: %v", err)
		}
		consumer.Close()
	}
}

// collect lê um tópico do início até ter n mensagens ou estourar o prazo.
func (p *pipeline) collect(topic string, n int, timeout time.Duration) []*kgo.Record {
	p.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(p.e.seeds...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		p.t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		out = append(out, fetches.Records()...)
	}
	return out
}

func (p *pipeline) decodeAggregates(records []*kgo.Record) map[telemetry.WindowKey]contract.WindowAggregateRecord {
	p.t.Helper()
	dec := contract.NewDecoder(p.e.registry)
	out := map[telemetry.WindowKey]contract.WindowAggregateRecord{}
	for _, r := range records {
		var agg contract.WindowAggregateRecord
		if err := dec.Decode(context.Background(), r.Value, &agg); err != nil {
			p.t.Fatal(err)
		}
		key := telemetry.WindowKey{GreenhouseID: agg.GreenhouseID, Type: telemetry.SensorType(agg.SensorType), Start: agg.WindowStart.UTC()}
		if _, dup := out[key]; dup {
			p.t.Errorf("janela emitida mais de uma vez: %+v", key)
		}
		out[key] = agg
	}
	return out
}

func reading(id, gh string, st telemetry.SensorType, v float64, at time.Time) telemetry.Reading {
	return telemetry.Reading{EventID: id, GreenhouseID: gh, SensorID: gh + "-" + string(st), Type: st, Value: v, MeasuredAt: at}
}

func TestPipelineEndToEndWithRestart(t *testing.T) {
	p := newPipeline(t)
	t0 := time.Now().UTC().Truncate(time.Minute).Add(-10 * time.Minute)
	T, H := telemetry.SensorTemperature, telemetry.SensorHumidity

	p.send(
		reading("a1", "gh-001", T, 20, t0),
		reading("a2", "gh-001", T, 24, t0.Add(20*time.Second)),
		reading("a2", "gh-001", T, 24, t0.Add(20*time.Second)), // retransmissão
		reading("a3", "gh-001", H, 70, t0.Add(30*time.Second)),
		reading("bad", "gh-001", H, 140, t0.Add(31*time.Second)), // fora da faixa física
		reading("b1", "gh-002", T, 35, t0.Add(10*time.Second)),
		reading("b2", "gh-002", T, 37, t0.Add(40*time.Second)),
	)
	p.sendRaw("gh-001", []byte(`{"not":"avro"}`))
	// Fecham o primeiro minuto de cada estufa e abrem o segundo.
	p.send(
		reading("a4", "gh-001", T, 22, t0.Add(70*time.Second)),
		reading("b3", "gh-002", T, 30, t0.Add(70*time.Second)),
	)

	stop := p.startAggregator()
	aggs := p.decodeAggregates(p.collect(schemas.TopicAggregates, 3, 60*time.Second))
	if len(aggs) != 3 {
		t.Fatalf("agregados = %d, quero 3: %+v", len(aggs), aggs)
	}
	gh1 := aggs[telemetry.WindowKey{GreenhouseID: "gh-001", Type: T, Start: t0}]
	if gh1.SampleCount != 2 || gh1.MinValue != 20 || gh1.MaxValue != 24 || gh1.MeanValue != 22 {
		t.Errorf("gh-001 temperatura = %+v (a duplicata não pode contar)", gh1)
	}
	if alerts := p.collect(schemas.TopicAlerts, 1, 30*time.Second); len(alerts) != 1 || string(alerts[0].Key) != "gh-002" {
		t.Errorf("esperava 1 alerta da gh-002, vieram %d", len(alerts))
	}
	dlq := p.collect(schemas.TopicReadingsDLQ, 2, 30*time.Second)
	reasons := map[string]bool{}
	for _, r := range dlq {
		for _, h := range r.Headers {
			if h.Key == aggregation.HeaderReason {
				reasons[string(h.Value)] = true
			}
		}
	}
	if len(dlq) != 2 || !reasons[aggregation.ReasonInvalid] || !reasons[aggregation.ReasonMalformed] {
		t.Errorf("DLQ = %d mensagens, motivos %v", len(dlq), reasons)
	}

	// Offsets commitados param no início das janelas abertas (segundo minuto).
	admin := kadm.NewClient(p.producer)
	offsets, err := admin.FetchOffsets(context.Background(), "it-aggregator")
	if err != nil || offsets.Error() != nil {
		t.Fatalf("fetch offsets: %v %v", err, offsets.Error())
	}
	if len(offsets) == 0 {
		t.Fatal("nenhum offset commitado")
	}
	stop()

	// "Crash" e nova instância: chega a leitura que fecha o segundo minuto.
	p.send(
		reading("a5", "gh-001", T, 26, t0.Add(90*time.Second)),
		reading("a6", "gh-001", T, 23, t0.Add(140*time.Second)),
		reading("b4", "gh-002", T, 23, t0.Add(140*time.Second)),
	)
	stop = p.startAggregator()
	defer stop()

	all := p.decodeAggregates(p.collect(schemas.TopicAggregates, 5, 60*time.Second))
	if len(all) != 5 {
		t.Fatalf("agregados depois do restart = %d, quero 5 (3 antigos + 2 do segundo minuto, sem repetição)", len(all))
	}
	second := all[telemetry.WindowKey{GreenhouseID: "gh-001", Type: T, Start: t0.Add(time.Minute)}]
	if second.SampleCount != 2 || second.MeanValue != 24 {
		t.Errorf("segundo minuto da gh-001 = %+v, quero as leituras de antes e depois do restart", second)
	}
}

type noThroughput struct{}

func (noThroughput) PerSecond() (float64, int) { return 0, 0 }

// A API de consulta lê a DLQ do broker: confere ordem (mais nova primeiro),
// limite e leitura dos headers de origem.
func TestInspectorReadsRecentDeadLetters(t *testing.T) {
	e := startRedpanda(t)
	ctx := context.Background()
	if _, err := kafka.EnsureTopics(ctx, e.seeds, 1, kafka.PipelineTopics(1, 0)); err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(e.seeds...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for i, key := range []string{"gh-a", "gh-b", "gh-c"} {
		rec := &kgo.Record{Topic: schemas.TopicReadingsDLQ, Key: []byte(key), Value: []byte("x"), Headers: []kgo.RecordHeader{
			{Key: aggregation.HeaderReason, Value: []byte("INVALID_READING")},
			{Key: aggregation.HeaderError, Value: []byte("fora da faixa")},
			{Key: aggregation.HeaderSourceTopic, Value: []byte(schemas.TopicReadings)},
			{Key: aggregation.HeaderSourcePartition, Value: []byte("2")},
			{Key: aggregation.HeaderSourceOffset, Value: []byte(string(rune('0' + i)))},
		}}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	in, err := kafka.NewInspector(e.seeds, "greenhouse-aggregator", noThroughput{})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	got, err := in.Recent(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Key != "gh-c" || got[1].Key != "gh-b" {
		t.Fatalf("esperado [gh-c gh-b], veio %+v", got)
	}
	if got[0].Reason != "INVALID_READING" || got[0].SourcePartition != 2 || got[0].SourceOffset != 2 || got[0].SourceTopic != schemas.TopicReadings {
		t.Fatalf("headers mal lidos: %+v", got[0])
	}
}
