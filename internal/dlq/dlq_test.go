package dlq_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/config"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/dlq"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

const (
	readings = "greenhouse.readings"
	deadQ    = "greenhouse.readings.dlq"
)

type fakeSource struct{ entries []dlq.Entry }

func (f *fakeSource) Read(_ context.Context, from int64, limit int) ([]dlq.Entry, int64, error) {
	var out []dlq.Entry
	for _, e := range f.entries {
		if e.Offset >= from && len(out) < limit {
			out = append(out, e)
		}
	}
	return out, int64(len(f.entries)), nil
}

type fakePublisher struct {
	mu   sync.Mutex
	sent []aggregation.Message
	err  error
}

func (f *fakePublisher) Publish(_ context.Context, msgs []aggregation.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, msgs...)
	return nil
}

var t0 = time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

// registry é um Schema Registry em memória com os schemas do repositório.
type registry struct{ schemas map[int]string }

func (r registry) SchemaByID(_ context.Context, id int) (string, error) {
	if s, ok := r.schemas[id]; ok {
		return s, nil
	}
	return "", fmt.Errorf("id %d: %w", id, contract.ErrUnknownSchema)
}

func latestSchema(t *testing.T, topic string) string {
	t.Helper()
	sub, err := schemas.Lookup(topic)
	if err != nil {
		t.Fatal(err)
	}
	text, err := sub.Latest()
	if err != nil {
		t.Fatal(err)
	}
	return text
}

func newRegistry(t *testing.T) registry {
	return registry{schemas: map[int]string{
		1: latestSchema(t, schemas.TopicReadings), 2: latestSchema(t, schemas.TopicAggregates), 3: latestSchema(t, schemas.TopicAlerts),
	}}
}

// encodedReading é uma leitura válida no formato de fio.
func encodedReading(t *testing.T, reg registry, id string, at time.Time) []byte {
	t.Helper()
	enc, err := contract.NewEncoder(1, reg.schemas[1])
	if err != nil {
		t.Fatal(err)
	}
	v, err := enc.Encode(contract.NewSensorReadingRecord(telemetry.Reading{EventID: id, GreenhouseID: "gh-001", SensorID: "gh-001-t",
		Type: telemetry.SensorTemperature, Value: 22, MeasuredAt: at}, "1.0.0"))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

type memCheckpoints struct {
	cps map[aggregation.TopicPartition]aggregation.Checkpoint
}

func (m *memCheckpoints) Load(context.Context) (map[aggregation.TopicPartition]aggregation.Checkpoint, error) {
	return m.cps, nil
}

func entryWith(offset int64, reason, source string, value []byte) dlq.Entry {
	return dlq.Entry{Offset: offset, Timestamp: t0.Add(10 * time.Minute), Key: []byte("gh-001"), Value: value,
		Headers: map[string]string{aggregation.HeaderReason: reason, aggregation.HeaderSourceTopic: source, aggregation.HeaderSourcePartition: "0"}}
}

type fixture struct {
	r    *dlq.Reprocessor
	pub  *fakePublisher
	src  *fakeSource
	reg  registry
	cps  *memCheckpoints
	rdb  *redis.Client
	deps dlq.Deps
}

func (f fixture) entry(t *testing.T, offset int64, reason, source string) dlq.Entry {
	return entryWith(offset, reason, source, encodedReading(t, f.reg, "ev-"+strconv.Itoa(int(offset)), t0))
}

func newFixture(t *testing.T, cfgFn func(*dlq.Config), mk func(f fixture) []dlq.Entry) fixture {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	f := fixture{pub: &fakePublisher{}, src: &fakeSource{}, reg: newRegistry(t), rdb: rdb,
		cps: &memCheckpoints{cps: map[aggregation.TopicPartition]aggregation.Checkpoint{}}}
	if mk != nil {
		f.src.entries = mk(f)
	}
	cfg := dlq.Config{Destination: readings, DLQTopic: deadQ, MaxBatch: 5, WindowSize: time.Minute}
	if cfgFn != nil {
		cfgFn(&cfg)
	}
	f.deps = dlq.Deps{Source: f.src, Publisher: f.pub, Ledger: dlq.NewRedisLedger(rdb), Decoder: contract.NewDecoder(f.reg),
		Checkpoints: f.cps, Clock: func() time.Time { return t0 }}
	r, err := dlq.New(cfg, f.deps)
	if err != nil {
		t.Fatal(err)
	}
	f.r = r
	return f
}

func fixed(t *testing.T, es ...func(f fixture) dlq.Entry) fixture {
	return newFixture(t, nil, func(f fixture) []dlq.Entry {
		out := make([]dlq.Entry, len(es))
		for i, e := range es {
			out[i] = e(f)
		}
		return out
	})
}

func e(t *testing.T, offset int64, reason, source string) func(f fixture) dlq.Entry {
	return func(f fixture) dlq.Entry { return f.entry(t, offset, reason, source) }
}

func TestDryRunIsTheDefaultAndWritesNothing(t *testing.T) {
	f := fixed(t, e(t, 0, "unknown_schema", readings), e(t, 1, "unknown_schema", readings))
	res, err := f.r.Run(context.Background(), dlq.Request{Operator: "ana"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.DryRun || len(res.Selected) != 2 || res.Replayed != 0 || len(f.pub.sent) != 0 {
		t.Fatalf("res=%+v enviadas=%d", res, len(f.pub.sent))
	}
	// Nada foi marcado: a execução real ainda enxerga as duas.
	res, err = f.r.Run(context.Background(), dlq.Request{Operator: "ana", Execute: true})
	if err != nil || res.Replayed != 2 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestExecuteIsIdempotentAndRecordsAuthor(t *testing.T) {
	f := fixed(t, e(t, 0, "unknown_schema", readings), e(t, 1, "invalid_reading", readings))
	ctx := context.Background()
	res, err := f.r.Run(ctx, dlq.Request{Operator: "ana", Execute: true})
	if err != nil || res.Replayed != 2 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	for _, m := range f.pub.sent {
		if m.Topic != readings || m.Headers[dlq.HeaderReplayedBy] != "ana" || m.Headers[dlq.HeaderReplayRun] != res.RunID {
			t.Fatalf("mensagem = %+v", m)
		}
	}
	res, err = f.r.Run(ctx, dlq.Request{Operator: "bruno", Execute: true})
	if err != nil || res.Replayed != 0 || len(f.pub.sent) != 2 {
		t.Fatalf("segunda execução republicou: res=%+v enviadas=%d", res, len(f.pub.sent))
	}
}

func TestBatchLimitAndCursor(t *testing.T) {
	f := newFixture(t, nil, func(f fixture) []dlq.Entry {
		var es []dlq.Entry
		for i := range 8 {
			es = append(es, f.entry(t, int64(i), "unknown_schema", readings))
		}
		return es
	})
	ctx := context.Background()
	if _, err := f.r.Run(ctx, dlq.Request{Limit: 6}); !errors.Is(err, dlq.ErrBatchLimit) {
		t.Fatalf("limite acima do teto: err=%v", err)
	}
	if _, err := f.r.Run(ctx, dlq.Request{Limit: -1}); !errors.Is(err, dlq.ErrBatchLimit) {
		t.Fatalf("limite negativo: err=%v", err)
	}
	res, _ := f.r.Run(ctx, dlq.Request{Operator: "ana", Limit: 3, Execute: true})
	if res.Replayed != 3 {
		t.Fatalf("replayed = %d", res.Replayed)
	}
	res, _ = f.r.Run(ctx, dlq.Request{Operator: "ana", Limit: 5, Execute: true})
	if res.Replayed != 5 || len(f.pub.sent) != 8 || res.Scanned != 5 {
		t.Fatalf("res=%+v enviadas=%d (o cursor devia pular os 3 primeiros)", res, len(f.pub.sent))
	}
}

func TestOnlyConfiguredDestinationIsEligible(t *testing.T) {
	f := fixed(t, e(t, 0, "unknown_schema", "outro.topico"), e(t, 1, "unknown_schema", readings))
	res, err := f.r.Run(context.Background(), dlq.Request{Operator: "ana", Execute: true})
	if err != nil || res.Replayed != 1 || res.Skipped["foreign_source"] != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if f.pub.sent[0].Topic != readings {
		t.Fatalf("destino = %s", f.pub.sent[0].Topic)
	}
}

func TestReasonFilterAndValidation(t *testing.T) {
	f := fixed(t, e(t, 0, "malformed", readings), e(t, 1, "unknown_schema", readings))
	res, err := f.r.Run(context.Background(), dlq.Request{Operator: "ana", Reason: "unknown_schema", Execute: true})
	if err != nil || res.Replayed != 1 || res.Selected[0].Offset != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if _, err := f.r.Run(context.Background(), dlq.Request{Reason: "qualquer"}); !errors.Is(err, dlq.ErrUnknownReason) {
		t.Fatalf("err = %v", err)
	}
}

func TestExecuteWithoutOperatorIsRefused(t *testing.T) {
	f := fixed(t, e(t, 0, "malformed", readings))
	if _, err := f.r.Run(context.Background(), dlq.Request{Execute: true}); !errors.Is(err, dlq.ErrOperatorRequired) {
		t.Fatalf("err = %v", err)
	}
	if len(f.pub.sent) != 0 {
		t.Fatal("publicou sem autoria")
	}
}

func TestPublishFailureLeavesMessagesRetryable(t *testing.T) {
	f := fixed(t, e(t, 0, "unknown_schema", readings))
	f.pub.err = errors.New("broker fora")
	if _, err := f.r.Run(context.Background(), dlq.Request{Operator: "ana", Execute: true}); err == nil {
		t.Fatal("esperava erro")
	}
	f.pub.err = nil
	res, err := f.r.Run(context.Background(), dlq.Request{Operator: "ana", Execute: true})
	if err != nil || res.Replayed != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestOnlyOneRunAtATime(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ledger := dlq.NewRedisLedger(rdb)
	release, err := ledger.Lock(context.Background(), "a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Lock(context.Background(), "b", time.Minute); !errors.Is(err, dlq.ErrBusy) {
		t.Fatalf("err = %v", err)
	}
	release()
	if _, err := ledger.Lock(context.Background(), "b", time.Minute); err != nil {
		t.Fatalf("lock liberado: %v", err)
	}
}

// slowSource simula um broker lento: só devolve quando o contexto acaba.
type slowSource struct{}

func (slowSource) Read(ctx context.Context, _ int64, _ int) ([]dlq.Entry, int64, error) {
	<-ctx.Done()
	return nil, 0, ctx.Err()
}

// O lock não é renovado: a execução inteira tem de acabar antes do TTL, senão
// outra começaria enquanto esta ainda publica.
func TestRunIsBoundedByABudgetShorterThanTheLockTTL(t *testing.T) {
	f := fixed(t, e(t, 0, "unknown_schema", readings))
	f.deps.Source = slowSource{}
	r, err := dlq.New(dlq.Config{Destination: readings, DLQTopic: deadQ, MaxBatch: 5, WindowSize: time.Minute, RunBudget: 50 * time.Millisecond}, f.deps)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = r.Run(context.Background(), dlq.Request{Operator: "ana", Execute: true})
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 5*time.Second {
		t.Fatalf("err=%v em %s", err, time.Since(start))
	}
	if len(f.pub.sent) != 0 {
		t.Fatal("publicou depois do prazo")
	}
	// O lock foi liberado: uma execução normal segue possível.
	f.deps.Source = f.src
	r2, _ := dlq.New(dlq.Config{Destination: readings, DLQTopic: deadQ, MaxBatch: 5, WindowSize: time.Minute}, f.deps)
	if res, err := r2.Run(context.Background(), dlq.Request{Operator: "ana", Execute: true}); err != nil || res.Replayed != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if _, err := dlq.New(dlq.Config{Destination: readings, DLQTopic: deadQ, MaxBatch: 5, WindowSize: time.Minute, RunBudget: 10 * time.Minute}, f.deps); err == nil {
		t.Fatal("prazo maior que o TTL do lock deveria ser recusado")
	}
}

func TestNewRefusesUnsafeConfig(t *testing.T) {
	f := fixed(t)
	for name, cfg := range map[string]dlq.Config{
		"destino é a DLQ":  {Destination: deadQ, DLQTopic: deadQ, MaxBatch: 1, WindowSize: time.Minute},
		"destino vazio":    {DLQTopic: deadQ, MaxBatch: 1, WindowSize: time.Minute},
		"sem teto de lote": {Destination: readings, DLQTopic: deadQ, WindowSize: time.Minute},
		"sem janela":       {Destination: readings, DLQTopic: deadQ, MaxBatch: 1},
	} {
		if _, err := dlq.New(cfg, f.deps); err == nil {
			t.Errorf("%s: esperava erro", name)
		}
	}
}

func TestConfigFromEnvCapsBatchAndPinsDestination(t *testing.T) {
	t.Setenv("DLQ_REPLAY_MAX_BATCH", "999999")
	var env config.Env
	cfg := dlq.ConfigFromEnv(&env)
	if cfg.MaxBatch != 1000 || cfg.Destination != readings || cfg.DLQTopic != deadQ || cfg.WindowSize != time.Minute {
		t.Fatalf("cfg = %+v", cfg)
	}
}

type outcomes struct {
	aggregation.NopObserver
	got []string
}

func (o *outcomes) RecordProcessed(s string) { o.got = append(o.got, s) }

type noopCommitter struct{}

func (noopCommitter) Commit(context.Context, map[aggregation.TopicPartition]int64) error { return nil }

type collectPublisher struct{ msgs []aggregation.Message }

func (c *collectPublisher) Publish(_ context.Context, m []aggregation.Message) error {
	c.msgs = append(c.msgs, m...)
	return nil
}

// memStore é o CheckpointStore do agregador; o reprocessador lê dele.
type memStore struct {
	cps map[aggregation.TopicPartition]aggregation.Checkpoint
}

func (m *memStore) Save(_ context.Context, cps []aggregation.Checkpoint) error {
	for _, cp := range cps {
		m.cps[cp.TopicPartition] = cp
	}
	return nil
}
func (m *memStore) Load(context.Context) (map[aggregation.TopicPartition]aggregation.Checkpoint, error) {
	return m.cps, nil
}

// Ponta a ponta com o agregador de verdade: uma leitura antiga, que o
// agregador descartaria como atrasada, não é republicada nem marcada; a
// recente é republicada e o agregador a aceita.
func TestReplayAgainstTheAggregatorDoesNotLoseLateReadings(t *testing.T) {
	reg := newRegistry(t)
	store := &memStore{cps: map[aggregation.TopicPartition]aggregation.Checkpoint{}}
	obs := &outcomes{}
	aggEnc, _ := contract.NewEncoder(2, reg.schemas[2])
	alertEnc, _ := contract.NewEncoder(3, reg.schemas[3])
	svc, err := aggregation.New(telemetry.WindowConfig{Size: time.Minute, AllowedLateness: 10 * time.Second}, telemetry.DefaultThresholds(),
		aggregation.Topics{Aggregates: schemas.TopicAggregates, Alerts: schemas.TopicAlerts, DLQ: schemas.TopicReadingsDLQ},
		aggregation.Deps{Decoder: contract.NewDecoder(reg), AggregateCodec: aggEnc, AlertCodec: alertEnc, Publisher: &collectPublisher{},
			Checkpoints: store, Committer: noopCommitter{}, Observer: obs, Clock: func() time.Time { return t0.Add(10 * time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	tp := aggregation.TopicPartition{Topic: readings, Partition: 0}
	if err := svc.Assign(context.Background(), []aggregation.TopicPartition{tp}); err != nil {
		t.Fatal(err)
	}
	// O fluxo ao vivo avança o watermark para 12:09:20.
	live := aggregation.Record{TopicPartition: tp, Offset: 100, Key: []byte("gh-001"), Value: encodedReading(t, reg, "live", t0.Add(9*time.Minute+30*time.Second))}
	if err := svc.HandleBatch(context.Background(), []aggregation.Record{live}); err != nil {
		t.Fatal(err)
	}

	old := encodedReading(t, reg, "old", t0)                                         // janela 12:00 já fechou
	recent := encodedReading(t, reg, "recent", t0.Add(9*time.Minute+40*time.Second)) // janela 12:09 ainda aberta
	f := newFixture(t, nil, func(fixture) []dlq.Entry {
		return []dlq.Entry{entryWith(0, "unknown_schema", readings, old), entryWith(1, "unknown_schema", readings, recent),
			entryWith(2, "malformed", readings, []byte("lixo"))}
	})
	f.deps.Checkpoints = store
	r, err := dlq.New(dlq.Config{Destination: readings, DLQTopic: deadQ, MaxBatch: 5, WindowSize: time.Minute}, f.deps)
	if err != nil {
		t.Fatal(err)
	}

	dry, err := r.Run(context.Background(), dlq.Request{Operator: "ana"})
	if err != nil || len(dry.Selected) != 1 || dry.Selected[0].Offset != 1 || len(dry.Blocked) != 2 ||
		dry.Blocked[0].Action != dlq.ActionWouldBeLate || dry.Blocked[1].Action != dlq.ActionUndecodable {
		t.Fatalf("dry-run = %+v err=%v", dry, err)
	}
	res, err := r.Run(context.Background(), dlq.Request{Operator: "ana", Execute: true})
	if err != nil || res.Replayed != 1 || len(f.pub.sent) != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}

	obs.got = nil
	back := aggregation.Record{TopicPartition: tp, Offset: 101, Key: f.pub.sent[0].Key, Value: f.pub.sent[0].Value}
	if err := svc.HandleBatch(context.Background(), []aggregation.Record{back}); err != nil {
		t.Fatal(err)
	}
	if len(obs.got) != 1 || obs.got[0] != "accepted" {
		t.Fatalf("o agregador não aceitou a leitura republicada: %v", obs.got)
	}
	// Prova de que o bloqueio estava certo: a antiga seria descartada.
	obs.got = nil
	if err := svc.HandleBatch(context.Background(), []aggregation.Record{{TopicPartition: tp, Offset: 102, Value: old}}); err != nil {
		t.Fatal(err)
	}
	if len(obs.got) != 1 || obs.got[0] != "late" {
		t.Fatalf("esperava late, veio %v", obs.got)
	}
	// A atrasada nunca foi publicada; só a recente saiu, e o que sobra na
	// DLQ é o que ainda não decodifica.
	again, _ := r.Run(context.Background(), dlq.Request{Operator: "ana", Execute: true})
	if again.Replayed != 0 || len(f.pub.sent) != 1 || len(again.Blocked) != 1 || again.Blocked[0].Action != dlq.ActionUndecodable {
		t.Fatalf("again = %+v enviadas=%d", again, len(f.pub.sent))
	}
}

// Com watermark por estufa, o relógio adiantado de uma estufa não torna
// atrasada a leitura de outra: a classificação usa o watermark da própria.
func TestClassificationUsesTheGreenhouseWatermark(t *testing.T) {
	reg := newRegistry(t)
	enc, _ := contract.NewEncoder(1, reg.schemas[1])
	value := func(gh string) []byte {
		v, err := enc.Encode(contract.NewSensorReadingRecord(telemetry.Reading{EventID: "e-" + gh, GreenhouseID: gh, SensorID: gh + "-t",
			Type: telemetry.SensorTemperature, Value: 22, MeasuredAt: t0}, "1.0.0"))
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	f := newFixture(t, nil, func(fixture) []dlq.Entry {
		return []dlq.Entry{entryWith(0, "unknown_schema", readings, value("gh-001")), entryWith(1, "unknown_schema", readings, value("gh-002"))}
	})
	f.cps.cps[aggregation.TopicPartition{Topic: readings, Partition: 0}] = aggregation.Checkpoint{
		Watermark:  t0.Add(10 * time.Minute),
		Watermarks: map[string]time.Time{"gh-001": t0.Add(10 * time.Minute), "gh-002": t0.Add(10 * time.Second)},
	}
	res, err := f.r.Run(context.Background(), dlq.Request{Operator: "ana"})
	if err != nil || len(res.Selected) != 1 || res.Selected[0].Offset != 1 || len(res.Blocked) != 1 || res.Blocked[0].Offset != 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}
