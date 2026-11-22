package dlq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/dlq"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

func encodeReading(t *testing.T, reg registry, id string, value float64, at time.Time) []byte {
	t.Helper()
	enc, err := contract.NewEncoder(1, reg.schemas[1])
	if err != nil {
		t.Fatal(err)
	}
	v, err := enc.Encode(contract.NewSensorReadingRecord(telemetry.Reading{EventID: id, GreenhouseID: "gh-001", SensorID: "gh-001-t",
		Type: telemetry.SensorTemperature, Value: value, MeasuredAt: at}, "1.0.0"))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// aggregatesFromTopic lê do que o agregador publicou o agregado emitido de
// cada janela: é a base que o Postgres teria depois do sink JDBC.
type aggregatesFromTopic struct {
	t    *testing.T
	reg  registry
	pub  *collectPublisher
	size time.Duration
}

func (a aggregatesFromTopic) Window(ctx context.Context, gh, sensor string, start time.Time) (dlq.Base, error) {
	for _, m := range a.pub.msgs {
		if m.Topic != schemas.TopicAggregates {
			continue
		}
		var rec contract.WindowAggregateRecord
		if err := contract.NewDecoder(a.reg).Decode(ctx, m.Value, &rec); err != nil {
			a.t.Fatal(err)
		}
		if rec.GreenhouseID == gh && rec.SensorType == sensor && rec.WindowStart.Equal(start) {
			return dlq.Base{Found: true, Count: rec.SampleCount, Sum: rec.MeanValue * float64(rec.SampleCount), Min: rec.MinValue, Max: rec.MaxValue}, nil
		}
	}
	return dlq.Base{}, nil
}

type backfillFixture struct {
	r        *dlq.Reprocessor
	svc      *aggregation.Service
	live     *collectPublisher // o que o agregador publicou
	bf       *fakePublisher    // o que o backfill publicou
	store    *dlq.RedisWindowStore
	deps     dlq.Deps
	reg      registry
	src      *fakeSource
	obs      *outcomes
	failNext *error
}

type switchPublisher struct {
	inner *fakePublisher
	err   *error
}

func (s switchPublisher) Publish(ctx context.Context, m []aggregation.Message) error {
	if *s.err != nil {
		return *s.err
	}
	return s.inner.Publish(ctx, m)
}

// newBackfillFixture sobe o agregador de verdade, fecha a janela 12:00 da
// gh-001 (leituras 20 e 24) e monta o reprocessador com backfill sobre a DLQ.
func newBackfillFixture(t *testing.T, dlqEntries func(t *testing.T, reg registry) []dlq.Entry) backfillFixture {
	t.Helper()
	reg := newRegistry(t)
	cps := &memStore{cps: map[aggregation.TopicPartition]aggregation.Checkpoint{}}
	live := &collectPublisher{}
	obs := &outcomes{}
	aggEnc, _ := contract.NewEncoder(2, reg.schemas[2])
	alertEnc, _ := contract.NewEncoder(3, reg.schemas[3])
	win := telemetry.WindowConfig{Size: time.Minute, AllowedLateness: 10 * time.Second, Scope: telemetry.ScopeGreenhouse}
	svc, err := aggregation.New(win, telemetry.DefaultThresholds(),
		aggregation.Topics{Aggregates: schemas.TopicAggregates, Alerts: schemas.TopicAlerts, DLQ: schemas.TopicReadingsDLQ},
		aggregation.Deps{Decoder: contract.NewDecoder(reg), AggregateCodec: aggEnc, AlertCodec: alertEnc, Publisher: live,
			Checkpoints: cps, Committer: noopCommitter{}, Observer: obs, Clock: func() time.Time { return t0.Add(10 * time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	tp := aggregation.TopicPartition{Topic: readings, Partition: 0}
	_ = svc.Assign(context.Background(), []aggregation.TopicPartition{tp})
	batch := []aggregation.Record{
		{TopicPartition: tp, Offset: 0, Key: []byte("gh-001"), Value: encodeReading(t, reg, "o1", 20, t0.Add(5*time.Second))},
		{TopicPartition: tp, Offset: 1, Key: []byte("gh-001"), Value: encodeReading(t, reg, "o2", 24, t0.Add(20*time.Second))},
		{TopicPartition: tp, Offset: 2, Key: []byte("gh-001"), Value: encodeReading(t, reg, "o3", 22, t0.Add(9*time.Minute+30*time.Second))},
	}
	if err := svc.HandleBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	bf := &fakePublisher{}
	var failNext error
	src := &fakeSource{entries: dlqEntries(t, reg)}
	store := dlq.NewRedisWindowStore(rdb)
	deps := dlq.Deps{Source: src, Publisher: switchPublisher{inner: bf, err: &failNext}, Ledger: dlq.NewRedisLedger(rdb),
		Decoder: contract.NewDecoder(reg), Checkpoints: cps, Clock: func() time.Time { return t0.Add(10 * time.Minute) },
		Backfill: &dlq.BackfillDeps{Ledger: dlq.NewRedisBackfillLedger(rdb), Windows: store,
			Reader: aggregatesFromTopic{t: t, reg: reg, pub: live, size: time.Minute}, AggregateCodec: aggEnc, AlertCodec: alertEnc,
			Thresholds: telemetry.DefaultThresholds()}}
	r, err := dlq.New(dlq.Config{Destination: readings, DLQTopic: deadQ, MaxBatch: 10, WindowSize: time.Minute}, deps)
	if err != nil {
		t.Fatal(err)
	}
	return backfillFixture{r: r, svc: svc, live: live, bf: bf, store: store, deps: deps, reg: reg, src: src, obs: obs, failNext: &failNext}
}

func lateEntries(t *testing.T, reg registry) []dlq.Entry {
	// Três leituras da janela 12:00 que a DLQ guardou (schema que não existia na
	// hora), uma delas duplicada na DLQ, e uma leitura da janela ainda aberta.
	return []dlq.Entry{
		entryWith(0, "unknown_schema", readings, encodeReading(t, reg, "late1", 50, t0.Add(30*time.Second))),
		entryWith(1, "unknown_schema", readings, encodeReading(t, reg, "late2", 60, t0.Add(40*time.Second))),
		entryWith(2, "unknown_schema", readings, encodeReading(t, reg, "late1", 50, t0.Add(30*time.Second))),
		entryWith(3, "unknown_schema", readings, encodeReading(t, reg, "open1", 22, t0.Add(9*time.Minute+40*time.Second))),
	}
}

func aggregatesOf(t *testing.T, reg registry, msgs []aggregation.Message) []contract.WindowAggregateRecord {
	t.Helper()
	var out []contract.WindowAggregateRecord
	for _, m := range msgs {
		if m.Topic != schemas.TopicAggregates {
			continue
		}
		var rec contract.WindowAggregateRecord
		if err := contract.NewDecoder(reg).Decode(context.Background(), m.Value, &rec); err != nil {
			t.Fatal(err)
		}
		out = append(out, rec)
	}
	return out
}

func TestBackfillRecomputesAClosedWindowIdempotentlyByEventID(t *testing.T) {
	f := newBackfillFixture(t, lateEntries)
	ctx := context.Background()

	// Premissa: o agregador de fato descartaria essas leituras.
	if got := aggregatesOf(t, f.reg, f.live.msgs); len(got) != 1 || got[0].SampleCount != 2 || got[0].MeanValue != 22 {
		t.Fatalf("agregado original = %+v", got)
	}

	dry, err := f.r.Run(ctx, dlq.Request{Operator: "ana", Backfill: true})
	if err != nil || !dry.DryRun || len(f.bf.sent) != 0 || dry.Backfill == nil || len(dry.Backfill.Windows) != 1 {
		t.Fatalf("dry-run = %+v err=%v", dry, err)
	}
	w := dry.Backfill.Windows[0]
	if w.NewEvents != 2 || w.BaseCount != 2 || w.ResultCount != 4 || w.ResultMean != 38.5 || w.Alert != "ABOVE_MAX" || dry.Skipped["not_late"] != 1 {
		t.Fatalf("janela no dry-run = %+v skipped=%v", w, dry.Skipped)
	}

	res, err := f.r.Run(ctx, dlq.Request{Operator: "ana", Backfill: true, Execute: true})
	if err != nil || res.Replayed != 3 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	aggs := aggregatesOf(t, f.reg, f.bf.sent)
	if len(aggs) != 1 || aggs[0].SampleCount != 4 || aggs[0].MeanValue != 38.5 || aggs[0].MinValue != 20 || aggs[0].MaxValue != 60 ||
		!aggs[0].WindowStart.Equal(t0) {
		t.Fatalf("agregado republicado = %+v", aggs)
	}
	var alerts int
	for _, m := range f.bf.sent {
		if m.Topic == schemas.TopicAlerts {
			alerts++
		}
		if m.Headers[dlq.HeaderBackfilledBy] != "ana" {
			t.Fatalf("sem autoria: %+v", m.Headers)
		}
	}
	if alerts != 1 {
		t.Fatalf("o agregado recuperado passou do limite e devia gerar 1 alerta, gerou %d", alerts)
	}

	// Repetir não soma de novo e não republica.
	sent := len(f.bf.sent)
	if res, err = f.r.Run(ctx, dlq.Request{Operator: "ana", Backfill: true, Execute: true}); err != nil || res.Replayed != 0 || len(f.bf.sent) != sent {
		t.Fatalf("repetição: res=%+v err=%v enviadas=%d", res, err, len(f.bf.sent))
	}
	// Mesmo sem o ledger da DLQ (recibos perdidos), o conjunto de event_ids da janela impede a soma dupla.
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	f.deps.Backfill.Ledger = dlq.NewRedisBackfillLedger(rdb)
	f.deps.Ledger = dlq.NewRedisLedger(rdb)
	r2, _ := dlq.New(dlq.Config{Destination: readings, DLQTopic: deadQ, MaxBatch: 10, WindowSize: time.Minute}, f.deps)
	if _, err := r2.Run(ctx, dlq.Request{Operator: "ana", Backfill: true, Execute: true}); err != nil {
		t.Fatal(err)
	}
	st, ok, _ := f.store.State(ctx, dlq.WindowRef{GreenhouseID: "gh-001", SensorType: "TEMPERATURE", Start: t0})
	if !ok || st.Count != 2 || st.Sum != 110 {
		t.Fatalf("estado da janela = %+v (cada event_id soma uma vez)", st)
	}
}

func TestBackfillFindsLateReadingsEvenAfterTheReplayCursorPassedThem(t *testing.T) {
	f := newBackfillFixture(t, lateEntries)
	ctx := context.Background()
	// A republicação bloqueia as atrasadas e o cursor dela passa por cima.
	rep, err := f.r.Run(ctx, dlq.Request{Operator: "ana", Execute: true})
	if err != nil || rep.Replayed != 1 || len(rep.Blocked) != 3 {
		t.Fatalf("replay = %+v err=%v", rep, err)
	}
	res, err := f.r.Run(ctx, dlq.Request{Operator: "ana", Backfill: true, Execute: true})
	if err != nil || res.Replayed != 3 {
		t.Fatalf("o backfill não pode depender do cursor da republicação: %+v err=%v", res, err)
	}
}

func TestBackfillRepublishesAPendingWindowAfterAPublishFailure(t *testing.T) {
	f := newBackfillFixture(t, lateEntries)
	ctx := context.Background()
	*f.failNext = errors.New("broker fora")
	if _, err := f.r.Run(ctx, dlq.Request{Operator: "ana", Backfill: true, Execute: true}); err == nil {
		t.Fatal("esperava erro de publicação")
	}
	*f.failNext = nil
	res, err := f.r.Run(ctx, dlq.Request{Operator: "bruno", Backfill: true, Execute: true})
	if err != nil || res.Backfill.Republished != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	aggs := aggregatesOf(t, f.reg, f.bf.sent)
	if len(aggs) != 1 || aggs[0].SampleCount != 4 {
		t.Fatalf("a janela pendente devia sair uma vez, com as 4 leituras: %+v", aggs)
	}
	// E nada mais é republicado depois.
	n := len(f.bf.sent)
	if _, err := f.r.Run(ctx, dlq.Request{Operator: "bruno", Backfill: true, Execute: true}); err != nil || len(f.bf.sent) != n {
		t.Fatalf("republicou de novo: err=%v enviadas=%d", err, len(f.bf.sent))
	}
}

func TestBackfillGuardrails(t *testing.T) {
	f := newBackfillFixture(t, lateEntries)
	ctx := context.Background()
	if _, err := f.r.Run(ctx, dlq.Request{Backfill: true, Execute: true}); !errors.Is(err, dlq.ErrOperatorRequired) {
		t.Fatalf("sem operador: %v", err)
	}
	if _, err := f.r.Run(ctx, dlq.Request{Backfill: true, Limit: 11}); !errors.Is(err, dlq.ErrBatchLimit) {
		t.Fatalf("teto de lote: %v", err)
	}
	// Janela fechada há menos que a idade mínima: o sink pode não ter entregado o original.
	young := newBackfillFixture(t, func(t *testing.T, reg registry) []dlq.Entry {
		return []dlq.Entry{entryWith(0, "unknown_schema", readings, encodeReading(t, reg, "y1", 25, t0.Add(8*time.Minute)))}
	})
	res, err := young.r.Run(ctx, dlq.Request{Operator: "ana", Backfill: true, Execute: true})
	if err != nil || res.Replayed != 0 || res.Skipped["too_recent"] != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	// Sem as dependências do backfill, o pedido é recusado.
	plain := newFixture(t, nil, nil)
	if _, err := plain.r.Run(ctx, dlq.Request{Backfill: true}); !errors.Is(err, dlq.ErrBackfillUnavailable) {
		t.Fatalf("sem backfill: %v", err)
	}
}
