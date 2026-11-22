package dlq_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/config"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/dlq"
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

func entry(offset int64, reason, source string) dlq.Entry {
	return dlq.Entry{Offset: offset, Key: []byte("gh-001"), Value: []byte("payload-" + strconv.Itoa(int(offset))),
		Headers: map[string]string{aggregation.HeaderReason: reason, aggregation.HeaderSourceTopic: source}}
}

type fixture struct {
	r   *dlq.Reprocessor
	pub *fakePublisher
	src *fakeSource
}

func newFixture(t *testing.T, entries ...dlq.Entry) fixture {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	f := fixture{pub: &fakePublisher{}, src: &fakeSource{entries: entries}}
	r, err := dlq.New(dlq.Config{Destination: readings, DLQTopic: deadQ, MaxBatch: 5},
		dlq.Deps{Source: f.src, Publisher: f.pub, Ledger: dlq.NewRedisLedger(rdb),
			Clock: func() time.Time { return time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	f.r = r
	return f
}

func TestDryRunIsTheDefaultAndWritesNothing(t *testing.T) {
	f := newFixture(t, entry(0, "unknown_schema", readings), entry(1, "unknown_schema", readings))
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
	f := newFixture(t, entry(0, "unknown_schema", readings), entry(1, "malformed", readings))
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
	var es []dlq.Entry
	for i := range 8 {
		es = append(es, entry(int64(i), "unknown_schema", readings))
	}
	f := newFixture(t, es...)
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
	f := newFixture(t, entry(0, "unknown_schema", "outro.topico"), entry(1, "unknown_schema", readings))
	res, err := f.r.Run(context.Background(), dlq.Request{Operator: "ana", Execute: true})
	if err != nil || res.Replayed != 1 || res.Skipped["foreign_source"] != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if f.pub.sent[0].Topic != readings {
		t.Fatalf("destino = %s", f.pub.sent[0].Topic)
	}
}

func TestReasonFilterAndValidation(t *testing.T) {
	f := newFixture(t, entry(0, "malformed", readings), entry(1, "unknown_schema", readings))
	res, err := f.r.Run(context.Background(), dlq.Request{Operator: "ana", Reason: "unknown_schema", Execute: true})
	if err != nil || res.Replayed != 1 || res.Selected[0].Offset != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if _, err := f.r.Run(context.Background(), dlq.Request{Reason: "qualquer"}); !errors.Is(err, dlq.ErrUnknownReason) {
		t.Fatalf("err = %v", err)
	}
}

func TestExecuteWithoutOperatorIsRefused(t *testing.T) {
	f := newFixture(t, entry(0, "malformed", readings))
	if _, err := f.r.Run(context.Background(), dlq.Request{Execute: true}); !errors.Is(err, dlq.ErrOperatorRequired) {
		t.Fatalf("err = %v", err)
	}
	if len(f.pub.sent) != 0 {
		t.Fatal("publicou sem autoria")
	}
}

func TestPublishFailureLeavesMessagesRetryable(t *testing.T) {
	f := newFixture(t, entry(0, "malformed", readings))
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

func TestNewRefusesUnsafeConfig(t *testing.T) {
	deps := dlq.Deps{Source: &fakeSource{}, Publisher: &fakePublisher{}, Ledger: dlq.NewRedisLedger(redis.NewClient(&redis.Options{}))}
	for name, cfg := range map[string]dlq.Config{
		"destino é a DLQ":  {Destination: deadQ, DLQTopic: deadQ, MaxBatch: 1},
		"destino vazio":    {DLQTopic: deadQ, MaxBatch: 1},
		"sem teto de lote": {Destination: readings, DLQTopic: deadQ},
	} {
		if _, err := dlq.New(cfg, deps); err == nil {
			t.Errorf("%s: esperava erro", name)
		}
	}
}

func TestConfigFromEnvCapsBatchAndPinsDestination(t *testing.T) {
	t.Setenv("DLQ_REPLAY_MAX_BATCH", "999999")
	var env config.Env
	cfg := dlq.ConfigFromEnv(&env)
	if cfg.MaxBatch != 1000 || cfg.Destination != readings || cfg.DLQTopic != deadQ {
		t.Fatalf("cfg = %+v", cfg)
	}
}
