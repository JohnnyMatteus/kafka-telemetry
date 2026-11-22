package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/api"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/dlq"
)

type replaySource struct{}

func (replaySource) Read(_ context.Context, from int64, limit int) ([]dlq.Entry, int64, error) {
	var out []dlq.Entry
	for o := from; o < 3 && len(out) < limit; o++ {
		out = append(out, dlq.Entry{Offset: o, Key: []byte("gh-001"), Value: []byte("x"),
			Headers: map[string]string{aggregation.HeaderReason: "unknown_schema", aggregation.HeaderSourceTopic: "greenhouse.readings"}})
	}
	return out, 3, nil
}

type replayPublisher struct {
	mu   sync.Mutex
	sent []aggregation.Message
}

func (p *replayPublisher) Publish(_ context.Context, m []aggregation.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent = append(p.sent, m...)
	return nil
}

type fixedDecoder struct{}

func (fixedDecoder) Decode(_ context.Context, _ []byte, v any) error {
	v.(*contract.SensorReadingRecord).MeasuredAt = now
	return nil
}

type noCheckpoints struct{}

func (noCheckpoints) Load(context.Context) (map[aggregation.TopicPartition]aggregation.Checkpoint, error) {
	return nil, nil
}

func newReplayServer(t *testing.T) (*httptest.Server, *replayPublisher) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	pub := &replayPublisher{}
	r, err := dlq.New(dlq.Config{Destination: "greenhouse.readings", DLQTopic: "greenhouse.readings.dlq", MaxBatch: 2, WindowSize: time.Minute},
		dlq.Deps{Source: replaySource{}, Publisher: pub, Ledger: dlq.NewRedisLedger(rdb), Decoder: fixedDecoder{}, Checkpoints: noCheckpoints{}})
	if err != nil {
		t.Fatal(err)
	}
	auth, _ := api.NewOperatorAuth([]string{"ana:" + anaToken})
	mux := http.NewServeMux()
	for _, rt := range api.Routes(api.Deps{Auth: auth, Replay: r}) {
		mux.Handle(rt.Pattern, rt.Handler)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, pub
}

func post(t *testing.T, srv *httptest.Server, token, body string) (int, map[string]any) {
	t.Helper()
	f := fixture{srv: srv}
	return f.doWith(t, "POST", "/dlq/replay", body, token)
}

func TestReplayRequiresOperatorToken(t *testing.T) {
	srv, pub := newReplayServer(t)
	if code, _ := post(t, srv, "", `{"dry_run":false}`); code != 401 {
		t.Fatalf("sem token: %d", code)
	}
	if code, _ := post(t, srv, "Bearer errado-errado", `{"dry_run":false}`); code != 401 {
		t.Fatalf("token inválido: %d", code)
	}
	if len(pub.sent) != 0 {
		t.Fatal("publicou sem autenticação")
	}
}

func TestReplayDefaultsToDryRun(t *testing.T) {
	srv, pub := newReplayServer(t)
	for _, body := range []string{``, `{}`, `{"limit":2}`, `{"dry_run":true}`} {
		code, res := post(t, srv, "Bearer "+anaToken, body)
		if code != 200 || res["dry_run"] != true || len(pub.sent) != 0 {
			t.Fatalf("corpo %q: code=%d res=%v enviadas=%d", body, code, res, len(pub.sent))
		}
	}
}

func TestReplayExecutesOnceWithAuthorFromToken(t *testing.T) {
	srv, pub := newReplayServer(t)
	code, res := post(t, srv, "Bearer "+anaToken, `{"limit":2,"dry_run":false}`)
	if code != 200 || res["replayed"] != 2.0 || res["operator"] != "ana" || res["destination_topic"] != "greenhouse.readings" {
		t.Fatalf("code=%d res=%v", code, res)
	}
	if pub.sent[0].Headers[dlq.HeaderReplayedBy] != "ana" || pub.sent[0].Topic != "greenhouse.readings" {
		t.Fatalf("mensagem = %+v", pub.sent[0])
	}
	// Repetir o mesmo pedido só pega o que sobrou (1 de 3), nunca republica.
	_, res = post(t, srv, "Bearer "+anaToken, `{"limit":2,"dry_run":false}`)
	if res["replayed"] != 1.0 || len(pub.sent) != 3 {
		t.Fatalf("res=%v enviadas=%d", res, len(pub.sent))
	}
}

func TestReplayRejectsDestinationLimitAndReason(t *testing.T) {
	srv, pub := newReplayServer(t)
	for name, body := range map[string]string{
		"destino no corpo":    `{"destination":"greenhouse.aggregates","dry_run":false}`,
		"limite acima":        `{"limit":3,"dry_run":false}`,
		"limite negativo":     `{"limit":-1}`,
		"motivo desconhecido": `{"reason":"x"}`,
		"json inválido":       `{`,
	} {
		if code, _ := post(t, srv, "Bearer "+anaToken, body); code != 400 {
			t.Errorf("%s: code=%d", name, code)
		}
	}
	if len(pub.sent) != 0 {
		t.Fatal("publicou numa requisição inválida")
	}
}

func TestReplayRouteAbsentWithoutReplayer(t *testing.T) {
	f := newFixture(t, fakeHealth{})
	code, _ := f.do(t, "POST", "/dlq/replay", "{}")
	if code != 404 && code != 405 {
		t.Fatalf("code = %d", code)
	}
}

func TestReplayBackfillIsRefusedWhenNotConfigured(t *testing.T) {
	srv, pub := newReplayServer(t)
	code, _ := post(t, srv, "Bearer "+anaToken, `{"backfill":true,"dry_run":false}`)
	if code != 501 || len(pub.sent) != 0 {
		t.Fatalf("code=%d enviadas=%d", code, len(pub.sent))
	}
}
