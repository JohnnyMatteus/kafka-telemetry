package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/alerting"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/api"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/history"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/pipeline"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/state"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

var now = time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

type fakeState struct{ list []state.Greenhouse }

func (f fakeState) Get(context.Context, string) (state.Greenhouse, error) {
	return state.Greenhouse{}, state.ErrNotFound
}
func (f fakeState) List(context.Context) ([]state.Greenhouse, error) { return f.list, nil }

type fakeSeries struct {
	from, to time.Time
	err      error
}

func (f *fakeSeries) Series(_ context.Context, _ string, from, to time.Time) (map[string]history.Series, error) {
	f.from, f.to = from, to
	return map[string]history.Series{"TEMPERATURE": {Unit: "celsius", Points: []history.Point{
		{WindowStart: now, WindowEnd: now.Add(time.Minute), SampleCount: 3, Mean: 20, Min: 19, Max: 21},
	}}}, f.err
}

type fakeAlerts struct {
	status  alerting.Status
	limit   int
	ackErr  error
	ackedBy string
}

func (f *fakeAlerts) List(_ context.Context, s alerting.Status, limit int) ([]alerting.Alert, error) {
	f.status, f.limit = s, limit
	return []alerting.Alert{{ID: "a1", GreenhouseID: "gh-001", SensorType: "TEMPERATURE", Rule: "ABOVE_MAX",
		WindowStart: now, WindowEnd: now.Add(time.Minute), ObservedValue: 36, LimitValue: 32}}, nil
}

func (f *fakeAlerts) Acknowledge(_ context.Context, id, operator, note string, at time.Time) (alerting.Alert, error) {
	f.ackedBy = operator
	if f.ackErr != nil {
		return alerting.Alert{}, f.ackErr
	}
	return alerting.Alert{ID: id, GreenhouseID: "gh-001", AcknowledgedAt: &at, AcknowledgedBy: &operator, Note: &note}, nil
}

const (
	anaToken   = "dev-only-token-ana"
	brunoToken = "dev-only-token-bruno"
)

type fakeDLQ struct{ limit int }

func (f *fakeDLQ) Recent(_ context.Context, limit int) ([]pipeline.DeadLetter, error) {
	f.limit = limit
	return []pipeline.DeadLetter{{Partition: 0, Offset: 7, Timestamp: now, Key: "gh-001", Reason: "INVALID_READING",
		Error: "valor fora da faixa", SourceTopic: "greenhouse.readings", SourcePartition: 2, SourceOffset: 99}}, nil
}

type fakeHealth struct{ err error }

func (f fakeHealth) Report(context.Context) (pipeline.Health, error) {
	return pipeline.Health{Group: "g", TotalLag: 12, Partitions: []pipeline.PartitionLag{{Partition: 0, Lag: 12}}}, f.err
}

type fixture struct {
	srv    *httptest.Server
	series *fakeSeries
	alerts *fakeAlerts
	dlq    *fakeDLQ
}

func newFixture(t *testing.T, health fakeHealth) fixture {
	t.Helper()
	f := fixture{series: &fakeSeries{}, alerts: &fakeAlerts{}, dlq: &fakeDLQ{}}
	auth, err := api.NewOperatorAuth([]string{"ana:" + anaToken, "bruno:" + brunoToken})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, r := range api.Routes(api.Deps{
		Auth:   auth,
		State:  fakeState{list: []state.Greenhouse{{GreenhouseID: "gh-001", Sensors: map[string]state.Sensor{"TEMPERATURE": {Mean: 20, Unit: "celsius"}}}}},
		Series: f.series, Alerts: f.alerts, DLQ: f.dlq, Health: health,
		Thresholds: telemetry.Thresholds{telemetry.SensorTemperature: {Min: 12, Max: 32}},
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:        func() time.Time { return now },
	}) {
		mux.Handle(r.Pattern, r.Handler)
	}
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// do envia com o token da Ana; doWith aceita outro cabeçalho Authorization.
func (f fixture) do(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()
	return f.doWith(t, method, path, body, "Bearer "+anaToken)
}

func (f fixture) doWith(t *testing.T, method, path, body, authorization string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, f.srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestGreenhousesAndThresholds(t *testing.T) {
	f := newFixture(t, fakeHealth{})
	code, body := f.do(t, "GET", "/greenhouses", "")
	ghs := body["greenhouses"].([]any)
	if code != 200 || len(ghs) != 1 || ghs[0].(map[string]any)["greenhouse_id"] != "gh-001" {
		t.Fatalf("code=%d body=%v", code, body)
	}
	code, body = f.do(t, "GET", "/thresholds", "")
	temp := body["thresholds"].(map[string]any)["TEMPERATURE"].(map[string]any)
	if code != 200 || temp["min"] != 12.0 || temp["max"] != 32.0 || temp["unit"] != "celsius" {
		t.Fatalf("thresholds = %v", body)
	}
}

func TestSeriesDefaultsAndValidation(t *testing.T) {
	f := newFixture(t, fakeHealth{})
	code, body := f.do(t, "GET", "/greenhouses/gh-001/series", "")
	pts := body["series"].(map[string]any)["TEMPERATURE"].(map[string]any)["points"].([]any)
	if code != 200 || len(pts) != 1 || body["greenhouse_id"] != "gh-001" {
		t.Fatalf("code=%d body=%v", code, body)
	}
	if !f.series.to.Equal(now) || f.series.to.Sub(f.series.from) != time.Hour {
		t.Fatalf("padrão deveria ser a última hora: %v..%v", f.series.from, f.series.to)
	}
	for _, q := range []string{
		"?from=ontem", "?to=amanha", "?from=2026-03-10T12:00:00Z&to=2026-03-10T11:00:00Z",
		"?from=2026-01-01T00:00:00Z&to=2026-03-10T00:00:00Z",
	} {
		if code, _ := f.do(t, "GET", "/greenhouses/gh-001/series"+q, ""); code != 400 {
			t.Errorf("%s: code=%d, esperado 400", q, code)
		}
	}
	if code, _ := f.do(t, "GET", "/greenhouses/gh%20x/series", ""); code != 400 {
		t.Errorf("id inválido deveria dar 400, veio %d", code)
	}
}

func TestAlertsListFilterAndAckContract(t *testing.T) {
	f := newFixture(t, fakeHealth{})
	code, body := f.do(t, "GET", "/alerts?status=open&limit=9999", "")
	a := body["alerts"].([]any)[0].(map[string]any)
	if code != 200 || f.alerts.status != alerting.StatusOpen || f.alerts.limit != 500 {
		t.Fatalf("status=%q limit=%d code=%d", f.alerts.status, f.alerts.limit, code)
	}
	if v, ok := a["acknowledged_at"]; !ok || v != nil {
		t.Errorf("acknowledged_at deve existir e ser null, veio %v (presente=%v)", v, ok)
	}
	if v, ok := a["note"]; !ok || v != nil {
		t.Errorf("note deve existir e ser null, veio %v", v)
	}
	for _, q := range []string{"?status=closed", "?limit=0", "?limit=abc"} {
		if code, _ := f.do(t, "GET", "/alerts"+q, ""); code != 400 {
			t.Errorf("%s: code=%d, esperado 400", q, code)
		}
	}

	code, body = f.do(t, "POST", "/alerts/a1/ack", `{"note":"ventilação ligada"}`)
	if code != 200 || body["note"] != "ventilação ligada" || body["acknowledged_at"] != "2026-03-10T12:00:00Z" {
		t.Fatalf("ack: code=%d body=%v", code, body)
	}
	if body["acknowledged_by"] != "ana" || f.alerts.ackedBy != "ana" {
		t.Fatalf("o reconhecimento deve gravar o operador do token: body=%v gravado=%q", body, f.alerts.ackedBy)
	}
	if code, _ = f.do(t, "POST", "/alerts/a1/ack", `{"note":`); code != 400 {
		t.Errorf("JSON inválido: %d", code)
	}
	if code, _ = f.do(t, "POST", "/alerts/a1/ack", `{"note":"`+strings.Repeat("x", 501)+`"}`); code != 400 {
		t.Errorf("nota longa: %d", code)
	}
	f.alerts.ackErr = alerting.ErrNotFound
	if code, _ = f.do(t, "POST", "/alerts/zzz/ack", `{}`); code != 404 {
		t.Errorf("inexistente: %d", code)
	}
	f.alerts.ackErr = alerting.ErrAlreadyAcknowledged
	if code, _ = f.do(t, "POST", "/alerts/a1/ack", `{}`); code != 409 {
		t.Errorf("já reconhecido: %d", code)
	}
	f.alerts.ackErr = errors.New("db fora")
	if code, _ = f.do(t, "POST", "/alerts/a1/ack", `{}`); code != 503 {
		t.Errorf("falha interna: %d", code)
	}
}

func TestDLQAndHealth(t *testing.T) {
	f := newFixture(t, fakeHealth{})
	code, body := f.do(t, "GET", "/dlq?limit=5", "")
	m := body["messages"].([]any)[0].(map[string]any)
	if code != 200 || f.dlq.limit != 5 || m["reason"] != "INVALID_READING" || m["source_partition"] != 2.0 || m["source_offset"] != 99.0 {
		t.Fatalf("dlq: code=%d f.limit=%d body=%v", code, f.dlq.limit, body)
	}
	code, body = f.do(t, "GET", "/pipeline/health", "")
	if code != 200 || body["total_lag"] != 12.0 {
		t.Fatalf("health: %d %v", code, body)
	}

	down := newFixture(t, fakeHealth{err: errors.New("broker fora")})
	if code, _ := down.do(t, "GET", "/pipeline/health", ""); code != 503 {
		t.Errorf("broker fora deveria dar 503, veio %d", code)
	}
}

func TestNoRoutesWithoutOperatorAuth(t *testing.T) {
	routes := api.Routes(api.Deps{Thresholds: telemetry.DefaultThresholds(), Logger: slog.Default()})
	if len(routes) != 0 {
		t.Fatalf("sem Auth a API não pode abrir nenhuma rota: %+v", routes)
	}
}

func TestRoutesAreOptionalPerPort(t *testing.T) {
	auth, err := api.NewOperatorAuth([]string{"ana:" + anaToken})
	if err != nil {
		t.Fatal(err)
	}
	routes := api.Routes(api.Deps{Auth: auth, Thresholds: telemetry.DefaultThresholds(), Logger: slog.Default()})
	if len(routes) != 1 || routes[0].Pattern != "GET /thresholds" {
		t.Fatalf("sem portas só /thresholds deveria existir: %+v", routes)
	}
}

func TestRequestsWithoutValidTokenGet401(t *testing.T) {
	f := newFixture(t, fakeHealth{})
	paths := []struct{ method, path string }{
		{"GET", "/thresholds"}, {"GET", "/greenhouses"}, {"GET", "/greenhouses/gh-001/series"},
		{"GET", "/alerts?status=open"}, {"POST", "/alerts/a1/ack"}, {"GET", "/dlq"}, {"GET", "/pipeline/health"},
	}
	for _, p := range paths {
		for _, header := range []string{"", "Basic " + anaToken, "Bearer ", "Bearer dev-only-token-x"} {
			if code, _ := f.doWith(t, p.method, p.path, "{}", header); code != 401 {
				t.Errorf("%s %s com cabeçalho %q: código %d, esperado 401", p.method, p.path, header, code)
			}
		}
	}
	if f.alerts.ackedBy != "" {
		t.Fatalf("requisição sem token não pode reconhecer alerta, gravou %q", f.alerts.ackedBy)
	}
}

func TestOperatorIsTheTokenOwner(t *testing.T) {
	f := newFixture(t, fakeHealth{})
	code, body := f.doWith(t, "POST", "/alerts/a1/ack", `{"note":"ok","acknowledged_by":"ana"}`, "Bearer "+brunoToken)
	if code != 200 || body["acknowledged_by"] != "bruno" || f.alerts.ackedBy != "bruno" {
		t.Fatalf("o operador vem do token, não do corpo: code=%d body=%v gravado=%q", code, body, f.alerts.ackedBy)
	}
}

func TestOperatorAuthConfigValidation(t *testing.T) {
	cases := map[string][]string{
		"sem dois pontos":   {"ana-dev-only-token"},
		"token curto":       {"ana:curto"},
		"operador repetido": {"ana:dev-only-token-1", "ANA:dev-only-token-2"},
		"lista vazia":       {},
	}
	for name, entries := range cases {
		if _, err := api.NewOperatorAuth(entries); err == nil {
			t.Errorf("%s: deveria recusar %v", name, entries)
		}
	}
}
