// Package api é o adaptador HTTP de consulta: expõe, em JSON snake_case, o
// último estado (Redis), as séries e os alertas (Postgres), a DLQ e a saúde
// do pipeline (Kafka) e as faixas agronômicas. Só depende de portas.
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/alerting"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/history"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/observability"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/pipeline"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/state"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

// StateReader lê o último estado de uma estufa e lista todas.
type StateReader interface {
	state.Reader
	state.Lister
}

// Deps são as portas usadas pelas rotas. Uma porta nula desliga as rotas que
// dependem dela, para o agregador rodar sem Redis, Postgres ou inspeção Kafka.
type Deps struct {
	State      StateReader
	Series     history.Repository
	Alerts     alerting.Repository
	DLQ        pipeline.DLQReader
	Health     pipeline.HealthReporter
	Thresholds telemetry.Thresholds
	Logger     *slog.Logger
	Now        func() time.Time
	// Auth exige token de operador em todas as rotas. Sem ele a API não é
	// registrada (nem /thresholds): não existe modo aberto.
	Auth *OperatorAuth
}

// Routes monta as rotas da API, todas protegidas por d.Auth. Sem Auth, nenhuma
// rota é registrada; /metrics e /healthz do agregador continuam abertos.
func Routes(d Deps) []observability.Route {
	if d.Auth == nil {
		return nil
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	h := &handlers{Deps: d}
	routes := []observability.Route{}
	add := func(pattern string, fn http.HandlerFunc) {
		routes = append(routes, observability.Route{Pattern: pattern, Handler: d.Auth.Middleware(fn)})
	}
	add("GET /thresholds", h.thresholds)
	if d.State != nil {
		add("GET /greenhouses", h.greenhouses)
		routes = append(routes, observability.Route{Pattern: "GET /greenhouses/{id}/state",
			Handler: d.Auth.Middleware(state.Handler(d.State, d.Logger))})
	}
	if d.Series != nil {
		add("GET /greenhouses/{id}/series", h.series)
	}
	if d.Alerts != nil {
		add("GET /alerts", h.listAlerts)
		add("POST /alerts/{id}/ack", h.ackAlert)
	}
	if d.DLQ != nil {
		add("GET /dlq", h.dlq)
	}
	if d.Health != nil {
		add("GET /pipeline/health", h.pipelineHealth)
	}
	return routes
}

type handlers struct{ Deps }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// unavailable registra a causa e devolve 503 sem vazar detalhes internos.
func (h *handlers) unavailable(w http.ResponseWriter, what string, err error) {
	h.Logger.Error("consulta falhou", "what", what, "error", err)
	writeError(w, http.StatusServiceUnavailable, what+" indisponível")
}
