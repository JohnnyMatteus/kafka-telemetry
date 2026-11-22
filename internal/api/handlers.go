package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/alerting"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/dlq"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/history"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/state"
)

const (
	defaultLimit  = 50
	maxLimit      = 500
	maxNoteRunes  = 500
	maxSeriesSpan = 31 * 24 * time.Hour
	defaultSpan   = time.Hour
)

func (h *handlers) greenhouses(w http.ResponseWriter, r *http.Request) {
	list, err := h.State.List(r.Context())
	if err != nil {
		h.unavailable(w, "estado", err)
		return
	}
	if list == nil {
		list = []state.Greenhouse{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"greenhouses": list})
}

type pointJSON struct {
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	SampleCount int       `json:"sample_count"`
	Mean        float64   `json:"mean"`
	Min         float64   `json:"min"`
	Max         float64   `json:"max"`
}

type seriesJSON struct {
	Unit   string      `json:"unit"`
	Points []pointJSON `json:"points"`
}

func (h *handlers) series(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !state.ValidID(id) {
		writeError(w, http.StatusBadRequest, "greenhouse_id inválido")
		return
	}
	to, from := h.Now().UTC(), time.Time{}
	var err error
	if v := r.URL.Query().Get("to"); v != "" {
		if to, err = time.Parse(time.RFC3339, v); err != nil {
			writeError(w, http.StatusBadRequest, "to deve ser RFC3339")
			return
		}
	}
	if v := r.URL.Query().Get("from"); v != "" {
		if from, err = time.Parse(time.RFC3339, v); err != nil {
			writeError(w, http.StatusBadRequest, "from deve ser RFC3339")
			return
		}
	} else {
		from = to.Add(-defaultSpan)
	}
	switch {
	case !from.Before(to):
		writeError(w, http.StatusBadRequest, "from deve ser anterior a to")
		return
	case to.Sub(from) > maxSeriesSpan:
		writeError(w, http.StatusBadRequest, "intervalo máximo de 31 dias")
		return
	}
	got, err := h.Series.Series(r.Context(), id, from, to)
	if err != nil {
		h.unavailable(w, "histórico", err)
		return
	}
	out := make(map[string]seriesJSON, len(got))
	for typ, s := range got {
		out[typ] = toSeriesJSON(s)
	}
	writeJSON(w, http.StatusOK, map[string]any{"greenhouse_id": id, "series": out})
}

func toSeriesJSON(s history.Series) seriesJSON {
	pts := make([]pointJSON, len(s.Points))
	for i, p := range s.Points {
		pts[i] = pointJSON{p.WindowStart, p.WindowEnd, p.SampleCount, p.Mean, p.Min, p.Max}
	}
	return seriesJSON{Unit: s.Unit, Points: pts}
}

type alertJSON struct {
	AlertID        string     `json:"alert_id"`
	GreenhouseID   string     `json:"greenhouse_id"`
	SensorType     string     `json:"sensor_type"`
	WindowStart    time.Time  `json:"window_start"`
	WindowEnd      time.Time  `json:"window_end"`
	Rule           string     `json:"rule"`
	ObservedValue  float64    `json:"observed_value"`
	LimitValue     float64    `json:"limit_value"`
	AcknowledgedAt *time.Time `json:"acknowledged_at"`
	AcknowledgedBy *string    `json:"acknowledged_by"`
	Note           *string    `json:"note"`
}

func toAlertJSON(a alerting.Alert) alertJSON {
	return alertJSON{a.ID, a.GreenhouseID, a.SensorType, a.WindowStart, a.WindowEnd, a.Rule,
		a.ObservedValue, a.LimitValue, a.AcknowledgedAt, a.AcknowledgedBy, a.Note}
}

// parseLimit lê ?limit= com padrão e teto; ok=false já respondeu 400.
func parseLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return defaultLimit, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		writeError(w, http.StatusBadRequest, "limit deve ser um inteiro positivo")
		return 0, false
	}
	return min(n, maxLimit), true
}

func (h *handlers) listAlerts(w http.ResponseWriter, r *http.Request) {
	status := alerting.Status(r.URL.Query().Get("status"))
	if !status.Valid() {
		writeError(w, http.StatusBadRequest, "status deve ser open ou acknowledged")
		return
	}
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	list, err := h.Alerts.List(r.Context(), status, limit)
	if err != nil {
		h.unavailable(w, "alertas", err)
		return
	}
	out := make([]alertJSON, len(list))
	for i, a := range list {
		out[i] = toAlertJSON(a)
	}
	writeJSON(w, http.StatusOK, map[string]any{"alerts": out})
}

func (h *handlers) ackAlert(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Note string `json:"note"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4<<10)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "corpo JSON inválido")
		return
	}
	if utf8.RuneCountInString(body.Note) > maxNoteRunes {
		writeError(w, http.StatusBadRequest, "note excede 500 caracteres")
		return
	}
	a, err := h.Alerts.Acknowledge(r.Context(), r.PathValue("id"), Operator(r.Context()), body.Note, h.Now().UTC())
	switch {
	case errors.Is(err, alerting.ErrNotFound):
		writeError(w, http.StatusNotFound, "alerta inexistente")
	case errors.Is(err, alerting.ErrAlreadyAcknowledged):
		writeError(w, http.StatusConflict, "alerta já reconhecido")
	case err != nil:
		h.unavailable(w, "alertas", err)
	default:
		writeJSON(w, http.StatusOK, toAlertJSON(a))
	}
}

type deadLetterJSON struct {
	Partition       int32     `json:"partition"`
	Offset          int64     `json:"offset"`
	Timestamp       time.Time `json:"timestamp"`
	Key             string    `json:"key"`
	Reason          string    `json:"reason"`
	Error           string    `json:"error"`
	SourceTopic     string    `json:"source_topic"`
	SourcePartition int32     `json:"source_partition"`
	SourceOffset    int64     `json:"source_offset"`
}

func (h *handlers) dlq(w http.ResponseWriter, r *http.Request) {
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	msgs, err := h.DLQ.Recent(r.Context(), limit)
	if err != nil {
		h.unavailable(w, "DLQ", err)
		return
	}
	out := make([]deadLetterJSON, len(msgs))
	for i, m := range msgs {
		out[i] = deadLetterJSON(m)
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": out})
}

// replayDLQ reprocessa a DLQ. O corpo escolhe só limite, motivo e dry_run;
// o tópico de destino é da configuração. dry_run ausente vale true.
func (h *handlers) replayDLQ(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Limit    int    `json:"limit"`
		Reason   string `json:"reason"`
		DryRun   *bool  `json:"dry_run"`
		Backfill bool   `json:"backfill"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields() // um "destination" no corpo é erro, não é ignorado
	if err := dec.Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "corpo JSON inválido")
		return
	}
	res, err := h.Replay.Run(r.Context(), dlq.Request{
		Operator: Operator(r.Context()), Limit: body.Limit, Reason: body.Reason,
		Execute: body.DryRun != nil && !*body.DryRun, Backfill: body.Backfill,
	})
	switch {
	case errors.Is(err, dlq.ErrBatchLimit), errors.Is(err, dlq.ErrUnknownReason):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, dlq.ErrBackfillUnavailable):
		writeError(w, http.StatusNotImplemented, err.Error())
	case errors.Is(err, dlq.ErrBusy):
		writeError(w, http.StatusConflict, "outro reprocessamento em andamento")
	case err != nil:
		h.unavailable(w, "reprocessamento da DLQ", err)
	default:
		writeJSON(w, http.StatusOK, res)
	}
}

type thresholdJSON struct {
	Min  float64 `json:"min"`
	Max  float64 `json:"max"`
	Unit string  `json:"unit"`
}

func (h *handlers) thresholds(w http.ResponseWriter, _ *http.Request) {
	out := make(map[string]thresholdJSON, len(h.Thresholds))
	for typ, l := range h.Thresholds {
		out[string(typ)] = thresholdJSON{Min: l.Min, Max: l.Max, Unit: typ.Unit()}
	}
	writeJSON(w, http.StatusOK, map[string]any{"thresholds": out})
}

func (h *handlers) pipelineHealth(w http.ResponseWriter, r *http.Request) {
	rep, err := h.Health.Report(r.Context())
	if err != nil {
		h.unavailable(w, "saúde do pipeline", err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}
