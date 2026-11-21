package state

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"time"
)

var validID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// Handler serve GET /greenhouses/{id}/state.
func Handler(r Reader, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		id := req.PathValue("id")
		if !validID.MatchString(id) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "greenhouse_id inválido"})
			return
		}
		start := time.Now()
		gh, err := r.Get(req.Context(), id)
		switch {
		case errors.Is(err, ErrNotFound):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "estufa sem estado", "greenhouse_id": id})
		case err != nil:
			logger.Error("leitura do estado falhou", "greenhouse_id", id, "error", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "estado indisponível"})
		default:
			w.Header().Set("X-State-Source", "redis")
			w.Header().Set("X-State-Read-Duration", time.Since(start).String())
			writeJSON(w, http.StatusOK, gh)
		}
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
