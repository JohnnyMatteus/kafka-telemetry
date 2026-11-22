// Package dlq reprocessa mensagens da DLQ de leituras: relê o que o agregador
// recusou e republica no tópico de leituras depois que a causa foi corrigida
// (um schema que passou a existir, por exemplo).
//
// Guardrails, todos aplicados em Reprocessor.Run:
//   - o destino é fixo na configuração; o pedido nunca escolhe o tópico;
//   - dry-run é o padrão: gravar exige DryRun=false explícito;
//   - o lote tem teto; o pedido acima dele é recusado;
//   - cada mensagem da DLQ é republicada no máximo uma vez (ledger durável) e
//     só uma execução roda por vez (lock);
//   - toda mensagem republicada e todo registro do ledger levam o operador.
package dlq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/config"
	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

// Headers acrescentados à mensagem republicada, para rastrear quem a reenviou.
const (
	HeaderReplayedBy = "dlq.replayed.by"
	HeaderReplayRun  = "dlq.replay.run"
	HeaderReplayFrom = "dlq.replay.source.offset"
)

// Ações e motivos de pulo que aparecem no resultado.
const (
	ActionWouldReplay = "would_replay"
	ActionReplayed    = "replayed"
	foreignSource     = "foreign_source"
	alreadyReplayed   = "already_replayed"
	filteredOut       = "filtered"
	emptyValue        = "empty_value"
	scanLimitReached  = "scan_limit"
)

const (
	defaultBatch     = 10
	absoluteMaxBatch = 1000
	scanChunk        = 500
	maxScan          = 10_000
	lockTTL          = 2 * time.Minute
)

var (
	// ErrBatchLimit indica um limite fora de 1..MaxBatch.
	ErrBatchLimit = errors.New("limite de lote inválido")
	// ErrUnknownReason indica um filtro de motivo que o agregador não emite.
	ErrUnknownReason = errors.New("motivo desconhecido")
	// ErrBusy indica que outra execução está em andamento.
	ErrBusy = errors.New("outro reprocessamento em andamento")
	// ErrOperatorRequired impede gravar sem autoria.
	ErrOperatorRequired = errors.New("operador obrigatório para reprocessar")
)

// Entry é uma mensagem da DLQ.
type Entry struct {
	Offset  int64
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// Source lê a DLQ em ordem a partir de um offset. end é o próximo offset a
// ser escrito na DLQ.
type Source interface {
	Read(ctx context.Context, from int64, limit int) (entries []Entry, end int64, err error)
}

// Receipt registra quem republicou uma mensagem da DLQ e quando.
type Receipt struct {
	Operator string    `json:"operator"`
	RunID    string    `json:"run_id"`
	At       time.Time `json:"at"`
}

// Ledger guarda, de forma durável, o que já foi republicado.
type Ledger interface {
	// Lock reserva a execução; devolve ErrBusy se outra já a detém.
	Lock(ctx context.Context, runID string, ttl time.Duration) (release func(), err error)
	Done(ctx context.Context, offsets []int64) (map[int64]Receipt, error)
	Record(ctx context.Context, receipts map[int64]Receipt) error
	Cursor(ctx context.Context) (int64, error)
	SetCursor(ctx context.Context, offset int64) error
}

// Config é a configuração fixa do reprocessador.
type Config struct {
	// Destination é o tópico para onde tudo é republicado.
	Destination string
	// DLQTopic é a DLQ de origem; o destino nunca pode ser ela.
	DLQTopic string
	// MaxBatch é o teto de mensagens por execução.
	MaxBatch int
	// Reasons são os motivos aceitos no filtro; vazio usa os do agregador.
	Reasons []string
}

// Deps reúne as portas.
type Deps struct {
	Source    Source
	Publisher aggregation.Publisher
	Ledger    Ledger
	Logger    *slog.Logger
	Clock     func() time.Time
}

// Request é o que o chamador pode escolher. Destino não está aqui de propósito.
type Request struct {
	Operator string
	// Limit vale 0 para o padrão (10).
	Limit  int
	Reason string
	// Execute grava; falso é dry-run.
	Execute bool
}

// Item é o destino de uma mensagem examinada.
type Item struct {
	Offset int64  `json:"offset"`
	Reason string `json:"reason"`
	Action string `json:"action"`
}

// Result resume uma execução.
type Result struct {
	RunID    string         `json:"run_id"`
	DryRun   bool           `json:"dry_run"`
	Operator string         `json:"operator"`
	Target   string         `json:"destination_topic"`
	Scanned  int            `json:"scanned"`
	Selected []Item         `json:"selected"`
	Replayed int            `json:"replayed"`
	Skipped  map[string]int `json:"skipped"`
}

// Reprocessor é o caso de uso.
type Reprocessor struct {
	cfg  Config
	deps Deps
}

// New valida a configuração.
func New(cfg Config, deps Deps) (*Reprocessor, error) {
	switch {
	case cfg.Destination == "" || cfg.DLQTopic == "":
		return nil, errors.New("dlq: destino e DLQ obrigatórios")
	case cfg.Destination == cfg.DLQTopic:
		return nil, errors.New("dlq: o destino não pode ser a própria DLQ")
	case cfg.MaxBatch < 1:
		return nil, errors.New("dlq: teto de lote deve ser positivo")
	case deps.Source == nil || deps.Publisher == nil || deps.Ledger == nil:
		return nil, errors.New("dlq: dependência obrigatória ausente")
	}
	if len(cfg.Reasons) == 0 {
		cfg.Reasons = []string{aggregation.ReasonMalformed, aggregation.ReasonUnknownSchema, aggregation.ReasonInvalid}
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	if deps.Clock == nil {
		deps.Clock = time.Now
	}
	return &Reprocessor{cfg: cfg, deps: deps}, nil
}

// MaxBatch devolve o teto configurado.
func (r *Reprocessor) MaxBatch() int { return r.cfg.MaxBatch }

// Run examina a DLQ e, só com Execute, republica as elegíveis.
func (r *Reprocessor) Run(ctx context.Context, req Request) (Result, error) {
	limit := req.Limit
	if limit == 0 {
		limit = min(defaultBatch, r.cfg.MaxBatch)
	}
	if limit < 1 || limit > r.cfg.MaxBatch {
		return Result{}, fmt.Errorf("%w: use de 1 a %d", ErrBatchLimit, r.cfg.MaxBatch)
	}
	if req.Reason != "" && !r.knownReason(req.Reason) {
		return Result{}, fmt.Errorf("%w: %q", ErrUnknownReason, req.Reason)
	}
	if req.Execute && req.Operator == "" {
		return Result{}, ErrOperatorRequired
	}

	res := Result{RunID: newRunID(), DryRun: !req.Execute, Operator: req.Operator,
		Target: r.cfg.Destination, Selected: []Item{}, Skipped: map[string]int{}}
	if req.Execute {
		release, err := r.deps.Ledger.Lock(ctx, res.RunID, lockTTL)
		if err != nil {
			return res, err
		}
		defer release()
	}

	cursor, err := r.deps.Ledger.Cursor(ctx)
	if err != nil {
		return res, fmt.Errorf("cursor do reprocessamento: %w", err)
	}
	picked, scanned, err := r.pick(ctx, cursor, limit, req, &res)
	if err != nil {
		return res, err
	}
	res.Scanned = len(scanned)
	for _, e := range picked {
		res.Selected = append(res.Selected, Item{Offset: e.Offset, Reason: e.Headers[aggregation.HeaderReason], Action: ActionWouldReplay})
	}
	if !req.Execute || len(picked) == 0 {
		r.deps.Logger.Info("dlq: dry-run", "run", res.RunID, "operator", req.Operator, "selected", len(picked))
		return res, nil
	}

	msgs := make([]aggregation.Message, len(picked))
	receipts := make(map[int64]Receipt, len(picked))
	now := r.deps.Clock().UTC()
	for i, e := range picked {
		msgs[i] = aggregation.Message{Topic: r.cfg.Destination, Key: e.Key, Value: e.Value, Headers: map[string]string{
			HeaderReplayedBy: req.Operator, HeaderReplayRun: res.RunID, HeaderReplayFrom: strconv.FormatInt(e.Offset, 10),
		}}
		receipts[e.Offset] = Receipt{Operator: req.Operator, RunID: res.RunID, At: now}
	}
	if err := r.deps.Publisher.Publish(ctx, msgs); err != nil {
		return res, fmt.Errorf("republicar %d mensagens: %w", len(msgs), err)
	}
	// Se gravar o ledger falhar depois do publish, a execução seguinte
	// republica o mesmo lote: o agregador deduplica por event_id enquanto o
	// evento está dentro do watermark. É o preço de não perder mensagem.
	if err := r.deps.Ledger.Record(ctx, receipts); err != nil {
		return res, fmt.Errorf("registrar republicação: %w", err)
	}
	for i := range res.Selected {
		res.Selected[i].Action = ActionReplayed
	}
	res.Replayed = len(picked)
	r.advanceCursor(ctx, cursor, scanned, receipts)
	r.deps.Logger.Info("dlq: reprocessamento", "run", res.RunID, "operator", req.Operator,
		"replayed", res.Replayed, "destination", r.cfg.Destination)
	return res, nil
}

// pick percorre a DLQ a partir do cursor e escolhe até limit mensagens.
func (r *Reprocessor) pick(ctx context.Context, cursor int64, limit int, req Request, res *Result) (picked, scanned []Entry, err error) {
	from := cursor
	for len(picked) < limit && len(scanned) < maxScan {
		entries, end, err := r.deps.Source.Read(ctx, from, scanChunk)
		if err != nil {
			return nil, nil, fmt.Errorf("ler a DLQ: %w", err)
		}
		if len(entries) == 0 {
			break
		}
		offsets := make([]int64, len(entries))
		for i, e := range entries {
			offsets[i] = e.Offset
		}
		done, err := r.deps.Ledger.Done(ctx, offsets)
		if err != nil {
			return nil, nil, fmt.Errorf("consultar o ledger: %w", err)
		}
		for _, e := range entries {
			from = e.Offset + 1
			if len(picked) >= limit {
				break
			}
			scanned = append(scanned, e)
			switch {
			case has(done, e.Offset):
				res.Skipped[alreadyReplayed]++
			case e.Headers[aggregation.HeaderSourceTopic] != r.cfg.Destination:
				res.Skipped[foreignSource]++
			case req.Reason != "" && e.Headers[aggregation.HeaderReason] != req.Reason:
				res.Skipped[filteredOut]++
			case len(e.Value) == 0:
				res.Skipped[emptyValue]++
			default:
				picked = append(picked, e)
			}
		}
		if from >= end {
			break
		}
	}
	if len(scanned) >= maxScan && len(picked) < limit {
		res.Skipped[scanLimitReached]++
	}
	return picked, scanned, nil
}

// advanceCursor pula o prefixo já resolvido para que a próxima execução não
// releia a DLQ inteira. Uma falha aqui só custa leitura extra.
func (r *Reprocessor) advanceCursor(ctx context.Context, cursor int64, scanned []Entry, fresh map[int64]Receipt) {
	offsets := make([]int64, len(scanned))
	for i, e := range scanned {
		offsets[i] = e.Offset
	}
	done, err := r.deps.Ledger.Done(ctx, offsets)
	if err != nil {
		r.deps.Logger.Warn("dlq: não foi possível avançar o cursor", "error", err)
		return
	}
	next := cursor
	for _, e := range scanned {
		if !has(done, e.Offset) && !has(fresh, e.Offset) {
			break
		}
		next = e.Offset + 1
	}
	if next > cursor {
		if err := r.deps.Ledger.SetCursor(ctx, next); err != nil {
			r.deps.Logger.Warn("dlq: não foi possível avançar o cursor", "error", err)
		}
	}
}

func (r *Reprocessor) knownReason(s string) bool {
	for _, k := range r.cfg.Reasons {
		if k == s {
			return true
		}
	}
	return false
}

func has[V any](m map[int64]V, k int64) bool { _, ok := m[k]; return ok }

func newRunID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ConfigFromEnv lê DLQ_REPLAY_TOPIC (padrão: o tópico de leituras) e
// DLQ_REPLAY_MAX_BATCH (padrão 100, teto absoluto 1000). É o único lugar onde
// o destino é definido, para o comando e a API não divergirem.
func ConfigFromEnv(env *config.Env) Config {
	return Config{
		Destination: env.String("DLQ_REPLAY_TOPIC", schemas.TopicReadings),
		DLQTopic:    schemas.TopicReadingsDLQ,
		MaxBatch:    min(env.Int("DLQ_REPLAY_MAX_BATCH", 100), absoluteMaxBatch),
	}
}
