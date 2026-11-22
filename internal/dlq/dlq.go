// Package dlq reprocessa mensagens da DLQ de leituras: relê o que o agregador
// recusou e republica no tópico de leituras depois que a causa foi corrigida
// (um schema que passou a existir, por exemplo).
//
// Guardrails, todos aplicados em Reprocessor.Run:
//   - o destino é fixo na configuração; o pedido nunca escolhe o tópico;
//   - dry-run é o padrão: gravar exige DryRun=false explícito;
//   - o lote tem teto; o pedido acima dele é recusado;
//   - cada mensagem da DLQ é republicada no máximo uma vez (ledger durável) e
//     só uma execução roda por vez (lock), com a execução inteira limitada a
//     um prazo menor que o TTL do lock;
//   - uma leitura que o agregador descartaria como atrasada (a janela dela
//     já fechou pelo watermark do último checkpoint) não é republicada nem
//     marcada: o resultado a classifica como would_be_late. Não existe hoje
//     um caminho de backfill que recalcule janelas fechadas (ADR 0012);
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
	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
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
	ActionWouldBeLate = "would_be_late"
	ActionUndecodable = "undecodable"
	// ActionInvalid marca a leitura que o domínio recusa (valor impossível,
	// relógio adiantado, campo vazio): voltaria à DLQ, e somá-la ao histórico
	// ou disparar alerta com ela seria corromper dado.
	ActionInvalid    = "invalid_reading"
	foreignSource    = "foreign_source"
	alreadyReplayed  = "already_replayed"
	filteredOut      = "filtered"
	emptyValue       = "empty_value"
	scanLimitReached = "scan_limit"
)

const (
	defaultBatch     = 10
	absoluteMaxBatch = 1000
	scanChunk        = 500
	maxScan          = 10_000
	lockTTL          = 2 * time.Minute
	// writeTimeout cobre o ledger depois do publish, que não pode ser
	// abortado pelo prazo da execução (senão a mensagem sairia sem recibo).
	writeTimeout = 5 * time.Second
)

var (
	// ErrBatchLimit indica um limite fora de 1..MaxBatch.
	ErrBatchLimit = errors.New("limite de lote inválido")
	// ErrUnknownReason indica um filtro de motivo que o agregador não emite.
	ErrUnknownReason = errors.New("motivo desconhecido")
	// ErrBusy indica que outra execução está em andamento.
	ErrBusy = errors.New("outro reprocessamento em andamento")
	// ErrBackfillUnavailable indica que o backfill não foi ligado (exige Postgres).
	ErrBackfillUnavailable = errors.New("backfill indisponível: exige POSTGRES_DSN")
	// ErrOperatorRequired impede gravar sem autoria.
	ErrOperatorRequired = errors.New("operador obrigatório para reprocessar")
)

// Entry é uma mensagem da DLQ.
type Entry struct {
	Offset int64
	// Timestamp é o instante em que a mensagem entrou na DLQ, logo o da
	// recepção original: a validação da leitura usa este, não o de agora.
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   map[string]string
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

// Checkpoints lê os checkpoints do agregador; aggregation.CheckpointStore a satisfaz.
type Checkpoints interface {
	Load(ctx context.Context) (map[aggregation.TopicPartition]aggregation.Checkpoint, error)
}

// Config é a configuração fixa do reprocessador.
type Config struct {
	// Destination é o tópico para onde tudo é republicado.
	Destination string
	// DLQTopic é a DLQ de origem; o destino nunca pode ser ela.
	DLQTopic string
	// MaxBatch é o teto de mensagens por execução.
	MaxBatch int
	// WindowSize é o tamanho da janela do agregador (WINDOW_SIZE); decide se
	// a janela de uma leitura já fechou.
	WindowSize time.Duration
	// RunBudget limita a execução inteira; vazio usa metade do TTL do lock.
	RunBudget time.Duration
	// Reasons são os motivos aceitos no filtro; vazio usa os do agregador.
	Reasons []string
	// AggregatesTopic e AlertsTopic são os destinos fixos do backfill; vazios
	// usam os tópicos do pipeline.
	AggregatesTopic string
	AlertsTopic     string
	// BackfillMinAge é a idade mínima do fim da janela para o backfill, para o
	// agregado original já ter chegado ao Postgres. Vazio vale 5 min.
	BackfillMinAge time.Duration
}

// Deps reúne as portas.
type Deps struct {
	Source    Source
	Publisher aggregation.Publisher
	Ledger    Ledger
	// Decoder lê o measured_at da leitura; Checkpoints dá o watermark de cada
	// partição de origem (os mesmos que o agregador grava).
	Decoder     aggregation.Decoder
	Checkpoints Checkpoints
	// Backfill liga o modo backfill; sem ele, Request.Backfill é recusado.
	Backfill *BackfillDeps
	Logger   *slog.Logger
	Clock    func() time.Time
}

// Request é o que o chamador pode escolher. Destino não está aqui de propósito.
type Request struct {
	Operator string
	// Limit vale 0 para o padrão (10).
	Limit  int
	Reason string
	// Execute grava; falso é dry-run.
	Execute bool
	// Backfill recupera as leituras de janela já fechada, recalculando e
	// republicando o agregado da janela (em vez de republicar a leitura).
	Backfill bool
}

// Item é o destino de uma mensagem examinada.
type Item struct {
	Offset int64  `json:"offset"`
	Reason string `json:"reason"`
	Action string `json:"action"`
}

// Result resume uma execução.
type Result struct {
	RunID    string `json:"run_id"`
	DryRun   bool   `json:"dry_run"`
	Operator string `json:"operator"`
	Target   string `json:"destination_topic"`
	Scanned  int    `json:"scanned"`
	Selected []Item `json:"selected"`
	// Blocked são as mensagens que não seriam recuperadas: would_be_late
	// (o agregador as descartaria), undecodable e invalid_reading (continuam
	// recusadas) e, no backfill, base_unknown.
	Blocked  []Item         `json:"blocked"`
	Replayed int            `json:"replayed"`
	Skipped  map[string]int `json:"skipped"`
	// Backfill descreve, no modo backfill, as janelas recalculadas (ou que
	// seriam).
	Backfill *BackfillResult `json:"backfill,omitempty"`
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
	case cfg.WindowSize <= 0:
		return nil, errors.New("dlq: tamanho da janela deve ser positivo")
	case cfg.RunBudget >= lockTTL:
		return nil, errors.New("dlq: o prazo da execução deve ser menor que o TTL do lock")
	case deps.Source == nil || deps.Publisher == nil || deps.Ledger == nil || deps.Decoder == nil || deps.Checkpoints == nil:
		return nil, errors.New("dlq: dependência obrigatória ausente")
	}
	if cfg.RunBudget <= 0 {
		cfg.RunBudget = lockTTL / 2
	}
	if cfg.AggregatesTopic == "" {
		cfg.AggregatesTopic = schemas.TopicAggregates
	}
	if cfg.AlertsTopic == "" {
		cfg.AlertsTopic = schemas.TopicAlerts
	}
	if cfg.BackfillMinAge <= 0 {
		cfg.BackfillMinAge = 5 * time.Minute
	}
	if deps.Backfill != nil {
		if err := deps.Backfill.validate(); err != nil {
			return nil, err
		}
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
	if req.Backfill && r.deps.Backfill == nil {
		return Result{}, ErrBackfillUnavailable
	}

	res := Result{RunID: newRunID(), DryRun: !req.Execute, Operator: req.Operator,
		Target: r.cfg.Destination, Selected: []Item{}, Blocked: []Item{}, Skipped: map[string]int{}}
	// Tudo abaixo roda dentro do prazo, menor que o TTL do lock: o lock não é
	// renovado, então nenhuma etapa pode sobreviver a ele.
	ctx, cancel := context.WithTimeout(ctx, r.cfg.RunBudget)
	defer cancel()
	if req.Execute {
		release, err := r.deps.Ledger.Lock(ctx, res.RunID, lockTTL)
		if err != nil {
			return res, err
		}
		defer release()
	}

	if req.Backfill {
		return r.backfill(ctx, req, limit, res)
	}
	cursor, err := r.deps.Ledger.Cursor(ctx)
	if err != nil {
		return res, fmt.Errorf("cursor do reprocessamento: %w", err)
	}
	picked, scanned, settled, err := r.pick(ctx, cursor, limit, req, &res)
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
	if err := ctx.Err(); err != nil {
		return res, fmt.Errorf("prazo da execução esgotado antes de publicar: %w", err)
	}
	if err := r.deps.Publisher.Publish(ctx, msgs); err != nil {
		return res, fmt.Errorf("republicar %d mensagens: %w", len(msgs), err)
	}
	wctx, wcancel := context.WithTimeout(context.WithoutCancel(ctx), 2*writeTimeout)
	defer wcancel()
	// Se gravar o ledger falhar depois do publish, a execução seguinte
	// republica o mesmo lote: o agregador deduplica por event_id enquanto o
	// evento está dentro do watermark. É o preço de não perder mensagem.
	if err := r.deps.Ledger.Record(wctx, receipts); err != nil {
		return res, fmt.Errorf("registrar republicação: %w", err)
	}
	for i := range res.Selected {
		res.Selected[i].Action = ActionReplayed
	}
	res.Replayed = len(picked)
	r.advanceCursor(wctx, cursor, scanned, receipts, settled)
	r.deps.Logger.Info("dlq: reprocessamento", "run", res.RunID, "operator", req.Operator,
		"replayed", res.Replayed, "destination", r.cfg.Destination)
	return res, nil
}

// pick percorre a DLQ a partir do cursor e escolhe até limit mensagens.
func (r *Reprocessor) pick(ctx context.Context, cursor int64, limit int, req Request, res *Result) (picked, scanned []Entry, settled map[int64]bool, err error) {
	settled = map[int64]bool{}
	marks, err := r.deps.Checkpoints.Load(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("carregar checkpoints: %w", err)
	}
	from := cursor
	for len(picked) < limit && len(scanned) < maxScan {
		entries, end, err := r.deps.Source.Read(ctx, from, scanChunk)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("ler a DLQ: %w", err)
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
			return nil, nil, nil, fmt.Errorf("consultar o ledger: %w", err)
		}
		var doneB map[int64]Receipt
		if r.deps.Backfill != nil {
			if doneB, err = r.deps.Backfill.Ledger.Done(ctx, offsets); err != nil {
				return nil, nil, nil, fmt.Errorf("consultar o ledger do backfill: %w", err)
			}
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
			case has(doneB, e.Offset):
				res.Skipped[alreadyBackfilled]++
				settled[e.Offset] = true
			case e.Headers[aggregation.HeaderSourceTopic] != r.cfg.Destination:
				res.Skipped[foreignSource]++
			case req.Reason != "" && e.Headers[aggregation.HeaderReason] != req.Reason:
				res.Skipped[filteredOut]++
			case len(e.Value) == 0:
				res.Skipped[emptyValue]++
			default:
				verdict, err := r.classify(ctx, e, marks)
				if err != nil {
					return nil, nil, nil, err
				}
				if verdict == ActionWouldReplay {
					picked = append(picked, e)
					continue
				}
				if verdict == ActionWouldBeLate {
					settled[e.Offset] = true
				}
				res.Skipped[verdict]++
				res.Blocked = append(res.Blocked, Item{Offset: e.Offset, Reason: e.Headers[aggregation.HeaderReason], Action: verdict})
			}
		}
		if from >= end {
			break
		}
	}
	if len(scanned) >= maxScan && len(picked) < limit {
		res.Skipped[scanLimitReached]++
	}
	return picked, scanned, settled, nil
}

// classify decide se a mensagem seria recuperada: decodifica a leitura (uma
// que ainda não decodifica voltaria à DLQ) e compara a janela dela com o
// watermark da partição de origem no último checkpoint. O watermark só
// avança, então "atrasada" aqui é definitivo; o contrário não é garantia,
// porque o watermark pode andar entre a republicação e o consumo.
func (r *Reprocessor) classify(ctx context.Context, e Entry, marks map[aggregation.TopicPartition]aggregation.Checkpoint) (string, error) {
	verdict, _, err := r.inspect(ctx, e, marks)
	return verdict, err
}

// inspect é o classify que também devolve a leitura decodificada.
func (r *Reprocessor) inspect(ctx context.Context, e Entry, marks map[aggregation.TopicPartition]aggregation.Checkpoint) (string, contract.SensorReadingRecord, error) {
	var rec contract.SensorReadingRecord
	if err := r.deps.Decoder.Decode(ctx, e.Value, &rec); err != nil {
		if errors.Is(err, contract.ErrMalformed) || errors.Is(err, contract.ErrUnknownSchema) {
			return ActionUndecodable, rec, nil
		}
		return "", rec, fmt.Errorf("decodificar o offset %d: %w", e.Offset, err)
	}
	// A mesma validação do agregador, com o instante de recepção original.
	received := e.Timestamp
	if received.IsZero() {
		received = r.deps.Clock()
	}
	if err := rec.Reading().Validate(received); err != nil {
		return ActionInvalid, rec, nil
	}
	part, err := strconv.ParseInt(e.Headers[aggregation.HeaderSourcePartition], 10, 32)
	if err != nil {
		return ActionWouldReplay, rec, nil
	}
	cp, ok := marks[aggregation.TopicPartition{Topic: r.cfg.Destination, Partition: int32(part)}]
	if !ok {
		return ActionWouldReplay, rec, nil
	}
	wm := watermarkFor(cp, rec.GreenhouseID)
	if wm.IsZero() {
		return ActionWouldReplay, rec, nil
	}
	windowEnd := rec.Reading().MeasuredAt.Truncate(r.cfg.WindowSize).Add(r.cfg.WindowSize)
	if !windowEnd.After(wm) {
		return ActionWouldBeLate, rec, nil
	}
	return ActionWouldReplay, rec, nil
}

// advanceCursor pula o prefixo já resolvido (republicado ou atrasado de vez: o
// watermark só avança, então uma leitura atrasada nunca deixa de ser) para que a próxima execução não
// releia a DLQ inteira. Uma falha aqui só custa leitura extra.
func (r *Reprocessor) advanceCursor(ctx context.Context, cursor int64, scanned []Entry, fresh map[int64]Receipt, settled map[int64]bool) {
	r.advanceCursorOn(ctx, r.deps.Ledger, cursor, scanned, fresh, settled)
}

func (r *Reprocessor) advanceCursorOn(ctx context.Context, ledger Ledger, cursor int64, scanned []Entry, fresh map[int64]Receipt, settled map[int64]bool) {
	offsets := make([]int64, len(scanned))
	for i, e := range scanned {
		offsets[i] = e.Offset
	}
	done, err := ledger.Done(ctx, offsets)
	if err != nil {
		r.deps.Logger.Warn("dlq: não foi possível avançar o cursor", "error", err)
		return
	}
	next := cursor
	for _, e := range scanned {
		if !has(done, e.Offset) && !has(fresh, e.Offset) && !settled[e.Offset] {
			break
		}
		next = e.Offset + 1
	}
	if next > cursor {
		if err := ledger.SetCursor(ctx, next); err != nil {
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
		Destination:    env.String("DLQ_REPLAY_TOPIC", schemas.TopicReadings),
		DLQTopic:       schemas.TopicReadingsDLQ,
		WindowSize:     env.Duration("WINDOW_SIZE", time.Minute),
		BackfillMinAge: env.Duration("DLQ_BACKFILL_MIN_AGE", 5*time.Minute),
		MaxBatch:       min(env.Int("DLQ_REPLAY_MAX_BATCH", 100), absoluteMaxBatch),
	}
}

// watermarkFor é o watermark que vale para a estufa no checkpoint: o dela, ou
// o piso (chave vazia, que no escopo por partição é o da partição). Um
// checkpoint antigo, sem mapa, só tem o da partição.
func watermarkFor(cp aggregation.Checkpoint, greenhouseID string) time.Time {
	if len(cp.Watermarks) == 0 {
		return cp.Watermark
	}
	w := cp.Watermarks[""]
	if g := cp.Watermarks[greenhouseID]; g.After(w) {
		w = g
	}
	return w
}
