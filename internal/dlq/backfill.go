package dlq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

// Backfill recupera as leituras cuja janela já fechou: o agregador as
// descartaria como atrasadas, então republicar a leitura não adianta. Em vez
// disso, recalcula o agregado da janela somando as leituras recuperadas ao
// que já foi emitido e o republica no tópico de agregados, onde o sink JDBC
// faz upsert pela chave (estufa, grandeza, início).
//
// Idempotência por event_id: o estado de cada janela (o agregado emitido
// original, fotografado na primeira vez, mais o acumulado das leituras
// recuperadas) e o conjunto de event_ids já somados ficam no Redis. O
// agregado publicado é sempre recalculado a partir da fotografia, nunca do
// que está no Postgres, então repetir a operação, ou retomá-la depois de um
// crash, nunca soma uma leitura duas vezes.

// Headers do agregado republicado pelo backfill.
const (
	HeaderBackfilledBy = "dlq.backfilled.by"
	HeaderBackfillRun  = "dlq.backfill.run"
)

// Ações e motivos de pulo do backfill.
const (
	ActionWouldBackfill = "would_backfill"
	ActionBackfilled    = "backfilled"
	notLate             = "not_late"
	tooRecent           = "too_recent"
	alreadyBackfilled   = "already_backfilled"
)

// WindowRef identifica uma janela.
type WindowRef struct {
	GreenhouseID string    `json:"greenhouse_id"`
	SensorType   string    `json:"sensor_type"`
	Start        time.Time `json:"window_start"`
}

func (w WindowRef) key() string {
	return w.GreenhouseID + "|" + w.SensorType + "|" + strconv.FormatInt(w.Start.UnixMilli(), 10)
}

func parseRef(k string) (WindowRef, error) {
	parts := strings.Split(k, "|")
	if len(parts) != 3 {
		return WindowRef{}, fmt.Errorf("janela inválida %q", k)
	}
	ms, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return WindowRef{}, fmt.Errorf("janela inválida %q", k)
	}
	return WindowRef{GreenhouseID: parts[0], SensorType: parts[1], Start: time.UnixMilli(ms).UTC()}, nil
}

// Base é o agregado já emitido da janela (Found falso: nenhuma leitura foi aceita).
type Base struct {
	Found bool
	Count int
	Sum   float64
	Min   float64
	Max   float64
}

// ReadingDelta é uma leitura recuperada.
type ReadingDelta struct {
	EventID string
	Value   float64
}

// WindowState é a fotografia da base mais o acumulado das leituras recuperadas.
type WindowState struct {
	Base    Base
	Count   int
	Sum     float64
	Min     float64
	Max     float64
	Version int64
}

// Merged é o agregado da janela com as leituras recuperadas.
func (s WindowState) Merged(ref WindowRef, size time.Duration) telemetry.Aggregate {
	n, sum := s.Count, s.Sum
	lo, hi := s.Min, s.Max
	if s.Base.Found {
		n += s.Base.Count
		sum += s.Base.Sum
		if s.Count == 0 || s.Base.Min < lo {
			lo = s.Base.Min
		}
		if s.Count == 0 || s.Base.Max > hi {
			hi = s.Base.Max
		}
	}
	mean := 0.0
	if n > 0 {
		mean = sum / float64(n)
	}
	return telemetry.Aggregate{
		Key:   telemetry.WindowKey{GreenhouseID: ref.GreenhouseID, Type: telemetry.SensorType(ref.SensorType), Start: ref.Start},
		End:   ref.Start.Add(size),
		Count: n, Min: lo, Max: hi, Mean: mean,
	}
}

// DirtyWindow é uma janela com acumulado ainda não publicado.
type DirtyWindow struct {
	Ref   WindowRef
	State WindowState
}

// WindowStore guarda, de forma durável, o estado do backfill de cada janela.
type WindowStore interface {
	State(ctx context.Context, ref WindowRef) (WindowState, bool, error)
	// Apply fotografa base (só na primeira vez) e soma as leituras cujo
	// event_id ainda não foi somado; devolve o estado e quantas entraram.
	Apply(ctx context.Context, ref WindowRef, base Base, events []ReadingDelta) (WindowState, int, error)
	// Dirty lista as janelas cujo estado ainda não foi publicado.
	Dirty(ctx context.Context, limit int) ([]DirtyWindow, error)
	// Published limpa a marca se a versão ainda for a publicada.
	Published(ctx context.Context, ref WindowRef, version int64) error
}

// AggregateReader lê o agregado já emitido de uma janela.
type AggregateReader interface {
	Window(ctx context.Context, greenhouseID, sensorType string, start time.Time) (Base, error)
}

// BackfillDeps reúne as portas do backfill.
type BackfillDeps struct {
	// Ledger é o ledger do backfill (offsets da DLQ já recuperados e o cursor
	// próprio), separado do de republicação.
	Ledger         Ledger
	Windows        WindowStore
	Reader         AggregateReader
	AggregateCodec aggregation.Encoder
	AlertCodec     aggregation.Encoder
	Thresholds     telemetry.Thresholds
}

func (d *BackfillDeps) validate() error {
	if d.Ledger == nil || d.Windows == nil || d.Reader == nil || d.AggregateCodec == nil || d.AlertCodec == nil {
		return errors.New("dlq: dependência do backfill ausente")
	}
	return nil
}

// WindowResult descreve uma janela recalculada.
type WindowResult struct {
	WindowRef
	NewEvents   int     `json:"new_events"`
	BaseCount   int     `json:"base_count"`
	ResultCount int     `json:"result_count"`
	ResultMean  float64 `json:"result_mean"`
	Alert       string  `json:"alert,omitempty"`
}

// BackfillResult resume o backfill.
type BackfillResult struct {
	Windows []WindowResult `json:"windows"`
	// Republished conta as janelas pendentes de execuções anteriores que esta
	// execução publicou.
	Republished int `json:"republished_pending"`
}

type candidate struct {
	entry Entry
	ref   WindowRef
	delta ReadingDelta
}

func (r *Reprocessor) backfill(ctx context.Context, req Request, limit int, res Result) (Result, error) {
	bf := r.deps.Backfill
	res.Target = r.cfg.AggregatesTopic
	res.Backfill = &BackfillResult{Windows: []WindowResult{}}

	if req.Execute {
		n, err := r.publishPending(ctx, req, res.RunID)
		if err != nil {
			return res, err
		}
		res.Backfill.Republished = n
	}

	cursor, err := bf.Ledger.Cursor(ctx)
	if err != nil {
		return res, fmt.Errorf("cursor do backfill: %w", err)
	}
	picked, scanned, settled, err := r.pickLate(ctx, cursor, limit, req, &res)
	if err != nil {
		return res, err
	}
	res.Scanned = len(scanned)

	byWindow := map[string][]candidate{}
	var order []string
	for _, c := range picked {
		k := c.ref.key()
		if _, ok := byWindow[k]; !ok {
			order = append(order, k)
		}
		byWindow[k] = append(byWindow[k], c)
		res.Selected = append(res.Selected, Item{Offset: c.entry.Offset, Reason: c.entry.Headers[aggregation.HeaderReason], Action: ActionWouldBackfill})
	}
	sort.Strings(order)
	if len(picked) == 0 {
		return res, nil
	}

	if !req.Execute {
		for _, k := range order {
			cs := byWindow[k]
			wr, err := r.previewWindow(ctx, cs)
			if err != nil {
				return res, err
			}
			res.Backfill.Windows = append(res.Backfill.Windows, wr)
		}
		r.deps.Logger.Info("dlq: dry-run do backfill", "run", res.RunID, "operator", req.Operator, "selected", len(picked))
		return res, nil
	}

	receipts := map[int64]Receipt{}
	now := r.deps.Clock().UTC()
	touched := map[string]WindowState{}
	for _, k := range order {
		cs := byWindow[k]
		ref := cs[0].ref
		base, err := r.baseFor(ctx, ref)
		if err != nil {
			return res, err
		}
		deltas := make([]ReadingDelta, len(cs))
		for i, c := range cs {
			deltas[i] = c.delta
		}
		st, added, err := bf.Windows.Apply(ctx, ref, base, deltas)
		if err != nil {
			return res, fmt.Errorf("somar leituras na janela %s: %w", k, err)
		}
		for _, c := range cs {
			receipts[c.entry.Offset] = Receipt{Operator: req.Operator, RunID: res.RunID, At: now}
		}
		touched[k] = st
		merged := st.Merged(ref, r.cfg.WindowSize)
		wr := WindowResult{WindowRef: ref, NewEvents: added, BaseCount: st.Base.Count, ResultCount: merged.Count, ResultMean: merged.Mean}
		if a, ok := bf.Thresholds.Evaluate(merged); ok {
			wr.Alert = string(a.Rule)
		}
		res.Backfill.Windows = append(res.Backfill.Windows, wr)
	}
	// As leituras já estão somadas de forma durável; o recibo da DLQ vem em
	// seguida e a publicação do agregado por último. Se algo falhar daqui em
	// diante, a janela fica marcada como pendente e a próxima execução a publica.
	wctx, wcancel := context.WithTimeout(context.WithoutCancel(ctx), 2*writeTimeout)
	defer wcancel()
	if err := bf.Ledger.Record(wctx, receipts); err != nil {
		return res, fmt.Errorf("registrar backfill: %w", err)
	}
	for i := range res.Selected {
		res.Selected[i].Action = ActionBackfilled
	}
	res.Replayed = len(picked)
	if err := ctx.Err(); err != nil {
		return res, fmt.Errorf("prazo da execução esgotado antes de publicar (as janelas ficam pendentes): %w", err)
	}
	for _, k := range order {
		ref := byWindow[k][0].ref
		if err := r.publishWindow(ctx, ref, touched[k], req.Operator, res.RunID); err != nil {
			return res, err
		}
	}
	r.advanceCursorOn(wctx, bf.Ledger, cursor, scanned, receipts, settled)
	r.deps.Logger.Info("dlq: backfill", "run", res.RunID, "operator", req.Operator, "entries", len(picked), "windows", len(order))
	return res, nil
}

// baseFor devolve a fotografia já guardada da janela ou, na primeira vez, o
// agregado emitido lido do Postgres. Se a janela não existe lá, só vale quando
// já é velha o bastante para o sink ter entregado o original; pickLate garante.
func (r *Reprocessor) baseFor(ctx context.Context, ref WindowRef) (Base, error) {
	bf := r.deps.Backfill
	if st, ok, err := bf.Windows.State(ctx, ref); err != nil {
		return Base{}, fmt.Errorf("estado da janela: %w", err)
	} else if ok {
		return st.Base, nil
	}
	b, err := bf.Reader.Window(ctx, ref.GreenhouseID, ref.SensorType, ref.Start)
	if err != nil {
		return Base{}, fmt.Errorf("ler o agregado emitido: %w", err)
	}
	return b, nil
}

func (r *Reprocessor) previewWindow(ctx context.Context, cs []candidate) (WindowResult, error) {
	ref := cs[0].ref
	base, err := r.baseFor(ctx, ref)
	if err != nil {
		return WindowResult{}, err
	}
	// Mostra o resultado como se as leituras ainda não somadas entrassem agora.
	st, _, err := r.deps.Backfill.Windows.State(ctx, ref)
	if err != nil {
		return WindowResult{}, err
	}
	st.Base = base
	seen := map[string]bool{}
	added := 0
	for _, c := range cs {
		if seen[c.delta.EventID] {
			continue
		}
		seen[c.delta.EventID] = true
		v := c.delta.Value
		if st.Count == 0 {
			st.Min, st.Max = v, v
		}
		st.Count++
		st.Sum += v
		st.Min, st.Max = math.Min(st.Min, v), math.Max(st.Max, v)
		added++
	}
	merged := st.Merged(ref, r.cfg.WindowSize)
	wr := WindowResult{WindowRef: ref, NewEvents: added, BaseCount: base.Count, ResultCount: merged.Count, ResultMean: merged.Mean}
	if a, ok := r.deps.Backfill.Thresholds.Evaluate(merged); ok {
		wr.Alert = string(a.Rule)
	}
	return wr, nil
}

// publishPending publica as janelas cujo acumulado ficou sem publicar numa
// execução anterior (crash, prazo, broker fora).
func (r *Reprocessor) publishPending(ctx context.Context, req Request, runID string) (int, error) {
	dirty, err := r.deps.Backfill.Windows.Dirty(ctx, 100)
	if err != nil {
		return 0, fmt.Errorf("janelas pendentes: %w", err)
	}
	for _, d := range dirty {
		if err := r.publishWindow(ctx, d.Ref, d.State, req.Operator, runID); err != nil {
			return 0, err
		}
	}
	return len(dirty), nil
}

// publishWindow publica o agregado recalculado (e o alerta, se o limite foi
// ultrapassado) e só então limpa a marca de pendente.
func (r *Reprocessor) publishWindow(ctx context.Context, ref WindowRef, st WindowState, operator, runID string) error {
	bf := r.deps.Backfill
	agg := st.Merged(ref, r.cfg.WindowSize)
	key := []byte(ref.GreenhouseID)
	headers := map[string]string{HeaderBackfilledBy: operator, HeaderBackfillRun: runID}
	value, err := bf.AggregateCodec.Encode(contract.NewWindowAggregateRecord(agg))
	if err != nil {
		return fmt.Errorf("serializar agregado: %w", err)
	}
	msgs := []aggregation.Message{{Topic: r.cfg.AggregatesTopic, Key: key, Value: value, Headers: headers}}
	if alert, ok := bf.Thresholds.Evaluate(agg); ok {
		av, err := bf.AlertCodec.Encode(contract.NewThresholdAlertRecord(alert))
		if err != nil {
			return fmt.Errorf("serializar alerta: %w", err)
		}
		msgs = append(msgs, aggregation.Message{Topic: r.cfg.AlertsTopic, Key: key, Value: av, Headers: headers})
	}
	if err := r.deps.Publisher.Publish(ctx, msgs); err != nil {
		return fmt.Errorf("publicar a janela %s: %w", ref.key(), err)
	}
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
	defer cancel()
	if err := bf.Windows.Published(wctx, ref, st.Version); err != nil {
		return fmt.Errorf("marcar a janela publicada: %w", err)
	}
	return nil
}

// pickLate percorre a DLQ a partir do cursor do backfill (que é próprio, não
// o da republicação) e escolhe as leituras de janela já fechada.
func (r *Reprocessor) pickLate(ctx context.Context, cursor int64, limit int, req Request, res *Result) (picked []candidate, scanned []Entry, settled map[int64]bool, err error) {
	settled = map[int64]bool{}
	bf := r.deps.Backfill
	marks, err := r.deps.Checkpoints.Load(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("carregar checkpoints: %w", err)
	}
	now := r.deps.Clock()
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
		doneB, err := bf.Ledger.Done(ctx, offsets)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("consultar o ledger do backfill: %w", err)
		}
		doneR, err := r.deps.Ledger.Done(ctx, offsets)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("consultar o ledger: %w", err)
		}
		for _, e := range entries {
			from = e.Offset + 1
			if len(picked) >= limit {
				break
			}
			scanned = append(scanned, e)
			switch {
			case has(doneB, e.Offset):
				res.Skipped[alreadyBackfilled]++
				continue
			case has(doneR, e.Offset):
				res.Skipped[alreadyReplayed]++
				settled[e.Offset] = true
				continue
			case e.Headers[aggregation.HeaderSourceTopic] != r.cfg.Destination:
				res.Skipped[foreignSource]++
				settled[e.Offset] = true
				continue
			case req.Reason != "" && e.Headers[aggregation.HeaderReason] != req.Reason:
				res.Skipped[filteredOut]++
				continue
			}
			verdict, rec, err := r.inspect(ctx, e, marks)
			if err != nil {
				return nil, nil, nil, err
			}
			switch verdict {
			case ActionUndecodable:
				res.Skipped[verdict]++
				res.Blocked = append(res.Blocked, Item{Offset: e.Offset, Reason: e.Headers[aggregation.HeaderReason], Action: verdict})
			case ActionWouldReplay:
				// Janela ainda aberta: é caso da republicação, não do backfill.
				res.Skipped[notLate]++
			default:
				ref := WindowRef{GreenhouseID: rec.GreenhouseID, SensorType: rec.SensorType,
					Start: rec.MeasuredAt.UTC().Truncate(r.cfg.WindowSize)}
				if ref.Start.Add(r.cfg.WindowSize).Add(r.cfg.BackfillMinAge).After(now) {
					res.Skipped[tooRecent]++
					continue
				}
				picked = append(picked, candidate{entry: e, ref: ref, delta: ReadingDelta{EventID: rec.EventID, Value: rec.Value}})
			}
		}
		if from >= end {
			break
		}
	}
	return picked, scanned, settled, nil
}

// --- Redis ---

const (
	winPrefix  = "dlq:backfill:win:"
	dirtyKey   = "dlq:backfill:dirty"
	fieldCount = "n"
)

// Soma só os event_ids novos (SADD) ao acumulado, na mesma operação atômica
// que fotografa a base na primeira vez e marca a janela como pendente.
var applyScript = redis.NewScript(`
if redis.call('HSETNX', KEYS[1], 'init', '1') == 1 then
  redis.call('HSET', KEYS[1], 'base_found', ARGV[2], 'base_n', ARGV[3], 'base_sum', ARGV[4], 'base_min', ARGV[5], 'base_max', ARGV[6],
    'n', '0', 'sum', '0', 'min', '0', 'max', '0', 'ver', '0')
end
local n = tonumber(redis.call('HGET', KEYS[1], 'n'))
local sum = tonumber(redis.call('HGET', KEYS[1], 'sum'))
local mn = tonumber(redis.call('HGET', KEYS[1], 'min'))
local mx = tonumber(redis.call('HGET', KEYS[1], 'max'))
local added = 0
for i = 7, #ARGV, 2 do
  if redis.call('SADD', KEYS[2], ARGV[i]) == 1 then
    local v = tonumber(ARGV[i + 1])
    if n == 0 then
      mn = v
      mx = v
    else
      if v < mn then mn = v end
      if v > mx then mx = v end
    end
    n = n + 1
    sum = sum + v
    added = added + 1
  end
end
if added > 0 then
  redis.call('HSET', KEYS[1], 'n', tostring(n), 'sum', string.format('%.17g', sum), 'min', string.format('%.17g', mn), 'max', string.format('%.17g', mx))
  redis.call('HINCRBY', KEYS[1], 'ver', 1)
  redis.call('SADD', KEYS[3], ARGV[1])
end
return added
`)

var publishedScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'ver') == ARGV[1] then return redis.call('SREM', KEYS[2], ARGV[2]) end
return 0
`)

// RedisWindowStore implementa WindowStore.
type RedisWindowStore struct{ client redis.UniversalClient }

// NewRedisWindowStore usa um cliente já configurado.
func NewRedisWindowStore(client redis.UniversalClient) *RedisWindowStore {
	return &RedisWindowStore{client: client}
}

func f(v float64) string { return strconv.FormatFloat(v, 'g', 17, 64) }

func (s *RedisWindowStore) State(ctx context.Context, ref WindowRef) (WindowState, bool, error) {
	m, err := s.client.HGetAll(ctx, winPrefix+ref.key()).Result()
	if err != nil {
		return WindowState{}, false, err
	}
	if len(m) == 0 {
		return WindowState{}, false, nil
	}
	num := func(k string) float64 { v, _ := strconv.ParseFloat(m[k], 64); return v }
	ver, _ := strconv.ParseInt(m["ver"], 10, 64)
	return WindowState{
		Base:  Base{Found: m["base_found"] == "1", Count: int(num("base_n")), Sum: num("base_sum"), Min: num("base_min"), Max: num("base_max")},
		Count: int(num(fieldCount)), Sum: num("sum"), Min: num("min"), Max: num("max"), Version: ver,
	}, true, nil
}

func (s *RedisWindowStore) Apply(ctx context.Context, ref WindowRef, base Base, events []ReadingDelta) (WindowState, int, error) {
	found := "0"
	if base.Found {
		found = "1"
	}
	args := []any{ref.key(), found, strconv.Itoa(base.Count), f(base.Sum), f(base.Min), f(base.Max)}
	for _, e := range events {
		args = append(args, e.EventID, f(e.Value))
	}
	k := winPrefix + ref.key()
	added, err := applyScript.Run(ctx, s.client, []string{k, k + ":events", dirtyKey}, args...).Int()
	if err != nil {
		return WindowState{}, 0, err
	}
	st, _, err := s.State(ctx, ref)
	return st, added, err
}

func (s *RedisWindowStore) Dirty(ctx context.Context, limit int) ([]DirtyWindow, error) {
	members, err := s.client.SMembers(ctx, dirtyKey).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(members)
	if len(members) > limit {
		members = members[:limit]
	}
	out := make([]DirtyWindow, 0, len(members))
	for _, m := range members {
		ref, err := parseRef(m)
		if err != nil {
			return nil, err
		}
		st, ok, err := s.State(ctx, ref)
		if err != nil {
			return nil, err
		}
		if !ok {
			_ = s.client.SRem(ctx, dirtyKey, m).Err()
			continue
		}
		out = append(out, DirtyWindow{Ref: ref, State: st})
	}
	return out, nil
}

func (s *RedisWindowStore) Published(ctx context.Context, ref WindowRef, version int64) error {
	return publishedScript.Run(ctx, s.client, []string{winPrefix + ref.key(), dirtyKey}, strconv.FormatInt(version, 10), ref.key()).Err()
}
