package telemetry

import (
	"errors"
	"math"
	"sort"
	"time"
)

// WindowKey identifica uma janela tumbling de uma grandeza de uma estufa.
type WindowKey struct {
	GreenhouseID string
	Type         SensorType
	Start        time.Time
}

// Aggregate é o resultado de uma janela fechada.
type Aggregate struct {
	Key   WindowKey
	End   time.Time
	Count int
	Min   float64
	Max   float64
	Mean  float64
}

// WatermarkScope define a granularidade do watermark.
type WatermarkScope string

const (
	// ScopePartition mantém um watermark por partição (o padrão histórico).
	ScopePartition WatermarkScope = "partition"
	// ScopeGreenhouse mantém um watermark por estufa: o relógio adiantado de
	// um sensor só adianta a própria estufa, e uma estufa calada não segura
	// as janelas das outras.
	ScopeGreenhouse WatermarkScope = "greenhouse"
)

// WindowConfig define o tamanho das janelas e a tolerância a atrasos.
type WindowConfig struct {
	Size            time.Duration
	AllowedLateness time.Duration
	// Scope vazio vale ScopePartition.
	Scope WatermarkScope
	// IdleTimeout liga o watermark ocioso: se uma chave do watermark (partição
	// ou estufa, conforme Scope) fica esse tempo, em tempo de processamento,
	// sem receber leituras, suas janelas abertas já vencidas são fechadas.
	// Zero desliga.
	IdleTimeout time.Duration
}

// ErrInvalidWindowConfig indica tamanho ou tolerância inválidos.
var ErrInvalidWindowConfig = errors.New("invalid window config")

// Validate confere a configuração.
func (c WindowConfig) Validate() error {
	if c.Size <= 0 || c.AllowedLateness < 0 || c.IdleTimeout < 0 {
		return ErrInvalidWindowConfig
	}
	switch c.Scope {
	case "", ScopePartition, ScopeGreenhouse:
		return nil
	}
	return ErrInvalidWindowConfig
}

// Outcome é o destino de uma leitura válida ao entrar na partição.
type Outcome string

const (
	OutcomeAccepted  Outcome = "accepted"
	OutcomeDuplicate Outcome = "duplicate"
	OutcomeLate      Outcome = "late"
)

type openWindow struct {
	key         WindowKey
	count       int
	sum         float64
	min         float64
	max         float64
	firstOffset int64
}

// PartitionWindows guarda as janelas abertas de uma partição do tópico de
// leituras e decide até onde é seguro commitar o offset.
//
// Invariantes:
//   - cada watermark só avança; uma janela fecha quando End <= watermark da
//     sua chave (a partição inteira ou a estufa, conforme Scope);
//   - uma leitura cuja janela já fechou é atrasada e descartada (isso também
//     cobre o reprocessamento de janelas já emitidas depois de um restart);
//   - o offset seguro para commit nunca ultrapassa o primeiro registro de uma
//     janela aberta, então um crash não perde leituras ainda não agregadas;
//   - o fechamento por ociosidade só fecha janelas cujo fim mais a tolerância
//     já passou no relógio de processamento, e eleva o watermark ao fim delas,
//     então nenhuma leitura volta a entrar numa janela já emitida.
type PartitionWindows struct {
	cfg        WindowConfig
	windows    map[WindowKey]*openWindow
	keys       map[string]*keyState
	floor      time.Time
	nextOffset int64
}

// keyState é o estado de tempo de uma chave do watermark.
type keyState struct {
	dedup        *Deduplicator
	maxEventTime time.Time
	watermark    time.Time
	lastActive   time.Time
}

// NewPartitionWindows cria o estado de uma partição. watermark é o valor
// restaurado do último checkpoint (zero quando não existe) e vale como piso
// para todas as chaves; nextOffset é o offset a partir do qual o consumo
// recomeça.
func NewPartitionWindows(cfg WindowConfig, watermark time.Time, nextOffset int64) (*PartitionWindows, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &PartitionWindows{
		cfg:        cfg,
		windows:    make(map[WindowKey]*openWindow),
		keys:       make(map[string]*keyState),
		floor:      watermark,
		nextOffset: nextOffset,
	}, nil
}

// scopeKey é a chave do watermark de uma estufa. A chave vazia é a da
// partição e, no escopo por estufa, o piso das estufas ainda não vistas.
func (p *PartitionWindows) scopeKey(greenhouseID string) string {
	if p.cfg.Scope == ScopeGreenhouse {
		return greenhouseID
	}
	return ""
}

func (p *PartitionWindows) state(key string) *keyState {
	st, ok := p.keys[key]
	if !ok {
		st = &keyState{dedup: NewDeduplicator()}
		p.keys[key] = st
	}
	return st
}

// wm é o watermark efetivo de uma chave.
func (p *PartitionWindows) wm(key string) time.Time {
	w := p.floor
	if st, ok := p.keys[key]; ok && st.watermark.After(w) {
		w = st.watermark
	}
	return w
}

// RestoreWatermarks restaura os watermarks de um checkpoint. A chave vazia é
// o piso; no escopo por partição vale o maior de todos, para trocar o escopo
// de um checkpoint existente nunca reabrir uma janela emitida.
func (p *PartitionWindows) RestoreWatermarks(m map[string]time.Time) {
	p.floor = m[""]
	for k, v := range m {
		if k == "" {
			continue
		}
		if p.cfg.Scope == ScopeGreenhouse {
			st := p.state(k)
			if v.After(st.watermark) {
				st.watermark = v
			}
		} else if v.After(p.floor) {
			p.floor = v
		}
	}
}

// Add incorpora uma leitura já validada que veio do offset informado, sem
// registrar atividade (o fechamento por ociosidade não a enxerga).
func (p *PartitionWindows) Add(r Reading, offset int64) Outcome {
	return p.AddAt(r, offset, time.Time{})
}

// AddAt é Add com o instante de processamento, usado para detectar ociosidade.
func (p *PartitionWindows) AddAt(r Reading, offset int64, now time.Time) Outcome {
	p.advanceOffset(offset)
	key := p.scopeKey(r.GreenhouseID)
	st := p.state(key)
	if !now.IsZero() {
		st.lastActive = now
	}
	if r.MeasuredAt.After(st.maxEventTime) {
		st.maxEventTime = r.MeasuredAt
		if w := st.maxEventTime.Add(-p.cfg.AllowedLateness); w.After(st.watermark) {
			st.watermark = w
		}
	}

	start := r.MeasuredAt.Truncate(p.cfg.Size)
	if !start.Add(p.cfg.Size).After(p.wm(key)) {
		return OutcomeLate
	}
	if st.dedup.Seen(r.EventID, r.MeasuredAt) {
		return OutcomeDuplicate
	}

	wkey := WindowKey{GreenhouseID: r.GreenhouseID, Type: r.Type, Start: start}
	w, ok := p.windows[wkey]
	if !ok {
		w = &openWindow{key: wkey, min: math.Inf(1), max: math.Inf(-1), firstOffset: offset}
		p.windows[wkey] = w
	}
	w.count++
	w.sum += r.Value
	w.min = math.Min(w.min, r.Value)
	w.max = math.Max(w.max, r.Value)
	return OutcomeAccepted
}

// Skip marca como processado um registro que não entra em janela nenhuma,
// por exemplo uma mensagem inválida que já foi enviada para a DLQ.
func (p *PartitionWindows) Skip(offset int64) { p.advanceOffset(offset) }

func (p *PartitionWindows) advanceOffset(offset int64) {
	if offset+1 > p.nextOffset {
		p.nextOffset = offset + 1
	}
}

func (p *PartitionWindows) aggregate(key WindowKey, w *openWindow) Aggregate {
	return Aggregate{Key: key, End: key.Start.Add(p.cfg.Size), Count: w.count, Min: w.min, Max: w.max, Mean: w.sum / float64(w.count)}
}

func sortAggregates(closed []Aggregate) {
	sort.Slice(closed, func(i, j int) bool {
		a, b := closed[i].Key, closed[j].Key
		if !a.Start.Equal(b.Start) {
			return a.Start.Before(b.Start)
		}
		if a.GreenhouseID != b.GreenhouseID {
			return a.GreenhouseID < b.GreenhouseID
		}
		return a.Type < b.Type
	})
}

func (p *PartitionWindows) evictDedup() {
	for k, st := range p.keys {
		st.dedup.EvictBefore(p.wm(k).Add(-p.cfg.Size))
	}
}

// CloseReady remove e devolve as janelas cujo fim já passou do watermark da
// sua chave, em ordem determinística (início, estufa, tipo).
func (p *PartitionWindows) CloseReady() []Aggregate {
	var closed []Aggregate
	for key, w := range p.windows {
		if key.Start.Add(p.cfg.Size).After(p.wm(p.scopeKey(key.GreenhouseID))) {
			continue
		}
		closed = append(closed, p.aggregate(key, w))
		delete(p.windows, key)
	}
	sortAggregates(closed)
	p.evictDedup()
	return closed
}

// CloseIdle fecha as janelas das chaves ociosas (sem leituras há IdleTimeout
// em tempo de processamento) cujo fim mais a tolerância já passou em now, e
// eleva o watermark da chave ao fim da última janela fechada. Sem
// IdleTimeout, não faz nada.
func (p *PartitionWindows) CloseIdle(now time.Time) []Aggregate {
	if p.cfg.IdleTimeout <= 0 {
		return nil
	}
	idle := map[string]bool{}
	for k, st := range p.keys {
		if !st.lastActive.IsZero() && now.Sub(st.lastActive) >= p.cfg.IdleTimeout {
			idle[k] = true
		}
	}
	var closed []Aggregate
	raised := map[string]time.Time{}
	for key, w := range p.windows {
		k := p.scopeKey(key.GreenhouseID)
		end := key.Start.Add(p.cfg.Size)
		if !idle[k] || end.Add(p.cfg.AllowedLateness).After(now) {
			continue
		}
		closed = append(closed, p.aggregate(key, w))
		delete(p.windows, key)
		if end.After(raised[k]) {
			raised[k] = end
		}
	}
	for k, end := range raised {
		if st := p.state(k); end.After(st.watermark) {
			st.watermark = end
		}
	}
	sortAggregates(closed)
	p.evictDedup()
	return closed
}

// CommitOffset devolve o próximo offset que pode ser commitado sem perder
// leituras de janelas ainda abertas.
func (p *PartitionWindows) CommitOffset() int64 {
	safe := p.nextOffset
	for _, w := range p.windows {
		if w.firstOffset < safe {
			safe = w.firstOffset
		}
	}
	return safe
}

// Watermark devolve o watermark mais avançado entre as chaves.
func (p *PartitionWindows) Watermark() time.Time {
	w := p.floor
	for _, st := range p.keys {
		if st.watermark.After(w) {
			w = st.watermark
		}
	}
	return w
}

// Watermarks devolve o watermark de cada chave para o checkpoint; a chave
// vazia é o piso (ou, no escopo por partição, o watermark da partição).
func (p *PartitionWindows) Watermarks() map[string]time.Time {
	out := map[string]time.Time{}
	if !p.floor.IsZero() {
		out[""] = p.floor
	}
	for k := range p.keys {
		if w := p.wm(k); !w.IsZero() {
			out[k] = w
		}
	}
	return out
}

// OpenWindows devolve quantas janelas estão abertas.
func (p *PartitionWindows) OpenWindows() int { return len(p.windows) }

// TrackedEvents devolve quantos ids os deduplicadores retêm.
func (p *PartitionWindows) TrackedEvents() int {
	n := 0
	for _, st := range p.keys {
		n += st.dedup.Len()
	}
	return n
}
