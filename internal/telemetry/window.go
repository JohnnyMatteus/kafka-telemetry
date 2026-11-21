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

// WindowConfig define o tamanho das janelas e a tolerância a atrasos.
type WindowConfig struct {
	Size            time.Duration
	AllowedLateness time.Duration
}

// ErrInvalidWindowConfig indica tamanho ou tolerância inválidos.
var ErrInvalidWindowConfig = errors.New("invalid window config")

// Validate confere a configuração.
func (c WindowConfig) Validate() error {
	if c.Size <= 0 || c.AllowedLateness < 0 {
		return ErrInvalidWindowConfig
	}
	return nil
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
//   - o watermark só avança; uma janela fecha quando End <= watermark;
//   - uma leitura cuja janela já fechou é atrasada e descartada (isso também
//     cobre o reprocessamento de janelas já emitidas depois de um restart);
//   - o offset seguro para commit nunca ultrapassa o primeiro registro de uma
//     janela aberta, então um crash não perde leituras ainda não agregadas.
type PartitionWindows struct {
	cfg          WindowConfig
	windows      map[WindowKey]*openWindow
	dedup        *Deduplicator
	maxEventTime time.Time
	watermark    time.Time
	nextOffset   int64
}

// NewPartitionWindows cria o estado de uma partição. watermark é o valor
// restaurado do último checkpoint (zero quando não existe) e nextOffset é o
// offset a partir do qual o consumo recomeça.
func NewPartitionWindows(cfg WindowConfig, watermark time.Time, nextOffset int64) (*PartitionWindows, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &PartitionWindows{
		cfg:        cfg,
		windows:    make(map[WindowKey]*openWindow),
		dedup:      NewDeduplicator(),
		watermark:  watermark,
		nextOffset: nextOffset,
	}, nil
}

// Add incorpora uma leitura já validada que veio do offset informado.
func (p *PartitionWindows) Add(r Reading, offset int64) Outcome {
	p.advanceOffset(offset)
	if r.MeasuredAt.After(p.maxEventTime) {
		p.maxEventTime = r.MeasuredAt
		if wm := p.maxEventTime.Add(-p.cfg.AllowedLateness); wm.After(p.watermark) {
			p.watermark = wm
		}
	}

	start := r.MeasuredAt.Truncate(p.cfg.Size)
	if !start.Add(p.cfg.Size).After(p.watermark) {
		return OutcomeLate
	}
	if p.dedup.Seen(r.EventID, r.MeasuredAt) {
		return OutcomeDuplicate
	}

	key := WindowKey{GreenhouseID: r.GreenhouseID, Type: r.Type, Start: start}
	w, ok := p.windows[key]
	if !ok {
		w = &openWindow{key: key, min: math.Inf(1), max: math.Inf(-1), firstOffset: offset}
		p.windows[key] = w
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

// CloseReady remove e devolve as janelas cujo fim já passou do watermark,
// em ordem determinística (início, estufa, tipo).
func (p *PartitionWindows) CloseReady() []Aggregate {
	var closed []Aggregate
	for key, w := range p.windows {
		end := key.Start.Add(p.cfg.Size)
		if end.After(p.watermark) {
			continue
		}
		closed = append(closed, Aggregate{
			Key:   key,
			End:   end,
			Count: w.count,
			Min:   w.min,
			Max:   w.max,
			Mean:  w.sum / float64(w.count),
		})
		delete(p.windows, key)
	}
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
	p.dedup.EvictBefore(p.watermark.Add(-p.cfg.Size))
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

// Watermark devolve o watermark atual da partição.
func (p *PartitionWindows) Watermark() time.Time { return p.watermark }

// OpenWindows devolve quantas janelas estão abertas.
func (p *PartitionWindows) OpenWindows() int { return len(p.windows) }

// TrackedEvents devolve quantos ids o deduplicador retém.
func (p *PartitionWindows) TrackedEvents() int { return p.dedup.Len() }
