package observability

import (
	"context"
	"sync"
	"time"
)

// Throughput mede registros por segundo numa janela deslizante, amostrando um
// contador monotônico. Uma amostra a cada intervalo basta para um painel.
type Throughput struct {
	source   func() int64
	window   time.Duration
	interval time.Duration
	now      func() time.Time

	mu      sync.Mutex
	samples []sample
}

type sample struct {
	at    time.Time
	value int64
}

// NewThroughput cria o medidor; Run precisa estar rodando para alimentá-lo.
func NewThroughput(source func() int64, window, interval time.Duration) *Throughput {
	return &Throughput{source: source, window: window, interval: interval, now: time.Now}
}

// Run amostra até o contexto acabar.
func (t *Throughput) Run(ctx context.Context) error {
	t.Sample()
	tick := time.NewTicker(t.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			t.Sample()
		}
	}
}

// Sample registra o valor atual e descarta amostras fora da janela.
func (t *Throughput) Sample() {
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.samples = append(t.samples, sample{at: now, value: t.source()})
	cut := 0
	for cut < len(t.samples)-1 && now.Sub(t.samples[cut].at) > t.window {
		cut++
	}
	t.samples = t.samples[cut:]
}

// PerSecond devolve a taxa na janela e quantos segundos ela realmente cobre.
func (t *Throughput) PerSecond() (float64, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.samples) < 2 {
		return 0, 0
	}
	first, last := t.samples[0], t.samples[len(t.samples)-1]
	span := last.at.Sub(first.at).Seconds()
	if span <= 0 {
		return 0, 0
	}
	return float64(last.value-first.value) / span, int(span)
}
