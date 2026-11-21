package observability

import (
	"testing"
	"time"
)

func TestThroughputSlidingWindow(t *testing.T) {
	var total int64
	clock := time.Unix(0, 0)
	th := NewThroughput(func() int64 { return total }, 30*time.Second, 5*time.Second)
	th.now = func() time.Time { return clock }

	if rate, _ := th.PerSecond(); rate != 0 {
		t.Fatalf("sem amostras a taxa deve ser 0, veio %v", rate)
	}
	for i := 0; i < 4; i++ { // 100 registros a cada 5 s = 20/s
		th.Sample()
		clock = clock.Add(5 * time.Second)
		total += 100
	}
	th.Sample()
	rate, window := th.PerSecond()
	if rate != 20 || window != 20 {
		t.Fatalf("taxa=%v janela=%v, esperado 20 e 20", rate, window)
	}

	// Depois de ficar parado além da janela, a taxa cai a zero.
	for i := 0; i < 8; i++ {
		clock = clock.Add(5 * time.Second)
		th.Sample()
	}
	if rate, _ := th.PerSecond(); rate != 0 {
		t.Fatalf("parado deveria dar 0, veio %v", rate)
	}
}
