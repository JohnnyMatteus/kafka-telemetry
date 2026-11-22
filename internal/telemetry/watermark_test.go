package telemetry_test

import (
	"testing"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

func scoped(t *testing.T, scope telemetry.WatermarkScope, idle time.Duration) *telemetry.PartitionWindows {
	t.Helper()
	p, err := telemetry.NewPartitionWindows(telemetry.WindowConfig{Size: time.Minute, AllowedLateness: 10 * time.Second, Scope: scope, IdleTimeout: idle}, time.Time{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// Um sensor com relógio 4 minutos adiantado, na estufa A.
func TestGreenhouseWatermarkIsolatesAClockSkewedSensor(t *testing.T) {
	skewed := reading("a1", "gh-A", telemetry.SensorTemperature, 20, base.Add(4*time.Minute))
	normal := reading("b1", "gh-B", telemetry.SensorTemperature, 20, base.Add(5*time.Second))

	byPartition := scoped(t, telemetry.ScopePartition, 0)
	byPartition.Add(skewed, 0)
	if got := byPartition.Add(normal, 1); got != telemetry.OutcomeLate {
		t.Fatalf("por partição, a estufa B devia perder a leitura (late), veio %s", got)
	}

	byGreenhouse := scoped(t, telemetry.ScopeGreenhouse, 0)
	byGreenhouse.Add(skewed, 0)
	if got := byGreenhouse.Add(normal, 1); got != telemetry.OutcomeAccepted {
		t.Fatalf("por estufa, a leitura da B devia ser aceita, veio %s", got)
	}
}

func TestGreenhouseWatermarkStillClosesAndDropsLateWithinTheSameGreenhouse(t *testing.T) {
	p := scoped(t, telemetry.ScopeGreenhouse, 0)
	p.Add(reading("1", "gh-A", telemetry.SensorTemperature, 20, base), 0)
	p.Add(reading("2", "gh-B", telemetry.SensorTemperature, 30, base), 1)
	p.Add(reading("3", "gh-A", telemetry.SensorTemperature, 22, base.Add(75*time.Second)), 2)
	closed := p.CloseReady()
	if len(closed) != 1 || closed[0].Key.GreenhouseID != "gh-A" {
		t.Fatalf("só a janela da A passou do watermark dela: %+v", closed)
	}
	if got := p.Add(reading("4", "gh-A", telemetry.SensorTemperature, 21, base.Add(5*time.Second)), 3); got != telemetry.OutcomeLate {
		t.Fatalf("outcome = %s", got)
	}
	if got := p.Add(reading("5", "gh-B", telemetry.SensorTemperature, 31, base.Add(5*time.Second)), 4); got != telemetry.OutcomeAccepted {
		t.Fatalf("a B ainda tem a janela aberta: %s", got)
	}
}

func TestIdleWatermarkClosesWindowsOfASilentGreenhouse(t *testing.T) {
	p := scoped(t, telemetry.ScopeGreenhouse, 2*time.Minute)
	t0 := base.Add(time.Hour) // relógio de processamento
	p.AddAt(reading("a", "gh-A", telemetry.SensorTemperature, 20, base), 0, t0)
	p.AddAt(reading("b", "gh-B", telemetry.SensorTemperature, 25, base), 1, t0)

	if got := p.CloseIdle(t0.Add(time.Minute)); len(got) != 0 {
		t.Fatalf("fechou antes do timeout de ociosidade: %+v", got)
	}
	// A B continua falando; só a A fica calada.
	p.AddAt(reading("b2", "gh-B", telemetry.SensorTemperature, 26, base.Add(20*time.Second)), 2, t0.Add(100*time.Second))
	closed := p.CloseIdle(t0.Add(3 * time.Minute))
	if len(closed) != 1 || closed[0].Key.GreenhouseID != "gh-A" || closed[0].Count != 1 {
		t.Fatalf("esperava só a janela da A fechada: %+v", closed)
	}
	// Depois do fechamento, uma leitura tardia daquela janela é descartada: nada é emitido duas vezes.
	if got := p.AddAt(reading("a2", "gh-A", telemetry.SensorTemperature, 21, base.Add(30*time.Second)), 3, t0.Add(4*time.Minute)); got != telemetry.OutcomeLate {
		t.Fatalf("outcome = %s", got)
	}
	if got := p.CommitOffset(); got != 1 {
		t.Fatalf("o offset seguro devia parar na janela aberta da B (offset 1), veio %d", got)
	}
}

func TestIdleWatermarkNeverClosesAWindowBeforeItsEndPlusLatenessInProcessingTime(t *testing.T) {
	// Relógio de processamento atrás do tempo de evento (leitura adiantada dentro da tolerância).
	now := base.Add(10 * time.Second)
	p2 := scoped(t, telemetry.ScopePartition, time.Second)
	p2.AddAt(reading("a", "gh-A", telemetry.SensorTemperature, 20, base.Add(5*time.Second)), 0, now)
	if got := p2.CloseIdle(now.Add(2 * time.Second)); len(got) != 0 {
		t.Fatalf("ocioso, mas a janela ainda não venceu no relógio de processamento: %+v", got)
	}
	if got := p2.CloseIdle(base.Add(71 * time.Second)); len(got) != 1 {
		t.Fatalf("depois do fim da janela mais a tolerância ela fecha: %+v", got)
	}
}

func TestIdleWatermarkIsOffByDefault(t *testing.T) {
	p := scoped(t, telemetry.ScopePartition, 0)
	p.AddAt(reading("a", "gh-A", telemetry.SensorTemperature, 20, base), 0, base)
	if got := p.CloseIdle(base.Add(24 * time.Hour)); len(got) != 0 {
		t.Fatalf("got = %+v", got)
	}
}

func TestWatermarksRoundTripThroughACheckpoint(t *testing.T) {
	p := scoped(t, telemetry.ScopeGreenhouse, 0)
	p.Add(reading("1", "gh-A", telemetry.SensorTemperature, 20, base.Add(3*time.Minute)), 0)
	p.Add(reading("2", "gh-B", telemetry.SensorTemperature, 20, base.Add(30*time.Second)), 1)
	saved := p.Watermarks()

	r := scoped(t, telemetry.ScopeGreenhouse, 0)
	r.RestoreWatermarks(saved)
	if got := r.Add(reading("3", "gh-A", telemetry.SensorTemperature, 20, base.Add(time.Minute)), 5); got != telemetry.OutcomeLate {
		t.Fatalf("A restaurada deveria descartar janela antiga: %s", got)
	}
	if got := r.Add(reading("4", "gh-B", telemetry.SensorTemperature, 20, base.Add(40*time.Second)), 6); got != telemetry.OutcomeAccepted {
		t.Fatalf("B restaurada não podia herdar o watermark da A: %s", got)
	}
}

// Um checkpoint antigo só tem o watermark da partição: vale como piso para
// todas as estufas, inclusive as que ainda não apareceram.
func TestLegacyCheckpointWatermarkActsAsFloorForEveryGreenhouse(t *testing.T) {
	floor := base.Add(5 * time.Minute)
	p, err := telemetry.NewPartitionWindows(telemetry.WindowConfig{Size: time.Minute, AllowedLateness: 10 * time.Second, Scope: telemetry.ScopeGreenhouse}, floor, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Add(reading("1", "gh-NOVA", telemetry.SensorTemperature, 20, base.Add(time.Minute)), 0); got != telemetry.OutcomeLate {
		t.Fatalf("outcome = %s", got)
	}
	if w := p.Watermarks()[""]; !w.Equal(floor) {
		t.Fatalf("o piso precisa sobreviver ao próximo checkpoint: %v", p.Watermarks())
	}
}

func TestSwitchingToPartitionScopeKeepsTheHighestWatermark(t *testing.T) {
	p := scoped(t, telemetry.ScopePartition, 0)
	p.RestoreWatermarks(map[string]time.Time{"gh-A": base.Add(5 * time.Minute), "gh-B": base.Add(time.Minute)})
	if got := p.Add(reading("1", "gh-B", telemetry.SensorTemperature, 20, base.Add(2*time.Minute)), 0); got != telemetry.OutcomeLate {
		t.Fatalf("outcome = %s", got)
	}
}

func TestInvalidWatermarkConfig(t *testing.T) {
	for _, c := range []telemetry.WindowConfig{
		{Size: time.Minute, Scope: "outro"},
		{Size: time.Minute, IdleTimeout: -time.Second},
	} {
		if _, err := telemetry.NewPartitionWindows(c, time.Time{}, 0); err == nil {
			t.Errorf("%+v: esperava erro", c)
		}
	}
}
