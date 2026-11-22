package aggregation_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

type outcomeLog struct {
	aggregation.NopObserver
	outcomes []string
	idle     int
}

func (o *outcomeLog) RecordProcessed(s string) { o.outcomes = append(o.outcomes, s) }
func (o *outcomeLog) IdleClosed(n int)         { o.idle += n }

// idleService monta um serviço com escopo por estufa, watermark ocioso e um
// relógio que o teste controla.
func (h *harness) idleService(window telemetry.WindowConfig, now *time.Time, obs aggregation.Observer) *aggregation.Service {
	h.t.Helper()
	aggEnc, _ := contract.NewEncoder(aggregateSchemaID, latest(h.t, "greenhouse.aggregates"))
	alertEnc, _ := contract.NewEncoder(alertSchemaID, latest(h.t, "greenhouse.alerts"))
	svc, err := aggregation.New(window, telemetry.DefaultThresholds(), topics, aggregation.Deps{
		Decoder: contract.NewDecoder(h.reg), AggregateCodec: aggEnc, AlertCodec: alertEnc,
		Publisher: h, Checkpoints: h, Committer: h, Observer: obs, Clock: func() time.Time { return *now },
	})
	if err != nil {
		h.t.Fatal(err)
	}
	return svc
}

func gh(id string, v float64, at time.Duration) telemetry.Reading {
	r := temp("e-"+id+"-"+at.String(), v, at)
	r.GreenhouseID = id
	return r
}

func TestTickClosesASilentGreenhouseAndKeepsTheDeliveryOrder(t *testing.T) {
	h := newHarness(t)
	now := t0.Add(30 * time.Second)
	obs := &outcomeLog{}
	window := telemetry.WindowConfig{Size: time.Minute, AllowedLateness: 5 * time.Second, Scope: telemetry.ScopeGreenhouse, IdleTimeout: 2 * time.Minute}
	svc := h.idleService(window, &now, obs)
	ctx := context.Background()
	if err := svc.Assign(ctx, []aggregation.TopicPartition{tp}); err != nil {
		t.Fatal(err)
	}
	// A estufa A fala uma vez e se cala; a B continua.
	if err := svc.HandleBatch(ctx, []aggregation.Record{h.record(0, gh("gh-A", 20, 10*time.Second)), h.record(1, gh("gh-B", 21, 10*time.Second))}); err != nil {
		t.Fatal(err)
	}
	h.calls, h.published = nil, nil

	now = t0.Add(60*time.Second + 70*time.Second) // 2 min depois da última atividade: só a A passou do timeout
	if err := svc.HandleBatch(ctx, []aggregation.Record{h.record(2, gh("gh-B", 22, 125*time.Second))}); err != nil {
		t.Fatal(err)
	}
	h.calls, h.published = nil, nil
	now = now.Add(2*time.Minute + time.Second)
	if err := svc.HandleBatch(ctx, []aggregation.Record{h.record(3, gh("gh-B", 23, 190*time.Second))}); err != nil {
		t.Fatal(err)
	}
	h.calls, h.published = nil, nil

	now = now.Add(3 * time.Minute) // B também ficou ociosa, A há muito tempo
	if err := svc.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(h.calls, []string{"publish", "checkpoint", "commit"}) {
		t.Fatalf("ordem das chamadas no Tick: %v", h.calls)
	}
	if obs.idle == 0 || len(h.onTopic("greenhouse.aggregates")) == 0 {
		t.Fatalf("o Tick não fechou nada: idle=%d publicados=%d", obs.idle, len(h.published))
	}
}

func TestTickClosesOnlyTheIdleGreenhouseAndTheClosedWindowIsNotReopened(t *testing.T) {
	h := newHarness(t)
	now := t0.Add(30 * time.Second)
	obs := &outcomeLog{}
	window := telemetry.WindowConfig{Size: time.Minute, AllowedLateness: 5 * time.Second, Scope: telemetry.ScopeGreenhouse, IdleTimeout: 2 * time.Minute}
	svc := h.idleService(window, &now, obs)
	ctx := context.Background()
	_ = svc.Assign(ctx, []aggregation.TopicPartition{tp})
	if err := svc.HandleBatch(ctx, []aggregation.Record{h.record(0, gh("gh-A", 20, 10*time.Second)), h.record(1, gh("gh-B", 21, 10*time.Second))}); err != nil {
		t.Fatal(err)
	}

	// 3 min depois a B volta a falar (nova janela, no tempo de evento de agora); a A segue calada.
	now = t0.Add(3*time.Minute + 30*time.Second)
	if err := svc.HandleBatch(ctx, []aggregation.Record{h.record(2, gh("gh-B", 22, 3*time.Minute+20*time.Second))}); err != nil {
		t.Fatal(err)
	}
	h.published, h.calls = nil, nil
	if err := svc.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var greenhouses []string
	for _, m := range h.onTopic("greenhouse.aggregates") {
		greenhouses = append(greenhouses, h.decodeAggregate(m).GreenhouseID)
	}
	if !slices.Equal(greenhouses, []string{"gh-A"}) {
		t.Fatalf("o Tick devia fechar só a estufa calada (A), fechou %v", greenhouses)
	}
	if cp := h.checkpoints[tp]; cp.Watermarks["gh-A"].IsZero() || !cp.Watermarks["gh-A"].Equal(t0.Add(time.Minute)) {
		t.Fatalf("o checkpoint precisa guardar o watermark da A elevado pelo fechamento: %+v", cp.Watermarks)
	}

	// A leitura tardia da A para a janela já emitida é descartada; nada reemitido.
	obs.outcomes = nil
	if err := svc.HandleBatch(ctx, []aggregation.Record{h.record(3, gh("gh-A", 33, 15*time.Second))}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(obs.outcomes, []string{"late"}) {
		t.Fatalf("outcomes = %v", obs.outcomes)
	}

	// Restart: o novo processo restaura o watermark de cada estufa do checkpoint.
	svc2 := h.idleService(window, &now, obs)
	_ = svc2.Assign(ctx, []aggregation.TopicPartition{tp})
	obs.outcomes = nil
	if err := svc2.HandleBatch(ctx, []aggregation.Record{h.record(10, gh("gh-A", 34, 20*time.Second)), h.record(11, gh("gh-B", 24, 3*time.Minute+40*time.Second))}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(obs.outcomes, []string{"late", "accepted"}) {
		t.Fatalf("depois do restart: %v", obs.outcomes)
	}
}

func TestTickIsANoopWithoutIdleTimeout(t *testing.T) {
	h := newHarness(t)
	now := t0.Add(30 * time.Second)
	svc := h.idleService(telemetry.WindowConfig{Size: time.Minute, AllowedLateness: 5 * time.Second}, &now, nil)
	_ = svc.Assign(context.Background(), []aggregation.TopicPartition{tp})
	_ = svc.HandleBatch(context.Background(), []aggregation.Record{h.record(0, gh("gh-A", 20, 10*time.Second))})
	h.calls = nil
	now = now.Add(time.Hour)
	if err := svc.Tick(context.Background()); err != nil || len(h.calls) != 0 {
		t.Fatalf("err=%v calls=%v", err, h.calls)
	}
}

// A estufa A segura o offset seguro (janela aberta) enquanto a B fecha janelas
// por evento e por ociosidade. Cada avanço de watermark da B precisa ir para o
// checkpoint, senão um crash relê do offset antigo e republica as janelas.
func TestCheckpointRecordsWatermarksEvenWhenTheSafeOffsetDoesNotMove(t *testing.T) {
	h := newHarness(t)
	now := t0.Add(30 * time.Second)
	window := telemetry.WindowConfig{Size: time.Minute, AllowedLateness: 5 * time.Second, Scope: telemetry.ScopeGreenhouse, IdleTimeout: 2 * time.Minute}
	svc := h.idleService(window, &now, nil)
	ctx := context.Background()
	_ = svc.Assign(ctx, []aggregation.TopicPartition{tp})

	a0 := h.record(0, gh("gh-A", 20, 10*time.Second))
	b1 := h.record(1, gh("gh-B", 21, 10*time.Second))
	if err := svc.HandleBatch(ctx, []aggregation.Record{a0, b1}); err != nil {
		t.Fatal(err)
	}
	if h.checkpoints[tp].Offset != 0 || h.commits[tp] != 0 {
		t.Fatalf("a janela aberta da A segura o offset em 0: %+v commits=%v", h.checkpoints[tp], h.commits)
	}

	// B fecha a janela 0 por evento; o offset seguro continua em 0.
	now = t0.Add(80 * time.Second)
	b2 := h.record(2, gh("gh-B", 22, 75*time.Second))
	h.calls, h.published = nil, nil
	if err := svc.HandleBatch(ctx, []aggregation.Record{b2}); err != nil {
		t.Fatal(err)
	}
	if len(h.onTopic("greenhouse.aggregates")) != 1 {
		t.Fatalf("a janela 0 da B devia ter fechado por evento: %d", len(h.onTopic("greenhouse.aggregates")))
	}
	if got, want := h.checkpoints[tp].Watermarks["gh-B"], t0.Add(70*time.Second); !got.Equal(want) {
		t.Fatalf("checkpoint sem o watermark da B por evento: %v, quero %v", got, want)
	}
	if !slices.Equal(h.calls, []string{"publish", "checkpoint"}) {
		t.Fatalf("offset não mudou, então grava o checkpoint e não commita: %v", h.calls)
	}

	// B fica ociosa e a janela 1 fecha pelo Tick; o checkpoint acompanha.
	// A segue falando (mesma janela aberta), então só a B está ociosa.
	now = t0.Add(80*time.Second + 3*time.Minute)
	a3 := h.record(3, gh("gh-A", 23, 20*time.Second))
	if err := svc.HandleBatch(ctx, []aggregation.Record{a3}); err != nil {
		t.Fatal(err)
	}
	h.calls, h.published = nil, nil
	if err := svc.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if got, want := h.checkpoints[tp].Watermarks["gh-B"], t0.Add(2*time.Minute); !got.Equal(want) {
		t.Fatalf("checkpoint sem o watermark da B por ociosidade: %v, quero %v", got, want)
	}
	if h.checkpoints[tp].Offset != 0 {
		t.Fatalf("a A continua segurando o offset: %+v", h.checkpoints[tp])
	}

	// Crash e reinício: relê do offset 0 e a B não é republicada.
	obs := &outcomeLog{}
	svc2 := h.idleService(window, &now, obs)
	_ = svc2.Assign(ctx, []aggregation.TopicPartition{tp})
	h.published = nil
	if err := svc2.HandleBatch(ctx, []aggregation.Record{a0, b1, b2, a3}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(obs.outcomes, []string{"accepted", "late", "late", "accepted"}) {
		t.Fatalf("depois do reinício só a A (janela aberta) é aceita: %v", obs.outcomes)
	}
	for _, m := range h.onTopic("greenhouse.aggregates") {
		if h.decodeAggregate(m).GreenhouseID == "gh-B" {
			t.Fatal("janela da B republicada depois do reinício")
		}
	}
}
