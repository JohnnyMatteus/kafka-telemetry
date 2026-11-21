package aggregation_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

const (
	readingSchemaID   = 1
	aggregateSchemaID = 2
	alertSchemaID     = 3
)

var (
	t0     = time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	tp     = aggregation.TopicPartition{Topic: schemas.TopicReadings, Partition: 0}
	topics = aggregation.Topics{Aggregates: schemas.TopicAggregates, Alerts: schemas.TopicAlerts, DLQ: schemas.TopicReadingsDLQ}
)

// registry é um Schema Registry em memória com os schemas do repositório.
type registry struct {
	schemas      map[int]string
	failuresLeft int
}

func (r *registry) SchemaByID(_ context.Context, id int) (string, error) {
	if r.failuresLeft > 0 {
		r.failuresLeft--
		return "", errors.New("registry indisponível")
	}
	s, ok := r.schemas[id]
	if !ok {
		return "", fmt.Errorf("id %d: %w", id, contract.ErrUnknownSchema)
	}
	return s, nil
}

// harness grava a ordem das chamadas para verificar publish -> checkpoint -> commit.
type harness struct {
	t           *testing.T
	reg         *registry
	readingEnc  *contract.Encoder
	calls       []string
	published   []aggregation.Message
	checkpoints map[aggregation.TopicPartition]aggregation.Checkpoint
	commits     map[aggregation.TopicPartition]int64
	publishErr  error
	saveErr     error
	commitErr   error
	svc         *aggregation.Service
}

func (h *harness) Publish(_ context.Context, msgs []aggregation.Message) error {
	h.calls = append(h.calls, "publish")
	if h.publishErr != nil {
		return h.publishErr
	}
	h.published = append(h.published, msgs...)
	return nil
}

func (h *harness) Save(_ context.Context, cps []aggregation.Checkpoint) error {
	h.calls = append(h.calls, "checkpoint")
	if h.saveErr != nil {
		return h.saveErr
	}
	for _, cp := range cps {
		h.checkpoints[cp.TopicPartition] = cp
	}
	return nil
}

func (h *harness) Load(context.Context) (map[aggregation.TopicPartition]aggregation.Checkpoint, error) {
	out := make(map[aggregation.TopicPartition]aggregation.Checkpoint, len(h.checkpoints))
	for k, v := range h.checkpoints {
		out[k] = v
	}
	return out, nil
}

func (h *harness) Commit(_ context.Context, offsets map[aggregation.TopicPartition]int64) error {
	h.calls = append(h.calls, "commit")
	if h.commitErr != nil {
		return h.commitErr
	}
	for k, v := range offsets {
		h.commits[k] = v
	}
	return nil
}

func latest(t *testing.T, topic string) string {
	t.Helper()
	sub, err := schemas.Lookup(topic)
	if err != nil {
		t.Fatal(err)
	}
	text, err := sub.Latest()
	if err != nil {
		t.Fatal(err)
	}
	return text
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	readingText, aggText, alertText := latest(t, schemas.TopicReadings), latest(t, schemas.TopicAggregates), latest(t, schemas.TopicAlerts)
	h := &harness{
		t:           t,
		reg:         &registry{schemas: map[int]string{readingSchemaID: readingText, aggregateSchemaID: aggText, alertSchemaID: alertText}},
		checkpoints: map[aggregation.TopicPartition]aggregation.Checkpoint{},
		commits:     map[aggregation.TopicPartition]int64{},
	}
	var err error
	if h.readingEnc, err = contract.NewEncoder(readingSchemaID, readingText); err != nil {
		t.Fatal(err)
	}
	h.svc = h.newService()
	return h
}

func (h *harness) newService() *aggregation.Service {
	h.t.Helper()
	aggEnc, err := contract.NewEncoder(aggregateSchemaID, latest(h.t, schemas.TopicAggregates))
	if err != nil {
		h.t.Fatal(err)
	}
	alertEnc, err := contract.NewEncoder(alertSchemaID, latest(h.t, schemas.TopicAlerts))
	if err != nil {
		h.t.Fatal(err)
	}
	svc, err := aggregation.New(
		telemetry.WindowConfig{Size: time.Minute, AllowedLateness: 5 * time.Second},
		telemetry.DefaultThresholds(),
		topics,
		aggregation.Deps{
			Decoder:        contract.NewDecoder(h.reg),
			AggregateCodec: aggEnc,
			AlertCodec:     alertEnc,
			Publisher:      h,
			Checkpoints:    h,
			Committer:      h,
			Clock:          func() time.Time { return t0.Add(10 * time.Minute) },
			RetryBackoff:   time.Millisecond,
		},
	)
	if err != nil {
		h.t.Fatal(err)
	}
	return svc
}

func (h *harness) record(offset int64, r telemetry.Reading) aggregation.Record {
	h.t.Helper()
	value, err := h.readingEnc.Encode(contract.NewSensorReadingRecord(r, "1.0.0"))
	if err != nil {
		h.t.Fatal(err)
	}
	return aggregation.Record{TopicPartition: tp, Offset: offset, Key: []byte(r.GreenhouseID), Value: value}
}

func temp(id string, v float64, at time.Duration) telemetry.Reading {
	return telemetry.Reading{EventID: id, GreenhouseID: "gh-001", SensorID: "gh-001-t", Type: telemetry.SensorTemperature, Value: v, MeasuredAt: t0.Add(at)}
}

func (h *harness) onTopic(topic string) []aggregation.Message {
	var out []aggregation.Message
	for _, m := range h.published {
		if m.Topic == topic {
			out = append(out, m)
		}
	}
	return out
}

func (h *harness) decodeAggregate(m aggregation.Message) contract.WindowAggregateRecord {
	h.t.Helper()
	var rec contract.WindowAggregateRecord
	if err := contract.NewDecoder(h.reg).Decode(context.Background(), m.Value, &rec); err != nil {
		h.t.Fatal(err)
	}
	return rec
}

func TestHandleBatchEmitsAggregateThenCheckpointsThenCommits(t *testing.T) {
	h := newHarness(t)
	batch := []aggregation.Record{
		h.record(0, temp("a", 20, 0)),
		h.record(1, temp("b", 24, 20*time.Second)),
		h.record(2, temp("c", 22, 40*time.Second)),
		h.record(3, temp("d", 23, 70*time.Second)), // fecha o primeiro minuto
	}
	if err := h.svc.HandleBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}

	aggs := h.onTopic(topics.Aggregates)
	if len(aggs) != 1 {
		t.Fatalf("agregados publicados = %d, quero 1", len(aggs))
	}
	got := h.decodeAggregate(aggs[0])
	if got.SampleCount != 3 || got.MinValue != 20 || got.MaxValue != 24 || got.MeanValue != 22 || !got.WindowStart.Equal(t0) || got.Unit != "celsius" {
		t.Fatalf("agregado = %+v", got)
	}
	if string(aggs[0].Key) != "gh-001" {
		t.Fatalf("chave = %q, a chave de partição é a estufa", aggs[0].Key)
	}
	if want := []string{"publish", "checkpoint", "commit"}; fmt.Sprint(h.calls) != fmt.Sprint(want) {
		t.Fatalf("ordem = %v, quero %v", h.calls, want)
	}
	// A leitura do offset 3 está numa janela aberta: o commit para nela.
	if h.commits[tp] != 3 || h.checkpoints[tp].Offset != 3 {
		t.Fatalf("commit = %d checkpoint = %+v, quero 3", h.commits[tp], h.checkpoints[tp])
	}
	if !h.checkpoints[tp].Watermark.Equal(t0.Add(65 * time.Second)) {
		t.Fatalf("watermark = %s", h.checkpoints[tp].Watermark)
	}
}

func TestHandleBatchPublishesAlertWhenLimitIsExceeded(t *testing.T) {
	h := newHarness(t)
	batch := []aggregation.Record{
		h.record(0, temp("a", 35, 0)),
		h.record(1, temp("b", 37, 30*time.Second)),
		h.record(2, temp("c", 25, 90*time.Second)),
	}
	if err := h.svc.HandleBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	alerts := h.onTopic(topics.Alerts)
	if len(alerts) != 1 {
		t.Fatalf("alertas = %d, quero 1", len(alerts))
	}
	var alert contract.ThresholdAlertRecord
	if err := contract.NewDecoder(h.reg).Decode(context.Background(), alerts[0].Value, &alert); err != nil {
		t.Fatal(err)
	}
	if alert.Rule != string(telemetry.RuleAboveMax) || alert.ObservedValue != 36 || alert.LimitValue != 32 || alert.AlertID == "" {
		t.Fatalf("alerta = %+v", alert)
	}
}

func TestInvalidMessagesGoToDLQAndDoNotBlockCommit(t *testing.T) {
	h := newHarness(t)
	outOfRange := temp("x", 150, 0)
	unknownSchema := contract.AppendFrame(nil, 99, []byte{0x02})
	batch := []aggregation.Record{
		{TopicPartition: tp, Offset: 0, Key: []byte("gh-001"), Value: []byte(`{"value": 21}`)},
		h.record(1, outOfRange),
		{TopicPartition: tp, Offset: 2, Key: []byte("gh-001"), Value: unknownSchema},
	}
	if err := h.svc.HandleBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	dlq := h.onTopic(topics.DLQ)
	if len(dlq) != 3 {
		t.Fatalf("DLQ = %d mensagens, quero 3", len(dlq))
	}
	wantReasons := []string{aggregation.ReasonMalformed, aggregation.ReasonInvalid, aggregation.ReasonUnknownSchema}
	for i, m := range dlq {
		if m.Headers[aggregation.HeaderReason] != wantReasons[i] {
			t.Errorf("mensagem %d: motivo %q, quero %q", i, m.Headers[aggregation.HeaderReason], wantReasons[i])
		}
		if m.Headers[aggregation.HeaderSourceOffset] != fmt.Sprint(i) || m.Headers[aggregation.HeaderSourceTopic] != tp.Topic {
			t.Errorf("mensagem %d sem origem: %v", i, m.Headers)
		}
		if string(m.Value) != string(batch[i].Value) {
			t.Errorf("mensagem %d: a DLQ deve guardar os bytes originais", i)
		}
	}
	if h.commits[tp] != 3 {
		t.Fatalf("commit = %d, quero 3 (nada aberto depois da DLQ)", h.commits[tp])
	}
}

func TestDuplicateDeliveryIsAggregatedOnce(t *testing.T) {
	h := newHarness(t)
	first := []aggregation.Record{h.record(0, temp("a", 20, 0)), h.record(1, temp("b", 30, 10*time.Second))}
	if err := h.svc.HandleBatch(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	// O gateway retransmite "b" e a leitura seguinte fecha a janela.
	second := []aggregation.Record{h.record(2, temp("b", 30, 10*time.Second)), h.record(3, temp("c", 22, 80*time.Second))}
	if err := h.svc.HandleBatch(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	aggs := h.onTopic(topics.Aggregates)
	if len(aggs) != 1 || h.decodeAggregate(aggs[0]).SampleCount != 2 {
		t.Fatalf("a duplicata foi contada: %+v", aggs)
	}
}

func TestPublishFailurePreventsCheckpointAndCommit(t *testing.T) {
	h := newHarness(t)
	h.publishErr = errors.New("NOT_ENOUGH_REPLICAS")
	err := h.svc.HandleBatch(context.Background(), []aggregation.Record{
		h.record(0, temp("a", 20, 0)), h.record(1, temp("b", 20, 70*time.Second)),
	})
	if !errors.Is(err, h.publishErr) {
		t.Fatalf("esperava o erro do publish, veio %v", err)
	}
	if fmt.Sprint(h.calls) != "[publish]" || len(h.commits) != 0 {
		t.Fatalf("não pode haver checkpoint nem commit depois de falha no publish: %v", h.calls)
	}
}

func TestCheckpointFailurePreventsCommit(t *testing.T) {
	h := newHarness(t)
	h.saveErr = errors.New("checkpoint topic offline")
	err := h.svc.HandleBatch(context.Background(), []aggregation.Record{h.record(0, temp("a", 20, 0))})
	if !errors.Is(err, h.saveErr) || len(h.commits) != 0 {
		t.Fatalf("err = %v commits = %v", err, h.commits)
	}
}

func TestTransientRegistryErrorIsRetriedNotDeadLettered(t *testing.T) {
	h := newHarness(t)
	h.reg.failuresLeft = 3
	if err := h.svc.HandleBatch(context.Background(), []aggregation.Record{h.record(0, temp("a", 20, 0))}); err != nil {
		t.Fatal(err)
	}
	if len(h.onTopic(topics.DLQ)) != 0 {
		t.Fatal("indisponibilidade do registry não pode mandar mensagem válida para a DLQ")
	}
}

func TestCancelledContextDuringRetryStopsWithoutCommit(t *testing.T) {
	h := newHarness(t)
	h.reg.failuresLeft = 1 << 30
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := h.svc.HandleBatch(ctx, []aggregation.Record{h.record(0, temp("a", 20, 0))})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("esperava DeadlineExceeded, veio %v", err)
	}
	if len(h.calls) != 0 {
		t.Fatalf("nada deveria ser publicado nem commitado: %v", h.calls)
	}
}

// Simula um crash: uma nova instância assume a partição a partir do offset
// commitado e do checkpoint. A janela já emitida não pode ser reemitida
// parcialmente e a janela aberta precisa sair completa.
func TestRestartFromCheckpointDoesNotReemitClosedWindows(t *testing.T) {
	h := newHarness(t)
	all := []aggregation.Record{
		h.record(0, temp("a", 20, 0)),
		h.record(1, temp("b", 26, 62*time.Second)),
		h.record(2, temp("c", 22, 66*time.Second)), // fecha o minuto 0
		h.record(3, temp("d", 24, 58*time.Second)), // atrasada demais: descartada
		h.record(4, temp("e", 28, 80*time.Second)),
	}
	if err := h.svc.HandleBatch(context.Background(), all[:3]); err != nil {
		t.Fatal(err)
	}
	committed := h.commits[tp]
	if committed != 1 {
		t.Fatalf("commit = %d, quero 1", committed)
	}

	// Nova instância, mesmo checkpoint store; reprocessa do offset commitado.
	h.published = nil
	restarted := h.newService()
	if err := restarted.Assign(context.Background(), []aggregation.TopicPartition{tp}); err != nil {
		t.Fatal(err)
	}
	replay := slices.Concat(all[committed:], []aggregation.Record{h.record(5, temp("f", 21, 130*time.Second))})
	if err := restarted.HandleBatch(context.Background(), replay); err != nil {
		t.Fatal(err)
	}
	aggs := h.onTopic(topics.Aggregates)
	if len(aggs) != 1 {
		t.Fatalf("agregados no replay = %d, quero só a janela do minuto 1", len(aggs))
	}
	got := h.decodeAggregate(aggs[0])
	if !got.WindowStart.Equal(t0.Add(time.Minute)) || got.SampleCount != 3 || got.MeanValue != 76.0/3 {
		t.Fatalf("janela reconstruída = %+v", got)
	}
}

func TestRevokeCommitsAndDropsState(t *testing.T) {
	h := newHarness(t)
	if err := h.svc.HandleBatch(context.Background(), []aggregation.Record{h.record(0, temp("a", 20, 0)), h.record(1, temp("b", 20, 70*time.Second))}); err != nil {
		t.Fatal(err)
	}
	h.calls = nil
	if err := h.svc.Revoke(context.Background(), []aggregation.TopicPartition{tp}); err != nil {
		t.Fatal(err)
	}
	if len(h.calls) != 0 {
		t.Fatalf("offset não mudou desde o último commit, revoke não deveria recommitar: %v", h.calls)
	}
	// Depois do revoke, a partição recomeça do zero no offset recebido.
	if err := h.svc.HandleBatch(context.Background(), []aggregation.Record{h.record(1, temp("b", 20, 70*time.Second))}); err != nil {
		t.Fatal(err)
	}
	if h.commits[tp] != 1 {
		t.Fatalf("commit = %d, quero 1", h.commits[tp])
	}
}

func TestNewRequiresDependencies(t *testing.T) {
	_, err := aggregation.New(telemetry.WindowConfig{Size: time.Minute}, nil, topics, aggregation.Deps{})
	if err == nil {
		t.Fatal("esperava erro por dependências ausentes")
	}
}

type stateStore struct {
	states []aggregation.SensorState
	err    error
}

func (s *stateStore) Update(_ context.Context, states []aggregation.SensorState) error {
	s.states = append(s.states, states...)
	return s.err
}

func TestStateProjectionIsUpdatedAndNeverBlocksTheCommit(t *testing.T) {
	for name, storeErr := range map[string]error{"ok": nil, "redis fora do ar": errors.New("redis fora do ar")} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			store := &stateStore{err: storeErr}
			svc, err := aggregation.New(
				telemetry.WindowConfig{Size: time.Minute, AllowedLateness: 5 * time.Second},
				telemetry.DefaultThresholds(), topics,
				aggregation.Deps{
					Decoder: contract.NewDecoder(h.reg), AggregateCodec: mustEncoder(t, aggregateSchemaID, schemas.TopicAggregates),
					AlertCodec: mustEncoder(t, alertSchemaID, schemas.TopicAlerts), Publisher: h, Checkpoints: h, Committer: h,
					State: store, Clock: func() time.Time { return t0.Add(10 * time.Minute) },
				})
			if err != nil {
				t.Fatal(err)
			}
			batch := []aggregation.Record{
				h.record(0, temp("a", 34, 0)),
				h.record(1, temp("b", 36, 20*time.Second)),
				h.record(2, temp("c", 22, 70*time.Second)),
			}
			if err := svc.HandleBatch(context.Background(), batch); err != nil {
				t.Fatal(err)
			}
			if len(store.states) != 1 || store.states[0].Aggregate.Mean != 35 || store.states[0].Alert == nil {
				t.Fatalf("estados = %+v, quero a janela fechada com alerta", store.states)
			}
			if len(h.commits) != 1 {
				t.Fatalf("commit = %v; a projeção não pode impedir o commit", h.commits)
			}
		})
	}
}

func mustEncoder(t *testing.T, id int, topic string) *contract.Encoder {
	t.Helper()
	enc, err := contract.NewEncoder(id, latest(t, topic))
	if err != nil {
		t.Fatal(err)
	}
	return enc
}
