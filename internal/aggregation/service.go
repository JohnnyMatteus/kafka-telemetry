package aggregation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strconv"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

// Motivos de envio para a DLQ, usados em header e métrica.
const (
	ReasonMalformed     = "malformed"
	ReasonUnknownSchema = "unknown_schema"
	ReasonInvalid       = "invalid_reading"
)

// Headers adicionados às mensagens da DLQ.
const (
	HeaderReason          = "dlq.reason"
	HeaderError           = "dlq.error"
	HeaderSourceTopic     = "dlq.source.topic"
	HeaderSourcePartition = "dlq.source.partition"
	HeaderSourceOffset    = "dlq.source.offset"
)

// Topics são os tópicos de saída.
type Topics struct {
	Aggregates string
	Alerts     string
	DLQ        string
}

// Deps reúne as dependências do serviço.
type Deps struct {
	Decoder         Decoder
	AggregateCodec  Encoder
	AlertCodec      Encoder
	Publisher       Publisher
	Checkpoints     CheckpointStore
	Committer       Committer
	State           StateStore
	Observer        Observer
	Logger          *slog.Logger
	Clock           func() time.Time
	RetryBackoff    time.Duration
	MaxRetryBackoff time.Duration
}

// Service implementa o pipeline de agregação de uma instância do consumidor.
// Não é seguro para uso concorrente: o adaptador Kafka chama Assign, Revoke
// e HandleBatch a partir do mesmo laço de poll.
type Service struct {
	window     telemetry.WindowConfig
	thresholds telemetry.Thresholds
	topics     Topics
	deps       Deps
	partitions map[TopicPartition]*telemetry.PartitionWindows
	restored   map[TopicPartition]Checkpoint
	committed  map[TopicPartition]int64
	// checkpointed guarda os watermarks do último checkpoint gravado por partição.
	checkpointed map[TopicPartition]map[string]time.Time
}

// New cria o serviço.
func New(window telemetry.WindowConfig, thresholds telemetry.Thresholds, topics Topics, deps Deps) (*Service, error) {
	if err := window.Validate(); err != nil {
		return nil, err
	}
	if deps.Decoder == nil || deps.AggregateCodec == nil || deps.AlertCodec == nil ||
		deps.Publisher == nil || deps.Checkpoints == nil || deps.Committer == nil {
		return nil, errors.New("aggregation: dependência obrigatória ausente")
	}
	if deps.Observer == nil {
		deps.Observer = NopObserver{}
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	if deps.Clock == nil {
		deps.Clock = time.Now
	}
	if deps.RetryBackoff <= 0 {
		deps.RetryBackoff = 100 * time.Millisecond
	}
	if deps.MaxRetryBackoff < deps.RetryBackoff {
		deps.MaxRetryBackoff = 5 * time.Second
	}
	return &Service{
		window:     window,
		thresholds: thresholds,
		topics:     topics,
		deps:       deps,
		partitions: make(map[TopicPartition]*telemetry.PartitionWindows),
		restored:   make(map[TopicPartition]Checkpoint),
		committed:  make(map[TopicPartition]int64),

		checkpointed: make(map[TopicPartition]map[string]time.Time),
	}, nil
}

// Assign carrega os checkpoints das partições recebidas num rebalance.
// O checkpoint é relido a cada rebalance porque outra instância pode tê-lo
// atualizado desde a última leitura.
func (s *Service) Assign(ctx context.Context, tps []TopicPartition) error {
	cps, err := s.deps.Checkpoints.Load(ctx)
	if err != nil {
		return fmt.Errorf("carregar checkpoints: %w", err)
	}
	for _, tp := range tps {
		delete(s.partitions, tp)
		if cp, ok := cps[tp]; ok {
			s.restored[tp] = cp
		} else {
			delete(s.restored, tp)
		}
		s.deps.Logger.Info("partição atribuída", "topic", tp.Topic, "partition", tp.Partition,
			"checkpoint_offset", cps[tp].Offset, "checkpoint_watermark", cps[tp].Watermark)
	}
	return nil
}

// Revoke commita o que for seguro nas partições revogadas e descarta o
// estado delas. Janelas abertas não são emitidas: o novo dono vai
// reconstruí-las a partir do offset commitado.
func (s *Service) Revoke(ctx context.Context, tps []TopicPartition) error {
	err := s.checkpointAndCommit(ctx, tps)
	s.Drop(tps)
	return err
}

// Drop descarta o estado sem commitar (partições perdidas, sem posse garantida).
func (s *Service) Drop(tps []TopicPartition) {
	for _, tp := range tps {
		delete(s.partitions, tp)
		delete(s.restored, tp)
		delete(s.committed, tp)
		delete(s.checkpointed, tp)
	}
}

// HandleBatch processa um lote de registros. A ordem é o que dá a garantia:
// publicar DLQ, agregados e alertas (com ack) -> gravar checkpoint -> commitar.
// A projeção do último estado é atualizada depois do publish e não participa
// da garantia.
// Qualquer erro devolvido deixa o offset sem commit; o processo deve parar e
// o próximo dono da partição reprocessa a partir do último commit.
func (s *Service) HandleBatch(ctx context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	var out []Message
	touched := make(map[TopicPartition]struct{})
	for _, rec := range records {
		p, err := s.partition(rec)
		if err != nil {
			return err
		}
		touched[rec.TopicPartition] = struct{}{}
		msg, err := s.process(ctx, p, rec)
		if err != nil {
			return err
		}
		if msg != nil {
			out = append(out, *msg)
		}
	}

	tps := make([]TopicPartition, 0, len(touched))
	var states []SensorState
	for tp := range touched {
		tps = append(tps, tp)
		closed, err := s.emitAll(s.partitions[tp].CloseReady())
		if err != nil {
			return err
		}
		out = append(out, closed.msgs...)
		states = append(states, closed.states...)
	}
	return s.flush(ctx, out, states, tps)
}

// emitted reúne o que sai de um conjunto de janelas fechadas.
type emitted struct {
	msgs   []Message
	states []SensorState
}

func (s *Service) emitAll(aggs []telemetry.Aggregate) (emitted, error) {
	var e emitted
	for _, agg := range aggs {
		msgs, state, err := s.emit(agg)
		if err != nil {
			return emitted{}, err
		}
		e.msgs = append(e.msgs, msgs...)
		e.states = append(e.states, state)
	}
	return e, nil
}

// flush é o trecho que dá a garantia de entrega e que HandleBatch e Tick
// compartilham: publica (com ack), atualiza a projeção, grava o checkpoint e
// só então commita.
func (s *Service) flush(ctx context.Context, out []Message, states []SensorState, tps []TopicPartition) error {
	alerts := 0
	for _, st := range states {
		if st.Alert != nil {
			alerts++
		}
	}
	if len(out) > 0 {
		if err := s.deps.Publisher.Publish(ctx, out); err != nil {
			return fmt.Errorf("publicar %d mensagens: %w", len(out), err)
		}
	}
	s.deps.Observer.Emitted(len(states), alerts)
	s.updateState(ctx, states)
	return s.checkpointAndCommit(ctx, tps)
}

// Tick fecha as janelas de chaves ociosas (watermark ocioso, WindowConfig.
// IdleTimeout) quando nenhum registro chega. Roda no mesmo laço de poll que
// HandleBatch e segue a mesma ordem: publicar, checkpoint, commit.
func (s *Service) Tick(ctx context.Context) error {
	if s.window.IdleTimeout <= 0 {
		return nil
	}
	now := s.deps.Clock()
	var out []Message
	var states []SensorState
	var tps []TopicPartition
	for tp, p := range s.partitions {
		closed := p.CloseIdle(now)
		if len(closed) == 0 {
			continue
		}
		e, err := s.emitAll(closed)
		if err != nil {
			return err
		}
		tps = append(tps, tp)
		out = append(out, e.msgs...)
		states = append(states, e.states...)
	}
	if len(states) == 0 {
		return nil
	}
	s.deps.Observer.IdleClosed(len(states))
	s.deps.Logger.Info("janelas fechadas por ociosidade", "windows", len(states), "partitions", len(tps))
	return s.flush(ctx, out, states, tps)
}

func (s *Service) partition(rec Record) (*telemetry.PartitionWindows, error) {
	if p, ok := s.partitions[rec.TopicPartition]; ok {
		return p, nil
	}
	var watermark time.Time
	if cp, ok := s.restored[rec.TopicPartition]; ok {
		watermark = cp.Watermark
	}
	p, err := telemetry.NewPartitionWindows(s.window, watermark, rec.Offset)
	if err != nil {
		return nil, err
	}
	if cp, ok := s.restored[rec.TopicPartition]; ok && len(cp.Watermarks) > 0 {
		p.RestoreWatermarks(cp.Watermarks)
	}
	s.partitions[rec.TopicPartition] = p
	return p, nil
}

// process trata um registro e devolve a mensagem de DLQ, se houver.
func (s *Service) process(ctx context.Context, p *telemetry.PartitionWindows, rec Record) (*Message, error) {
	var payload contract.SensorReadingRecord
	if err := s.decodeWithRetry(ctx, rec.Value, &payload); err != nil {
		reason := ReasonMalformed
		switch {
		case errors.Is(err, contract.ErrUnknownSchema):
			reason = ReasonUnknownSchema
		case errors.Is(err, contract.ErrMalformed):
		default:
			return nil, err // contexto cancelado durante o retry
		}
		p.Skip(rec.Offset)
		return s.deadLetter(rec, reason, err), nil
	}

	reading := payload.Reading()
	now := s.deps.Clock()
	if err := reading.Validate(now); err != nil {
		p.Skip(rec.Offset)
		return s.deadLetter(rec, ReasonInvalid, err), nil
	}

	outcome := p.AddAt(reading, rec.Offset, now)
	s.deps.Observer.RecordProcessed(string(outcome))
	if outcome == telemetry.OutcomeAccepted {
		s.deps.Observer.EndToEndLatency(now.Sub(reading.MeasuredAt))
	}
	return nil, nil
}

// decodeWithRetry insiste enquanto o erro for transitório (registry fora do
// ar). Mandar para a DLQ por causa de uma indisponibilidade descartaria
// mensagens válidas; bloquear a partição é o comportamento correto.
func (s *Service) decodeWithRetry(ctx context.Context, b []byte, v *contract.SensorReadingRecord) error {
	backoff := s.deps.RetryBackoff
	for {
		err := s.deps.Decoder.Decode(ctx, b, v)
		if err == nil || errors.Is(err, contract.ErrMalformed) || errors.Is(err, contract.ErrUnknownSchema) {
			return err
		}
		s.deps.Logger.Warn("erro transitório ao decodificar, tentando de novo", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return fmt.Errorf("decodificação interrompida: %w", errors.Join(ctx.Err(), err))
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, s.deps.MaxRetryBackoff)
	}
}

func (s *Service) deadLetter(rec Record, reason string, cause error) *Message {
	s.deps.Observer.DeadLettered(reason)
	s.deps.Observer.RecordProcessed("dead_lettered")
	s.deps.Logger.Warn("mensagem enviada para a DLQ", "reason", reason, "error", cause,
		"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset)
	return &Message{
		Topic: s.topics.DLQ,
		Key:   rec.Key,
		Value: rec.Value,
		Headers: map[string]string{
			HeaderReason:          reason,
			HeaderError:           cause.Error(),
			HeaderSourceTopic:     rec.Topic,
			HeaderSourcePartition: strconv.Itoa(int(rec.Partition)),
			HeaderSourceOffset:    strconv.FormatInt(rec.Offset, 10),
		},
	}
}

// updateState atualiza a projeção depois do publish. Uma falha só é
// registrada: a projeção se corrige na próxima janela, e travar o pipeline
// por ela atrasaria agregados, alertas e o commit.
func (s *Service) updateState(ctx context.Context, states []SensorState) {
	if s.deps.State == nil || len(states) == 0 {
		return
	}
	if err := s.deps.State.Update(ctx, states); err != nil {
		s.deps.Logger.Warn("falha ao atualizar o último estado das estufas", "states", len(states), "error", err)
	}
}

func (s *Service) emit(agg telemetry.Aggregate) ([]Message, SensorState, error) {
	state := SensorState{Aggregate: agg}
	key := []byte(agg.Key.GreenhouseID)
	value, err := s.deps.AggregateCodec.Encode(contract.NewWindowAggregateRecord(agg))
	if err != nil {
		return nil, state, fmt.Errorf("serializar agregado: %w", err)
	}
	msgs := []Message{{Topic: s.topics.Aggregates, Key: key, Value: value}}
	alert, ok := s.thresholds.Evaluate(agg)
	if !ok {
		return msgs, state, nil
	}
	state.Alert = &alert
	value, err = s.deps.AlertCodec.Encode(contract.NewThresholdAlertRecord(alert))
	if err != nil {
		return nil, state, fmt.Errorf("serializar alerta: %w", err)
	}
	s.deps.Logger.Info("limite ultrapassado", "greenhouse_id", alert.GreenhouseID, "sensor_type", alert.Type,
		"rule", alert.Rule, "observed", alert.Observed, "limit", alert.Limit, "window_start", alert.WindowStart)
	return append(msgs, Message{Topic: s.topics.Alerts, Key: key, Value: value}), state, nil
}

// checkpointAndCommit grava o checkpoint quando o offset seguro OU qualquer
// watermark mudou, e commita só quando o offset mudou. Com watermark por
// estufa, uma estufa com janela aberta segura o offset enquanto as outras
// fecham janelas e avançam os próprios watermarks; sem gravá-los, um crash
// releria do offset antigo e republicaria janelas já emitidas.
func (s *Service) checkpointAndCommit(ctx context.Context, tps []TopicPartition) error {
	var cps []Checkpoint
	offsets := make(map[TopicPartition]int64)
	for _, tp := range tps {
		p, ok := s.partitions[tp]
		if !ok {
			continue
		}
		offset := p.CommitOffset()
		wms := p.Watermarks()
		s.deps.Observer.PartitionState(tp, p.OpenWindows(), p.TrackedEvents(), p.Watermark())
		last, committed := s.committed[tp]
		offsetChanged := !committed || last != offset
		if !offsetChanged && maps.Equal(s.checkpointed[tp], wms) {
			continue
		}
		cps = append(cps, Checkpoint{TopicPartition: tp, Offset: offset, Watermark: p.Watermark(), Watermarks: wms})
		if offsetChanged {
			offsets[tp] = offset
		}
	}
	if len(cps) == 0 {
		return nil
	}
	if err := s.deps.Checkpoints.Save(ctx, cps); err != nil {
		return fmt.Errorf("gravar checkpoint: %w", err)
	}
	for _, cp := range cps {
		s.checkpointed[cp.TopicPartition] = cp.Watermarks
	}
	if len(offsets) == 0 {
		return nil
	}
	if err := s.deps.Committer.Commit(ctx, offsets); err != nil {
		return fmt.Errorf("commitar offsets: %w", err)
	}
	for tp, off := range offsets {
		s.committed[tp] = off
		s.deps.Observer.Committed(tp, off)
	}
	return nil
}
