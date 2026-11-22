package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Job é uma entrega que falhou por motivo transitório e espera nova tentativa.
type Job struct {
	Key         string `json:"key"`
	Alert       Alert  `json:"alert"`
	Channel     string `json:"channel"`
	RecipientID string `json:"recipient_id"`
	Attempts    int    `json:"attempts"`
	// Retries conta as tentativas agendadas já feitas; define o backoff.
	Retries       int       `json:"retries"`
	FirstFailedAt time.Time `json:"first_failed_at"`
	LastError     string    `json:"last_error"`
}

// QueueStats descreve a fila, para métrica e alerta.
type QueueStats struct {
	Depth     int64
	OldestAge time.Duration
}

// Retries é a fila durável e agendada de retries. Due devolve os jobs
// vencidos e os esconde por lease: se o processo cair no meio, eles voltam a
// ficar visíveis quando o lease acabar.
type Retries interface {
	Enqueue(ctx context.Context, j Job, at time.Time) error
	Due(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]Job, error)
	Reschedule(ctx context.Context, j Job, at time.Time) error
	Complete(ctx context.Context, key string) error
	Stats(ctx context.Context, now time.Time) (QueueStats, error)
}

// RetryPlan define a agenda dos retries duráveis.
type RetryPlan struct {
	// Base e Cap limitam o backoff exponencial entre tentativas agendadas.
	Base time.Duration
	Cap  time.Duration
	// MaxAge é a idade máxima de um aviso na fila; depois dela ele expira
	// (métrica e log de erro): avisar de um alerta velho demais confunde.
	MaxAge time.Duration
	// Lease é por quanto tempo um job fica reservado a quem o pegou.
	Lease time.Duration
}

func (p RetryPlan) withDefaults() RetryPlan {
	if p.Base <= 0 {
		p.Base = 30 * time.Second
	}
	if p.Cap < p.Base {
		p.Cap = max(5*time.Minute, p.Base)
	}
	if p.MaxAge <= 0 {
		p.MaxAge = 24 * time.Hour
	}
	if p.Lease <= 0 {
		p.Lease = 2 * time.Minute
	}
	return p
}

// delay é o intervalo antes da tentativa seguinte, dado o total já feito.
func (p RetryPlan) delay(attempts int) time.Duration {
	d := p.Base
	for i := 1; i < attempts && d < p.Cap; i++ {
		d *= 2
	}
	return min(d, p.Cap)
}

// ProcessDue faz uma tentativa de cada retry vencido. Sucesso conclui o job;
// falha permanente o descarta; falha transitória o reagenda, até MaxAge.
// Só erro de infraestrutura volta.
func (s *Service) ProcessDue(ctx context.Context) error {
	now := s.deps.Clock()
	jobs, err := s.deps.Retries.Due(ctx, now, 100, s.deps.RetryPlan.Lease)
	if err != nil {
		return fmt.Errorf("retries vencidos: %w", err)
	}
	for _, j := range jobs {
		if err := s.retryOne(ctx, j); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) retryOne(ctx context.Context, j Job) error {
	if done, err := s.deps.Sent.Has(ctx, j.Key); err != nil {
		return fmt.Errorf("consultar avisos enviados: %w", err)
	} else if done {
		return s.deps.Retries.Complete(ctx, j.Key)
	}
	ch := s.channel(j.Channel)
	rec, found, err := s.recipient(ctx, j)
	if err != nil {
		return err
	}
	if ch == nil || !found {
		s.deps.Logger.Warn("retry descartado: canal ou destinatário não existe mais", "key", j.Key)
		return s.deps.Retries.Complete(ctx, j.Key)
	}
	sendErr := ch.Send(ctx, rec, Compose(j.Alert))
	now := s.deps.Clock()
	switch {
	case sendErr == nil:
		s.deps.Observer.Notified(j.Channel, OutcomeSent)
		if err := s.deps.Sent.Mark(ctx, j.Key); err != nil {
			return fmt.Errorf("registrar aviso enviado: %w", err)
		}
		return s.deps.Retries.Complete(ctx, j.Key)
	case ctx.Err() != nil:
		return fmt.Errorf("retry interrompido: %w", errors.Join(ctx.Err(), sendErr))
	case errors.Is(sendErr, ErrPermanent):
		s.deps.Observer.Notified(j.Channel, OutcomePermanent)
		s.deps.Logger.Error("aviso recusado em definitivo no retry", "alert_id", j.Alert.ID, "channel", j.Channel, "recipient", j.RecipientID, "error", sendErr)
		return s.deps.Retries.Complete(ctx, j.Key)
	case now.Sub(j.FirstFailedAt) >= s.deps.RetryPlan.MaxAge:
		s.deps.Observer.Notified(j.Channel, OutcomeExpired)
		s.deps.Logger.Error("aviso expirou sem ser entregue", "alert_id", j.Alert.ID, "channel", j.Channel, "recipient", j.RecipientID,
			"age", now.Sub(j.FirstFailedAt).String(), "error", sendErr)
		return s.deps.Retries.Complete(ctx, j.Key)
	}
	j.Attempts++
	j.Retries++
	j.LastError = sendErr.Error()
	s.deps.Observer.Notified(j.Channel, OutcomeFailed)
	return s.deps.Retries.Reschedule(ctx, j, now.Add(s.deps.RetryPlan.delay(j.Retries+1)))
}

func (s *Service) channel(name string) Channel {
	for _, c := range s.deps.Channels {
		if c.Name() == name {
			return c
		}
	}
	return nil
}

func (s *Service) recipient(ctx context.Context, j Job) (Recipient, bool, error) {
	rs, err := s.deps.Directory.Lookup(ctx, j.Alert.GreenhouseID)
	if err != nil {
		return Recipient{}, false, fmt.Errorf("diretório de destinatários: %w", err)
	}
	for _, r := range rs {
		if r.ID == j.RecipientID {
			return r, true, nil
		}
	}
	return Recipient{}, false, nil
}

// StatsObserver recebe o retrato da fila a cada ciclo do worker.
type StatsObserver interface {
	QueueStats(QueueStats)
}

// RunRetries processa os retries vencidos a cada interval até o contexto
// acabar. Erro de infraestrutura é registrado e tentado no próximo ciclo: o
// worker nunca derruba o consumo.
func (s *Service) RunRetries(ctx context.Context, interval time.Duration, stats StatsObserver) error {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if err := s.ProcessDue(ctx); err != nil && ctx.Err() == nil {
			s.deps.Logger.Error("ciclo de retries falhou", "error", err)
		}
		if st, err := s.deps.Retries.Stats(ctx, s.deps.Clock()); err == nil && stats != nil {
			stats.QueueStats(st)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

const (
	retryZSet = "notify:v1:retry:due"
	retryJobs = "notify:v1:retry:jobs"
)

// Move o vencimento dos jobs vencidos para now+lease e os devolve: um worker
// que cai deixa os jobs voltarem sozinhos.
var leaseDue = redis.NewScript(`
local keys = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[3])
for _, k in ipairs(keys) do redis.call('ZADD', KEYS[1], ARGV[2], k) end
return keys
`)

// RedisRetries implementa Retries: um zset de vencimentos e um hash de jobs.
type RedisRetries struct{ client redis.UniversalClient }

// NewRedisRetries usa um cliente já configurado.
func NewRedisRetries(client redis.UniversalClient) *RedisRetries {
	return &RedisRetries{client: client}
}

func score(t time.Time) float64 { return float64(t.UnixMilli()) }

func (r *RedisRetries) Enqueue(ctx context.Context, j Job, at time.Time) error {
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	pipe := r.client.TxPipeline()
	pipe.HSetNX(ctx, retryJobs, j.Key, b)
	pipe.ZAddNX(ctx, retryZSet, redis.Z{Score: score(at), Member: j.Key})
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisRetries) Reschedule(ctx context.Context, j Job, at time.Time) error {
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	pipe := r.client.TxPipeline()
	pipe.HSet(ctx, retryJobs, j.Key, b)
	pipe.ZAdd(ctx, retryZSet, redis.Z{Score: score(at), Member: j.Key})
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisRetries) Complete(ctx context.Context, key string) error {
	pipe := r.client.TxPipeline()
	pipe.ZRem(ctx, retryZSet, key)
	pipe.HDel(ctx, retryJobs, key)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisRetries) Due(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]Job, error) {
	keys, err := leaseDue.Run(ctx, r.client, []string{retryZSet}, score(now), score(now.Add(lease)), limit).StringSlice()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	var out []Job
	for _, k := range keys {
		raw, err := r.client.HGet(ctx, retryJobs, k).Bytes()
		if errors.Is(err, redis.Nil) {
			_ = r.client.ZRem(ctx, retryZSet, k).Err()
			continue
		}
		if err != nil {
			return nil, err
		}
		var j Job
		if err := json.Unmarshal(raw, &j); err != nil {
			return nil, fmt.Errorf("job %s: %w", k, err)
		}
		out = append(out, j)
	}
	return out, nil
}

func (r *RedisRetries) Stats(ctx context.Context, now time.Time) (QueueStats, error) {
	depth, err := r.client.HLen(ctx, retryJobs).Result()
	if err != nil || depth == 0 {
		return QueueStats{}, err
	}
	var oldest time.Time
	vals, err := r.client.HVals(ctx, retryJobs).Result()
	if err != nil {
		return QueueStats{}, err
	}
	for _, v := range vals {
		var j Job
		if json.Unmarshal([]byte(v), &j) == nil && (oldest.IsZero() || j.FirstFailedAt.Before(oldest)) {
			oldest = j.FirstFailedAt
		}
	}
	age := time.Duration(0)
	if !oldest.IsZero() {
		age = now.Sub(oldest)
	}
	return QueueStats{Depth: depth, OldestAge: age}, nil
}
