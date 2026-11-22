// Package notify avisa o produtor rural quando um alerta de limite é emitido.
// Consome o tópico de alertas, resolve quem recebe cada estufa e entrega por
// portas de canal (e-mail, webhook assinado), com idempotência por
// alerta+canal+destinatário e retry com backoff.
//
// Entrega é at-least-once: o offset só é commitado depois de todos os canais
// terem sido tentados, e o registro de avisos enviados no Redis evita o aviso
// repetido quando o mesmo alerta chega de novo (restart do agregador, rebalance).
package notify

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

// Resultados registrados por canal.
const (
	OutcomeSent       = "sent"
	OutcomeDuplicate  = "duplicate"
	OutcomeFailed     = "failed"
	OutcomePermanent  = "permanent_failure"
	OutcomeNoContact  = "no_recipient"
	OutcomeUndecoded  = "undecodable"
	keyPrefix         = "notify:v1:"
	defaultAttempts   = 5
	defaultBackoff    = 200 * time.Millisecond
	defaultMaxBackoff = 5 * time.Second
)

// ErrPermanent marca uma falha que repetir não resolve (endereço recusado,
// 4xx do webhook): o canal desiste sem esgotar as tentativas.
var ErrPermanent = errors.New("falha permanente de entrega")

// Alert é o alerta recebido do tópico.
type Alert struct {
	ID           string
	GreenhouseID string
	SensorType   telemetry.SensorType
	Rule         telemetry.AlertRule
	WindowStart  time.Time
	WindowEnd    time.Time
	Observed     float64
	Limit        float64
}

// Recipient é um produtor rural com seus contatos.
type Recipient struct {
	ID         string
	Name       string
	Email      string
	WebhookURL string
}

// Directory diz quem recebe os alertas de uma estufa.
type Directory interface {
	Lookup(ctx context.Context, greenhouseID string) ([]Recipient, error)
}

// Message é o aviso pronto para um canal.
type Message struct {
	Alert   Alert
	Subject string
	Body    string
}

// Channel entrega um aviso. Address devolve o endereço do destinatário nesse
// canal, vazio se ele não o usa.
type Channel interface {
	Name() string
	Address(r Recipient) string
	Send(ctx context.Context, to string, m Message) error
}

// Sent lembra, de forma durável, quais avisos já saíram. Só marca depois da
// entrega: uma reserva antes dela perderia o aviso se o processo caísse no
// meio; o custo de marcar depois é um aviso repetido nessa janela rara.
type Sent interface {
	Has(ctx context.Context, key string) (bool, error)
	Mark(ctx context.Context, key string) error
}

// Observer recebe o resultado de cada entrega, para métricas.
type Observer interface {
	Notified(channel, outcome string)
}

// NopObserver descarta os eventos.
type NopObserver struct{}

func (NopObserver) Notified(string, string) {}

// Retry define as tentativas por canal.
type Retry struct {
	Attempts   int
	Backoff    time.Duration
	MaxBackoff time.Duration
}

// Deps reúne as dependências do serviço.
type Deps struct {
	Channels  []Channel
	Directory Directory
	Sent      Sent
	Observer  Observer
	Logger    *slog.Logger
	Retry     Retry
	// Wait espera entre tentativas; o padrão respeita o contexto.
	Wait func(ctx context.Context, d time.Duration) error
}

// Service entrega os avisos de um alerta.
type Service struct{ deps Deps }

// New valida as dependências.
func New(deps Deps) (*Service, error) {
	if len(deps.Channels) == 0 || deps.Directory == nil || deps.Sent == nil {
		return nil, errors.New("notify: canais, diretório e registro de avisos são obrigatórios")
	}
	if deps.Observer == nil {
		deps.Observer = NopObserver{}
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	if deps.Retry.Attempts <= 0 {
		deps.Retry.Attempts = defaultAttempts
	}
	if deps.Retry.Backoff <= 0 {
		deps.Retry.Backoff = defaultBackoff
	}
	if deps.Retry.MaxBackoff < deps.Retry.Backoff {
		deps.Retry.MaxBackoff = max(defaultMaxBackoff, deps.Retry.Backoff)
	}
	if deps.Wait == nil {
		deps.Wait = wait
	}
	return &Service{deps: deps}, nil
}

func wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Notify avisa todos os destinatários da estufa por todos os canais que eles
// usam. Falha de entrega é registrada e não derruba o consumo: o alerta
// continua na API e no painel. Só um erro de infraestrutura (Redis, diretório,
// contexto cancelado) volta, e então o offset não é commitado.
func (s *Service) Notify(ctx context.Context, a Alert) error {
	recipients, err := s.deps.Directory.Lookup(ctx, a.GreenhouseID)
	if err != nil {
		return fmt.Errorf("diretório de destinatários: %w", err)
	}
	if len(recipients) == 0 {
		s.deps.Observer.Notified("none", OutcomeNoContact)
		s.deps.Logger.Warn("alerta sem destinatário", "alert_id", a.ID, "greenhouse_id", a.GreenhouseID)
		return nil
	}
	msg := Compose(a)
	for _, r := range recipients {
		for _, ch := range s.deps.Channels {
			to := ch.Address(r)
			if to == "" {
				continue
			}
			if err := s.deliver(ctx, ch, r, to, msg); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Service) deliver(ctx context.Context, ch Channel, r Recipient, to string, m Message) error {
	key := keyPrefix + m.Alert.ID + ":" + ch.Name() + ":" + r.ID
	done, err := s.deps.Sent.Has(ctx, key)
	if err != nil {
		return fmt.Errorf("consultar avisos enviados: %w", err)
	}
	if done {
		s.deps.Observer.Notified(ch.Name(), OutcomeDuplicate)
		return nil
	}
	sendErr := s.sendWithRetry(ctx, ch, to, m)
	if sendErr == nil {
		s.deps.Observer.Notified(ch.Name(), OutcomeSent)
		if err := s.deps.Sent.Mark(ctx, key); err != nil {
			return fmt.Errorf("registrar aviso enviado: %w", err)
		}
		return nil
	}
	if ctx.Err() != nil {
		return fmt.Errorf("aviso interrompido: %w", errors.Join(ctx.Err(), sendErr))
	}
	outcome := OutcomeFailed
	if errors.Is(sendErr, ErrPermanent) {
		outcome = OutcomePermanent
	}
	s.deps.Observer.Notified(ch.Name(), outcome)
	s.deps.Logger.Error("aviso não entregue", "alert_id", m.Alert.ID, "channel", ch.Name(), "recipient", r.ID, "outcome", outcome, "error", sendErr)
	return nil
}

func (s *Service) sendWithRetry(ctx context.Context, ch Channel, to string, m Message) error {
	backoff := s.deps.Retry.Backoff
	var err error
	for attempt := 1; attempt <= s.deps.Retry.Attempts; attempt++ {
		if err = ch.Send(ctx, to, m); err == nil || errors.Is(err, ErrPermanent) || ctx.Err() != nil {
			return err
		}
		s.deps.Logger.Warn("falha ao avisar, tentando de novo", "channel", ch.Name(), "attempt", attempt, "error", err)
		if attempt == s.deps.Retry.Attempts {
			break
		}
		if werr := s.deps.Wait(ctx, backoff); werr != nil {
			return errors.Join(werr, err)
		}
		backoff = min(backoff*2, s.deps.Retry.MaxBackoff)
	}
	return err
}
