package notify

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

// Consumer liga o tópico de alertas ao Service. Implementa kafka.Handler e,
// por isso, roda no mesmo laço de consumo do agregador, com commit manual.
type Consumer struct {
	Decoder   aggregation.Decoder
	Service   *Service
	Committer aggregation.Committer
	Observer  Observer
	Logger    *slog.Logger
	Backoff   time.Duration
}

func (c *Consumer) Assign(context.Context, []aggregation.TopicPartition) error { return nil }
func (c *Consumer) Revoke(context.Context, []aggregation.TopicPartition) error { return nil }
func (c *Consumer) Drop([]aggregation.TopicPartition)                          {}

// HandleBatch avisa cada alerta e só então commita. Um alerta que não
// decodifica nunca vai decodificar: é registrado e pulado, para não travar a
// partição. Erro transitório do registry é tentado de novo.
func (c *Consumer) HandleBatch(ctx context.Context, records []aggregation.Record) error {
	next := map[aggregation.TopicPartition]int64{}
	for _, rec := range records {
		alert, err := c.decode(ctx, rec)
		switch {
		case err == nil:
			if err := c.Service.Notify(ctx, alert); err != nil {
				return err
			}
		case errors.Is(err, contract.ErrMalformed) || errors.Is(err, contract.ErrUnknownSchema):
			c.observer().Notified("none", OutcomeUndecoded)
			c.logger().Error("alerta ilegível ignorado", "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "error", err)
		default:
			return err
		}
		next[rec.TopicPartition] = rec.Offset + 1
	}
	if len(next) == 0 {
		return nil
	}
	if err := c.Committer.Commit(ctx, next); err != nil {
		return fmt.Errorf("commitar offsets: %w", err)
	}
	return nil
}

func (c *Consumer) decode(ctx context.Context, rec aggregation.Record) (Alert, error) {
	backoff := c.Backoff
	if backoff <= 0 {
		backoff = 200 * time.Millisecond
	}
	var payload contract.ThresholdAlertRecord
	for {
		err := c.Decoder.Decode(ctx, rec.Value, &payload)
		if err == nil {
			a := Alert{
				ID: payload.AlertID, GreenhouseID: payload.GreenhouseID, SensorType: telemetry.SensorType(payload.SensorType),
				Rule: telemetry.AlertRule(payload.Rule), WindowStart: payload.WindowStart, WindowEnd: payload.WindowEnd,
				Observed: payload.ObservedValue, Limit: payload.LimitValue,
			}
			if err := a.validate(); err != nil {
				return Alert{}, err
			}
			return a, nil
		}
		if errors.Is(err, contract.ErrMalformed) || errors.Is(err, contract.ErrUnknownSchema) {
			return Alert{}, err
		}
		c.logger().Warn("erro transitório ao decodificar o alerta, tentando de novo", "error", err)
		if werr := wait(ctx, backoff); werr != nil {
			return Alert{}, errors.Join(werr, err)
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func (c *Consumer) logger() *slog.Logger {
	if c.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return c.Logger
}

func (c *Consumer) observer() Observer {
	if c.Observer == nil {
		return NopObserver{}
	}
	return c.Observer
}
