// Command notifier consome os alertas de limite e avisa o produtor rural por
// e-mail (SMTP; Mailpit no compose) e por webhook assinado, com idempotência
// no Redis e retry com backoff.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/config"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/kafka"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/notify"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/observability"
	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

type settings struct {
	brokers      []string
	registry     string
	group        string
	metricsAddr  string
	redisAddr    string
	recipients   string
	smtpAddr     string
	smtpFrom     string
	smtpUser     string
	smtpPassword string
	allowHTTP    bool
	attempts     int
	retryMaxAge  time.Duration
	retryBase    time.Duration
	retryEvery   time.Duration
}

func main() { os.Exit(realMain()) }

func realMain() int {
	logger := observability.NewLogger("notifier")
	s, err := load()
	if err != nil {
		logger.Error("configuração inválida", "error", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, s, logger); err != nil {
		logger.Error("notificador encerrado com erro", "error", err)
		return 1
	}
	logger.Info("notificador encerrado")
	return 0
}

func load() (settings, error) {
	var env config.Env
	s := settings{
		brokers:      env.List("KAFKA_BROKERS", "localhost:9092"),
		registry:     env.String("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		group:        env.String("NOTIFIER_GROUP", "greenhouse-notifier"),
		metricsAddr:  env.String("METRICS_ADDR", ":9103"),
		redisAddr:    env.String("REDIS_ADDR", ""),
		recipients:   env.String("NOTIFY_RECIPIENTS_FILE", ""),
		smtpAddr:     env.String("NOTIFY_SMTP_ADDR", ""),
		smtpFrom:     env.String("NOTIFY_SMTP_FROM", "alertas@example.com"),
		smtpUser:     env.String("NOTIFY_SMTP_USER", ""),
		smtpPassword: env.String("NOTIFY_SMTP_PASSWORD", ""),
		allowHTTP:    env.String("NOTIFY_WEBHOOK_ALLOW_HTTP", "") == "true",
		attempts:     env.Int("NOTIFY_MAX_ATTEMPTS", 5),
		retryMaxAge:  env.Duration("NOTIFY_RETRY_MAX_AGE", 24*time.Hour),
		retryBase:    env.Duration("NOTIFY_RETRY_BASE", 30*time.Second),
		retryEvery:   env.Duration("NOTIFY_RETRY_INTERVAL", 5*time.Second),
	}
	if err := env.Err(); err != nil {
		return s, err
	}
	switch {
	case s.redisAddr == "":
		return s, errors.New("REDIS_ADDR é obrigatório: a idempotência dos avisos mora no Redis")
	case s.recipients == "":
		return s, errors.New("NOTIFY_RECIPIENTS_FILE é obrigatório")
	case s.retryEvery <= 0:
		return s, errors.New("NOTIFY_RETRY_INTERVAL deve ser positivo")
	}
	return s, nil
}

// channels liga o e-mail se houver SMTP e o webhook se algum produtor usa
// webhook (cada um com o seu segredo, vindo de variável de ambiente).
func channels(s settings, dir *notify.FileDirectory) ([]notify.Channel, error) {
	var out []notify.Channel
	if s.smtpAddr != "" {
		out = append(out, &notify.EmailChannel{Addr: s.smtpAddr, From: s.smtpFrom, Username: s.smtpUser, Password: s.smtpPassword})
	}
	if dir.HasWebhooks() {
		out = append(out, notify.NewWebhookChannel(s.allowHTTP))
	}
	if len(out) == 0 {
		return nil, errors.New("nenhum canal ligado: informe NOTIFY_SMTP_ADDR ou cadastre produtores com webhook_url")
	}
	return out, nil
}

func run(ctx context.Context, s settings, logger *slog.Logger) error {
	reg := observability.NewRegistry()
	metrics := observability.NewNotifierMetrics(reg)

	dir, err := notify.LoadDirectory(s.recipients)
	if err != nil {
		return err
	}
	chans, err := channels(s, dir)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(&redis.Options{Addr: s.redisAddr})
	defer func() { _ = rdb.Close() }()
	svc, err := notify.New(notify.Deps{
		Channels: chans, Directory: dir, Sent: notify.NewRedisSent(rdb), Retries: notify.NewRedisRetries(rdb), Observer: metrics, Logger: logger,
		Retry:     notify.Retry{Attempts: s.attempts},
		RetryPlan: notify.RetryPlan{Base: s.retryBase, MaxAge: s.retryMaxAge},
	})
	if err != nil {
		return err
	}

	registry, err := kafka.NewRegistry(s.registry)
	if err != nil {
		return err
	}
	consumer, err := kafka.NewGroupConsumer(kafka.GroupConsumerConfig{
		Seeds: s.brokers, Group: s.group, Topic: schemas.TopicAlerts, MaxPollRecords: 100, Linger: 5 * time.Millisecond,
	}, logger)
	if err != nil {
		return err
	}
	defer consumer.Close()
	handler := &notify.Consumer{Decoder: contract.NewDecoder(registry), Service: svc, Committer: consumer, Observer: metrics, Logger: logger}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return observability.Serve(gctx, s.metricsAddr, reg, logger) })
	g.Go(func() error { return svc.RunRetries(gctx, s.retryEvery, metrics) })
	g.Go(func() error {
		logger.Info("notificador iniciado", "group", s.group, "topic", schemas.TopicAlerts, "channels", len(chans))
		return consumer.Run(gctx, handler, metrics)
	})
	if err := g.Wait(); err != nil {
		return fmt.Errorf("notificador: %w", err)
	}
	return nil
}
