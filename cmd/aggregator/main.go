// Command aggregator consome as leituras, agrega em janelas de 1 minuto por
// estufa e grandeza, publica agregados e alertas, manda mensagens inválidas
// para a DLQ e commita offsets manualmente depois do processamento.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/api"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/config"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/dlq"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/kafka"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/observability"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/postgres"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/state"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

type settings struct {
	brokers        []string
	registry       string
	group          string
	metricsAddr    string
	redisAddr      string
	postgresDSN    string
	maxPollRecords int
	linger         time.Duration
	window         telemetry.WindowConfig
	thresholds     telemetry.Thresholds
	dlqReplay      dlq.Config
	operators      *api.OperatorAuth
}

func main() { os.Exit(realMain()) }

func realMain() int {
	logger := observability.NewLogger("aggregator")
	s, err := load()
	if err != nil {
		logger.Error("configuração inválida", "error", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, s, logger); err != nil {
		logger.Error("agregador encerrado com erro", "error", err)
		return 1
	}
	logger.Info("agregador encerrado")
	return 0
}

func load() (settings, error) {
	var env config.Env
	s := settings{
		brokers:        env.List("KAFKA_BROKERS", "localhost:9092"),
		registry:       env.String("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		group:          env.String("AGGREGATOR_GROUP", "greenhouse-aggregator"),
		metricsAddr:    env.String("METRICS_ADDR", ":9102"),
		redisAddr:      env.String("REDIS_ADDR", ""),
		postgresDSN:    env.String("POSTGRES_DSN", ""),
		maxPollRecords: env.Int("MAX_POLL_RECORDS", 2000),
		linger:         env.Duration("PRODUCER_LINGER", 5*time.Millisecond),
		window: telemetry.WindowConfig{
			Size:            env.Duration("WINDOW_SIZE", time.Minute),
			AllowedLateness: env.Duration("ALLOWED_LATENESS", 10*time.Second),
			// "greenhouse" isola o relógio de cada estufa; "partition" é o
			// comportamento anterior. IdleTimeout 0 desliga o watermark ocioso.
			Scope:       telemetry.WatermarkScope(env.String("WATERMARK_SCOPE", string(telemetry.ScopeGreenhouse))),
			IdleTimeout: env.Duration("WATERMARK_IDLE_TIMEOUT", 2*time.Minute),
		},
	}
	s.thresholds = env.Thresholds()
	s.dlqReplay = dlq.ConfigFromEnv(&env)
	if err := env.Err(); err != nil {
		return s, err
	}
	// Sem OPERATOR_TOKENS a API de consulta fica desligada, em vez de aberta.
	if entries := env.List("OPERATOR_TOKENS", ""); len(entries) > 0 {
		auth, err := api.NewOperatorAuth(entries)
		if err != nil {
			return s, err
		}
		s.operators = auth
	}
	return s, s.window.Validate()
}

func run(ctx context.Context, s settings, logger *slog.Logger) error {
	reg := observability.NewRegistry()
	metrics := observability.NewAggregatorMetrics(reg)

	tracing, err := observability.SetupTracing(ctx, "aggregator", s.group)
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer func() { _ = tracing.Shutdown(context.Background()) }()

	registry, err := kafka.NewRegistry(s.registry)
	if err != nil {
		return err
	}
	aggEnc, err := encoderFor(ctx, registry, schemas.TopicAggregates)
	if err != nil {
		return err
	}
	alertEnc, err := encoderFor(ctx, registry, schemas.TopicAlerts)
	if err != nil {
		return err
	}

	consumer, err := kafka.NewGroupConsumer(kafka.GroupConsumerConfig{
		Seeds:          s.brokers,
		Group:          s.group,
		Topic:          schemas.TopicReadings,
		MaxPollRecords: s.maxPollRecords,
		Linger:         s.linger,
		Hooks:          tracing.Hooks,
		TickInterval:   tickInterval(s.window),
	}, logger)
	if err != nil {
		return err
	}
	defer consumer.Close()

	// Cada fonte da API é opcional: sem Redis ou Postgres configurado, as rotas
	// que dependem dela simplesmente não existem.
	apiDeps := api.Deps{Thresholds: s.thresholds, Logger: logger, Auth: s.operators}
	if s.operators == nil {
		logger.Warn("OPERATOR_TOKENS vazio: API de consulta desligada")
	}
	throughput := observability.NewThroughput(metrics.ConsumedTotal, time.Minute, 5*time.Second)
	inspector, err := kafka.NewInspector(s.brokers, s.group, throughput)
	if err != nil {
		return err
	}
	defer inspector.Close()
	apiDeps.DLQ, apiDeps.Health = inspector, inspector
	var stateStore aggregation.StateStore
	if s.redisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: s.redisAddr})
		defer func() { _ = rdb.Close() }()
		store := state.NewRedisStore(rdb)
		stateStore = state.NewInstrumented(store, reg)
		apiDeps.State = store
		// O ledger da DLQ mora no Redis; sem ele não há reprocessamento.
		replay, err := dlq.New(s.dlqReplay, dlq.Deps{
			Source: inspector, Publisher: kafka.NewPublisher(consumer.Client()),
			Ledger: dlq.NewRedisLedger(rdb), Decoder: contract.NewDecoder(registry),
			Checkpoints: kafka.NewCheckpointStore(consumer.Client(), s.brokers, schemas.TopicCheckpoints), Logger: logger,
		})
		if err != nil {
			return err
		}
		apiDeps.Replay = replay
	}
	if s.postgresDSN != "" {
		pg, err := postgres.Open(ctx, s.postgresDSN)
		if err != nil {
			return err
		}
		defer pg.Close()
		apiDeps.Series, apiDeps.Alerts = pg.Series(), pg.Alerts()
	}
	routes := api.Routes(apiDeps)

	svc, err := aggregation.New(s.window, s.thresholds,
		aggregation.Topics{Aggregates: schemas.TopicAggregates, Alerts: schemas.TopicAlerts, DLQ: schemas.TopicReadingsDLQ},
		aggregation.Deps{
			Decoder:        contract.NewDecoder(registry),
			AggregateCodec: aggEnc,
			AlertCodec:     alertEnc,
			Publisher:      kafka.NewPublisher(consumer.Client()),
			Checkpoints:    kafka.NewCheckpointStore(consumer.Client(), s.brokers, schemas.TopicCheckpoints),
			Committer:      consumer,
			State:          stateStore,
			Observer:       metrics,
			Logger:         logger,
		})
	if err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return throughput.Run(gctx) })
	g.Go(func() error { return observability.Serve(gctx, s.metricsAddr, reg, logger, routes...) })
	g.Go(func() error {
		logger.Info("agregador iniciado", "group", s.group, "window", s.window.Size.String(),
			"allowed_lateness", s.window.AllowedLateness.String(), "brokers", s.brokers)
		return consumer.Run(gctx, svc, metrics)
	})
	return g.Wait()
}

func encoderFor(ctx context.Context, registry *kafka.Registry, topic string) (*contract.Encoder, error) {
	sub, err := schemas.Lookup(topic)
	if err != nil {
		return nil, err
	}
	text, err := sub.Latest()
	if err != nil {
		return nil, err
	}
	id, err := registry.Lookup(ctx, sub.Name, text)
	if err != nil {
		return nil, err
	}
	return contract.NewEncoder(id, text)
}

// tickInterval acorda o laço de consumo para fechar janelas ociosas; sem
// watermark ocioso não há motivo para acordar.
func tickInterval(w telemetry.WindowConfig) time.Duration {
	if w.IdleTimeout <= 0 {
		return 0
	}
	return min(time.Second, w.IdleTimeout)
}
