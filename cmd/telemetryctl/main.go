// Command telemetryctl prepara o ambiente: cria tópicos, confere a
// compatibilidade dos schemas do repositório e registra os schemas no
// Schema Registry com compatibilidade BACKWARD.
//
//	telemetryctl init              topics + schemas register, para o docker-compose
//	telemetryctl topics            cria os tópicos (idempotente)
//	telemetryctl schemas check     confere compatibilidade localmente, sem rede
//	telemetryctl schemas register  configura BACKWARD, pergunta ao registry e registra
//	telemetryctl dlq replay        reprocessa a DLQ (dry-run, a menos que -execute)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/config"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/dlq"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/kafka"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/postgres"
	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

const usage = `uso:
  telemetryctl init
  telemetryctl topics [-partitions N] [-replication N] [-min-isr N]
  telemetryctl schemas check
  telemetryctl schemas register
  telemetryctl dlq replay [-limit N] [-reason R] [-backfill] [-execute -operator NOME]

variáveis: KAFKA_BROKERS (localhost:9092), SCHEMA_REGISTRY_URL (http://localhost:8081),
           KAFKA_TLS_*, KAFKA_SASL_*, SCHEMA_REGISTRY_USER/PASSWORD_FILE/CA_FILE (profile secure),
           REDIS_ADDR, POSTGRES_DSN (backfill), DLQ_REPLAY_TOPIC, DLQ_REPLAY_MAX_BATCH,
           DLQ_BACKFILL_MIN_AGE, WINDOW_SIZE, THRESHOLD_* (só para dlq replay)`

func main() { os.Exit(realMain()) }

func realMain() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "erro:", err)
		return 1
	}
	return 0
}

func run(ctx context.Context, args []string) error {
	var env config.Env
	brokers := env.List("KAFKA_BROKERS", "localhost:9092")
	registryURL := env.String("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	if err := env.Err(); err != nil {
		return err
	}
	sec, err := kafka.SecurityFromEnv(&env)
	if err != nil {
		return err
	}
	kopts, err := sec.Opts()
	if err != nil {
		return err
	}
	rsec, err := kafka.RegistrySecurityFromEnv(&env)
	if err != nil {
		return err
	}
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch {
	case len(args) == 1 && args[0] == "init":
		if err := topics(ctx, brokers, nil, kopts); err != nil {
			return err
		}
		return register(ctx, registryURL, rsec)
	case args[0] == "topics":
		return topics(ctx, brokers, args[1:], kopts)
	case len(args) == 2 && args[0] == "schemas" && args[1] == "check":
		return checkLocal()
	case len(args) >= 2 && args[0] == "dlq" && args[1] == "replay":
		return replay(ctx, brokers, registryURL, &env, args[2:], kopts, rsec)
	case len(args) == 2 && args[0] == "schemas" && args[1] == "register":
		return register(ctx, registryURL, rsec)
	default:
		return errors.New(usage)
	}
}

// replay reprocessa a DLQ. Sem -execute só mostra o que seria republicado.
// O operador é declarado pela pessoa que roda o comando, que já precisa de
// acesso ao broker e ao Redis; fica gravado no ledger e nas mensagens.
func replay(ctx context.Context, brokers []string, registryURL string, env *config.Env, args []string, kopts []kgo.Opt, rsec kafka.RegistrySecurity) error {
	fs := flag.NewFlagSet("dlq replay", flag.ContinueOnError)
	limit := fs.Int("limit", 0, "mensagens por execução (padrão 10, teto DLQ_REPLAY_MAX_BATCH)")
	reason := fs.String("reason", "", "só este motivo (malformed, unknown_schema, invalid_reading)")
	execute := fs.Bool("execute", false, "republica de verdade; sem isso é dry-run")
	backfill := fs.Bool("backfill", false, "recupera leituras de janela já fechada, recalculando e republicando o agregado (exige POSTGRES_DSN)")
	operator := fs.String("operator", "", "quem executa; obrigatório com -execute")
	if err := fs.Parse(args); err != nil {
		return err
	}
	redisAddr := env.String("REDIS_ADDR", "")
	postgresDSN := env.String("POSTGRES_DSN", "")
	sinkGroup := env.String("DLQ_BACKFILL_SINK_GROUP", kafka.DefaultSinkGroup)
	thresholds := env.Thresholds()
	cfg := dlq.ConfigFromEnv(env)
	if err := env.Err(); err != nil {
		return err
	}
	if redisAddr == "" {
		return errors.New("REDIS_ADDR é obrigatório: o ledger da DLQ mora no Redis")
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rdb.Close() }()
	inspector, err := kafka.NewInspector(brokers, "", nil, kopts...)
	if err != nil {
		return err
	}
	defer inspector.Close()
	producer, err := kgo.NewClient(append(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, kafka.ProducerOptions(5*time.Millisecond)...), kopts...)...)
	if err != nil {
		return err
	}
	defer producer.Close()

	registry, err := kafka.NewRegistry(registryURL, rsec)
	if err != nil {
		return err
	}
	deps := dlq.Deps{Source: inspector, Publisher: kafka.NewPublisher(producer),
		Ledger: dlq.NewRedisLedger(rdb), Decoder: contract.NewDecoder(registry),
		Checkpoints: kafka.NewCheckpointStore(producer, brokers, schemas.TopicCheckpoints, kopts...), Logger: slog.Default()}
	if postgresDSN != "" {
		pg, err := postgres.Open(ctx, postgresDSN)
		if err != nil {
			return err
		}
		defer pg.Close()
		aggEnc, err := registry.EncoderFor(ctx, schemas.TopicAggregates)
		if err != nil {
			return err
		}
		alertEnc, err := registry.EncoderFor(ctx, schemas.TopicAlerts)
		if err != nil {
			return err
		}
		deps.Backfill = &dlq.BackfillDeps{Sink: kafka.NewSinkProof(inspector, sinkGroup), Ledger: dlq.NewRedisBackfillLedger(rdb), Windows: dlq.NewRedisWindowStore(rdb),
			Reader: pg.Aggregates(), AggregateCodec: aggEnc, AlertCodec: alertEnc, Thresholds: thresholds}
	}
	r, err := dlq.New(cfg, deps)
	if err != nil {
		return err
	}
	op := ""
	if *operator != "" {
		op = "cli:" + *operator
	}
	res, err := r.Run(ctx, dlq.Request{Operator: op, Limit: *limit, Reason: *reason, Execute: *execute, Backfill: *backfill})
	if err != nil {
		return err
	}
	out, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(out))
	if res.DryRun {
		fmt.Fprintln(os.Stderr, "dry-run: nada foi republicado; use -execute -operator NOME")
	}
	return nil
}

func topics(ctx context.Context, brokers, args []string, kopts []kgo.Opt) error {
	fs := flag.NewFlagSet("topics", flag.ContinueOnError)
	partitions := fs.Int("partitions", 6, "partições dos tópicos de dados")
	replication := fs.Int("replication", 1, "fator de replicação (3 em produção)")
	minISR := fs.Int("min-isr", 1, "min.insync.replicas (2 em produção)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	created, err := kafka.EnsureTopics(ctx, brokers, int16(*replication), kafka.PipelineTopics(int32(*partitions), *minISR), kopts...)
	for _, t := range created {
		fmt.Println("tópico criado:", t)
	}
	if err == nil && len(created) == 0 {
		fmt.Println("todos os tópicos já existiam")
	}
	return err
}

func checkLocal() error {
	var failed error
	for _, sub := range schemas.Subjects {
		texts, err := sub.Texts()
		if err != nil {
			return err
		}
		for i := range texts {
			if err := contract.CheckCompatibility(contract.BackwardTransitive, texts[i], texts[:i]...); err != nil {
				failed = errors.Join(failed, fmt.Errorf("%s (%s): %w", sub.Name, sub.Versions[i], err))
			}
		}
		if failed == nil {
			fmt.Printf("ok  %s: %d versões compatíveis (BACKWARD_TRANSITIVE)\n", sub.Name, len(texts))
		}
	}
	return failed
}

// register aplica as versões em ordem. O registry é a fonte da verdade: a
// checagem remota roda antes de cada registro e uma incompatibilidade aborta
// sem registrar nada daquele subject.
func register(ctx context.Context, url string, rsec kafka.RegistrySecurity) error {
	registry, err := kafka.NewRegistry(url, rsec)
	if err != nil {
		return err
	}
	for _, sub := range schemas.Subjects {
		if err := registry.SetBackward(ctx, sub.Name); err != nil {
			return err
		}
		texts, err := sub.Texts()
		if err != nil {
			return err
		}
		for i, text := range texts {
			ok, messages, err := registry.CheckCompatible(ctx, sub.Name, text)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("%s: %s incompatível com o registry: %v", sub.Name, sub.Versions[i], messages)
			}
			id, err := registry.Register(ctx, sub.Name, text)
			if err != nil {
				return err
			}
			fmt.Printf("ok  %s <- %s (id %d)\n", sub.Name, sub.Versions[i], id)
		}
	}
	return nil
}
