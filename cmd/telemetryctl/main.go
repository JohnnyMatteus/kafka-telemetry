// Command telemetryctl prepara o ambiente: cria tópicos, confere a
// compatibilidade dos schemas do repositório e registra os schemas no
// Schema Registry com compatibilidade BACKWARD.
//
//	telemetryctl init              topics + schemas register, para o docker-compose
//	telemetryctl topics            cria os tópicos (idempotente)
//	telemetryctl schemas check     confere compatibilidade localmente, sem rede
//	telemetryctl schemas register  configura BACKWARD, pergunta ao registry e registra
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/config"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/kafka"
	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

const usage = `uso:
  telemetryctl init
  telemetryctl topics [-partitions N] [-replication N] [-min-isr N]
  telemetryctl schemas check
  telemetryctl schemas register

variáveis: KAFKA_BROKERS (localhost:9092), SCHEMA_REGISTRY_URL (http://localhost:8081)`

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
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch {
	case len(args) == 1 && args[0] == "init":
		if err := topics(ctx, brokers, nil); err != nil {
			return err
		}
		return register(ctx, registryURL)
	case args[0] == "topics":
		return topics(ctx, brokers, args[1:])
	case len(args) == 2 && args[0] == "schemas" && args[1] == "check":
		return checkLocal()
	case len(args) == 2 && args[0] == "schemas" && args[1] == "register":
		return register(ctx, registryURL)
	default:
		return errors.New(usage)
	}
}

func topics(ctx context.Context, brokers, args []string) error {
	fs := flag.NewFlagSet("topics", flag.ContinueOnError)
	partitions := fs.Int("partitions", 6, "partições dos tópicos de dados")
	replication := fs.Int("replication", 1, "fator de replicação (3 em produção)")
	minISR := fs.Int("min-isr", 1, "min.insync.replicas (2 em produção)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	created, err := kafka.EnsureTopics(ctx, brokers, int16(*replication), kafka.PipelineTopics(int32(*partitions), *minISR))
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
func register(ctx context.Context, url string) error {
	registry, err := kafka.NewRegistry(url)
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
