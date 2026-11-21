// Command simulator gera leituras dos sensores das estufas e publica no
// tópico de leituras com produtor idempotente e acks=all. Também é a
// ferramenta do teste de carga: ao final imprime throughput e latência de ack.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/config"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/kafka"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/observability"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/simulator"
	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

type options struct {
	brokers     []string
	registry    string
	metricsAddr string
	rate        float64
	duration    time.Duration
	linger      time.Duration
	firmware    string
	sim         simulator.Config
}

func main() { os.Exit(realMain()) }

func realMain() int {
	logger := observability.NewLogger("simulator")
	opts, err := parseOptions()
	if err != nil {
		logger.Error("configuração inválida", "error", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, opts, logger); err != nil {
		logger.Error("simulador encerrado com erro", "error", err)
		return 1
	}
	return 0
}

func parseOptions() (options, error) {
	var env config.Env
	o := options{
		brokers:     env.List("KAFKA_BROKERS", "localhost:9092"),
		registry:    env.String("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		metricsAddr: env.String("METRICS_ADDR", ":9101"),
	}
	flag.IntVar(&o.sim.Greenhouses, "greenhouses", env.Int("SIM_GREENHOUSES", 20), "número de estufas (4 sensores cada)")
	flag.Float64Var(&o.rate, "rate", env.Float("SIM_RATE", 80), "leituras por segundo no total")
	flag.DurationVar(&o.duration, "duration", env.Duration("SIM_DURATION", 0), "duração da simulação (0 = até ser interrompido)")
	flag.Float64Var(&o.sim.DuplicateRatio, "duplicate-ratio", env.Float("SIM_DUPLICATE_RATIO", 0.02), "fração de retransmissões")
	flag.Float64Var(&o.sim.InvalidRatio, "invalid-ratio", env.Float("SIM_INVALID_RATIO", 0.005), "fração de leituras inválidas")
	flag.Float64Var(&o.sim.AnomalyRatio, "anomaly-ratio", env.Float("SIM_ANOMALY_RATIO", 0.1), "fração de estufas superaquecidas")
	flag.Uint64Var(&o.sim.Seed, "seed", uint64(env.Int("SIM_SEED", 1)), "semente do gerador")
	flag.DurationVar(&o.linger, "linger", env.Duration("PRODUCER_LINGER", 5*time.Millisecond), "espera para formar lotes")
	flag.StringVar(&o.firmware, "firmware", env.String("SIM_FIRMWARE", "1.4.0"), "firmware_version (campo opcional de v2)")
	flag.Parse()
	if err := env.Err(); err != nil {
		return o, err
	}
	if o.rate <= 0 {
		return o, errors.New("rate deve ser positivo")
	}
	return o, o.sim.Validate()
}

func run(ctx context.Context, o options, logger *slog.Logger) error {
	reg := observability.NewRegistry()
	metrics := observability.NewSimulatorMetrics(reg)

	tracing, err := observability.SetupTracing(ctx, "simulator", "")
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer func() { _ = tracing.Shutdown(context.Background()) }()

	encoder, err := readingEncoder(ctx, o.registry)
	if err != nil {
		return err
	}
	gen, err := simulator.New(o.sim)
	if err != nil {
		return err
	}
	client, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(o.brokers...),
		kgo.ClientID("greenhouse-simulator"),
		kgo.DefaultProduceTopic(schemas.TopicReadings),
		kgo.WithHooks(tracing.Hooks...),
	}, kafka.ProducerOptions(o.linger)...)...)
	if err != nil {
		return err
	}
	defer client.Close()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, gctx := errgroup.WithContext(runCtx)
	g.Go(func() error { return observability.Serve(gctx, o.metricsAddr, reg, logger) })

	stats := &stats{}
	g.Go(func() error {
		defer cancel()
		logger.Info("simulação iniciada", "greenhouses", o.sim.Greenhouses, "sensors", gen.Sensors(),
			"rate", o.rate, "duration", o.duration.String(), "brokers", o.brokers)
		err := produce(gctx, client, gen, encoder, o, metrics, stats)
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer flushCancel()
		if ferr := client.Flush(flushCtx); ferr != nil {
			err = errors.Join(err, fmt.Errorf("flush: %w", ferr))
		}
		stats.log(logger)
		return err
	})
	return g.Wait()
}

func readingEncoder(ctx context.Context, url string) (*contract.Encoder, error) {
	registry, err := kafka.NewRegistry(url)
	if err != nil {
		return nil, err
	}
	sub, err := schemas.Lookup(schemas.TopicReadings)
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

// produce gera leituras no ritmo pedido. A cada tick calcula quantas leituras
// já deveriam ter saído desde o início, o que corrige atrasos do ticker.
func produce(ctx context.Context, client *kgo.Client, gen *simulator.Generator, enc *contract.Encoder,
	o options, metrics *observability.SimulatorMetrics, st *stats) error {
	start := time.Now()
	st.start = start
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var sent int64
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			if o.duration > 0 && now.Sub(start) >= o.duration {
				return nil
			}
			due := int64(now.Sub(start).Seconds() * o.rate)
			for ; sent < due; sent++ {
				reading := gen.Next(now)
				value, err := enc.Encode(contract.NewSensorReadingRecord(reading, o.firmware))
				if err != nil {
					return fmt.Errorf("serializar leitura: %w", err)
				}
				sensorType := string(reading.Type)
				enqueued := time.Now()
				client.Produce(ctx, &kgo.Record{Key: []byte(reading.GreenhouseID), Value: value}, func(_ *kgo.Record, err error) {
					latency := time.Since(enqueued)
					if err != nil {
						metrics.Errors.Inc()
						st.failed()
						return
					}
					metrics.Produced.WithLabelValues(sensorType).Inc()
					metrics.Latency.Observe(latency.Seconds())
					st.acked(latency)
				})
			}
		}
	}
}

// stats acumula o resultado da execução para o resumo do teste de carga.
type stats struct {
	mu        sync.Mutex
	start     time.Time
	latencies []time.Duration
	errors    int
}

func (s *stats) acked(d time.Duration) {
	s.mu.Lock()
	s.latencies = append(s.latencies, d)
	s.mu.Unlock()
}

func (s *stats) failed() {
	s.mu.Lock()
	s.errors++
	s.mu.Unlock()
}

func (s *stats) log(logger *slog.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	elapsed := time.Since(s.start)
	lat := slices.Clone(s.latencies)
	slices.Sort(lat)
	pct := func(p float64) string {
		if len(lat) == 0 {
			return "n/a"
		}
		return lat[min(int(float64(len(lat))*p), len(lat)-1)].Round(100 * time.Microsecond).String()
	}
	logger.Info("resumo da simulação",
		"acked", len(lat),
		"errors", s.errors,
		"elapsed", elapsed.Round(time.Millisecond).String(),
		"throughput_per_s", fmt.Sprintf("%.1f", float64(len(lat))/elapsed.Seconds()),
		"ack_p50", pct(0.50), "ack_p95", pct(0.95), "ack_p99", pct(0.99), "ack_max", pct(1),
	)
}
