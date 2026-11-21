package observability

import (
	"context"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Tracing guarda o provider e os hooks do franz-go que propagam o contexto
// de trace pelos headers das mensagens (W3C traceparent).
type Tracing struct {
	Hooks    []kgo.Hook
	Shutdown func(context.Context) error
}

// SetupTracing liga o tracing quando OTEL_EXPORTER_OTLP_ENDPOINT está
// definido; sem ele, devolve hooks vazios e nenhum custo. A amostragem segue
// as variáveis padrão do SDK (OTEL_TRACES_SAMPLER e OTEL_TRACES_SAMPLER_ARG),
// importante porque cada leitura gera um span de produce e um de consumo.
func SetupTracing(ctx context.Context, service, consumerGroup string) (Tracing, error) {
	noop := Tracing{Shutdown: func(context.Context) error { return nil }}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return noop, nil
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return noop, err
	}
	res, err := resource.New(ctx, resource.WithFromEnv(), resource.WithTelemetrySDK(),
		resource.WithAttributes(attribute.String("service.name", service)))
	if err != nil {
		return noop, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(prop)

	opts := []kotel.TracerOpt{kotel.TracerProvider(tp), kotel.TracerPropagator(prop)}
	if consumerGroup != "" {
		opts = append(opts, kotel.ConsumerGroup(consumerGroup))
	}
	k := kotel.NewKotel(kotel.WithTracer(kotel.NewTracer(opts...)))
	return Tracing{Hooks: k.Hooks(), Shutdown: tp.Shutdown}, nil
}
