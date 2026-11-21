#!/usr/bin/env bash
# Teste de carga do pipeline usando o próprio simulador como gerador.
#
#   ./scripts/load-test.sh [taxa/s] [duração] [estufas]
#   make loadtest LOAD_RATE=2000 LOAD_DURATION=60s LOAD_GREENHOUSES=200
#
# Pré-requisito: `make up` (Kafka, Schema Registry e agregador no ar).
# O simulador do compose é pausado durante o teste para não misturar carga.
# Saída: resumo do produtor (throughput, latência de ack p50/p95/p99) e, do
# lado do agregador, registros consumidos, lag máximo e tempo para zerar o lag.
set -euo pipefail

RATE="${1:-2000}"
DURATION="${2:-60s}"
GREENHOUSES="${3:-200}"
AGGREGATOR_METRICS="${AGGREGATOR_METRICS:-http://localhost:9102/metrics}"
OUT_DIR="${OUT_DIR:-loadtest-results}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT_DIR"
REPORT="$OUT_DIR/$STAMP.txt"

metric_sum() {
  curl -fsS "$AGGREGATOR_METRICS" | awk -v name="$1" '$1 ~ "^"name"({|$)" {s += $NF} END {printf "%.0f", s}'
}

echo "pausando o simulador do compose"
docker compose stop simulator >/dev/null 2>&1 || true

consumed_before="$(metric_sum telemetry_aggregator_records_consumed_total)"
aggregates_before="$(metric_sum telemetry_aggregator_aggregates_emitted_total)"

echo "carga: ${RATE}/s por ${DURATION} com ${GREENHOUSES} estufas"
go build -o bin/simulator ./cmd/simulator
METRICS_ADDR=":19101" ./bin/simulator -rate "$RATE" -duration "$DURATION" -greenhouses "$GREENHOUSES" \
  -seed "$(date +%s)" -duplicate-ratio 0.01 -invalid-ratio 0.001 > "$OUT_DIR/$STAMP-simulator.log" 2>&1 &
SIM_PID=$!

max_lag=0
while kill -0 "$SIM_PID" 2>/dev/null; do
  lag="$(metric_sum telemetry_aggregator_consumer_lag)"
  (( lag > max_lag )) && max_lag=$lag
  sleep 1
done
wait "$SIM_PID"

drain_start=$(date +%s)
while :; do
  lag="$(metric_sum telemetry_aggregator_consumer_lag)"
  (( lag > max_lag )) && max_lag=$lag
  (( lag == 0 )) && break
  (( $(date +%s) - drain_start > 300 )) && { echo "lag não zerou em 5 minutos"; break; }
  sleep 1
done
drain_seconds=$(( $(date +%s) - drain_start ))

consumed=$(( $(metric_sum telemetry_aggregator_records_consumed_total) - consumed_before ))
aggregates=$(( $(metric_sum telemetry_aggregator_aggregates_emitted_total) - aggregates_before ))
summary="$(grep 'resumo da simulação' "$OUT_DIR/$STAMP-simulator.log" || true)"

{
  echo "teste de carga $STAMP"
  echo "parâmetros: rate=${RATE}/s duration=${DURATION} greenhouses=${GREENHOUSES}"
  echo "produtor: $summary"
  echo "agregador: consumidos=$consumed agregados_emitidos=$aggregates lag_max=$max_lag drenagem_apos_fim=${drain_seconds}s"
} | tee "$REPORT"

echo "retomando o simulador do compose"
docker compose start simulator >/dev/null 2>&1 || true
