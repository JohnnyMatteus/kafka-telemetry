#!/usr/bin/env sh
# Sobe o ambiente completo (com Kafka UI), espera os conectores e o agregador
# e grava a evidência em vídeo com Playwright (e2e/evidencia).
set -eu
cd "$(dirname "$0")/.."

: "${SIM_RATE:=400}"
export SIM_RATE
compose="docker compose -p kafka-telemetry --profile core --profile observability --profile tools"
$compose up -d --build

connect="http://localhost:${CONNECT_PORT:-8083}"
aggregator="http://localhost:${AGGREGATOR_METRICS_PORT:-9102}"
echo "aguardando conectores RUNNING e agregador no ar..."
for _ in $(seq 1 120); do
  running=$(curl -fsS "$connect/connectors?expand=status" 2>/dev/null | grep -o '"connector":{"state":"RUNNING"' | wc -l || true)
  if [ "${running:-0}" -ge 3 ] && curl -fsS "$aggregator/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

export SCHEMA_REGISTRY_URL="http://localhost:${SCHEMA_REGISTRY_PORT:-8081}"
export CONNECT_URL="$connect" AGGREGATOR_URL="$aggregator"
export KAFKA_UI_URL="http://localhost:${KAFKA_UI_PORT:-8080}"
export PROMETHEUS_URL="http://localhost:${PROMETHEUS_PORT:-9090}"
export MINIO_CONSOLE_URL="http://localhost:${MINIO_CONSOLE_PORT:-9001}"
export PG_URL="postgres://telemetry:${POSTGRES_PASSWORD:-dev-only-change-me}@localhost:${POSTGRES_PORT:-5432}/telemetry"

cd e2e/evidencia
npm ci --no-audit --no-fund
npx playwright install chromium
npx playwright test
