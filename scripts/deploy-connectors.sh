#!/usr/bin/env sh
# Aplica cada connect/*.json no Kafka Connect com PUT /connectors/<nome>/config.
# PUT cria ou atualiza, então o script é idempotente. O nome do conector é o
# nome do arquivo sem extensão.
set -eu

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
DIR="${1:-$(dirname "$0")/../connect}"

until curl -fsS "$CONNECT_URL/connectors" >/dev/null 2>&1; do
  echo "aguardando Kafka Connect em $CONNECT_URL..."
  sleep 3
done

for file in "$DIR"/*.json; do
  name="$(basename "$file" .json)"
  echo "aplicando $name"
  curl -fsS -X PUT -H "Content-Type: application/json" --data @"$file" \
    "$CONNECT_URL/connectors/$name/config" >/dev/null
done

curl -fsS "$CONNECT_URL/connectors?expand=status"
echo
