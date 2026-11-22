#!/usr/bin/env bash
# ACLs por serviço do profile secure. Idempotente: kafka-acls --add repete sem erro.
# Princípio: cada serviço só tem o que usa; o resto é negado (allow.everyone.if.no.acl.found=false).
# O usuário `ops` é superusuário, só para criar tópicos e rodar este script.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka-secure:29092}"
CONFIG="${COMMAND_CONFIG:-/etc/kafka/secrets/ops.properties}"
acl() { # PRINCIPAL args...
  local p="$1"; shift
  kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$CONFIG" --add --allow-principal "User:$p" "$@"
}

READINGS=greenhouse.readings
AGGREGATES=greenhouse.aggregates
ALERTS=greenhouse.alerts
DLQ=greenhouse.readings.dlq
CHECKPOINTS=greenhouse.aggregator.checkpoints

# Simulador: só escreve leituras.
acl simulator --operation Write --operation Describe --topic "$READINGS"
acl simulator --operation IdempotentWrite --cluster

# Agregador: lê leituras; escreve agregados, alertas, DLQ e checkpoints; lê DLQ e
# checkpoints (reprocessador e restauração); republica leituras (reprocessador da DLQ).
acl aggregator --operation Read --operation Write --operation Describe --topic "$READINGS"
acl aggregator --operation Write --operation Describe --topic "$AGGREGATES" --topic "$ALERTS"
acl aggregator --operation Read --operation Write --operation Describe --topic "$DLQ" --topic "$CHECKPOINTS"
acl aggregator --operation Read --operation Describe --group greenhouse-aggregator
# Prova de que o sink do conector alcançou o tópico (backfill): só descrever o grupo dele.
acl aggregator --operation Describe --group connect-greenhouse-aggregates-jdbc-sink
acl aggregator --operation IdempotentWrite --cluster

# Notificador: só lê alertas.
acl notifier --operation Read --operation Describe --topic "$ALERTS"
acl notifier --operation Read --operation Describe --group greenhouse-notifier

# Schema Registry: dono do tópico _schemas.
acl schemaregistry --operation Read --operation Write --operation Describe --operation DescribeConfigs --operation Create --topic _schemas
acl schemaregistry --operation Read --group schema-registry
acl schemaregistry --operation Describe --operation DescribeConfigs --cluster

kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$CONFIG" --list
