# CLAUDE.md

Guia para sessões de agente neste repositório.

## Visão

Pipeline de telemetria com Kafka para as estufas da cooperativa fictícia AgroVale Exemplo. Núcleo: **contratos de schema** (Avro + Schema Registry, BACKWARD) e **garantias de entrega** (produtor idempotente, offset seguro, publish → checkpoint → commit, deduplicação, DLQ). Leia `docs/design-doc.md` e os dois roteiros em `docs/roteiros/` antes de mudar o núcleo.

## Regras

- Domínio 100% fictício. Nenhum nome real de empresa, cliente ou pessoa; e-mails só `@example.com`.
- Nenhum segredo real. Senhas de desenvolvimento só em compose/exemplos, marcadas `dev-only-...`. Conectores leem segredos por `${env:...}`.
- Docs em português do Brasil; identificadores de código em inglês.
- `internal/telemetry` é puro: sem Kafka, sem relógio de parede, sem I/O.
- `internal/aggregation` fala com infraestrutura só pelas portas de `ports.go`. Teste novo comportamento ali com fakes em memória.
- Não mude a ordem publish → checkpoint → commit em `Service.HandleBatch` sem atualizar o roteiro de garantias e os testes de restart.
- Erro transitório (registry, broker) nunca manda mensagem para a DLQ.
- Nada gerado pelo perfil seguro (`.secure/`: CA, chaves, senhas, JAAS) entra no Git nem no contexto de build (`.dockerignore`); `internal/hygiene` falha se entrar. Um guardrail citado em ADR ou comentário precisa estar aplicado no código e testado pelo ponto de entrada.
- Schema novo: acrescente um arquivo `vN.avsc` em `schemas/avro/` e a versão em `schemas/catalog.go`. Campos novos são opcionais com default. Nunca edite uma versão já publicada.
- Decisão de arquitetura nova vira ADR em `docs/adr/` (Contexto, Decisão, Consequências, Alternativas).
- Commits em Conventional Commits, mensagem via arquivo (`git commit -F`), terminando com a linha de coautoria.

## Comandos

```bash
make test              # unitários (-race, cobertura)
make test-integration  # Redpanda via testcontainers; pula sem Docker
make lint              # gofmt, go vet (com e sem tag integration), golangci-lint
make schemas-check     # portão de compatibilidade
make build             # binários em ./bin
make up / ps / logs / down / clean / seed
make loadtest LOAD_RATE=2000 LOAD_DURATION=60s LOAD_GREENHOUSES=200
make secure-init       # material do perfil seguro em .secure/ (nunca versionado)
make up-secure         # perfil secure do compose (TLS, mTLS, SASL, ACLs, registry autenticado)
```

## Estrutura

| Caminho | Conteúdo |
| --- | --- |
| `cmd/simulator` | Produtor e ferramenta de carga |
| `cmd/aggregator` | Consumidor de agregação |
| `cmd/notifier` | Consumidor de alertas que avisa o produtor rural (e-mail, webhook) |
| `cmd/telemetryctl` | `init`, `topics`, `schemas check`, `schemas register`, `dlq replay` |
| `internal/telemetry` | Domínio: `Reading`, `PartitionWindows`, `Deduplicator`, `Thresholds` |
| `internal/contract` | Formato de fio, `Encoder`/`Decoder`, `CheckCompatibility`, registros Avro |
| `internal/aggregation` | `Service` e portas |
| `internal/kafka` | Adaptadores franz-go: `GroupConsumer`, `Publisher`, `CheckpointStore`, `Registry`, admin |
| `internal/observability` | Prometheus, slog, OpenTelemetry |
| `internal/api` | API de consulta no agregador, autenticada por token de operador |
| `internal/dlq` | Reprocessador da DLQ: republicação e backfill de janelas fechadas (Redis) |
| `internal/notify` | Notificador: canais, idempotência, retry durável |
| `internal/hygiene` | Regras do repositório (segredos, mounts do perfil seguro, dashboards, verificador de carga) |
| `internal/state` | Último estado por estufa no Redis (`RedisStore`, decorador `Instrumented`, `GET /greenhouses/{id}/state`) |
| `e2e/evidencia` | Evidência em vídeo com Playwright (`make evidencia`) |
| `schemas/` | `.avsc` versionados e fixtures de mudanças incompatíveis em `testdata/` |
| `connect/` | Dockerfile do Connect e conectores JDBC e S3 |
| `web/` | Front-end Canteiro (Vue) |
| `deploy/` | DDL do Postgres, Prometheus (alertas e SLOs), Grafana, Helm, ACLs do perfil seguro, limiares da carga noturna |

## Fluxo de trabalho

1. Branch a partir da `main` (`feat/...`, `fix/...`).
2. Teste primeiro no domínio ou no caso de uso; depois o adaptador.
3. `make lint test` antes de cada commit; `make test-integration` se mexeu em `internal/kafka`.
4. Atualize README, design doc, roteiros ou ADRs quando a mudança afetar o que eles descrevem.
5. PR com o template; o CI precisa passar (lint, compatibilidade de schemas, testes, integração e build).
