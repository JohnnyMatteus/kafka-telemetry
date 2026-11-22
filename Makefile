# Alvos padronizados do projeto. `make help` lista todos.
SHELL := /bin/sh
COMPOSE ?= docker compose
PROFILES ?= --profile core --profile observability
ALL_PROFILES := --profile core --profile observability --profile tools
GOLANGCI_LINT_VERSION ?= v2.15.0
LOAD_DURATION ?= 60s
LOAD_RATE ?= 2000
LOAD_GREENHOUSES ?= 200

.DEFAULT_GOAL := help

.PHONY: help
help: ## Lista os alvos
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)


.PHONY: up
up: ## Sobe núcleo + observabilidade (Kafka KRaft, Schema Registry, Connect, Postgres, MinIO, apps, Prometheus, Jaeger)
	$(COMPOSE) $(PROFILES) up -d --build

.PHONY: up-tools
up-tools: ## Sobe também a Kafka UI (profile tools)
	$(COMPOSE) $(ALL_PROFILES) up -d --build

.PHONY: down
down: ## Para o ambiente (mantém os volumes)
	$(COMPOSE) $(ALL_PROFILES) down

.PHONY: logs
logs: ## Segue os logs (SERVICE=aggregator para um só)
	$(COMPOSE) $(ALL_PROFILES) logs -f --tail=100 $(SERVICE)

.PHONY: ps
ps: ## Estado dos serviços
	$(COMPOSE) $(ALL_PROFILES) ps

.PHONY: seed
seed: ## Recria tópicos/schemas, reaplica conectores e injeta 30 s de leituras
	$(COMPOSE) $(PROFILES) run --rm init
	$(COMPOSE) $(PROFILES) run --rm connectors
	$(COMPOSE) $(PROFILES) run --rm --no-deps simulator simulator -duration 30s -rate 200 -greenhouses 20

.PHONY: clean
clean: ## Remove containers, volumes e binários locais
	$(COMPOSE) $(ALL_PROFILES) down -v --remove-orphans
	go clean -testcache
	rm -f coverage.out
	rm -f bin/simulator bin/aggregator bin/telemetryctl

.PHONY: secure-init
secure-init: ## Gera CA, certificados, senhas e JAAS do profile secure em .secure/ (não versionado)
	./scripts/secure-init.sh

.PHONY: up-secure
up-secure: ## Sobe o profile secure: Kafka com TLS+mTLS+SASL+ACLs, registry autenticado, simulador, agregador e notificador
	@test -f .secure/secure.env || (echo "rode make secure-init antes" && exit 1)
	$(COMPOSE) --env-file .secure/secure.env --profile secure up -d --build

.PHONY: down-secure
down-secure: ## Para o profile secure (mantém os volumes)
	$(COMPOSE) --profile secure down

.PHONY: network
network: ## Cria a rede compartilhada do ecossistema (portfolio-net)
	docker network inspect portfolio-net >/dev/null 2>&1 || docker network create portfolio-net

.PHONY: up-portfolio
up-portfolio: network ## Sobe o ambiente também ligado à portfolio-net
	$(COMPOSE) -f docker-compose.yml -f docker-compose.portfolio.yml $(PROFILES) up -d --build


.PHONY: build
build: ## Compila os binários em ./bin
	CGO_ENABLED=0 go build -trimpath -o bin/ ./cmd/...

.PHONY: fmt
fmt: ## Formata o código
	gofmt -w .

.PHONY: lint
lint: ## gofmt, go vet e golangci-lint
	@test -z "$$(gofmt -l .)" || (gofmt -l . && echo "rode make fmt" && exit 1)
	go vet ./...
	go vet -tags integration ./...
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

.PHONY: test
test: ## Testes unitários com race detector e cobertura
	go test -race -count=1 -coverprofile=coverage.out ./...

.PHONY: test-integration
test-integration: ## Testes de integração (Redpanda via testcontainers; pula sem Docker)
	go test -tags integration -count=1 -timeout 10m ./...

.PHONY: schemas-check
schemas-check: ## Portão de compatibilidade dos schemas, sem rede
	go run ./cmd/telemetryctl schemas check

.PHONY: evidencia
evidencia: ## Sobe tudo e grava a evidência em vídeo (Playwright, e2e/evidencia)
	./scripts/evidencia.sh

.PHONY: loadtest
loadtest: ## Teste de carga contra o ambiente local (LOAD_RATE, LOAD_DURATION, LOAD_GREENHOUSES)
	./scripts/load-test.sh $(LOAD_RATE) $(LOAD_DURATION) $(LOAD_GREENHOUSES)

.PHONY: web-dev
web-dev: ## Front-end Canteiro em modo dev (Vite, proxy /api para o agregador em :9102)
	cd web && npm ci && npm run dev
