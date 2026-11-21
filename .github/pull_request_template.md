## O que muda

<!-- Uma ou duas frases sobre o problema e a solução. -->

## Como testar

- [ ] `make test`
- [ ] `make test-integration` (se mexeu em adaptadores Kafka)
- [ ] `make schemas-check` (se mexeu em `schemas/`)

## Contratos e garantias

- [ ] Mudança de schema é BACKWARD_TRANSITIVE (campo novo é opcional com default)
- [ ] Ordem publish → checkpoint → commit preservada
- [ ] Nenhuma mensagem válida pode ir para a DLQ por erro transitório

## Checklist

- [ ] ADR criado ou atualizado se houve decisão de arquitetura
- [ ] Documentação (README, design doc, roteiros) atualizada
- [ ] Sem segredos ou dados reais
