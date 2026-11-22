# 0017. Helm e autoscaling do agregador pelo lag

- Status: Aceito
- Data: 2026-11-22

## Contexto

O agregador é um consumidor de grupo: cada partição de `greenhouse.readings` tem um dono, e o estado (janelas, watermarks) é reconstruído a partir do checkpoint quando o dono muda ([ADR 0003](0003-at-least-once-com-idempotencia.md), [ADR 0014](0014-watermark-por-estufa-e-ocioso.md)). Escalar horizontalmente é, portanto, seguro, mas só até o número de partições, e o sinal certo de carga é o lag do grupo, não a CPU.

## Decisão

- Chart em `deploy/helm/kafka-telemetry`: agregador (Deployment, Service, PodDisruptionBudget, ServiceMonitor opcional), notificador (opcional, com o `recipients.json` num ConfigMap) e simulador (opcional, só para testes). Sondas em `/healthz`, contêiner sem root, sistema de arquivos somente leitura, sem capabilities.
- **O chart não cria Secrets.** Senhas e tokens vêm de Secrets existentes citados em `secrets.aggregator`, `kafka.security.secretName`, `schemaRegistry.auth.secretName` e `notifier.secretName`; TLS, mTLS e SASL seguem o [ADR 0016](0016-perfil-seguro-tls-sasl-acls.md) (senha por arquivo montado).
- **Autoscaling pelo lag com KEDA** (`autoscaling.enabled`): o chart cria um `ScaledObject` com o gatilho `kafka` (grupo do agregador, tópico de leituras, `lagThreshold` registros em atraso por réplica). O número de réplicas tende a `lag_total / lagThreshold`. O teto é `min(maxReplicas, kafka.partitions)`, e o chart recusa `minReplicas` acima das partições, porque réplicas além delas ficam ociosas. A descida é lenta (janela de estabilização de 5 min e uma réplica por minuto), porque cada mudança de réplicas é um rebalance. Com o autoscaling ligado o Deployment não fixa `replicas`.
- **Teste de carga noturno** (`.github/workflows/nightly-load.yml`, também sob demanda): sobe o agregador e o que ele precisa, gera carga com o simulador (`scripts/load-test.sh`, que agora também grava `latest.json`) e roda `scripts/loadtest-check.sh`, que reprova por piso absoluto (vazão efetiva, latência de ack p99, erros, consumo frente ao confirmado, drenagem do lag) e por regressão frente à última execução aprovada (guardada em cache): queda de vazão, alta de p99, de lag máximo e de tempo de drenagem. Campo ausente reprova; nada vira zero. A base só é atualizada depois de aprovada, para uma regressão não rebaixar a referência.

## Consequências

- **Nada disto foi exercitado nesta máquina.** Não há `helm` nem cluster aqui; o chart só será renderizado e conferido pelo job `helm` do CI (lint estrito e renderizações com asserts: sem Secrets, teto pelas partições, recusa de `minReplicas` inválido, exigência do Secret de segurança). O `ScaledObject` e o comportamento do KEDA com o grupo do agregador **não foram testados**, nem em cluster nem em kind. Os asserts do CI provam que o YAML sai como esperado, não que o escalonamento funciona.
- O teste de carga noturno **nunca rodou**. O verificador (`loadtest-check.sh`) tem teste unitário com resultados sintéticos (`internal/hygiene`), mas a carga em si, as medições reais e os pisos são hipóteses: os valores de `deploy/loadtest/thresholds.json` são conservadores e não foram calibrados; ajuste-os depois das primeiras execuções. Um runner compartilhado do GitHub tem ruído, e os limiares de regressão podem exigir folga maior.
- O gatilho `kafka` do KEDA lê o lag direto do broker e precisa de credenciais do broker se ele exigir SASL/TLS (`autoscaling.authenticationRef` aponta para um `TriggerAuthentication`, que o chart não cria).
- Escalar pelo lag não resolve um consumidor travado: lag que não cai com o máximo de réplicas pede investigação (alerta `AggregatorStalled` e SLO de lag).
- Não há probe de prontidão que espere o checkpoint; o grupo faz o rebalance e o `Assign` restaura o estado.

## Alternativas

- **HPA com métrica externa (`telemetry_aggregator_consumer_lag` via Prometheus Adapter)**: não exige KEDA, mas exige o adapter e uma regra de conversão por cluster, e a métrica é atualizada só quando um lote chega. O KEDA lê o lag no broker.
- **HPA por CPU**: o agregador fica limitado por I/O e por partição; CPU baixa com lag alto é o caso comum.
- **StatefulSet com réplicas fixas por partição**: o estado vive no checkpoint, então não há motivo para identidade estável.
