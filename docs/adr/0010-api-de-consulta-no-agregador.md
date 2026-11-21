# 0010. API de consulta no processo do agregador

- Status: Aceito
- Data: 2026-11-21

## Contexto

O painel precisa de último estado, séries, alertas com reconhecimento, DLQ, faixas agronômicas e saúde do pipeline. As fontes são Redis (estado), Postgres (séries e alertas) e Kafka (DLQ e lag). O agregador já tem um servidor HTTP (`METRICS_ADDR`, `:9102`) com `/metrics`, `/healthz` e `GET /greenhouses/{id}/state`, e o front fala com um único upstream via proxy `/api`.

## Decisão

- A API roda **no processo do agregador**, no mesmo servidor HTTP e na mesma porta. Não há binário novo.
- É um adaptador de entrada em `internal/api`, que só conhece portas (`state.Reader`/`Lister`, `history.Repository`, `alerting.Repository`, `pipeline.DLQReader`/`HealthReporter`). Os adaptadores de saída são `state.RedisStore`, `postgres` e `kafka.Inspector`.
- Cada fonte é opcional. Sem `REDIS_ADDR`, `POSTGRES_DSN` ou Kafka, as rotas que dependem dela não são montadas. `GET /thresholds` sempre existe.
- As faixas agronômicas vêm de `THRESHOLD_<TIPO>_MIN|MAX`, lidas uma vez na partida. O mesmo valor alimenta a avaliação de alertas e `GET /thresholds`, então a API nunca mostra uma faixa diferente da que gerou os alertas. Mínimo >= máximo impede a partida.
- O reconhecimento de alerta é a única escrita da API: colunas `acknowledged_at` e `note` em `greenhouse_alerts`, atualizadas por `UPDATE ... WHERE acknowledged_at IS NULL`. O sink JDBC faz upsert apenas das colunas do registro, então um replay do alerta não apaga o reconhecimento. As colunas também são criadas por `ALTER TABLE ... IF NOT EXISTS` na partida, para volumes antigos.
- As consultas ao Kafka usam um cliente próprio (`kafka.Inspector`), separado do consumidor do pipeline, com timeout de 5 s.

## Consequências

- Uma porta, um upstream, sem novo serviço no compose: o proxy do front fica trivial.
- A API compartilha ciclo de vida e CPU com o consumo. Consultas são leves e limitadas (limite máximo de 500 itens, intervalo de séries de até 31 dias), mas uma carga alta de leitura disputa recursos com o pipeline.
- O agregador passa a depender do Postgres na inicialização quando `POSTGRES_DSN` está definido (migração das colunas).
- Escalar a API separada do consumo exige extraí-la: como `internal/api` só depende de portas, isso vira um `cmd/api` com o mesmo `Routes`, sem mudar os handlers.
- A API não tem autenticação; o ack identifica quando, não quem. Ficam para a evolução junto com SASL/mTLS.

## Alternativas consideradas

- **Binário `cmd/api` separado**: isola recursos e escala sozinho, mas adiciona um serviço, uma imagem e uma porta para uma carga pequena. Fica como evolução.
- **Consultar o Kafka direto para séries e alertas**: o tópico não é consultável por intervalo; o Postgres já é o destino idempotente dessas projeções.
