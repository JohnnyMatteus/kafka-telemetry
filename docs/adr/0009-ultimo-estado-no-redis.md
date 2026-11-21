# 0009. Último estado de cada estufa no Redis

- Status: Aceito
- Data: 2026-11-21

## Contexto

O painel e o futuro consumidor de alertas precisam responder "como está a estufa agora?" em milissegundos. O Postgres tem o histórico, mas a pergunta vira uma consulta por `max(window_start)` em cada grandeza, e o tópico de agregados não é consultável por chave.

## Decisão

- O agregador grava, depois do publish de cada lote, o agregado mais recente de cada grandeza num **hash por estufa** (`greenhouse:state:<id>`, um campo JSON por tipo de sensor).
- Um script Lua só sobrescreve com janela igual ou mais nova: o replay depois de um crash reemite janelas antigas e não pode regredir o estado.
- A gravação passa pela porta `aggregation.StateStore`; o adaptador `state.RedisStore` usa go-redis num pipeline (uma ida por lote).
- A projeção **não participa da garantia de entrega**: falha no Redis é registrada e contada (`telemetry_aggregator_state_updates_total{result="error"}`), sem impedir checkpoint e commit.
- Leitura em `GET /greenhouses/{id}/state` no servidor HTTP do agregador. Sem `REDIS_ADDR`, a projeção e a rota ficam desligadas.
- Sem TTL: uma estufa que parou de reportar continua visível com o `window_end` antigo, o que é mais útil do que sumir.

## Consequências

- Leitura O(1) por estufa, independente do tamanho do histórico.
- Depois de uma falha do Redis, o estado pode ficar uma janela atrás até a próxima janela fechar.
- Mais um serviço no compose (com AOF e volume, para sobreviver a restart).

## Alternativas consideradas

- **Consultar o Postgres**: correto, mas a consulta de "último por grandeza" custa mais e acopla o painel ao esquema do histórico.
- **Tópico compactado com o último estado**: exigiria um consumidor que materializa a tabela para responder por chave.
- **Gravar no Redis antes do checkpoint, como parte da garantia**: um Redis fora do ar pararia o pipeline por uma informação derivada.
