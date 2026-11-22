# 0015. Backfill de janelas fechadas

- Status: Aceito (completa o [ADR 0012](0012-reprocessador-da-dlq.md))
- Data: 2026-11-22

## Contexto

O reprocessador da DLQ republica a leitura no tópico de leituras. Quando a causa é corrigida horas depois, a janela da leitura já fechou pelo watermark e o agregador a descarta como `late`; republicar só produziria um falso sucesso. O ADR 0012 passou a bloquear essas leituras (`would_be_late`) e deixou o caminho de recuperação em aberto.

## Decisão

Um modo **backfill** no mesmo caso de uso (`Request.Backfill`; `POST /dlq/replay` com `"backfill":true`; `telemetryctl dlq replay -backfill`) que, para as leituras de janela já fechada, **recalcula o agregado da janela e o republica** em `greenhouse.aggregates`, onde o sink JDBC faz upsert por (estufa, grandeza, início). Se o novo agregado ultrapassa o limite, o alerta é reavaliado e publicado em `greenhouse.alerts`; o `alert_id` é determinístico, então um alerta já existente é o mesmo registro e o notificador não o repete.

- **O que se soma**: o agregado já emitido da janela (lido do Postgres na primeira vez, `postgres.Aggregates`) mais as leituras recuperadas. Média ponderada pela contagem, mínimo e máximo combinados.
- **Idempotência por `event_id`**: no Redis, por janela, ficam (a) a **fotografia** do agregado emitido, tirada na primeira vez, (b) o acumulado das leituras recuperadas e (c) o conjunto de `event_id` já somados, tudo atualizado por um script atômico. O agregado publicado é **sempre recalculado a partir da fotografia**, nunca do que está no Postgres (que já pode ter o resultado anterior do próprio backfill), então repetir o pedido, ou perder os recibos da DLQ, nunca soma uma leitura duas vezes.
- **Retomada depois de falha**: a janela é marcada pendente quando o acumulado muda e só deixa de ser quando a publicação é confirmada (comparando a versão). Se o processo cai, o broker está fora ou o prazo acaba entre somar e publicar, a próxima execução com `-execute` publica as pendentes primeiro.
- **Não depende do cursor da republicação**: o backfill tem ledger e cursor próprios (`dlq:backfill:*`), enquanto o cursor da republicação passa por cima das atrasadas. As duas operações compartilham o lock, então nunca rodam juntas. Cada uma pula o que a outra já resolveu.
- **Guardrails herdados**: dry-run por padrão (mostra, por janela, base, leituras novas, resultado e alerta), teto de lote, operador obrigatório e gravado nos headers (`dlq.backfilled.by`, `dlq.backfill.run`), prazo menor que o TTL do lock, destinos fixos por configuração (os tópicos de agregados e alertas, nunca do pedido). Só recupera janelas cujo fim passou há mais de `DLQ_BACKFILL_MIN_AGE` (5 min), para o agregado original já ter chegado ao Postgres; as mais novas ficam como `too_recent` e voltam numa execução seguinte.
- Exige Redis e Postgres. Sem o Postgres, o pedido recebe `501` e o comando falha.

## Consequências

- Uma leitura que também foi aceita pelo agregador sob outro offset (retransmissão com schema conhecido enquanto a primeira foi para a DLQ) seria contada duas vezes: o backfill só deduplica entre as leituras que ele próprio soma, porque o conjunto de `event_id` do agregador vive na memória dele.
- Quem lê o Postgres vê o agregado antigo até o sink aplicar o novo (segundos). A sequência no tópico é a verdade: o agregado republicado vem depois do original, e o upsert deixa o último.
- O Redis é a fonte do estado do backfill. Perder o Redis perde as fotografias e os conjuntos; um novo backfill da mesma janela partiria do Postgres, que já tem o resultado anterior, e somaria de novo. Backup do Redis, ou não limpá-lo, faz parte da operação.
- O estado de último estado das estufas (Redis do agregador) não é atualizado pelo backfill: ele guarda a janela mais nova de cada estufa e ignora uma mais antiga.
- Não há teste de integração com Kafka e Postgres reais nesta máquina; o fluxo foi testado com o agregador de verdade, o Redis em memória e um leitor que lê do que o agregador publicou. O leitor do Postgres tem um teste de integração que só o CI roda.

## Alternativas

- **Recalcular a janela relendo o tópico de leituras**: exato, mas caro, e leituras de mais de 7 dias já saíram da retenção.
- **Somar sobre o que está no Postgres a cada execução**: simples, mas não idempotente, porque o Postgres já contém o resultado anterior.
- **Republicar a leitura e relaxar o watermark**: reabriria janelas emitidas para todo o fluxo ao vivo.
