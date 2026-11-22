# 0013. Notificador de alertas ao produtor rural

- Status: Aceito
- Data: 2026-11-22

## Contexto

Os alertas de limite chegam ao tópico `greenhouse.alerts`, ao Postgres e ao painel, mas o produtor rural só os vê se abrir o painel. É preciso avisá-lo ativamente, sem duplicar o aviso quando o mesmo alerta é reemitido (o agregador reemite janelas depois de um crash; o `alert_id` é determinístico justamente para isso) e sem perder o aviso quando o canal está fora do ar.

## Decisão

- Um binário próprio, `cmd/notifier`, consome `greenhouse.alerts` em um grupo próprio (`greenhouse-notifier`), reaproveitando o `GroupConsumer` com commit manual. O agregador não conhece o notificador.
- O caso de uso fica em `internal/notify`, com portas para o diretório de produtores, os canais (`Channel`) e o registro de avisos enviados (`Sent`). Adaptadores: e-mail por SMTP (`net/smtp`; Mailpit no compose), webhook HTTP assinado, diretório em arquivo JSON e `Sent` no Redis.
- **Quem recebe**: `NOTIFY_RECIPIENTS_FILE`, um JSON de produtores com e-mail e/ou URL de webhook e a lista de estufas de cada um. Não existe cadastro de produtores no pipeline; o arquivo é de operação e é validado na partida.
- **Idempotência**: antes de enviar, consulta a chave `alert_id + canal + produtor` no Redis; depois de enviar, marca (45 dias, além da retenção do tópico). Marcar depois, e não reservar antes, é proposital: uma reserva antes do envio perderia o aviso se o processo caísse no meio. O custo é um aviso repetido nessa janela rara. O webhook leva `X-Telemetry-Event-Id` para o receptor deduplicar.
- **Retry em dois níveis.** Primeiro, até `NOTIFY_MAX_ATTEMPTS` (5) tentativas rápidas por canal e destinatário, com backoff exponencial (200 ms a 5 s). Falha permanente (SMTP 5xx, webhook 4xx exceto 408 e 429, endereço inválido, URL não https, produtor sem segredo) não é repetida e é contada como `permanent_failure`. Se a falha transitória esgota as tentativas rápidas, o aviso vai para uma **fila durável e agendada no Redis** (zset de vencimentos + hash de jobs) e só então o offset é commitado: um SMTP fora por uma hora não perde o aviso. Um worker no próprio notificador processa os jobs vencidos a cada `NOTIFY_RETRY_INTERVAL` (5 s), com backoff exponencial de `NOTIFY_RETRY_BASE` (30 s) a 5 min, e reserva cada job por lease (um worker que cai o devolve sozinho). **Idade máxima** `NOTIFY_RETRY_MAX_AGE` (24 h): depois dela o aviso expira, com log de erro e `outcome="expired"`.
- **Observabilidade da fila**: `telemetry_notifier_retry_queue_depth`, `telemetry_notifier_retry_oldest_age_seconds` e o contador por resultado (`queued`, `failed`, `expired`, `sent`). Alertas Prometheus: `NotifierRetryQueueStale` (mais antigo > 10 min) e `NotifierDeliveryExpired` (expirou ou foi recusado em definitivo). Os canais são independentes entre si.
- Só um erro de infraestrutura (Redis, diretório, contexto cancelado) derruba o lote sem commit.
- **Webhook assinado, com segredo por produtor**: corpo JSON, cabeçalhos `X-Telemetry-Timestamp`, `X-Telemetry-Signature: v1=<hex HMAC-SHA256 de "timestamp.corpo">`. Cada produtor tem o seu segredo: o JSON de produtores traz só o **nome** de uma variável de ambiente (`webhook_secret_env`), nunca o segredo; a partida falha se a variável não existe ou tem menos de 16 caracteres. Assim, quem recebe o segredo para verificar os próprios webhooks não consegue forjar alertas no webhook de outro produtor. A URL tem de ser https (http só com `NOTIFY_WEBHOOK_ALLOW_HTTP=true`, para desenvolvimento) e redirecionamentos não são seguidos. `notify.Verify` é a verificação que o receptor deve fazer, com tolerância de tempo. O canal de webhook só liga se algum produtor tem `webhook_url`.
- **Entrada não confiável**: o tópico de alertas aceita outros escritores além do agregador. `alert_id` e `greenhouse_id` precisam casar com `[A-Za-z0-9_-]{1,64}`, senão o alerta é registrado e pulado; mesmo assim o `Message-ID` do e-mail é um hash do id e todo valor que entra em cabeçalho é saneado.
- E-mail: endereços e cabeçalhos são validados contra injeção de quebra de linha; STARTTLS é usado quando o servidor o anuncia; autenticação (`NOTIFY_SMTP_USER`/`PASSWORD`) só é enviada pelo `PlainAuth`, que recusa texto claro fora de localhost.

## Consequências

- Entrega at-least-once. Duplicatas só acontecem num crash entre o envio e a marcação, ou numa corrida de rebalance.
- Um aviso que passa de 24 h na fila expira (o alerta continua na API e no painel); avisar sobre um alerta de ontem confundiria mais do que ajudaria. Um aviso pode, no pior caso, sair com atraso de até a idade máxima.
- O Redis é ponto único da fila e da idempotência; sem ele o notificador para (o offset não avança) em vez de perder avisos.
- O adaptador de SMTP e o de webhook foram exercitados contra um servidor SMTP local de teste e receptores HTTP locais (o Mailpit do compose foi apenas validado com `docker compose config`, não executado nesta máquina), não contra um provedor real de e-mail nem um receptor de terceiros. Autenticação e TLS de um provedor real não foram exercitados.
- No compose, o Mailpit só publica a UI, em `127.0.0.1:8025`; o SMTP (1025) fica na rede interna porque o Mailpit aceita qualquer remetente. A porta de métricas do notificador também só escuta no loopback.
- Os produtores do compose (`deploy/notifier/recipients.json`) são fictícios, em `@example.com`.
- Não há preferências por produtor (horário, severidade) nem confirmação de leitura.

## Alternativas

- **Notificar de dentro do agregador**: acopla o pipeline de garantia de entrega a um canal externo lento e a mais um ponto de falha.
- **Kafka Connect HTTP/e-mail sink**: sem idempotência por destinatário nem assinatura de payload sob nosso controle.
- **Segredo HMAC único para todos os produtores**: qualquer produtor que o conhece forja alertas no webhook dos outros.
- **Só as tentativas rápidas, sem fila**: perde todos os avisos de um período de indisponibilidade do canal.
- **Reservar a chave antes de enviar**: evita duplicata, mas perde aviso em crash; preferimos o aviso repetido ao aviso perdido.
- **Cadastro de produtores no Postgres**: mais flexível, mas exige tela e API de cadastro que ninguém pediu ainda.
