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
- **Retry**: até `NOTIFY_MAX_ATTEMPTS` (5) por canal e destinatário, com backoff exponencial (200 ms a 5 s). Falha permanente (SMTP 5xx, webhook 4xx exceto 408 e 429, endereço inválido, URL não https) não é repetida. Esgotadas as tentativas, a falha é contada (`telemetry_notifier_notifications_total{outcome="failed"}`), registrada e **não marcada**: o offset avança, e se o alerta chegar de novo ele é tentado outra vez. Um alerta tem os canais independentes entre si.
- Só um erro de infraestrutura (Redis, diretório, contexto cancelado) derruba o lote sem commit.
- **Webhook assinado**: corpo JSON, cabeçalhos `X-Telemetry-Timestamp`, `X-Telemetry-Signature: v1=<hex HMAC-SHA256 de "timestamp.corpo">`. O segredo vem de `NOTIFY_WEBHOOK_SECRET` (16 ou mais caracteres), nunca do arquivo de produtores. A URL tem de ser https (http só com `NOTIFY_WEBHOOK_ALLOW_HTTP=true`, para desenvolvimento) e redirecionamentos não são seguidos. `notify.Verify` é a verificação que o receptor deve fazer, com tolerância de tempo.
- E-mail: endereços e cabeçalhos são validados contra injeção de quebra de linha; STARTTLS é usado quando o servidor o anuncia; autenticação (`NOTIFY_SMTP_USER`/`PASSWORD`) só é enviada pelo `PlainAuth`, que recusa texto claro fora de localhost.

## Consequências

- Entrega at-least-once. Duplicatas só acontecem num crash entre o envio e a marcação, ou numa corrida de rebalance.
- Sem fila durável de tentativas: se o canal ficar fora além das tentativas, o aviso daquele alerta se perde (o alerta continua na API e no painel) e o alerta Prometheus `NotifierDeliveryFailing` dispara. Uma fila de reenvio seria uma decisão própria.
- O adaptador de SMTP e o de webhook foram exercitados contra um servidor SMTP local de teste, o Mailpit do compose e um receptor HTTP local, não contra um provedor real de e-mail nem um receptor de terceiros. Autenticação e TLS de um provedor real não foram exercitados.
- Os produtores do compose (`deploy/notifier/recipients.json`) são fictícios, em `@example.com`.
- Não há preferências por produtor (horário, severidade) nem confirmação de leitura.

## Alternativas

- **Notificar de dentro do agregador**: acopla o pipeline de garantia de entrega a um canal externo lento e a mais um ponto de falha.
- **Kafka Connect HTTP/e-mail sink**: sem idempotência por destinatário nem assinatura de payload sob nosso controle.
- **Reservar a chave antes de enviar**: evita duplicata, mas perde aviso em crash; preferimos o aviso repetido ao aviso perdido.
- **Cadastro de produtores no Postgres**: mais flexível, mas exige tela e API de cadastro que ninguém pediu ainda.
