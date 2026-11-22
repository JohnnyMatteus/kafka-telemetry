#!/usr/bin/env bash
# Prova, de dentro da rede do compose e como o SIMULADOR, que o broker nega o
# que as ACLs e o mTLS devem negar. Roda no CI (job secure-profile) numa imagem
# cp-kafka, com o diretório .secure/simulator montado em /s. Não imprime
# senhas, JAAS nem propriedades: só o resultado de cada prova.
set -uo pipefail

B="${BOOTSTRAP:-kafka-secure:29092}"
S="${SECRETS_DIR:-/s}"
W="$(mktemp -d)"
trap 'rm -rf -- "$W"' EXIT
fail=0

cat "$S/simulator.key" "$S/simulator.crt" > "$W/client.pem"   # keystore PEM: chave PKCS#8 e depois o certificado
props() { # ARQUIVO SENHA COM_CERTIFICADO
  {
    echo "security.protocol=SASL_SSL"
    echo "sasl.mechanism=PLAIN"
    printf 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="simulator" password="%s";\n' "$2"
    echo "ssl.truststore.type=PEM"
    echo "ssl.truststore.location=$S/ca.crt"
    if [[ "$3" == "sim" ]]; then
      echo "ssl.keystore.type=PEM"
      echo "ssl.keystore.location=$W/client.pem"
    fi
  } > "$1"
}
props "$W/ok.properties" "$(cat "$S/simulator.password")" sim
props "$W/nocert.properties" "$(cat "$S/simulator.password")" nao
props "$W/badpass.properties" "senha-errada" sim

# prova NOME PADRÃO COMANDO...: o comando tem de falhar e a saída tem de casar com o padrão.
denied() {
  local name="$1" pattern="$2"; shift 2
  local out rc
  out="$(timeout 120 "$@" 2>&1)"; rc=$?
  if [[ $rc -ne 0 ]] && grep -Eq "$pattern" <<<"$out"; then
    echo "ok   negado: $name"
  else
    echo "FALHA $name (saída $rc; linhas relevantes abaixo)"
    grep -Ei "authoriz|handshake|authentication|timed out|exception" <<<"$out" | sed -E 's/(password|secret)[^ ]*/[redigido]/Ig' | head -5
    fail=1
  fi
}
allowed() {
  local name="$1"; shift
  local out rc
  out="$(timeout 120 "$@" 2>&1)"; rc=$?
  if [[ $rc -eq 0 ]] && grep -Eq "$ALLOWED_PATTERN" <<<"$out"; then
    echo "ok   permitido: $name"
  else
    echo "FALHA $name (saída $rc; linhas relevantes abaixo)"
    grep -Ei "exception|error|authoriz|handshake|authentication|skip getting" <<<"$out" | sed -E 's/(password|secret|jaas|keystore|truststore)[^ ]*/[redigido]/Ig' | head -8
    fail=1
  fi
}
# Variante do consumidor: o kafka-console-consumer sai com 0 mesmo quando o tópico
# é negado (loga TopicAuthorizationException e segue). Aqui o critério é a exceção
# de autorização no stderr E nenhum registro no stdout. Só para este caso.
denied_consumer() {
  local name="$1" pattern="$2"; shift 2
  local out err rc
  out="$(timeout 120 "$@" 2>"$W/err")"; rc=$?
  err="$(cat "$W/err")"
  if grep -Eq "$pattern" <<<"$err" && [[ -z "$(tr -d '[:space:]' <<<"$out")" ]]; then
    echo "ok   negado: $name (exceção de autorização e nenhum registro lido; saída $rc)"
  else
    echo "FALHA $name (saída $rc, registros lidos: $(grep -c . <<<"$out"))"
    grep -Ei "authoriz|exception|error" <<<"$err" | sed -E 's/(password|secret|jaas)[^ ]*/[redigido]/Ig' | head -5
    fail=1
  fi
}

# Controle positivo: com credenciais e certificado válidos o simulador autentica
# e lista os offsets do tópico que ele escreve; sem isto, uma negação abaixo poderia
# ser só um erro de configuração do cliente. kafka-get-offsets (Kafka 3.9.0,
# GetOffsetShell) só chama listTopics, describeTopics e listOffsets, ou seja, só
# exige Describe no tópico, que é o que o simulador tem. O kafka-topics --describe
# NÃO serve: chama também describeConfigs e describeCluster, que o simulador não
# tem e não deve ter.
ALLOWED_PATTERN='^greenhouse\.readings:[0-9]+:[0-9]+$'
allowed "simulador lista os offsets de greenhouse.readings" \
  kafka-get-offsets --bootstrap-server "$B" --command-config "$W/ok.properties" --topic greenhouse.readings --time -1

denied_consumer "simulador lê greenhouse.alerts" 'Not authorized to access topics: \[greenhouse\.alerts\]' \
  kafka-console-consumer --bootstrap-server "$B" --consumer.config "$W/ok.properties" \
  --topic greenhouse.alerts --partition 0 --offset earliest --timeout-ms 20000

# --sync é necessário: no kafka-console-producer 3.9 o envio assíncrono só LOGA o erro
# do callback e o processo sai com 0 (core/src/main/scala/kafka/tools/ConsoleProducer.scala,
# tag 3.9.0: send() com ErrorLoggingCallback; main() termina em Exit.exit(0)). Com
# --sync o send(...).get() lança e o processo sai com 1.
denied "simulador escreve em greenhouse.aggregator.checkpoints" 'Not authorized to access topics: \[greenhouse\.aggregator\.checkpoints\]' \
  bash -c "printf 'k:v\n' | kafka-console-producer --sync --bootstrap-server '$B' --producer.config '$W/ok.properties' --topic greenhouse.aggregator.checkpoints --property parse.key=true --property key.separator=:"

# Tem de falhar no handshake TLS (o broker exige certificado de cliente), não por
# timeout: o padrão não aceita "Timed out".
denied "cliente sem certificado (mTLS)" 'SSL handshake failed|SSLHandshakeException|bad_certificate|certificate_required' \
  kafka-get-offsets --bootstrap-server "$B" --command-config "$W/nocert.properties" --topic greenhouse.readings --time -1

denied "senha SASL errada" 'Authentication failed|SaslAuthenticationException' \
  kafka-get-offsets --bootstrap-server "$B" --command-config "$W/badpass.properties" --topic greenhouse.readings --time -1

# Repete o controle positivo depois das negações: prova que o broker seguia no ar
# e que as credenciais continuavam válidas, então nenhuma negação acima foi só um
# broker fora.
allowed "simulador ainda lista os offsets de greenhouse.readings (depois das negações)" \
  kafka-get-offsets --bootstrap-server "$B" --command-config "$W/ok.properties" --topic greenhouse.readings --time -1

exit "$fail"
