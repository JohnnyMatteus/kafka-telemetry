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
  if timeout 120 "$@" >/dev/null 2>&1; then echo "ok   permitido: $name"; else echo "FALHA $name"; fail=1; fi
}

# Controle positivo: com credenciais e certificado válidos o simulador autentica
# e descreve o tópico que ele escreve; sem isto, uma negação abaixo poderia ser
# só um erro de configuração do cliente.
allowed "simulador descreve greenhouse.readings" \
  kafka-topics --bootstrap-server "$B" --command-config "$W/ok.properties" --describe --topic greenhouse.readings

denied "simulador lê greenhouse.alerts" 'Not authorized to access topics: \[greenhouse\.alerts\]' \
  kafka-console-consumer --bootstrap-server "$B" --consumer.config "$W/ok.properties" \
  --topic greenhouse.alerts --partition 0 --offset earliest --timeout-ms 20000

denied "simulador escreve em greenhouse.aggregator.checkpoints" 'Not authorized to access topics: \[greenhouse\.aggregator\.checkpoints\]' \
  bash -c "printf 'k:v\n' | kafka-console-producer --bootstrap-server '$B' --producer.config '$W/ok.properties' --topic greenhouse.aggregator.checkpoints --property parse.key=true --property key.separator=:"

denied "cliente sem certificado (mTLS)" 'SSL|handshake|authentication|Timed out|disconnected' \
  kafka-topics --bootstrap-server "$B" --command-config "$W/nocert.properties" --list

denied "senha SASL errada" 'Authentication failed|SaslAuthenticationException|Timed out' \
  kafka-topics --bootstrap-server "$B" --command-config "$W/badpass.properties" --list

exit "$fail"
