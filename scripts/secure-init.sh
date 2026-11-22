#!/usr/bin/env bash
# Gera o material do profile `secure` em .secure/ (ignorado pelo git): uma CA
# de desenvolvimento, um certificado por serviço (mTLS), uma senha aleatória
# por serviço (SASL e Schema Registry), os JAAS e os keystores do lado Java.
# Nada disto é versionado e nada é reaproveitado entre máquinas.
#
#   ./scripts/secure-init.sh            gera, se ainda não existir
#   ./scripts/secure-init.sh --force    apaga e gera de novo
#
# Requer openssl e keytool (um JDK); sem keytool local, usa o do contêiner
# confluentinc/cp-kafka, que o profile já baixa.
set -euo pipefail

DIR="${SECURE_DIR:-$(cd "$(dirname "$0")/.." && pwd)/.secure}"
IMAGE="${KAFKA_IMAGE:-confluentinc/cp-kafka:7.9.1}"
SERVICES=(aggregator simulator notifier)   # clientes Kafka da aplicação
REGISTRY_USERS=(aggregator simulator notifier ops)

if [[ "${1:-}" == "--force" ]]; then rm -rf "$DIR"; fi
if [[ -f "$DIR/secure.env" ]]; then echo "$DIR já existe (use --force para refazer)"; exit 0; fi
mkdir -p "$DIR"
umask 022   # leitura para o usuário do contêiner; é um diretório de desenvolvimento local

rand() { openssl rand -base64 24 | tr -d '/+=\n'; }

keytool_run() {
  if keytool -help >/dev/null 2>&1; then keytool "$@"   # o stub do macOS existe sem JDK; por isso testa de verdade
  else docker run --rm -v "$DIR:/work" -w /work --entrypoint keytool "$IMAGE" "$@"; fi
}

# --- CA --------------------------------------------------------------------
openssl req -x509 -newkey rsa:3072 -nodes -days 30 -subj "/CN=kafka-telemetry-dev-ca" \
  -keyout "$DIR/ca.key" -out "$DIR/ca.crt" 2>/dev/null

# cert NOME CN [SAN]: chave PKCS#8, certificado assinado pela CA (cliente e servidor).
cert() {
  local name="$1" cn="$2" san="${3:-}"
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out "$DIR/$name.key" 2>/dev/null
  openssl req -new -key "$DIR/$name.key" -subj "/CN=$cn" -out "$DIR/$name.csr"
  {
    echo "extendedKeyUsage=serverAuth,clientAuth"
    [[ -n "$san" ]] && echo "subjectAltName=$san"
  } > "$DIR/$name.ext"
  openssl x509 -req -in "$DIR/$name.csr" -CA "$DIR/ca.crt" -CAkey "$DIR/ca.key" -CAcreateserial \
    -days 30 -extfile "$DIR/$name.ext" -out "$DIR/$name.crt" 2>/dev/null
  rm -f "$DIR/$name.csr" "$DIR/$name.ext"
}

STORE_PASSWORD="$(rand)"

cert kafka-secure kafka-secure "DNS:kafka-secure,DNS:localhost"
cert schema-registry-secure schema-registry-secure "DNS:schema-registry-secure,DNS:localhost"
for s in "${SERVICES[@]}" ops schemaregistry; do cert "$s" "$s"; done

# --- keystores do lado Java (broker e registry) ------------------------------
p12() { # NOME: PKCS12 com chave + certificado + CA
  openssl pkcs12 -export -inkey "$DIR/$1.key" -in "$DIR/$1.crt" -certfile "$DIR/ca.crt" \
    -name "$1" -passout "pass:$STORE_PASSWORD" -out "$DIR/$1.keystore.p12"
}
p12 kafka-secure
p12 schema-registry-secure
p12 schemaregistry   # cliente do registry no Kafka (mTLS)
keytool_run -importcert -noprompt -alias ca -file ca.crt -storetype PKCS12 \
  -keystore truststore.p12 -storepass "$STORE_PASSWORD" >/dev/null
# Convenção das imagens Confluent: arquivos *_creds com a senha.
for f in keystore_creds key_creds truststore_creds; do printf '%s' "$STORE_PASSWORD" > "$DIR/$f"; done

# --- senhas por serviço ------------------------------------------------------
# bash 3.2 (macOS) não tem arrays associativos: as senhas ficam em arquivos e
# são lidas de volta por kpw/rpw.
kpw() { cat "$DIR/$1.password"; }
rpw() { cat "$DIR/registry-$1.password"; }
for s in "${SERVICES[@]}" ops schemaregistry broker; do printf '%s' "$(rand)" > "$DIR/$s.password"; done
for s in "${REGISTRY_USERS[@]}"; do printf '%s' "$(rand)" > "$DIR/registry-$s.password"; done

# --- JAAS do broker: usuários SASL/PLAIN (a conexão já é TLS com certificado de cliente)
{
  echo 'KafkaServer {'
  echo '  org.apache.kafka.common.security.plain.PlainLoginModule required'
  echo "  username=\"broker\" password=\"$(kpw broker)\""
  for s in "${SERVICES[@]}" ops schemaregistry broker; do
    echo "  user_$s=\"$(kpw "$s")\""
  done | sed '$ s/$/;/'
  echo '};'
} > "$DIR/kafka_server_jaas.conf"

# --- JAAS do registry: login HTTP basic + cliente Kafka (sem segredo no compose)
{
  echo 'SchemaRegistry {'
  echo '  org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required'
  echo '  file="/etc/schema-registry/secrets/registry-users.properties" debug="false";'
  echo '};'
  echo 'KafkaClient {'
  echo '  org.apache.kafka.common.security.plain.PlainLoginModule required'
  echo "  username=\"schemaregistry\" password=\"$(kpw schemaregistry)\";"
  echo '};'
} > "$DIR/registry_jaas.conf"
for s in "${REGISTRY_USERS[@]}"; do echo "$s: $(rpw "$s"),admin"; done > "$DIR/registry-users.properties"

# --- propriedades do cliente de administração (kafka-acls) --------------------
cat > "$DIR/ops.properties" <<PROPS
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="ops" password="$(kpw ops)";
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/secrets/truststore.p12
ssl.truststore.password=$STORE_PASSWORD
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/kafka/secrets/ops.keystore.p12
ssl.keystore.password=$STORE_PASSWORD
PROPS
p12 ops

# O compose lê só a senha dos keystores daqui (--env-file); o resto são arquivos.
printf 'SECURE_STORE_PASSWORD=%s\n' "$STORE_PASSWORD" > "$DIR/secure.env"
chmod 600 "$DIR"/*.key "$DIR/secure.env" 2>/dev/null || true
echo "material do profile secure gerado em $DIR (válido por 30 dias, não versionado)"
echo "suba com: make up-secure"
