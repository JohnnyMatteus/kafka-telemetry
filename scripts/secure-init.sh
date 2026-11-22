#!/usr/bin/env bash
# Gera o material do profile `secure` em .secure/ (ignorado pelo git): uma CA
# de desenvolvimento, um certificado e uma senha por serviço (SASL e registry),
# os JAAS e os keystores do lado Java.
#
# Cada principal tem o PRÓPRIO diretório, e o compose monta em cada contêiner
# só o dele:
#   .secure/ca/          ca.key (nunca montado em contêiner algum)
#   .secure/broker/      keystore e truststore do broker, JAAS com todos os usuários SASL
#   .secure/registry/    keystores do registry, JAAS e usuários HTTP basic
#   .secure/ops/         credenciais do superusuário (job de ACLs e de init)
#   .secure/<serviço>/   aggregator, simulator, notifier: ca.crt, certificado, chave e senhas dele
# O diretório raiz fica com permissão 0700 (só o dono da máquina atravessa); dentro dos
# diretórios de principal os arquivos são legíveis (0644), porque o usuário do contêiner
# não é o dono no host.
#
#   ./scripts/secure-init.sh            gera, se ainda não existir
#   ./scripts/secure-init.sh --force    refaz, mas só apaga um diretório que este script criou
#
# Requer openssl e keytool (um JDK); sem keytool funcional, usa o do contêiner
# confluentinc/cp-kafka.
set -euo pipefail

DIR="${SECURE_DIR:-$(cd "$(dirname "$0")/.." && pwd)/.secure}"
IMAGE="${KAFKA_IMAGE:-confluentinc/cp-kafka:7.9.1}"
MARKER=".created-by-secure-init"
CLIENTS=(aggregator simulator notifier)
REGISTRY_USERS=(aggregator simulator notifier ops healthcheck)

die() { echo "erro: $*" >&2; exit 1; }

# Nunca apaga nem escreve num caminho que o script não criou.
[[ "$DIR" == /* ]] || die "SECURE_DIR precisa ser um caminho absoluto"
case "$DIR" in
  /|"$HOME"|"$HOME"/) die "recusado: $DIR não pode ser a raiz nem o diretório pessoal" ;;
esac
[[ ! -L "$DIR" ]] || die "$DIR é um link simbólico"
if [[ -e "$DIR" ]]; then
  if [[ "${1:-}" == "--force" ]]; then
    [[ -f "$DIR/$MARKER" ]] || die "recusado: $DIR não tem o marcador $MARKER, então não foi criado por este script; apague à mão se for o caso"
    rm -rf -- "$DIR"
  elif [[ -f "$DIR/secure.env" ]]; then
    echo "$DIR já existe (use --force para refazer)"; exit 0
  elif [[ -n "$(ls -A "$DIR")" ]]; then
    die "recusado: $DIR existe, não está vazio e não é meu; informe outro SECURE_DIR"
  fi
fi
umask 022
mkdir -p "$DIR"
touch "$DIR/$MARKER"
chmod 700 "$DIR"
for d in ca broker registry ops "${CLIENTS[@]}"; do mkdir -p "$DIR/$d"; done
chmod 700 "$DIR/ca"

rand() { openssl rand -base64 24 | tr -d '/+=\n'; }

keytool_run() { # roda no diretório $1
  local wd="$1"; shift
  if keytool -help >/dev/null 2>&1; then (cd "$wd" && keytool "$@")   # o stub do macOS existe sem JDK; por isso testa de verdade
  else docker run --rm -v "$wd:/work" -w /work --entrypoint keytool "$IMAGE" "$@"; fi
}

# --- CA --------------------------------------------------------------------
openssl req -x509 -newkey rsa:3072 -nodes -days 30 -subj "/CN=kafka-telemetry-dev-ca" \
  -keyout "$DIR/ca/ca.key" -out "$DIR/ca/ca.crt" 2>/dev/null
chmod 600 "$DIR/ca/ca.key"
for d in broker registry ops "${CLIENTS[@]}"; do cp "$DIR/ca/ca.crt" "$DIR/$d/ca.crt"; done

# cert DIRETÓRIO NOME CN [SAN]: chave PKCS#8 e certificado assinado pela CA no diretório do principal.
cert() {
  local dest="$1" name="$2" cn="$3" san="${4:-}" w="$DIR/ca/tmp-$2"
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out "$dest/$name.key" 2>/dev/null
  openssl req -new -key "$dest/$name.key" -subj "/CN=$cn" -out "$w.csr"
  { echo "extendedKeyUsage=serverAuth,clientAuth"; [[ -n "$san" ]] && echo "subjectAltName=$san"; true; } > "$w.ext"
  openssl x509 -req -in "$w.csr" -CA "$DIR/ca/ca.crt" -CAkey "$DIR/ca/ca.key" -CAcreateserial \
    -days 30 -extfile "$w.ext" -out "$dest/$name.crt" 2>/dev/null
  rm -f "$w.csr" "$w.ext"
}

# p12 DIRETÓRIO NOME: PKCS12 com chave, certificado e CA, no diretório do principal.
STORE_PASSWORD="$(rand)"
p12() {
  openssl pkcs12 -export -inkey "$1/$2.key" -in "$1/$2.crt" -certfile "$1/ca.crt" \
    -name "$2" -passout "pass:$STORE_PASSWORD" -out "$1/$2.keystore.p12"
}
truststore() { # DIRETÓRIO: truststore com a CA e os arquivos *_creds que as imagens Confluent esperam
  keytool_run "$1" -importcert -noprompt -alias ca -file ca.crt -storetype PKCS12 \
    -keystore truststore.p12 -storepass "$STORE_PASSWORD" >/dev/null
  for f in keystore_creds key_creds truststore_creds; do printf '%s' "$STORE_PASSWORD" > "$1/$f"; done
}

# --- certificados e keystores ------------------------------------------------
cert "$DIR/broker" kafka-secure kafka-secure "DNS:kafka-secure,DNS:localhost"
p12 "$DIR/broker" kafka-secure
truststore "$DIR/broker"
cert "$DIR/registry" schema-registry-secure schema-registry-secure "DNS:schema-registry-secure,DNS:localhost"
p12 "$DIR/registry" schema-registry-secure
cert "$DIR/registry" schemaregistry schemaregistry      # cliente do registry no Kafka (mTLS)
p12 "$DIR/registry" schemaregistry
truststore "$DIR/registry"
rm -f "$DIR/registry"/schemaregistry.key "$DIR/registry"/schemaregistry.crt "$DIR/registry"/schema-registry-secure.key "$DIR/registry"/schema-registry-secure.crt
cert "$DIR/ops" ops ops
p12 "$DIR/ops" ops
truststore "$DIR/ops"
for s in "${CLIENTS[@]}"; do cert "$DIR/$s" "$s" "$s"; done

# --- senhas: cada uma só no diretório de quem a usa ---------------------------
# bash 3.2 (macOS) não tem arrays associativos: lidas de volta de arquivos.
for s in "${CLIENTS[@]}" ops; do printf '%s' "$(rand)" > "$DIR/$s/$s.password"; done
for s in "${CLIENTS[@]}" ops; do printf '%s' "$(rand)" > "$DIR/$s/registry-$s.password"; done
printf '%s' "$(rand)" > "$DIR/broker/broker.password"
printf '%s' "$(rand)" > "$DIR/registry/schemaregistry.password"
printf '%s' "$(rand)" > "$DIR/registry/registry-healthcheck.password"
for s in "${CLIENTS[@]}" ops; do cp "$DIR/$s/registry-$s.password" "$DIR/registry/registry-$s.password"; done
kpw() { cat "$DIR/${2:-$1}/$1.password"; }   # kpw USUÁRIO [DIRETÓRIO]

# --- JAAS do broker: usuários SASL/PLAIN (a conexão já é TLS com certificado de cliente).
{
  echo 'KafkaServer {'
  echo '  org.apache.kafka.common.security.plain.PlainLoginModule required'
  echo "  username=\"broker\" password=\"$(kpw broker broker)\""
  for s in "${CLIENTS[@]}" ops; do echo "  user_$s=\"$(kpw "$s")\""; done
  echo "  user_schemaregistry=\"$(kpw schemaregistry registry)\""
  echo "  user_broker=\"$(kpw broker broker)\";"
  echo '};'
} > "$DIR/broker/kafka_server_jaas.conf"

# Cliente do próprio broker (healthcheck): usuário broker, com o keystore dele.
cat > "$DIR/broker/broker.properties" <<PROPS
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="broker" password="$(kpw broker broker)";
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/secrets/truststore.p12
ssl.truststore.password=$STORE_PASSWORD
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/kafka/secrets/kafka-secure.keystore.p12
ssl.keystore.password=$STORE_PASSWORD
PROPS

# --- JAAS do registry: login HTTP basic + cliente Kafka (sem segredo no compose)
{
  echo 'SchemaRegistry {'
  echo '  org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required'
  echo '  file="/etc/schema-registry/secrets/registry-users.properties" debug="false";'
  echo '};'
  echo 'KafkaClient {'
  echo '  org.apache.kafka.common.security.plain.PlainLoginModule required'
  echo "  username=\"schemaregistry\" password=\"$(kpw schemaregistry registry)\";"
  echo '};'
} > "$DIR/registry/registry_jaas.conf"
for s in "${REGISTRY_USERS[@]}"; do echo "$s: $(cat "$DIR/registry/registry-$s.password"),admin"; done > "$DIR/registry/registry-users.properties"

# --- cliente de administração (kafka-acls): só no diretório do ops ------------
cat > "$DIR/ops/ops.properties" <<PROPS
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

# O compose lê só a senha dos keystores daqui (--env-file); nenhum contêiner monta este arquivo.
printf 'SECURE_STORE_PASSWORD=%s\n' "$STORE_PASSWORD" > "$DIR/secure.env"
chmod 600 "$DIR/secure.env"

# Permissões: o usuário do contêiner não é o dono no host, então os arquivos dos
# diretórios de principal são legíveis; a raiz 0700 impede o acesso de outros
# usuários da máquina, e cada contêiner só vê o diretório dele.
for d in broker registry ops "${CLIENTS[@]}"; do
  chmod 755 "$DIR/$d"
  find "$DIR/$d" -type f -exec chmod 644 {} +
done
echo "material do profile secure gerado em $DIR (válido por 30 dias, não versionado)"
echo "suba com: make up-secure"
