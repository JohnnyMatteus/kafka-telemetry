# 0016. Perfil seguro: TLS, mTLS, SASL, ACLs por serviço e registry autenticado

- Status: Aceito
- Data: 2026-11-22

## Contexto

O compose padrão fala com o Kafka em texto claro e sem autenticação, e o Schema Registry é aberto. Serve para desenvolvimento, mas o desenho de garantias (quem pode escrever leituras, quem lê alertas, quem registra schema) só vale se o broker e o registry impuserem isso. É preciso um ambiente que mostre a segurança funcionando sem versionar nenhum segredo.

## Decisão

- **Cliente (código)**: `kafka.Security` e `kafka.RegistrySecurity` (`internal/kafka/security.go`), lidos de `KAFKA_TLS_*`, `KAFKA_SASL_*` e `SCHEMA_REGISTRY_*`, passam por todos os clientes: consumidor, produtor, admin, inspeção, checkpoint, simulador, notificador, agregador e `telemetryctl`. A senha vem de arquivo (`*_PASSWORD_FILE`). A partida recusa SASL sem TLS, certificado sem chave, mecanismo desconhecido e senha do registry por `http` fora de loopback. Sem nenhuma variável, nada muda (profile padrão).
- **Profile `secure` do compose**: `kafka-secure` (KRaft, dois listeners `SASL_SSL` com `ssl.client.auth=required`, ou seja, mTLS mais SASL/PLAIN, `StandardAuthorizer`, `allow.everyone.if.no.acl.found=false`, superusuários só `broker` e `ops`), `schema-registry-secure` (HTTPS e login HTTP basic, um usuário por serviço, falando com o Kafka como `schemaregistry`), `acls-secure` (aplica `deploy/secure/acls.sh`), `init-secure`, `simulator-secure`, `aggregator-secure` e `notifier-secure`. Nenhuma porta do broker é publicada; a API do agregador só em `127.0.0.1`.
- **ACLs por serviço** (`deploy/secure/acls.sh`, versionado, sem segredo): o simulador só escreve leituras; o agregador lê leituras, escreve agregados, alertas, DLQ e checkpoints, lê DLQ e checkpoints, republica leituras (reprocessador) e só descreve o grupo do conector (prova do sink do backfill); o notificador só lê alertas e o grupo dele; o registry é dono de `_schemas`. O resto é negado.
- **Nenhum segredo no Git**: `make secure-init` (`scripts/secure-init.sh`, compatível com bash 3.2) gera em `.secure/` (no `.gitignore`) uma CA de desenvolvimento válida por 30 dias, um certificado por serviço, uma senha aleatória por serviço (SASL e registry), os JAAS e os keystores PKCS12. O compose só tem caminhos para esses arquivos e uma única variável, `SECURE_STORE_PASSWORD`, vinda de `--env-file .secure/secure.env`, com padrão vazio para `compose config` passar sem o material. `internal/hygiene` falha o CI se algum arquivo versionado for chave, keystore ou senha, ou se o compose tiver segredo literal.

## Consequências

- **O profile nunca foi executado.** Foi validado por `docker compose config -q`, pelos testes do código cliente (TLS com CA, mTLS, SASL, basic auth contra um servidor TLS local) e por uma execução do `secure-init.sh` que confirmou a estrutura dos arquivos e que os certificados verificam contra a CA. A configuração do broker e do registry (variáveis `KAFKA_*`, `SCHEMA_REGISTRY_*`, JAAS, ACLs) segue a documentação das imagens Confluent, mas não foi exercitada contra um broker; o primeiro `make up-secure` pode exigir ajustes. O `keytool` do gerador, sem JDK local, cai para o contêiner `cp-kafka`, que também não foi executado.
- O basic auth do registry **autentica, mas não autoriza por operação**: qualquer usuário válido lê e registra. Separar leitura de escrita exige o plugin de segurança do Confluent ou um proxy. Hoje a separação real está só no Kafka (quem escreve em `_schemas` é o registry).
- SASL/PLAIN com TLS e certificado de cliente: a senha trafega cifrada e a identidade é a do SASL (o principal da ACL). SCRAM exigiria criar as credenciais no armazenamento do KRaft antes do broker aceitar conexões; o cliente já o suporta (`KAFKA_SASL_MECHANISM=SCRAM-SHA-512`).
- **Fora do profile**: Kafka Connect (JDBC e S3), MinIO, web e observabilidade. Portanto o Postgres não é alimentado ali, e o backfill e as séries da API ficam sem dados. Conectar o Connect exige SASL/TLS no worker, nos consumidores e produtores dele e autenticação do conversor Avro nos arquivos de conector; fica pendente.
- O certificado dura 30 dias e não há rotação; `make secure-init` com `--force` refaz tudo (os volumes do broker precisam ser recriados junto, porque os usuários mudam).
- Senhas e chaves ficam em arquivos legíveis do diretório local (permissão para o usuário do contêiner): é material de desenvolvimento, não de produção. Em produção, os certificados e as credenciais vêm de um cofre.

## Alternativas

- **SASL/SCRAM**: credenciais no broker em vez de em um JAAS estático; melhor para produção, mas exige um passo de bootstrap por usuário.
- **Só TLS, sem SASL**: a identidade viria só do CN do certificado (`ssl.principal.mapping.rules`); mais simples, mas o PLAIN sobre TLS também protege a senha e permite o mesmo conjunto de ACLs.
- **Overlay `docker-compose.secure.yml`**: evita duplicar serviços, mas a mistura de listeners e hostnames fica mais frágil que serviços com nomes próprios num profile.
