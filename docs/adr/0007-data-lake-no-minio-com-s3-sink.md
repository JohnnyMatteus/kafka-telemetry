# 0007. Data lake da telemetria bruta no MinIO com S3 sink em Parquet

- Status: Aceito
- Data: 2026-11-21

## Contexto

O agrônomo da cooperativa quer analisar meses de leituras brutas (curvas de temperatura, correlação CO2 x luminosidade) e treinar modelos. O tópico de leituras retém 7 dias, e guardar tudo no Postgres seria caro e lento para análise colunar. Os agregados de 1 minuto vão para o Postgres; falta um arquivo barato e durável do dado bruto.

## Decisão

- **MinIO** como object storage compatível com S3 no ambiente local (imagem `bitnamilegacy/minio`, versão fixada, com o bucket criado na subida).
- Conector **S3 sink** da Confluent no Kafka Connect (`connect/greenhouse-readings-s3-archive.json`) arquivando `greenhouse.readings` em **Parquet** (snappy), particionado por hora do `measured_at` (`raw/greenhouse.readings/year=/month=/day=/hour=`), com `schema.compatibility=BACKWARD`.
- Endpoint e credenciais por variável de ambiente (`S3_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`): o mesmo JSON roda na AWS trocando as variáveis (ou usando IAM role).
- Mensagens que o conversor Avro não consegue ler vão para `greenhouse.readings.connect-dlq`.

## Consequências

- Data lake consultável por Athena/Trino/DuckDB sem código próprio; partição Hive por hora facilita poda de partições.
- O arquivamento é independente do agregador: um bug no agregador não afeta o dado bruto, que pode ser reprocessado.
- O S3 sink grava cada arquivo uma vez por offset (nomes determinísticos por partição e offset inicial), então reprocessamentos sobrescrevem em vez de duplicar.
- Mais um conector para operar e mais memória no Connect.
- As imagens oficiais do MinIO deixaram de ser publicadas no Docker Hub; usamos a imagem empacotada pela Bitnami (legado), fixada por versão. Em produção o destino é o S3 da AWS.

## Alternativas consideradas

- **LocalStack**: emula vários serviços AWS, mas o domínio só precisa de S3; MinIO é mais leve e tem console próprio.
- **JSON no lugar de Parquet**: legível, porém 5 a 10 vezes maior e sem leitura colunar.
- **Produzir para S3 a partir do agregador**: acopla o dado bruto ao agregador e reimplementa o que o Connect já faz.
- **Aumentar a retenção do tópico (tiered storage)**: mantém o dado no Kafka, mas não serve para consulta analítica.
