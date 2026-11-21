# Kafka Connect com os conectores versionados do projeto:
#   - JDBC sink (inclui o driver do PostgreSQL) para agregados e alertas;
#   - S3 sink para arquivar a telemetria bruta no MinIO (ou S3 de verdade).
FROM confluentinc/cp-kafka-connect:7.9.1
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.8.0 \
 && confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.6.21
