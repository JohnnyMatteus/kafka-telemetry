# 0001. Avro com Schema Registry como formato dos tópicos

- Status: Aceito
- Data: 2026-11-21

## Contexto

Os sensores das estufas da AgroVale Exemplo mudam de firmware ao longo do tempo e cada versão pode mandar campos novos. Produtor (gateways e simulador), agregador, Kafka Connect e quem ler o data lake precisam concordar sobre o formato, e um produtor não pode quebrar consumidores que ainda não foram atualizados. Precisamos de um contrato verificável por máquina, com regras de evolução, e que o Kafka Connect entenda sem código próprio.

## Decisão

Usar **Avro** com o **Confluent Schema Registry** e a TopicNameStrategy (`<tópico>-value`). As mensagens usam o formato de fio do registry (byte mágico 0 + id do schema em 4 bytes + payload Avro). Os schemas ficam versionados em `schemas/avro/`, com todas as versões de cada subject, e o produtor usa sempre a mais nova.

## Consequências

- O Kafka Connect lê os tópicos com o `AvroConverter` e o sink JDBC e o S3 sink derivam colunas e arquivos Parquet do schema, sem mapeamento manual.
- O consumidor decodifica qualquer versão: busca o schema do escritor pelo id (com cache) e decodifica para um registro Go que conhece a versão mais nova.
- Mensagens são compactas (sem nomes de campo no payload), o que importa com milhares de leituras por segundo.
- Depender do registry no caminho de leitura cria um ponto de falha; o consumidor separa "schema inexistente" (DLQ) de "registry indisponível" (retry com backoff, sem DLQ).
- Avro exige schema também para ler; ferramentas genéricas precisam do registry.

## Alternativas consideradas

- **Protobuf**: geração de código forte e boa evolução por números de campo, mas o ganho principal (tipagem gerada) pesa menos em Go com structs simples, e Avro é o formato mais natural para Kafka Connect e Parquet. Regras de compatibilidade do Protobuf no registry são menos intuitivas para remoção de campos.
- **JSON Schema**: legível, mas mensagens maiores e compatibilidade mais fraca (o JSON Schema não tem defaults para resolução de leitor/escritor como o Avro).
- **JSON sem schema**: zero governança; uma mudança de campo quebra consumidores em produção e só aparece em runtime.
