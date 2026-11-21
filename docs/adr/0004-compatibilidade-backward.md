# 0004. Compatibilidade BACKWARD no registry e BACKWARD_TRANSITIVE no CI

- Status: Aceito
- Data: 2026-11-21

## Contexto

Em telemetria, os consumidores (agregador, Connect) costumam ser atualizados antes dos milhares de gateways no campo. O tópico também é relido do início em reprocessamentos, então a versão nova do consumidor precisa ler dados antigos.

## Decisão

- Subjects configurados como **BACKWARD** no Schema Registry (`telemetryctl schemas register` aplica a configuração antes de registrar).
- O CI exige **BACKWARD_TRANSITIVE** sobre as versões do repositório (`telemetryctl schemas check` e `TestRepositorySchemasAreBackwardCompatible`), mais forte que o registry, para que um replay longo nunca quebre.
- Testes documentam os casos: adicionar campo opcional com default passa; adicionar campo obrigatório sem default falha; trocar tipo falha; remover campo obrigatório **passa** em BACKWARD e falha em FORWARD/FULL.
- O teste de integração confere que o registry real concorda com o portão local.

## Consequências

- Consumidores podem ser atualizados primeiro; produtores antigos continuam funcionando.
- Remover um campo é permitido por BACKWARD, mas quebraria consumidores antigos ainda em operação: a política do time é deprecar (tornar opcional) antes de remover.
- A checagem local usa a biblioteca Avro do Go, que é mais estrita que o registry em enums com default; o registry é a fonte da verdade e por isso também é consultado no registro.

## Alternativas consideradas

- **FULL / FULL_TRANSITIVE**: protege também consumidores antigos, mas obriga todo campo novo a ter default e proíbe remoção de vez; avaliar quando houver consumidores externos à cooperativa.
- **FORWARD**: privilegia produtores atualizados primeiro, o oposto da realidade dos gateways.
- **NONE**: nenhuma proteção.
