# 0014. Watermark por estufa e watermark ocioso

- Status: Aceito (evolui a decisão de watermark por partição do [ADR 0005](0005-janelas-por-tempo-de-evento.md))
- Data: 2026-11-22

## Contexto

O ADR 0005 deixou dois limites registrados: uma partição sem tráfego não avança o watermark e segura suas janelas, e um sensor com relógio adiantado (até 5 min aceitos) adianta o watermark da partição inteira e faz leituras de outras estufas virarem atrasadas.

## Decisão

- **Watermark por estufa** (`WATERMARK_SCOPE=greenhouse`, o padrão do agregador): cada estufa tem o seu watermark (maior `measured_at` dela menos a tolerância), o seu deduplicador e a sua decisão de "atrasada". `partition` mantém o comportamento anterior. A chave da partição, ou da estufa, é a única coisa que muda em `telemetry.PartitionWindows`; o offset seguro continua um só por partição (primeira leitura de qualquer janela aberta), então o commit não ficou por estufa.
- **Watermark ocioso** (`WATERMARK_IDLE_TIMEOUT`, 2 min por padrão; 0 desliga): se uma chave (estufa, ou a partição) fica esse tempo, em tempo de **processamento**, sem leituras, `Service.Tick` fecha as janelas abertas dela cujo fim mais a tolerância já passou no relógio de processamento, e eleva o watermark da chave ao fim da última janela fechada. Não avança o watermark além disso, então nenhuma leitura volta a entrar numa janela já emitida: uma leitura tardia daquela janela é descartada como atrasada. O `GroupConsumer` acorda a cada segundo (`TickInterval`) para chamar `Tick` quando não chega registro.
- **Garantia de entrega intacta**: `Tick` e `HandleBatch` compartilham o mesmo trecho final (`flush`): publicar com ack, atualizar a projeção, gravar o checkpoint, commitar. Teste de ordem no `Tick`.
- **Checkpoint gravado quando o offset OU algum watermark muda** (o commit só quando o offset muda). Uma estufa com janela aberta segura o offset enquanto as outras fecham janelas; sem gravar os watermarks delas, um crash releria do offset antigo e republicaria janelas já emitidas.
- **Checkpoint**: o JSON ganha `watermarks_ms` (estufa → ms; a chave `""` é o piso para estufas ainda não vistas). `watermark_ms` continua sendo o maior valor. Um checkpoint antigo, sem o mapa, vale como piso para todas as estufas; trocar o escopo para `partition` usa o maior watermark de todas, para nunca reabrir uma janela emitida. O reprocessador da DLQ (ADR 0012) usa o watermark da estufa da leitura.

## Consequências

- Uma estufa calada não segura mais as janelas dela além do timeout, e um relógio adiantado só prejudica a própria estufa.
- O estado cresce com o número de estufas por partição (um deduplicador e três timestamps por estufa vista); com a chave de partição igual à estufa, cada estufa vive em uma partição só, então o total é o número de estufas do tópico.
- O fechamento ocioso supõe que o tempo de evento acompanha o de processamento. Uma estufa que reenvia leituras antigas depois de ficar offline mais que `WATERMARK_IDLE_TIMEOUT + tolerância` as vê descartadas como atrasadas; o timeout é o botão para esse equilíbrio. Antes do `Tick`, essas leituras entravam, mas a janela nunca fechava enquanto a estufa ficasse calada.
- O comportamento de janelas fechadas e atrasadas muda de granularidade: o limite de "atrasada" é o da estufa, não o da partição. A métrica de watermark por partição passa a mostrar o mais avançado entre as estufas.
- Não há teste de integração do `Tick` com um broker real nesta máquina; o caminho é coberto com o `Service` e fakes em memória, e o laço do `GroupConsumer` (poll com prazo) só é validado pelo CI de integração, que não o exercita hoje.

## Alternativas

- **Só watermark ocioso na partição**: resolve a partição calada, mas não o relógio adiantado de uma estufa.
- **Avançar o watermark ocioso até `agora - tolerância`**: fecharia mais janelas, mas assume que o tempo de evento é o de parede e descartaria leituras legítimas de um backlog.
- **Offset seguro por estufa**: o consumo é por partição, então o commit por estufa não existe no Kafka; seria só estado extra.
