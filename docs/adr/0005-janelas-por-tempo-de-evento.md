# 0005. Janelas tumbling por tempo de evento com watermark por partição

- Status: Aceito; o watermark por partição foi evoluído pelo [ADR 0014](0014-watermark-por-estufa-e-ocioso.md) (por estufa e ocioso)
- Data: 2026-11-21

## Contexto

Leituras chegam com atraso e fora de ordem (gateways com conexão instável retransmitem lotes). Agregar por horário de chegada misturaria minutos e tornaria o reprocessamento não determinístico.

## Decisão

- Janelas **tumbling de 1 minuto** por estufa e grandeza, pelo `measured_at` (tempo de evento).
- **Watermark por partição** = maior `measured_at` visto − tolerância (10 s por padrão). Uma janela fecha quando seu fim é ≤ watermark.
- Leituras de janelas já fechadas são descartadas como atrasadas (métrica `outcome="late"`).
- Retenção do deduplicador também em tempo de evento, presa ao watermark.

## Consequências

- Reprocessar o mesmo trecho do tópico produz os mesmos agregados.
- O agregado de um minuto sai cerca de 10 s depois do fim do minuto, desde que chegue tráfego na partição.
- Uma partição sem tráfego não avança o watermark e segura suas janelas; com 20+ estufas por partição isso não acontece na prática. Evolução: watermark ocioso por tempo de processamento.
- Um sensor com relógio adiantado (até 5 min aceitos) adianta o watermark da partição e faz leituras de outras estufas virarem atrasadas; mitigação futura é watermark por estufa.

## Alternativas consideradas

- **Tempo de processamento**: simples, mas não determinístico e sensível a lag.
- **Watermark por estufa**: mais preciso, porém o offset seguro e o checkpoint ficariam por estufa e o estado cresce com o número de estufas.
- **Janelas deslizantes**: mais caras e desnecessárias para o painel de 1 minuto.
