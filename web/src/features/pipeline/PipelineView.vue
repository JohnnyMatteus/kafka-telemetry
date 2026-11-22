<script setup lang="ts">
import { computed } from 'vue'
import { useDlq, usePipelineHealth } from '@/api/queries'
import StateBlock from '@/components/StateBlock.vue'
import { formatDateTime, formatNumber } from '@/domain/format'
import { LAG_SLO } from '@/domain/pipeline'

const health = usePipelineHealth()
const dlq = useDlq()

const scaleMax = computed(() => Math.max(LAG_SLO * 1.5, ...(health.data.value?.partitions.map((p) => p.lag) ?? [0])))
const pct = (n: number) => `${(n / scaleMax.value) * 100}%`
const headers = [
  { title: 'Quando', key: 'timestamp' },
  { title: 'Motivo', key: 'reason' },
  { title: 'Origem', key: 'origin', sortable: false },
  { title: 'Erro', key: 'error', sortable: false },
]
</script>

<template>
  <v-container fluid class="pa-4" style="max-width: 1100px">
    <h1 class="text-h5 mb-1">Pipeline</h1>
    <p class="text-body-2 text-medium-emphasis mb-4">Visão para engenharia de dados: atraso do consumidor, vazão e mensagens rejeitadas.</p>

    <v-skeleton-loader v-if="health.isPending.value" type="card" />
    <StateBlock v-else-if="health.isError.value" tone="error" title="Saúde do pipeline indisponível" text="O agregador não respondeu em /pipeline/health." />
    <v-row v-else-if="health.data.value">
      <v-col cols="12" md="4">
        <v-card class="pa-4 h-100">
          <div class="text-caption text-medium-emphasis">Atraso total do consumidor</div>
          <div class="reading text-h5" :class="health.data.value.total_lag > LAG_SLO ? 'text-error' : 'text-primary'">
            {{ formatNumber(health.data.value.total_lag) }} mensagens
          </div>
          <div class="text-body-2">Meta: até {{ formatNumber(LAG_SLO) }} · grupo {{ health.data.value.group }} ({{ health.data.value.group_state }})</div>
        </v-card>
      </v-col>
      <v-col cols="12" md="4">
        <v-card class="pa-4 h-100">
          <div class="text-caption text-medium-emphasis">Taxa de entrada</div>
          <div class="reading text-h5">{{ formatNumber(health.data.value.throughput_records_per_second, 1) }} leituras/s</div>
          <div class="text-body-2">Média dos últimos {{ health.data.value.throughput_window_seconds }} s no tópico {{ health.data.value.topic }}</div>
        </v-card>
      </v-col>
      <v-col cols="12" md="4">
        <v-card class="pa-4 h-100">
          <div class="text-caption text-medium-emphasis">Mensagens na DLQ</div>
          <div class="reading text-h5" :class="health.data.value.dlq_messages ? 'text-error' : 'text-primary'">
            {{ formatNumber(health.data.value.dlq_messages) }}
          </div>
          <div class="text-body-2">{{ health.data.value.dlq_messages ? 'Leituras rejeitadas esperando análise, abaixo.' : 'Nenhuma leitura rejeitada.' }}</div>
        </v-card>
      </v-col>
      <v-col cols="12">
        <v-card class="pa-4">
          <h2 class="text-subtitle-1 mb-3">Atraso por partição</h2>
          <div v-for="p in health.data.value.partitions" :key="p.partition" class="lag-row" role="img"
            :aria-label="`Partição ${p.partition}: atraso ${p.lag}, ${p.lag > LAG_SLO ? 'acima' : 'dentro'} da meta de ${LAG_SLO}`">
            <span class="label-narrow">P{{ p.partition }}</span>
            <div class="track">
              <div class="corridor" :style="{ width: pct(LAG_SLO) }" />
              <div class="bar" :class="p.lag > LAG_SLO ? 'hot' : 'ok'" :style="{ width: pct(Math.max(p.lag, 0)) }" />
            </div>
            <span class="reading">{{ formatNumber(p.lag) }} {{ p.lag > LAG_SLO ? '▲' : '' }}</span>
          </div>
          <p class="text-caption text-medium-emphasis mt-2 mb-0">A faixa verde tracejada é a meta de atraso; barra laranja passou dela.</p>
        </v-card>
      </v-col>
    </v-row>

    <h2 class="text-subtitle-1 mt-6 mb-2">Fila de rejeitadas (DLQ)</h2>
    <v-skeleton-loader v-if="dlq.isPending.value" type="table" />
    <StateBlock v-else-if="dlq.isError.value" tone="error" title="Não foi possível ler a DLQ" />
    <StateBlock v-else-if="dlq.data.value?.length === 0" tone="success" title="DLQ vazia" text="Nenhuma leitura foi rejeitada pela validação." />
    <v-card v-else>
      <v-data-table :headers="headers" :items="dlq.data.value ?? []" item-value="offset">
        <template #item.timestamp="{ item }"><span class="reading">{{ formatDateTime(item.timestamp) }}</span></template>
        <template #item.reason="{ item }"><v-chip color="error">{{ item.reason }}</v-chip></template>
        <template #item.origin="{ item }"><span class="reading">{{ item.source_topic }}/{{ item.source_partition }}@{{ item.source_offset }}</span></template>
        <template #item.error="{ item }">{{ item.error ?? '' }}</template>
      </v-data-table>
    </v-card>
  </v-container>
</template>

<style scoped>
.lag-row { display: grid; grid-template-columns: 36px 1fr 90px; align-items: center; gap: 12px; margin-bottom: 6px; }
.track { position: relative; height: 18px; background: rgb(var(--v-theme-surface-variant)); }
.corridor { position: absolute; inset: 0 auto 0 0; background: rgb(var(--v-theme-band-fill)); border-right: 2px dashed rgb(var(--v-theme-band-edge)); }
.bar { position: absolute; left: 0; top: 5px; height: 8px; }
.bar.ok { background: rgb(var(--v-theme-primary)); }
.bar.hot { background: rgb(var(--v-theme-dev-hot)); }
</style>
