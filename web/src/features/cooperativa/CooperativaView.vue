<script setup lang="ts">
import { mdiClose, mdiOpenInNew } from '@mdi/js'
import { useClock } from '@/domain/clock'
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAlerts, useGreenhouses, useThresholds } from '@/api/queries'
import GreenhouseGlyph from '@/components/signature/GreenhouseGlyph.vue'
import Legend from '@/components/signature/Legend.vue'
import StateBlock from '@/components/StateBlock.vue'
import { alertSentence, UNITS } from '@/domain/alerts'
import { formatTime, shortName } from '@/domain/format'
import { isOutside, quadrantStates } from '@/domain/glyph'
import GreenhouseCharts from '../estufa/GreenhouseCharts.vue'

const route = useRoute()
const router = useRouter()
const now = useClock(30_000)
const greenhouses = useGreenhouses()
const thresholds = useThresholds()
const open = useAlerts('open')

const selected = computed(() => (route.query.estufa as string | undefined) ?? null)
const select = (id: string | null) => router.replace({ query: id ? { estufa: id } : {} })

const list = computed(() => [...(greenhouses.data.value ?? [])].sort((a, b) => a.greenhouse_id.localeCompare(b.greenhouse_id)))
const outsideCount = computed(() =>
  thresholds.data.value
    ? list.value.filter((g) => quadrantStates(g, thresholds.data.value!, now.value).some(isOutside)).length
    : 0,
)
const lastReading = computed(() => {
  const ends = list.value.flatMap((g) => Object.values(g.sensors).map((s) => s!.window_end.getTime()))
  return ends.length ? new Date(Math.max(...ends)) : null
})
const selectedAlerts = computed(() => (open.data.value ?? []).filter((a) => a.greenhouse_id === selected.value))
const loading = computed(() => greenhouses.isPending.value || thresholds.isPending.value)
const failed = computed(() => greenhouses.isError.value || thresholds.isError.value)
</script>

<template>
  <div class="pa-4">
    <header class="d-flex align-baseline ga-4 flex-wrap mb-4">
      <h1 class="text-h5">Cooperativa AgroVale Exemplo</h1>
      <span v-if="list.length" class="text-body-1 tnum" :class="outsideCount ? 'text-warning' : 'text-primary'">
        <strong>{{ outsideCount }}</strong> de {{ list.length }} estufas fora da faixa
      </span>
      <span v-if="lastReading" class="reading text-medium-emphasis">última leitura às {{ formatTime(lastReading) }}</span>
    </header>

    <div v-if="loading" class="grid" aria-busy="true" aria-label="Carregando estufas">
      <v-skeleton-loader v-for="n in 12" :key="n" type="image" width="96" height="96" :style="{ borderRadius: 0 }" />
    </div>
    <StateBlock
      v-else-if="failed" tone="error" title="Sem conexão com a API de telemetria"
      text="Não consegui ler o estado das estufas. A grade volta sozinha quando o agregador responder."
    />
    <StateBlock
      v-else-if="list.length === 0" title="Nenhuma estufa enviou leitura ainda"
      text="Quando o simulador ou os sensores publicarem, os ladrilhos aparecem aqui."
    />
    <div v-else class="grid">
      <button
        v-for="g in list" :key="g.greenhouse_id" type="button" class="tile-button"
        :aria-pressed="selected === g.greenhouse_id" :class="{ active: selected === g.greenhouse_id }"
        @click="select(selected === g.greenhouse_id ? null : g.greenhouse_id)"
      >
        <GreenhouseGlyph :greenhouse="g" :thresholds="thresholds.data.value!" :now="now" />
      </button>
    </div>

    <Legend class="mt-4 legend-fixed" />

    <v-navigation-drawer :model-value="!!selected" temporary location="right" width="440" @update:model-value="(v: boolean) => !v && select(null)">
      <div v-if="selected" class="pa-4">
        <div class="d-flex align-center mb-3">
          <h2 class="text-h6">Estufa {{ shortName(selected) }}</h2>
          <v-spacer />
          <v-btn :to="{ name: 'estufa', params: { id: selected } }" variant="text" :append-icon="mdiOpenInNew">Detalhes</v-btn>
          <v-btn icon variant="text" aria-label="Fechar painel" @click="select(null)"><v-icon :icon="mdiClose" /></v-btn>
        </div>
        <p class="text-caption text-medium-emphasis mb-3">Última hora, média de 1 min com mínimo e máximo; o corredor verde é a faixa agronômica.</p>
        <GreenhouseCharts :id="selected" :hours="1" :now="now" :height="130" />
        <h3 class="text-subtitle-1 mt-4 mb-2">Alertas abertos</h3>
        <StateBlock v-if="selectedAlerts.length === 0" tone="success" title="Dentro da faixa" text="Nenhum alerta aberto para esta estufa." />
        <v-alert v-for="a in selectedAlerts" :key="a.alert_id" type="warning" class="mb-2">{{ alertSentence(a, UNITS[a.sensor_type], now) }}</v-alert>
      </div>
    </v-navigation-drawer>
  </div>
</template>

<style scoped>
.grid { display: flex; flex-wrap: wrap; gap: 4px; }
.tile-button { padding: 0; border: 0; background: none; cursor: pointer; border-radius: 0; line-height: 0; }
.tile-button.active { outline: 3px solid rgb(var(--v-theme-accent)); outline-offset: 1px; }
.legend-fixed { max-width: 860px; }
@media (max-width: 1280px) { .grid :deep(.tile), .grid :deep(svg) { width: 72px !important; height: 72px !important; } }
</style>
