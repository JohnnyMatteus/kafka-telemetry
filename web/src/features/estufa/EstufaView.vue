<script setup lang="ts">
import { mdiArrowLeft } from '@mdi/js'
import { useClock } from '@/domain/clock'
import { computed, ref } from 'vue'
import { useAlerts, useThresholds } from '@/api/queries'
import { SENSOR_TYPES } from '@/api/schemas'
import StateBlock from '@/components/StateBlock.vue'
import { alertSentence, UNITS } from '@/domain/alerts'
import { formatDateTime, formatNumber, shortName } from '@/domain/format'
import { SENSOR_META } from '@/domain/icons'
import GreenhouseCharts from './GreenhouseCharts.vue'

const props = defineProps<{ id: string }>()
const hours = ref(6)
const now = useClock(60_000)
const thresholds = useThresholds()
const open = useAlerts('open')
const closed = useAlerts('acknowledged')
const events = computed(() =>
  [...(open.data.value ?? []), ...(closed.data.value ?? [])]
    .filter((a) => a.greenhouse_id === props.id)
    .sort((a, b) => b.window_start.getTime() - a.window_start.getTime()),
)
</script>

<template>
  <v-container fluid class="pa-4">
    <div class="d-flex align-center ga-3 mb-4 flex-wrap">
      <v-btn :to="{ name: 'cooperativa' }" variant="text" :prepend-icon="mdiArrowLeft">Cooperativa</v-btn>
      <h1 class="text-h5">Estufa {{ shortName(id) }}</h1>
      <span class="reading text-medium-emphasis">{{ id }}</span>
      <v-spacer />
      <v-btn-toggle v-model="hours" mandatory density="comfortable" color="primary" variant="outlined" aria-label="Período">
        <v-btn :value="1">1 h</v-btn>
        <v-btn :value="6">6 h</v-btn>
        <v-btn :value="24">24 h</v-btn>
      </v-btn-toggle>
    </div>
    <v-row>
      <v-col cols="12" lg="8">
        <v-card class="pa-4">
          <GreenhouseCharts :id="id" :hours="hours" :now="now" :height="150" />
        </v-card>
      </v-col>
      <v-col cols="12" lg="4">
        <v-card class="pa-4 mb-4">
          <h2 class="text-subtitle-1 mb-2">Faixa agronômica</h2>
          <v-skeleton-loader v-if="thresholds.isPending.value" type="paragraph" />
          <ul v-else class="list-unstyled">
            <li v-for="t in SENSOR_TYPES" :key="t" class="d-flex justify-space-between py-1">
              <span><v-icon :icon="SENSOR_META[t].icon" size="18" class="mr-1" />{{ SENSOR_META[t].label }}</span>
              <span v-if="thresholds.data.value?.[t]" class="reading">
                {{ formatNumber(thresholds.data.value[t]!.min, 1) }} a {{ formatNumber(thresholds.data.value[t]!.max, 1) }} {{ thresholds.data.value[t]!.unit }}
              </span>
            </li>
          </ul>
          <p class="text-caption text-medium-emphasis mt-2 mb-0">Limites configurados no agregador; a média de 1 min fora deles abre alerta.</p>
        </v-card>
        <v-card class="pa-4">
          <h2 class="text-subtitle-1 mb-2">Eventos</h2>
          <StateBlock v-if="events.length === 0" tone="success" title="Nenhum alerta nesta estufa" text="Nenhuma grandeza saiu da faixa no histórico disponível." />
          <ul v-else class="list-unstyled">
            <li v-for="a in events" :key="a.alert_id" class="py-2 border-b">
              <div class="d-flex justify-space-between align-baseline">
                <span class="text-body-2">{{ alertSentence(a, UNITS[a.sensor_type], now) }}</span>
                <v-chip :color="a.acknowledged_at ? 'info' : 'warning'">{{ a.acknowledged_at ? 'reconhecido' : 'aberto' }}</v-chip>
              </div>
              <div class="reading text-medium-emphasis">{{ formatDateTime(a.window_start) }} · média {{ formatNumber(a.observed_value, 1) }}</div>
              <div v-if="a.note" class="text-caption mt-1">Nota: {{ a.note }}</div>
            </li>
          </ul>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<style scoped>
.list-unstyled { list-style: none; padding: 0; margin: 0; }
</style>
