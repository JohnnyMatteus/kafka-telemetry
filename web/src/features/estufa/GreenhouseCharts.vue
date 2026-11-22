<script setup lang="ts">
import { computed } from 'vue'
import { useSeries, useThresholds } from '@/api/queries'
import { SENSOR_TYPES } from '@/api/schemas'
import BandChart from '@/components/signature/BandChart.vue'
import StateBlock from '@/components/StateBlock.vue'
import { SENSOR_META } from '@/domain/icons'

const props = defineProps<{ id: string; hours: number; now: Date; height?: number }>()

const series = useSeries(() => props.id, () => props.hours)
const thresholds = useThresholds()
const to = computed(() => props.now)
const from = computed(() => new Date(props.now.getTime() - props.hours * 3_600_000))
const digits = { TEMPERATURE: 1, HUMIDITY: 0, CO2: 0, LUMINOSITY: 0 } as const
</script>

<template>
  <v-skeleton-loader v-if="series.isPending.value || thresholds.isPending.value" type="image, image" />
  <StateBlock
    v-else-if="series.isError.value || thresholds.isError.value"
    tone="error" title="Não foi possível carregar as séries desta estufa"
    text="A API de consulta não respondeu. A tela tenta de novo a cada 30 s."
  />
  <div v-else class="d-flex flex-column ga-4">
    <template v-for="t in SENSOR_TYPES" :key="t">
      <BandChart
        v-if="series.data.value?.series[t] && thresholds.data.value?.[t]"
        :title="SENSOR_META[t].label"
        :points="series.data.value.series[t]!.points"
        :unit="series.data.value.series[t]!.unit"
        :band="thresholds.data.value[t]!"
        :digits="digits[t]"
        :height="height"
        :from="from"
        :to="to"
      />
    </template>
  </div>
</template>
