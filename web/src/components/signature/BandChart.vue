<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { useTheme } from 'vuetify'
import type { SeriesPoint } from '@/api/schemas'
import type { Band } from '@/domain/deviation'
import { formatNumber } from '@/domain/format'

const props = defineProps<{
  points: SeriesPoint[]
  band: Band
  unit: string
  title: string
  digits?: number
  height?: number
  from: Date
  to: Date
}>()

const el = ref<HTMLDivElement>()
let plot: uPlot | undefined
let observer: ResizeObserver | undefined
const theme = useTheme()
const colors = computed(() => theme.current.value.colors)

const data = computed<uPlot.AlignedData>(() => [
  props.points.map((p) => (p.window_start.getTime() + p.window_end.getTime()) / 2000),
  props.points.map((p) => p.mean),
  props.points.map((p) => p.min),
  props.points.map((p) => p.max),
])

const summary = computed(() => {
  const last = props.points.at(-1)
  if (!last) return 'sem leituras no período'
  const out = props.points.filter((p) => p.mean < props.band.min || p.mean > props.band.max).length
  return `última média ${formatNumber(last.mean, props.digits ?? 1)} ${props.unit}; ${out} de ${props.points.length} janelas fora da faixa`
})

// O corredor agronômico é desenhado antes das séries para ficar por baixo delas.
function corridor(u: uPlot) {
  const { ctx } = u
  const c = colors.value
  const top = u.valToPos(props.band.max, 'y', true)
  const bottom = u.valToPos(props.band.min, 'y', true)
  const { left, width } = u.bbox
  ctx.save()
  ctx.fillStyle = c['band-fill'] ?? '#CFE3C4'
  ctx.fillRect(left, top, width, bottom - top)
  ctx.strokeStyle = c['band-edge'] ?? '#5E8F55'
  ctx.lineWidth = 1.5 * devicePixelRatio
  ctx.setLineDash([6 * devicePixelRatio, 4 * devicePixelRatio])
  for (const y of [top, bottom]) {
    ctx.beginPath()
    ctx.moveTo(left, y)
    ctx.lineTo(left + width, y)
    ctx.stroke()
  }
  ctx.restore()
}

// Médias fora do corredor ganham a cor fria ou quente.
function outliers(u: uPlot) {
  const { ctx } = u
  const c = colors.value
  const xs = u.data[0]
  const ys = u.data[1] ?? []
  ctx.save()
  for (let i = 0; i < xs.length; i++) {
    const y = ys[i]
    if (y == null || (y >= props.band.min && y <= props.band.max)) continue
    ctx.fillStyle = (y > props.band.max ? c['dev-hot'] : c['dev-cold']) ?? '#C2410C'
    ctx.beginPath()
    ctx.arc(u.valToPos(xs[i] as number, 'x', true), u.valToPos(y, 'y', true), 3.5 * devicePixelRatio, 0, Math.PI * 2)
    ctx.fill()
  }
  ctx.restore()
}

function options(width: number): uPlot.Options {
  const c = colors.value
  const ink = c['on-surface'] ?? '#1A2117'
  const grid = { stroke: `${ink}22`, width: 1 }
  const digits = props.digits ?? 1
  const pad = (props.band.max - props.band.min) * 0.25
  return {
    width,
    height: props.height ?? 170,
    padding: [8, 8, 0, 0],
    legend: { show: false },
    cursor: { drag: { x: false, y: false } },
    scales: {
      x: { time: true, range: () => [props.from.getTime() / 1000, props.to.getTime() / 1000] },
      y: {
        range: (_u, min, max) => [Math.min(min, props.band.min - pad), Math.max(max, props.band.max + pad)],
      },
    },
    axes: [
      { stroke: ink, grid, ticks: { stroke: `${ink}44` }, font: "11px 'Archivo Variable'", values: (_u, v) => v.map((t) => new Date(t * 1000).toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' })) },
      { stroke: ink, grid, ticks: { stroke: `${ink}44` }, size: 52, font: "11px 'Azeret Mono'", values: (_u, v) => v.map((t) => formatNumber(t, digits)) },
    ],
    series: [
      {},
      { label: 'Média', stroke: c.primary, width: 2, points: { show: false } },
      { label: 'Mínimo', stroke: `${c.secondary}99`, width: 1, points: { show: false } },
      { label: 'Máximo', stroke: `${c.secondary}99`, width: 1, points: { show: false } },
    ],
    bands: [{ series: [3, 2], fill: `${c.secondary}22` }],
    hooks: { drawClear: [corridor], draw: [outliers] },
  }
}

function build() {
  if (!el.value) return
  plot?.destroy()
  plot = new uPlot(options(el.value.clientWidth || 600), data.value, el.value)
}

onMounted(() => {
  build()
  observer = new ResizeObserver(() => el.value && plot?.setSize({ width: el.value.clientWidth, height: props.height ?? 170 }))
  if (el.value) observer.observe(el.value)
})
onBeforeUnmount(() => {
  observer?.disconnect()
  plot?.destroy()
})
watch(data, () => plot?.setData(data.value))
watch([colors, () => props.band, () => props.from, () => props.to], build)
</script>

<template>
  <figure class="ma-0">
    <figcaption class="d-flex align-baseline justify-space-between mb-1">
      <span class="text-subtitle-2 title-wide">{{ title }}</span>
      <span class="reading text-medium-emphasis">{{ formatNumber(band.min, digits ?? 1) }} a {{ formatNumber(band.max, digits ?? 1) }} {{ unit }}</span>
    </figcaption>
    <div ref="el" role="img" :aria-label="`${title}: ${summary}`" style="width: 100%" />
    <div v-if="points.length === 0" class="text-caption text-medium-emphasis">Sem leituras no período.</div>
  </figure>
</template>
