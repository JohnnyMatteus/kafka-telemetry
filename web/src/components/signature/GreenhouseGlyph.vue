<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import type { Greenhouse, Thresholds } from '@/api/schemas'
import { describeQuadrant, isOutside, quadrantStates, type QuadrantState } from '@/domain/glyph'
import { SENSOR_META } from '@/domain/icons'
import { shortName } from '@/domain/format'

const props = withDefaults(
  defineProps<{ greenhouse: Greenhouse; thresholds: Thresholds; now: Date; size?: number; compact?: boolean }>(),
  { size: 96, compact: false },
)

const SIZE = 96
const HALF = SIZE / 2
const GUTTER = 2

const quadrants = computed(() => quadrantStates(props.greenhouse, props.thresholds, props.now))
const outside = computed(() => quadrants.value.filter(isOutside))
const name = computed(() => shortName(props.greenhouse.greenhouse_id))
const firstOut = computed(() => outside.value[0])
const arrow = computed(() => (firstOut.value?.state === 'above' ? '▲' : '▼'))

const label = computed(
  () => `Estufa ${name.value}: ${quadrants.value.map(describeQuadrant).join('; ')}`,
)

// Pisca a borda uma vez quando o conjunto de grandezas fora da faixa muda.
const flashing = ref(false)
watch(
  () => outside.value.map((q) => `${q.type}:${q.state}`).join(),
  (_, before) => {
    if (before === undefined) return
    flashing.value = false
    requestAnimationFrame(() => (flashing.value = true))
  },
)

function origin(q: QuadrantState) {
  const m = SENSOR_META[q.type]
  return { x: m.col * HALF + (m.col ? GUTTER : 0), y: m.row * HALF + (m.row ? GUTTER : 0) }
}

// Quadrante dentro da faixa usa a cor da faixa; fora, a escala divergente frio/quente.
function fill(q: QuadrantState): string {
  if (q.state === 'stale' || q.state === 'missing') return 'rgb(var(--v-theme-surface-variant))'
  if (q.state === 'inside') return 'rgb(var(--v-theme-band-fill))'
  const dev = q.state === 'below' ? 'dev-cold' : 'dev-hot'
  const pct = Math.round(35 + 65 * q.intensity)
  return `color-mix(in srgb, rgb(var(--v-theme-${dev})) ${pct}%, rgb(var(--v-theme-band-fill)))`
}

const strong = (q: QuadrantState) => isOutside(q) && q.intensity > 0.55
const ink = (q: QuadrantState) => (strong(q) ? 'rgb(var(--v-theme-surface))' : 'rgb(var(--v-theme-on-surface))')
</script>

<template>
  <div :class="['tile', { 'tile-flash': flashing }]" :style="{ width: `${size}px`, height: `${size}px` }" @animationend="flashing = false">
    <svg :viewBox="`0 0 ${SIZE} ${SIZE}`" :width="size" :height="size" role="img" :aria-label="label" style="display: block">
      <defs>
        <pattern id="hachura" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
          <line x1="0" y1="0" x2="0" y2="6" stroke="rgb(var(--v-theme-on-surface))" stroke-opacity="0.28" stroke-width="1.5" />
        </pattern>
      </defs>
      <g v-for="q in quadrants" :key="q.type" :transform="`translate(${origin(q).x} ${origin(q).y})`">
        <rect :width="HALF - GUTTER" :height="HALF - GUTTER" :fill="fill(q)" />
        <rect
          v-if="q.state === 'stale' || q.state === 'missing'"
          :width="HALF - GUTTER"
          :height="HALF - GUTTER"
          fill="url(#hachura)"
        />
        <rect
          v-if="q.state === 'inside'"
          x="0.5" y="0.5" :width="HALF - GUTTER - 1" :height="HALF - GUTTER - 1"
          fill="none" stroke="rgb(var(--v-theme-band-edge))" stroke-width="1" stroke-dasharray="3 2"
        />
        <svg
          :x="SENSOR_META[q.type].col ? 22 : 3"
          :y="SENSOR_META[q.type].row ? 22 : 3"
          width="18" height="18" viewBox="0 0 24 24"
        >
          <path :d="SENSOR_META[q.type].icon" :fill="ink(q)" />
        </svg>
      </g>
      <g v-if="!compact">
        <rect :x="HALF - 20" :y="HALF - 14" width="40" height="28" fill="rgb(var(--v-theme-surface))" stroke="rgb(var(--v-theme-on-surface))" stroke-opacity="0.35" />
        <text :x="HALF" :y="firstOut ? HALF - 1 : HALF + 5" text-anchor="middle" font-size="15" font-weight="700" fill="rgb(var(--v-theme-on-surface))" class="title-wide tnum">{{ name }}</text>
        <text v-if="firstOut" :x="HALF" :y="HALF + 11" text-anchor="middle" font-size="10" class="label-narrow" :fill="firstOut.state === 'above' ? 'rgb(var(--v-theme-dev-hot))' : 'rgb(var(--v-theme-dev-cold))'">{{ arrow }} {{ SENSOR_META[firstOut.type].short }}{{ outside.length > 1 ? ` +${outside.length - 1}` : '' }}</text>
      </g>
    </svg>
  </div>
</template>
