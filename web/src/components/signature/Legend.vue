<script setup lang="ts">
import { SENSOR_TYPES } from '@/api/schemas'
import { SENSOR_META } from '@/domain/icons'
</script>

<template>
  <div class="legend pa-2 text-caption" aria-label="Legenda dos quadrantes">
    <div class="d-flex align-center ga-3 flex-wrap">
      <span v-for="t in SENSOR_TYPES" :key="t" class="d-inline-flex align-center ga-1">
        <v-icon :icon="SENSOR_META[t].icon" size="16" /> {{ SENSOR_META[t].label }}
      </span>
      <span class="d-inline-flex align-center ga-1"><i class="sw" style="background: rgb(var(--v-theme-band-fill)); border: 1px dashed rgb(var(--v-theme-band-edge))" /> na faixa</span>
      <span class="d-inline-flex align-center ga-1"><i class="sw" style="background: rgb(var(--v-theme-dev-cold))" /> ▼ abaixo</span>
      <span class="d-inline-flex align-center ga-1"><i class="sw" style="background: rgb(var(--v-theme-dev-hot))" /> ▲ acima</span>
      <span class="d-inline-flex align-center ga-1"><i class="sw hatch" /> sem leitura</span>
    </div>
    <div class="text-medium-emphasis mt-1">Ordem dos quadrantes: T e U em cima, CO₂ e Luz embaixo.</div>
  </div>
</template>

<style scoped>
.legend { background: rgb(var(--v-theme-surface)); border: 1px solid rgba(var(--v-border-color), var(--v-border-opacity)); border-radius: 6px; }
.sw { display: inline-block; width: 12px; height: 12px; }
.hatch { background: repeating-linear-gradient(45deg, rgb(var(--v-theme-surface-variant)) 0 3px, rgba(var(--v-theme-on-surface), 0.3) 3px 4px); }
</style>
