<script setup lang="ts">
import { useClock } from '@/domain/clock'
import { computed, ref } from 'vue'
import { ApiError } from '@/api/client'
import { useAcknowledge, useAlerts, useGreenhouses, useThresholds } from '@/api/queries'
import type { Alert } from '@/api/schemas'
import GreenhouseGlyph from '@/components/signature/GreenhouseGlyph.vue'
import StateBlock from '@/components/StateBlock.vue'
import { alertSentence, UNITS } from '@/domain/alerts'
import { formatDateTime, formatNumber, formatTime, shortName } from '@/domain/format'

const now = useClock(30_000)
const tab = ref<'open' | 'acknowledged'>('open')
const alerts = useAlerts(tab)
const greenhouses = useGreenhouses()
const thresholds = useThresholds()
const ack = useAcknowledge()

// Alerta aberto há mais tempo primeiro: é o que o técnico precisa ver antes.
const sorted = computed(() =>
  [...(alerts.data.value ?? [])].sort((a, b) => a.window_start.getTime() - b.window_start.getTime()),
)

const target = ref<Alert | null>(null)
const note = ref('')
const error = ref<string | null>(null)
const dialog = computed({ get: () => target.value !== null, set: (v) => !v && (target.value = null) })

function start(a: Alert) {
  target.value = a
  note.value = ''
  error.value = null
}

async function confirm() {
  if (!target.value) return
  try {
    await ack.mutateAsync({ id: target.value.alert_id, note: note.value.trim() })
    target.value = null
  } catch (e) {
    const status = e instanceof ApiError ? e.status : 0
    error.value =
      status === 409 ? 'Outro técnico já reconheceu este alerta. A lista foi atualizada.'
      : status === 404 ? 'Este alerta não existe mais.'
      : 'Não foi possível registrar o reconhecimento. Tente de novo.'
    if (status === 409 || status === 404) alerts.refetch()
  }
}
</script>

<template>
  <v-container fluid class="pa-4" style="max-width: 960px">
    <h1 class="text-h5 mb-3">Alertas</h1>
    <v-tabs v-model="tab" color="primary" class="mb-4">
      <v-tab value="open" class="text-none">Abertos</v-tab>
      <v-tab value="acknowledged" class="text-none">Reconhecidos</v-tab>
    </v-tabs>

    <v-skeleton-loader v-if="alerts.isPending.value" type="list-item-two-line, list-item-two-line" />
    <StateBlock v-else-if="alerts.isError.value" tone="error" title="Não foi possível carregar os alertas" text="A lista tenta de novo a cada 10 s." />
    <template v-else-if="sorted.length === 0">
      <StateBlock
        v-if="tab === 'open'" tone="success" title="Todas as estufas dentro da faixa agora"
        :text="`Última verificação às ${formatTime(now)}.`"
      />
      <StateBlock v-else title="Nenhum alerta reconhecido ainda" text="Os alertas reconhecidos aparecem aqui com a nota de quem tratou." />
      <div v-if="tab === 'open' && greenhouses.data.value && thresholds.data.value" class="d-flex flex-wrap ga-1 ma-4">
        <GreenhouseGlyph
          v-for="g in greenhouses.data.value" :key="g.greenhouse_id"
          :greenhouse="g" :thresholds="thresholds.data.value" :now="now" :size="40" compact
        />
      </div>
    </template>
    <v-card v-for="a in sorted" :key="a.alert_id" class="mb-2 pa-4 d-flex align-center ga-4 flex-wrap">
      <v-chip :color="a.rule === 'ABOVE_MAX' ? 'error' : 'info'">{{ a.rule === 'ABOVE_MAX' ? '▲ acima' : '▼ abaixo' }}</v-chip>
      <div class="flex-grow-1">
        <div class="text-body-1">{{ alertSentence(a, UNITS[a.sensor_type], now) }}, estufa {{ shortName(a.greenhouse_id) }}</div>
        <div class="reading text-medium-emphasis">
          média {{ formatNumber(a.observed_value, 1) }} {{ UNITS[a.sensor_type] }} · desde {{ formatDateTime(a.window_start) }}
        </div>
        <div v-if="a.acknowledged_at" class="text-caption mt-1">
          Reconhecido<span v-if="a.acknowledged_by"> por {{ a.acknowledged_by }}</span> em {{ formatDateTime(a.acknowledged_at) }}<span v-if="a.note">: {{ a.note }}</span>
        </div>
      </div>
      <v-btn v-if="!a.acknowledged_at" color="primary" @click="start(a)">Reconhecer</v-btn>
    </v-card>

    <v-dialog v-model="dialog" max-width="480">
      <v-card v-if="target" class="pa-4">
        <h2 class="text-h6 mb-1">Reconhecer alerta</h2>
        <p class="text-body-2 mb-3">{{ alertSentence(target, UNITS[target.sensor_type], now) }}, estufa {{ shortName(target.greenhouse_id) }}</p>
        <v-text-field v-model="note" label="Nota (o que foi feito)" counter="200" maxlength="200" autofocus @keyup.enter="confirm" />
        <v-alert v-if="error" type="error" class="mb-3">{{ error }}</v-alert>
        <div class="d-flex justify-end ga-2">
          <v-btn variant="text" @click="target = null">Cancelar</v-btn>
          <v-btn color="primary" :loading="ack.isPending.value" @click="confirm">Reconhecer</v-btn>
        </div>
      </v-card>
    </v-dialog>
  </v-container>
</template>
