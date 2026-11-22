import { useIntervalFn } from '@vueuse/core'
import { shallowRef } from 'vue'

// Relógio que avança em passo fixo: telas de leitura precisam de "agora" sem recalcular a cada frame.
export function useClock(everyMs: number) {
  const now = shallowRef(new Date())
  useIntervalFn(() => (now.value = new Date()), everyMs)
  return now
}
