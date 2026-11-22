import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query'
import { computed, type MaybeRefOrGetter, toValue } from 'vue'
import { api } from './client'

export const useGreenhouses = () =>
  useQuery({ queryKey: ['greenhouses'], queryFn: api.greenhouses, refetchInterval: 10_000 })

// A faixa agronômica muda raramente: não vale refazer a consulta a cada tela.
export const useThresholds = () => useQuery({ queryKey: ['thresholds'], queryFn: api.thresholds, staleTime: 5 * 60_000 })

export function useSeries(id: MaybeRefOrGetter<string>, hours: MaybeRefOrGetter<number>) {
  return useQuery({
    queryKey: computed(() => ['series', toValue(id), toValue(hours)]),
    queryFn: () => {
      const to = new Date()
      return api.series(toValue(id), new Date(to.getTime() - toValue(hours) * 3_600_000), to)
    },
    refetchInterval: 30_000,
  })
}

export const useAlerts = (status: MaybeRefOrGetter<'open' | 'acknowledged'>, enabled: MaybeRefOrGetter<boolean> = true) =>
  useQuery({
    queryKey: computed(() => ['alerts', toValue(status)]),
    queryFn: () => api.alerts(toValue(status)),
    refetchInterval: 10_000,
    enabled: computed(() => toValue(enabled)),
  })

export function useAcknowledge() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (v: { id: string; note: string }) => api.acknowledge(v.id, v.note),
    onSuccess: () => client.invalidateQueries({ queryKey: ['alerts'] }),
  })
}

export const useDlq = () => useQuery({ queryKey: ['dlq'], queryFn: () => api.dlq(), refetchInterval: 15_000 })
export const usePipelineHealth = () =>
  useQuery({ queryKey: ['pipeline-health'], queryFn: api.pipelineHealth, refetchInterval: 5_000 })
