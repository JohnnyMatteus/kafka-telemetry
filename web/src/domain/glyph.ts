import { SENSOR_TYPES, type Greenhouse, type SensorType, type Thresholds } from '@/api/schemas'
import { deviation, isStale, type Side } from './deviation'
import { formatDuration, formatReading } from './format'
import { SENSOR_META } from './icons'

export interface QuadrantState {
  type: SensorType
  state: 'stale' | 'missing' | Side
  intensity: number
  reading: string | null
  staleFor: string | null
}

export function quadrantStates(gh: Greenhouse, thresholds: Thresholds, now: Date): QuadrantState[] {
  return SENSOR_TYPES.map((type) => {
    const sensor = gh.sensors[type]
    const band = thresholds[type]
    if (!sensor || !band) return { type, state: 'missing', intensity: 0, reading: null, staleFor: null }
    const reading = formatReading(type, sensor.mean, sensor.unit)
    if (isStale(sensor.window_end, now)) {
      return { type, state: 'stale', intensity: 0, reading, staleFor: formatDuration(sensor.window_end, now) }
    }
    const d = deviation(sensor.mean, band)
    return { type, state: d.side, intensity: d.intensity, reading, staleFor: null }
  })
}

export const isOutside = (q: QuadrantState) => q.state === 'below' || q.state === 'above'

export function describeQuadrant(q: QuadrantState): string {
  const name = SENSOR_META[q.type].label
  switch (q.state) {
    case 'missing':
      return `${name}: sem leitura`
    case 'stale':
      return `${name}: sem leitura há ${q.staleFor}`
    case 'below':
      return `${name} ${q.reading}, abaixo da faixa`
    case 'above':
      return `${name} ${q.reading}, acima da faixa`
    default:
      return `${name} ${q.reading}, dentro da faixa`
  }
}
