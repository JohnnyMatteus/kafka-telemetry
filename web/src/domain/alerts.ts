import type { Alert } from '@/api/schemas'
import { formatDuration, formatNumber } from './format'
import { SENSOR_META } from './icons'

// "Temperatura acima de 32 °C há 14 min": o texto responde desde quando.
export function alertSentence(a: Alert, unit: string, now: Date): string {
  const side = a.rule === 'ABOVE_MAX' ? 'acima' : 'abaixo'
  return `${SENSOR_META[a.sensor_type].label} ${side} de ${formatNumber(a.limit_value, 1)} ${unit} há ${formatDuration(a.window_start, now)}`
}

export const UNITS: Record<Alert['sensor_type'], string> = {
  TEMPERATURE: '°C',
  HUMIDITY: '%',
  CO2: 'ppm',
  LUMINOSITY: 'lx',
}
