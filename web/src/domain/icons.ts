import { mdiMoleculeCo2, mdiThermometer, mdiWaterPercent, mdiWhiteBalanceSunny } from '@mdi/js'
import type { SensorType } from '@/api/schemas'

// Ordem e posição fixas: T, U (linha de cima), CO2, Luz (linha de baixo).
export const SENSOR_META: Record<SensorType, { label: string; short: string; icon: string; col: 0 | 1; row: 0 | 1 }> = {
  TEMPERATURE: { label: 'Temperatura', short: 'T', icon: mdiThermometer, col: 0, row: 0 },
  HUMIDITY: { label: 'Umidade', short: 'U', icon: mdiWaterPercent, col: 1, row: 0 },
  CO2: { label: 'CO₂', short: 'CO₂', icon: mdiMoleculeCo2, col: 0, row: 1 },
  LUMINOSITY: { label: 'Luz', short: 'Luz', icon: mdiWhiteBalanceSunny, col: 1, row: 1 },
}
