import { formatDistanceStrict, format } from 'date-fns'
import { ptBR } from 'date-fns/locale'
import type { SensorType } from '@/api/schemas'

const num = (digits: number) => new Intl.NumberFormat('pt-BR', { maximumFractionDigits: digits, minimumFractionDigits: digits })
const FORMATS: Record<SensorType, Intl.NumberFormat> = {
  TEMPERATURE: num(1),
  HUMIDITY: num(0),
  CO2: num(0),
  LUMINOSITY: num(0),
}

export function formatReading(type: SensorType, value: number, unit: string): string {
  return `${FORMATS[type].format(value)} ${unit}`
}

export const formatTime = (d: Date) => format(d, 'HH:mm', { locale: ptBR })
export const formatDateTime = (d: Date) => format(d, "dd/MM 'às' HH:mm", { locale: ptBR })
export const formatDuration = (from: Date, to: Date) => formatDistanceStrict(from, to, { locale: ptBR })
export const formatNumber = (n: number, digits = 0) => num(digits).format(n)

// "gh-007" vira "07"; o ladrilho é pequeno demais para o identificador inteiro.
export function shortName(id: string): string {
  const m = id.match(/(\d+)$/)
  return m?.[1] ?? id.slice(-3)
}
