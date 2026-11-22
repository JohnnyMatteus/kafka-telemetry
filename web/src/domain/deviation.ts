export interface Band {
  min: number
  max: number
}

export type Side = 'below' | 'inside' | 'above'

export interface Deviation {
  side: Side
  /** 0 dentro da faixa; cresce com a distância até a faixa, em larguras de faixa, limitado a 1. */
  intensity: number
}

// A cor do quadrante vem da distância até a faixa, não do valor bruto.
export function deviation(value: number, band: Band): Deviation {
  const width = band.max - band.min
  if (value < band.min) return { side: 'below', intensity: clamp((band.min - value) / (width * 0.25)) }
  if (value > band.max) return { side: 'above', intensity: clamp((value - band.max) / (width * 0.25)) }
  return { side: 'inside', intensity: 0 }
}

const clamp = (n: number) => Math.min(1, Math.max(0, n))

export const STALE_AFTER_MS = 7 * 60_000

export function isStale(windowEnd: Date, now: Date): boolean {
  return now.getTime() - windowEnd.getTime() > STALE_AFTER_MS
}
