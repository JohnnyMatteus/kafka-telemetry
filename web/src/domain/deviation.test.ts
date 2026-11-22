import { describe, expect, it } from 'vitest'
import { deviation, isStale } from './deviation'

const band = { min: 12, max: 32 }

describe('deviation', () => {
  it('dentro da faixa não tem intensidade', () => {
    expect(deviation(12, band)).toEqual({ side: 'inside', intensity: 0 })
    expect(deviation(32, band)).toEqual({ side: 'inside', intensity: 0 })
  })
  it('mede a distância até a faixa, não o valor bruto', () => {
    expect(deviation(34.5, band)).toEqual({ side: 'above', intensity: 0.5 })
    expect(deviation(9.5, band)).toEqual({ side: 'below', intensity: 0.5 })
  })
  it('satura em 1 longe da faixa', () => {
    expect(deviation(80, band).intensity).toBe(1)
  })
  it('considera sem leitura após 7 min', () => {
    const now = new Date('2026-11-21T10:10:00Z')
    expect(isStale(new Date('2026-11-21T10:04:00Z'), now)).toBe(false)
    expect(isStale(new Date('2026-11-21T10:02:00Z'), now)).toBe(true)
  })
})
