import { mount } from '@vue/test-utils'
import { describe, expect, it } from 'vitest'
import type { Greenhouse, Thresholds } from '@/api/schemas'
import GreenhouseGlyph from './GreenhouseGlyph.vue'

const now = new Date('2026-11-21T10:10:00Z')
const thresholds: Thresholds = {
  TEMPERATURE: { min: 12, max: 32, unit: '°C' },
  HUMIDITY: { min: 50, max: 90, unit: '%' },
  CO2: { min: 350, max: 1500, unit: 'ppm' },
  LUMINOSITY: { min: 0, max: 80000, unit: 'lx' },
}
const reading = (mean: number, unit: string, end: string) => ({
  window_start: new Date('2026-11-21T10:00:00Z'),
  window_end: new Date(end),
  sample_count: 10,
  mean,
  min: mean,
  max: mean,
  unit,
})
const fresh = '2026-11-21T10:09:00Z'

const greenhouse: Greenhouse = {
  greenhouse_id: 'gh-007',
  sensors: {
    TEMPERATURE: reading(34.5, '°C', fresh),
    HUMIDITY: reading(70, '%', fresh),
    CO2: reading(900, 'ppm', '2026-11-21T09:55:00Z'),
  },
}

function label() {
  const w = mount(GreenhouseGlyph, { props: { greenhouse, thresholds, now } })
  return { w, text: w.get('svg').attributes('aria-label') ?? '' }
}

describe('GreenhouseGlyph', () => {
  it('descreve cada quadrante em texto, sem depender só da cor', () => {
    const { text } = label()
    expect(text).toContain('Estufa 007')
    expect(text).toContain('Temperatura 34,5 °C, acima da faixa')
    expect(text).toContain('Umidade 70 %, dentro da faixa')
    expect(text).toContain('CO₂: sem leitura há 15 minutos')
    expect(text).toContain('Luz: sem leitura')
  })

  it('mostra seta e grandeza fora da faixa no centro', () => {
    const { w } = label()
    expect(w.text()).toContain('▲ T')
  })

  it('desenha quatro quadrantes em posições fixas', () => {
    const { w } = label()
    const transforms = w.findAll('g[transform]').map((g) => g.attributes('transform'))
    expect(transforms).toEqual(['translate(0 0)', 'translate(50 0)', 'translate(0 50)', 'translate(50 50)'])
  })
})
