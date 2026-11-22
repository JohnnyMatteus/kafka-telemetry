import { describe, expect, it } from 'vitest'
import { safeRedirect } from './session'

describe('safeRedirect', () => {
  it('aceita caminho interno, com query e hash', () => {
    expect(safeRedirect('/estufas/gh-007?h=1#topo')).toBe('/estufas/gh-007?h=1#topo')
  })

  it('recusa destino externo ou protocolo relativo, que levaria o operador para outro site', () => {
    expect(safeRedirect('https://exemplo.com/alertas')).toBe('/')
    expect(safeRedirect('//exemplo.com')).toBe('/')
    expect(safeRedirect('/\\exemplo.com')).toBe('/')
  })

  it('cai na raiz quando o valor não é texto', () => {
    expect(safeRedirect(undefined)).toBe('/')
    expect(safeRedirect(['/alertas'])).toBe('/')
  })
})
