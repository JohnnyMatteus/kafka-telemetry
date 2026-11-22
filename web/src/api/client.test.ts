import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { clearOperatorToken, operatorToken, setOperatorToken } from '@/auth/session'
import { api, ApiError } from './client'

const sensor = {
  window_start: '2026-11-21T10:00:00Z',
  window_end: '2026-11-21T10:01:00Z',
  sample_count: 12,
  mean: 33.4,
  min: 32.1,
  max: 34.8,
  unit: '°C',
  alert: { rule: 'ABOVE_MAX', observed: 33.4, limit: 32 },
}

function stub(body: unknown, status = 200) {
  const fetchMock = vi.fn(async () => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } }))
  vi.stubGlobal('fetch', fetchMock)
  return fetchMock
}

beforeEach(() => clearOperatorToken())
afterEach(() => vi.unstubAllGlobals())

const authOf = (init: RequestInit | undefined) => new Headers(init?.headers).get('authorization')

describe('cliente da API (contrato-canteiro)', () => {
  it('lê /greenhouses com sensores parciais e datas', async () => {
    stub({ greenhouses: [{ greenhouse_id: 'gh-007', sensors: { TEMPERATURE: sensor } }] })
    const [gh] = await api.greenhouses()
    expect(gh?.sensors.TEMPERATURE?.window_end).toBeInstanceOf(Date)
    expect(gh?.sensors.HUMIDITY).toBeUndefined()
  })

  it('envia from/to em RFC3339 na série', async () => {
    const f = stub({ greenhouse_id: 'gh-007', series: { TEMPERATURE: { unit: '°C', points: [{ window_start: sensor.window_start, window_end: sensor.window_end, sample_count: 3, mean: 24, min: 23, max: 25 }] } } })
    const r = await api.series('gh-007', new Date('2026-11-21T09:00:00Z'), new Date('2026-11-21T10:00:00Z'))
    expect(r.series.TEMPERATURE?.points).toHaveLength(1)
    const [url] = f.mock.calls[0] as unknown as [string]
    expect(url).toContain('/api/greenhouses/gh-007/series')
    expect(url).toContain('from=2026-11-21T09:00:00.000Z')
  })

  it('lê alertas, aceitando acknowledged_at e note nulos', async () => {
    stub({ alerts: [{ alert_id: 'a1', greenhouse_id: 'gh-007', sensor_type: 'TEMPERATURE', window_start: sensor.window_start, window_end: sensor.window_end, rule: 'ABOVE_MAX', observed_value: 33.4, limit_value: 32, acknowledged_at: null, acknowledged_by: null, note: null }] })
    const [a] = await api.alerts('open')
    expect(a?.acknowledged_at).toBeNull()
    expect(a?.acknowledged_by).toBeNull()
  })

  it('reconhece alerta com POST {note} e devolve o alerta atualizado', async () => {
    const f = stub({ alert_id: 'a1', greenhouse_id: 'gh-007', sensor_type: 'CO2', window_start: sensor.window_start, window_end: sensor.window_end, rule: 'BELOW_MIN', observed_value: 300, limit_value: 350, acknowledged_at: '2026-11-21T10:05:00Z', acknowledged_by: 'ana', note: 'ventilação aberta' })
    const a = await api.acknowledge('a1', 'ventilação aberta')
    expect(a.acknowledged_at).toBeInstanceOf(Date)
    expect(a.acknowledged_by).toBe('ana')
    const [, init] = f.mock.calls[0] as unknown as [string, RequestInit]
    expect(init.method).toBe('POST')
    expect(JSON.parse(init.body as string)).toEqual({ note: 'ventilação aberta' })
  })

  it('traduz 409 e 404 do ack em ApiError com o status', async () => {
    stub({ error: 'já reconhecido' }, 409)
    await expect(api.acknowledge('a1', '')).rejects.toMatchObject({ status: 409 })
    stub({ error: 'inexistente' }, 404)
    await expect(api.acknowledge('zz', '')).rejects.toBeInstanceOf(ApiError)
  })

  it('lê DLQ, faixas e saúde do pipeline', async () => {
    stub({ messages: [{ partition: 0, offset: 5, timestamp: sensor.window_end, key: null, reason: 'OUT_OF_RANGE', error: 'umidade 140', source_topic: 'readings', source_partition: 1, source_offset: 90 }] })
    expect((await api.dlq())[0]?.reason).toBe('OUT_OF_RANGE')
    stub({ thresholds: { TEMPERATURE: { min: 12, max: 32, unit: '°C' } } })
    expect((await api.thresholds()).TEMPERATURE?.max).toBe(32)
    stub({ group: 'agg', group_state: 'Stable', topic: 'readings', total_lag: 10, partitions: [{ partition: 0, committed_offset: 90, end_offset: 100, lag: 10 }], dlq_messages: 1, throughput_records_per_second: 33.5, throughput_window_seconds: 60 })
    expect((await api.pipelineHealth()).partitions[0]?.lag).toBe(10)
  })

  it('rejeita resposta fora do contrato em vez de entregar dado torto', async () => {
    stub({ greenhouses: [{ greenhouse_id: 'gh-1', sensors: { PRESSURE: sensor } }] })
    await expect(api.greenhouses()).rejects.toThrow()
  })

  it('envia o token do operador no header Authorization quando a sessão tem um', async () => {
    setOperatorToken('dev-only-token-ana')
    const f = stub({ thresholds: {} })
    await api.thresholds()
    const [, init] = f.mock.calls[0] as unknown as [string, RequestInit]
    expect(authOf(init)).toBe('Bearer dev-only-token-ana')
  })

  it('não envia Authorization sem sessão', async () => {
    const f = stub({ thresholds: {} })
    await api.thresholds()
    const [, init] = f.mock.calls[0] as unknown as [string, RequestInit]
    expect(authOf(init)).toBeNull()
  })

  it('401 derruba a sessão para o guard levar ao login', async () => {
    setOperatorToken('dev-only-token-ana')
    stub({ error: 'token inválido' }, 401)
    await expect(api.dlq()).rejects.toMatchObject({ status: 401 })
    expect(operatorToken.value).toBeNull()
  })

  it('login confere o token com o header dele e não grava nada antes do sucesso', async () => {
    const f = stub({ thresholds: {} })
    await api.login('dev-only-token-bruno')
    const [, init] = f.mock.calls[0] as unknown as [string, RequestInit]
    expect(authOf(init)).toBe('Bearer dev-only-token-bruno')
    expect(operatorToken.value).toBeNull()
  })

  it('login com token recusado vira ApiError 401', async () => {
    stub({ error: 'token inválido' }, 401)
    await expect(api.login('errado-errado-errado')).rejects.toBeInstanceOf(ApiError)
  })
})
