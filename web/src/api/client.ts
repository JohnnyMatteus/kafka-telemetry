import { FetchError, ofetch } from 'ofetch'
import type { z } from 'zod'
import { clearOperatorToken, operatorToken } from '@/auth/session'
import {
  alertsResponse,
  alertSchema,
  dlqResponse,
  greenhousesResponse,
  pipelineHealthSchema,
  seriesResponse,
  thresholdsResponse,
  type Alert,
} from './schemas'

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

const http = ofetch.create({
  baseURL: '/api',
  retry: 0,
  onRequest({ options }) {
    // O login já passa o próprio header; a sessão guardada só preenche quando falta.
    const headers = new Headers(options.headers)
    if (operatorToken.value && !headers.has('Authorization')) {
      headers.set('Authorization', `Bearer ${operatorToken.value}`)
      options.headers = headers
    }
  },
  onResponseError({ response }) {
    // Token expirou, foi revogado ou trocado: a tela volta ao login pelo guard de rota.
    if (response.status === 401) clearOperatorToken()
  },
})

// Valida na fronteira: resposta fora do contrato vira erro explícito, não dado torto na tela.
async function request<S extends z.ZodType>(schema: S, path: string, options?: Parameters<typeof http>[1]) {
  try {
    return schema.parse(await http(path, options)) as z.infer<S>
  } catch (e) {
    if (e instanceof FetchError) throw new ApiError(e.statusCode ?? 0, e.message)
    throw e
  }
}

export const api = {
  // Confere o token contra uma rota protegida barata antes de guardá-lo na sessão.
  login: (token: string) =>
    request(thresholdsResponse, '/thresholds', { headers: { Authorization: `Bearer ${token}` } }).then(() => undefined),
  greenhouses: () => request(greenhousesResponse, '/greenhouses').then((r) => r.greenhouses),
  series: (id: string, from: Date, to: Date) =>
    request(seriesResponse, `/greenhouses/${encodeURIComponent(id)}/series`, {
      query: { from: from.toISOString(), to: to.toISOString() },
    }),
  alerts: (status: 'open' | 'acknowledged', limit = 100) =>
    request(alertsResponse, '/alerts', { query: { status, limit } }).then((r) => r.alerts),
  acknowledge: (id: string, note: string): Promise<Alert> =>
    request(alertSchema, `/alerts/${encodeURIComponent(id)}/ack`, { method: 'POST', body: { note } }),
  dlq: (limit = 50) => request(dlqResponse, '/dlq', { query: { limit } }).then((r) => r.messages),
  thresholds: () => request(thresholdsResponse, '/thresholds').then((r) => r.thresholds),
  pipelineHealth: () => request(pipelineHealthSchema, '/pipeline/health'),
}
