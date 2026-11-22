import { ref } from 'vue'

// Token fica só na aba (sessionStorage), como na decisão do ADR 0011: fecha a aba, sai.
const KEY = 'canteiro-operador-token'

function read(): string | null {
  try {
    return sessionStorage.getItem(KEY)
  } catch {
    // Sem sessionStorage (navegador bloqueado), a sessão vale só na memória desta carga.
    return null
  }
}

export const operatorToken = ref<string | null>(read())

export function setOperatorToken(token: string) {
  try {
    sessionStorage.setItem(KEY, token)
  } catch {
    // Mesmo motivo de read(): a sessão continua valendo até recarregar a página.
  }
  operatorToken.value = token
}

export function clearOperatorToken() {
  try {
    sessionStorage.removeItem(KEY)
  } catch {
    // Ver setOperatorToken.
  }
  operatorToken.value = null
}

// Aceita só caminhos internos: um ?redirect= externo não pode levar o operador para outro site.
export function safeRedirect(value: unknown): string {
  // Navegadores tratam "/\\host" como "//host", por isso barra invertida também é recusada.
  if (typeof value !== 'string' || !value.startsWith('/') || /^\/[/\\]/.test(value)) return '/'
  return value
}
