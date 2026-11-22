<script setup lang="ts">
import { mdiLogin } from '@mdi/js'
import { ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api, ApiError } from '@/api/client'
import { safeRedirect, setOperatorToken } from '@/auth/session'

const route = useRoute()
const router = useRouter()
const token = ref('')
const pending = ref(false)
const error = ref<string | null>(null)

async function entrar() {
  const value = token.value.trim()
  if (!value) {
    error.value = 'Informe o token do operador.'
    return
  }
  pending.value = true
  error.value = null
  try {
    await api.login(value)
    setOperatorToken(value)
    await router.replace(safeRedirect(route.query.redirect))
  } catch (e) {
    error.value =
      e instanceof ApiError && e.status === 401
        ? 'Token inválido. Confira com quem administra o acesso.'
        : 'Não foi possível falar com a API. Tente de novo.'
  } finally {
    pending.value = false
  }
}
</script>

<template>
  <v-container class="fill-height justify-center" style="max-width: 440px">
    <v-card class="pa-6 w-100" variant="outlined">
      <div class="d-flex align-center ga-3 mb-4">
        <svg width="22" height="22" viewBox="0 0 22 22" aria-hidden="true">
          <rect x="0" y="0" width="10" height="10" fill="rgb(var(--v-theme-primary))" />
          <rect x="12" y="0" width="10" height="10" fill="rgb(var(--v-theme-band-fill))" stroke="rgb(var(--v-theme-band-edge))" />
          <rect x="0" y="12" width="10" height="10" fill="rgb(var(--v-theme-band-fill))" stroke="rgb(var(--v-theme-band-edge))" />
          <rect x="12" y="12" width="10" height="10" fill="rgb(var(--v-theme-dev-hot))" />
        </svg>
        <h1 class="text-h5">Canteiro</h1>
      </div>
      <p class="text-body-2 text-medium-emphasis mb-4">
        Entre com o token de operador para ver as estufas, os alertas e o pipeline.
      </p>
      <v-form @submit.prevent="entrar">
        <v-text-field
          v-model="token"
          label="Token de operador"
          type="password"
          autocomplete="off"
          autofocus
          :error-messages="error ? [error] : []"
          :disabled="pending"
        />
        <v-btn type="submit" color="primary" block size="large" class="text-none" :loading="pending" :prepend-icon="mdiLogin">
          Entrar
        </v-btn>
      </v-form>
    </v-card>
  </v-container>
</template>
