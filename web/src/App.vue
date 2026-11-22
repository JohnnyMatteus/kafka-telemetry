<script setup lang="ts">
import { mdiLogout, mdiWeatherNight, mdiWhiteBalanceSunny } from '@mdi/js'
import { useStorage } from '@vueuse/core'
import { useQueryClient } from '@tanstack/vue-query'
import { computed, watch, watchEffect } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useTheme } from 'vuetify'
import { useAlerts } from '@/api/queries'
import { clearOperatorToken, operatorToken } from '@/auth/session'

const theme = useTheme()
const mode = useStorage<'canteiroLight' | 'canteiroDark'>('canteiro-tema', 'canteiroLight')
watchEffect(() => theme.change(mode.value))
const dark = computed(() => mode.value === 'canteiroDark')

const route = useRoute()
const router = useRouter()
const queryClient = useQueryClient()

const open = useAlerts('open', () => operatorToken.value !== null)
const openCount = computed(() => open.data.value?.length ?? 0)

// Sair, ou token rejeitado (401), derruba o cache e volta ao login de onde parou.
watch(operatorToken, (token) => {
  if (token) return
  queryClient.clear()
  router.push({ name: 'login', query: { redirect: route.fullPath } })
})

function logout() {
  clearOperatorToken()
}
</script>

<template>
  <v-app>
    <v-app-bar v-if="!route.meta.public" border="b">
      <router-link to="/" class="text-decoration-none d-flex align-center ml-4 mr-6" style="color: inherit">
        <svg width="22" height="22" viewBox="0 0 22 22" aria-hidden="true" class="mr-2">
          <rect x="0" y="0" width="10" height="10" fill="rgb(var(--v-theme-primary))" />
          <rect x="12" y="0" width="10" height="10" fill="rgb(var(--v-theme-band-fill))" stroke="rgb(var(--v-theme-band-edge))" />
          <rect x="0" y="12" width="10" height="10" fill="rgb(var(--v-theme-band-fill))" stroke="rgb(var(--v-theme-band-edge))" />
          <rect x="12" y="12" width="10" height="10" fill="rgb(var(--v-theme-dev-hot))" />
        </svg>
        <span class="title-wide text-h6">Canteiro</span>
      </router-link>
      <v-tabs color="primary" density="comfortable">
        <v-tab to="/" exact class="text-none">Cooperativa</v-tab>
        <v-tab to="/alertas" class="text-none">
          Alertas
          <v-chip v-if="openCount" color="warning" class="ml-2 reading" aria-label="alertas abertos">{{ openCount }}</v-chip>
        </v-tab>
        <v-tab to="/pipeline" class="text-none">Pipeline</v-tab>
      </v-tabs>
      <v-spacer />
      <span class="text-body-2 text-medium-emphasis mr-4 d-none d-md-inline">AgroVale Exemplo</span>
      <v-btn
        icon variant="text" :aria-label="dark ? 'Usar tema claro' : 'Usar tema escuro'"
        @click="mode = dark ? 'canteiroLight' : 'canteiroDark'"
      >
        <v-icon :icon="dark ? mdiWhiteBalanceSunny : mdiWeatherNight" />
      </v-btn>
      <v-btn v-if="operatorToken" icon variant="text" aria-label="Sair" class="mr-2" @click="logout">
        <v-icon :icon="mdiLogout" />
      </v-btn>
    </v-app-bar>
    <v-main>
      <router-view />
    </v-main>
  </v-app>
</template>
