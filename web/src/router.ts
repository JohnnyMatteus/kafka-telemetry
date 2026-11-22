import { createRouter, createWebHistory } from 'vue-router'
import { operatorToken } from './auth/session'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/login', name: 'login', component: () => import('./features/login/LoginView.vue'), meta: { public: true } },
    { path: '/', name: 'cooperativa', component: () => import('./features/cooperativa/CooperativaView.vue') },
    { path: '/estufas/:id', name: 'estufa', component: () => import('./features/estufa/EstufaView.vue'), props: true },
    { path: '/alertas', name: 'alertas', component: () => import('./features/alertas/AlertasView.vue') },
    { path: '/pipeline', name: 'pipeline', component: () => import('./features/pipeline/PipelineView.vue') },
  ],
})

router.beforeEach((to) => {
  if (!to.meta.public && !operatorToken.value) return { name: 'login', query: { redirect: to.fullPath } }
  if (to.name === 'login' && operatorToken.value) return { path: '/' }
})

export default router
