import { createRouter, createWebHistory } from 'vue-router'

export default createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'cooperativa', component: () => import('./features/cooperativa/CooperativaView.vue') },
    { path: '/estufas/:id', name: 'estufa', component: () => import('./features/estufa/EstufaView.vue'), props: true },
    { path: '/alertas', name: 'alertas', component: () => import('./features/alertas/AlertasView.vue') },
    { path: '/pipeline', name: 'pipeline', component: () => import('./features/pipeline/PipelineView.vue') },
  ],
})
