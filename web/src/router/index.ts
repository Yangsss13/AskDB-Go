import { createRouter, createWebHistory } from 'vue-router'
import { getToken } from '../api/client'

const routes = [
  { path: '/', redirect: '/console' },
  { path: '/login', component: () => import('../views/LoginView.vue'), meta: { public: true } },
  { path: '/register', component: () => import('../views/RegisterView.vue'), meta: { public: true } },
  { path: '/data-sources', component: () => import('../views/DataSourcesView.vue') },
  { path: '/console', component: () => import('../views/ConsoleView.vue') },
]

export const router = createRouter({
  history: createWebHistory(),
  routes,
})

router.beforeEach((to) => {
  if (!to.meta.public && !getToken()) {
    return '/login'
  }
})
