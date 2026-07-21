<template>
  <nav class="nav">
    <span class="nav-brand">AskDB</span>
    <template v-if="isLoggedIn">
      <router-link to="/console">控制台</router-link>
      <router-link to="/data-sources">数据源</router-link>
      <button class="btn-link" @click="logout">退出</button>
    </template>
    <template v-else>
      <router-link to="/login">登录</router-link>
      <router-link to="/register">注册</router-link>
    </template>
  </nav>
  <main class="main">
    <router-view />
  </main>
</template>

<script setup lang="ts">
import { computed, watch } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { getToken, clearToken } from './api/client'

const router = useRouter()
const route = useRoute()

// Re-compute on route change so nav stays in sync after login/logout.
const isLoggedIn = computed(() => {
  void route.path
  return !!getToken()
})

watch(
  () => route.path,
  () => { /* triggers isLoggedIn recompute */ },
)

function logout() {
  clearToken()
  router.push('/login')
}
</script>

<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: system-ui, sans-serif; font-size: 14px; color: #1a1a1a; background: #f5f5f5; }
a { color: #2563eb; text-decoration: none; }
a:hover { text-decoration: underline; }
button { cursor: pointer; }
input, select, textarea {
  border: 1px solid #d1d5db;
  border-radius: 4px;
  padding: 6px 10px;
  font-size: 14px;
  width: 100%;
}
input:focus, select:focus, textarea:focus { outline: 2px solid #2563eb; outline-offset: -1px; }
.btn {
  display: inline-block;
  padding: 7px 16px;
  border-radius: 4px;
  border: 1px solid transparent;
  font-size: 14px;
  background: #2563eb;
  color: #fff;
}
.btn:hover { background: #1d4ed8; }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }
.btn-secondary { background: #fff; color: #374151; border-color: #d1d5db; }
.btn-secondary:hover { background: #f9fafb; }
.btn-danger { background: #dc2626; }
.btn-danger:hover { background: #b91c1c; }
.btn-link { background: none; border: none; color: #2563eb; padding: 0; font-size: 14px; }
.btn-link:hover { text-decoration: underline; }
.error { color: #dc2626; font-size: 13px; margin-top: 4px; }
.nav {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 0 24px;
  height: 48px;
  background: #fff;
  border-bottom: 1px solid #e5e7eb;
}
.nav-brand { font-weight: 600; font-size: 16px; margin-right: auto; }
.main { max-width: 960px; margin: 32px auto; padding: 0 16px; }
.card {
  background: #fff;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 24px;
}
.field { margin-bottom: 14px; }
.field label { display: block; font-size: 13px; font-weight: 500; margin-bottom: 4px; }
.row { display: flex; gap: 12px; align-items: center; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid #e5e7eb; }
th { background: #f9fafb; font-weight: 500; }
.tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 9999px;
  font-size: 12px;
  font-weight: 500;
}
.tag-pending { background: #fef3c7; color: #92400e; }
.tag-running { background: #dbeafe; color: #1e40af; }
.tag-succeeded { background: #d1fae5; color: #065f46; }
.tag-failed { background: #fee2e2; color: #991b1b; }
</style>
