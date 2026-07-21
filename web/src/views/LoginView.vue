<template>
  <div class="auth-wrap">
    <div class="card auth-card">
      <h2>登录</h2>
      <form @submit.prevent="submit">
        <div class="field">
          <label>邮箱</label>
          <input v-model="form.email" type="email" required autocomplete="email" />
        </div>
        <div class="field">
          <label>密码</label>
          <input v-model="form.password" type="password" required autocomplete="current-password" />
        </div>
        <p v-if="errorMsg" class="error">{{ errorMsg }}</p>
        <button class="btn" type="submit" :disabled="loading" style="width:100%;margin-top:8px">
          {{ loading ? '登录中…' : '登录' }}
        </button>
      </form>
      <p style="margin-top:14px;text-align:center;font-size:13px">
        没有账号？<router-link to="/register">注册</router-link>
      </p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { authApi } from '../api/auth'
import { ApiError } from '../api/client'

const router = useRouter()
const form = ref({ email: '', password: '' })
const loading = ref(false)
const errorMsg = ref('')

async function submit() {
  errorMsg.value = ''
  loading.value = true
  try {
    await authApi.loginAndStore(form.value)
    router.push('/console')
  } catch (e) {
    if (e instanceof ApiError) {
      const code = (e.body as { error?: string }).error
      errorMsg.value = code === 'invalid_credentials' ? '邮箱或密码错误' : (code ?? '登录失败')
    } else {
      errorMsg.value = '网络错误'
    }
  } finally {
    loading.value = false
  }
}
</script>

<style scoped>
.auth-wrap { display: flex; justify-content: center; padding-top: 60px; }
.auth-card { width: 360px; }
h2 { margin-bottom: 20px; font-size: 20px; }
</style>
