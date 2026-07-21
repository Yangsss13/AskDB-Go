<template>
  <div class="auth-wrap">
    <div class="card auth-card">
      <h2>注册</h2>
      <form @submit.prevent="submit">
        <div class="field">
          <label>邮箱</label>
          <input v-model="form.email" type="email" required autocomplete="email" />
        </div>
        <div class="field">
          <label>密码</label>
          <input v-model="form.password" type="password" required autocomplete="new-password" minlength="8" />
        </div>
        <p v-if="errorMsg" class="error">{{ errorMsg }}</p>
        <button class="btn" type="submit" :disabled="loading" style="width:100%;margin-top:8px">
          {{ loading ? '注册中…' : '注册' }}
        </button>
      </form>
      <p style="margin-top:14px;text-align:center;font-size:13px">
        已有账号？<router-link to="/login">登录</router-link>
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
    await authApi.register(form.value)
    // Auto-login after registration.
    await authApi.loginAndStore(form.value)
    router.push('/console')
  } catch (e) {
    if (e instanceof ApiError) {
      const code = (e.body as { error?: string }).error
      errorMsg.value =
        code === 'email_already_registered' ? '该邮箱已注册' :
        code === 'invalid_password' ? '密码不符合要求' :
        (code ?? '注册失败')
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
