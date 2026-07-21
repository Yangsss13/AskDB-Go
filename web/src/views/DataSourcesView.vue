<template>
  <div>
    <div class="row" style="margin-bottom:20px">
      <h2 style="flex:1;font-size:18px">数据源</h2>
      <button class="btn" @click="openCreate">+ 新建</button>
    </div>

    <div v-if="loadError" class="error" style="margin-bottom:12px">{{ loadError }}</div>

    <div class="card" style="padding:0;overflow:hidden">
      <table>
        <thead>
          <tr>
            <th>名称</th><th>主机</th><th>数据库</th><th>TLS</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="sources.length === 0">
            <td colspan="5" style="text-align:center;color:#6b7280;padding:24px">暂无数据源</td>
          </tr>
          <tr v-for="ds in sources" :key="ds.id">
            <td>{{ ds.label }}</td>
            <td>{{ ds.host }}:{{ ds.port }}</td>
            <td>{{ ds.database_name }}</td>
            <td>{{ ds.tls_mode }}</td>
            <td>
              <div class="row" style="gap:8px;flex-wrap:nowrap">
                <button class="btn btn-secondary" style="padding:4px 10px" @click="openEdit(ds)">编辑</button>
                <button
                  class="btn btn-secondary"
                  style="padding:4px 10px"
                  :disabled="testingId === ds.id"
                  @click="testConn(ds.id)"
                >{{ testingId === ds.id ? '测试中…' : '测试' }}</button>
                <button class="btn btn-danger" style="padding:4px 10px" @click="confirmDelete(ds)">删除</button>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <p v-if="testResult" :class="testResult.ok ? '' : 'error'" style="margin-top:10px">
      {{ testResult.msg }}
    </p>

    <!-- Form modal -->
    <div v-if="showForm" class="overlay" @click.self="closeForm">
      <div class="card modal-card">
        <h3 style="margin-bottom:16px">{{ editing ? '编辑数据源' : '新建数据源' }}</h3>
        <form @submit.prevent="saveForm">
          <div class="field">
            <label>名称</label>
            <input v-model="form.label" required />
          </div>
          <div class="row">
            <div class="field" style="flex:2">
              <label>主机</label>
              <input v-model="form.host" required />
            </div>
            <div class="field" style="flex:1">
              <label>端口</label>
              <input v-model.number="form.port" type="number" min="1" max="65535" required />
            </div>
          </div>
          <div class="field">
            <label>数据库名</label>
            <input v-model="form.database_name" required />
          </div>
          <div class="row">
            <div class="field" style="flex:1">
              <label>用户名</label>
              <input v-model="form.username" required />
            </div>
            <div class="field" style="flex:1">
              <label>密码{{ editing ? '（留空保持不变）' : '' }}</label>
              <input v-model="form.password" type="password" :required="!editing" autocomplete="new-password" />
            </div>
          </div>
          <div class="row">
            <div class="field" style="flex:1">
              <label>TLS 模式</label>
              <select v-model="form.tls_mode">
                <option value="disabled">disabled</option>
                <option value="verify-full">verify-full</option>
              </select>
            </div>
            <div class="field" style="flex:1">
              <label>连接超时（秒，0=默认5s）</label>
              <input v-model.number="form.connect_timeout_sec" type="number" min="0" max="255" />
            </div>
          </div>
          <p v-if="formError" class="error">{{ formError }}</p>
          <div class="row" style="margin-top:16px;justify-content:flex-end;gap:8px">
            <button type="button" class="btn btn-secondary" @click="closeForm">取消</button>
            <button class="btn" type="submit" :disabled="saving">{{ saving ? '保存中…' : '保存' }}</button>
          </div>
        </form>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { datasourceApi } from '../api/datasource'
import { formatApiError } from '../api/client'
import type { DataSourceResponse, CreateDataSourceRequest, UpdateDataSourceRequest } from '../types'

const sources = ref<DataSourceResponse[]>([])
const loadError = ref('')
const testingId = ref<number | null>(null)
const testResult = ref<{ ok: boolean; msg: string } | null>(null)

const showForm = ref(false)
const editing = ref<DataSourceResponse | null>(null)
const saving = ref(false)
const formError = ref('')

const defaultForm = () => ({
  label: '',
  host: '',
  port: 3306,
  database_name: '',
  username: '',
  password: '',
  tls_mode: 'disabled',
  connect_timeout_sec: 0,
})
const form = ref(defaultForm())

onMounted(loadSources)

async function loadSources() {
  loadError.value = ''
  try {
    sources.value = await datasourceApi.list()
  } catch (e) {
    loadError.value = `加载失败：${formatApiError(e)}`
  }
}

function openCreate() {
  editing.value = null
  form.value = defaultForm()
  formError.value = ''
  showForm.value = true
}

function openEdit(ds: DataSourceResponse) {
  editing.value = ds
  form.value = {
    label: ds.label,
    host: ds.host,
    port: ds.port,
    database_name: ds.database_name,
    username: ds.username,
    password: '',
    tls_mode: ds.tls_mode,
    connect_timeout_sec: ds.connect_timeout_sec,
  }
  formError.value = ''
  showForm.value = true
}

function closeForm() {
  showForm.value = false
  form.value.password = ''
}

async function saveForm() {
  formError.value = ''
  saving.value = true

  // Copy form fields into a local payload before clearing the reactive form's
  // password. The request keeps the password value it needs; the form (and its
  // bound <input>) no longer holds it once the request is in flight — success,
  // failure, or an aborted/network exception all leave form.password empty.
  const isEdit = !!editing.value
  const enteredPassword = form.value.password
  const basePayload = {
    label: form.value.label,
    host: form.value.host,
    port: form.value.port,
    database_name: form.value.database_name,
    username: form.value.username,
    tls_mode: form.value.tls_mode,
    connect_timeout_sec: form.value.connect_timeout_sec,
  }
  form.value.password = ''

  try {
    if (isEdit) {
      // Empty password on edit means "keep current" — omit the field entirely
      // so the backend's nil-means-unchanged semantics apply.
      const req: UpdateDataSourceRequest = enteredPassword
        ? { ...basePayload, password: enteredPassword }
        : { ...basePayload }
      await datasourceApi.update(editing.value!.id, req)
    } else {
      const req: CreateDataSourceRequest = { ...basePayload, password: enteredPassword }
      await datasourceApi.create(req)
    }
    showForm.value = false
    await loadSources()
  } catch (e) {
    formError.value = `保存失败：${formatApiError(e)}`
  } finally {
    saving.value = false
  }
}

async function testConn(id: number) {
  testResult.value = null
  testingId.value = id
  try {
    await datasourceApi.testConnection(id)
    testResult.value = { ok: true, msg: '连接成功' }
  } catch (e) {
    testResult.value = { ok: false, msg: `连接失败：${formatApiError(e)}` }
  } finally {
    testingId.value = null
  }
}

async function confirmDelete(ds: DataSourceResponse) {
  if (!confirm(`确认删除数据源「${ds.label}」？`)) return
  try {
    await datasourceApi.delete(ds.id)
    await loadSources()
  } catch (e) {
    // Surfaces DATA_SOURCE_HAS_ACTIVE_JOBS (and any other backend code) verbatim.
    loadError.value = `删除失败：${formatApiError(e)}`
  }
}
</script>

<style scoped>
.overlay {
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,.4);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 100;
}
.modal-card { width: 520px; max-height: 90vh; overflow-y: auto; }
</style>
