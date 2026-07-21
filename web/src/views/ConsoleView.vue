<template>
  <div>
    <h2 style="font-size:18px;margin-bottom:20px">查询控制台</h2>

    <div class="card" style="margin-bottom:20px">
      <form @submit.prevent="submitQuery">
        <div class="field">
          <label>数据源</label>
          <select v-model="selectedDsId" required>
            <option value="" disabled>请选择数据源…</option>
            <option v-for="ds in sources" :key="ds.id" :value="ds.id">
              {{ ds.label }} ({{ ds.host }}:{{ ds.port }}/{{ ds.database_name }})
            </option>
          </select>
        </div>
        <div class="field">
          <label>自然语言问题</label>
          <textarea
            v-model="question"
            rows="3"
            placeholder="例如：查询最近 7 天注册的用户数量"
            required
          />
        </div>
        <p v-if="submitError" class="error">{{ submitError }}</p>
        <button class="btn" type="submit" :disabled="submitting || polling">
          {{ submitting ? '提交中…' : '提交查询' }}
        </button>
      </form>
    </div>

    <!-- Job status -->
    <div v-if="job" class="card" style="margin-bottom:20px">
      <div class="row" style="margin-bottom:12px">
        <div style="flex:1">
          <strong>任务 #{{ job.job_id }}</strong>
          <span :class="statusTagClass(job.status)" class="tag" style="margin-left:8px">
            {{ statusLabel(job.status) }}
          </span>
        </div>
        <span style="color:#6b7280;font-size:13px">{{ fmtTime(job.created_at) }}</span>
      </div>
      <p style="color:#4b5563;margin-bottom:8px">{{ job.question }}</p>
      <div v-if="job.generated_sql" style="margin-bottom:8px">
        <label style="font-size:12px;font-weight:500;color:#6b7280">生成的 SQL</label>
        <pre class="sql-pre">{{ job.generated_sql }}</pre>
      </div>
      <div v-if="job.execution_duration_ms != null" style="font-size:13px;color:#6b7280">
        执行耗时 {{ job.execution_duration_ms }} ms，{{ job.row_count }} 行
      </div>
      <div v-if="job.status === 'failed'" class="error" style="margin-top:6px">
        {{ failedJobDetail(job) }}
      </div>
      <div v-if="polling" style="font-size:13px;color:#6b7280;margin-top:8px">轮询中…</div>
    </div>

    <!-- Result table -->
    <div v-if="result" class="card" style="padding:0;overflow:hidden">
      <div style="padding:12px 16px;font-size:13px;color:#6b7280;border-bottom:1px solid #e5e7eb">
        {{ result.row_count }} 行 · 缓存至 {{ fmtTime(result.expires_at) }}
      </div>
      <div style="overflow-x:auto">
        <table>
          <thead>
            <tr>
              <!-- Use index key to handle duplicate column names safely. -->
              <th v-for="(col, ci) in result.columns" :key="ci">{{ col }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(row, ri) in result.rows" :key="ri">
              <td v-for="(cell, ci) in row" :key="ci">{{ formatCell(cell) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'
import { datasourceApi } from '../api/datasource'
import { queryjobApi } from '../api/queryjob'
import { ApiError, formatApiError } from '../api/client'
import type { DataSourceResponse, QueryJobResponse, QueryResultResponse } from '../types'

const sources = ref<DataSourceResponse[]>([])
const selectedDsId = ref<number | ''>('')
const question = ref('')
const submitting = ref(false)
const submitError = ref('')
const job = ref<QueryJobResponse | null>(null)
const result = ref<QueryResultResponse | null>(null)
const polling = ref(false)

// Version counter (plain number, not reactive) guards against stale async callbacks.
// Each new submission increments the version; any callback whose captured version
// no longer matches currentVersion silently drops its result.
let currentVersion = 0
let activeAbort: AbortController | null = null
let pollTimer: ReturnType<typeof setTimeout> | null = null

const MAX_POLL_RETRIES = 3

onMounted(async () => {
  try {
    sources.value = await datasourceApi.list()
  } catch {
    // best-effort; user can still type a question if sources loaded partially
  }
})

onUnmounted(cancelActive)

function cancelActive() {
  currentVersion++
  if (activeAbort) { activeAbort.abort(); activeAbort = null }
  if (pollTimer) { clearTimeout(pollTimer); pollTimer = null }
  polling.value = false
}

function isAbortError(e: unknown): boolean {
  return e instanceof DOMException && e.name === 'AbortError'
}

async function submitQuery() {
  cancelActive()
  const myVersion = currentVersion  // cancelActive incremented it; capture here
  const abort = new AbortController()
  activeAbort = abort

  submitting.value = true
  submitError.value = ''
  result.value = null
  job.value = null

  try {
    const res = await queryjobApi.submit(
      { question: question.value, data_source_id: selectedDsId.value as number },
      abort.signal,
    )
    if (myVersion !== currentVersion) return

    job.value = {
      job_id: res.job_id,
      status: res.status,
      question: question.value,
      created_at: res.created_at,
    }
    schedulePoll(res.job_id, myVersion, abort.signal)
  } catch (e) {
    if (isAbortError(e) || myVersion !== currentVersion) return
    submitError.value = `提交失败：${formatApiError(e)}`
  } finally {
    if (myVersion === currentVersion) submitting.value = false
  }
}

function schedulePoll(jobId: number, version: number, signal: AbortSignal) {
  if (version !== currentVersion) return
  polling.value = true
  pollTimer = setTimeout(() => poll(jobId, version, signal, 0), 1500)
}

async function poll(jobId: number, version: number, signal: AbortSignal, retries: number) {
  if (version !== currentVersion) return
  pollTimer = null

  try {
    const j = await queryjobApi.get(jobId, signal)
    if (version !== currentVersion) return

    job.value = j

    if (j.status === 'succeeded') {
      polling.value = false
      fetchResult(jobId, version, signal)
    } else if (j.status === 'failed') {
      polling.value = false
    } else {
      // Still in progress — schedule next poll. No duplicate risk: pollTimer
      // is set only here and cleared on every cancelActive / new submission.
      pollTimer = setTimeout(() => poll(jobId, version, signal, 0), 2000)
    }
  } catch (e) {
    if (isAbortError(e) || version !== currentVersion) return

    if (e instanceof ApiError) {
      if (e.status === 404) {
        polling.value = false
        submitError.value = `任务不存在或无权访问：${formatApiError(e)}`
        return
      }
      // 429 or 5xx: transient — retry with linear back-off before giving up.
      if ((e.status === 429 || e.status >= 500) && retries < MAX_POLL_RETRIES) {
        pollTimer = setTimeout(
          () => poll(jobId, version, signal, retries + 1),
          2000 * (retries + 1),
        )
        return
      }
      polling.value = false
      if (e.status === 429) {
        submitError.value = `请求过于频繁：${formatApiError(e)}`
      } else if (e.status >= 500) {
        submitError.value = `服务暂时异常：${formatApiError(e)}`
      } else {
        submitError.value = `查询状态失败：${formatApiError(e)}`
      }
    } else {
      // Network error
      if (retries < MAX_POLL_RETRIES) {
        pollTimer = setTimeout(
          () => poll(jobId, version, signal, retries + 1),
          2000 * (retries + 1),
        )
        return
      }
      polling.value = false
      submitError.value = `网络请求失败：${formatApiError(e)}`
    }
  }
}

async function fetchResult(jobId: number, version: number, signal: AbortSignal) {
  try {
    const r = await queryjobApi.getResult(jobId, signal)
    if (version !== currentVersion) return
    result.value = r
  } catch (e) {
    if (isAbortError(e) || version !== currentVersion) return

    if (e instanceof ApiError) {
      if (e.status === 410) {
        // RESULT_EXPIRED: the job succeeded, only the cached result is gone.
        submitError.value = `任务成功，但查询结果已过期：${formatApiError(e)}`
        return
      }
      if (e.status === 404) {
        submitError.value = `任务不存在或无权访问：${formatApiError(e)}`
        return
      }
      if (e.status === 503) {
        // RESULT_UNAVAILABLE / RESULT_STORE_UNAVAILABLE / RESULT_CORRUPTED —
        // a result-store problem, not a query-execution failure.
        submitError.value = `查询结果暂时不可用：${formatApiError(e)}`
        return
      }
      if (e.status === 409) {
        // RESULT_NOT_READY / QUERY_JOB_FAILED — distinct from a store outage.
        submitError.value = `查询结果暂不可用：${formatApiError(e)}`
        return
      }
      submitError.value = `获取结果失败：${formatApiError(e)}`
    } else {
      submitError.value = `获取结果失败：${formatApiError(e)}`
    }
  }
}

const STATUS_LABELS: Record<string, string> = {
  pending:    '待处理',
  queued:     '已入队',
  generating: '生成SQL',
  validating: '校验中',
  executing:  '执行中',
  retrying:   '重试中',
  succeeded:  '成功',
  failed:     '失败',
}

const STATUS_TAG_CLASSES: Record<string, string> = {
  pending:    'tag-pending',
  queued:     'tag-pending',
  generating: 'tag-running',
  validating: 'tag-running',
  executing:  'tag-running',
  retrying:   'tag-running',
  succeeded:  'tag-succeeded',
  failed:     'tag-failed',
}

function statusLabel(status: string): string {
  return STATUS_LABELS[status] ?? status
}

function statusTagClass(status: string): string {
  return STATUS_TAG_CLASSES[status] ?? 'tag-pending'
}

function formatCell(cell: string | number | boolean | null): string {
  if (cell === null || cell === undefined) return 'NULL'
  return String(cell)
}

// Renders the terminal failed-job detail line, preserving the backend's
// original error_code/error_message verbatim (never guessed or replaced).
function failedJobDetail(j: QueryJobResponse): string {
  const parts = [j.error_code, j.error_message].filter(Boolean)
  return parts.length > 0 ? parts.join(': ') : '查询失败'
}

function fmtTime(iso: string) {
  return new Date(iso).toLocaleString('zh-CN')
}
</script>

<style scoped>
.sql-pre {
  background: #f3f4f6;
  border-radius: 4px;
  padding: 10px 12px;
  font-size: 12px;
  font-family: monospace;
  white-space: pre-wrap;
  word-break: break-all;
  margin-top: 4px;
}
</style>
