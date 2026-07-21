import { router } from '../router'

// Token is kept in memory only. sessionStorage provides tab-level restore.
const SESSION_KEY = 'askdb_token'

let _token: string | null = sessionStorage.getItem(SESSION_KEY)

export function getToken(): string | null {
  return _token
}

export function setToken(token: string): void {
  _token = token
  sessionStorage.setItem(SESSION_KEY, token)
}

export function clearToken(): void {
  _token = null
  sessionStorage.removeItem(SESSION_KEY)
}

export class ApiError extends Error {
  public readonly errorCode: string | undefined
  public readonly errorMessage: string | undefined

  constructor(
    public readonly status: number,
    public readonly body: Record<string, unknown>,
  ) {
    super(`API error ${status}`)
    // Go handlers use two shapes:
    //   auth endpoints:  { error: "code" }
    //   other endpoints: { error_code: "CODE", error_message: "msg" }
    this.errorCode = (body.error_code ?? body.error) as string | undefined
    this.errorMessage = (
      body.error_message ?? (typeof body.error === 'string' ? body.error : undefined)
    ) as string | undefined
  }
}

export interface RequestOpts {
  signal?: AbortSignal
  // Custom text parser (e.g. lossless-json for big-integer safety).
  parseText?: (text: string) => unknown
}

async function request<T>(path: string, init: RequestInit = {}, opts: RequestOpts = {}): Promise<T> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(init.headers as Record<string, string>),
  }
  if (_token) {
    headers['Authorization'] = `Bearer ${_token}`
  }

  const res = await fetch(path, { ...init, headers, signal: opts.signal })

  if (res.status === 401) {
    clearToken()
    router.push('/login')
    throw new ApiError(res.status, { error: 'unauthorized' })
  }

  if (!res.ok) {
    let body: Record<string, unknown> = {}
    try {
      body = await res.json()
    } catch {
      // ignore parse failure — body stays empty
    }
    throw new ApiError(res.status, body)
  }

  if (res.status === 204) {
    return undefined as unknown as T
  }

  if (opts.parseText) {
    const text = await res.text()
    return opts.parseText(text) as T
  }

  return res.json() as Promise<T>
}

// formatApiError renders a user-facing detail string that always preserves the
// backend's original error_code / error_message verbatim (never guessed or
// replaced by a generic translation). Falls back to the HTTP status only when
// the backend sent neither field. Never reads e.body directly — only the
// validated errorCode/errorMessage fields extracted in the constructor above.
export function formatApiError(e: unknown): string {
  if (e instanceof ApiError) {
    const parts = [e.errorCode, e.errorMessage].filter(Boolean)
    return parts.length > 0 ? parts.join(': ') : `HTTP ${e.status}`
  }
  return '网络请求失败'
}

export const http = {
  get: <T>(path: string, opts?: RequestOpts) =>
    request<T>(path, { method: 'GET' }, opts),
  post: <T>(path: string, body?: unknown, opts?: RequestOpts) =>
    request<T>(path, { method: 'POST', body: body !== undefined ? JSON.stringify(body) : undefined }, opts),
  put: <T>(path: string, body?: unknown, opts?: RequestOpts) =>
    request<T>(path, { method: 'PUT', body: body !== undefined ? JSON.stringify(body) : undefined }, opts),
  delete: <T>(path: string, opts?: RequestOpts) =>
    request<T>(path, { method: 'DELETE' }, opts),
}
