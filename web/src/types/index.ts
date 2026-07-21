// API types matching Go handler DTOs exactly.

export interface RegisterRequest {
  email: string
  password: string
}

export interface RegisterResponse {
  user_id: number
  email: string
  created_at: string
}

export interface LoginRequest {
  email: string
  password: string
}

export interface LoginResponse {
  token: string
  expires_at: string
}

export interface AuthErrorResponse {
  error: string
}

export interface DataSourceResponse {
  id: number
  label: string
  host: string
  port: number
  database_name: string
  username: string
  tls_mode: string
  connect_timeout_sec: number
  created_at: string
  updated_at: string
}

export interface CreateDataSourceRequest {
  label: string
  host: string
  port: number
  database_name: string
  username: string
  password: string
  tls_mode: string
  connect_timeout_sec: number
}

export interface UpdateDataSourceRequest {
  label?: string
  host?: string
  port?: number
  database_name?: string
  username?: string
  password?: string
  tls_mode?: string
  connect_timeout_sec?: number
}

export interface SubmitJobRequest {
  question: string
  data_source_id: number
}

export interface SubmitJobResponse {
  job_id: number
  status: string
  created_at: string
}

export interface QueryJobResponse {
  job_id: number
  question: string
  status: string
  generated_sql?: string
  row_count?: number
  execution_duration_ms?: number
  error_code?: string
  error_message?: string
  result_expires_at?: string
  created_at: string
  finished_at?: string
}

export interface QueryResultResponse {
  job_id: number
  columns: string[]
  // Cells may be string for big-integer values that exceed Number.MAX_SAFE_INTEGER.
  rows: (string | number | boolean | null)[][]
  row_count: number
  cached_at: string
  expires_at: string
}
