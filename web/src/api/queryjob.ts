import { parse as losslessParse, isSafeNumber } from 'lossless-json'
import { http } from './client'
import type {
  SubmitJobRequest,
  SubmitJobResponse,
  QueryJobResponse,
  QueryResultResponse,
} from '../types'

// Parse query-result JSON without precision loss.
// Safe integers → number; unsafe integers → string (preserves all digits).
// Only used for GET /query-jobs/:id/result where dynamic row data may contain bigints.
function parseResultLossless(text: string): unknown {
  return losslessParse(text, undefined, (value: string) =>
    isSafeNumber(value) ? parseFloat(value) : value,
  )
}

export const queryjobApi = {
  submit: (req: SubmitJobRequest, signal?: AbortSignal) =>
    http.post<SubmitJobResponse>('/api/v1/query-jobs', req, { signal }),

  get: (id: number, signal?: AbortSignal) =>
    http.get<QueryJobResponse>(`/api/v1/query-jobs/${id}`, { signal }),

  getResult: (id: number, signal?: AbortSignal) =>
    http.get<QueryResultResponse>(`/api/v1/query-jobs/${id}/result`, {
      signal,
      parseText: parseResultLossless,
    }),
}
