import { http } from './client'
import type {
  DataSourceResponse,
  CreateDataSourceRequest,
  UpdateDataSourceRequest,
} from '../types'

const BASE = '/api/v1/data-sources'

export const datasourceApi = {
  list: () => http.get<DataSourceResponse[]>(BASE),
  getById: (id: number) => http.get<DataSourceResponse>(`${BASE}/${id}`),
  create: (req: CreateDataSourceRequest) =>
    http.post<DataSourceResponse>(BASE, req),
  update: (id: number, req: UpdateDataSourceRequest) =>
    http.put<DataSourceResponse>(`${BASE}/${id}`, req),
  delete: (id: number) => http.delete<void>(`${BASE}/${id}`),
  testConnection: (id: number) =>
    http.post<{ status: string }>(`${BASE}/${id}/test`),
}
