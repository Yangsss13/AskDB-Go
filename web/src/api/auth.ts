import { http, setToken } from './client'
import type { LoginRequest, LoginResponse, RegisterRequest, RegisterResponse } from '../types'

export const authApi = {
  register: (req: RegisterRequest) =>
    http.post<RegisterResponse>('/api/v1/auth/register', req),

  login: (req: LoginRequest) =>
    http.post<LoginResponse>('/api/v1/auth/login', req),

  loginAndStore: async (req: LoginRequest): Promise<LoginResponse> => {
    const res = await http.post<LoginResponse>('/api/v1/auth/login', req)
    setToken(res.token)
    return res
  },
}
