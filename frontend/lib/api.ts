/**
 * Typed API client for the FastAPI backend.
 * Handles Bearer auth, JSON parsing, and refresh-on-401.
 */

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:8000";

export class ApiError extends Error {
  status: number;
  code: string;
  detail?: string;
  constructor(status: number, code: string, message: string, detail?: string) {
    super(message);
    this.status = status;
    this.code = code;
    this.detail = detail;
  }
}

type RequestOptions = {
  method?: "GET" | "POST" | "PATCH" | "DELETE";
  body?: unknown;
  token?: string | null;
  headers?: Record<string, string>;
  cache?: RequestCache;
};

export async function apiRequest<T>(
  path: string,
  options: RequestOptions = {},
): Promise<T> {
  const { method = "GET", body, token, headers = {}, cache } = options;

  const finalHeaders: Record<string, string> = {
    Accept: "application/json",
    ...headers,
  };
  if (body !== undefined) finalHeaders["Content-Type"] = "application/json";
  if (token) finalHeaders["Authorization"] = `Bearer ${token}`;

  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers: finalHeaders,
    body: body !== undefined ? JSON.stringify(body) : undefined,
    credentials: "include", // send refresh cookie
    cache,
  });

  if (res.status === 204) return undefined as T;

  let payload: any = null;
  const text = await res.text();
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = text;
    }
  }

  if (!res.ok) {
    const code = payload?.code ?? "HTTP_ERROR";
    const message =
      payload?.error ?? payload?.detail ?? `Request failed (${res.status})`;
    throw new ApiError(res.status, code, message, payload?.detail);
  }

  return payload as T;
}

// ---------- Auth endpoints ----------

export type User = {
  id: string;
  email: string;
  full_name: string;
  organization: string;
  role: string;
  is_active: boolean;
};

export type TokenResponse = {
  access_token: string;
  token_type: string;
  user: User;
};

export function signup(payload: {
  email: string;
  password: string;
  full_name?: string;
  organization?: string;
}) {
  return apiRequest<TokenResponse>("/auth/signup", {
    method: "POST",
    body: payload,
  });
}

export function login(payload: { email: string; password: string }) {
  return apiRequest<TokenResponse>("/auth/login", {
    method: "POST",
    body: payload,
  });
}

export function logout() {
  return apiRequest<void>("/auth/logout", { method: "POST" });
}

export function me(token: string) {
  return apiRequest<User>("/auth/me", { token });
}

export function refresh() {
  return apiRequest<TokenResponse>("/auth/refresh", { method: "POST" });
}

// ---------- Detection ----------

export type Anomaly = {
  farm_id?: string | null;
  animal_type?: string | null;
  date?: string | null;
  severity?: string | null;
  score?: number | null;
  description?: string | null;
};

export type DetectResponse = {
  run_id: string;
  success: boolean;
  records_processed: number;
  anomalies_detected: number;
  anomalies: Anomaly[];
  warnings: string[];
  errors: string[];
  quality_score?: number | null;
  report_path?: string | null;
  duration_ms: number;
  features_used: string[];
};

export function detect(
  token: string,
  records: Record<string, unknown>[],
) {
  return apiRequest<DetectResponse>("/v1/detect", {
    method: "POST",
    token,
    body: { records },
  });
}

// ---------- Runs ----------

export type RunSummary = {
  id: string;
  status: string;
  records_submitted: number;
  records_processed: number;
  anomalies_detected: number;
  quality_score?: number | null;
  duration_ms?: number | null;
  created_at: string;
  completed_at?: string | null;
};

export type RunListResponse = {
  items: RunSummary[];
  total: number;
  limit: number;
  offset: number;
};

export type RunDetail = RunSummary & {
  error_message?: string | null;
  anomalies: Anomaly[];
  reports: { id: string; format: string; size_bytes: number; created_at: string }[];
};

export function listRuns(
  token: string,
  params: { limit?: number; offset?: number; status?: string } = {},
) {
  const q = new URLSearchParams();
  if (params.limit != null) q.set("limit", String(params.limit));
  if (params.offset != null) q.set("offset", String(params.offset));
  if (params.status) q.set("status", params.status);
  const suffix = q.toString() ? `?${q}` : "";
  return apiRequest<RunListResponse>(`/runs${suffix}`, { token });
}

export function getRun(token: string, runId: string) {
  return apiRequest<RunDetail>(`/runs/${runId}`, { token });
}

export function deleteRun(token: string, runId: string) {
  return apiRequest<void>(`/runs/${runId}`, { method: "DELETE", token });
}

// ---------- API keys ----------

export type ApiKey = {
  id: string;
  name: string;
  prefix: string;
  created_at: string;
  last_used_at?: string | null;
  revoked_at?: string | null;
};

export type ApiKeyCreated = {
  id: string;
  name: string;
  prefix: string;
  key: string;
};

export function listApiKeys(token: string) {
  return apiRequest<ApiKey[]>("/api-keys", { token });
}

export function createApiKey(token: string, name: string) {
  return apiRequest<ApiKeyCreated>("/api-keys", {
    method: "POST",
    token,
    body: { name },
  });
}

export function revokeApiKey(token: string, id: string) {
  return apiRequest<void>(`/api-keys/${id}`, { method: "DELETE", token });
}