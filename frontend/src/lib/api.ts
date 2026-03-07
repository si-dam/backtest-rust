const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080/api/v1";

export interface SymbolRecord {
  symbol_contract: string;
}

export interface JobRecord {
  id: string;
  job_type: string;
  status: string;
  payload_json: Record<string, unknown>;
  result_json: Record<string, unknown>;
  error_json: Record<string, unknown>;
  progress_json: Record<string, unknown>;
  attempt: number;
  max_attempts: number;
  lease_until: string | null;
  locked_by: string | null;
  created_at: string;
  updated_at: string;
}

export interface BarRecord {
  ts: string;
  session_date: string;
  symbol_contract: string;
  timeframe: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  trade_count: number;
}

export interface ProfileLevel {
  price_level: number;
  value: number;
  volume: number;
}

export interface Profile {
  id: string;
  label: string;
  start: string;
  end: string;
  max_value: number;
  total_value: number;
  value_area_enabled: boolean;
  value_area_percent: number;
  value_area_poc: number | null;
  value_area_low: number | null;
  value_area_high: number | null;
  value_area_volume: number;
  levels: ProfileLevel[];
}

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`${apiBaseUrl}${path}`);
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `request failed: ${response.status}`);
  }
  return (await response.json()) as T;
}

async function postJson<TRequest, TResponse>(path: string, payload: TRequest): Promise<TResponse> {
  const response = await fetch(`${apiBaseUrl}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `request failed: ${response.status}`);
  }

  return (await response.json()) as TResponse;
}

export function getSymbols() {
  return fetchJson<{ symbols: SymbolRecord[] }>("/symbols");
}

export function getJob(jobId: string) {
  return fetchJson<JobRecord>(`/jobs/${jobId}`);
}

export function createIngestionJob(payload: {
  file_path: string;
  symbol_contract?: string;
  rebuild?: boolean;
}) {
  return postJson<typeof payload, { job_id: string }>("/ingestion/jobs", payload);
}

export function getBars(symbolContract: string, params: URLSearchParams) {
  return fetchJson<{ symbol_contract: string; bars: BarRecord[] }>(`/markets/${symbolContract}/bars?${params.toString()}`);
}

export function getPresetProfiles(symbolContract: string, params: URLSearchParams) {
  return fetchJson<{
    symbol_contract: string;
    timezone: string;
    preset: string;
    metric: string;
    tick_aggregation: number;
    profiles: Profile[];
  }>(`/markets/${symbolContract}/profiles/preset?${params.toString()}`);
}
