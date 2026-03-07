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

export interface RebuildJobResponse {
  symbol_contract: string;
  start: string;
  end: string;
  large_orders_threshold?: number;
  jobs: Array<{
    job_id: string;
    job_type: string;
  }>;
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

export interface LargeOrderRecord {
  ts: string;
  session_date: string;
  symbol_contract: string;
  method: string;
  threshold: number;
  trade_price: number;
  trade_size: number;
  side: string;
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

export interface AreaProfileResponse {
  symbol_contract: string;
  timezone: string;
  metric: string;
  tick_aggregation: number;
  profile: Profile;
}

export interface BacktestRunRecord {
  id: string;
  job_id: string | null;
  strategy_id: string;
  name: string;
  status: string;
  params_json: Record<string, unknown>;
  metrics_json: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface BacktestTradeRecord {
  id: string;
  run_id: string;
  symbol_contract: string;
  entry_ts: string | null;
  exit_ts: string | null;
  entry_price: number | null;
  exit_price: number | null;
  qty: number | null;
  pnl: number | null;
  notes_json: Record<string, unknown>;
  created_at: string;
}

export interface BacktestAnalytics {
  trades: number;
  wins: number;
  losses: number;
  total_pnl: number;
  avg_pnl: number;
  max_drawdown: number;
}

export interface DatasetExportRecord {
  id: string;
  job_id: string | null;
  export_kind: string;
  manifest_path: string;
  schema_version: string;
  payload_json: Record<string, unknown>;
  created_at: string;
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

export function getJobs(params?: URLSearchParams) {
  const suffix = params ? `?${params.toString()}` : "";
  return fetchJson<{ jobs: JobRecord[] }>(`/jobs${suffix}`);
}

export function replayJob(jobId: string, allowAnyStatus = false) {
  return postJson<{ allow_any_status: boolean }, { job_id: string }>(`/jobs/${jobId}/replay`, {
    allow_any_status: allowAnyStatus,
  });
}

export function createIngestionJob(payload: {
  file_path: string;
  symbol_contract?: string;
  rebuild?: boolean;
}) {
  return postJson<typeof payload, { job_id: string }>("/ingestion/jobs", payload);
}

export function createBacktestJob(payload: {
  name: string;
  strategy_id: string;
  params: Record<string, unknown>;
}) {
  return postJson<typeof payload, { job_id: string }>("/backtests/jobs", payload);
}

export function createDatasetJob(payload: {
  export_kind: "bars" | "ticks" | "backtest_trades";
  payload: Record<string, unknown>;
}) {
  return postJson<typeof payload, { job_id: string }>("/datasets/jobs", payload);
}

export function getDatasetExports(params?: URLSearchParams) {
  const suffix = params ? `?${params.toString()}` : "";
  return fetchJson<{ exports: DatasetExportRecord[] }>(`/datasets/exports${suffix}`);
}

export function getBars(symbolContract: string, params: URLSearchParams) {
  return fetchJson<{ symbol_contract: string; bars: BarRecord[] }>(`/markets/${symbolContract}/bars?${params.toString()}`);
}

export function getLargeOrders(symbolContract: string, params: URLSearchParams) {
  return fetchJson<{ symbol_contract: string; large_orders: LargeOrderRecord[] }>(
    `/markets/${symbolContract}/large-orders?${params.toString()}`,
  );
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

export function getAreaProfile(symbolContract: string, params: URLSearchParams) {
  return fetchJson<AreaProfileResponse>(`/markets/${symbolContract}/profiles/area?${params.toString()}`);
}

export function getBacktestRuns() {
  return fetchJson<{ runs: BacktestRunRecord[] }>("/backtests/runs");
}

export function getBacktestRun(runId: string) {
  return fetchJson<BacktestRunRecord>(`/backtests/runs/${runId}`);
}

export function getBacktestRunTrades(runId: string) {
  return fetchJson<{ run_id: string; trades: BacktestTradeRecord[] }>(`/backtests/runs/${runId}/trades`);
}

export function getBacktestRunAnalytics(runId: string) {
  return fetchJson<{ run_id: string; analytics: BacktestAnalytics }>(`/backtests/runs/${runId}/analytics`);
}

export function createMarketRebuildJobs(
  symbolContract: string,
  payload: {
    start: string;
    end: string;
    tick_size?: number;
    large_orders_threshold?: number;
    profile_timezone?: string;
    target?: "bars" | "profiles" | "all";
  },
) {
  return postJson<typeof payload, RebuildJobResponse>(`/markets/${symbolContract}/rebuild/jobs`, payload);
}
