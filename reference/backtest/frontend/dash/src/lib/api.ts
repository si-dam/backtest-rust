export type Json = Record<string, unknown> | Array<unknown>;

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...(init?.headers ?? {}),
    },
  });
  if (!res.ok) {
    let detail = `HTTP ${res.status}`;
    try {
      const payload = await res.json();
      if (typeof payload?.detail === 'string' && payload.detail) detail = payload.detail;
    } catch {
      // no-op
    }
    throw new Error(detail);
  }
  return (await res.json()) as T;
}

export type WatchFile = { name: string; size_bytes: number };
export type SymbolRow = { symbol_contract: string; start_ts: string; end_ts: string; tick_count: number };
export type JobRow = { id: string; status: string; payload?: { file_path?: string }; created_at: string };
export type BacktestRun = {
  id: string;
  name: string;
  strategy_id: string;
  metrics?: { net_pnl?: number };
  status: string;
  created_at: string;
};

export type BacktestTrade = {
  id: string;
  symbol_contract?: string;
  entry_ts: string | null;
  exit_ts: string | null;
  entry_price?: number | null;
  exit_price?: number | null;
  qty: number;
  pnl: number;
  side: 'long' | 'short' | 'flat';
  notes?: string | null;
};

export type BacktestRunConfig = {
  run_id: string;
  name: string;
  strategy_id: string;
  params: Record<string, unknown>;
  status: string;
  created_at: string;
};

export type BacktestAnalytics = {
  run: BacktestRun & { trade_count?: number };
  summary?: {
    trades?: number;
    wins?: number;
    losses?: number;
    win_rate?: number;
    max_drawdown?: number;
    net_pnl?: number;
    profit_factor?: number | null;
    avg_pnl?: number;
    avg_win?: number | null;
    avg_loss?: number | null;
    largest_loser?: number | null;
    max_consecutive_losses?: number;
  };
  equity_curve?: Array<{ ts?: string | null; equity_pnl?: number }>;
  drawdown_curve?: Array<{ ts?: string | null; drawdown_pnl?: number }>;
  pnl_by_time_of_day?: Array<{ bucket_hhmm: string; trades: number; pnl: number }>;
  pnl_by_day?: Array<{ date: string; trades: number; pnl: number }>;
  outliers?: {
    best_10_days?: Array<{ date: string; trades: number; pnl: number; first_entry_ts?: string | null; last_exit_ts?: string | null }>;
    worst_10_days?: Array<{ date: string; trades: number; pnl: number; first_entry_ts?: string | null; last_exit_ts?: string | null }>;
  };
};

export type BacktestStrategyParam = {
  name: string;
  type: string;
  required: boolean;
  default?: string | number | boolean;
  options?: string[];
};

export type BacktestStrategy = {
  id: string;
  label: string;
  params: BacktestStrategyParam[];
  defaults?: Record<string, string | number | boolean>;
};

export type ChartBar = {
  ts: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
};

export type LargeOrder = {
  ts: string;
  price: number;
  qty: number;
  side?: string;
};

export async function getWatchFiles() {
  return request<WatchFile[]>('/ingest/watch-files');
}

export async function queueIngest(file_name: string, symbol_contract: string | null, rebuild: boolean) {
  return request<{ count: number }>('/ingest/jobs', {
    method: 'POST',
    body: JSON.stringify({ file_name, symbol_contract, scan_watch_dir: false, rebuild }),
  });
}

export async function getSymbols() {
  return request<SymbolRow[]>('/symbols');
}

export async function getIngestJobs() {
  return request<JobRow[]>('/ingest/jobs');
}

export async function getQueueHealth() {
  return request<{ queue_name: string; queued_count: number; active_worker_count: number }>('/ingest/queue-health');
}

export async function queueBacktest(payload: { mode: 'run' | 'sweep'; name: string; strategy_id: string; params: Record<string, unknown> }) {
  return request<{ job_id: string }>('/backtests/jobs', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
}

export async function getBacktestRuns() {
  return request<BacktestRun[]>('/backtests/runs');
}

export async function getBacktestStrategies() {
  return request<BacktestStrategy[]>('/backtests/strategies');
}

export type BacktestJob = {
  id: string;
  job_type: string;
  status: string;
  payload?: Record<string, unknown>;
  result?: Record<string, unknown> | null;
  error?: string | null;
  created_at: string;
  updated_at: string;
};

export async function getBacktestJob(jobId: string) {
  return request<BacktestJob>(`/backtests/jobs/${jobId}`);
}

export async function getBacktestRunTrades(runId: string) {
  return request<BacktestTrade[]>(`/backtests/runs/${runId}/trades`);
}

export async function getBacktestRunConfig(runId: string) {
  return request<BacktestRunConfig>(`/backtests/runs/${runId}/export/config.json`);
}

export async function getBacktestRunAnalytics(runId: string) {
  return request<BacktestAnalytics>(`/backtests/runs/${runId}/analytics`);
}

export type SymbolCoverage = {
  symbol_contract: string;
  first_ts?: string | null;
  last_ts?: string | null;
  session_count?: number | null;
  missing_weekday_count?: number | null;
  missing_weekday_dates?: string[] | null;
};

export async function getSymbolCoverage(symbolContract: string) {
  return request<SymbolCoverage>(`/symbols/${encodeURIComponent(symbolContract)}/coverage`);
}

export async function getBars(params: {
  symbol_contract: string;
  timeframe: string;
  start: string;
  end: string;
  bar_type: string;
  bar_size?: number;
}) {
  const q = new URLSearchParams();
  q.set('symbol_contract', params.symbol_contract);
  q.set('timeframe', params.timeframe);
  q.set('start', params.start);
  q.set('end', params.end);
  q.set('bar_type', params.bar_type);
  if (params.bar_size && params.bar_type !== 'time') q.set('bar_size', String(params.bar_size));
  return request<ChartBar[]>(`/chart/bars?${q.toString()}`);
}

export async function getLargeOrders(params: { symbol_contract: string; start: string; end: string; fixed_threshold: number }) {
  const q = new URLSearchParams({
    symbol_contract: params.symbol_contract,
    start: params.start,
    end: params.end,
    method: 'fixed',
    fixed_threshold: String(params.fixed_threshold),
  });
  return request<LargeOrder[]>(`/chart/overlays/large-orders?${q.toString()}`);
}

export async function getVwapPreset(params: {
  symbol_contract: string;
  start: string;
  end: string;
  timezone: string;
  preset: 'day' | 'eth' | 'rth';
}) {
  const q = new URLSearchParams({
    symbol_contract: params.symbol_contract,
    start: params.start,
    end: params.end,
    timezone: params.timezone,
    preset: params.preset,
  });
  return request<{ segments?: Array<{ points: Array<{ ts: string; vwap: number }> }> }>(`/chart/overlays/vwap/preset?${q.toString()}`);
}
