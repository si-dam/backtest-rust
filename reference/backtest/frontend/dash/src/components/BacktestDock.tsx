import { FormEvent, useEffect, useMemo, useRef, useState } from 'react';
import {
  getBacktestJob,
  getBacktestRunAnalytics,
  getBacktestRunConfig,
  getBacktestRunTrades,
  getBacktestRuns,
  getBacktestStrategies,
  getSymbolCoverage,
  queueBacktest,
  type BacktestAnalytics,
  type BacktestJob,
  type BacktestRun,
  type BacktestRunConfig,
  type BacktestStrategy,
  type BacktestTrade,
  type SymbolCoverage,
} from '../lib/api';

type BacktestTab = 'summary' | 'trades' | 'analytics' | 'compare' | 'exports';

type Props = {
  paneId: 'a' | 'b';
  open: boolean;
  width: number;
  activeTab: BacktestTab;
  onClose: () => void;
  onWidthChange: (next: number) => void;
  onTabChange: (next: BacktestTab) => void;
  chartContext: {
    symbol: string;
    start: string;
    end: string;
    timeframe: string;
  };
  onChartContextChange: (patch: Partial<{ symbol: string; start: string; end: string; timeframe: string }>) => void;
};

const BACKTEST_QUEUE_TIMEOUT_MS = 60 * 60 * 1000;
const BACKTEST_RUNTIME_TIMEOUT_MS = 25 * 60 * 1000;
const BACKTEST_POLL_INTERVAL_MS = 2000;
const FALLBACK_STRATEGY_ID = 'orb_breakout_v1';
const HIDDEN_STRATEGY_IDS = new Set(['scaffold']);

const CORE_PARAM_NAMES_BY_STRATEGY: Record<string, Set<string>> = {
  [FALLBACK_STRATEGY_ID]: new Set(['timeframe', 'ib_minutes', 'tp_r_multiple', 'rth_only']),
};

type PresetRow = { name: string; strategy_id: string; mode: 'run' | 'sweep'; params: Record<string, unknown> };

function clampWidth(width: number): number {
  return Math.max(320, Math.min(760, Math.round(width)));
}

function titleCaseKey(value: string): string {
  return String(value || '')
    .split('_')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function paramLabel(name: string): string {
  if (name === 'ib_minutes') return 'OR length';
  if (name === 'rth_only') return 'RTH Only';
  if (name === 'tp_r_multiple') return 'R:R';
  if (name === 'contracts') return 'Number of Contracts';
  return titleCaseKey(name);
}

function parseTimeframeMinutes(timeframe: string): number {
  const text = String(timeframe || '').trim().toLowerCase();
  if (text.endsWith('m')) {
    const minutes = Number.parseInt(text.slice(0, -1), 10);
    return Number.isFinite(minutes) && minutes > 0 ? minutes : 1;
  }
  if (text.endsWith('h')) {
    const hours = Number.parseInt(text.slice(0, -1), 10);
    const minutes = hours * 60;
    return Number.isFinite(minutes) && minutes > 0 ? minutes : 60;
  }
  if (text.endsWith('d')) {
    const days = Number.parseInt(text.slice(0, -1), 10);
    const minutes = days * 24 * 60;
    return Number.isFinite(minutes) && minutes > 0 ? minutes : 24 * 60;
  }
  return 1;
}

function normalizeDatetimeLocal(raw: string): string {
  const value = String(raw || '').trim();
  if (!value) return '';
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) return value;
  const yyyy = parsed.getFullYear();
  const mm = String(parsed.getMonth() + 1).padStart(2, '0');
  const dd = String(parsed.getDate()).padStart(2, '0');
  const hh = String(parsed.getHours()).padStart(2, '0');
  const mi = String(parsed.getMinutes()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}`;
}

function normalizeDateInput(raw: string): string {
  const value = String(raw || '').trim();
  if (!value) return '';
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) return '';
  return parsed.toISOString().slice(0, 10);
}

function simplifyTradeTimestamp(raw: string | null | undefined): string {
  const value = String(raw ?? '').trim();
  if (!value) return '';
  const withSpace = value.replace('T', ' ');
  const withoutTz = withSpace.replace(/([+-]\d{2}:\d{2}|Z)$/i, '');
  return withoutTz.replace(/\.\d+$/, '');
}

function tradeOutcomeLabel(trade: BacktestTrade): 'Win' | 'Loss' | 'Flat' {
  const pnl = Number(trade.pnl);
  if (!Number.isFinite(pnl) || pnl === 0) return 'Flat';
  return pnl > 0 ? 'Win' : 'Loss';
}

function tradeTpSlLabel(trade: BacktestTrade): string {
  const rawNotes = String(trade.notes ?? '').trim();
  if (!rawNotes) return '-';
  try {
    const decoded = JSON.parse(rawNotes) as { exit_reason?: unknown };
    const exitReason = String(decoded.exit_reason ?? '').trim().toLowerCase();
    if (exitReason === 'target') return 'Full TP';
    if (exitReason === 'stop') return 'Full SL';
    return '-';
  } catch {
    return '-';
  }
}

function parseStrictInteger(raw: string, paramName: string): number {
  const text = String(raw || '').trim();
  if (!/^[+-]?\d+$/.test(text)) {
    throw new Error(`${paramName} must be an integer`);
  }
  const parsed = Number(text);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${paramName} must be a safe integer`);
  }
  return parsed;
}

function parseParamValue(type: string, raw: string, paramName: string): unknown {
  const normalizedType = String(type || '').toLowerCase();
  if (normalizedType === 'integer') {
    return parseStrictInteger(raw, paramName);
  }
  if (normalizedType === 'number') {
    const text = String(raw || '').trim();
    if (!text) return 0;
    const parsed = Number(text);
    if (!Number.isFinite(parsed)) {
      throw new Error(`${paramName} must be a number`);
    }
    return parsed;
  }
  if (normalizedType === 'boolean') {
    const text = String(raw || '').trim().toLowerCase();
    if (!text) return false;
    if (['1', 'true', 't', 'yes', 'y', 'on'].includes(text)) return true;
    if (['0', 'false', 'f', 'no', 'n', 'off'].includes(text)) return false;
    return false;
  }
  return String(raw ?? '');
}

function curvePath(rows: Array<{ y: number }>, width: number, height: number): string {
  if (!rows.length) return '';
  const ys = rows.map((r) => r.y).filter(Number.isFinite);
  if (!ys.length) return '';
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const span = Math.max(1e-9, maxY - minY);
  const xStep = rows.length <= 1 ? 0 : (width - 8) / (rows.length - 1);
  return rows
    .map((r, idx) => {
      const x = 4 + idx * xStep;
      const y = 4 + ((maxY - r.y) / span) * (height - 8);
      return `${idx === 0 ? 'M' : 'L'}${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(' ');
}

function parseCsvValues(raw: string): string[] {
  return String(raw || '')
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean);
}

function parseBatchPeriods(
  raw: string,
  defaultStart: string,
  defaultEnd: string
): Array<{ label: string; start: string; end: string }> {
  const lines = String(raw || '').split('\n').map((line) => line.trim()).filter(Boolean);
  if (!lines.length) {
    return [{ label: 'selected', start: defaultStart, end: defaultEnd }];
  }

  const periods: Array<{ label: string; start: string; end: string }> = [];
  lines.forEach((line, idx) => {
    const parts = line.split(',').map((part) => part.trim()).filter(Boolean);
    if (parts.length === 2) {
      periods.push({ label: `period-${idx + 1}`, start: parts[0], end: parts[1] });
      return;
    }
    if (parts.length >= 3) {
      periods.push({ label: parts[0], start: parts[1], end: parts[2] });
      return;
    }
    throw new Error(`Invalid batch period line: ${line}`);
  });
  return periods;
}

function toIsoOrRaw(value: string): string {
  const text = String(value || '').trim();
  if (!text) return '';
  const parsed = new Date(text);
  if (!Number.isFinite(parsed.getTime())) return text;
  return parsed.toISOString();
}

async function sleepMs(ms: number) {
  await new Promise((r) => window.setTimeout(r, ms));
}

export function BacktestDock(props: Props) {
  const [runs, setRuns] = useState<BacktestRun[]>([]);
  const [selectedRunId, setSelectedRunId] = useState('');
  const [strategies, setStrategies] = useState<BacktestStrategy[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [reloadToken, setReloadToken] = useState(0);

  const [analytics, setAnalytics] = useState<BacktestAnalytics | null>(null);
  const [trades, setTrades] = useState<BacktestTrade[]>([]);
  const [config, setConfig] = useState<BacktestRunConfig | null>(null);

  const [jobStatus, setJobStatus] = useState('');
  const [coverage, setCoverage] = useState<SymbolCoverage | null>(null);
  const coverageCacheRef = useRef<Map<string, SymbolCoverage>>(new Map());
  const [tradeSideFilter, setTradeSideFilter] = useState<'all' | 'long' | 'short' | 'flat'>('all');
  const [tradeResultFilter, setTradeResultFilter] = useState<'all' | 'Win' | 'Loss' | 'Flat'>('all');
  const [tradeTpSlFilter, setTradeTpSlFilter] = useState<'all' | 'Full TP' | 'Full SL' | '-'>('all');

  const [mode, setMode] = useState<'run' | 'sweep'>(() => {
    try {
      const raw = localStorage.getItem('dash-react.chart.backtest.form.v1');
      if (!raw) return 'run';
      const parsed = JSON.parse(raw) as { mode?: 'run' | 'sweep' };
      return parsed.mode === 'sweep' ? 'sweep' : 'run';
    } catch {
      return 'run';
    }
  });

  const [name, setName] = useState(() => {
    try {
      const raw = localStorage.getItem('dash-react.chart.backtest.form.v1');
      if (!raw) return 'Chart Backtest';
      const parsed = JSON.parse(raw) as { name?: string };
      return parsed.name || 'Chart Backtest';
    } catch {
      return 'Chart Backtest';
    }
  });

  const [strategyId, setStrategyId] = useState(() => {
    try {
      const raw = localStorage.getItem('dash-react.chart.backtest.form.v1');
      if (!raw) return '';
      const parsed = JSON.parse(raw) as { strategyId?: string };
      return parsed.strategyId || '';
    } catch {
      return '';
    }
  });

  const [paramsMap, setParamsMap] = useState<Record<string, string>>(() => {
    try {
      const raw = localStorage.getItem('dash-react.chart.backtest.form.v1');
      if (!raw) return {};
      const parsed = JSON.parse(raw) as { paramsMap?: Record<string, string> };
      return parsed.paramsMap || {};
    } catch {
      return {};
    }
  });

  const [advancedParamsOpen, setAdvancedParamsOpen] = useState<boolean>(() => {
    try {
      const raw = localStorage.getItem('dash-react.chart.backtest.view.v1');
      if (!raw) return false;
      const parsed = JSON.parse(raw) as { advancedParamsOpen?: boolean };
      return parsed.advancedParamsOpen === true;
    } catch {
      return false;
    }
  });

  const [runOptionsOpen, setRunOptionsOpen] = useState<boolean>(() => {
    try {
      const raw = localStorage.getItem('dash-react.chart.backtest.view.v1');
      if (!raw) return true;
      const parsed = JSON.parse(raw) as { runOptionsOpen?: boolean };
      return parsed.runOptionsOpen !== false;
    } catch {
      return true;
    }
  });

  const [splitEnabled, setSplitEnabled] = useState<boolean>(() => {
    try {
      const raw = localStorage.getItem('dash-react.chart.backtest.form.v1');
      if (!raw) return false;
      const parsed = JSON.parse(raw) as { splitEnabled?: boolean };
      return parsed.splitEnabled === true;
    } catch {
      return false;
    }
  });
  const [splitAtLocal, setSplitAtLocal] = useState(() => {
    try {
      const raw = localStorage.getItem('dash-react.chart.backtest.form.v1');
      if (!raw) return '';
      const parsed = JSON.parse(raw) as { splitAtLocal?: string };
      return parsed.splitAtLocal || '';
    } catch {
      return '';
    }
  });
  const [batchSymbols, setBatchSymbols] = useState(() => {
    try {
      const raw = localStorage.getItem('dash-react.chart.backtest.form.v1');
      if (!raw) return '';
      const parsed = JSON.parse(raw) as { batchSymbols?: string };
      return parsed.batchSymbols || '';
    } catch {
      return '';
    }
  });
  const [batchPeriods, setBatchPeriods] = useState(() => {
    try {
      const raw = localStorage.getItem('dash-react.chart.backtest.form.v1');
      if (!raw) return '';
      const parsed = JSON.parse(raw) as { batchPeriods?: string };
      return parsed.batchPeriods || '';
    } catch {
      return '';
    }
  });

  const presetsStorageKey = useMemo(
    () => `dash-react.chart.pane.${props.paneId}.backtest-presets.v1`,
    [props.paneId]
  );
  const [presetName, setPresetName] = useState('');
  const [presetSelected, setPresetSelected] = useState('');
  const [presets, setPresets] = useState<PresetRow[]>(() => {
    try {
      const raw = localStorage.getItem(presetsStorageKey);
      if (!raw) return [];
      const parsed = JSON.parse(raw) as PresetRow[];
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  });

  const [compareLeftId, setCompareLeftId] = useState('');
  const [compareRightId, setCompareRightId] = useState('');
  const [compareLeftAnalytics, setCompareLeftAnalytics] = useState<BacktestAnalytics | null>(null);
  const [compareRightAnalytics, setCompareRightAnalytics] = useState<BacktestAnalytics | null>(null);

  const [fieldError, setFieldError] = useState<Record<string, string>>({});
  const lastPayloadRef = useRef<{ mode: 'run' | 'sweep'; name: string; strategy_id: string; params: Record<string, unknown> } | null>(null);
  const previousStrategyIdRef = useRef('');

  const selectedRun = useMemo(
    () => runs.find((r) => r.id === selectedRunId) ?? null,
    [runs, selectedRunId]
  );
  const filteredTrades = useMemo(
    () =>
      trades.filter((trade) => {
        if (tradeSideFilter !== 'all' && trade.side !== tradeSideFilter) return false;
        const result = tradeOutcomeLabel(trade);
        if (tradeResultFilter !== 'all' && result !== tradeResultFilter) return false;
        const tpSl = tradeTpSlLabel(trade);
        if (tradeTpSlFilter !== 'all' && tpSl !== tradeTpSlFilter) return false;
        return true;
      }),
    [trades, tradeSideFilter, tradeResultFilter, tradeTpSlFilter]
  );

  const selectedStrategy = useMemo(
    () => strategies.find((s) => s.id === strategyId) ?? null,
    [strategies, strategyId]
  );

  const coreNames = useMemo(() => CORE_PARAM_NAMES_BY_STRATEGY[strategyId] ?? new Set<string>(), [strategyId]);
  const forceAdvancedNames = useMemo(() => new Set(['tp_r_multiple']), []);
  const dynamicParams = useMemo(
    () => (selectedStrategy?.params ?? []).filter((p) => !['symbol_contract', 'start', 'end'].includes(p.name)),
    [selectedStrategy]
  );
  const timeframeParamSpec = useMemo(() => dynamicParams.find((p) => p.name === 'timeframe') ?? null, [dynamicParams]);
  const ibMinutesParamSpec = useMemo(() => dynamicParams.find((p) => p.name === 'ib_minutes') ?? null, [dynamicParams]);
  const rthOnlyParamSpec = useMemo(() => dynamicParams.find((p) => p.name === 'rth_only') ?? null, [dynamicParams]);
  const coreParams = useMemo(
    () => dynamicParams.filter((p) => coreNames.has(p.name) && !forceAdvancedNames.has(p.name) && !['timeframe', 'ib_minutes', 'rth_only'].includes(p.name)),
    [dynamicParams, coreNames, forceAdvancedNames]
  );
  const advancedParams = useMemo(
    () => dynamicParams.filter((p) => (!coreNames.has(p.name) || forceAdvancedNames.has(p.name)) && !['timeframe', 'ib_minutes', 'rth_only'].includes(p.name)),
    [dynamicParams, coreNames, forceAdvancedNames]
  );

  useEffect(() => {
    if (!props.open) return;
    setLoading(true);
    setError('');
    void Promise.all([getBacktestRuns(), getBacktestStrategies()])
      .then(([nextRuns, nextStrategies]) => {
        setRuns(nextRuns);
        const visible = nextStrategies.filter((s) => !HIDDEN_STRATEGY_IDS.has(s.id));
        const list = visible.length ? visible : nextStrategies;
        setStrategies(list);
        if (!selectedRunId && nextRuns.length) setSelectedRunId(nextRuns[0].id);
        const preferred = list.find((s) => s.id === strategyId) ?? list.find((s) => s.id === FALLBACK_STRATEGY_ID) ?? list[0];
        if (preferred) setStrategyId(preferred.id);
      })
      .catch((err: Error) => {
        setError(err.message);
        setStrategies([
          {
            id: FALLBACK_STRATEGY_ID,
            label: 'ORB Breakout V1',
            defaults: {
              name: 'ORB Breakout V1',
              timeframe: '1m',
              ib_minutes: 15,
              rth_only: true,
              stop_mode: 'or_boundary',
              tp_r_multiple: 2.0,
              entry_mode: 'first_outside',
              strategy_mode: 'breakout_only',
              big_trade_threshold: 25,
              contracts: 1,
            },
            params: [
              { name: 'symbol_contract', type: 'string', required: true },
              { name: 'start', type: 'datetime', required: true },
              { name: 'end', type: 'datetime', required: true },
              { name: 'timeframe', type: 'enum', required: true, options: ['1m', '3m', '5m', '15m', '30m', '60m'], default: '1m' },
              { name: 'ib_minutes', type: 'integer', required: true, default: 15 },
              { name: 'rth_only', type: 'boolean', required: true, default: true },
              { name: 'stop_mode', type: 'enum', required: true, options: ['or_boundary', 'or_mid'], default: 'or_boundary' },
              { name: 'tp_r_multiple', type: 'number', required: true, default: 2.0 },
              { name: 'entry_mode', type: 'enum', required: true, options: ['first_outside', 'reentry_after_stop'], default: 'first_outside' },
              { name: 'strategy_mode', type: 'enum', required: true, options: ['breakout_only', 'big_order_required'], default: 'breakout_only' },
              { name: 'big_trade_threshold', type: 'integer', required: true, default: 25 },
              { name: 'contracts', type: 'integer', required: false, default: 1 },
            ],
          },
        ]);
        if (!strategyId) setStrategyId(FALLBACK_STRATEGY_ID);
      })
      .finally(() => setLoading(false));
  }, [props.open, reloadToken, selectedRunId, strategyId]);

  useEffect(() => {
    if (!selectedStrategy) return;

    const isStrategyChanged = previousStrategyIdRef.current !== selectedStrategy.id;
    const prefills: Record<string, string> = {};

    for (const p of selectedStrategy.params) {
      if (p.name === 'symbol_contract' || p.name === 'start' || p.name === 'end') continue;

      let nextValue: string | undefined;
      if (p.name === 'timeframe') {
        const tf = props.chartContext.timeframe;
        if (Array.isArray(p.options) && p.options.includes(tf)) {
          nextValue = tf;
        }
      }

      if (nextValue === undefined) {
        const strategyDefault = selectedStrategy.defaults && p.name in selectedStrategy.defaults ? selectedStrategy.defaults[p.name] : undefined;
        const paramDefault = p.default;
        const defaultValue = strategyDefault ?? paramDefault;
        if (defaultValue !== undefined) {
          nextValue = String(defaultValue);
        } else if (p.type === 'enum' && Array.isArray(p.options) && p.options.length > 0) {
          nextValue = String(p.options[0]);
        } else {
          nextValue = p.type === 'boolean' ? 'false' : '';
        }
      }

      prefills[p.name] = nextValue;
    }

    setParamsMap((prev) => {
      if (isStrategyChanged) {
        return prefills;
      }
      const merged: Record<string, string> = {};
      Object.keys(prefills).forEach((key) => {
        const prevValue = prev[key];
        merged[key] = prevValue !== undefined && String(prevValue).trim() !== '' ? prevValue : prefills[key];
      });
      return merged;
    });

    if (isStrategyChanged) {
      const runNameDefault = selectedStrategy.defaults?.name;
      if (runNameDefault !== undefined) {
        setName(String(runNameDefault));
      } else {
        setName(selectedStrategy.label);
      }
    }

    previousStrategyIdRef.current = selectedStrategy.id;
  }, [selectedStrategy, props.chartContext.timeframe]);

  useEffect(() => {
    const payload = { mode, name, strategyId, paramsMap, splitEnabled, splitAtLocal, batchSymbols, batchPeriods };
    try {
      localStorage.setItem('dash-react.chart.backtest.form.v1', JSON.stringify(payload));
    } catch {
      // Ignore storage failures.
    }
  }, [mode, name, strategyId, paramsMap, splitEnabled, splitAtLocal, batchSymbols, batchPeriods]);

  useEffect(() => {
    const payload = { advancedParamsOpen, runOptionsOpen };
    try {
      localStorage.setItem('dash-react.chart.backtest.view.v1', JSON.stringify(payload));
    } catch {
      // ignore
    }
  }, [advancedParamsOpen, runOptionsOpen]);

  useEffect(() => {
    try {
      localStorage.setItem(presetsStorageKey, JSON.stringify(presets));
    } catch {
      // ignore
    }
  }, [presets, presetsStorageKey]);

  useEffect(() => {
    if (!props.open || !selectedRunId) return;
    setLoading(true);
    setError('');
    void Promise.all([
      getBacktestRunAnalytics(selectedRunId),
      getBacktestRunTrades(selectedRunId),
      getBacktestRunConfig(selectedRunId),
    ])
      .then(([nextAnalytics, nextTrades, nextConfig]) => {
        setAnalytics(nextAnalytics);
        setTrades(nextTrades);
        setConfig(nextConfig);
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [props.open, selectedRunId, reloadToken]);

  useEffect(() => {
    if (!props.open) return;
    const symbol = String(props.chartContext.symbol || '').trim();
    if (!symbol) {
      setCoverage(null);
      return;
    }
    const cached = coverageCacheRef.current.get(symbol);
    if (cached) {
      setCoverage(cached);
      return;
    }
    void getSymbolCoverage(symbol)
      .then((out) => {
        coverageCacheRef.current.set(symbol, out);
        setCoverage(out);
      })
      .catch(() => setCoverage(null));
  }, [props.open, props.chartContext.symbol, reloadToken]);

  useEffect(() => {
    if (!props.open) return;
    const onMove = (event: PointerEvent) => props.onWidthChange(clampWidth(window.innerWidth - event.clientX));
    const onUp = () => {
      window.removeEventListener('pointermove', onMove);
      window.removeEventListener('pointerup', onUp);
    };
    const onStartResize = (event: PointerEvent) => {
      const target = event.target as HTMLElement | null;
      if (!target || !target.classList.contains('backtest-dock-resize')) return;
      window.addEventListener('pointermove', onMove);
      window.addEventListener('pointerup', onUp);
    };
    window.addEventListener('pointerdown', onStartResize);
    return () => {
      window.removeEventListener('pointerdown', onStartResize);
      window.removeEventListener('pointermove', onMove);
      window.removeEventListener('pointerup', onUp);
    };
  }, [props.open, props.onWidthChange]);

  async function pollJob(jobId: string): Promise<BacktestJob> {
    const startedAt = Date.now();
    while (true) {
      const job = await getBacktestJob(jobId);
      const status = String(job.status || '').toLowerCase();
      if (['succeeded', 'failed', 'canceled'].includes(status)) return job;

      const ageMs = Date.now() - startedAt;
      const timeoutMs = status === 'queued' ? BACKTEST_QUEUE_TIMEOUT_MS : BACKTEST_RUNTIME_TIMEOUT_MS;
      if (ageMs > timeoutMs) throw new Error(`Backtest job timed out (${status}).`);
      await sleepMs(BACKTEST_POLL_INTERVAL_MS);
    }
  }

  function buildPayload() {
    const strategy = selectedStrategy ?? null;
    if (!strategy) throw new Error('Select a strategy before running backtest.');

    const symbol = String(props.chartContext.symbol || '').trim();
    const start = String(props.chartContext.start || '').trim();
    const end = String(props.chartContext.end || '').trim();
    if (!symbol || !start || !end) throw new Error('Select symbol and valid date range before running backtest.');

    const nextFieldErrors: Record<string, string> = {};
    const params: Record<string, unknown> = { symbol_contract: symbol, start, end };

    for (const p of strategy.params ?? []) {
      if (p.name === 'symbol_contract' || p.name === 'start' || p.name === 'end') continue;
      if (p.name === 'timeframe') continue; // timeframe comes from paramsMap but also drives chart; handled below

      const raw = paramsMap[p.name] ?? '';
      if (raw === '' && !p.required) continue;
      try {
        params[p.name] = parseParamValue(p.type, raw, p.name);
      } catch (err) {
        nextFieldErrors[p.name] = (err as Error).message;
      }
    }

    // Always include timeframe in params for parity with legacy payloads.
    const timeframeValue = String(paramsMap.timeframe || props.chartContext.timeframe || '').trim();
    if (timeframeValue) params.timeframe = timeframeValue;

    setFieldError(nextFieldErrors);
    if (Object.keys(nextFieldErrors).length) throw new Error('Fix invalid parameter values before running.');

    const defaults = (strategy.defaults && typeof strategy.defaults === 'object') ? strategy.defaults : {};
    const defaultName = String(defaults.name ?? strategy.label ?? strategy.id ?? 'Backtest Run');
    return {
      mode: 'run' as const,
      name: String(name || defaultName),
      strategy_id: String(strategy.id),
      params,
    };
  }

  async function onQueue(event: FormEvent) {
    event.preventDefault();
    setLoading(true);
    setJobStatus('');
    try {
      const payload = buildPayload();
      lastPayloadRef.current = payload;
      setJobStatus('Queueing backtest job...');
      const out = await queueBacktest(payload);
      setJobStatus(`Queued job ${out.job_id}. Waiting for completion...`);
      const job = await pollJob(out.job_id);
      const runId = (job.result && typeof job.result === 'object' && 'run_id' in job.result)
        ? String((job.result as Record<string, unknown>).run_id || '')
        : '';
      const runIds = (job.result && typeof job.result === 'object' && Array.isArray((job.result as Record<string, unknown>).run_ids))
        ? ((job.result as Record<string, unknown>).run_ids as unknown[]).map((x) => String(x))
        : [];
      const primaryRunId = runId || (runIds.length ? runIds[0] : '');

      const nextRuns = await getBacktestRuns();
      setRuns(nextRuns);
      if (primaryRunId) setSelectedRunId(primaryRunId);

      setJobStatus(primaryRunId ? `Run ${primaryRunId} loaded.` : 'Backtest completed. Refreshing run list.');
    } catch (err) {
      setJobStatus((err as Error).message);
    } finally {
      setLoading(false);
    }
  }

  async function rerunLast() {
    const payload = lastPayloadRef.current;
    if (!payload) {
      setJobStatus('No prior run payload found. Run once first.');
      return;
    }
    setLoading(true);
    setJobStatus('Queueing rerun...');
    try {
      const out = await queueBacktest(payload);
      const job = await pollJob(out.job_id);
      const runId = (job.result && typeof job.result === 'object' && 'run_id' in job.result)
        ? String((job.result as Record<string, unknown>).run_id || '')
        : '';
      const runIds = (job.result && typeof job.result === 'object' && Array.isArray((job.result as Record<string, unknown>).run_ids))
        ? ((job.result as Record<string, unknown>).run_ids as unknown[]).map((x) => String(x))
        : [];
      const primaryRunId = runId || (runIds.length ? runIds[0] : '');

      const nextRuns = await getBacktestRuns();
      setRuns(nextRuns);
      if (primaryRunId) setSelectedRunId(primaryRunId);
      setJobStatus('Rerun completed.');
    } catch (err) {
      setJobStatus((err as Error).message);
    } finally {
      setLoading(false);
    }
  }

  function savePreset() {
    const presetLabel = String(presetName || '').trim();
    if (!presetLabel) {
      setJobStatus('Preset name is required.');
      return;
    }
    try {
      const payload = buildPayload();
      const row: PresetRow = { name: presetLabel, strategy_id: payload.strategy_id, mode: payload.mode, params: payload.params };
      setPresets((prev) => {
        const next = [...prev];
        const idx = next.findIndex((p) => p.name === presetLabel);
        if (idx >= 0) next[idx] = row;
        else next.push(row);
        next.sort((a, b) => String(a.name).localeCompare(String(b.name)));
        return next;
      });
      setPresetSelected(presetLabel);
      setJobStatus(`Preset "${presetLabel}" saved.`);
    } catch (err) {
      setJobStatus((err as Error).message);
    }
  }

  function applyPreset(preset: PresetRow | null) {
    if (!preset) return;
    const params = (preset.params && typeof preset.params === 'object') ? preset.params : {};

    setMode(preset.mode === 'sweep' ? 'sweep' : 'run');
    if (preset.strategy_id) setStrategyId(String(preset.strategy_id));

    const symbol = typeof (params as Record<string, unknown>).symbol_contract === 'string' ? String((params as Record<string, unknown>).symbol_contract) : '';
    const start = typeof (params as Record<string, unknown>).start === 'string' ? String((params as Record<string, unknown>).start) : '';
    const end = typeof (params as Record<string, unknown>).end === 'string' ? String((params as Record<string, unknown>).end) : '';
    const timeframe = typeof (params as Record<string, unknown>).timeframe === 'string' ? String((params as Record<string, unknown>).timeframe) : '';

    const patch: Partial<{ symbol: string; start: string; end: string; timeframe: string }> = {};
    if (symbol) patch.symbol = symbol;
    if (start) patch.start = toIsoOrRaw(start) || start;
    if (end) patch.end = toIsoOrRaw(end) || end;
    if (timeframe) patch.timeframe = timeframe;
    if (Object.keys(patch).length) props.onChartContextChange(patch);

    const split = (params as Record<string, unknown>).split;
    if (split && typeof split === 'object') {
      const enabled = Boolean((split as Record<string, unknown>).enabled);
      const splitAt = typeof (split as Record<string, unknown>).split_at === 'string' ? String((split as Record<string, unknown>).split_at) : '';
      setSplitEnabled(enabled);
      setSplitAtLocal(splitAt ? normalizeDatetimeLocal(splitAt) : '');
    } else {
      setSplitEnabled(false);
      setSplitAtLocal('');
    }

    const batch = (params as Record<string, unknown>).batch;
    if (batch && typeof batch === 'object') {
      const symbols = Array.isArray((batch as Record<string, unknown>).symbols)
        ? ((batch as Record<string, unknown>).symbols as unknown[]).map((x) => String(x))
        : [];
      setBatchSymbols(symbols.join(', '));
      const periods = Array.isArray((batch as Record<string, unknown>).periods) ? ((batch as Record<string, unknown>).periods as unknown[]) : [];
      const text = periods
        .map((row) => {
          if (!row || typeof row !== 'object') return '';
          const label = String((row as Record<string, unknown>).label || '');
          const start = String((row as Record<string, unknown>).start || '');
          const end = String((row as Record<string, unknown>).end || '');
          return `${label},${start},${end}`;
        })
        .filter(Boolean)
        .join('\n');
      setBatchPeriods(text);
    } else {
      setBatchSymbols('');
      setBatchPeriods('');
    }

    setParamsMap((prev) => {
      const next = { ...prev };
      Object.entries(params).forEach(([key, value]) => {
        if (key === 'symbol_contract' || key === 'start' || key === 'end') return;
        if (key === 'split' || key === 'batch') return;
        if (value === undefined || value === null) return;
        next[key] = String(value);
      });
      return next;
    });
  }

  function loadSelectedPreset() {
    const name = String(presetSelected || '').trim();
    if (!name) {
      setJobStatus('Select a preset to load.');
      return;
    }
    const preset = presets.find((p) => p.name === name) ?? null;
    if (!preset) {
      setJobStatus('Preset not found.');
      return;
    }
    applyPreset(preset);
    setJobStatus(`Preset "${name}" loaded.`);
  }

  function deleteSelectedPreset() {
    const name = String(presetSelected || '').trim();
    if (!name) {
      setJobStatus('Select a preset to delete.');
      return;
    }
    setPresets((prev) => prev.filter((p) => p.name !== name));
    setJobStatus(`Preset "${name}" deleted.`);
  }

  function focusRangeAroundTrade(trade: BacktestTrade) {
    const entry = trade.entry_ts ? Date.parse(trade.entry_ts) : NaN;
    const exit = trade.exit_ts ? Date.parse(trade.exit_ts) : NaN;
    const startRef = Number.isFinite(entry) ? entry : Number.isFinite(exit) ? exit : NaN;
    if (!Number.isFinite(startRef)) return;
    const endRef = Number.isFinite(exit) ? exit : startRef;

    const tf = String(paramsMap.timeframe || props.chartContext.timeframe || '1m');
    const minutes = parseTimeframeMinutes(tf);
    const paddingMinutes = Math.max(30, minutes * 20);

    const symbolPatch = trade.symbol_contract ? String(trade.symbol_contract) : '';
    props.onChartContextChange({
      ...(symbolPatch ? { symbol: symbolPatch } : {}),
      start: new Date(startRef - paddingMinutes * 60 * 1000).toISOString(),
      end: new Date(endRef + paddingMinutes * 60 * 1000).toISOString(),
    });
  }

  function focusOnOutlier(row: { first_entry_ts?: string | null; last_exit_ts?: string | null; date?: string }) {
    const first = row.first_entry_ts ? Date.parse(row.first_entry_ts) : NaN;
    const last = row.last_exit_ts ? Date.parse(row.last_exit_ts) : NaN;
    if (Number.isFinite(first) && Number.isFinite(last)) {
      props.onChartContextChange({
        start: new Date(first - 30 * 60 * 1000).toISOString(),
        end: new Date(last + 30 * 60 * 1000).toISOString(),
      });
      return;
    }
    const day = String(row.date || '').trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(day)) {
      props.onChartContextChange({ start: `${day}T00:00:00.000Z`, end: `${day}T23:59:59.000Z` });
    }
  }

  function openNativeDatePicker(inputEl: HTMLInputElement | null) {
    if (!inputEl) return;
    if (typeof inputEl.showPicker === 'function') {
      inputEl.showPicker();
      return;
    }
    inputEl.focus();
  }

  if (!props.open) return null;

  const equityPath = curvePath((analytics?.equity_curve ?? []).map((r) => ({ y: Number(r.equity_pnl) || 0 })), 320, 96);
  const drawdownPath = curvePath((analytics?.drawdown_curve ?? []).map((r) => ({ y: Number(r.drawdown_pnl) || 0 })), 320, 96);
  const startDate = normalizeDateInput(props.chartContext.start);
  const endDate = normalizeDateInput(props.chartContext.end);

  const gaps = Array.isArray(coverage?.missing_weekday_dates) ? (coverage?.missing_weekday_dates ?? []) : [];
  const gapPreview = gaps.slice(0, 6).join(', ');
  const hiddenGapCount = Math.max(0, gaps.length - 6);

  return (
    <aside className="backtest-dock" style={{ width: `${props.width}px` }}>
      <div className="backtest-dock-resize" title="Resize backtest panel" />
      <header className="backtest-dock-header">
        <strong>Backtests</strong>
        <button type="button" onClick={props.onClose}>Close</button>
      </header>

      <div className="backtest-dock-scroll">
        <section className="backtest-setup-layer">
          <div className="backtest-setup-title">Run Setup</div>
          <form className="backtest-run-form" onSubmit={(e) => void onQueue(e)}>
            <div className="bt-grid-2">
              <label className="bt-inline"><span>Strategy</span>
                <select className="control-fluid" value={strategyId} onChange={(e) => setStrategyId(e.target.value)}>
                  {!strategies.length && <option value="">No strategies</option>}
                  {strategies.map((strategy) => (
                    <option key={strategy.id} value={strategy.id}>{strategy.label}</option>
                  ))}
                </select>
              </label>
              <label className="bt-inline"><span>Run Name</span>
                <input className="control-fluid" value={name} onChange={(e) => setName(e.target.value)} />
              </label>
            </div>

            <div className="bt-grid-5">
              <label className="bt-inline"><span>Start</span>
                <input
                  className="control-fluid"
                  type="date"
                  value={startDate}
                  onClick={(e) => openNativeDatePicker(e.currentTarget)}
                  onFocus={(e) => openNativeDatePicker(e.currentTarget)}
                  onChange={(e) => {
                    const day = String(e.target.value || '').trim();
                    if (!day) return;
                    props.onChartContextChange({ start: `${day}T00:00:00.000Z` });
                  }}
                />
              </label>
              <label className="bt-inline"><span>End</span>
                <input
                  className="control-fluid"
                  type="date"
                  value={endDate}
                  onClick={(e) => openNativeDatePicker(e.currentTarget)}
                  onFocus={(e) => openNativeDatePicker(e.currentTarget)}
                  onChange={(e) => {
                    const day = String(e.target.value || '').trim();
                    if (!day) return;
                    props.onChartContextChange({ end: `${day}T23:59:59.999Z` });
                  }}
                />
              </label>
              <label className="bt-inline"><span>Timeframe</span>
                {timeframeParamSpec && timeframeParamSpec.type === 'enum' && Array.isArray(timeframeParamSpec.options) ? (
                  <select
                    className="control-fluid"
                    value={paramsMap.timeframe ?? props.chartContext.timeframe ?? ''}
                    onChange={(e) => {
                      const next = e.target.value;
                      setParamsMap((prev) => ({ ...prev, timeframe: next }));
                      props.onChartContextChange({ timeframe: next });
                    }}
                  >
                    {timeframeParamSpec.options.map((opt) => <option key={opt} value={opt}>{opt}</option>)}
                  </select>
                ) : (
                  <select
                    className="control-fluid"
                    value={paramsMap.timeframe ?? props.chartContext.timeframe ?? ''}
                    onChange={(e) => {
                      const next = e.target.value;
                      setParamsMap((prev) => ({ ...prev, timeframe: next }));
                      props.onChartContextChange({ timeframe: next });
                    }}
                  >
                    {['1m', '2m', '3m', '5m', '15m', '30m', '60m', '4h', '1d'].map((opt) => <option key={opt} value={opt}>{opt}</option>)}
                  </select>
                )}
              </label>
              <label className="bt-inline"><span>OR length</span>
                {ibMinutesParamSpec ? (
                  <input
                    className="control-fluid"
                    type="number"
                    min={1}
                    step={1}
                    value={paramsMap.ib_minutes ?? ''}
                    onChange={(e) => setParamsMap((prev) => ({ ...prev, ib_minutes: e.target.value }))}
                  />
                ) : (
                  <input className="control-fluid" type="number" min={1} step={1} value="" disabled />
                )}
                {fieldError.ib_minutes && <div className="bt-field-error">{fieldError.ib_minutes}</div>}
              </label>
              <label className="bt-inline"><span>RTH Only</span>
                {rthOnlyParamSpec ? (
                  <input
                    className="bt-checkbox"
                    type="checkbox"
                    checked={(paramsMap.rth_only ?? 'false') === 'true'}
                    onChange={(e) => setParamsMap((prev) => ({ ...prev, rth_only: e.target.checked ? 'true' : 'false' }))}
                  />
                ) : (
                  <input className="bt-checkbox" type="checkbox" checked={false} disabled />
                )}
              </label>
            </div>

            <div className="backtest-context">
              <div><span className="bt-small-label">Symbol</span><span>{props.chartContext.symbol || '-'}</span></div>
              <div><span className="bt-small-label">Start</span><span>{props.chartContext.start || '-'}</span></div>
              <div><span className="bt-small-label">End</span><span>{props.chartContext.end || '-'}</span></div>
              <div><span className="bt-small-label">Timeframe</span><span>{props.chartContext.timeframe || '-'}</span></div>
            </div>

            {!!coreParams.length && (
              <div className="bt-param-grid bt-core-param-grid" aria-label="Core parameters">
                {coreParams.map((p) => (
                  <div key={p.name} className="bt-field" data-param-name={p.name}>
                    <label className="bt-field-label">{paramLabel(p.name)}{p.required ? '*' : ''}</label>
                    {p.type === 'enum' ? (
                      <select className="control-fluid" value={paramsMap[p.name] ?? ''} onChange={(e) => setParamsMap((prev) => ({ ...prev, [p.name]: e.target.value }))}>
                        {(p.options ?? []).map((opt) => <option key={opt} value={opt}>{opt}</option>)}
                      </select>
                    ) : p.type === 'boolean' ? (
                      <input
                        className="bt-checkbox"
                        type="checkbox"
                        checked={(paramsMap[p.name] ?? 'false') === 'true'}
                        onChange={(e) => setParamsMap((prev) => ({ ...prev, [p.name]: e.target.checked ? 'true' : 'false' }))}
                      />
                    ) : (
                      <input
                        className="control-fluid"
                        value={paramsMap[p.name] ?? ''}
                        onChange={(e) => setParamsMap((prev) => ({ ...prev, [p.name]: e.target.value }))}
                        placeholder={p.type}
                      />
                    )}
                    {fieldError[p.name] && <div className="bt-field-error">{fieldError[p.name]}</div>}
                  </div>
                ))}
              </div>
            )}

            {!!advancedParams.length && (
              <details className="bt-details" open={advancedParamsOpen} onToggle={(e) => setAdvancedParamsOpen((e.target as HTMLDetailsElement).open)}>
                <summary>Advanced Parameters</summary>
                <div className="bt-param-grid">
                  {advancedParams.map((p) => (
                    <div key={p.name} className="bt-field">
                      <label className="bt-field-label">{paramLabel(p.name)}{p.required ? '*' : ''}</label>
                      {p.type === 'enum' ? (
                        <select className="control-fluid" value={paramsMap[p.name] ?? ''} onChange={(e) => setParamsMap((prev) => ({ ...prev, [p.name]: e.target.value }))}>
                          {(p.options ?? []).map((opt) => <option key={opt} value={opt}>{opt}</option>)}
                        </select>
                      ) : p.type === 'boolean' ? (
                        <input
                          className="bt-checkbox"
                          type="checkbox"
                          checked={(paramsMap[p.name] ?? 'false') === 'true'}
                          onChange={(e) => setParamsMap((prev) => ({ ...prev, [p.name]: e.target.checked ? 'true' : 'false' }))}
                        />
                      ) : (
                        <input
                          className="control-fluid"
                          value={paramsMap[p.name] ?? ''}
                          onChange={(e) => setParamsMap((prev) => ({ ...prev, [p.name]: e.target.value }))}
                          placeholder={p.type}
                        />
                      )}
                      {fieldError[p.name] && <div className="bt-field-error">{fieldError[p.name]}</div>}
                    </div>
                  ))}
                </div>
              </details>
            )}

            <div className="bt-button-row">
              <button className="control-fluid" type="submit" disabled={loading}>Run Backtest</button>
            </div>
          </form>

          <div className="bt-status">{jobStatus || ''}</div>
        </section>

        <section className="backtest-results-layer">
          <div className="backtest-results-header">
            <div className="bt-results-title">Results</div>
            <select className="control-fluid" value={selectedRunId} onChange={(e) => setSelectedRunId(e.target.value)}>
              {!runs.length && <option value="">No runs</option>}
              {runs.map((run) => (
                <option key={run.id} value={run.id}>{run.name} ({run.strategy_id})</option>
              ))}
            </select>
            <button className="control-sm" type="button" onClick={() => setReloadToken((v) => v + 1)}>Refresh</button>
          </div>

          <div className="backtest-dock-tabs">
            {(['summary', 'trades', 'analytics', 'compare', 'exports'] as const).map((tab) => (
              <button key={tab} type="button" className={props.activeTab === tab ? 'active' : ''} onClick={() => props.onTabChange(tab)}>
                {tab === 'summary' ? 'Summary' : tab === 'trades' ? 'Trades' : tab === 'analytics' ? 'Analytics' : tab === 'compare' ? 'Compare' : 'Exports'}
              </button>
            ))}
          </div>

          <section className="backtest-dock-body">
            {loading && <p className="status">Loading backtest data...</p>}
            {error && <p className="status">{error}</p>}

            {!loading && !error && props.activeTab === 'summary' && (
              <div className="bt-sections">
                <section className="bt-section">
                  <h4>Reality Check</h4>
                  {!coverage && <div className="bt-empty">Select a symbol to load data coverage.</div>}
                  {coverage && (
                    <div className="bt-coverage-grid">
                      <div><span className="bt-small-label">First</span><span>{coverage.first_ts || '-'}</span></div>
                      <div><span className="bt-small-label">Last</span><span>{coverage.last_ts || '-'}</span></div>
                      <div><span className="bt-small-label">Sessions</span><span>{String(coverage.session_count ?? '-')}</span></div>
                      <div><span className="bt-small-label">Missing Weekdays</span><span>{String(coverage.missing_weekday_count ?? '-')}</span></div>
                      <div className="full"><span className="bt-small-label">Gap Dates</span><span>{gapPreview || '-'}{hiddenGapCount ? ` (+${hiddenGapCount} more)` : ''}</span></div>
                    </div>
                  )}
                </section>

                <section className="bt-section">
                  <h4>Summary Metrics</h4>
                  <div className="backtest-kpis">
                    <div><span>Run</span><strong>{selectedRun?.name ?? '-'}</strong></div>
                    <div><span>Status</span><strong>{selectedRun?.status ?? '-'}</strong></div>
                    <div><span>Net PnL</span><strong>{String(selectedRun?.metrics?.net_pnl ?? analytics?.summary?.net_pnl ?? '-')}</strong></div>
                    <div><span>Trades</span><strong>{String(analytics?.summary?.trades ?? '-')}</strong></div>
                    <div><span>Win Rate</span><strong>{String(analytics?.summary?.win_rate ?? '-')}</strong></div>
                    <div><span>Max DD</span><strong>{String(analytics?.summary?.max_drawdown ?? '-')}</strong></div>
                    <div><span>Profit Factor</span><strong>{String(analytics?.summary?.profit_factor ?? '-')}</strong></div>
                    <div><span>Largest Loser</span><strong>{String(analytics?.summary?.largest_loser ?? '-')}</strong></div>
                  </div>
                </section>

                <section className="bt-section">
                  <h4>Performance Curves</h4>
                  <div className="backtest-analytics-blocks">
                    <div>
                      <div className="backtest-curve-title">Equity Curve</div>
                      <svg className="backtest-curve" viewBox="0 0 320 96" preserveAspectRatio="none">
                        <path d={equityPath} stroke="#8ab4f8" strokeWidth="1.8" fill="none" />
                      </svg>
                    </div>
                    <div>
                      <div className="backtest-curve-title">Drawdown Curve</div>
                      <svg className="backtest-curve" viewBox="0 0 320 96" preserveAspectRatio="none">
                        <path d={drawdownPath} stroke="#ef5350" strokeWidth="1.8" fill="none" />
                      </svg>
                    </div>
                  </div>
                </section>
              </div>
            )}

            {!loading && !error && props.activeTab === 'trades' && (
              <div className="bt-sections">
                <section className="bt-section">
                  <div className="bt-grid-3">
                    <label className="bt-inline"><span>Side</span>
                      <select className="control-fluid" value={tradeSideFilter} onChange={(e) => setTradeSideFilter(e.target.value as 'all' | 'long' | 'short' | 'flat')}>
                        <option value="all">All</option>
                        <option value="long">Long</option>
                        <option value="short">Short</option>
                        <option value="flat">Flat</option>
                      </select>
                    </label>
                    <label className="bt-inline"><span>Result</span>
                      <select className="control-fluid" value={tradeResultFilter} onChange={(e) => setTradeResultFilter(e.target.value as 'all' | 'Win' | 'Loss' | 'Flat')}>
                        <option value="all">All</option>
                        <option value="Win">Win</option>
                        <option value="Loss">Loss</option>
                        <option value="Flat">Flat</option>
                      </select>
                    </label>
                    <label className="bt-inline"><span>TP/SL</span>
                      <select className="control-fluid" value={tradeTpSlFilter} onChange={(e) => setTradeTpSlFilter(e.target.value as 'all' | 'Full TP' | 'Full SL' | '-')}>
                        <option value="all">All</option>
                        <option value="Full TP">Full TP</option>
                        <option value="Full SL">Full SL</option>
                        <option value="-">Other</option>
                      </select>
                    </label>
                  </div>
                </section>
                <div className="backtest-table-wrap">
                  <table>
                    <thead>
                      <tr><th>Entry</th><th>Exit</th><th>Side</th><th>Qty</th><th>Entry Px</th><th>Exit Px</th><th>PnL</th><th>Result</th><th>TP/SL</th></tr>
                    </thead>
                    <tbody>
                      {filteredTrades.map((trade) => (
                        <tr key={trade.id} className="bt-row-click" onClick={() => focusRangeAroundTrade(trade)} title="Click to center chart around trade">
                          <td>{simplifyTradeTimestamp(trade.entry_ts)}</td>
                          <td>{simplifyTradeTimestamp(trade.exit_ts)}</td>
                          <td>{trade.side}</td>
                          <td>{trade.qty}</td>
                          <td>{trade.entry_price ?? ''}</td>
                          <td>{trade.exit_price ?? ''}</td>
                          <td>{trade.pnl}</td>
                          <td>{tradeOutcomeLabel(trade)}</td>
                          <td>{tradeTpSlLabel(trade)}</td>
                        </tr>
                      ))}
                      {!filteredTrades.length && <tr><td colSpan={9}>-</td></tr>}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {!loading && !error && props.activeTab === 'analytics' && (
              <div className="bt-sections">
                <section className="bt-section">
                  <h4>Distribution</h4>
                  <div className="bt-grid-2">
                    <div>
                      <div className="bt-small-label">By Time of Day</div>
                      <table className="bt-mini-table">
                        <thead><tr><th>HH:MM</th><th>Trades</th><th>PnL</th></tr></thead>
                        <tbody>
                          {(analytics?.pnl_by_time_of_day ?? []).map((row) => (
                            <tr key={row.bucket_hhmm}><td>{row.bucket_hhmm}</td><td>{row.trades}</td><td>{row.pnl.toFixed(2)}</td></tr>
                          ))}
                          {!(analytics?.pnl_by_time_of_day ?? []).length && <tr><td colSpan={3}>-</td></tr>}
                        </tbody>
                      </table>
                    </div>
                    <div>
                      <div className="bt-small-label">By Day</div>
                      <table className="bt-mini-table">
                        <thead><tr><th>Date</th><th>Trades</th><th>PnL</th></tr></thead>
                        <tbody>
                          {(analytics?.pnl_by_day ?? []).slice(-10).map((row) => (
                            <tr key={row.date}><td>{row.date}</td><td>{row.trades}</td><td>{row.pnl.toFixed(2)}</td></tr>
                          ))}
                          {!(analytics?.pnl_by_day ?? []).length && <tr><td colSpan={3}>-</td></tr>}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </section>

                <section className="bt-section">
                  <h4>Outliers</h4>
                  <div className="bt-grid-2">
                    <div>
                      <div className="bt-small-label">Best 10 Days</div>
                      <table className="bt-mini-table">
                        <thead><tr><th>Date</th><th>Trades</th><th>PnL</th></tr></thead>
                        <tbody>
                          {(analytics?.outliers?.best_10_days ?? []).map((row) => (
                            <tr key={`best-${row.date}`} className="bt-row-click" onClick={() => focusOnOutlier(row)} title="Click to focus chart on this day">
                              <td>{row.date}</td><td>{row.trades}</td><td>{Number(row.pnl).toFixed(2)}</td>
                            </tr>
                          ))}
                          {!(analytics?.outliers?.best_10_days ?? []).length && <tr><td colSpan={3}>-</td></tr>}
                        </tbody>
                      </table>
                    </div>
                    <div>
                      <div className="bt-small-label">Worst 10 Days</div>
                      <table className="bt-mini-table">
                        <thead><tr><th>Date</th><th>Trades</th><th>PnL</th></tr></thead>
                        <tbody>
                          {(analytics?.outliers?.worst_10_days ?? []).map((row) => (
                            <tr key={`worst-${row.date}`} className="bt-row-click" onClick={() => focusOnOutlier(row)} title="Click to focus chart on this day">
                              <td>{row.date}</td><td>{row.trades}</td><td>{Number(row.pnl).toFixed(2)}</td>
                            </tr>
                          ))}
                          {!(analytics?.outliers?.worst_10_days ?? []).length && <tr><td colSpan={3}>-</td></tr>}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </section>
              </div>
            )}

            {!loading && !error && props.activeTab === 'compare' && (
              <div className="backtest-compare-block">
                <div className="bt-grid-2">
                  <label className="bt-inline"><span>Left Run</span>
                    <select className="control-fluid" value={compareLeftId} onChange={(e) => setCompareLeftId(e.target.value)}>
                      <option value="">Select run</option>
                      {runs.map((run) => <option key={run.id} value={run.id}>{run.name} ({run.strategy_id})</option>)}
                    </select>
                  </label>
                  <label className="bt-inline"><span>Right Run</span>
                    <select className="control-fluid" value={compareRightId} onChange={(e) => setCompareRightId(e.target.value)}>
                      <option value="">Select run</option>
                      {runs.map((run) => <option key={run.id} value={run.id}>{run.name} ({run.strategy_id})</option>)}
                    </select>
                  </label>
                  <div className="bt-button-row full">
                    <button
                      className="control-md"
                      type="button"
                      onClick={() => {
                        if (!compareLeftId || !compareRightId) return;
                        setJobStatus('Refreshing compare...');
                        void Promise.all([getBacktestRunAnalytics(compareLeftId), getBacktestRunAnalytics(compareRightId)])
                          .then(([l, r]) => {
                            setCompareLeftAnalytics(l);
                            setCompareRightAnalytics(r);
                            setJobStatus('');
                          })
                          .catch(() => {
                            setCompareLeftAnalytics(null);
                            setCompareRightAnalytics(null);
                            setJobStatus('Compare failed.');
                          });
                      }}
                      disabled={!compareLeftId || !compareRightId}
                    >
                      Refresh Compare
                    </button>
                  </div>
                </div>

                {(compareLeftAnalytics && compareRightAnalytics) && (
                  <table>
                    <thead><tr><th>Metric</th><th>Left</th><th>Right</th></tr></thead>
                    <tbody>
                      <tr><td>Net PnL</td><td>{String(compareLeftAnalytics.summary?.net_pnl ?? '-')}</td><td>{String(compareRightAnalytics.summary?.net_pnl ?? '-')}</td></tr>
                      <tr><td>Trades</td><td>{String(compareLeftAnalytics.summary?.trades ?? '-')}</td><td>{String(compareRightAnalytics.summary?.trades ?? '-')}</td></tr>
                      <tr><td>Win Rate</td><td>{String(compareLeftAnalytics.summary?.win_rate ?? '-')}</td><td>{String(compareRightAnalytics.summary?.win_rate ?? '-')}</td></tr>
                      <tr><td>Max DD</td><td>{String(compareLeftAnalytics.summary?.max_drawdown ?? '-')}</td><td>{String(compareRightAnalytics.summary?.max_drawdown ?? '-')}</td></tr>
                    </tbody>
                  </table>
                )}
              </div>
            )}

            {!loading && !error && props.activeTab === 'exports' && (
              <div className="backtest-exports-block">
                <a className="tv-button-link" href={selectedRunId ? `/backtests/runs/${selectedRunId}/export/config.json` : '#'} target="_blank" rel="noreferrer">Download Config JSON</a>
                <a className="tv-button-link" href={selectedRunId ? `/backtests/runs/${selectedRunId}/export/trades.csv` : '#'} target="_blank" rel="noreferrer">Download Trades CSV</a>
                <pre className="backtest-json">{JSON.stringify(config, null, 2)}</pre>
              </div>
            )}
          </section>
        </section>
      </div>
    </aside>
  );
}
