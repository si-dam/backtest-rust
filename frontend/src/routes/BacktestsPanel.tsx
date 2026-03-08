import { useEffect, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import {
  getBacktestRunConfigExportUrl,
  createBacktestJob,
  getBacktestRun,
  getBacktestRunAnalytics,
  getBacktestRuns,
  getBacktestRunTradesExportUrl,
  getBacktestStrategies,
  getBacktestRunTrades,
  getJob,
  type BacktestRunRecord,
  type JobRecord,
} from "../lib/api";

interface BacktestsPanelProps {
  selectedSymbol: string;
}

export default function BacktestsPanel({ selectedSymbol }: BacktestsPanelProps) {
  const [mode, setMode] = useState<"run" | "sweep">("run");
  const [name, setName] = useState("ORB Breakout V1");
  const [lookbackDays, setLookbackDays] = useState("5");
  const [timeframe, setTimeframe] = useState("1m");
  const [ibMinutes, setIbMinutes] = useState("15");
  const [stopMode, setStopMode] = useState("or_boundary");
  const [entryMode, setEntryMode] = useState("first_outside");
  const [tpMultiple, setTpMultiple] = useState("2");
  const [contracts, setContracts] = useState("1");
  const [timezone, setTimezone] = useState("America/New_York");
  const [sessionStart, setSessionStart] = useState("09:30:00");
  const [sessionEnd, setSessionEnd] = useState("16:00:00");
  const [rthOnly, setRthOnly] = useState(true);
  const [splitEnabled, setSplitEnabled] = useState(false);
  const [splitAt, setSplitAt] = useState("");
  const [sweepSymbols, setSweepSymbols] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [segmentFilter, setSegmentFilter] = useState("all");
  const [runSearch, setRunSearch] = useState("");
  const [currentSymbolOnly, setCurrentSymbolOnly] = useState(true);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);

  const runsQuery = useQuery({
    queryKey: ["backtest-runs"],
    queryFn: getBacktestRuns,
    refetchInterval: 4000,
  });

  const strategiesQuery = useQuery({
    queryKey: ["backtest-strategies"],
    queryFn: getBacktestStrategies,
    staleTime: 60_000,
  });

  const filteredRuns = (runsQuery.data?.runs ?? []).filter((run) => {
    const params = run.params_json ?? {};
    const runSymbol = typeof params.symbol_contract === "string" ? params.symbol_contract : null;
    const segment = readRunSegment(run) ?? "none";
    const matchesStatus = statusFilter === "all" || run.status === statusFilter;
    const matchesSegment =
      segmentFilter === "all" || (segmentFilter === "none" ? segment === "none" : segment === segmentFilter);
    const matchesSymbol = !currentSymbolOnly || !selectedSymbol || runSymbol === selectedSymbol;
    const searchValue = runSearch.trim().toLowerCase();
    const matchesSearch =
      searchValue.length === 0 ||
      run.name.toLowerCase().includes(searchValue) ||
      run.strategy_id.toLowerCase().includes(searchValue) ||
      (runSymbol?.toLowerCase().includes(searchValue) ?? false);
    return matchesStatus && matchesSegment && matchesSymbol && matchesSearch;
  });

  useEffect(() => {
    if (!selectedRunId && filteredRuns.length > 0) {
      setSelectedRunId(filteredRuns[0].id);
    }
  }, [filteredRuns, selectedRunId]);

  useEffect(() => {
    if (selectedRunId && !filteredRuns.some((run) => run.id === selectedRunId)) {
      setSelectedRunId(filteredRuns[0]?.id ?? null);
    }
  }, [filteredRuns, selectedRunId]);

  const createJobMutation = useMutation({
    mutationFn: () => {
      const end = new Date();
      const start = new Date(end.getTime() - 1000 * 60 * 60 * 24 * Number(lookbackDays));
      const resolvedSplitAt =
        splitEnabled && splitAt
          ? new Date(splitAt)
          : new Date(start.getTime() + Math.floor((end.getTime() - start.getTime()) / 2));

      return createBacktestJob({
        mode,
        name,
        strategy_id: "orb_breakout_v1",
        params: {
          symbol_contract: selectedSymbol,
          start: start.toISOString(),
          end: end.toISOString(),
          timeframe,
          ib_minutes: Number(ibMinutes),
          stop_mode: stopMode,
          tp_r_multiple: Number(tpMultiple),
          entry_mode: entryMode,
          contracts: Number(contracts),
          timezone,
          session_start: sessionStart,
          session_end: sessionEnd,
          rth_only: rthOnly,
          ...(splitEnabled
            ? {
                split: {
                  enabled: true,
                  split_at: resolvedSplitAt.toISOString(),
                },
              }
            : {}),
          ...(mode === "sweep"
            ? {
                batch: {
                  symbols: parseSweepSymbols(sweepSymbols, selectedSymbol),
                },
              }
            : {}),
        },
      });
    },
    onSuccess: (response) => {
      setActiveJobId(response.job_id);
      runsQuery.refetch();
    },
  });

  const jobQuery = useQuery({
    queryKey: ["backtest-job", activeJobId],
    queryFn: () => getJob(activeJobId!),
    enabled: Boolean(activeJobId),
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      return status === "succeeded" || status === "failed" || status === "dead_letter" ? false : 2000;
    },
  });

  const runDetailQuery = useQuery({
    queryKey: ["backtest-run", selectedRunId],
    queryFn: () => getBacktestRun(selectedRunId!),
    enabled: Boolean(selectedRunId),
  });

  const tradesQuery = useQuery({
    queryKey: ["backtest-trades", selectedRunId],
    queryFn: () => getBacktestRunTrades(selectedRunId!),
    enabled: Boolean(selectedRunId),
  });

  const analyticsQuery = useQuery({
    queryKey: ["backtest-analytics", selectedRunId],
    queryFn: () => getBacktestRunAnalytics(selectedRunId!),
    enabled: Boolean(selectedRunId),
  });

  useEffect(() => {
    const runIds = readJobRunIds(jobQuery.data);
    if (runIds.length > 0) {
      setSelectedRunId(runIds[0]);
    }
  }, [jobQuery.data]);

  const selectedRun = runDetailQuery.data;
  const selectedParams = selectedRun?.params_json ?? {};
  const selectedMetrics = selectedRun?.metrics_json ?? {};
  const splitMeta = asRecord(selectedParams.split);
  const latestJobSummary = summarizeJobResult(jobQuery.data);
  const selectedSymbolFromRun =
    typeof selectedParams.symbol_contract === "string" ? selectedParams.symbol_contract : null;
  const activeStrategy = strategiesQuery.data?.find((strategy) => strategy.id === "orb_breakout_v1");

  function applySelectedRunParams() {
    if (!selectedRun) {
      return;
    }

    const params = selectedRun.params_json ?? {};
    setName(selectedRun.name.replace(/\s+\[(IS|OOS)\]$/i, ""));
    setTimeframe(typeof params.timeframe === "string" ? params.timeframe : "1m");
    setIbMinutes(String(params.ib_minutes ?? 15));
    setStopMode(typeof params.stop_mode === "string" ? params.stop_mode : "or_boundary");
    setEntryMode(typeof params.entry_mode === "string" ? params.entry_mode : "first_outside");
    setTpMultiple(String(params.tp_r_multiple ?? 2));
    setContracts(String(params.contracts ?? 1));
    setTimezone(typeof params.timezone === "string" ? params.timezone : "America/New_York");
    setSessionStart(typeof params.session_start === "string" ? params.session_start : "09:30:00");
    setSessionEnd(typeof params.session_end === "string" ? params.session_end : "16:00:00");
    setRthOnly(typeof params.rth_only === "boolean" ? params.rth_only : true);
    const batch = asRecord(params.batch);
    const batchSymbols = Array.isArray(batch?.symbols)
      ? batch.symbols.filter((value): value is string => typeof value === "string")
      : [];
    setMode(batchSymbols.length > 0 ? "sweep" : "run");
    setSweepSymbols(batchSymbols.join(", "));

    const split = asRecord(params.split);
    const enabled = split?.enabled === true;
    setSplitEnabled(enabled);
    setSplitAt(enabled ? toDatetimeLocalValue(split?.split_at) : "");
  }

  return (
    <section className="panel-grid">
      <article className="panel control-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Backtests</p>
            <h2>Submit ORB breakout jobs</h2>
          </div>
          <span className="pill">{selectedSymbol || "No symbol"}</span>
        </div>
        <div className="form-grid">
          <label className="field">
            <span className="field-label">Mode</span>
            <select className="field-input" value={mode} onChange={(event) => setMode(event.target.value as "run" | "sweep")}>
              <option value="run">Single run</option>
              <option value="sweep">Symbol sweep</option>
            </select>
          </label>
          <label className="field">
            <span className="field-label">Run name</span>
            <input className="field-input" value={name} onChange={(event) => setName(event.target.value)} />
          </label>
          <label className="field">
            <span className="field-label">Lookback</span>
            <select className="field-input" value={lookbackDays} onChange={(event) => setLookbackDays(event.target.value)}>
              <option value="1">1 day</option>
              <option value="5">5 days</option>
              <option value="10">10 days</option>
              <option value="20">20 days</option>
            </select>
          </label>
          <label className="field">
            <span className="field-label">Timeframe</span>
            <select className="field-input" value={timeframe} onChange={(event) => setTimeframe(event.target.value)}>
              <option value="1m">1m</option>
              <option value="3m">3m</option>
              <option value="5m">5m</option>
              <option value="15m">15m</option>
              <option value="30m">30m</option>
              <option value="60m">60m</option>
            </select>
          </label>
          <label className="field">
            <span className="field-label">IB minutes</span>
            <input className="field-input" value={ibMinutes} onChange={(event) => setIbMinutes(event.target.value)} />
          </label>
          <label className="field">
            <span className="field-label">Stop mode</span>
            <select className="field-input" value={stopMode} onChange={(event) => setStopMode(event.target.value)}>
              <option value="or_boundary">OR boundary</option>
              <option value="or_mid">OR mid</option>
            </select>
          </label>
          <label className="field">
            <span className="field-label">Entry mode</span>
            <select className="field-input" value={entryMode} onChange={(event) => setEntryMode(event.target.value)}>
              <option value="first_outside">First outside</option>
              <option value="reentry_after_stop">Reentry after stop</option>
            </select>
          </label>
          <label className="field">
            <span className="field-label">TP multiple</span>
            <input className="field-input" value={tpMultiple} onChange={(event) => setTpMultiple(event.target.value)} />
          </label>
          <label className="field">
            <span className="field-label">Contracts</span>
            <input className="field-input" value={contracts} onChange={(event) => setContracts(event.target.value)} />
          </label>
          <label className="field">
            <span className="field-label">Timezone</span>
            <select className="field-input" value={timezone} onChange={(event) => setTimezone(event.target.value)}>
              <option value="America/New_York">America/New_York</option>
              <option value="America/Chicago">America/Chicago</option>
              <option value="UTC">UTC</option>
            </select>
          </label>
          <label className="field">
            <span className="field-label">Session start</span>
            <input className="field-input" value={sessionStart} onChange={(event) => setSessionStart(event.target.value)} />
          </label>
          <label className="field">
            <span className="field-label">Session end</span>
            <input className="field-input" value={sessionEnd} onChange={(event) => setSessionEnd(event.target.value)} />
          </label>
          <label className="field checkbox-field">
            <input checked={rthOnly} type="checkbox" onChange={(event) => setRthOnly(event.target.checked)} />
            <span className="field-label">RTH only</span>
          </label>
          <label className="field">
            <span className="field-label">Split run</span>
            <select
              className="field-input"
              value={splitEnabled ? "enabled" : "disabled"}
              onChange={(event) => setSplitEnabled(event.target.value === "enabled")}
            >
              <option value="disabled">Disabled</option>
              <option value="enabled">In-sample / out-of-sample</option>
            </select>
          </label>
          {splitEnabled ? (
            <label className="field">
              <span className="field-label">Split at</span>
              <input
                className="field-input"
                type="datetime-local"
                value={splitAt}
                onChange={(event) => setSplitAt(event.target.value)}
              />
            </label>
          ) : null}
          {mode === "sweep" ? (
            <label className="field field-wide">
              <span className="field-label">Sweep symbols</span>
              <input
                className="field-input"
                placeholder={selectedSymbol ? `${selectedSymbol}, ESM6, RTYM6` : "NQM6, ESM6, RTYM6"}
                value={sweepSymbols}
                onChange={(event) => setSweepSymbols(event.target.value)}
              />
            </label>
          ) : null}
        </div>
        <div className="action-row">
          <button
            className="primary-button"
            disabled={!selectedSymbol || createJobMutation.isPending}
            onClick={() => createJobMutation.mutate()}
            type="button"
          >
            {createJobMutation.isPending ? "Submitting…" : mode === "sweep" ? "Run sweep" : "Run backtest"}
          </button>
        </div>
        {splitEnabled ? (
          <p className="microcopy">Leave split time blank to split the selected window at its midpoint.</p>
        ) : null}
        {mode === "sweep" ? (
          <p className="microcopy">Sweep uses the current date window for each symbol. Leave the field blank to sweep the selected symbol only.</p>
        ) : null}
        {activeStrategy ? <p className="microcopy">{activeStrategy.description}</p> : null}
        {createJobMutation.isError ? <p className="error-copy">{createJobMutation.error.message}</p> : null}
        {latestJobSummary ? (
          <div className="job-card">
            <div className="profile-header">
              <strong>Latest job result</strong>
              <span className={`status-badge status-${jobQuery.data?.status ?? "queued"}`}>{jobQuery.data?.status}</span>
            </div>
            <p className="microcopy">
              {latestJobSummary.createdRuns} run(s), {latestJobSummary.tradeCount} trade(s)
              {latestJobSummary.failedRuns ? `, ${latestJobSummary.failedRuns} failed` : ""}
              {latestJobSummary.splitGroupId ? `, split group ${latestJobSummary.splitGroupId}` : ""}
            </p>
            {latestJobSummary.runIds.length > 0 ? (
              <p className="microcopy">Created runs: {latestJobSummary.runIds.join(", ")}</p>
            ) : null}
          </div>
        ) : null}
      </article>

      <article className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Runs</p>
            <h2>Recent backtest runs</h2>
          </div>
          <span className="pill">{filteredRuns.length} visible</span>
        </div>
        <div className="form-grid">
          <label className="field">
            <span className="field-label">Status</span>
            <select className="field-input" value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
              <option value="all">All</option>
              <option value="completed">Completed</option>
              <option value="running">Running</option>
              <option value="failed">Failed</option>
            </select>
          </label>
          <label className="field">
            <span className="field-label">Segment</span>
            <select className="field-input" value={segmentFilter} onChange={(event) => setSegmentFilter(event.target.value)}>
              <option value="all">All</option>
              <option value="none">Unsplit</option>
              <option value="is">IS</option>
              <option value="oos">OOS</option>
            </select>
          </label>
          <label className="field">
            <span className="field-label">Search</span>
            <input className="field-input" value={runSearch} onChange={(event) => setRunSearch(event.target.value)} />
          </label>
          <label className="field checkbox-field">
            <input
              checked={currentSymbolOnly}
              type="checkbox"
              onChange={(event) => setCurrentSymbolOnly(event.target.checked)}
            />
            <span className="field-label">Current symbol only</span>
          </label>
        </div>
        {runsQuery.isLoading ? <p>Loading runs…</p> : null}
        {runsQuery.isError ? <p className="error-copy">{runsQuery.error.message}</p> : null}
        <div className="stack">
          {filteredRuns.map((run) => (
            <button
              className={selectedRunId === run.id ? "nav-button active" : "nav-button"}
              key={run.id}
              onClick={() => setSelectedRunId(run.id)}
              type="button"
            >
              <div className="run-row">
                <strong>{run.name}</strong>
                <span className={`status-badge status-${run.status}`}>{run.status}</span>
              </div>
              <span className="microcopy">
                {typeof run.params_json.symbol_contract === "string" ? run.params_json.symbol_contract : run.strategy_id}
                {readRunSegment(run) ? ` • ${readRunSegment(run)?.toUpperCase()}` : ""}
                {" • "}
                {new Date(run.updated_at).toLocaleString()}
              </span>
            </button>
          ))}
          {!runsQuery.isLoading && filteredRuns.length === 0 ? (
            <p className="microcopy">No runs match the current filters.</p>
          ) : null}
        </div>
      </article>

      <article className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Run detail</p>
            <h2>{selectedRun?.name ?? selectedRunId ?? "Select a run"}</h2>
          </div>
          <span className="pill">{analyticsQuery.data?.analytics.trades ?? 0} trades</span>
        </div>
        {selectedRun ? (
          <>
            <div className="inline-actions">
              <button className="secondary-button" onClick={applySelectedRunParams} type="button">
                Load selected run into form
              </button>
              <a
                className="secondary-button"
                href={getBacktestRunConfigExportUrl(selectedRun.id)}
                rel="noreferrer"
                target="_blank"
              >
                Config JSON
              </a>
              <a
                className="secondary-button"
                href={getBacktestRunTradesExportUrl(selectedRun.id)}
                rel="noreferrer"
                target="_blank"
              >
                Trades CSV
              </a>
              {selectedSymbolFromRun && selectedSymbolFromRun !== selectedSymbol ? (
                <span className="microcopy">Run symbol: {selectedSymbolFromRun}</span>
              ) : null}
            </div>
            <div className="detail-grid">
              <div className="detail-card">
                <span className="metric-label">Window</span>
                <strong>
                  {formatMaybeDate(selectedParams.start)} to {formatMaybeDate(selectedParams.end)}
                </strong>
                <p className="microcopy">
                  {String(selectedParams.timeframe ?? "1m")} bars • IB {String(selectedParams.ib_minutes ?? "15")}m
                </p>
              </div>
              <div className="detail-card">
                <span className="metric-label">Risk model</span>
                <strong>{String(selectedParams.stop_mode ?? "or_boundary")}</strong>
                <p className="microcopy">
                  TP {formatMaybeNumber(selectedParams.tp_r_multiple)}R • {String(selectedParams.contracts ?? 1)} contract(s)
                </p>
              </div>
              <div className="detail-card">
                <span className="metric-label">Session</span>
                <strong>{String(selectedParams.timezone ?? "UTC")}</strong>
                <p className="microcopy">
                  {String(selectedParams.session_start ?? "09:30:00")} to {String(selectedParams.session_end ?? "16:00:00")}
                  {" • "}
                  {selectedParams.rth_only === false ? "All sessions" : "RTH only"}
                </p>
              </div>
              {splitMeta ? (
                <div className="detail-card">
                  <span className="metric-label">Split</span>
                  <strong>{String(splitMeta.segment ?? "segment").toUpperCase()}</strong>
                  <p className="microcopy">
                    Split at {formatMaybeDate(splitMeta.split_at)}
                    {splitMeta.group_id ? ` • ${String(splitMeta.group_id)}` : ""}
                  </p>
                </div>
              ) : null}
            </div>
          </>
        ) : null}
        {analyticsQuery.data ? (
          <div className="metric-grid backtest-metrics-grid">
            <div>
              <span className="metric-label">Total PnL</span>
              <strong>{analyticsQuery.data.analytics.total_pnl.toFixed(2)}</strong>
            </div>
            <div>
              <span className="metric-label">Avg PnL</span>
              <strong>{analyticsQuery.data.analytics.avg_pnl.toFixed(2)}</strong>
            </div>
            <div>
              <span className="metric-label">Wins / losses</span>
              <strong>
                {analyticsQuery.data.analytics.wins} / {analyticsQuery.data.analytics.losses}
              </strong>
            </div>
            <div>
              <span className="metric-label">Win rate</span>
              <strong>{formatPercent(selectedMetrics.win_rate)}</strong>
            </div>
            <div>
              <span className="metric-label">Total R</span>
              <strong>{formatMaybeNumber(selectedMetrics.total_r)}</strong>
            </div>
            <div>
              <span className="metric-label">Max DD</span>
              <strong>{analyticsQuery.data.analytics.max_drawdown.toFixed(2)}</strong>
            </div>
          </div>
        ) : null}
        {selectedRun ? (
          <details className="detail-disclosure">
            <summary>Run metrics JSON</summary>
            <pre className="json-card compact-card">{JSON.stringify(selectedRun.metrics_json, null, 2)}</pre>
          </details>
        ) : null}
        <div className="table-card">
          <table className="trade-table">
            <thead>
              <tr>
                <th>Side</th>
                <th>Entry</th>
                <th>Exit</th>
                <th>Qty</th>
                <th>PnL</th>
                <th>Exit reason</th>
                <th>R</th>
              </tr>
            </thead>
            <tbody>
              {tradesQuery.data?.trades.map((trade) => {
                const notes = asRecord(trade.notes_json);
                return (
                  <tr key={trade.id}>
                    <td>{typeof notes?.side === "string" ? notes.side : "n/a"}</td>
                    <td>{trade.entry_ts ? new Date(trade.entry_ts).toLocaleString() : "n/a"}</td>
                    <td>{trade.exit_ts ? new Date(trade.exit_ts).toLocaleString() : "n/a"}</td>
                    <td>{trade.qty?.toFixed(0) ?? "n/a"}</td>
                    <td>{trade.pnl?.toFixed(2) ?? "0.00"}</td>
                    <td>{typeof notes?.exit_reason === "string" ? notes.exit_reason : "n/a"}</td>
                    <td>{typeof notes?.r_multiple === "number" ? notes.r_multiple.toFixed(2) : "n/a"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {!tradesQuery.data?.trades.length ? <p className="microcopy">No trades recorded for this run.</p> : null}
        </div>
      </article>
    </section>
  );
}

function summarizeJobResult(job?: JobRecord | null) {
  const result = asRecord(job?.result_json);
  if (!result) {
    return null;
  }

  return {
    createdRuns: numberOrFallback(result.created_runs, 0),
    failedRuns: numberOrFallback(result.failed_runs, 0),
    tradeCount: numberOrFallback(result.trade_count, 0),
    splitGroupId: typeof result.split_group_id === "string" ? result.split_group_id : null,
    runIds: readRunIds(result.run_ids),
  };
}

function readJobRunIds(job?: JobRecord | null) {
  return readRunIds(asRecord(job?.result_json)?.run_ids);
}

function readRunSegment(run: BacktestRunRecord) {
  const segment = asRecord(run.params_json.split)?.segment;
  return typeof segment === "string" ? segment : null;
}

function readRunIds(value: unknown) {
  if (!Array.isArray(value)) {
    return [];
  }

  return value.filter((entry): entry is string => typeof entry === "string");
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function numberOrFallback(value: unknown, fallback: number) {
  return typeof value === "number" ? value : fallback;
}

function formatMaybeDate(value: unknown) {
  if (typeof value !== "string") {
    return "n/a";
  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString();
}

function formatMaybeNumber(value: unknown) {
  return typeof value === "number" ? value.toFixed(2) : String(value ?? "n/a");
}

function formatPercent(value: unknown) {
  return typeof value === "number" ? `${(value * 100).toFixed(1)}%` : "n/a";
}

function toDatetimeLocalValue(value: unknown) {
  if (typeof value !== "string") {
    return "";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }

  const year = parsed.getFullYear();
  const month = String(parsed.getMonth() + 1).padStart(2, "0");
  const day = String(parsed.getDate()).padStart(2, "0");
  const hours = String(parsed.getHours()).padStart(2, "0");
  const minutes = String(parsed.getMinutes()).padStart(2, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}`;
}

function parseSweepSymbols(raw: string, selectedSymbol: string) {
  const values = raw
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
  if (values.length > 0) {
    return values;
  }
  return selectedSymbol ? [selectedSymbol] : [];
}
