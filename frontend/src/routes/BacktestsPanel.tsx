import { useEffect, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import {
  createBacktestJob,
  getBacktestRunAnalytics,
  getBacktestRuns,
  getBacktestRunTrades,
  getJob,
} from "../lib/api";

interface BacktestsPanelProps {
  selectedSymbol: string;
}

export default function BacktestsPanel({ selectedSymbol }: BacktestsPanelProps) {
  const [name, setName] = useState("ORB Breakout V1");
  const [lookbackDays, setLookbackDays] = useState("5");
  const [timeframe, setTimeframe] = useState("1m");
  const [ibMinutes, setIbMinutes] = useState("15");
  const [stopMode, setStopMode] = useState("or_boundary");
  const [entryMode, setEntryMode] = useState("first_outside");
  const [contracts, setContracts] = useState("1");
  const [splitEnabled, setSplitEnabled] = useState(false);
  const [splitAt, setSplitAt] = useState("");
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);

  const runsQuery = useQuery({
    queryKey: ["backtest-runs"],
    queryFn: getBacktestRuns,
    refetchInterval: 4000,
  });

  useEffect(() => {
    if (!selectedRunId && runsQuery.data?.runs.length) {
      setSelectedRunId(runsQuery.data.runs[0].id);
    }
  }, [runsQuery.data, selectedRunId]);

  const createJobMutation = useMutation({
    mutationFn: () => {
      const end = new Date();
      const start = new Date(end.getTime() - 1000 * 60 * 60 * 24 * Number(lookbackDays));
      const resolvedSplitAt =
        splitEnabled && splitAt
          ? new Date(splitAt)
          : new Date(start.getTime() + Math.floor((end.getTime() - start.getTime()) / 2));
      return createBacktestJob({
        name,
        strategy_id: "orb_breakout_v1",
        params: {
          symbol_contract: selectedSymbol,
          start: start.toISOString(),
          end: end.toISOString(),
          timeframe,
          ib_minutes: Number(ibMinutes),
          stop_mode: stopMode,
          tp_r_multiple: 2.0,
          entry_mode: entryMode,
          contracts: Number(contracts),
          rth_only: true,
          ...(splitEnabled
            ? {
                split: {
                  enabled: true,
                  split_at: resolvedSplitAt.toISOString(),
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

  return (
    <section className="panel-grid">
      <article className="panel control-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Backtests</p>
            <h2>Submit ORB breakout runs</h2>
          </div>
          <span className="pill">{selectedSymbol || "No symbol"}</span>
        </div>
        <div className="form-grid">
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
            <span className="field-label">Contracts</span>
            <input className="field-input" value={contracts} onChange={(event) => setContracts(event.target.value)} />
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
        </div>
        {splitEnabled ? (
          <p className="microcopy">Leave split time blank to split the selected window at its midpoint.</p>
        ) : null}
        <div className="action-row">
          <button
            className="primary-button"
            disabled={!selectedSymbol || createJobMutation.isPending}
            onClick={() => createJobMutation.mutate()}
            type="button"
          >
            {createJobMutation.isPending ? "Submitting…" : "Run backtest"}
          </button>
        </div>
        {createJobMutation.isError ? <p className="error-copy">{createJobMutation.error.message}</p> : null}
        {jobQuery.data ? <pre className="json-card compact-card">{JSON.stringify(jobQuery.data, null, 2)}</pre> : null}
      </article>

      <article className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Runs</p>
            <h2>Recent backtest runs</h2>
          </div>
          <span className="pill">{runsQuery.data?.runs.length ?? 0} runs</span>
        </div>
        {runsQuery.isLoading ? <p>Loading runs…</p> : null}
        {runsQuery.isError ? <p className="error-copy">{runsQuery.error.message}</p> : null}
        <div className="stack">
          {runsQuery.data?.runs.map((run) => (
            <button
              className={selectedRunId === run.id ? "nav-button active" : "nav-button"}
              key={run.id}
              onClick={() => setSelectedRunId(run.id)}
              type="button"
            >
              <strong>{run.name}</strong>
              <span className="microcopy">
                {run.strategy_id} • {run.status} • {new Date(run.updated_at).toLocaleString()}
              </span>
            </button>
          ))}
        </div>
      </article>

      <article className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Run detail</p>
            <h2>{selectedRunId ?? "Select a run"}</h2>
          </div>
          <span className="pill">{analyticsQuery.data?.analytics.trades ?? 0} trades</span>
        </div>
        {analyticsQuery.data ? (
          <div className="metric-grid">
            <div>
              <span className="metric-label">Total PnL</span>
              <strong>{analyticsQuery.data.analytics.total_pnl.toFixed(2)}</strong>
            </div>
            <div>
              <span className="metric-label">Avg PnL</span>
              <strong>{analyticsQuery.data.analytics.avg_pnl.toFixed(2)}</strong>
            </div>
            <div>
              <span className="metric-label">Wins</span>
              <strong>{analyticsQuery.data.analytics.wins}</strong>
            </div>
            <div>
              <span className="metric-label">Max DD</span>
              <strong>{analyticsQuery.data.analytics.max_drawdown.toFixed(2)}</strong>
            </div>
          </div>
        ) : null}
        <div className="stack">
          {tradesQuery.data?.trades.map((trade) => (
            <div className="job-card" key={trade.id}>
              <div className="profile-header">
                <strong>{trade.symbol_contract}</strong>
                <span>{trade.pnl?.toFixed(2) ?? "0.00"}</span>
              </div>
              <p className="microcopy">
                {trade.entry_ts ? new Date(trade.entry_ts).toLocaleString() : "n/a"} to{" "}
                {trade.exit_ts ? new Date(trade.exit_ts).toLocaleString() : "n/a"}
              </p>
              <pre className="json-card compact-card">{JSON.stringify(trade.notes_json, null, 2)}</pre>
            </div>
          ))}
        </div>
      </article>
    </section>
  );
}
