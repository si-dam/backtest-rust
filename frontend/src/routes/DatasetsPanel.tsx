import { useEffect, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { createDatasetJob, getBacktestRuns, getDatasetExports, getJob } from "../lib/api";

interface DatasetsPanelProps {
  selectedSymbol: string;
}

type ExportKind = "bars" | "ticks" | "backtest_trades";

export default function DatasetsPanel({ selectedSymbol }: DatasetsPanelProps) {
  const [exportKind, setExportKind] = useState<ExportKind>("bars");
  const [timeframe, setTimeframe] = useState("1m");
  const [barType, setBarType] = useState("time");
  const [barSize, setBarSize] = useState("1500");
  const [lookbackDays, setLookbackDays] = useState("5");
  const [selectedRunId, setSelectedRunId] = useState("");
  const [activeJobId, setActiveJobId] = useState<string | null>(null);

  const runsQuery = useQuery({
    queryKey: ["dataset-backtest-runs"],
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
      if (exportKind === "backtest_trades") {
        return createDatasetJob({
          export_kind: "backtest_trades",
          payload: {
            run_id: selectedRunId,
          },
        });
      }

      const end = new Date();
      const start = new Date(
        end.getTime() - 1000 * 60 * 60 * 24 * Number(lookbackDays || "5"),
      );
      return createDatasetJob({
        export_kind: exportKind,
        payload: {
          symbol_contract: selectedSymbol,
          start: start.toISOString(),
          end: end.toISOString(),
          ...(exportKind === "bars"
            ? {
                timeframe,
                bar_type: barType,
                bar_size:
                  barType === "time" ? undefined : Number(barSize || "0"),
              }
            : {}),
        },
      });
    },
    onSuccess: (response) => {
      setActiveJobId(response.job_id);
      exportsQuery.refetch();
    },
  });

  const exportsQuery = useQuery({
    queryKey: ["dataset-exports", exportKind],
    queryFn: () => {
      const params = new URLSearchParams();
      params.set("limit", "12");
      params.set("export_kind", exportKind);
      return getDatasetExports(params);
    },
    refetchInterval: 4000,
  });

  const jobQuery = useQuery({
    queryKey: ["dataset-job", activeJobId],
    queryFn: () => getJob(activeJobId!),
    enabled: Boolean(activeJobId),
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      return status === "succeeded" || status === "failed" || status === "dead_letter"
        ? false
        : 2000;
    },
  });

  return (
    <section className="panel-grid">
      <article className="panel control-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Datasets</p>
            <h2>Queue dataset export jobs</h2>
          </div>
          <span className="pill">{activeJobId ? "queued" : "idle"}</span>
        </div>
        <div className="form-grid">
          <label className="field">
            <span className="field-label">Export kind</span>
            <select
              className="field-input"
              value={exportKind}
              onChange={(event) => setExportKind(event.target.value as ExportKind)}
            >
              <option value="bars">Bars</option>
              <option value="ticks">Ticks</option>
              <option value="backtest_trades">Backtest trades</option>
            </select>
          </label>
          {exportKind !== "backtest_trades" ? (
            <>
              <label className="field">
                <span className="field-label">Symbol</span>
                <input className="field-input" value={selectedSymbol} readOnly />
              </label>
              <label className="field">
                <span className="field-label">Lookback</span>
                <select
                  className="field-input"
                  value={lookbackDays}
                  onChange={(event) => setLookbackDays(event.target.value)}
                >
                  <option value="1">1 day</option>
                  <option value="5">5 days</option>
                  <option value="10">10 days</option>
                  <option value="20">20 days</option>
                </select>
              </label>
            </>
          ) : null}
          {exportKind === "bars" ? (
            <>
              <label className="field">
                <span className="field-label">Timeframe</span>
                <select
                  className="field-input"
                  value={timeframe}
                  onChange={(event) => setTimeframe(event.target.value)}
                >
                  <option value="1m">1m</option>
                  <option value="3m">3m</option>
                  <option value="5m">5m</option>
                  <option value="15m">15m</option>
                </select>
              </label>
              <label className="field">
                <span className="field-label">Bar type</span>
                <select
                  className="field-input"
                  value={barType}
                  onChange={(event) => setBarType(event.target.value)}
                >
                  <option value="time">Time</option>
                  <option value="tick">Tick</option>
                  <option value="volume">Volume</option>
                  <option value="range">Range</option>
                </select>
              </label>
              {barType !== "time" ? (
                <label className="field">
                  <span className="field-label">Bar size</span>
                  <input
                    className="field-input"
                    value={barSize}
                    onChange={(event) => setBarSize(event.target.value)}
                  />
                </label>
              ) : null}
            </>
          ) : null}
          {exportKind === "backtest_trades" ? (
            <label className="field">
              <span className="field-label">Backtest run</span>
              <select
                className="field-input"
                value={selectedRunId}
                onChange={(event) => setSelectedRunId(event.target.value)}
              >
                {!runsQuery.data?.runs.length ? <option value="">No runs yet</option> : null}
                {runsQuery.data?.runs.map((run) => (
                  <option key={run.id} value={run.id}>
                    {run.name} ({run.status})
                  </option>
                ))}
              </select>
            </label>
          ) : null}
        </div>
        <div className="action-row">
          <button
            className="primary-button"
            disabled={
              createJobMutation.isPending ||
              (exportKind !== "backtest_trades" && !selectedSymbol) ||
              (exportKind === "backtest_trades" && !selectedRunId)
            }
            onClick={() => createJobMutation.mutate()}
            type="button"
          >
            {createJobMutation.isPending ? "Submitting…" : "Queue export job"}
          </button>
        </div>
        {createJobMutation.isError ? (
          <p className="error-copy">{createJobMutation.error.message}</p>
        ) : null}
        <p className="microcopy">
          Export jobs write Parquet plus a manifest under the configured artifact root. Use bars or
          ticks for market datasets, or backtest trades for run output snapshots.
        </p>
      </article>

      <article className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Export job</p>
            <h2>{activeJobId ?? "No job yet"}</h2>
          </div>
          <span className="pill">{jobQuery.data?.status ?? "idle"}</span>
        </div>

        {!activeJobId ? <p>Queue a dataset export to start polling `/api/v1/jobs/:job_id`.</p> : null}
        {jobQuery.isLoading ? <p>Loading job state…</p> : null}
        {jobQuery.isError ? <p className="error-copy">{jobQuery.error.message}</p> : null}
        {jobQuery.data ? (
          <div className="stack">
            <div className="list-row">
              <strong>Type</strong>
              <span>{jobQuery.data.job_type}</span>
            </div>
            <div className="list-row">
              <strong>Attempt</strong>
              <span>
                {jobQuery.data.attempt} / {jobQuery.data.max_attempts}
              </span>
            </div>
            <div className="list-row">
              <strong>Updated</strong>
              <span>{new Date(jobQuery.data.updated_at).toLocaleString()}</span>
            </div>
            <pre className="json-card">{JSON.stringify(jobQuery.data.progress_json, null, 2)}</pre>
            {Object.keys(jobQuery.data.result_json).length ? (
              <pre className="json-card">{JSON.stringify(jobQuery.data.result_json, null, 2)}</pre>
            ) : null}
            {Object.keys(jobQuery.data.error_json).length ? (
              <pre className="json-card error-card">
                {JSON.stringify(jobQuery.data.error_json, null, 2)}
              </pre>
            ) : null}
          </div>
        ) : null}
      </article>

      <article className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Recent exports</p>
            <h2>{exportKind}</h2>
          </div>
          <span className="pill">{exportsQuery.data?.exports.length ?? 0} items</span>
        </div>
        {exportsQuery.isLoading ? <p>Loading exports…</p> : null}
        {exportsQuery.isError ? <p className="error-copy">{exportsQuery.error.message}</p> : null}
        <div className="stack">
          {exportsQuery.data?.exports.map((datasetExport) => (
            <div className="job-card" key={datasetExport.id}>
              <div className="profile-header">
                <strong>{datasetExport.export_kind}</strong>
                <span>{new Date(datasetExport.created_at).toLocaleString()}</span>
              </div>
              <p className="microcopy">{datasetExport.schema_version}</p>
              <p className="microcopy">{datasetExport.manifest_path}</p>
              <pre className="json-card compact-card">
                {JSON.stringify(datasetExport.payload_json, null, 2)}
              </pre>
            </div>
          ))}
        </div>
      </article>
    </section>
  );
}
