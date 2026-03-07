import { useEffect, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { createDatasetJob, getBacktestRuns, getDatasetExports, getJob } from "../lib/api";

interface DatasetsPanelProps {
  selectedSymbol: string;
}

type ExportKind = "bars" | "ticks" | "backtest_trades";
type ProfilePreset = "day" | "week" | "rth" | "eth";
type ProfileMetric = "volume" | "delta";

export default function DatasetsPanel({ selectedSymbol }: DatasetsPanelProps) {
  const [exportKind, setExportKind] = useState<ExportKind | "preset_profiles">("bars");
  const [timeframe, setTimeframe] = useState("1m");
  const [barType, setBarType] = useState("time");
  const [barSize, setBarSize] = useState("1500");
  const [lookbackDays, setLookbackDays] = useState("5");
  const [selectedRunId, setSelectedRunId] = useState("");
  const [preset, setPreset] = useState<ProfilePreset>("day");
  const [timezone, setTimezone] = useState("America/New_York");
  const [metric, setMetric] = useState<ProfileMetric>("volume");
  const [tickAggregation, setTickAggregation] = useState("1");
  const [valueAreaEnabled, setValueAreaEnabled] = useState(true);
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
                bar_size: barType === "time" ? undefined : Number(barSize || "0"),
              }
            : {}),
          ...(exportKind === "preset_profiles"
            ? {
                preset,
                timezone,
                metric,
                tick_aggregation: Number(tickAggregation || "1"),
                value_area_enabled: valueAreaEnabled,
                value_area_percent: 70,
                max_segments: 12,
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

  const latestExportQuery = useQuery({
    queryKey: ["dataset-export-by-job", exportKind, activeJobId],
    queryFn: () => {
      const params = new URLSearchParams();
      params.set("limit", "1");
      params.set("export_kind", exportKind);
      params.set("job_id", activeJobId!);
      return getDatasetExports(params);
    },
    enabled: Boolean(activeJobId),
    refetchInterval: (query) => {
      const hasExport = (query.state.data?.exports.length ?? 0) > 0;
      return hasExport ? false : 2000;
    },
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
              <option value="preset_profiles">Preset profiles</option>
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
          {exportKind === "preset_profiles" ? (
            <>
              <label className="field">
                <span className="field-label">Preset</span>
                <select
                  className="field-input"
                  value={preset}
                  onChange={(event) => setPreset(event.target.value as ProfilePreset)}
                >
                  <option value="day">Day</option>
                  <option value="week">Week</option>
                  <option value="rth">RTH</option>
                  <option value="eth">ETH</option>
                </select>
              </label>
              <label className="field">
                <span className="field-label">Metric</span>
                <select
                  className="field-input"
                  value={metric}
                  onChange={(event) => setMetric(event.target.value as ProfileMetric)}
                >
                  <option value="volume">Volume</option>
                  <option value="delta">Delta</option>
                </select>
              </label>
              <label className="field">
                <span className="field-label">Aggregation</span>
                <select
                  className="field-input"
                  value={tickAggregation}
                  onChange={(event) => setTickAggregation(event.target.value)}
                >
                  <option value="1">1x</option>
                  <option value="2">2x</option>
                  <option value="4">4x</option>
                  <option value="8">8x</option>
                </select>
              </label>
              <label className="field">
                <span className="field-label">Timezone</span>
                <select
                  className="field-input"
                  value={timezone}
                  onChange={(event) => setTimezone(event.target.value)}
                >
                  <option value="America/New_York">America/New_York</option>
                  <option value="America/Chicago">America/Chicago</option>
                  <option value="UTC">UTC</option>
                </select>
              </label>
              <label className="field checkbox-field">
                <input
                  checked={valueAreaEnabled}
                  type="checkbox"
                  onChange={(event) => setValueAreaEnabled(event.target.checked)}
                />
                <span className="field-label">Value area</span>
              </label>
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
          Export jobs write Parquet plus a manifest under the configured artifact root. Use bars,
          ticks, preset profiles, or backtest trades depending on the dataset you want to hand off.
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

      <article className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Persisted export</p>
            <h2>{activeJobId ?? "No job yet"}</h2>
          </div>
          <span className="pill">
            {latestExportQuery.data?.exports.length ? "recorded" : activeJobId ? "waiting" : "idle"}
          </span>
        </div>
        {!activeJobId ? <p>Queue an export job to look up its persisted export record.</p> : null}
        {latestExportQuery.isLoading ? <p>Checking for recorded export…</p> : null}
        {latestExportQuery.isError ? <p className="error-copy">{latestExportQuery.error.message}</p> : null}
        {latestExportQuery.data?.exports[0] ? (
          <div className="job-card">
            <div className="profile-header">
              <strong>{latestExportQuery.data.exports[0].export_kind}</strong>
              <span>{latestExportQuery.data.exports[0].schema_version}</span>
            </div>
            <p className="microcopy">{summarizeExport(latestExportQuery.data.exports[0].payload_json)}</p>
            <p className="microcopy">{latestExportQuery.data.exports[0].manifest_path}</p>
            <pre className="json-card compact-card">
              {JSON.stringify(latestExportQuery.data.exports[0].payload_json, null, 2)}
            </pre>
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
              <p className="microcopy">
                {datasetExport.schema_version}
                {datasetExport.job_id ? ` • job ${datasetExport.job_id}` : ""}
                {datasetExport.job_id === activeJobId ? " • latest job" : ""}
              </p>
              <p className="microcopy">{summarizeExport(datasetExport.payload_json)}</p>
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

function summarizeExport(payload: Record<string, unknown>) {
  const rowCount = typeof payload.row_count === "number" ? payload.row_count : null;
  const request = asRecord(payload.request);
  const run = asRecord(payload.run);

  if (run) {
    return [
      rowCount !== null ? `${rowCount} rows` : null,
      typeof run.name === "string" ? run.name : null,
      typeof run.strategy_id === "string" ? run.strategy_id : null,
    ]
      .filter(Boolean)
      .join(" • ");
  }

  if (request) {
    return [
      rowCount !== null ? `${rowCount} rows` : null,
      typeof request.symbol_contract === "string" ? request.symbol_contract : null,
      typeof request.preset === "string" ? request.preset : null,
      typeof request.timeframe === "string" ? request.timeframe : null,
      typeof request.bar_type === "string" ? request.bar_type : null,
      typeof request.metric === "string" ? request.metric : null,
    ]
      .filter(Boolean)
      .join(" • ");
  }

  return rowCount !== null ? `${rowCount} rows` : "Export manifest";
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}
