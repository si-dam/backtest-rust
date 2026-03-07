import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createMarketRebuildJobs, getJobs, replayJob } from "../lib/api";

interface JobsPanelProps {
  selectedSymbol: string;
}

function buildJobsParams(status: string, jobType: string) {
  const params = new URLSearchParams({ limit: "40" });
  if (status !== "all") {
    params.set("status", status);
  }
  if (jobType !== "all") {
    params.set("job_type", jobType);
  }
  return params;
}

export default function JobsPanel({ selectedSymbol }: JobsPanelProps) {
  const queryClient = useQueryClient();
  const [statusFilter, setStatusFilter] = useState("all");
  const [jobTypeFilter, setJobTypeFilter] = useState("all");
  const [rebuildTarget, setRebuildTarget] = useState<"bars" | "profiles" | "large_orders" | "all">("all");
  const [lookbackDays, setLookbackDays] = useState("5");
  const [largeOrdersThreshold, setLargeOrdersThreshold] = useState("25");
  const [profileTimezone, setProfileTimezone] = useState("America/New_York");

  const jobsQuery = useQuery({
    queryKey: ["jobs", statusFilter, jobTypeFilter],
    queryFn: () => getJobs(buildJobsParams(statusFilter, jobTypeFilter)),
    refetchInterval: 2500,
  });

  const replayMutation = useMutation({
    mutationFn: (jobId: string) => replayJob(jobId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["jobs"] });
    },
  });

  const rebuildMutation = useMutation({
    mutationFn: () => {
      const end = new Date();
      const start = new Date(end.getTime() - 1000 * 60 * 60 * 24 * Number(lookbackDays));
      return createMarketRebuildJobs(selectedSymbol, {
        start: start.toISOString(),
        end: end.toISOString(),
        large_orders_threshold: Number(largeOrdersThreshold),
        profile_timezone: profileTimezone,
        target: rebuildTarget,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["jobs"] });
    },
  });

  const replayableStatuses = useMemo(() => new Set(["failed", "dead_letter"]), []);

  return (
    <section className="panel-grid">
      <article className="panel control-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Operator actions</p>
            <h2>Jobs and rebuilds</h2>
          </div>
          <span className="pill">{jobsQuery.data?.jobs.length ?? 0} rows</span>
        </div>
        <div className="form-grid">
          <label className="field">
            <span className="field-label">Status</span>
            <select className="field-input" value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
              <option value="all">All</option>
              <option value="queued">Queued</option>
              <option value="running">Running</option>
              <option value="succeeded">Succeeded</option>
              <option value="failed">Failed</option>
              <option value="dead_letter">Dead letter</option>
            </select>
          </label>
          <label className="field">
            <span className="field-label">Job type</span>
            <select className="field-input" value={jobTypeFilter} onChange={(event) => setJobTypeFilter(event.target.value)}>
              <option value="all">All</option>
              <option value="ingestion">Ingestion</option>
              <option value="build_bars">Build bars</option>
              <option value="build_profiles">Build profiles</option>
              <option value="backtest_run">Backtest run</option>
              <option value="dataset_export">Dataset export</option>
            </select>
          </label>
        </div>
        <p className="microcopy">
          Failed or dead-letter jobs can be replayed. Running jobs are automatically reclaimable after lease expiry.
        </p>
      </article>

      <article className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Rebuild</p>
            <h2>{selectedSymbol || "No symbol selected"}</h2>
          </div>
          <span className="pill">{rebuildTarget}</span>
        </div>
        <div className="stack">
          <div className="form-grid">
            <label className="field">
              <span className="field-label">Target</span>
              <select
                className="field-input"
                value={rebuildTarget}
                onChange={(event) =>
                  setRebuildTarget(event.target.value as "bars" | "profiles" | "large_orders" | "all")
                }
              >
                <option value="all">Bars + profiles + large orders</option>
                <option value="bars">Bars only</option>
                <option value="profiles">Profiles only</option>
                <option value="large_orders">Large orders only</option>
              </select>
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
              <span className="field-label">Profile timezone</span>
              <select
                className="field-input"
                value={profileTimezone}
                onChange={(event) => setProfileTimezone(event.target.value)}
              >
                <option value="America/New_York">America/New_York</option>
                <option value="America/Chicago">America/Chicago</option>
                <option value="UTC">UTC</option>
              </select>
            </label>
            <label className="field">
              <span className="field-label">Large order threshold</span>
              <input
                className="field-input"
                value={largeOrdersThreshold}
                onChange={(event) => setLargeOrdersThreshold(event.target.value)}
              />
            </label>
          </div>
          <button
            className="primary-button"
            disabled={!selectedSymbol || rebuildMutation.isPending}
            onClick={() => rebuildMutation.mutate()}
            type="button"
          >
            {rebuildMutation.isPending ? "Queueing…" : "Queue rebuild jobs"}
          </button>
          {rebuildMutation.isError ? <p className="error-copy">{rebuildMutation.error.message}</p> : null}
          {rebuildMutation.data ? (
            <pre className="json-card">{JSON.stringify(rebuildMutation.data, null, 2)}</pre>
          ) : null}
        </div>
      </article>

      <article className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Jobs</p>
            <h2>Recent control-plane activity</h2>
          </div>
          <span className="pill">{statusFilter}</span>
        </div>
        {jobsQuery.isLoading ? <p>Loading jobs…</p> : null}
        {jobsQuery.isError ? <p className="error-copy">{jobsQuery.error.message}</p> : null}
        <div className="stack">
          {jobsQuery.data?.jobs.map((job) => (
            <div className="job-card" key={job.id}>
              <div className="profile-header">
                <div>
                  <strong>{job.job_type}</strong>
                  <p className="microcopy">
                    {job.id} • {new Date(job.updated_at).toLocaleString()}
                  </p>
                </div>
                <span className={`status-badge status-${job.status}`}>{job.status}</span>
              </div>
              <div className="metric-grid">
                <div>
                  <span className="metric-label">Attempt</span>
                  <strong>
                    {job.attempt} / {job.max_attempts}
                  </strong>
                </div>
                <div>
                  <span className="metric-label">Worker</span>
                  <strong>{job.locked_by ?? "n/a"}</strong>
                </div>
                <div>
                  <span className="metric-label">Stage</span>
                  <strong>{String(job.progress_json.stage ?? "n/a")}</strong>
                </div>
              </div>
              <div className="action-row">
                <button
                  className="secondary-button"
                  disabled={!replayableStatuses.has(job.status) || replayMutation.isPending}
                  onClick={() => replayMutation.mutate(job.id)}
                  type="button"
                >
                  Replay
                </button>
              </div>
              <pre className="json-card compact-card">{JSON.stringify(job.progress_json, null, 2)}</pre>
              {Object.keys(job.error_json).length ? (
                <pre className="json-card error-card compact-card">{JSON.stringify(job.error_json, null, 2)}</pre>
              ) : null}
            </div>
          ))}
        </div>
      </article>
    </section>
  );
}
