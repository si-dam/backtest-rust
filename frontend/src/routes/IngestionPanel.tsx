import { useEffect, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { createIngestionJob, getJob } from "../lib/api";

interface IngestionPanelProps {
  selectedSymbol: string;
}

export default function IngestionPanel({ selectedSymbol }: IngestionPanelProps) {
  const [filePath, setFilePath] = useState("");
  const [symbolOverride, setSymbolOverride] = useState(selectedSymbol);
  const [rebuild, setRebuild] = useState(false);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);

  useEffect(() => {
    if (!symbolOverride && selectedSymbol) {
      setSymbolOverride(selectedSymbol);
    }
  }, [selectedSymbol, symbolOverride]);

  const createJobMutation = useMutation({
    mutationFn: createIngestionJob,
    onSuccess: (response) => {
      setActiveJobId(response.job_id);
    },
  });

  const jobQuery = useQuery({
    queryKey: ["job", activeJobId],
    queryFn: () => getJob(activeJobId!),
    enabled: Boolean(activeJobId),
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      return status === "succeeded" || status === "failed" || status === "dead_letter" ? false : 2000;
    },
  });

  return (
    <section className="panel-grid">
      <article className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Ingestion</p>
            <h2>Queue a server-local market data file</h2>
          </div>
          <span className="pill">{activeJobId ? "queued" : "idle"}</span>
        </div>
        <form
          className="stack"
          onSubmit={(event) => {
            event.preventDefault();
            createJobMutation.mutate({
              file_path: filePath,
              symbol_contract: symbolOverride || undefined,
              rebuild,
            });
          }}
        >
          <label className="field">
            <span className="field-label">File path</span>
            <input
              className="field-input"
              placeholder="data/NQH6_ticks.csv"
              required
              value={filePath}
              onChange={(event) => setFilePath(event.target.value)}
            />
          </label>

          <label className="field">
            <span className="field-label">Symbol override</span>
            <input
              className="field-input"
              placeholder="Optional if file already contains the contract"
              value={symbolOverride}
              onChange={(event) => setSymbolOverride(event.target.value)}
            />
          </label>

          <label className="field checkbox-field">
            <input checked={rebuild} type="checkbox" onChange={(event) => setRebuild(event.target.checked)} />
            <span className="field-label">Rebuild derived artifacts after ingest</span>
          </label>

          <button className="primary-button" disabled={createJobMutation.isPending} type="submit">
            {createJobMutation.isPending ? "Submitting…" : "Submit ingestion job"}
          </button>
        </form>

        {createJobMutation.isError ? (
          <p className="error-copy">{createJobMutation.error.message}</p>
        ) : null}
        <p className="microcopy">
          Phase 1 keeps ingest semantics server-local. The path must remain inside the configured ingest root.
        </p>
      </article>

      <article className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Job status</p>
            <h2>{activeJobId ?? "No job yet"}</h2>
          </div>
          <span className="pill">{jobQuery.data?.status ?? "idle"}</span>
        </div>

        {!activeJobId ? <p>Submit an ingest job to start polling `/api/v1/jobs/:job_id`.</p> : null}
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
              <pre className="json-card error-card">{JSON.stringify(jobQuery.data.error_json, null, 2)}</pre>
            ) : null}
          </div>
        ) : null}
      </article>
    </section>
  );
}
