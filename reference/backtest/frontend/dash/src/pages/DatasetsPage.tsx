import { FormEvent, useEffect, useState } from 'react';
import { getIngestJobs, getQueueHealth, getSymbols, getWatchFiles, queueIngest, type JobRow, type SymbolRow, type WatchFile } from '../lib/api';

export function DatasetsPage() {
  const [files, setFiles] = useState<WatchFile[]>([]);
  const [symbols, setSymbols] = useState<SymbolRow[]>([]);
  const [jobs, setJobs] = useState<JobRow[]>([]);
  const [fileName, setFileName] = useState('');
  const [symbolContract, setSymbolContract] = useState('');
  const [rebuild, setRebuild] = useState(false);
  const [status, setStatus] = useState('');
  const [loading, setLoading] = useState(false);

  async function refresh() {
    setLoading(true);
    try {
      const [nextFiles, nextSymbols, nextJobs, queueHealth] = await Promise.all([
        getWatchFiles(),
        getSymbols(),
        getIngestJobs(),
        getQueueHealth().catch(() => null),
      ]);
      setFiles(nextFiles);
      setSymbols(nextSymbols);
      setJobs(nextJobs);
      if (!fileName && nextFiles.length > 0) {
        setFileName(nextFiles[0].name);
      }
      if (queueHealth && queueHealth.queued_count > 0 && queueHealth.active_worker_count === 0) {
        setStatus(`Warning: ${queueHealth.queued_count} queued ingest job(s) with no active worker for queue '${queueHealth.queue_name}'.`);
      }
    } catch (err) {
      setStatus((err as Error).message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void refresh();
  }, []);

  async function onSubmit(event: FormEvent) {
    event.preventDefault();
    if (!fileName) {
      setStatus('Choose a market data file before queueing ingest.');
      return;
    }
    setLoading(true);
    try {
      const queued = await queueIngest(fileName, symbolContract.trim() || null, rebuild);
      setStatus(`Queued ${queued.count} ingest job(s).`);
      await refresh();
    } catch (err) {
      setStatus((err as Error).message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <section>
      <h2>Datasets & Ingest</h2>
      <p className="hint">Watch directory: ./data/watch (supports .csv, .txt)</p>
      <form className="grid-form" onSubmit={onSubmit}>
        <label>
          Market data file
          <select className="control-xl" value={fileName} onChange={(e) => setFileName(e.target.value)}>
            {files.map((f) => (
              <option key={f.name} value={f.name}>{f.name} ({f.size_bytes} bytes)</option>
            ))}
          </select>
        </label>
        <label>
          Symbol contract override
          <input className="control-lg" value={symbolContract} onChange={(e) => setSymbolContract(e.target.value)} placeholder="e.g., NQH6" />
        </label>
        <label className="checkbox-row">
          <input type="checkbox" checked={rebuild} onChange={(e) => setRebuild(e.target.checked)} /> Rebuild existing source records
        </label>
        <div className="button-row">
          <button className="control-md" type="submit" disabled={loading}>Queue Ingest</button>
          <button className="control-sm" type="button" onClick={() => void refresh()} disabled={loading}>Refresh</button>
        </div>
      </form>
      {status && <p className="status">{status}</p>}
      <h3>Symbols</h3>
      <table>
        <thead><tr><th>Symbol</th><th>Start</th><th>End</th><th>Ticks</th></tr></thead>
        <tbody>
          {symbols.map((r) => <tr key={r.symbol_contract}><td>{r.symbol_contract}</td><td>{r.start_ts}</td><td>{r.end_ts}</td><td>{r.tick_count}</td></tr>)}
        </tbody>
      </table>
      <h3>Recent Ingest Jobs</h3>
      <table>
        <thead><tr><th>Job</th><th>Status</th><th>File</th><th>Created</th></tr></thead>
        <tbody>
          {jobs.map((j) => <tr key={j.id}><td>{j.id}</td><td>{j.status}</td><td>{j.payload?.file_path ?? ''}</td><td>{j.created_at}</td></tr>)}
        </tbody>
      </table>
    </section>
  );
}
