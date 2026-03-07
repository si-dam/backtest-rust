import { FormEvent, useEffect, useState } from 'react';
import { getBacktestRuns, queueBacktest, type BacktestRun } from '../lib/api';

export function BacktestsPage() {
  const [mode, setMode] = useState<'run' | 'sweep'>('run');
  const [name, setName] = useState('Scaffold Backtest');
  const [strategyId, setStrategyId] = useState('scaffold');
  const [paramsJson, setParamsJson] = useState('{}');
  const [status, setStatus] = useState('');
  const [runs, setRuns] = useState<BacktestRun[]>([]);
  const [loading, setLoading] = useState(false);

  async function refreshRuns() {
    setLoading(true);
    try {
      setRuns(await getBacktestRuns());
    } catch (err) {
      setStatus((err as Error).message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void refreshRuns();
  }, []);

  async function onSubmit(event: FormEvent) {
    event.preventDefault();
    let params: Record<string, unknown>;
    try {
      const parsed = JSON.parse(paramsJson || '{}');
      if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
        setStatus('Params must be a JSON object.');
        return;
      }
      params = parsed as Record<string, unknown>;
    } catch {
      setStatus('Params must be valid JSON.');
      return;
    }

    setLoading(true);
    try {
      const out = await queueBacktest({ mode, name: name || 'Scaffold Backtest', strategy_id: strategyId || 'scaffold', params });
      setStatus(`Queued backtest job ${out.job_id}.`);
      await refreshRuns();
    } catch (err) {
      setStatus((err as Error).message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <section>
      <h2>Backtest Runs</h2>
      <form className="grid-form" onSubmit={onSubmit}>
        <label>Mode
          <select className="control-md" value={mode} onChange={(e) => setMode(e.target.value as 'run' | 'sweep')}>
            <option value="run">Run</option>
            <option value="sweep">Sweep</option>
          </select>
        </label>
        <label>Run name
          <input className="control-lg" value={name} onChange={(e) => setName(e.target.value)} />
        </label>
        <label>Strategy id
          <input className="control-lg" value={strategyId} onChange={(e) => setStrategyId(e.target.value)} />
        </label>
        <label>Params (JSON object)
          <textarea className="control-fluid" value={paramsJson} onChange={(e) => setParamsJson(e.target.value)} rows={5} />
        </label>
        <div className="button-row">
          <button className="control-md" type="submit" disabled={loading}>Queue Backtest</button>
          <button className="control-md" type="button" onClick={() => void refreshRuns()} disabled={loading}>Refresh Runs</button>
        </div>
      </form>
      {status && <p className="status">{status}</p>}
      <table>
        <thead><tr><th>Run ID</th><th>Name</th><th>Strategy</th><th>Net PnL</th><th>Status</th><th>Created</th></tr></thead>
        <tbody>
          {runs.map((r) => (
            <tr key={r.id}><td>{r.id}</td><td>{r.name}</td><td>{r.strategy_id}</td><td>{r.metrics?.net_pnl ?? ''}</td><td>{r.status}</td><td>{r.created_at}</td></tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}
