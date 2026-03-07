import { useEffect, useMemo, useState } from 'react';
import { getSymbols, type SymbolRow } from '../lib/api';
import { ChartPane } from '../components/ChartPane';
import { BacktestDock } from '../components/BacktestDock';

type LayoutMode = 'single' | 'horizontal' | 'vertical';

type PaneState = {
  symbol: string;
  timeframe: string;
  barType: 'time' | 'tick' | 'volume' | 'range';
  barSize: number;
  showVolume: boolean;
  showLarge: boolean;
  largeThreshold: number;
  showVwap: boolean;
  showOrb: boolean;
  startIso: string;
  endIso: string;
};

type BacktestTab = 'summary' | 'trades' | 'analytics' | 'compare' | 'exports';

const DEFAULT_PANE: PaneState = {
  symbol: '',
  timeframe: '1m',
  barType: 'time',
  barSize: 1500,
  showVolume: false,
  showLarge: false,
  largeThreshold: 25,
  showVwap: false,
  showOrb: false,
  startIso: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
  endIso: new Date().toISOString(),
};

function load<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

export function ChartWorkspacePage() {
  const [symbols, setSymbols] = useState<SymbolRow[]>([]);
  const [layout, setLayout] = useState<LayoutMode>(() => load<LayoutMode>('dash-react.chart.workspace.layout.v1', 'single'));
  const [activePane, setActivePane] = useState<'a' | 'b'>(() => load<'a' | 'b'>('dash-react.chart.workspace.active.v1', 'a'));
  const [paneA, setPaneA] = useState<PaneState>(() => load('dash-react.chart.pane.a.state.v1', DEFAULT_PANE));
  const [paneB, setPaneB] = useState<PaneState>(() => load('dash-react.chart.pane.b.state.v1', DEFAULT_PANE));
  const [backtestOpen, setBacktestOpen] = useState<boolean>(() => load<boolean>('dash-react.chart.backtest.open.v1', true));
  const [backtestWidth, setBacktestWidth] = useState<number>(() => load<number>('dash-react.chart.backtest.width.v1', 420));
  const [backtestTab, setBacktestTab] = useState<BacktestTab>(() => load<BacktestTab>('dash-react.chart.backtest.tab.v1', 'summary'));

  function alignRangeToSymbol(symbol: SymbolRow, previous: PaneState): PaneState {
    const startMs = Date.parse(symbol.start_ts);
    const endMs = Date.parse(symbol.end_ts);
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
      return previous;
    }
    const oneDayMs = 24 * 60 * 60 * 1000;
    const preferredStart = Math.max(startMs, endMs - oneDayMs);
    const prevStart = Date.parse(previous.startIso);
    const prevEnd = Date.parse(previous.endIso);
    const prevInRange = Number.isFinite(prevStart) && Number.isFinite(prevEnd) && prevStart < prevEnd && prevEnd <= endMs && prevStart >= startMs;
    if (prevInRange) return previous;
    return {
      ...previous,
      startIso: new Date(preferredStart).toISOString(),
      endIso: new Date(endMs).toISOString(),
    };
  }

  useEffect(() => {
    void getSymbols()
      .then((rows) => {
        const nextRows = rows.filter((r) => r.symbol_contract);
        setSymbols(nextRows);
        const fallback = nextRows[0];
        if (!fallback) return;

        setPaneA((prev) => {
          const activeSymbol = nextRows.find((r) => r.symbol_contract === prev.symbol) ?? fallback;
          const withSymbol = prev.symbol ? prev : { ...prev, symbol: activeSymbol.symbol_contract };
          return alignRangeToSymbol(activeSymbol, withSymbol);
        });

        setPaneB((prev) => {
          const activeSymbol = nextRows.find((r) => r.symbol_contract === prev.symbol) ?? fallback;
          const withSymbol = prev.symbol ? prev : { ...prev, symbol: activeSymbol.symbol_contract };
          return alignRangeToSymbol(activeSymbol, withSymbol);
        });
      })
      .catch(() => undefined);
  }, []);

  useEffect(() => localStorage.setItem('dash-react.chart.workspace.layout.v1', JSON.stringify(layout)), [layout]);
  useEffect(() => localStorage.setItem('dash-react.chart.workspace.active.v1', JSON.stringify(activePane)), [activePane]);
  useEffect(() => localStorage.setItem('dash-react.chart.pane.a.state.v1', JSON.stringify(paneA)), [paneA]);
  useEffect(() => localStorage.setItem('dash-react.chart.pane.b.state.v1', JSON.stringify(paneB)), [paneB]);
  useEffect(() => localStorage.setItem('dash-react.chart.backtest.open.v1', JSON.stringify(backtestOpen)), [backtestOpen]);
  useEffect(() => localStorage.setItem('dash-react.chart.backtest.width.v1', JSON.stringify(backtestWidth)), [backtestWidth]);
  useEffect(() => localStorage.setItem('dash-react.chart.backtest.tab.v1', JSON.stringify(backtestTab)), [backtestTab]);
  useEffect(() => {
    // Force chart canvases to recompute dimensions after dock transitions/resizes.
    window.requestAnimationFrame(() => window.dispatchEvent(new Event('resize')));
  }, [backtestOpen, backtestWidth, layout]);

  const state = activePane === 'a' ? paneA : paneB;
  const setState = activePane === 'a' ? setPaneA : setPaneB;

  const paneGridClass = useMemo(() => `pane-grid pane-grid-${layout}`, [layout]);

  return (
    <section className="workspace">
      <header className="workspace-topbar">
        <select className="control-md" value={activePane} onChange={(e) => setActivePane(e.target.value as 'a' | 'b')}>
          <option value="a">Pane A</option>
          <option value="b">Pane B</option>
        </select>
        <select className="control-md" value={layout} onChange={(e) => setLayout(e.target.value as LayoutMode)}>
          <option value="single">Single</option>
          <option value="horizontal">Side by side</option>
          <option value="vertical">Stacked</option>
        </select>
        <select className="control-lg" value={state.symbol} onChange={(e) => setState((s) => ({ ...s, symbol: e.target.value }))}>
          {symbols.map((s) => <option key={s.symbol_contract} value={s.symbol_contract}>{s.symbol_contract}</option>)}
        </select>
        <select className="control-md" value={state.barType} onChange={(e) => setState((s) => ({ ...s, barType: e.target.value as PaneState['barType'] }))}>
          <option value="time">Time</option>
          <option value="tick">Tick</option>
          <option value="volume">Volume</option>
          <option value="range">Range</option>
        </select>
        <input className="control-sm" type="number" min={1} value={state.barSize} onChange={(e) => setState((s) => ({ ...s, barSize: Number(e.target.value || 1) }))} />
        <select className="control-md" value={state.timeframe} onChange={(e) => setState((s) => ({ ...s, timeframe: e.target.value }))}>
          {['1m', '2m', '3m', '5m', '15m', '30m', '60m', '4h', '1d'].map((t) => <option key={t} value={t}>{t}</option>)}
        </select>
        <label><input type="checkbox" checked={state.showVolume} onChange={(e) => setState((s) => ({ ...s, showVolume: e.target.checked }))} /> VOL</label>
        <label><input type="checkbox" checked={state.showLarge} onChange={(e) => setState((s) => ({ ...s, showLarge: e.target.checked }))} /> BIG</label>
        <label><input type="checkbox" checked={state.showVwap} onChange={(e) => setState((s) => ({ ...s, showVwap: e.target.checked }))} /> VW</label>
        <label><input type="checkbox" checked={state.showOrb} onChange={(e) => setState((s) => ({ ...s, showOrb: e.target.checked }))} /> ORB</label>
        <label>Large
          <input className="control-sm" type="number" min={1} value={state.largeThreshold} onChange={(e) => setState((s) => ({ ...s, largeThreshold: Number(e.target.value || 1) }))} />
        </label>
        <button
          type="button"
          className={backtestOpen ? 'workspace-bt-toggle active' : 'workspace-bt-toggle'}
          onClick={() => setBacktestOpen((prev) => !prev)}
        >
          BT
        </button>
      </header>

      <div className="workspace-main-row">
        <div className="workspace-chart-column">
          <div className={paneGridClass}>
            <div onPointerDown={() => setActivePane('a')}>
              <ChartPane paneId="a" active={activePane === 'a'} {...paneA} />
            </div>
            {layout !== 'single' && (
              <div onPointerDown={() => setActivePane('b')}>
                <ChartPane paneId="b" active={activePane === 'b'} {...paneB} />
              </div>
            )}
          </div>
        </div>

        <BacktestDock
          paneId={activePane}
          open={backtestOpen}
          width={backtestWidth}
          activeTab={backtestTab}
          onClose={() => setBacktestOpen(false)}
          onWidthChange={setBacktestWidth}
          onTabChange={setBacktestTab}
          chartContext={{
            symbol: state.symbol,
            start: state.startIso,
            end: state.endIso,
            timeframe: state.timeframe,
          }}
          onChartContextChange={(patch) => {
            setState((s) => ({
              ...s,
              symbol: patch.symbol ?? s.symbol,
              timeframe: patch.timeframe ?? s.timeframe,
              startIso: patch.start ?? s.startIso,
              endIso: patch.end ?? s.endIso,
            }));
          }}
        />
      </div>
    </section>
  );
}
