import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import ChartCard from "../components/ChartCard";
import { getBars, getLargeOrders } from "../lib/api";

function buildBarsParams(lookbackHours: number, barType: string, timeframe: string, barSize: string) {
  const end = new Date();
  const start = new Date(end.getTime() - 1000 * 60 * 60 * lookbackHours);
  const params = new URLSearchParams({
    timeframe,
    start: start.toISOString(),
    end: end.toISOString(),
    bar_type: barType,
  });

  if (barType !== "time") {
    params.set("bar_size", barSize);
  }

  return params;
}

interface MarketOverviewProps {
  selectedSymbol: string;
}

function buildLargeOrdersParams(lookbackHours: number, fixedThreshold: string) {
  const end = new Date();
  const start = new Date(end.getTime() - 1000 * 60 * 60 * lookbackHours);
  return new URLSearchParams({
    start: start.toISOString(),
    end: end.toISOString(),
    method: "fixed",
    fixed_threshold: fixedThreshold,
  });
}

export default function MarketOverview({ selectedSymbol }: MarketOverviewProps) {
  const [lookbackHours, setLookbackHours] = useState("24");
  const [barType, setBarType] = useState("time");
  const [timeframe, setTimeframe] = useState("1m");
  const [barSize, setBarSize] = useState("1500");
  const [largeOrderThreshold, setLargeOrderThreshold] = useState("25");

  const barsQuery = useQuery({
    queryKey: ["bars", selectedSymbol, lookbackHours, barType, timeframe, barSize],
    queryFn: () =>
      getBars(
        selectedSymbol,
        buildBarsParams(Number(lookbackHours), barType, timeframe, barSize),
      ),
    enabled: Boolean(selectedSymbol),
  });

  const largeOrdersQuery = useQuery({
    queryKey: ["large-orders", selectedSymbol, lookbackHours, largeOrderThreshold],
    queryFn: () =>
      getLargeOrders(
        selectedSymbol,
        buildLargeOrdersParams(Number(lookbackHours), largeOrderThreshold),
      ),
    enabled: Boolean(selectedSymbol),
  });

  return (
    <section className="panel-grid">
      <article className="panel control-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Query controls</p>
            <h2>{selectedSymbol || "No symbol selected"}</h2>
          </div>
          <span className="pill">{barsQuery.data?.bars.length ?? 0} rows</span>
        </div>
        <div className="form-grid">
          <label className="field">
            <span className="field-label">Lookback</span>
            <select
              className="field-input"
              value={lookbackHours}
              onChange={(event) => setLookbackHours(event.target.value)}
            >
              <option value="6">6 hours</option>
              <option value="24">24 hours</option>
              <option value="72">3 days</option>
              <option value="168">7 days</option>
            </select>
          </label>

          <label className="field">
            <span className="field-label">Bar type</span>
            <select className="field-input" value={barType} onChange={(event) => setBarType(event.target.value)}>
              <option value="time">Time</option>
              <option value="tick">Tick</option>
              <option value="volume">Volume</option>
              <option value="range">Range</option>
            </select>
          </label>

          {barType === "time" ? (
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
          ) : (
            <label className="field">
              <span className="field-label">Bar size</span>
              <select className="field-input" value={barSize} onChange={(event) => setBarSize(event.target.value)}>
                {barType === "tick" ? (
                  <>
                    <option value="1500">1500</option>
                    <option value="3000">3000</option>
                    <option value="5000">5000</option>
                  </>
                ) : null}
                {barType === "volume" ? (
                  <>
                    <option value="500">500</option>
                    <option value="1000">1000</option>
                    <option value="5000">5000</option>
                  </>
                ) : null}
                {barType === "range" ? (
                  <>
                    <option value="20">20 ticks</option>
                    <option value="40">40 ticks</option>
                    <option value="80">80 ticks</option>
                  </>
                ) : null}
              </select>
            </label>
          )}

          <label className="field">
            <span className="field-label">Large order threshold</span>
            <input
              className="field-input"
              value={largeOrderThreshold}
              onChange={(event) => setLargeOrderThreshold(event.target.value)}
            />
          </label>
        </div>
        <p className="microcopy">
          Bars and fixed-threshold large orders are loaded from persisted ClickHouse tables, not built on the request path.
        </p>
      </article>

      <article className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Status</p>
            <h2>Query health</h2>
          </div>
          <span className="pill">{selectedSymbol || "none"}</span>
        </div>
        <div className="stack">
          <div className="list-row">
            <strong>State</strong>
            <span>{barsQuery.isLoading ? "Loading" : barsQuery.isError ? "Error" : "Ready"}</span>
          </div>
          <div className="list-row">
            <strong>Window</strong>
            <span>{lookbackHours}h</span>
          </div>
          <div className="list-row">
            <strong>Series</strong>
            <span>{barType === "time" ? timeframe : `${barType}:${barSize}`}</span>
          </div>
          <div className="list-row">
            <strong>Large orders</strong>
            <span>{largeOrdersQuery.data?.large_orders.length ?? 0} rows</span>
          </div>
        </div>
      </article>

      <article className="panel panel-wide">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Bars</p>
            <h2>{selectedSymbol || "No symbol selected"} recent candles</h2>
          </div>
          <span className="pill">{barType === "time" ? timeframe : `${barType}:${barSize}`}</span>
        </div>
        {!selectedSymbol ? <p>Select a symbol from the sidebar to begin.</p> : null}
        {barsQuery.isError ? <p>Unable to load bars from the Rust API.</p> : null}
        {barsQuery.isLoading ? <p>Loading bars…</p> : null}
        {barsQuery.data?.bars.length ? <ChartCard bars={barsQuery.data.bars} /> : null}
      </article>

      <article className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Large orders</p>
            <h2>Fixed threshold overlay rows</h2>
          </div>
          <span className="pill">{largeOrderThreshold}</span>
        </div>
        {largeOrdersQuery.isError ? <p>Unable to load large orders from the Rust API.</p> : null}
        {largeOrdersQuery.isLoading ? <p>Loading large orders…</p> : null}
        <div className="stack">
          {largeOrdersQuery.data?.large_orders.slice(0, 8).map((row) => (
            <div className="job-card" key={`${row.ts}-${row.trade_price}-${row.trade_size}`}>
              <div className="profile-header">
                <strong>{row.side}</strong>
                <span>{row.trade_size.toFixed(2)}</span>
              </div>
              <p className="microcopy">
                {new Date(row.ts).toLocaleString()} • {row.trade_price.toFixed(2)}
              </p>
            </div>
          ))}
        </div>
      </article>
    </section>
  );
}
