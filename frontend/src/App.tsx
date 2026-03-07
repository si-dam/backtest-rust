import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { getSymbols } from "./lib/api";
import IngestionPanel from "./routes/IngestionPanel";
import JobsPanel from "./routes/JobsPanel";
import MarketOverview from "./routes/MarketOverview";
import ProfilesPanel from "./routes/ProfilesPanel";

type Page = "market" | "profiles" | "ingestion" | "jobs";

export default function App() {
  const [page, setPage] = useState<Page>("market");
  const [selectedSymbol, setSelectedSymbol] = useState("");
  const symbolsQuery = useQuery({
    queryKey: ["symbols"],
    queryFn: getSymbols,
  });

  useEffect(() => {
    if (!symbolsQuery.data?.symbols.length) {
      return;
    }

    const existing = symbolsQuery.data.symbols.some((symbol) => symbol.symbol_contract === selectedSymbol);
    if (!existing) {
      setSelectedSymbol(symbolsQuery.data.symbols[0].symbol_contract);
    }
  }, [selectedSymbol, symbolsQuery.data]);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="sidebar-top">
          <div>
            <p className="eyebrow">backtest-rust</p>
            <h1>Market Core</h1>
            <p className="muted">
              React shell for the Rust `/api/v1` runtime. This slice covers ingestion, bars, and
              persisted profiles.
            </p>
          </div>
          <div className="sidebar-card">
            <p className="eyebrow">Active symbol</p>
            <select
              aria-label="Select active symbol"
              className="field-input"
              value={selectedSymbol}
              onChange={(event) => setSelectedSymbol(event.target.value)}
            >
              {!symbolsQuery.data?.symbols.length ? <option value="">No symbols yet</option> : null}
              {symbolsQuery.data?.symbols.map((symbol) => (
                <option key={symbol.symbol_contract} value={symbol.symbol_contract}>
                  {symbol.symbol_contract}
                </option>
              ))}
            </select>
            <p className="microcopy">
              {symbolsQuery.isLoading
                ? "Loading symbols from /api/v1/symbols"
                : `${symbolsQuery.data?.symbols.length ?? 0} known contracts`}
            </p>
          </div>
        </div>
        <nav className="nav">
          <button
            className={page === "market" ? "nav-button active" : "nav-button"}
            onClick={() => setPage("market")}
            type="button"
          >
            Market
          </button>
          <button
            className={page === "profiles" ? "nav-button active" : "nav-button"}
            onClick={() => setPage("profiles")}
            type="button"
          >
            Profiles
          </button>
          <button
            className={page === "ingestion" ? "nav-button active" : "nav-button"}
            onClick={() => setPage("ingestion")}
            type="button"
          >
            Ingestion
          </button>
          <button
            className={page === "jobs" ? "nav-button active" : "nav-button"}
            onClick={() => setPage("jobs")}
            type="button"
          >
            Jobs
          </button>
        </nav>
        <div className="sidebar-card">
          <p className="eyebrow">Runtime contract</p>
          <p className="muted">
            Async jobs go through `/api/v1/ingestion/jobs` and `/api/v1/jobs/:job_id`. Market views
            read persisted bars and profile artifacts.
          </p>
        </div>
      </aside>
      <main className="content">
        {page === "market" ? <MarketOverview selectedSymbol={selectedSymbol} /> : null}
        {page === "profiles" ? <ProfilesPanel selectedSymbol={selectedSymbol} /> : null}
        {page === "ingestion" ? <IngestionPanel selectedSymbol={selectedSymbol} /> : null}
        {page === "jobs" ? <JobsPanel selectedSymbol={selectedSymbol} /> : null}
      </main>
    </div>
  );
}
