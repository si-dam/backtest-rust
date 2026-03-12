import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { getSymbols } from "./lib/api";
import BacktestsPanel from "./routes/BacktestsPanel";
import DatasetsPanel from "./routes/DatasetsPanel";
import IngestionPanel from "./routes/IngestionPanel";
import JobsPanel from "./routes/JobsPanel";
import ChartWorkspace from "./routes/MarketOverview";
import ProfilesPanel from "./routes/ProfilesPanel";

type Page = "chart" | "profiles" | "ingestion" | "jobs" | "backtests" | "datasets";

export default function App() {
  const [page, setPage] = useState<Page>("chart");
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

  const isChartPage = page === "chart";

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="sidebar-top">
          <div>
            <p className="eyebrow">backtest-rust</p>
            <h1>Market Core</h1>
            <p className="muted">
              Chart is the main workspace. Admin keeps the existing operational tools for ingestion,
              jobs, backtests, datasets, and profile inspection.
            </p>
          </div>
          <div className="sidebar-card">
            <p className="eyebrow">Current symbol</p>
            <strong className="sidebar-symbol">{selectedSymbol || "No symbol loaded"}</strong>
            <p className="microcopy">
              {symbolsQuery.isLoading
                ? "Loading contracts from /api/v1/symbols"
                : `${symbolsQuery.data?.symbols.length ?? 0} known contracts. Change it from Chart.`}
            </p>
          </div>
        </div>
        <nav className="nav">
          <button
            className={page === "chart" ? "nav-button active" : "nav-button"}
            onClick={() => setPage("chart")}
            type="button"
          >
            Chart
          </button>
          <p className="nav-section-label">Admin</p>
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
          <button
            className={page === "backtests" ? "nav-button active" : "nav-button"}
            onClick={() => setPage("backtests")}
            type="button"
          >
            Backtests
          </button>
          <button
            className={page === "datasets" ? "nav-button active" : "nav-button"}
            onClick={() => setPage("datasets")}
            type="button"
          >
            Datasets
          </button>
        </nav>
        <div className="sidebar-card">
          <p className="eyebrow">Runtime contract</p>
          <p className="muted">
            Chart reads persisted market artifacts from `/api/v1/markets/*`. Admin actions continue
            to go through the existing `/api/v1` job and export endpoints.
          </p>
        </div>
      </aside>
      <main className={isChartPage ? "content chart-content" : "content"}>
        {page === "chart" ? (
          <ChartWorkspace
            selectedSymbol={selectedSymbol}
            onSelectedSymbolChange={setSelectedSymbol}
            symbolOptions={symbolsQuery.data?.symbols.map((symbol) => symbol.symbol_contract) ?? []}
          />
        ) : null}
        {page === "profiles" ? <ProfilesPanel selectedSymbol={selectedSymbol} /> : null}
        {page === "ingestion" ? <IngestionPanel selectedSymbol={selectedSymbol} /> : null}
        {page === "jobs" ? <JobsPanel selectedSymbol={selectedSymbol} /> : null}
        {page === "backtests" ? <BacktestsPanel selectedSymbol={selectedSymbol} /> : null}
        {page === "datasets" ? <DatasetsPanel selectedSymbol={selectedSymbol} /> : null}
      </main>
    </div>
  );
}
