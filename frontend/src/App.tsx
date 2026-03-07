import { useState } from "react";
import MarketOverview from "./routes/MarketOverview";
import ProfilesPanel from "./routes/ProfilesPanel";

type Page = "market" | "profiles";

export default function App() {
  const [page, setPage] = useState<Page>("market");

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div>
          <p className="eyebrow">backtest-rust</p>
          <h1>Market Core</h1>
          <p className="muted">
            React shell for the Rust `/api/v1` runtime. The first slice focuses on market data,
            bars, and profiles.
          </p>
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
        </nav>
      </aside>
      <main className="content">{page === "market" ? <MarketOverview /> : <ProfilesPanel />}</main>
    </div>
  );
}
