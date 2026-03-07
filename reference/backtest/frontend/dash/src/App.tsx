import { Navigate, NavLink, Route, Routes, useLocation } from 'react-router-dom';
import { DatasetsPage } from './pages/DatasetsPage';
import { BacktestsPage } from './pages/BacktestsPage';
import { ChartWorkspacePage } from './pages/ChartWorkspacePage';

export function App() {
  const location = useLocation();
  const isChart = location.pathname === '/dash/chart';

  return (
    <div className={isChart ? 'app app-chart' : 'app'}>
      {!isChart && (
        <header className="app-header">
          <h1>Futures Backtest Platform</h1>
          <nav>
            <NavLink to="/dash/datasets">Datasets</NavLink>
            <NavLink to="/dash/chart">Chart Explorer</NavLink>
            <NavLink to="/dash/backtests">Backtest Runs</NavLink>
          </nav>
        </header>
      )}
      <main className={isChart ? 'main-chart' : 'main'}>
        <Routes>
          <Route path="/dash" element={<Navigate to="/dash/datasets" replace />} />
          <Route path="/dash/" element={<Navigate to="/dash/datasets" replace />} />
          <Route path="/dash/datasets" element={<DatasetsPage />} />
          <Route path="/dash/backtests" element={<BacktestsPage />} />
          <Route path="/dash/chart" element={<ChartWorkspacePage />} />
          <Route path="*" element={<Navigate to="/dash/datasets" replace />} />
        </Routes>
      </main>
    </div>
  );
}
