from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_backtests
from app.db.models import BacktestRun, BacktestTrade
from app.db.postgres import get_db_session


class _FakeScalarResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _FakeDB:
    def __init__(self, run: BacktestRun, trades: list[BacktestTrade]):
        self.run = run
        self.trades = trades

    def scalar(self, statement):
        query_text = str(statement)
        if "backtest_runs" in query_text:
            return self.run
        if "count(backtest_trades.id)" in query_text:
            return len(self.trades)
        return None

    def scalars(self, statement):
        query_text = str(statement)
        if "backtest_runs" in query_text:
            return _FakeScalarResult([self.run])
        if "backtest_trades" in query_text:
            return _FakeScalarResult(self.trades)
        return _FakeScalarResult([])

    def execute(self, statement):
        query_text = str(statement)
        if "count(backtest_trades.id)" in query_text and "GROUP BY" in query_text:
            return _FakeScalarResult([(self.run.id, len(self.trades))])
        return _FakeScalarResult([])


def _client(fake_db: _FakeDB) -> TestClient:
    app = FastAPI()
    app.include_router(routes_backtests.router)

    def _fake_dep():
        yield fake_db

    app.dependency_overrides[get_db_session] = _fake_dep
    return TestClient(app)


def _build_run_and_trade():
    run = BacktestRun(
        name="Run A",
        strategy_id="orb_breakout_v1",
        params={"timeframe": "1m", "ib_minutes": 15},
        metrics={"net_pnl": 12.5, "win_rate": 0.6, "max_drawdown": 3.0},
        status="completed",
    )
    run.id = uuid.uuid4()
    run.created_at = datetime(2026, 2, 28, 12, 0, tzinfo=timezone.utc)

    trade = BacktestTrade(
        run_id=run.id,
        symbol_contract="NQH6",
        entry_ts=datetime(2026, 2, 18, 14, 32, tzinfo=timezone.utc),
        exit_ts=datetime(2026, 2, 18, 14, 35, tzinfo=timezone.utc),
        entry_price=22000.0,
        exit_price=22008.0,
        qty=1.0,
        pnl=8.0,
        notes='{"exit_reason":"target","r_multiple":2.0}',
    )
    trade.id = uuid.uuid4()
    return run, trade


def test_backtest_strategies_endpoint_contract():
    run, trade = _build_run_and_trade()
    client = _client(_FakeDB(run=run, trades=[trade]))

    response = client.get("/backtests/strategies")
    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, list)
    strategy_ids = {row["id"] for row in payload}
    assert "orb_breakout_v1" in strategy_ids
    for strategy in payload:
        assert isinstance(strategy.get("params"), list)
        for param in strategy["params"]:
            assert "name" in param
            assert "type" in param
            assert "required" in param

    orb = next(row for row in payload if row["id"] == "orb_breakout_v1")
    orb_param_names = {p["name"] for p in orb["params"]}
    assert "rth_only" in orb_param_names


def test_backtest_run_endpoint_returns_single_run():
    run, trade = _build_run_and_trade()
    client = _client(_FakeDB(run=run, trades=[trade]))

    response = client.get(f"/backtests/runs/{run.id}")
    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == str(run.id)
    assert payload["strategy_id"] == "orb_breakout_v1"
    assert payload["params"]["timeframe"] == "1m"
    assert payload["trade_count"] == 1


def test_backtest_trades_endpoint_includes_side_and_notes():
    run, trade = _build_run_and_trade()
    client = _client(_FakeDB(run=run, trades=[trade]))

    response = client.get(f"/backtests/runs/{run.id}/trades")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["side"] == "long"
    assert payload[0]["notes"] == trade.notes


def test_backtest_analytics_endpoint_contract():
    run, trade = _build_run_and_trade()
    client = _client(_FakeDB(run=run, trades=[trade]))

    response = client.get(f"/backtests/runs/{run.id}/analytics")
    assert response.status_code == 200
    payload = response.json()
    assert payload["run"]["id"] == str(run.id)
    assert payload["summary"]["trades"] == 1
    assert "equity_curve" in payload
    assert "drawdown_curve" in payload
    assert "pnl_by_time_of_day" in payload
    assert "pnl_by_day" in payload
    assert "outliers" in payload


def test_backtest_export_config_and_csv_routes():
    run, trade = _build_run_and_trade()
    client = _client(_FakeDB(run=run, trades=[trade]))

    config_response = client.get(f"/backtests/runs/{run.id}/export/config.json")
    assert config_response.status_code == 200
    config_payload = config_response.json()
    assert config_payload["run_id"] == str(run.id)
    assert config_payload["strategy_id"] == "orb_breakout_v1"

    csv_response = client.get(f"/backtests/runs/{run.id}/export/trades.csv")
    assert csv_response.status_code == 200
    assert csv_response.headers["content-type"].startswith("text/csv")
    csv_text = csv_response.text
    assert "symbol_contract" in csv_text
    assert "exit_reason" in csv_text
    assert "r_multiple" in csv_text


def test_backtest_runs_list_includes_trade_count():
    run, trade = _build_run_and_trade()
    client = _client(_FakeDB(run=run, trades=[trade]))

    response = client.get("/backtests/runs")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["id"] == str(run.id)
    assert payload[0]["trade_count"] == 1
