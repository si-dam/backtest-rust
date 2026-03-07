from __future__ import annotations

import uuid
from datetime import datetime

import pandas as pd

from app.db.models import BacktestRun, BacktestTrade, Job, JobType
from app.services import backtest as backtest_service


class _FakeSession:
    def __init__(self):
        self.added = []
        self.commit_count = 0

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        for obj in self.added:
            if isinstance(obj, BacktestRun) and not getattr(obj, "id", None):
                obj.id = uuid.uuid4()

    def commit(self):
        self.commit_count += 1

    def refresh(self, obj):
        if isinstance(obj, BacktestRun) and not getattr(obj, "id", None):
            obj.id = uuid.uuid4()


def _job() -> Job:
    job = Job(job_type=JobType.run_backtest, payload={})
    job.id = uuid.uuid4()
    return job


def test_run_backtest_scaffold_keeps_scaffold_strategy():
    db = _FakeSession()

    result = backtest_service.run_backtest_scaffold(
        db=db,
        job=_job(),
        name="Scaffold Test",
        strategy_id="scaffold",
        params={},
    )

    runs = [obj for obj in db.added if isinstance(obj, BacktestRun)]
    assert len(runs) == 1
    assert runs[0].strategy_id == "scaffold"
    assert result["strategy_id"] == "scaffold"


def test_run_backtest_orb_breakout_persists_run_and_trades(monkeypatch):
    bars = pd.DataFrame(
        [
            {
                "ts": datetime.fromisoformat("2026-02-18T09:31:00-05:00"),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 10.0,
            },
            {
                "ts": datetime.fromisoformat("2026-02-18T09:32:00-05:00"),
                "open": 100.0,
                "high": 103.0,
                "low": 100.0,
                "close": 102.0,
                "volume": 20.0,
            },
            {
                "ts": datetime.fromisoformat("2026-02-18T09:33:00-05:00"),
                "open": 102.0,
                "high": 108.0,
                "low": 101.0,
                "close": 106.0,
                "volume": 20.0,
            },
            {
                "ts": datetime.fromisoformat("2026-02-18T09:34:00-05:00"),
                "open": 106.0,
                "high": 107.0,
                "low": 105.0,
                "close": 106.0,
                "volume": 20.0,
            },
        ]
    )
    monkeypatch.setattr(backtest_service, "ensure_bars", lambda **_kwargs: bars)

    db = _FakeSession()
    result = backtest_service.run_backtest_scaffold(
        db=db,
        job=_job(),
        name="ORB Run",
        strategy_id="orb_breakout_v1",
        params={
            "symbol_contract": "NQH6",
            "start": "2026-02-18T09:30:00-05:00",
            "end": "2026-02-18T16:00:00-05:00",
            "timeframe": "1m",
            "ib_minutes": 1,
            "session_start": "09:30:00",
            "session_end": "16:00:00",
            "stop_mode": "or_boundary",
            "tp_r_multiple": 2.0,
            "entry_mode": "first_outside",
            "strategy_mode": "breakout_only",
            "big_trade_threshold": 25,
        },
    )

    runs = [obj for obj in db.added if isinstance(obj, BacktestRun)]
    trades = [obj for obj in db.added if isinstance(obj, BacktestTrade)]

    assert len(runs) == 1
    assert runs[0].strategy_id == "orb_breakout_v1"
    assert runs[0].metrics["net_pnl"] == runs[0].metrics["total_pnl"]
    assert "win_rate" in runs[0].metrics
    assert "max_drawdown" in runs[0].metrics
    assert len(trades) == 1
    assert trades[0].notes
    assert result["strategy_id"] == "orb_breakout_v1"
    assert result["trade_count"] == 1


def test_run_backtest_orb_breakout_applies_contract_size(monkeypatch):
    bars = pd.DataFrame(
        [
            {
                "ts": datetime.fromisoformat("2026-02-18T09:31:00-05:00"),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 10.0,
            }
        ]
    )
    trades = pd.DataFrame(
        [
            {
                "session_date": "2026-02-18",
                "timeframe": "1m",
                "ib_minutes": 1,
                "side": "long",
                "entry_time": datetime.fromisoformat("2026-02-18T09:31:00-05:00"),
                "entry_price": 100.0,
                "stop_price": 99.0,
                "target_price": 102.0,
                "exit_time": datetime.fromisoformat("2026-02-18T09:32:00-05:00"),
                "exit_price": 102.5,
                "exit_reason": "target",
                "pnl": 2.5,
                "r_multiple": 2.5,
            }
        ]
    )
    monkeypatch.setattr(backtest_service, "ensure_bars", lambda **_kwargs: bars)
    monkeypatch.setattr(backtest_service, "simulate_orb_breakout_strategy", lambda **_kwargs: trades)

    db = _FakeSession()
    backtest_service.run_backtest_scaffold(
        db=db,
        job=_job(),
        name="ORB Contracts",
        strategy_id="orb_breakout_v1",
        params={
            "symbol_contract": "NQH6",
            "start": "2026-02-18T09:30:00-05:00",
            "end": "2026-02-18T16:00:00-05:00",
            "timeframe": "1m",
            "ib_minutes": 1,
            "stop_mode": "or_boundary",
            "tp_r_multiple": 2.0,
            "entry_mode": "first_outside",
            "strategy_mode": "breakout_only",
            "big_trade_threshold": 25,
            "contracts": 3,
        },
    )

    run = next(obj for obj in db.added if isinstance(obj, BacktestRun))
    trade = next(obj for obj in db.added if isinstance(obj, BacktestTrade))

    assert run.params["contracts"] == 3
    assert run.metrics["net_pnl"] == 7.5
    assert trade.qty == 3.0
    assert trade.pnl == 7.5
    assert '"contracts": 3' in trade.notes


def test_run_backtest_orb_breakout_with_split_creates_two_runs(monkeypatch):
    bars = pd.DataFrame(
        [
            {
                "ts": datetime.fromisoformat("2026-02-18T09:31:00-05:00"),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 10.0,
            },
            {
                "ts": datetime.fromisoformat("2026-02-18T09:32:00-05:00"),
                "open": 100.0,
                "high": 103.0,
                "low": 100.0,
                "close": 102.0,
                "volume": 20.0,
            },
            {
                "ts": datetime.fromisoformat("2026-02-18T09:33:00-05:00"),
                "open": 102.0,
                "high": 108.0,
                "low": 101.0,
                "close": 106.0,
                "volume": 20.0,
            },
            {
                "ts": datetime.fromisoformat("2026-02-18T09:34:00-05:00"),
                "open": 106.0,
                "high": 107.0,
                "low": 105.0,
                "close": 106.0,
                "volume": 20.0,
            },
        ]
    )
    monkeypatch.setattr(backtest_service, "ensure_bars", lambda **_kwargs: bars)

    db = _FakeSession()
    result = backtest_service.run_backtest_scaffold(
        db=db,
        job=_job(),
        name="ORB Split",
        strategy_id="orb_breakout_v1",
        params={
            "symbol_contract": "NQH6",
            "start": "2026-02-18T09:30:00-05:00",
            "end": "2026-02-18T16:00:00-05:00",
            "split": {"enabled": True, "split_at": "2026-02-18T12:00:00-05:00"},
            "timeframe": "1m",
            "ib_minutes": 1,
            "rth_only": True,
            "stop_mode": "or_boundary",
            "tp_r_multiple": 2.0,
            "entry_mode": "first_outside",
            "strategy_mode": "breakout_only",
            "big_trade_threshold": 25,
        },
    )

    runs = [obj for obj in db.added if isinstance(obj, BacktestRun)]
    assert len(runs) == 2
    assert result["created_runs"] == 2
    assert len(result["run_ids"]) == 2
    for run in runs:
        assert run.params["split"]["enabled"] is True
        assert run.params["split"]["segment"] in {"is", "oos"}


def test_run_backtest_orb_breakout_rejects_invalid_split(monkeypatch):
    monkeypatch.setattr(backtest_service, "ensure_bars", lambda **_kwargs: pd.DataFrame())
    db = _FakeSession()
    try:
        backtest_service.run_backtest_scaffold(
            db=db,
            job=_job(),
            name="ORB Split Invalid",
            strategy_id="orb_breakout_v1",
            params={
                "symbol_contract": "NQH6",
                "start": "2026-02-18T09:30:00-05:00",
                "end": "2026-02-18T16:00:00-05:00",
                "split": {"enabled": True, "split_at": "2026-02-18T08:00:00-05:00"},
                "timeframe": "1m",
                "ib_minutes": 1,
                "rth_only": True,
                "stop_mode": "or_boundary",
                "tp_r_multiple": 2.0,
                "entry_mode": "first_outside",
                "strategy_mode": "breakout_only",
                "big_trade_threshold": 25,
            },
        )
        raise AssertionError("Expected ValueError")
    except ValueError as exc:
        assert "split.split_at" in str(exc)


def test_run_sweep_orb_batch_matrix(monkeypatch):
    bars = pd.DataFrame(
        [
            {
                "ts": datetime.fromisoformat("2026-02-18T09:31:00-05:00"),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 10.0,
            },
            {
                "ts": datetime.fromisoformat("2026-02-18T09:32:00-05:00"),
                "open": 100.0,
                "high": 103.0,
                "low": 100.0,
                "close": 102.0,
                "volume": 20.0,
            },
            {
                "ts": datetime.fromisoformat("2026-02-18T09:33:00-05:00"),
                "open": 102.0,
                "high": 108.0,
                "low": 101.0,
                "close": 106.0,
                "volume": 20.0,
            },
            {
                "ts": datetime.fromisoformat("2026-02-18T09:34:00-05:00"),
                "open": 106.0,
                "high": 107.0,
                "low": 105.0,
                "close": 106.0,
                "volume": 20.0,
            },
        ]
    )
    monkeypatch.setattr(backtest_service, "ensure_bars", lambda **_kwargs: bars)

    db = _FakeSession()
    result = backtest_service.run_sweep_scaffold(
        db=db,
        job=_job(),
        name="Sweep",
        strategy_id="orb_breakout_v1",
        params={
            "timeframe": "1m",
            "ib_minutes": 1,
            "rth_only": True,
            "stop_mode": "or_boundary",
            "tp_r_multiple": 2.0,
            "entry_mode": "first_outside",
            "strategy_mode": "breakout_only",
            "big_trade_threshold": 25,
            "batch": {
                "symbols": ["NQH6", "ESH6"],
                "periods": [
                    {"label": "wk1", "start": "2026-02-18T09:30:00-05:00", "end": "2026-02-18T16:00:00-05:00"},
                    {"label": "wk2", "start": "2026-02-19T09:30:00-05:00", "end": "2026-02-19T16:00:00-05:00"},
                ],
            },
        },
    )

    assert result["mode"] == "sweep"
    assert result["created_runs"] == 4
    assert result["failed_runs"] == 0
    assert len(result["run_ids"]) == 4
