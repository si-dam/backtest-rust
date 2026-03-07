from __future__ import annotations

import uuid
from datetime import datetime
from types import SimpleNamespace

from app.services.backtest_analytics import build_backtest_analytics


def _trade(entry_ts: str, exit_ts: str, pnl: float | None, notes: str = ""):
    return SimpleNamespace(
        id=uuid.uuid4(),
        entry_ts=datetime.fromisoformat(entry_ts),
        exit_ts=datetime.fromisoformat(exit_ts),
        pnl=pnl,
        notes=notes,
    )


def test_backtest_analytics_empty_payload():
    payload = build_backtest_analytics([], timezone="America/Chicago")
    assert payload["summary"]["trades"] == 0
    assert payload["summary"]["profit_factor"] is None
    assert payload["equity_curve"] == []
    assert payload["drawdown_curve"] == []
    assert payload["outliers"]["best_10_days"] == []
    assert payload["outliers"]["worst_10_days"] == []


def test_backtest_analytics_mixed_trades_and_outliers():
    trades = [
        _trade("2026-02-19T15:00:00+00:00", "2026-02-19T15:02:00+00:00", -4.0),
        _trade("2026-02-18T15:00:00+00:00", "2026-02-18T15:03:00+00:00", 10.0),
        _trade("2026-02-18T16:00:00+00:00", "2026-02-18T16:05:00+00:00", -5.0),
        _trade("2026-02-20T15:30:00+00:00", "2026-02-20T15:35:00+00:00", 2.0),
        _trade("2026-02-20T16:00:00+00:00", "2026-02-20T16:05:00+00:00", None),
    ]

    payload = build_backtest_analytics(trades, timezone="America/Chicago")
    summary = payload["summary"]

    assert summary["trades"] == 4
    assert summary["wins"] == 2
    assert summary["losses"] == 2
    assert summary["net_pnl"] == 3.0
    assert summary["expectancy"] == 0.75
    assert summary["avg_win"] == 6.0
    assert summary["avg_loss"] == -4.5
    assert summary["largest_loser"] == -5.0
    assert summary["max_consecutive_losses"] == 2
    assert round(summary["profit_factor"], 6) == round(12.0 / 9.0, 6)
    assert summary["max_drawdown"] == 9.0

    assert len(payload["equity_curve"]) == 4
    assert [row["equity_pnl"] for row in payload["equity_curve"]] == [10.0, 5.0, 1.0, 3.0]
    assert [row["drawdown_pnl"] for row in payload["drawdown_curve"]] == [0.0, 5.0, 9.0, 7.0]

    by_day = {row["date"]: row for row in payload["pnl_by_day"]}
    assert by_day["2026-02-18"]["pnl"] == 5.0
    assert by_day["2026-02-19"]["pnl"] == -4.0
    assert by_day["2026-02-20"]["pnl"] == 2.0

    best = payload["outliers"]["best_10_days"]
    worst = payload["outliers"]["worst_10_days"]
    assert best[0]["date"] == "2026-02-18"
    assert worst[0]["date"] == "2026-02-19"


def test_backtest_analytics_profit_factor_is_null_without_losses():
    trades = [
        _trade("2026-02-18T15:00:00+00:00", "2026-02-18T15:02:00+00:00", 3.0),
        _trade("2026-02-19T15:00:00+00:00", "2026-02-19T15:02:00+00:00", 1.0),
    ]
    payload = build_backtest_analytics(trades, timezone="America/Chicago")
    assert payload["summary"]["losses"] == 0
    assert payload["summary"]["profit_factor"] is None
