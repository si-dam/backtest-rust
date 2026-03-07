from __future__ import annotations

import pandas as pd

from orbt.metrics import summarize_trades


def test_summarize_trades_includes_full_tp_wins() -> None:
    trades = pd.DataFrame(
        [
            {"pnl": 10.0, "r_multiple": 1.0, "exit_reason": "target"},
            {"pnl": 5.0, "r_multiple": 0.5, "exit_reason": "session_close"},
            {"pnl": -4.0, "r_multiple": -1.0, "exit_reason": "stop"},
            {"pnl": -2.0, "r_multiple": -0.2, "exit_reason": "session_close"},
            {"pnl": 8.0, "r_multiple": 1.0, "exit_reason": "target"},
        ]
    )

    summary = summarize_trades(trades)

    assert summary["trades"] == 5
    assert summary["wins"] == 3
    assert summary["losses"] == 2
    assert summary["full_tp_wins"] == 2
    assert summary["full_losses"] == 1


def test_summarize_trades_empty_full_tp_wins_zero() -> None:
    trades = pd.DataFrame(columns=["pnl", "r_multiple", "exit_reason"])
    summary = summarize_trades(trades)
    assert summary["full_tp_wins"] == 0
    assert summary["full_losses"] == 0
