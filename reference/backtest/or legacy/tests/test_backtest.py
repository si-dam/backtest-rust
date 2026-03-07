from __future__ import annotations

import pandas as pd

from orbt.backtest import simulate_orb_big_trade_strategy


def _bars() -> pd.DataFrame:
    index = pd.to_datetime(
        [
            "2026-02-18 09:31:00",
            "2026-02-18 09:32:00",
            "2026-02-18 09:33:00",
            "2026-02-18 09:34:00",
        ]
    )
    return pd.DataFrame(
        {
            "open": [100, 100, 102, 106],
            "high": [101, 103, 108, 107],
            "low": [99, 100, 101, 105],
            "close": [100, 102, 106, 106],
            "volume": [10, 20, 20, 20],
            "bid_volume": [5, 5, 5, 5],
            "ask_volume": [5, 15, 15, 15],
            "tick_count": [10, 20, 20, 20],
            "has_big_buy": [False, True, False, False],
            "has_big_sell": [False, False, False, False],
            "max_big_buy": [0, 30, 0, 0],
            "max_big_sell": [0, 0, 0, 0],
            "session_date": ["2026-02-18"] * 4,
        },
        index=index,
    )


def test_entry_exit_or_boundary() -> None:
    trades, _ = simulate_orb_big_trade_strategy(
        bars=_bars(),
        timeframe_min=1,
        ib_minutes=1,
        session_start="09:30:00",
        session_end="16:00:00",
        stop_mode="or_boundary",
        tp_r_multiple=2.0,
    )

    assert len(trades) == 1
    t = trades.iloc[0]
    assert t["side"] == "long"
    assert t["stop_price"] == 99
    assert t["exit_reason"] == "target"


def test_entry_exit_or_mid() -> None:
    trades, _ = simulate_orb_big_trade_strategy(
        bars=_bars(),
        timeframe_min=1,
        ib_minutes=1,
        session_start="09:30:00",
        session_end="16:00:00",
        stop_mode="or_mid",
        tp_r_multiple=2.0,
    )

    assert len(trades) == 1
    t = trades.iloc[0]
    assert t["stop_price"] == 100
    assert t["exit_reason"] == "target"


def _bars_reentry_case() -> pd.DataFrame:
    index = pd.to_datetime(
        [
            "2026-02-18 09:31:00",
            "2026-02-18 09:32:00",
            "2026-02-18 09:33:00",
            "2026-02-18 09:34:00",
            "2026-02-18 09:35:00",
            "2026-02-18 09:36:00",
        ]
    )
    return pd.DataFrame(
        {
            "open": [100, 100, 102, 98, 98, 95],
            "high": [101, 103, 103, 99, 99, 94],
            "low": [99, 101, 98, 97, 95, 91],
            "close": [100, 102, 99, 98, 96, 92],
            "volume": [10, 20, 20, 20, 20, 20],
            "bid_volume": [5, 5, 5, 15, 15, 15],
            "ask_volume": [5, 15, 5, 5, 5, 5],
            "tick_count": [10, 20, 20, 20, 20, 20],
            "has_big_buy": [False, True, False, False, False, False],
            "has_big_sell": [False, False, False, True, False, False],
            "max_big_buy": [0, 30, 0, 0, 0, 0],
            "max_big_sell": [0, 0, 0, 30, 0, 0],
            "session_date": ["2026-02-18"] * 6,
        },
        index=index,
    )


def test_reentry_mode_disabled_keeps_single_trade_after_stop() -> None:
    trades, _ = simulate_orb_big_trade_strategy(
        bars=_bars_reentry_case(),
        timeframe_min=1,
        ib_minutes=1,
        session_start="09:30:00",
        session_end="16:00:00",
        stop_mode="or_boundary",
        tp_r_multiple=2.0,
        entry_mode="first_outside",
    )

    assert len(trades) == 1
    assert trades.iloc[0]["exit_reason"] == "stop"


def test_reentry_mode_allows_second_trade_only_after_stop() -> None:
    trades, _ = simulate_orb_big_trade_strategy(
        bars=_bars_reentry_case(),
        timeframe_min=1,
        ib_minutes=1,
        session_start="09:30:00",
        session_end="16:00:00",
        stop_mode="or_boundary",
        tp_r_multiple=2.0,
        entry_mode="reentry_after_stop",
    )

    assert len(trades) == 2
    assert trades.iloc[0]["side"] == "long"
    assert trades.iloc[0]["exit_reason"] == "stop"
    assert trades.iloc[1]["side"] == "short"
    assert trades.iloc[1]["exit_reason"] == "target"


def _bars_breakout_only_case() -> pd.DataFrame:
    index = pd.to_datetime(
        [
            "2026-02-18 09:31:00",
            "2026-02-18 09:46:00",
            "2026-02-18 09:47:00",
        ]
    )
    return pd.DataFrame(
        {
            "open": [100, 100, 102],
            "high": [101, 103, 105],
            "low": [99, 100, 101],
            "close": [100, 102, 104],
            "volume": [10, 20, 20],
            "bid_volume": [5, 5, 5],
            "ask_volume": [5, 15, 15],
            "tick_count": [10, 20, 20],
            "has_big_buy": [False, False, False],
            "has_big_sell": [False, False, False],
            "max_big_buy": [0, 0, 0],
            "max_big_sell": [0, 0, 0],
            "session_date": ["2026-02-18"] * 3,
        },
        index=index,
    )


def test_breakout_only_strategy_can_enter_without_big_print() -> None:
    trades_strict, _ = simulate_orb_big_trade_strategy(
        bars=_bars_breakout_only_case(),
        timeframe_min=1,
        ib_minutes=1,
        session_start="09:30:00",
        session_end="16:00:00",
        stop_mode="or_boundary",
        tp_r_multiple=1.0,
        strategy_mode="big_order_required",
    )
    assert len(trades_strict) == 0

    trades_breakout_only, _ = simulate_orb_big_trade_strategy(
        bars=_bars_breakout_only_case(),
        timeframe_min=1,
        ib_minutes=1,
        session_start="09:30:00",
        session_end="16:00:00",
        stop_mode="or_boundary",
        tp_r_multiple=1.0,
        strategy_mode="breakout_only",
    )
    assert len(trades_breakout_only) == 1
    assert trades_breakout_only.iloc[0]["side"] == "long"
