import pandas as pd

from app.strategy.orb_breakout_v1 import simulate_orb_breakout_strategy


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
        },
        index=index,
    )


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
        },
        index=index,
    )


def test_orb_breakout_entry_exit_or_boundary():
    trades = simulate_orb_breakout_strategy(
        bars=_bars(),
        timeframe="1m",
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


def test_orb_breakout_entry_exit_or_mid():
    trades = simulate_orb_breakout_strategy(
        bars=_bars(),
        timeframe="1m",
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


def test_orb_breakout_reentry_after_stop():
    trades = simulate_orb_breakout_strategy(
        bars=_bars_reentry_case(),
        timeframe="1m",
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
