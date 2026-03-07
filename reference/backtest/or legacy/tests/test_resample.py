from __future__ import annotations

import pandas as pd

from orbt.resample import resample_ticks_to_bars


def test_big_trade_flags_propagate_to_resampled_bar() -> None:
    ticks = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-02-18 09:30:00.000",
                    "2026-02-18 09:30:10.000",
                    "2026-02-18 09:30:20.000",
                ]
            ),
            "seq": [0, 0, 0],
            "open": [100.0, 101.0, 99.0],
            "high": [100.0, 101.0, 99.0],
            "low": [100.0, 101.0, 99.0],
            "last": [100.0, 101.0, 99.0],
            "volume": [1, 30, 26],
            "number_of_trades": [1, 1, 1],
            "bid_volume": [1, 0, 26],
            "ask_volume": [0, 30, 0],
        }
    )

    bars = resample_ticks_to_bars(
        ticks=ticks,
        timeframe_min=1,
        threshold=25,
        session_start="09:30:00",
        session_end="16:00:00",
    )

    assert len(bars) == 1
    row = bars.iloc[0]
    assert bool(row["has_big_buy"])
    assert bool(row["has_big_sell"])
    assert int(row["max_big_buy"]) == 30
    assert int(row["max_big_sell"]) == 26


def test_layered_bars_use_ohlc_price_and_tick_big_order_features() -> None:
    from orbt.resample import build_layered_bars

    ticks = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-02-18 09:31:10.000",
                    "2026-02-18 09:32:10.000",
                ]
            ),
            "seq": [0, 0],
            "last": [100.2, 100.8],
            "volume": [1, 30],
            "bid_volume": [1, 0],
            "ask_volume": [0, 30],
        }
    )

    # Full minute OHLC base (3 candles) even though ticks are sparse.
    ohlc_1m = pd.DataFrame(
        {
            "open": [100.0, 100.4, 100.6],
            "high": [100.5, 100.9, 101.2],
            "low": [99.8, 100.2, 100.5],
            "close": [100.4, 100.6, 101.0],
            "volume": [10, 12, 15],
        },
        index=pd.to_datetime(
            [
                "2026-02-18 09:31:00",
                "2026-02-18 09:32:00",
                "2026-02-18 09:33:00",
            ]
        ),
    )

    bars = build_layered_bars(
        ticks=ticks,
        timeframe_min=1,
        threshold=25,
        session_start="09:30:00",
        session_end="16:00:00",
        ohlc_1m=ohlc_1m,
    )

    assert len(bars) == 3
    # Price candles come from OHLC base.
    assert float(bars.loc[pd.Timestamp("2026-02-18 09:32:00"), "open"]) == 100.4
    assert float(bars.loc[pd.Timestamp("2026-02-18 09:33:00"), "close"]) == 101.0
    # Big order feature comes from tick stream (09:32:10 falls into 09:33 bar).
    assert bool(bars.loc[pd.Timestamp("2026-02-18 09:32:00"), "has_big_buy"]) is False
    assert bool(bars.loc[pd.Timestamp("2026-02-18 09:33:00"), "has_big_buy"]) is True
    assert int(bars.loc[pd.Timestamp("2026-02-18 09:33:00"), "max_big_buy"]) == 30
