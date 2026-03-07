from __future__ import annotations

import pandas as pd

from orbt.signals import ORLevels, compute_opening_range_for_day, find_first_breakout_signal


def test_compute_opening_range_levels() -> None:
    index = pd.to_datetime(
        [
            "2026-02-18 09:31:00",
            "2026-02-18 09:32:00",
            "2026-02-18 09:33:00",
            "2026-02-18 09:34:00",
        ]
    )
    day_bars = pd.DataFrame(
        {
            "open": [100, 101, 102, 103],
            "high": [101, 104, 103, 105],
            "low": [99, 100, 101, 102],
            "close": [100, 103, 102, 104],
            "has_big_buy": [False, False, False, False],
            "has_big_sell": [False, False, False, False],
        },
        index=index,
    )

    levels = compute_opening_range_for_day(day_bars, ib_minutes=3, session_start="09:30:00")
    assert levels is not None
    assert levels.or_high == 104
    assert levels.or_low == 99
    assert levels.or_mid == 101.5


def test_first_breakout_without_same_candle_big_print_returns_none() -> None:
    index = pd.to_datetime(
        [
            "2026-02-18 09:46:00",
            "2026-02-18 09:47:00",
            "2026-02-18 09:48:00",
        ]
    )
    day_bars = pd.DataFrame(
        {
            "close": [105.5, 106.0, 106.5],
            "has_big_buy": [False, True, True],
            "has_big_sell": [False, False, False],
        },
        index=index,
    )

    levels = ORLevels(or_high=105.0, or_low=100.0, or_mid=102.5, ib_end=pd.Timestamp("2026-02-18 09:45:00"))

    signal = find_first_breakout_signal(day_bars, levels=levels, session_end="16:00:00")
    assert signal is None


def test_first_breakout_without_big_print_allowed_triggers_signal() -> None:
    index = pd.to_datetime(
        [
            "2026-02-18 09:46:00",
            "2026-02-18 09:47:00",
        ]
    )
    day_bars = pd.DataFrame(
        {
            "close": [105.5, 106.0],
            "has_big_buy": [False, True],
            "has_big_sell": [False, False],
        },
        index=index,
    )

    levels = ORLevels(or_high=105.0, or_low=100.0, or_mid=102.5, ib_end=pd.Timestamp("2026-02-18 09:45:00"))

    signal = find_first_breakout_signal(
        day_bars,
        levels=levels,
        session_end="16:00:00",
        require_big_trade=False,
    )
    assert signal == (pd.Timestamp("2026-02-18 09:46:00"), "long")


def test_first_breakout_with_same_candle_big_print_triggers_signal() -> None:
    index = pd.to_datetime(
        [
            "2026-02-18 09:46:00",
            "2026-02-18 09:47:00",
        ]
    )
    day_bars = pd.DataFrame(
        {
            "close": [105.5, 106.0],
            "has_big_buy": [True, False],
            "has_big_sell": [False, False],
        },
        index=index,
    )

    levels = ORLevels(or_high=105.0, or_low=100.0, or_mid=102.5, ib_end=pd.Timestamp("2026-02-18 09:45:00"))

    signal = find_first_breakout_signal(day_bars, levels=levels, session_end="16:00:00")
    assert signal == (pd.Timestamp("2026-02-18 09:46:00"), "long")
