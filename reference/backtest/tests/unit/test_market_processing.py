from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from app.services.market_processing import (
    build_bars_from_ticks,
    build_range_bars_from_ticks,
    build_tick_bars_from_ticks,
    build_volume_bars_from_ticks,
    build_delta_profile_levels,
    build_session_profile,
    build_volume_profile_levels,
    classify_trade_side,
    detect_large_orders,
    normalize_sierra_csv,
    normalize_sierra_ohlc_1m,
    resolve_symbol_tick_size,
)


def test_normalize_sierra_csv_with_datetime_column():
    raw = pd.DataFrame(
        {
            "Date Time": ["2026-02-24 08:30:00", "2026-02-24 08:30:01"],
            "Price": [22000.25, 22000.50],
            "Volume": [3, 150],
            "Bid": [22000.0, 22000.25],
            "Ask": [22000.25, 22000.5],
        }
    )

    out = normalize_sierra_csv(raw, dataset_tz="America/Chicago", symbol_contract="NQH6")

    assert list(out.columns) == [
        "ts",
        "trade_price",
        "trade_size",
        "bid_price",
        "ask_price",
        "symbol_contract",
        "session_date",
    ]
    assert out.iloc[0]["symbol_contract"] == "NQH6"
    assert str(out.iloc[0]["ts"].tzinfo) in {"America/Chicago", "CST", "CDT"}


def test_normalize_sierra_ohlc_1m_with_date_time_columns():
    raw = pd.DataFrame(
        {
            "Date": ["2026/02/24", "2026/02/24"],
            "Time": ["08:30:00", "08:31:00"],
            "Open": [22000.25, 22000.5],
            "High": [22001.0, 22001.25],
            "Low": [22000.0, 22000.25],
            "Last": [22000.75, 22001.0],
            "Volume": [10, 12],
            "NumberOfTrades": [4, 5],
        }
    )

    out = normalize_sierra_ohlc_1m(raw, dataset_tz="America/Chicago", symbol_contract="NQH6")

    assert list(out.columns) == [
        "ts",
        "session_date",
        "timeframe",
        "symbol_contract",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
    ]
    assert len(out) == 2
    assert set(out["timeframe"]) == {"1m"}
    assert set(out["symbol_contract"]) == {"NQH6"}


def test_normalize_sierra_ohlc_1m_rejects_non_minute_aligned_timestamps():
    raw = pd.DataFrame(
        {
            "Date": ["2026/02/24"],
            "Time": ["08:30:00.500"],
            "Open": [22000.25],
            "High": [22001.0],
            "Low": [22000.0],
            "Last": [22000.75],
            "Volume": [10],
            "NumberOfTrades": [4],
        }
    )

    with pytest.raises(ValueError, match="minute-aligned"):
        normalize_sierra_ohlc_1m(raw, dataset_tz="America/Chicago", symbol_contract="NQH6")


def test_build_bars_profile_and_large_orders():
    ts = pd.date_range(start="2026-02-24 08:30:00", periods=6, freq="10s", tz="America/Chicago")
    ticks = pd.DataFrame(
        {
            "ts": ts,
            "session_date": [datetime(2026, 2, 24).date()] * 6,
            "symbol_contract": ["ESH6"] * 6,
            "trade_price": [6100.0, 6100.25, 6100.5, 6100.25, 6100.75, 6101.0],
            "trade_size": [2, 3, 4, 50, 120, 5],
            "bid_price": [6099.75, 6100.0, 6100.25, 6100.0, 6100.5, 6100.75],
            "ask_price": [6100.0, 6100.25, 6100.5, 6100.25, 6100.75, 6101.0],
        }
    )

    bars = build_bars_from_ticks(ticks, timeframe="1m", dataset_tz="America/Chicago")
    assert len(bars) == 1
    assert float(bars.iloc[0]["open"]) == 6100.0
    assert float(bars.iloc[0]["close"]) == 6101.0
    assert float(bars.iloc[0]["volume"]) == 184.0

    bars_4h = build_bars_from_ticks(ticks, timeframe="4h", dataset_tz="America/Chicago")
    assert len(bars_4h) == 1
    assert bars_4h.iloc[0]["timeframe"] == "4h"

    bars_3m = build_bars_from_ticks(ticks, timeframe="3m", dataset_tz="America/Chicago")
    assert len(bars_3m) == 1
    assert bars_3m.iloc[0]["timeframe"] == "3m"

    bars_1d = build_bars_from_ticks(ticks, timeframe="1d", dataset_tz="America/Chicago")
    assert len(bars_1d) == 1
    assert bars_1d.iloc[0]["timeframe"] == "1d"

    profile = build_session_profile(ticks, tick_size=0.25)
    assert not profile.empty
    assert set(profile.columns) == {"session_date", "symbol_contract", "price_level", "volume"}

    fixed = detect_large_orders(ticks, method="fixed", fixed_threshold=100.0)
    assert len(fixed) == 1
    assert float(fixed.iloc[0]["trade_size"]) == 120.0
    assert fixed.iloc[0]["side"] == "buy"

    relative = detect_large_orders(ticks, method="relative", percentile=80.0)
    assert not relative.empty
    assert "threshold" in relative.columns
    assert "side" in relative.columns


def test_build_tick_bars_from_ticks_includes_partial_bar():
    ts = pd.date_range(start="2026-02-24 08:30:00", periods=5, freq="1s", tz="America/Chicago")
    ticks = pd.DataFrame(
        {
            "ts": ts,
            "session_date": [datetime(2026, 2, 24).date()] * 5,
            "symbol_contract": ["ESH6"] * 5,
            "trade_price": [100.0, 101.0, 102.0, 101.5, 101.25],
            "trade_size": [1, 1, 1, 1, 1],
            "bid_price": [99.75] * 5,
            "ask_price": [100.25] * 5,
        }
    )
    out = build_tick_bars_from_ticks(ticks=ticks, bar_size=2, dataset_tz="America/Chicago")
    assert len(out) == 3
    assert list(out["trade_count"]) == [2, 2, 1]
    assert out.iloc[0]["ts"] == ts[1]
    assert out.iloc[2]["ts"] == ts[4]


def test_build_volume_bars_from_ticks_uses_trade_size_and_partial_bar():
    ts = pd.date_range(start="2026-02-24 08:30:00", periods=4, freq="1s", tz="America/Chicago")
    ticks = pd.DataFrame(
        {
            "ts": ts,
            "session_date": [datetime(2026, 2, 24).date()] * 4,
            "symbol_contract": ["NQH6"] * 4,
            "trade_price": [22000.0, 22000.25, 22000.5, 22000.75],
            "trade_size": [200.0, 300.0, 400.0, 50.0],
            "bid_price": [21999.75] * 4,
            "ask_price": [22000.25] * 4,
        }
    )
    out = build_volume_bars_from_ticks(ticks=ticks, bar_size=500, dataset_tz="America/Chicago")
    assert len(out) == 2
    assert list(out["volume"]) == [500.0, 450.0]
    assert list(out["trade_count"]) == [2, 2]
    assert out.iloc[0]["ts"] == ts[1]
    assert out.iloc[1]["ts"] == ts[3]


def test_build_range_bars_from_ticks_fixed_span_no_overlap():
    ts = pd.date_range(start="2026-02-24 08:30:00", periods=6, freq="1s", tz="America/Chicago")
    ticks = pd.DataFrame(
        {
            "ts": ts,
            "session_date": [datetime(2026, 2, 24).date()] * 6,
            "symbol_contract": ["NQH6"] * 6,
            "trade_price": [22000.0, 22000.25, 22000.5, 22000.75, 22001.0, 22000.75],
            "trade_size": [1, 1, 1, 1, 1, 1],
            "bid_price": [21999.75] * 6,
            "ask_price": [22000.25] * 6,
        }
    )
    out = build_range_bars_from_ticks(ticks=ticks, bar_size=2, tick_size=0.25, dataset_tz="America/Chicago")
    assert len(out) >= 2
    # Range=2 ticks => 0.5 point. First completed bar should close exactly at +0.5 from open.
    assert float(out.iloc[0]["open"]) == 22000.0
    assert float(out.iloc[0]["close"]) == 22000.5
    # Completed bar timestamp should be the close tick timestamp.
    assert out.iloc[0]["ts"] == ts[2]


def test_detect_large_orders_side_classification():
    ts = pd.date_range(start="2026-02-24 08:30:00", periods=6, freq="1s", tz="America/Chicago")
    ticks = pd.DataFrame(
        {
            "ts": ts,
            "session_date": [datetime(2026, 2, 24).date()] * 6,
            "symbol_contract": ["ESH6"] * 6,
            "trade_price": [101.0, 100.0, 100.75, 100.25, 101.0, 100.0],
            "trade_size": [200, 200, 200, 200, 200, 200],
            "bid_price": [100.75, 100.0, 100.0, 100.0, np.nan, 100.0],
            "ask_price": [101.0, 100.25, 101.0, 101.0, 101.0, 100.0],
        }
    )

    out = detect_large_orders(ticks, method="fixed", fixed_threshold=100.0)
    side = classify_trade_side(ticks)

    assert list(side) == ["buy", "sell", "buy", "sell", "unknown", "sell"]
    assert list(out["side"]) == list(side)


def test_resolve_symbol_tick_size_uses_prefix_and_fallback():
    assert resolve_symbol_tick_size("NQH6") == 0.25
    assert resolve_symbol_tick_size("CLJ6") == 0.01
    assert resolve_symbol_tick_size("UNKNOWN") == 0.25
    assert resolve_symbol_tick_size("UNKNOWN", fallback=0.5) == 0.5


def test_build_volume_profile_levels_rounds_and_filters():
    ticks = pd.DataFrame(
        {
            "trade_price": [100.01, 100.24, 100.26, 100.74, 101.01],
            "trade_size": [1, 2, 3, 4, 5],
        }
    )
    out = build_volume_profile_levels(ticks=ticks, tick_size=0.25, price_min=100.1, price_max=100.9)
    assert list(out.columns) == ["price_level", "volume"]
    assert list(out["price_level"]) == [100.25, 100.75]
    assert list(out["volume"]) == [5, 4]


def test_build_delta_profile_levels_signed_and_filters():
    ticks = pd.DataFrame(
        {
            "trade_price": [100.0, 100.24, 100.26, 100.74, 100.75, 101.01],
            "trade_size": [1, 2, 3, 4, 5, 6],
            "bid_price": [99.75, 100.0, 100.0, 100.5, 100.5, 100.75],
            "ask_price": [100.0, 100.25, 100.5, 100.75, 101.0, 101.0],
            "side": ["buy", "buy", "sell", "buy", "sell", "unknown"],
        }
    )
    out = build_delta_profile_levels(ticks=ticks, tick_size=0.25, price_min=100.1, price_max=100.9)
    assert list(out.columns) == ["price_level", "delta"]
    assert list(out["price_level"]) == [100.25, 100.75]
    # 100.25 => +2 -3 = -1 ; 100.75 => +4 -5 = -1
    assert list(out["delta"]) == [-1.0, -1.0]


def test_build_volume_profile_levels_uses_half_up_tick_rounding():
    ticks = pd.DataFrame(
        {
            "trade_price": [100.25, 100.75, 101.25, 101.75],
            "trade_size": [1, 1, 1, 1],
        }
    )
    out = build_volume_profile_levels(ticks=ticks, tick_size=0.5)
    assert list(out["price_level"]) == [100.5, 101.0, 101.5, 102.0]
    assert list(out["volume"]) == [1, 1, 1, 1]


def test_build_delta_profile_levels_uses_half_up_tick_rounding():
    ticks = pd.DataFrame(
        {
            "trade_price": [100.25, 100.75, 101.25, 101.75],
            "trade_size": [1, 1, 1, 1],
            "side": ["buy", "buy", "buy", "buy"],
        }
    )
    out = build_delta_profile_levels(ticks=ticks, tick_size=0.5)
    assert list(out["price_level"]) == [100.5, 101.0, 101.5, 102.0]
    assert list(out["delta"]) == [1.0, 1.0, 1.0, 1.0]
