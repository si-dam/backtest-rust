from datetime import datetime

import pandas as pd
import pytest

from app.config import get_settings
from app.db.duck import get_duckdb_connection, init_duckdb
from app.services.aggregation import (
    ensure_area_volume_profile,
    ensure_bars,
    ensure_large_orders,
    ensure_preset_volume_profiles,
    ensure_session_profile,
    precompute_non_time_bar_presets,
    resolve_non_time_presets,
)
from app.services import aggregation as aggregation_service


def _insert_ticks(rows: pd.DataFrame) -> None:
    con = get_duckdb_connection()
    try:
        con.register("ticks_df", rows)
        con.execute(
            """
            INSERT INTO ticks (ts, session_date, symbol_contract, trade_price, trade_size, bid_price, ask_price, source_file)
            SELECT ts, session_date, symbol_contract, trade_price, trade_size, bid_price, ask_price, source_file
            FROM ticks_df
            """
        )
        con.unregister("ticks_df")
    finally:
        con.close()


def _insert_bars(rows: pd.DataFrame) -> None:
    con = get_duckdb_connection()
    try:
        con.register("bars_df", rows)
        con.execute(
            """
            INSERT INTO bars (ts, session_date, timeframe, symbol_contract, open, high, low, close, volume, trade_count)
            SELECT ts, session_date, timeframe, symbol_contract, open, high, low, close, volume, trade_count
            FROM bars_df
            """
        )
        con.unregister("bars_df")
    finally:
        con.close()


def _load_timeframes(symbol_contract: str) -> set[str]:
    con = get_duckdb_connection()
    try:
        rows = con.execute(
            """
            SELECT DISTINCT timeframe
            FROM bars
            WHERE symbol_contract = ?
            """,
            [symbol_contract],
        ).fetchall()
    finally:
        con.close()
    return {str(row[0]) for row in rows}


def test_duckdb_derived_tables(temp_duckdb):
    init_duckdb()

    rows = pd.DataFrame(
        {
            "ts": pd.date_range("2026-02-24 08:30:00", periods=10, freq="15s", tz="America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()] * 10,
            "symbol_contract": ["NQH6"] * 10,
            "trade_price": [22000 + (i * 0.25) for i in range(10)],
            "trade_size": [1, 2, 3, 4, 5, 6, 120, 8, 9, 10],
            "bid_price": [21999.75 + (i * 0.25) for i in range(10)],
            "ask_price": [22000.00 + (i * 0.25) for i in range(10)],
            "source_file": ["fixture.csv"] * 10,
        }
    )

    _insert_ticks(rows)

    start = rows["ts"].min().to_pydatetime()
    end = rows["ts"].max().to_pydatetime()

    bars = ensure_bars("NQH6", "1m", start, end)
    assert not bars.empty
    bars_4h = ensure_bars("NQH6", "4h", start, end)
    assert not bars_4h.empty
    bars_1d = ensure_bars("NQH6", "1d", start, end)
    assert not bars_1d.empty

    profile = ensure_session_profile("NQH6", "2026-02-24")
    assert not profile.empty

    large = ensure_large_orders("NQH6", start, end, method="fixed", fixed_threshold=100.0, percentile=99.0)
    assert not large.empty
    assert "side" in large.columns
    assert set(large["side"]) == {"buy"}


def test_ensure_bars_rebuilds_for_expanded_ranges(temp_duckdb):
    init_duckdb()

    rows_a = pd.DataFrame(
        {
            "ts": pd.date_range("2026-02-24 08:30:00", periods=4, freq="15s", tz="America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()] * 4,
            "symbol_contract": ["NQH6"] * 4,
            "trade_price": [22000.0, 22000.25, 22000.5, 22000.75],
            "trade_size": [1, 2, 3, 4],
            "bid_price": [21999.75, 22000.0, 22000.25, 22000.5],
            "ask_price": [22000.0, 22000.25, 22000.5, 22000.75],
            "source_file": ["fixture-a.csv"] * 4,
        }
    )
    _insert_ticks(rows_a)

    start = rows_a["ts"].min().to_pydatetime()
    end_a = rows_a["ts"].max().to_pydatetime()
    bars_initial = ensure_bars("NQH6", "1m", start, end_a)
    assert len(bars_initial) == 1

    rows_b = pd.DataFrame(
        {
            "ts": pd.date_range("2026-02-24 08:31:00", periods=4, freq="15s", tz="America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()] * 4,
            "symbol_contract": ["NQH6"] * 4,
            "trade_price": [22001.0, 22001.25, 22001.5, 22001.75],
            "trade_size": [5, 6, 7, 8],
            "bid_price": [22000.75, 22001.0, 22001.25, 22001.5],
            "ask_price": [22001.0, 22001.25, 22001.5, 22001.75],
            "source_file": ["fixture-b.csv"] * 4,
        }
    )
    _insert_ticks(rows_b)

    end_b = rows_b["ts"].max().to_pydatetime()
    bars_expanded = ensure_bars("NQH6", "1m", start, end_b)
    assert len(bars_expanded) == 2


def test_ensure_bars_backfills_edges_without_recomputing_overlap(temp_duckdb):
    init_duckdb()

    cached_rows = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2026-02-24 08:31:00"]).tz_localize("America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()],
            "timeframe": ["1m"],
            "symbol_contract": ["NQH26"],
            "open": [999.0],
            "high": [999.5],
            "low": [998.5],
            "close": [999.25],
            "volume": [50.0],
            "trade_count": [2],
        }
    )
    _insert_bars(cached_rows)

    ticks = pd.DataFrame(
        {
            "ts": pd.to_datetime(
                [
                    "2026-02-24 08:30:00",
                    "2026-02-24 08:30:30",
                    "2026-02-24 08:31:00",
                    "2026-02-24 08:31:30",
                    "2026-02-24 08:32:00",
                    "2026-02-24 08:32:30",
                ]
            ).tz_localize("America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()] * 6,
            "symbol_contract": ["NQH26"] * 6,
            "trade_price": [100.0, 101.0, 200.0, 201.0, 300.0, 301.0],
            "trade_size": [1, 1, 1, 1, 1, 1],
            "bid_price": [99.75] * 6,
            "ask_price": [100.25] * 6,
            "source_file": ["fixture-overlap.csv"] * 6,
        }
    )
    _insert_ticks(ticks)

    start = pd.Timestamp("2026-02-24 08:30:00", tz="America/Chicago").to_pydatetime()
    end = pd.Timestamp("2026-02-24 08:32:59", tz="America/Chicago").to_pydatetime()

    bars = ensure_bars("NQH26", "1m", start, end)
    assert len(bars) == 3
    middle = bars.sort_values("ts").reset_index(drop=True).iloc[1]
    assert float(middle["open"]) == 999.0


def test_ensure_bars_force_recompute_replaces_overlap(temp_duckdb):
    init_duckdb()

    cached_rows = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2026-02-24 08:31:00"]).tz_localize("America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()],
            "timeframe": ["1m"],
            "symbol_contract": ["NQH26"],
            "open": [999.0],
            "high": [999.5],
            "low": [998.5],
            "close": [999.25],
            "volume": [50.0],
            "trade_count": [2],
        }
    )
    _insert_bars(cached_rows)

    ticks = pd.DataFrame(
        {
            "ts": pd.to_datetime(
                [
                    "2026-02-24 08:30:00",
                    "2026-02-24 08:30:30",
                    "2026-02-24 08:31:00",
                    "2026-02-24 08:31:30",
                    "2026-02-24 08:32:00",
                    "2026-02-24 08:32:30",
                ]
            ).tz_localize("America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()] * 6,
            "symbol_contract": ["NQH26"] * 6,
            "trade_price": [100.0, 101.0, 200.0, 201.0, 300.0, 301.0],
            "trade_size": [1, 1, 1, 1, 1, 1],
            "bid_price": [99.75] * 6,
            "ask_price": [100.25] * 6,
            "source_file": ["fixture-force.csv"] * 6,
        }
    )
    _insert_ticks(ticks)

    start = pd.Timestamp("2026-02-24 08:30:00", tz="America/Chicago").to_pydatetime()
    end = pd.Timestamp("2026-02-24 08:32:59", tz="America/Chicago").to_pydatetime()

    bars = ensure_bars("NQH26", "1m", start, end, force_recompute=True)
    assert len(bars) == 3
    middle = bars.sort_values("ts").reset_index(drop=True).iloc[1]
    assert float(middle["open"]) == 200.0


def test_ensure_bars_uses_cached_rows_when_ticks_missing(temp_duckdb):
    init_duckdb()

    bars_rows = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2026-02-24 08:30:00"]).tz_localize("America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()],
            "timeframe": ["1m"],
            "symbol_contract": ["NQH26"],
            "open": [22000.0],
            "high": [22001.0],
            "low": [21999.5],
            "close": [22000.5],
            "volume": [42.0],
            "trade_count": [3],
        }
    )
    _insert_bars(bars_rows)

    start = pd.Timestamp("2026-02-24 08:00:00", tz="America/Chicago").to_pydatetime()
    end = pd.Timestamp("2026-02-24 09:00:00", tz="America/Chicago").to_pydatetime()

    bars = ensure_bars("NQH26", "1m", start, end)
    assert len(bars) == 1
    assert float(bars.iloc[0]["open"]) == 22000.0


def test_ensure_bars_rebuilds_from_lower_timeframe_when_ticks_missing(temp_duckdb):
    init_duckdb()

    bars_rows = pd.DataFrame(
        {
            "ts": pd.to_datetime(
                [
                    "2026-02-24 08:30:00",
                    "2026-02-24 08:35:00",
                    "2026-02-24 08:40:00",
                ]
            ).tz_localize("America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()] * 3,
            "timeframe": ["5m"] * 3,
            "symbol_contract": ["NQH26"] * 3,
            "open": [22000.0, 22000.5, 22001.0],
            "high": [22000.5, 22001.2, 22001.6],
            "low": [21999.8, 22000.2, 22000.9],
            "close": [22000.4, 22001.0, 22001.5],
            "volume": [10.0, 20.0, 30.0],
            "trade_count": [2, 3, 4],
        }
    )
    _insert_bars(bars_rows)

    start = pd.Timestamp("2026-02-24 08:30:00", tz="America/Chicago").to_pydatetime()
    end = pd.Timestamp("2026-02-24 08:45:00", tz="America/Chicago").to_pydatetime()

    bars = ensure_bars("NQH26", "15m", start, end)
    assert len(bars) == 1
    row = bars.iloc[0]
    assert float(row["open"]) == 22000.0
    assert float(row["high"]) == 22001.6
    assert float(row["low"]) == 21999.8
    assert float(row["close"]) == 22001.5
    assert float(row["volume"]) == 60.0
    assert int(row["trade_count"]) == 9


def test_large_orders_reflects_updated_thresholds(temp_duckdb):
    init_duckdb()

    rows = pd.DataFrame(
        {
            "ts": pd.date_range("2026-02-24 08:30:00", periods=10, freq="15s", tz="America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()] * 10,
            "symbol_contract": ["NQH6"] * 10,
            "trade_price": [22000 + (i * 0.25) for i in range(10)],
            "trade_size": [1, 2, 3, 4, 5, 6, 120, 8, 9, 10],
            "bid_price": [21999.75 + (i * 0.25) for i in range(10)],
            "ask_price": [22000.00 + (i * 0.25) for i in range(10)],
            "source_file": ["fixture.csv"] * 10,
        }
    )
    _insert_ticks(rows)

    start = rows["ts"].min().to_pydatetime()
    end = rows["ts"].max().to_pydatetime()

    strict = ensure_large_orders("NQH6", start, end, method="fixed", fixed_threshold=100.0, percentile=99.0)
    relaxed = ensure_large_orders("NQH6", start, end, method="fixed", fixed_threshold=5.0, percentile=99.0)

    assert len(strict) == 1
    assert len(relaxed) > len(strict)
    assert "side" in strict.columns
    assert strict.iloc[0]["side"] == "buy"


def test_ensure_large_orders_non_persistent_does_not_rewrite_table(temp_duckdb):
    init_duckdb()

    rows = pd.DataFrame(
        {
            "ts": pd.date_range("2026-02-24 08:30:00", periods=6, freq="15s", tz="America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()] * 6,
            "symbol_contract": ["NQH6"] * 6,
            "trade_price": [22000.0, 22000.25, 22000.5, 22000.75, 22001.0, 22001.25],
            "trade_size": [1, 2, 3, 4, 110, 6],
            "bid_price": [21999.75] * 6,
            "ask_price": [22000.0] * 6,
            "source_file": ["fixture-large.csv"] * 6,
        }
    )
    _insert_ticks(rows)

    start = rows["ts"].min().to_pydatetime()
    end = rows["ts"].max().to_pydatetime()

    persisted = ensure_large_orders("NQH6", start, end, method="fixed", fixed_threshold=100.0, percentile=99.0, persist=True)
    assert len(persisted) == 1

    con = get_duckdb_connection()
    try:
        before_count = con.execute("SELECT COUNT(*) FROM large_orders WHERE symbol_contract = 'NQH6'").fetchone()[0]
    finally:
        con.close()

    volatile = ensure_large_orders("NQH6", start, end, method="fixed", fixed_threshold=5.0, percentile=99.0, persist=False)
    assert len(volatile) > len(persisted)

    con = get_duckdb_connection()
    try:
        after_count = con.execute("SELECT COUNT(*) FROM large_orders WHERE symbol_contract = 'NQH6'").fetchone()[0]
    finally:
        con.close()

    assert after_count == before_count


def test_precompute_non_time_bar_presets_persists_expected_timeframes(temp_duckdb):
    init_duckdb()

    rows = pd.DataFrame(
        {
            "ts": pd.date_range("2026-02-24 08:30:00", periods=24, freq="15s", tz="America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()] * 24,
            "symbol_contract": ["NQH6"] * 24,
            "trade_price": [22000 + (i * 0.25) for i in range(24)],
            "trade_size": [1 + (i % 5) for i in range(24)],
            "bid_price": [21999.75 + (i * 0.25) for i in range(24)],
            "ask_price": [22000.0 + (i * 0.25) for i in range(24)],
            "source_file": ["preset-fixture.csv"] * 24,
        }
    )
    _insert_ticks(rows)

    payload = precompute_non_time_bar_presets("NQH6")
    assert payload["symbol_contract"] == "NQH6"
    assert len(payload["presets"]) == 5

    expected = {"tick:1500", "volume:500", "volume:750", "volume:1000", "range:40"}
    assert expected.issubset(_load_timeframes("NQH6"))


def test_precompute_non_time_bar_presets_supports_single_preset(temp_duckdb):
    init_duckdb()

    rows = pd.DataFrame(
        {
            "ts": pd.date_range("2026-02-24 08:30:00", periods=12, freq="15s", tz="America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()] * 12,
            "symbol_contract": ["NQH6"] * 12,
            "trade_price": [22000 + (i * 0.25) for i in range(12)],
            "trade_size": [1 + (i % 3) for i in range(12)],
            "bid_price": [21999.75 + (i * 0.25) for i in range(12)],
            "ask_price": [22000.0 + (i * 0.25) for i in range(12)],
            "source_file": ["single-preset.csv"] * 12,
        }
    )
    _insert_ticks(rows)

    payload = precompute_non_time_bar_presets("NQH6", presets=resolve_non_time_presets(["volume:750"]))
    assert [row["timeframe"] for row in payload["presets"]] == ["volume:750"]
    assert "volume:750" in _load_timeframes("NQH6")


def test_ensure_bars_non_time_uses_persisted_rows_when_fresh(temp_duckdb, monkeypatch):
    init_duckdb()

    rows = pd.DataFrame(
        {
            "ts": pd.date_range("2026-02-24 08:30:00", periods=24, freq="15s", tz="America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()] * 24,
            "symbol_contract": ["NQH6"] * 24,
            "trade_price": [22000 + (i * 0.25) for i in range(24)],
            "trade_size": [1 + (i % 5) for i in range(24)],
            "bid_price": [21999.75 + (i * 0.25) for i in range(24)],
            "ask_price": [22000.0 + (i * 0.25) for i in range(24)],
            "source_file": ["fresh-fixture.csv"] * 24,
        }
    )
    _insert_ticks(rows)
    precompute_non_time_bar_presets("NQH6")

    def _fail_builder(*_args, **_kwargs):
        raise AssertionError("non-time bars should be served from persisted rows when fresh")

    monkeypatch.setattr(aggregation_service, "_build_non_time_bars_chunked", _fail_builder)

    start = rows["ts"].min().to_pydatetime()
    end = rows["ts"].max().to_pydatetime()
    bars = ensure_bars("NQH6", "1m", start, end, bar_type="tick", bar_size=1500)
    assert len(bars) > 0
    assert set(bars["timeframe"]) == {"tick:1500"}


def test_ensure_bars_non_time_rebuilds_when_new_ticks_arrive(temp_duckdb):
    init_duckdb()

    rows = pd.DataFrame(
        {
            "ts": pd.date_range("2026-02-24 08:30:00", periods=24, freq="15s", tz="America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()] * 24,
            "symbol_contract": ["NQH6"] * 24,
            "trade_price": [22000 + (i * 0.25) for i in range(24)],
            "trade_size": [1 + (i % 5) for i in range(24)],
            "bid_price": [21999.75 + (i * 0.25) for i in range(24)],
            "ask_price": [22000.0 + (i * 0.25) for i in range(24)],
            "source_file": ["stale-base.csv"] * 24,
        }
    )
    _insert_ticks(rows)
    precompute_non_time_bar_presets("NQH6")

    newer_rows = pd.DataFrame(
        {
            "ts": pd.date_range("2026-02-24 08:36:00", periods=8, freq="15s", tz="America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()] * 8,
            "symbol_contract": ["NQH6"] * 8,
            "trade_price": [22020 + (i * 0.25) for i in range(8)],
            "trade_size": [2] * 8,
            "bid_price": [22019.75 + (i * 0.25) for i in range(8)],
            "ask_price": [22020.0 + (i * 0.25) for i in range(8)],
            "source_file": ["stale-new.csv"] * 8,
        }
    )
    _insert_ticks(newer_rows)

    start = rows["ts"].min().to_pydatetime()
    end = newer_rows["ts"].max().to_pydatetime()
    bars = ensure_bars("NQH6", "1m", start, end, bar_type="tick", bar_size=1500)
    assert len(bars) > 0

    con = get_duckdb_connection()
    try:
        latest_bar = con.execute(
            """
            SELECT MAX(ts)
            FROM bars
            WHERE symbol_contract = 'NQH6'
              AND timeframe = 'tick:1500'
            """
        ).fetchone()[0]
    finally:
        con.close()
    assert pd.Timestamp(latest_bar) >= pd.Timestamp(newer_rows["ts"].max())


def test_ensure_large_orders_persistent_mode_keeps_other_threshold_rows(temp_duckdb):
    init_duckdb()

    rows = pd.DataFrame(
        {
            "ts": pd.date_range("2026-02-24 08:30:00", periods=8, freq="15s", tz="America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()] * 8,
            "symbol_contract": ["NQH6"] * 8,
            "trade_price": [22000 + (i * 0.25) for i in range(8)],
            "trade_size": [5, 10, 15, 20, 30, 60, 80, 120],
            "bid_price": [21999.75 + (i * 0.25) for i in range(8)],
            "ask_price": [22000.00 + (i * 0.25) for i in range(8)],
            "source_file": ["fixture-threshold.csv"] * 8,
        }
    )
    _insert_ticks(rows)

    start = rows["ts"].min().to_pydatetime()
    end = rows["ts"].max().to_pydatetime()

    strict = ensure_large_orders("NQH6", start, end, method="fixed", fixed_threshold=100.0, percentile=99.0, persist=True)
    assert len(strict) == 1

    con = get_duckdb_connection()
    try:
        strict_before = con.execute(
            "SELECT COUNT(*) FROM large_orders WHERE symbol_contract = 'NQH6' AND method = 'fixed' AND threshold = 100.0"
        ).fetchone()[0]
    finally:
        con.close()
    assert strict_before == 1

    ensure_large_orders("NQH6", start, end, method="fixed", fixed_threshold=20.0, percentile=99.0, persist=True)

    con = get_duckdb_connection()
    try:
        strict_after = con.execute(
            "SELECT COUNT(*) FROM large_orders WHERE symbol_contract = 'NQH6' AND method = 'fixed' AND threshold = 100.0"
        ).fetchone()[0]
    finally:
        con.close()

    assert strict_after == strict_before


def test_preset_and_area_volume_profiles(temp_duckdb):
    init_duckdb()
    dataset_tz = get_settings().dataset_tz
    ts = pd.to_datetime(
        [
            # Fixed UTC points around ET session boundaries (09:30-16:00 ET)
            # so this test remains stable regardless DATASET_TZ.
            "2026-02-24T14:25:00Z",  # 09:25 ET (ETH)
            "2026-02-24T14:35:00Z",  # 09:35 ET (RTH)
            "2026-02-24T20:59:00Z",  # 15:59 ET (RTH)
            "2026-02-24T21:05:00Z",  # 16:05 ET (ETH)
            "2026-02-25T14:20:00Z",  # 09:20 ET (ETH)
            "2026-02-25T14:31:00Z",  # 09:31 ET (RTH)
            "2026-02-25T21:01:00Z",  # 16:01 ET (ETH)
            "2026-03-02T20:00:00Z",  # 15:00 ET (RTH)
            "2026-03-02T21:00:00Z",  # 16:00 ET (ETH)
        ]
    )

    rows = pd.DataFrame(
        {
            "ts": ts,
            "session_date": ts.tz_convert(dataset_tz).date,
            "symbol_contract": ["NQH6"] * 9,
            "trade_price": [100.0, 100.25, 100.5, 100.75, 100.0, 100.25, 100.5, 100.75, 101.0],
            "trade_size": [1, 2, 3, 4, 5, 6, 7, 8, 9],
            "bid_price": [99.75] * 9,
            "ask_price": [100.0] * 9,
            "source_file": ["fixture-profile.csv"] * 9,
        }
    )
    _insert_ticks(rows)

    start = rows["ts"].min().to_pydatetime()
    end = rows["ts"].max().to_pydatetime()

    day_profiles = ensure_preset_volume_profiles("NQH6", start, end, preset="day")
    assert day_profiles["preset"] == "day"
    assert day_profiles["metric"] == "volume"
    assert len(day_profiles["profiles"]) == 5
    assert day_profiles["profiles"][0]["label"] == "Day 2026-02-23"

    week_profiles = ensure_preset_volume_profiles("NQH6", start, end, preset="week")
    assert len(week_profiles["profiles"]) == 2

    rth_profiles = ensure_preset_volume_profiles("NQH6", start, end, preset="rth")
    rth_by_label = {row["label"]: row["total_volume"] for row in rth_profiles["profiles"]}
    assert rth_by_label["RTH 2026-02-24"] == 5.0
    assert rth_by_label["RTH 2026-02-25"] == 6.0
    assert rth_by_label["RTH 2026-03-02"] == 8.0

    eth_profiles = ensure_preset_volume_profiles("NQH6", start, end, preset="eth")
    eth_by_label = {row["label"]: row["total_volume"] for row in eth_profiles["profiles"]}
    assert eth_by_label["ETH 2026-02-23"] == 1.0
    assert eth_by_label["ETH 2026-02-24"] == 9.0
    assert eth_by_label["ETH 2026-02-25"] == 7.0
    assert eth_by_label["ETH 2026-03-02"] == 9.0

    utc_rth_profiles = ensure_preset_volume_profiles("NQH6", start, end, preset="rth", profile_timezone="UTC")
    utc_rth_by_label = {row["label"]: row["total_volume"] for row in utc_rth_profiles["profiles"]}
    assert utc_rth_by_label["RTH 2026-02-24"] == 5.0
    assert utc_rth_by_label["RTH 2026-02-25"] == 6.0
    assert utc_rth_by_label["RTH 2026-03-02"] == 8.0
    assert utc_rth_profiles["timezone"] == "UTC"
    rth_profile = next((row for row in rth_profiles["profiles"] if row["label"] == "RTH 2026-02-24"), None)
    assert rth_profile is not None
    expected_default_rth_end = pd.Timestamp("2026-02-24 16:00:00", tz="America/New_York").tz_convert(dataset_tz)
    default_rth_end = pd.Timestamp(rth_profile["end"])
    assert default_rth_end.hour == expected_default_rth_end.hour
    assert default_rth_end.minute == expected_default_rth_end.minute

    et_rth_profiles = ensure_preset_volume_profiles("NQH6", start, end, preset="rth", profile_timezone="America/New_York")
    et_rth_profile = next((row for row in et_rth_profiles["profiles"] if row["label"] == "RTH 2026-02-24"), None)
    assert et_rth_profile is not None
    assert pd.Timestamp(et_rth_profile["end"]).hour == 16
    assert pd.Timestamp(et_rth_profile["end"]).minute == 0

    eth_profile = next((row for row in eth_profiles["profiles"] if row["label"] == "ETH 2026-02-24"), None)
    assert eth_profile is not None
    eth_end = pd.Timestamp(eth_profile["end"])
    expected_default_eth_end = pd.Timestamp("2026-02-25 09:30:00", tz="America/New_York").tz_convert(dataset_tz)
    assert eth_end.date().isoformat() == expected_default_eth_end.date().isoformat()
    assert eth_end.hour == expected_default_eth_end.hour
    assert eth_end.minute == expected_default_eth_end.minute

    day_profile = next((row for row in day_profiles["profiles"] if row["label"] == "Day 2026-02-24"), None)
    assert day_profile is not None
    day_end = pd.Timestamp(day_profile["end"])
    expected_default_day_end = pd.Timestamp("2026-02-25 16:00:00", tz="America/New_York").tz_convert(dataset_tz)
    assert day_end.date().isoformat() == expected_default_day_end.date().isoformat()
    assert day_end.hour == expected_default_day_end.hour
    assert day_end.minute == expected_default_day_end.minute

    area_profile = ensure_area_volume_profile(
        symbol_contract="NQH6",
        start=pd.Timestamp("2026-02-24T14:00:00Z").to_pydatetime(),
        end=pd.Timestamp("2026-02-24T22:00:00Z").to_pydatetime(),
        price_min=100.8,
        price_max=100.2,
        area_id="vp-test",
    )
    assert area_profile["mode"] == "area"
    assert area_profile["profile"]["id"] == "vp-test"
    assert area_profile["profile"]["price_min"] == 100.2
    assert area_profile["profile"]["price_max"] == 100.8
    assert area_profile["profile"]["total_volume"] == 9.0
    assert area_profile["profile"]["total_value"] == 9.0

    aggregated_day_profiles = ensure_preset_volume_profiles(
        "NQH6",
        start,
        end,
        preset="day",
        tick_aggregation=2,
        value_area_enabled=True,
        value_area_percent=70.0,
    )
    assert aggregated_day_profiles["tick_aggregation"] == 2
    assert aggregated_day_profiles["tick_size"] == 0.5
    assert all(row["value_area_enabled"] is True for row in aggregated_day_profiles["profiles"])
    assert all(row["value_area_low"] is not None for row in aggregated_day_profiles["profiles"])
    assert all(row["value_area_high"] is not None for row in aggregated_day_profiles["profiles"])

    aggregated_area_profile = ensure_area_volume_profile(
        symbol_contract="NQH6",
        start=pd.Timestamp("2026-02-24T14:00:00Z").to_pydatetime(),
        end=pd.Timestamp("2026-02-24T22:00:00Z").to_pydatetime(),
        price_min=100.8,
        price_max=100.2,
        area_id="vp-agg",
        tick_aggregation=2,
        value_area_enabled=True,
        value_area_percent=70.0,
    )
    assert aggregated_area_profile["tick_aggregation"] == 2
    assert aggregated_area_profile["tick_size"] == 0.5
    assert aggregated_area_profile["profile"]["value_area_enabled"] is True
    assert aggregated_area_profile["profile"]["value_area_low"] is not None
    assert aggregated_area_profile["profile"]["value_area_high"] is not None

    delta_day_profiles = ensure_preset_volume_profiles("NQH6", start, end, preset="day", metric="delta")
    assert delta_day_profiles["metric"] == "delta"
    assert len(delta_day_profiles["profiles"]) == 5
    assert all("total_value" in row for row in delta_day_profiles["profiles"])
    with pytest.raises(ValueError, match="Unsupported preset: week"):
        ensure_preset_volume_profiles("NQH6", start, end, preset="week", metric="delta")

    delta_area_profile = ensure_area_volume_profile(
        symbol_contract="NQH6",
        start=pd.Timestamp("2026-02-24T14:00:00Z").to_pydatetime(),
        end=pd.Timestamp("2026-02-24T22:00:00Z").to_pydatetime(),
        price_min=100.8,
        price_max=100.2,
        area_id="dp-test",
        metric="delta",
    )
    assert delta_area_profile["metric"] == "delta"
    assert delta_area_profile["profile"]["id"] == "dp-test"
    assert delta_area_profile["profile"]["total_value"] == 9.0
    assert delta_area_profile["profile"]["max_value"] == 4.0

    with pytest.raises(ValueError):
        ensure_preset_volume_profiles("NQH6", start, end, preset="day", profile_timezone="Mars/Phobos")
