from __future__ import annotations

from datetime import datetime

import pandas as pd

from app.config import get_settings
from app.db.duck import get_duckdb_connection, init_duckdb
from app.services.profile_backfill import run_preset_profile_backfill


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


def _seed_ticks_with_two_symbols() -> None:
    dataset_tz = get_settings().dataset_tz
    ts = pd.to_datetime(
        [
            # Week 9
            "2026-02-24T14:25:00Z",  # ETH
            "2026-02-24T14:35:00Z",  # RTH
            "2026-02-24T20:59:00Z",  # RTH
            "2026-02-24T21:05:00Z",  # ETH
            "2026-02-25T14:31:00Z",  # RTH
            "2026-02-25T21:01:00Z",  # ETH
            # Week 10
            "2026-03-02T20:00:00Z",  # RTH
            "2026-03-02T21:00:00Z",  # ETH
        ]
    )
    rows = pd.DataFrame(
        {
            "ts": ts,
            "session_date": ts.tz_convert(dataset_tz).date,
            "symbol_contract": ["NQH26"] * len(ts),
            "trade_price": [20000.0 + (i * 0.25) for i in range(len(ts))],
            "trade_size": [i + 1 for i in range(len(ts))],
            "bid_price": [19999.75 + (i * 0.25) for i in range(len(ts))],
            "ask_price": [20000.0 + (i * 0.25) for i in range(len(ts))],
            "source_file": ["fixture-backfill.csv"] * len(ts),
        }
    )
    _insert_ticks(rows)

    rows_other = pd.DataFrame(
        {
            "ts": [pd.Timestamp("2026-02-24T14:25:00Z")],
            "session_date": [datetime(2026, 2, 24).date()],
            "symbol_contract": ["NQH6"],
            "trade_price": [100.0],
            "trade_size": [1.0],
            "bid_price": [99.75],
            "ask_price": [100.0],
            "source_file": ["fixture-other.csv"],
        }
    )
    _insert_ticks(rows_other)


def test_init_duckdb_creates_preset_profile_tables_and_unique_indexes(temp_duckdb):
    init_duckdb()
    con = get_duckdb_connection()
    try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        assert "preset_profile_segments" in tables
        assert "preset_profile_levels" in tables

        con.execute(
            """
            INSERT INTO preset_profile_segments (
                symbol_contract, profile_timezone, metric, preset, tick_aggregation, tick_size,
                profile_id, label, segment_start, segment_end, max_value, total_value,
                value_area_enabled, value_area_percent, value_area_poc, value_area_low,
                value_area_high, value_area_volume, level_count
            ) VALUES (
                'NQH26', 'America/New_York', 'volume', 'day', 1, 0.25,
                'day-2026-02-24', 'Day 2026-02-24', TIMESTAMPTZ '2026-02-24 16:00:00-05:00',
                TIMESTAMPTZ '2026-02-25 16:00:00-05:00', 10.0, 50.0,
                false, 70.0, NULL, NULL, NULL, 0.0, 1
            )
            """
        )
        con.execute(
            """
            INSERT INTO preset_profile_levels (
                symbol_contract, profile_timezone, metric, preset, tick_aggregation, profile_id, price_level, value
            ) VALUES ('NQH26', 'America/New_York', 'volume', 'day', 1, 'day-2026-02-24', 20000.0, 5.0)
            """
        )

        try:
            con.execute(
                """
                INSERT INTO preset_profile_segments (
                    symbol_contract, profile_timezone, metric, preset, tick_aggregation, tick_size,
                    profile_id, label, segment_start, segment_end, max_value, total_value,
                    value_area_enabled, value_area_percent, value_area_poc, value_area_low,
                    value_area_high, value_area_volume, level_count
                ) VALUES (
                    'NQH26', 'America/New_York', 'volume', 'day', 1, 0.25,
                    'day-2026-02-24', 'Day 2026-02-24', TIMESTAMPTZ '2026-02-24 16:00:00-05:00',
                    TIMESTAMPTZ '2026-02-25 16:00:00-05:00', 10.0, 50.0,
                    false, 70.0, NULL, NULL, NULL, 0.0, 1
                )
                """
            )
            raise AssertionError("Expected duplicate segment insert to fail")
        except Exception:
            pass

        try:
            con.execute(
                """
                INSERT INTO preset_profile_levels (
                    symbol_contract, profile_timezone, metric, preset, tick_aggregation, profile_id, price_level, value
                ) VALUES ('NQH26', 'America/New_York', 'volume', 'day', 1, 'day-2026-02-24', 20000.0, 7.0)
                """
            )
            raise AssertionError("Expected duplicate level insert to fail")
        except Exception:
            pass
    finally:
        con.close()


def test_run_profile_backfill_persists_required_combos_and_levels(temp_duckdb):
    init_duckdb()
    _seed_ticks_with_two_symbols()

    result = run_preset_profile_backfill(
        symbols=["NQH26"],
        profile_timezone="America/New_York",
        full_rebuild=True,
    )
    assert result["status"] == "ok"
    assert result["symbols_processed"] == ["NQH26"]

    con = get_duckdb_connection()
    try:
        combos = set(
            con.execute(
                """
                SELECT metric, preset, tick_aggregation
                FROM preset_profile_segments
                WHERE symbol_contract = 'NQH26'
                GROUP BY metric, preset, tick_aggregation
                """
            ).fetchall()
        )
        expected = {
            ("volume", "week", 1),
            ("volume", "day", 1),
            ("volume", "rth", 1),
            ("volume", "eth", 1),
            ("volume", "week", 4),
            ("volume", "day", 4),
            ("volume", "rth", 4),
            ("volume", "eth", 4),
            ("delta", "day", 6),
            ("delta", "rth", 6),
            ("delta", "eth", 6),
        }
        assert combos == expected

        delta_week_rows = con.execute(
            """
            SELECT COUNT(*)
            FROM preset_profile_segments
            WHERE symbol_contract = 'NQH26' AND metric = 'delta' AND preset = 'week'
            """
        ).fetchone()[0]
        assert int(delta_week_rows) == 0

        mismatches = con.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT
                    s.symbol_contract,
                    s.profile_timezone,
                    s.metric,
                    s.preset,
                    s.tick_aggregation,
                    s.profile_id,
                    s.level_count,
                    COUNT(l.price_level) AS actual_levels
                FROM preset_profile_segments s
                LEFT JOIN preset_profile_levels l
                  ON s.symbol_contract = l.symbol_contract
                 AND s.profile_timezone = l.profile_timezone
                 AND s.metric = l.metric
                 AND s.preset = l.preset
                 AND s.tick_aggregation = l.tick_aggregation
                 AND s.profile_id = l.profile_id
                WHERE s.symbol_contract = 'NQH26'
                GROUP BY
                    s.symbol_contract,
                    s.profile_timezone,
                    s.metric,
                    s.preset,
                    s.tick_aggregation,
                    s.profile_id,
                    s.level_count
            ) q
            WHERE q.level_count <> q.actual_levels
            """
        ).fetchone()[0]
        assert int(mismatches) == 0

        duplicate_segments = con.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT
                    symbol_contract,
                    profile_timezone,
                    metric,
                    preset,
                    tick_aggregation,
                    profile_id,
                    COUNT(*) AS c
                FROM preset_profile_segments
                GROUP BY 1, 2, 3, 4, 5, 6
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
        assert int(duplicate_segments) == 0
    finally:
        con.close()


def test_run_profile_backfill_full_rebuild_replaces_stale_rows(temp_duckdb):
    init_duckdb()
    _seed_ticks_with_two_symbols()

    run_preset_profile_backfill(
        symbols=["NQH26"],
        profile_timezone="America/New_York",
        full_rebuild=True,
    )

    con = get_duckdb_connection()
    try:
        con.execute(
            """
            INSERT INTO preset_profile_segments (
                symbol_contract, profile_timezone, metric, preset, tick_aggregation, tick_size,
                profile_id, label, segment_start, segment_end, max_value, total_value,
                value_area_enabled, value_area_percent, value_area_poc, value_area_low,
                value_area_high, value_area_volume, level_count
            ) VALUES (
                'NQH26', 'America/New_York', 'volume', 'day', 1, 0.25,
                'stale-profile', 'Stale', TIMESTAMPTZ '2026-02-20 16:00:00-05:00',
                TIMESTAMPTZ '2026-02-21 16:00:00-05:00', 1.0, 1.0,
                false, 70.0, NULL, NULL, NULL, 0.0, 1
            )
            """
        )
        con.execute(
            """
            INSERT INTO preset_profile_levels (
                symbol_contract, profile_timezone, metric, preset, tick_aggregation, profile_id, price_level, value
            ) VALUES ('NQH26', 'America/New_York', 'volume', 'day', 1, 'stale-profile', 19999.0, 1.0)
            """
        )
    finally:
        con.close()

    run_preset_profile_backfill(
        symbols=["NQH26"],
        profile_timezone="America/New_York",
        full_rebuild=True,
    )

    con = get_duckdb_connection()
    try:
        stale_segments = con.execute(
            "SELECT COUNT(*) FROM preset_profile_segments WHERE profile_id = 'stale-profile'"
        ).fetchone()[0]
        stale_levels = con.execute(
            "SELECT COUNT(*) FROM preset_profile_levels WHERE profile_id = 'stale-profile'"
        ).fetchone()[0]
        assert int(stale_segments) == 0
        assert int(stale_levels) == 0
    finally:
        con.close()


def test_run_profile_backfill_respects_symbol_filter(temp_duckdb):
    init_duckdb()
    _seed_ticks_with_two_symbols()

    run_preset_profile_backfill(
        symbols=["NQH26"],
        profile_timezone="America/New_York",
        full_rebuild=True,
    )

    con = get_duckdb_connection()
    try:
        primary_rows = con.execute(
            "SELECT COUNT(*) FROM preset_profile_segments WHERE symbol_contract = 'NQH26'"
        ).fetchone()[0]
        other_rows = con.execute(
            "SELECT COUNT(*) FROM preset_profile_segments WHERE symbol_contract = 'NQH6'"
        ).fetchone()[0]
        assert int(primary_rows) > 0
        assert int(other_rows) == 0
    finally:
        con.close()
