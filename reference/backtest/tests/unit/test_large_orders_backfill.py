from __future__ import annotations

from datetime import datetime

import pandas as pd

from app.db.duck import get_duckdb_connection, init_duckdb
from app.services.large_orders_backfill import run_large_orders_backfill


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


def _count_rows(sql: str, params: list | None = None) -> int:
    con = get_duckdb_connection()
    try:
        row = con.execute(sql, params or []).fetchone()
        return int(row[0]) if row else 0
    finally:
        con.close()


def test_large_orders_backfill_marks_sessions_and_is_incremental(temp_duckdb):
    init_duckdb()
    rows = pd.DataFrame(
        {
            "ts": pd.to_datetime(
                [
                    "2026-02-24 08:30:00",
                    "2026-02-24 08:30:15",
                    "2026-02-25 08:30:00",
                    "2026-02-25 08:30:15",
                ]
            ).tz_localize("America/Chicago"),
            "session_date": [
                datetime(2026, 2, 24).date(),
                datetime(2026, 2, 24).date(),
                datetime(2026, 2, 25).date(),
                datetime(2026, 2, 25).date(),
            ],
            "symbol_contract": ["NQH26"] * 4,
            "trade_price": [22000.0, 22000.25, 22010.0, 22010.25],
            "trade_size": [5.0, 10.0, 22.0, 30.0],
            "bid_price": [21999.75, 22000.0, 22009.75, 22010.0],
            "ask_price": [22000.0, 22000.25, 22010.0, 22010.25],
            "source_file": ["fixture-large-backfill.csv"] * 4,
        }
    )
    _insert_ticks(rows)

    first = run_large_orders_backfill(symbols=["NQH26"], threshold=20.0)
    assert first["status"] == "ok"
    assert first["sessions_processed"] == 2
    assert first["sessions_skipped"] == 0
    assert first["inserted_orders"] == 2
    assert first["symbol_stats"]["NQH26"]["sessions_processed"] == 2

    state_rows = _count_rows(
        """
        SELECT COUNT(*)
        FROM large_order_backfill_state
        WHERE symbol_contract = ? AND method = 'fixed' AND threshold = 20.0
        """,
        ["NQH26"],
    )
    assert state_rows == 2

    zero_marker = _count_rows(
        """
        SELECT COUNT(*)
        FROM large_order_backfill_state
        WHERE symbol_contract = ? AND session_date = DATE '2026-02-24' AND row_count = 0
        """,
        ["NQH26"],
    )
    assert zero_marker == 1

    second = run_large_orders_backfill(symbols=["NQH26"], threshold=20.0)
    assert second["sessions_processed"] == 0
    assert second["sessions_skipped"] == 2
    assert second["inserted_orders"] == 0

    more_rows = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2026-02-26 08:30:00", "2026-02-26 08:30:15"]).tz_localize("America/Chicago"),
            "session_date": [datetime(2026, 2, 26).date(), datetime(2026, 2, 26).date()],
            "symbol_contract": ["NQH26", "NQH26"],
            "trade_price": [22020.0, 22020.25],
            "trade_size": [19.0, 21.0],
            "bid_price": [22019.75, 22020.0],
            "ask_price": [22020.0, 22020.25],
            "source_file": ["fixture-large-backfill-2.csv"] * 2,
        }
    )
    _insert_ticks(more_rows)

    third = run_large_orders_backfill(symbols=["NQH26"], threshold=20.0)
    assert third["sessions_processed"] == 1
    assert third["sessions_skipped"] == 2
    assert third["inserted_orders"] == 1


def test_large_orders_backfill_unknown_symbol_is_skipped(temp_duckdb):
    init_duckdb()
    result = run_large_orders_backfill(symbols=["UNKNOWN_SYMBOL"], threshold=20.0)
    assert result["status"] == "ok"
    assert result["symbols_processed"] == []
    assert result["symbol_stats"]["UNKNOWN_SYMBOL"]["status"] == "skipped"
    assert result["symbol_stats"]["UNKNOWN_SYMBOL"]["reason"] == "no_ticks"
