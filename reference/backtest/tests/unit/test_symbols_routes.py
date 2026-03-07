from datetime import datetime

import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_symbols
from app.db.duck import get_duckdb_connection, init_duckdb


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


def _build_client() -> TestClient:
    app = FastAPI()
    app.include_router(routes_symbols.router)
    return TestClient(app)


def test_symbols_route_includes_symbols_with_only_bars(temp_duckdb):
    init_duckdb()
    ticks = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2026-02-24 08:30:00"]).tz_localize("America/Chicago"),
            "session_date": [datetime(2026, 2, 24).date()],
            "symbol_contract": ["NQH6"],
            "trade_price": [22000.0],
            "trade_size": [1.0],
            "bid_price": [21999.75],
            "ask_price": [22000.0],
            "source_file": ["fixture.csv"],
        }
    )
    bars = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2026-02-20 08:30:00", "2026-02-21 08:31:00"]).tz_localize("America/Chicago"),
            "session_date": [datetime(2026, 2, 20).date(), datetime(2026, 2, 21).date()],
            "timeframe": ["1m", "1m"],
            "symbol_contract": ["NQH26", "NQH26"],
            "open": [22000.0, 22001.0],
            "high": [22000.5, 22001.5],
            "low": [21999.5, 22000.5],
            "close": [22000.25, 22001.25],
            "volume": [10.0, 12.0],
            "trade_count": [2, 2],
        }
    )
    _insert_ticks(ticks)
    _insert_bars(bars)

    client = _build_client()
    response = client.get("/symbols")

    assert response.status_code == 200
    payload = response.json()
    by_symbol = {row["symbol_contract"]: row for row in payload}
    assert "NQH6" in by_symbol
    assert "NQH26" in by_symbol
    assert by_symbol["NQH6"]["tick_count"] == 1
    assert by_symbol["NQH26"]["tick_count"] == 0
    assert by_symbol["NQH26"]["bar_count"] == 2


def test_symbol_coverage_route_reports_weekday_gaps_only(temp_duckdb):
    init_duckdb()
    bars = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2026-02-23 08:30:00", "2026-02-25 08:31:00"]).tz_localize("America/Chicago"),
            "session_date": [datetime(2026, 2, 23).date(), datetime(2026, 2, 25).date()],
            "timeframe": ["1m", "1m"],
            "symbol_contract": ["NQM6", "NQM6"],
            "open": [22000.0, 22001.0],
            "high": [22000.5, 22001.5],
            "low": [21999.5, 22000.5],
            "close": [22000.25, 22001.25],
            "volume": [10.0, 12.0],
            "trade_count": [2, 2],
        }
    )
    _insert_bars(bars)

    client = _build_client()
    response = client.get("/symbols/NQM6/coverage")

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol_contract"] == "NQM6"
    assert payload["session_count"] == 2
    assert payload["missing_weekday_count"] == 1
    assert payload["missing_weekday_dates"] == ["2026-02-24"]


def test_symbol_coverage_route_ignores_weekend_gap_and_404_for_unknown(temp_duckdb):
    init_duckdb()
    ticks = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2026-02-27 14:30:00", "2026-03-02 14:31:00"]).tz_localize("America/Chicago"),
            "session_date": [datetime(2026, 2, 27).date(), datetime(2026, 3, 2).date()],
            "symbol_contract": ["ESH6", "ESH6"],
            "trade_price": [6100.0, 6101.0],
            "trade_size": [2.0, 2.0],
            "bid_price": [6099.75, 6100.75],
            "ask_price": [6100.0, 6101.0],
            "source_file": ["fixture.csv", "fixture.csv"],
        }
    )
    _insert_ticks(ticks)

    client = _build_client()
    coverage = client.get("/symbols/ESH6/coverage")
    assert coverage.status_code == 200
    assert coverage.json()["missing_weekday_count"] == 0

    unknown = client.get("/symbols/UNKNOWN/coverage")
    assert unknown.status_code == 404
