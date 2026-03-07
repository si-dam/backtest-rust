from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd

from app.config import get_settings
from app.db.duck import get_duckdb_connection
from app.services.aggregation import ensure_bars, ensure_session_profile, load_ticks


class DuckDBMarketDataStore:
    def get_bars(self, symbol_contract: str, timeframe: str, start: datetime, end: datetime) -> pd.DataFrame:
        return ensure_bars(symbol_contract=symbol_contract, timeframe=timeframe, start=start, end=end)

    def get_ticks(self, symbol_contract: str, start: datetime, end: datetime) -> pd.DataFrame:
        return load_ticks(symbol_contract=symbol_contract, start=start, end=end)

    def get_session_profile(self, symbol_contract: str, session_date: date | str) -> pd.DataFrame:
        if isinstance(session_date, date):
            session_date = session_date.isoformat()
        return ensure_session_profile(symbol_contract=symbol_contract, session_date=session_date)


def get_symbol_coverage(symbol_contract: str) -> dict | None:
    con = get_duckdb_connection()
    try:
        bounds = con.execute(
            """
            WITH unioned AS (
                SELECT ts, session_date
                FROM ticks
                WHERE symbol_contract = ?
                UNION ALL
                SELECT ts, session_date
                FROM bars
                WHERE symbol_contract = ?
            )
            SELECT
                min(ts) AS first_ts,
                max(ts) AS last_ts,
                count(DISTINCT session_date) AS session_count
            FROM unioned
            """,
            [symbol_contract, symbol_contract],
        ).fetchone()
        if not bounds:
            return None

        first_ts = bounds[0]
        last_ts = bounds[1]
        session_count = int(bounds[2] or 0)
        if first_ts is None or last_ts is None or session_count == 0:
            return None

        session_rows = con.execute(
            """
            WITH unioned AS (
                SELECT session_date
                FROM ticks
                WHERE symbol_contract = ?
                UNION
                SELECT session_date
                FROM bars
                WHERE symbol_contract = ?
            )
            SELECT session_date
            FROM unioned
            ORDER BY session_date
            """,
            [symbol_contract, symbol_contract],
        ).fetchall()
    finally:
        con.close()

    session_dates = [row[0] for row in session_rows if row and row[0] is not None]
    if not session_dates:
        return None

    min_session = min(session_dates)
    max_session = max(session_dates)
    observed = {d for d in session_dates}

    missing_weekday_dates: list[str] = []
    for day in pd.date_range(min_session, max_session, freq="D"):
        day_date = day.date()
        if day.weekday() >= 5:
            continue
        if day_date not in observed:
            missing_weekday_dates.append(day_date.isoformat())

    dataset_tz = ZoneInfo(get_settings().dataset_tz)
    first_ts_value = pd.Timestamp(first_ts)
    if first_ts_value.tzinfo is None:
        first_ts_value = first_ts_value.tz_localize("UTC")
    last_ts_value = pd.Timestamp(last_ts)
    if last_ts_value.tzinfo is None:
        last_ts_value = last_ts_value.tz_localize("UTC")
    first_iso = first_ts_value.tz_convert(dataset_tz).isoformat()
    last_iso = last_ts_value.tz_convert(dataset_tz).isoformat()

    return {
        "symbol_contract": symbol_contract,
        "first_ts": first_iso,
        "last_ts": last_iso,
        "session_count": int(session_count),
        "missing_weekday_dates": missing_weekday_dates,
        "missing_weekday_count": len(missing_weekday_dates),
        "timezone": get_settings().dataset_tz,
    }
