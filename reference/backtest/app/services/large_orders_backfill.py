from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd

from app.db.duck import get_duckdb_connection
from app.services.aggregation import ensure_large_orders

_BACKFILL_METHOD = "fixed"
_BACKFILL_PERCENTILE = 99.0


@dataclass(frozen=True)
class SessionWindow:
    session_date: date
    start_ts: datetime
    end_ts: datetime


def _normalize_symbols(symbols: list[str]) -> list[str]:
    normalized = [str(symbol).strip() for symbol in symbols if str(symbol).strip()]
    deduped: list[str] = []
    seen: set[str] = set()
    for symbol in normalized:
        key = symbol.upper()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(symbol)
    if not deduped:
        raise ValueError("symbols must contain at least one non-empty symbol_contract")
    return deduped


def _list_symbol_windows(symbol_contract: str) -> list[SessionWindow]:
    con = get_duckdb_connection()
    try:
        rows = con.execute(
            """
            SELECT session_date, min(ts) AS start_ts, max(ts) AS end_ts
            FROM ticks
            WHERE symbol_contract = ?
            GROUP BY session_date
            ORDER BY session_date
            """,
            [symbol_contract],
        ).fetchall()
    finally:
        con.close()

    out: list[SessionWindow] = []
    for session_date, start_ts, end_ts in rows:
        if session_date is None or start_ts is None or end_ts is None:
            continue
        out.append(
            SessionWindow(
                session_date=session_date,
                start_ts=pd.Timestamp(start_ts).to_pydatetime(),
                end_ts=pd.Timestamp(end_ts).to_pydatetime(),
            )
        )
    return out


def _list_processed_session_dates(symbol_contract: str, method: str, threshold: float) -> set[date]:
    con = get_duckdb_connection()
    try:
        rows = con.execute(
            """
            SELECT session_date
            FROM large_order_backfill_state
            WHERE symbol_contract = ?
              AND method = ?
              AND threshold = ?
            """,
            [symbol_contract, method, float(threshold)],
        ).fetchall()
    finally:
        con.close()

    return {row[0] for row in rows if row and row[0] is not None}


def _upsert_backfill_state(
    *,
    symbol_contract: str,
    session_date: date,
    method: str,
    threshold: float,
    row_count: int,
) -> None:
    con = get_duckdb_connection()
    try:
        con.execute(
            """
            DELETE FROM large_order_backfill_state
            WHERE symbol_contract = ?
              AND session_date = ?
              AND method = ?
              AND threshold = ?
            """,
            [symbol_contract, session_date, method, float(threshold)],
        )
        con.execute(
            """
            INSERT INTO large_order_backfill_state (
                symbol_contract,
                session_date,
                method,
                threshold,
                processed_at,
                row_count
            )
            VALUES (?, ?, ?, ?, now(), ?)
            """,
            [symbol_contract, session_date, method, float(threshold), int(row_count)],
        )
    finally:
        con.close()


def run_large_orders_backfill(symbols: list[str], threshold: float = 20.0) -> dict:
    normalized_symbols = _normalize_symbols(symbols)
    fixed_threshold = float(threshold)
    if fixed_threshold <= 0:
        raise ValueError("threshold must be > 0")

    symbol_stats: dict[str, dict] = {}
    total_sessions = 0
    total_processed = 0
    total_skipped = 0
    total_inserted_orders = 0

    for symbol_contract in normalized_symbols:
        windows = _list_symbol_windows(symbol_contract)
        if not windows:
            symbol_stats[symbol_contract] = {
                "status": "skipped",
                "reason": "no_ticks",
                "sessions_total": 0,
                "sessions_processed": 0,
                "sessions_skipped": 0,
                "inserted_orders": 0,
            }
            continue

        processed_dates = _list_processed_session_dates(symbol_contract, _BACKFILL_METHOD, fixed_threshold)
        symbol_processed = 0
        symbol_skipped = 0
        symbol_inserted_orders = 0

        for window in windows:
            if window.session_date in processed_dates:
                symbol_skipped += 1
                continue

            large_orders = ensure_large_orders(
                symbol_contract=symbol_contract,
                start=window.start_ts,
                end=window.end_ts,
                method=_BACKFILL_METHOD,
                fixed_threshold=fixed_threshold,
                percentile=_BACKFILL_PERCENTILE,
                persist=True,
            )
            row_count = int(len(large_orders))
            _upsert_backfill_state(
                symbol_contract=symbol_contract,
                session_date=window.session_date,
                method=_BACKFILL_METHOD,
                threshold=fixed_threshold,
                row_count=row_count,
            )
            symbol_processed += 1
            symbol_inserted_orders += row_count

        symbol_stats[symbol_contract] = {
            "status": "backfilled",
            "sessions_total": int(len(windows)),
            "sessions_processed": int(symbol_processed),
            "sessions_skipped": int(symbol_skipped),
            "inserted_orders": int(symbol_inserted_orders),
            "start_session": str(windows[0].session_date),
            "end_session": str(windows[-1].session_date),
        }

        total_sessions += len(windows)
        total_processed += symbol_processed
        total_skipped += symbol_skipped
        total_inserted_orders += symbol_inserted_orders

    return {
        "status": "ok",
        "symbols_requested": normalized_symbols,
        "method": _BACKFILL_METHOD,
        "threshold": fixed_threshold,
        "symbols_processed": [symbol for symbol, row in symbol_stats.items() if row["status"] == "backfilled"],
        "symbol_stats": symbol_stats,
        "sessions_total": int(total_sessions),
        "sessions_processed": int(total_processed),
        "sessions_skipped": int(total_skipped),
        "inserted_orders": int(total_inserted_orders),
    }
