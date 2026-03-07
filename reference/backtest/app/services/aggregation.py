from __future__ import annotations

import copy
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from app.config import get_settings
from app.db.duck import get_duckdb_connection
from app.services.market_processing import (
    TIMEFRAME_RULES,
    _append_bar_row,
    build_bars_from_ticks,
    build_delta_profile_levels,
    build_session_profile,
    build_volume_profile_levels,
    detect_large_orders,
    resolve_symbol_tick_size,
)
from app.services.runtime_cache import TTLCache


BAR_COLUMNS = ["ts", "session_date", "timeframe", "symbol_contract", "open", "high", "low", "close", "volume", "trade_count"]
PROFILE_METRICS = {"volume", "delta"}
NON_TIME_BAR_PRESETS: tuple[tuple[str, int], ...] = (
    ("tick", 1500),
    ("volume", 500),
    ("volume", 750),
    ("volume", 1000),
    ("range", 40),
)
NON_TIME_BAR_CHUNK_DAYS = 1
EXCHANGE_SESSION_TZ = ZoneInfo("America/New_York")
RTH_SESSION_START_MINUTES = (9 * 60) + 30
RTH_SESSION_END_MINUTES = 16 * 60
DAY_PROFILE_START_MINUTES = 16 * 60

_TICK_CACHE = TTLCache(ttl_seconds=10.0, max_entries=64)
_PROFILE_WINDOW_CACHE = TTLCache(ttl_seconds=10.0, max_entries=64)
_PROFILE_PRESET_CACHE = TTLCache(ttl_seconds=15.0, max_entries=160)
_PROFILE_AREA_CACHE = TTLCache(ttl_seconds=15.0, max_entries=256)
_LARGE_ORDERS_CACHE = TTLCache(ttl_seconds=10.0, max_entries=160)


def _empty_bars_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=BAR_COLUMNS)


def non_time_bar_timeframe(bar_type: str, bar_size: int) -> str:
    normalized_bar_type = str(bar_type or "").strip().lower()
    normalized_bar_size = int(bar_size)
    if normalized_bar_type not in {"tick", "volume", "range"}:
        raise ValueError(f"Unsupported non-time bar_type: {bar_type}")
    if normalized_bar_size < 1:
        raise ValueError("bar_size must be >= 1")
    return f"{normalized_bar_type}:{normalized_bar_size}"


def parse_non_time_preset_key(preset_key: str) -> tuple[str, int]:
    normalized = str(preset_key or "").strip().lower()
    try:
        bar_type, raw_size = normalized.split(":", 1)
    except ValueError as exc:
        raise ValueError(f"Invalid preset key: {preset_key}") from exc
    bar_size = int(raw_size)
    timeframe = non_time_bar_timeframe(bar_type=bar_type, bar_size=bar_size)
    bar_type_parsed, bar_size_parsed = timeframe.split(":", 1)
    return bar_type_parsed, int(bar_size_parsed)


def resolve_non_time_presets(preset_keys: list[str] | tuple[str, ...] | None = None) -> tuple[tuple[str, int], ...]:
    if not preset_keys:
        return NON_TIME_BAR_PRESETS
    resolved = [parse_non_time_preset_key(key) for key in preset_keys]
    # Preserve caller order while removing duplicates.
    deduped = dict.fromkeys(resolved)
    return tuple(deduped.keys())


def _cache_db_namespace() -> str:
    return str(get_settings().duckdb_path)


def _dt_cache_token(value: datetime | None) -> str:
    if value is None:
        return ""
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(_dataset_zone())
    return stamp.isoformat()


def _zone_key(zone: ZoneInfo) -> str:
    return getattr(zone, "key", str(zone))


def clear_runtime_caches() -> None:
    _TICK_CACHE.clear()
    _PROFILE_WINDOW_CACHE.clear()
    _PROFILE_PRESET_CACHE.clear()
    _PROFILE_AREA_CACHE.clear()
    _LARGE_ORDERS_CACHE.clear()


def invalidate_runtime_symbol_caches(symbol_contract: str) -> None:
    symbol = str(symbol_contract)
    db_key = _cache_db_namespace()

    def _match(key) -> bool:
        return isinstance(key, tuple) and len(key) >= 3 and key[1] == db_key and key[2] == symbol

    _TICK_CACHE.invalidate(_match)
    _PROFILE_WINDOW_CACHE.invalidate(_match)
    _PROFILE_PRESET_CACHE.invalidate(_match)
    _PROFILE_AREA_CACHE.invalidate(_match)
    _LARGE_ORDERS_CACHE.invalidate(_match)


def _timeframe_to_seconds(timeframe: str) -> int:
    value = timeframe.strip().lower()
    if value.endswith("s"):
        return int(value[:-1])
    if value.endswith("m"):
        return int(value[:-1]) * 60
    if value.endswith("h"):
        return int(value[:-1]) * 3600
    if value.endswith("d"):
        return int(value[:-1]) * 86400
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _lower_timeframes(target_timeframe: str) -> list[str]:
    target_seconds = _timeframe_to_seconds(target_timeframe)
    candidates = [tf for tf in TIMEFRAME_RULES if _timeframe_to_seconds(tf) < target_seconds]
    return sorted(candidates, key=_timeframe_to_seconds, reverse=True)


def _build_bars_from_existing_bars(source_bars: pd.DataFrame, timeframe: str, dataset_tz: str) -> pd.DataFrame:
    rule = TIMEFRAME_RULES.get(timeframe)
    if not rule:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    if source_bars.empty:
        return _empty_bars_frame()

    df = source_bars.copy()
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df = df.dropna(subset=["ts"]).sort_values("ts").set_index("ts")
    if df.empty:
        return _empty_bars_frame()

    rows: list[pd.DataFrame] = []
    for symbol, group in df.groupby("symbol_contract"):
        agg = group.resample(rule).agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            trade_count=("trade_count", "sum"),
        )
        agg = agg.dropna(subset=["open", "high", "low", "close"]).reset_index()
        agg["symbol_contract"] = symbol
        agg["timeframe"] = timeframe
        agg["session_date"] = agg["ts"].dt.tz_convert(ZoneInfo(dataset_tz)).dt.date
        rows.append(agg)

    if not rows:
        return _empty_bars_frame()
    return pd.concat(rows, ignore_index=True)[BAR_COLUMNS]


def _rebuild_from_lower_timeframe_bars(
    symbol_contract: str,
    timeframe: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    dataset_tz = get_settings().dataset_tz
    for source_timeframe in _lower_timeframes(timeframe):
        source_bars = load_bars(symbol_contract=symbol_contract, timeframe=source_timeframe, start=start, end=end)
        if source_bars.empty:
            continue
        rebuilt = _build_bars_from_existing_bars(source_bars=source_bars, timeframe=timeframe, dataset_tz=dataset_tz)
        if rebuilt.empty:
            continue
        upsert_bars(rebuilt, symbol_contract=symbol_contract, timeframe=timeframe, start=start, end=end)
        return rebuilt
    return _empty_bars_frame()


def _normalize_ticks_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    parsed_ts = pd.to_datetime(df["ts"], errors="coerce")
    if parsed_ts.dt.tz is None:
        parsed_ts = parsed_ts.dt.tz_localize("UTC")
    out = df.copy()
    out["ts"] = parsed_ts.dt.tz_convert(get_settings().dataset_tz)
    return out


def load_ticks(symbol_contract: str, start: datetime | None = None, end: datetime | None = None) -> pd.DataFrame:
    cache_key = (
        "ticks",
        _cache_db_namespace(),
        str(symbol_contract),
        _dt_cache_token(start),
        _dt_cache_token(end),
    )
    cached = _TICK_CACHE.get(cache_key)
    if cached is not None:
        return cached.copy(deep=True)

    con = get_duckdb_connection()
    try:
        query = """
            SELECT ts, session_date, symbol_contract, trade_price, trade_size, bid_price, ask_price
            FROM ticks
            WHERE symbol_contract = ?
        """
        params: list = [symbol_contract]
        if start is not None:
            query += " AND ts >= ?"
            params.append(start)
        if end is not None:
            query += " AND ts <= ?"
            params.append(end)
        query += " ORDER BY ts"
        df = con.execute(query, params).fetch_df()
    finally:
        con.close()

    normalized = _normalize_ticks_df(df)
    _TICK_CACHE.set(cache_key, normalized.copy(deep=True))
    return normalized


def upsert_bars(df: pd.DataFrame, symbol_contract: str, timeframe: str, start: datetime, end: datetime) -> None:
    con = get_duckdb_connection()
    try:
        con.execute(
            """
            DELETE FROM bars
            WHERE symbol_contract = ?
              AND timeframe = ?
              AND ts >= ?
              AND ts <= ?
            """,
            [symbol_contract, timeframe, start, end],
        )
        if not df.empty:
            con.register("bars_df", df)
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
    invalidate_runtime_symbol_caches(symbol_contract)


def load_bars(symbol_contract: str, timeframe: str, start: datetime, end: datetime) -> pd.DataFrame:
    con = get_duckdb_connection()
    try:
        df = con.execute(
            """
            SELECT ts, session_date, timeframe, symbol_contract, open, high, low, close, volume, trade_count
            FROM bars
            WHERE symbol_contract = ?
              AND timeframe = ?
              AND ts >= ?
              AND ts <= ?
            ORDER BY ts
            """,
            [symbol_contract, timeframe, start, end],
        ).fetch_df()
    finally:
        con.close()

    if df.empty:
        return df

    # Guard against overlapping range upserts that can transiently leave duplicate rows.
    df = df.sort_values("ts").drop_duplicates(subset=["ts", "symbol_contract", "timeframe"], keep="last")

    parsed_ts = pd.to_datetime(df["ts"], errors="coerce")
    if parsed_ts.dt.tz is None:
        parsed_ts = parsed_ts.dt.tz_localize("UTC")
    df["ts"] = parsed_ts.dt.tz_convert(get_settings().dataset_tz)
    return df


def _replace_bars_for_symbol_timeframe(symbol_contract: str, timeframe: str, bars: pd.DataFrame) -> None:
    con = get_duckdb_connection()
    try:
        con.execute(
            """
            DELETE FROM bars
            WHERE symbol_contract = ?
              AND timeframe = ?
            """,
            [symbol_contract, timeframe],
        )
        if not bars.empty:
            con.register("bars_df", bars)
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
    invalidate_runtime_symbol_caches(symbol_contract)


def _latest_tick_ts(symbol_contract: str) -> datetime | None:
    con = get_duckdb_connection()
    try:
        row = con.execute(
            """
            SELECT MAX(ts)
            FROM ticks
            WHERE symbol_contract = ?
            """,
            [symbol_contract],
        ).fetchone()
    finally:
        con.close()
    if not row or row[0] is None:
        return None
    stamp = pd.Timestamp(row[0])
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    return stamp.tz_convert(get_settings().dataset_tz).to_pydatetime()


def _latest_bar_ts(symbol_contract: str, timeframe: str) -> datetime | None:
    con = get_duckdb_connection()
    try:
        row = con.execute(
            """
            SELECT MAX(ts)
            FROM bars
            WHERE symbol_contract = ?
              AND timeframe = ?
            """,
            [symbol_contract, timeframe],
        ).fetchone()
    finally:
        con.close()
    if not row or row[0] is None:
        return None
    stamp = pd.Timestamp(row[0])
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    return stamp.tz_convert(get_settings().dataset_tz).to_pydatetime()


def _symbol_tick_bounds(symbol_contract: str) -> tuple[datetime | None, datetime | None]:
    con = get_duckdb_connection()
    try:
        row = con.execute(
            """
            SELECT MIN(ts), MAX(ts)
            FROM ticks
            WHERE symbol_contract = ?
            """,
            [symbol_contract],
        ).fetchone()
    finally:
        con.close()
    if not row or row[0] is None or row[1] is None:
        return None, None

    min_ts = pd.Timestamp(row[0])
    max_ts = pd.Timestamp(row[1])
    if min_ts.tzinfo is None:
        min_ts = min_ts.tz_localize("UTC")
    if max_ts.tzinfo is None:
        max_ts = max_ts.tz_localize("UTC")
    zone = ZoneInfo(get_settings().dataset_tz)
    return min_ts.tz_convert(zone).to_pydatetime(), max_ts.tz_convert(zone).to_pydatetime()


def _load_ticks_range(symbol_contract: str, start: datetime, end: datetime) -> pd.DataFrame:
    con = get_duckdb_connection()
    try:
        df = con.execute(
            """
            SELECT ts, session_date, symbol_contract, trade_price, trade_size, bid_price, ask_price
            FROM ticks
            WHERE symbol_contract = ?
              AND ts >= ?
              AND ts <= ?
            ORDER BY ts
            """,
            [symbol_contract, start, end],
        ).fetch_df()
    finally:
        con.close()
    return _normalize_ticks_df(df)


def _iter_symbol_tick_chunks(symbol_contract: str, *, chunk_days: int = NON_TIME_BAR_CHUNK_DAYS):
    min_ts, max_ts = _symbol_tick_bounds(symbol_contract=symbol_contract)
    if min_ts is None or max_ts is None:
        return

    window = timedelta(days=max(1, int(chunk_days)))
    cursor = pd.Timestamp(min_ts)
    stop = pd.Timestamp(max_ts)
    edge = pd.Timedelta(microseconds=1)
    while cursor <= stop:
        chunk_end = min(cursor + window - edge, stop)
        chunk = _load_ticks_range(
            symbol_contract=symbol_contract,
            start=cursor.to_pydatetime(),
            end=chunk_end.to_pydatetime(),
        )
        if not chunk.empty:
            yield chunk
        cursor = chunk_end + edge


def _build_non_time_bars_chunked(
    *,
    symbol_contract: str,
    bar_type: str,
    bar_size: int,
    dataset_tz: str,
    chunk_days: int = NON_TIME_BAR_CHUNK_DAYS,
) -> pd.DataFrame:
    timeframe_value = non_time_bar_timeframe(bar_type=bar_type, bar_size=bar_size)
    normalized_bar_type = str(bar_type).strip().lower()
    size = int(bar_size)
    if size < 1:
        raise ValueError("bar_size must be >= 1")

    dataset_zone = ZoneInfo(dataset_tz)
    rows: list[dict] = []
    tick_state = {"prices": [], "qtys": [], "ts": []}
    volume_state = {"open": None, "high": None, "low": None, "close": None, "volume": 0.0, "count": 0, "ts": None}
    range_state = {"open": None, "high": None, "low": None, "close": None, "volume": 0.0, "count": 0, "ts": None}
    range_span = resolve_symbol_tick_size(symbol_contract=symbol_contract, fallback=0.25) * size

    for chunk in _iter_symbol_tick_chunks(symbol_contract=symbol_contract, chunk_days=chunk_days):
        prices = pd.to_numeric(chunk["trade_price"], errors="coerce").to_numpy(dtype=float)
        qtys = pd.to_numeric(chunk["trade_size"], errors="coerce").to_numpy(dtype=float)
        ts_values = pd.to_datetime(chunk["ts"], errors="coerce")
        if ts_values.dt.tz is None:
            ts_values = ts_values.dt.tz_localize("UTC")
        ts_list = ts_values.tolist()

        if normalized_bar_type == "tick":
            all_prices = [*tick_state["prices"], *prices.tolist()]
            all_qtys = [*tick_state["qtys"], *qtys.tolist()]
            all_ts = [*tick_state["ts"], *[pd.Timestamp(value) for value in ts_list]]
            n = len(all_prices)
            consumed = 0
            for stop_idx in range(size, n + 1, size):
                start_idx = stop_idx - size
                segment_prices = all_prices[start_idx:stop_idx]
                segment_qtys = all_qtys[start_idx:stop_idx]
                close_ts = all_ts[stop_idx - 1]
                _append_bar_row(
                    rows,
                    ts=close_ts,
                    symbol_contract=symbol_contract,
                    timeframe=timeframe_value,
                    open_price=float(segment_prices[0]),
                    high_price=float(np.max(segment_prices)),
                    low_price=float(np.min(segment_prices)),
                    close_price=float(segment_prices[-1]),
                    volume=float(np.sum(segment_qtys)),
                    trade_count=int(size),
                    dataset_zone=dataset_zone,
                )
                consumed = stop_idx
            tick_state["prices"] = all_prices[consumed:]
            tick_state["qtys"] = all_qtys[consumed:]
            tick_state["ts"] = all_ts[consumed:]
            continue

        for idx in range(len(prices)):
            price = float(prices[idx])
            qty = float(qtys[idx])
            current_ts = pd.Timestamp(ts_list[idx])
            state = volume_state if normalized_bar_type == "volume" else range_state
            if state["open"] is None:
                state["open"] = state["high"] = state["low"] = state["close"] = price
                state["volume"] = qty
                state["count"] = 1
                state["ts"] = current_ts
            else:
                state["high"] = max(float(state["high"]), price)
                state["low"] = min(float(state["low"]), price)
                state["close"] = price
                state["volume"] += qty
                state["count"] += 1
                state["ts"] = current_ts

            if normalized_bar_type == "volume":
                if float(state["volume"]) >= size:
                    _append_bar_row(
                        rows,
                        ts=state["ts"],
                        symbol_contract=symbol_contract,
                        timeframe=timeframe_value,
                        open_price=float(state["open"]),
                        high_price=float(state["high"]),
                        low_price=float(state["low"]),
                        close_price=float(state["close"]),
                        volume=float(state["volume"]),
                        trade_count=int(state["count"]),
                        dataset_zone=dataset_zone,
                    )
                    state["open"] = state["high"] = state["low"] = state["close"] = None
                    state["volume"] = 0.0
                    state["count"] = 0
                    state["ts"] = None
            else:
                fixed_close = None
                if float(state["close"]) >= (float(state["open"]) + range_span):
                    fixed_close = float(state["open"]) + range_span
                elif float(state["close"]) <= (float(state["open"]) - range_span):
                    fixed_close = float(state["open"]) - range_span
                if fixed_close is not None:
                    _append_bar_row(
                        rows,
                        ts=state["ts"],
                        symbol_contract=symbol_contract,
                        timeframe=timeframe_value,
                        open_price=float(state["open"]),
                        high_price=max(float(state["high"]), fixed_close),
                        low_price=min(float(state["low"]), fixed_close),
                        close_price=fixed_close,
                        volume=float(state["volume"]),
                        trade_count=int(state["count"]),
                        dataset_zone=dataset_zone,
                    )
                    state["open"] = state["high"] = state["low"] = state["close"] = fixed_close
                    state["volume"] = 0.0
                    state["count"] = 0
                    state["ts"] = current_ts

    if normalized_bar_type == "tick":
        if tick_state["prices"]:
            _append_bar_row(
                rows,
                ts=tick_state["ts"][-1],
                symbol_contract=symbol_contract,
                timeframe=timeframe_value,
                open_price=float(tick_state["prices"][0]),
                high_price=float(np.max(tick_state["prices"])),
                low_price=float(np.min(tick_state["prices"])),
                close_price=float(tick_state["prices"][-1]),
                volume=float(np.sum(tick_state["qtys"])),
                trade_count=int(len(tick_state["prices"])),
                dataset_zone=dataset_zone,
            )
    elif normalized_bar_type == "volume":
        if volume_state["count"] > 0:
            _append_bar_row(
                rows,
                ts=volume_state["ts"],
                symbol_contract=symbol_contract,
                timeframe=timeframe_value,
                open_price=float(volume_state["open"]),
                high_price=float(volume_state["high"]),
                low_price=float(volume_state["low"]),
                close_price=float(volume_state["close"]),
                volume=float(volume_state["volume"]),
                trade_count=int(volume_state["count"]),
                dataset_zone=dataset_zone,
            )
    elif normalized_bar_type == "range":
        if range_state["count"] > 0:
            _append_bar_row(
                rows,
                ts=range_state["ts"],
                symbol_contract=symbol_contract,
                timeframe=timeframe_value,
                open_price=float(range_state["open"]),
                high_price=float(range_state["high"]),
                low_price=float(range_state["low"]),
                close_price=float(range_state["close"]),
                volume=float(range_state["volume"]),
                trade_count=int(range_state["count"]),
                dataset_zone=dataset_zone,
            )
    else:
        raise ValueError(f"Unsupported non-time bar_type: {bar_type}")

    if not rows:
        return _empty_bars_frame()
    return pd.DataFrame(rows, columns=BAR_COLUMNS)


def _recompute_non_time_timeframe(
    *,
    symbol_contract: str,
    bar_type: str,
    bar_size: int,
    chunk_days: int = NON_TIME_BAR_CHUNK_DAYS,
) -> pd.DataFrame:
    timeframe_value = non_time_bar_timeframe(bar_type=bar_type, bar_size=bar_size)
    bars = _build_non_time_bars_chunked(
        symbol_contract=symbol_contract,
        bar_type=bar_type,
        bar_size=bar_size,
        dataset_tz=get_settings().dataset_tz,
        chunk_days=chunk_days,
    )
    _replace_bars_for_symbol_timeframe(symbol_contract=symbol_contract, timeframe=timeframe_value, bars=bars)
    return bars


def _non_time_timeframe_stale(symbol_contract: str, timeframe: str) -> bool:
    latest_tick = _latest_tick_ts(symbol_contract=symbol_contract)
    if latest_tick is None:
        return False
    latest_bar = _latest_bar_ts(symbol_contract=symbol_contract, timeframe=timeframe)
    if latest_bar is None:
        return True
    return pd.Timestamp(latest_bar) < pd.Timestamp(latest_tick)


def _has_ticks_outside_cached_bars(
    symbol_contract: str,
    start: datetime,
    end: datetime,
    *,
    first_bar_ts: datetime,
    last_bar_ts: datetime,
) -> bool:
    con = get_duckdb_connection()
    try:
        row = con.execute(
            """
            SELECT 1
            FROM ticks
            WHERE symbol_contract = ?
              AND ts >= ?
              AND ts <= ?
              AND (ts < ? OR ts > ?)
            LIMIT 1
            """,
            [symbol_contract, start, end, first_bar_ts, last_bar_ts],
        ).fetchone()
        return row is not None
    finally:
        con.close()


SUPPORTED_BAR_TYPES = {"time", "tick", "volume", "range"}


def list_symbols_with_ticks() -> list[str]:
    con = get_duckdb_connection()
    try:
        rows = con.execute(
            """
            SELECT DISTINCT symbol_contract
            FROM ticks
            WHERE symbol_contract IS NOT NULL
              AND TRIM(symbol_contract) <> ''
            ORDER BY symbol_contract
            """
        ).fetchall()
    finally:
        con.close()
    return [str(row[0]) for row in rows]


def precompute_non_time_bar_presets(
    symbol_contract: str,
    *,
    presets: tuple[tuple[str, int], ...] | None = None,
    full_rebuild: bool = False,
    chunk_days: int = NON_TIME_BAR_CHUNK_DAYS,
) -> dict:
    normalized_symbol = str(symbol_contract).strip()
    if not normalized_symbol:
        raise ValueError("symbol_contract is required")

    resolved_presets = tuple(presets) if presets is not None else NON_TIME_BAR_PRESETS
    has_ticks = _latest_tick_ts(symbol_contract=normalized_symbol) is not None
    results: list[dict] = []
    for bar_type, bar_size in resolved_presets:
        timeframe_value = non_time_bar_timeframe(bar_type=bar_type, bar_size=bar_size)
        if not has_ticks:
            if full_rebuild:
                _replace_bars_for_symbol_timeframe(
                    symbol_contract=normalized_symbol,
                    timeframe=timeframe_value,
                    bars=_empty_bars_frame(),
                )
            results.append(
                {
                    "bar_type": bar_type,
                    "bar_size": int(bar_size),
                    "timeframe": timeframe_value,
                    "row_count": 0,
                }
            )
            continue

        bars = _build_non_time_bars_chunked(
            symbol_contract=normalized_symbol,
            bar_type=bar_type,
            bar_size=int(bar_size),
            dataset_tz=get_settings().dataset_tz,
            chunk_days=chunk_days,
        )
        _replace_bars_for_symbol_timeframe(
            symbol_contract=normalized_symbol,
            timeframe=timeframe_value,
            bars=bars,
        )
        results.append(
            {
                "bar_type": bar_type,
                "bar_size": int(bar_size),
                "timeframe": timeframe_value,
                "row_count": int(len(bars)),
            }
        )

    return {
        "symbol_contract": normalized_symbol,
        "full_rebuild": bool(full_rebuild),
        "presets": results,
    }


def _recompute_time_bars_range(
    symbol_contract: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    dataset_tz: str,
    *,
    cached_fallback: pd.DataFrame | None = None,
) -> pd.DataFrame:
    ticks = load_ticks(symbol_contract=symbol_contract, start=start, end=end)
    if ticks.empty:
        if cached_fallback is not None and not cached_fallback.empty:
            return cached_fallback
        return _rebuild_from_lower_timeframe_bars(
            symbol_contract=symbol_contract,
            timeframe=timeframe,
            start=start,
            end=end,
        )

    bars = build_bars_from_ticks(ticks, timeframe=timeframe, dataset_tz=dataset_tz)
    upsert_bars(bars, symbol_contract=symbol_contract, timeframe=timeframe, start=start, end=end)
    if not bars.empty:
        return bars
    if cached_fallback is not None:
        return cached_fallback
    return _empty_bars_frame()


def ensure_bars(
    symbol_contract: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    bar_type: str = "time",
    bar_size: int | None = None,
    force_recompute: bool = False,
) -> pd.DataFrame:
    normalized_bar_type = str(bar_type or "time").strip().lower()
    if normalized_bar_type not in SUPPORTED_BAR_TYPES:
        raise ValueError(f"Unsupported bar_type {bar_type}")
    if normalized_bar_type == "time" and timeframe not in TIMEFRAME_RULES:
        raise ValueError(f"Unsupported timeframe {timeframe}")
    if normalized_bar_type != "time":
        if bar_size is None:
            raise ValueError("bar_size is required for non-time bar_type")
        if int(bar_size) < 1:
            raise ValueError("bar_size must be >= 1")

    dataset_tz = get_settings().dataset_tz
    if normalized_bar_type == "tick":
        timeframe_value = non_time_bar_timeframe(bar_type=normalized_bar_type, bar_size=int(bar_size))
        if force_recompute or _non_time_timeframe_stale(symbol_contract=symbol_contract, timeframe=timeframe_value):
            _recompute_non_time_timeframe(
                symbol_contract=symbol_contract,
                bar_type=normalized_bar_type,
                bar_size=int(bar_size),
            )
        return load_bars(symbol_contract=symbol_contract, timeframe=timeframe_value, start=start, end=end)
    if normalized_bar_type == "volume":
        timeframe_value = non_time_bar_timeframe(bar_type=normalized_bar_type, bar_size=int(bar_size))
        if force_recompute or _non_time_timeframe_stale(symbol_contract=symbol_contract, timeframe=timeframe_value):
            _recompute_non_time_timeframe(
                symbol_contract=symbol_contract,
                bar_type=normalized_bar_type,
                bar_size=int(bar_size),
            )
        return load_bars(symbol_contract=symbol_contract, timeframe=timeframe_value, start=start, end=end)
    if normalized_bar_type == "range":
        timeframe_value = non_time_bar_timeframe(bar_type=normalized_bar_type, bar_size=int(bar_size))
        if force_recompute or _non_time_timeframe_stale(symbol_contract=symbol_contract, timeframe=timeframe_value):
            _recompute_non_time_timeframe(
                symbol_contract=symbol_contract,
                bar_type=normalized_bar_type,
                bar_size=int(bar_size),
            )
        return load_bars(symbol_contract=symbol_contract, timeframe=timeframe_value, start=start, end=end)

    cached_bars = load_bars(symbol_contract=symbol_contract, timeframe=timeframe, start=start, end=end)
    if force_recompute:
        return _recompute_time_bars_range(
            symbol_contract=symbol_contract,
            timeframe=timeframe,
            start=start,
            end=end,
            dataset_tz=dataset_tz,
            cached_fallback=cached_bars,
        )

    if cached_bars.empty:
        return _recompute_time_bars_range(
            symbol_contract=symbol_contract,
            timeframe=timeframe,
            start=start,
            end=end,
            dataset_tz=dataset_tz,
            cached_fallback=cached_bars,
        )

    first_bar_ts = pd.Timestamp(cached_bars["ts"].iloc[0]).to_pydatetime()
    last_bar_ts = pd.Timestamp(cached_bars["ts"].iloc[-1]).to_pydatetime()
    timeframe_step = pd.Timedelta(seconds=_timeframe_to_seconds(timeframe))
    edge_step = pd.Timedelta(microseconds=1)

    if start < first_bar_ts:
        prefix_end = min(end, (pd.Timestamp(first_bar_ts) - edge_step).to_pydatetime())
        if start <= prefix_end:
            _recompute_time_bars_range(
                symbol_contract=symbol_contract,
                timeframe=timeframe,
                start=start,
                end=prefix_end,
                dataset_tz=dataset_tz,
            )

    if end > last_bar_ts:
        suffix_start = max(start, (pd.Timestamp(last_bar_ts) + timeframe_step).to_pydatetime())
        if suffix_start <= end:
            _recompute_time_bars_range(
                symbol_contract=symbol_contract,
                timeframe=timeframe,
                start=suffix_start,
                end=end,
                dataset_tz=dataset_tz,
            )

    refreshed = load_bars(symbol_contract=symbol_contract, timeframe=timeframe, start=start, end=end)
    return refreshed if not refreshed.empty else cached_bars


def ensure_session_profile(symbol_contract: str, session_date: str, tick_size: float = 0.25) -> pd.DataFrame:
    con = get_duckdb_connection()
    try:
        existing = con.execute(
            """
            SELECT session_date, symbol_contract, price_level, volume
            FROM session_profiles
            WHERE symbol_contract = ? AND session_date = ?
            ORDER BY price_level
            """,
            [symbol_contract, session_date],
        ).fetch_df()
    finally:
        con.close()

    if not existing.empty:
        return existing

    con = get_duckdb_connection()
    try:
        ticks = con.execute(
            """
            SELECT ts, session_date, symbol_contract, trade_price, trade_size, bid_price, ask_price
            FROM ticks
            WHERE symbol_contract = ? AND session_date = ?
            ORDER BY ts
            """,
            [symbol_contract, session_date],
        ).fetch_df()
    finally:
        con.close()

    if ticks.empty:
        return existing

    ticks["ts"] = pd.to_datetime(ticks["ts"], utc=True).dt.tz_convert(get_settings().dataset_tz)
    profile = build_session_profile(ticks, tick_size=tick_size)

    con = get_duckdb_connection()
    try:
        con.execute(
            "DELETE FROM session_profiles WHERE symbol_contract = ? AND session_date = ?",
            [symbol_contract, session_date],
        )
        con.register("profile_df", profile)
        con.execute(
            """
            INSERT INTO session_profiles (session_date, symbol_contract, price_level, volume)
            SELECT session_date, symbol_contract, price_level, volume
            FROM profile_df
            """
        )
        con.unregister("profile_df")
    finally:
        con.close()
    invalidate_runtime_symbol_caches(symbol_contract)
    return profile


def _dataset_zone() -> ZoneInfo:
    return ZoneInfo(get_settings().dataset_tz)


def _resolve_profile_zone(profile_timezone: str | None) -> ZoneInfo:
    if not profile_timezone:
        return _dataset_zone()
    return ZoneInfo(profile_timezone)


def _to_zone(value: datetime, zone: ZoneInfo) -> datetime:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize(zone)
    else:
        ts = ts.tz_convert(zone)
    return ts.to_pydatetime()


def _to_dataset_tz(value: datetime) -> datetime:
    return _to_zone(value, _dataset_zone())


def load_ticks_for_profile_window(
    symbol_contract: str,
    start: datetime,
    end: datetime,
    profile_zone: ZoneInfo,
) -> pd.DataFrame:
    cache_key = (
        "profile_window",
        _cache_db_namespace(),
        str(symbol_contract),
        _dt_cache_token(start),
        _dt_cache_token(end),
        _zone_key(profile_zone),
    )
    cached = _PROFILE_WINDOW_CACHE.get(cache_key)
    if cached is not None:
        return cached.copy(deep=True)

    query_start = _to_dataset_tz(start)
    query_end = _to_dataset_tz(end)
    ticks = load_ticks(symbol_contract=symbol_contract, start=query_start, end=query_end)
    if ticks.empty:
        out = pd.DataFrame(
            columns=[
                "ts",
                "session_date",
                "symbol_contract",
                "trade_price",
                "trade_size",
                "bid_price",
                "ask_price",
                "local_ts",
                "exchange_ts",
                "exchange_day_session_start",
                "exchange_rth_session_start",
            ]
        )
        _PROFILE_WINDOW_CACHE.set(cache_key, out)
        return out.copy(deep=True)

    df = ticks.copy()
    local_ts = pd.to_datetime(df["ts"], errors="coerce")
    if local_ts.dt.tz is None:
        local_ts = local_ts.dt.tz_localize(profile_zone)
    else:
        local_ts = local_ts.dt.tz_convert(profile_zone)
    df["local_ts"] = local_ts

    local_start = pd.Timestamp(start).tz_convert(profile_zone)
    local_end = pd.Timestamp(end).tz_convert(profile_zone)
    df = df[(df["local_ts"] >= local_start) & (df["local_ts"] <= local_end)].copy()
    if df.empty:
        out = df
        _PROFILE_WINDOW_CACHE.set(cache_key, out)
        return out.copy(deep=True)

    rth_session_offset = pd.to_timedelta(RTH_SESSION_START_MINUTES, unit="m")
    day_session_offset = pd.to_timedelta(DAY_PROFILE_START_MINUTES, unit="m")

    exchange_ts = df["local_ts"].dt.tz_convert(EXCHANGE_SESSION_TZ)
    df["exchange_ts"] = exchange_ts
    df["exchange_day_session_start"] = (exchange_ts - day_session_offset).dt.floor("D") + day_session_offset
    df["exchange_rth_session_start"] = (exchange_ts - rth_session_offset).dt.floor("D") + rth_session_offset

    _PROFILE_WINDOW_CACHE.set(cache_key, df.copy(deep=True))
    return df


def _resolve_tick_size(symbol_contract: str, tick_size: float | None) -> float:
    if tick_size is not None:
        return float(tick_size)
    return resolve_symbol_tick_size(symbol_contract=symbol_contract, fallback=0.25)


def _effective_profile_tick_size(symbol_contract: str, tick_size: float | None, tick_aggregation: int) -> float:
    aggregation = int(tick_aggregation)
    if aggregation < 1:
        raise ValueError("tick_aggregation must be >= 1")
    return _resolve_tick_size(symbol_contract=symbol_contract, tick_size=tick_size) * aggregation


def _normalize_profile_metric(metric: str | None) -> str:
    normalized = str(metric or "volume").strip().lower()
    if normalized not in PROFILE_METRICS:
        raise ValueError(f"Unsupported metric: {metric}")
    return normalized


def _build_metric_profile_levels(
    ticks: pd.DataFrame,
    metric: str,
    tick_size: float,
    price_min: float | None = None,
    price_max: float | None = None,
) -> pd.DataFrame:
    if metric == "delta":
        source = ticks.reindex(columns=["trade_price", "trade_size", "bid_price", "ask_price"]).copy()
        return build_delta_profile_levels(
            ticks=source,
            tick_size=tick_size,
            group_columns=[],
            price_min=price_min,
            price_max=price_max,
        )
    source = ticks.reindex(columns=["trade_price", "trade_size"]).copy()
    return build_volume_profile_levels(
        ticks=source,
        tick_size=tick_size,
        group_columns=[],
        price_min=price_min,
        price_max=price_max,
    )


def _build_profile_payload(
    profile_id: str,
    label: str,
    start: datetime,
    end: datetime,
    levels: pd.DataFrame,
    metric: str,
    value_area_enabled: bool = False,
    value_area_percent: float = 70.0,
) -> dict:
    value_column = "delta" if metric == "delta" else "volume"
    value_area = {
        "enabled": bool(value_area_enabled and metric == "volume"),
        "percent": float(value_area_percent),
        "poc": None,
        "low": None,
        "high": None,
        "volume": 0.0,
    }

    if levels.empty:
        rows: list[dict] = []
        values = np.array([], dtype=float)
    else:
        clean_levels = levels.sort_values("price_level").reset_index(drop=True)
        price_levels = pd.to_numeric(clean_levels["price_level"], errors="coerce").to_numpy(dtype=float)
        values = pd.to_numeric(clean_levels[value_column], errors="coerce").to_numpy(dtype=float)
        valid = np.isfinite(price_levels) & np.isfinite(values)
        price_levels = price_levels[valid]
        values = values[valid]
        rows = [
            {
                "price_level": float(price),
                "value": float(value),
                # Backward compatibility for existing clients.
                "volume": float(value),
            }
            for price, value in zip(price_levels.tolist(), values.tolist())
        ]

    if values.size == 0:
        max_value = 0.0
        total_value = 0.0
    else:
        max_value = float(np.max(np.abs(values))) if metric == "delta" else float(np.max(values))
        total_value = float(np.sum(values))

    if value_area["enabled"] and total_value > 0 and rows:
        target_total = total_value * max(0.0, min(100.0, float(value_area_percent))) / 100.0
        volumes = [float(row["value"]) for row in rows]
        poc_idx = int(np.argmax(np.asarray(volumes, dtype=float)))
        value_area_total = volumes[poc_idx]
        low_idx = poc_idx
        high_idx = poc_idx

        while value_area_total < target_total and (low_idx > 0 or high_idx < len(rows) - 1):
            left_idx = low_idx - 1 if low_idx > 0 else None
            right_idx = high_idx + 1 if high_idx < len(rows) - 1 else None
            left_value = volumes[left_idx] if left_idx is not None else -1.0
            right_value = volumes[right_idx] if right_idx is not None else -1.0

            if left_idx is not None and right_idx is not None and left_value == right_value:
                low_idx = left_idx
                value_area_total += left_value
                if value_area_total >= target_total:
                    break
                high_idx = right_idx
                value_area_total += right_value
                continue
            if right_idx is not None and right_value > left_value:
                high_idx = right_idx
                value_area_total += right_value
                continue
            if left_idx is not None:
                low_idx = left_idx
                value_area_total += left_value
                continue
            if right_idx is not None:
                high_idx = right_idx
                value_area_total += right_value

        value_area["poc"] = float(rows[poc_idx]["price_level"])
        value_area["low"] = float(rows[low_idx]["price_level"])
        value_area["high"] = float(rows[high_idx]["price_level"])
        value_area["volume"] = float(value_area_total)

    return {
        "id": profile_id,
        "label": label,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "max_value": max_value,
        "total_value": total_value,
        # Backward compatibility for existing clients.
        "max_volume": max_value,
        "total_volume": total_value,
        "value_area": value_area,
        "value_area_enabled": value_area["enabled"],
        "value_area_percent": value_area["percent"],
        "value_area_poc": value_area["poc"],
        "value_area_low": value_area["low"],
        "value_area_high": value_area["high"],
        "value_area_volume": value_area["volume"],
        "levels": rows,
    }


def ensure_preset_volume_profiles(
    symbol_contract: str,
    start: datetime,
    end: datetime,
    preset: str,
    profile_timezone: str | None = None,
    metric: str = "volume",
    tick_size: float | None = None,
    tick_aggregation: int = 1,
    value_area_enabled: bool = False,
    value_area_percent: float = 70.0,
    max_segments: int = 120,
) -> dict:
    normalized_metric = _normalize_profile_metric(metric)
    normalized_preset = preset.strip().lower()
    if normalized_preset not in {"day", "week", "rth", "eth"}:
        raise ValueError(f"Unsupported preset: {preset}")
    if normalized_metric == "delta" and normalized_preset == "week":
        raise ValueError("Unsupported preset: week")
    if not 0 < float(value_area_percent) <= 100:
        raise ValueError("value_area_percent must be > 0 and <= 100")

    try:
        profile_zone = _resolve_profile_zone(profile_timezone)
    except Exception as exc:
        raise ValueError(f"Invalid profile timezone: {profile_timezone}") from exc
    start_dt = _to_zone(start, profile_zone)
    end_dt = _to_zone(end, profile_zone)
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt

    effective_tick_size = _effective_profile_tick_size(
        symbol_contract=symbol_contract,
        tick_size=tick_size,
        tick_aggregation=tick_aggregation,
    )
    capped_segments = max(1, int(max_segments))
    value_area_flag = bool(value_area_enabled and normalized_metric == "volume")

    cache_key = (
        "profile_preset",
        _cache_db_namespace(),
        str(symbol_contract),
        _dt_cache_token(start_dt),
        _dt_cache_token(end_dt),
        normalized_preset,
        _zone_key(profile_zone),
        normalized_metric,
        float(effective_tick_size),
        int(tick_aggregation),
        value_area_flag,
        float(value_area_percent),
        int(capped_segments),
    )
    cached = _PROFILE_PRESET_CACHE.get(cache_key)
    if cached is not None:
        return copy.deepcopy(cached)

    response = {
        "symbol_contract": symbol_contract,
        "timezone": getattr(profile_zone, "key", str(profile_zone)),
        "preset": normalized_preset,
        "metric": normalized_metric,
        "tick_size": effective_tick_size,
        "tick_aggregation": int(tick_aggregation),
        "value_area_enabled": value_area_flag,
        "value_area_percent": float(value_area_percent),
        "profiles": [],
    }

    df = load_ticks_for_profile_window(symbol_contract=symbol_contract, start=start_dt, end=end_dt, profile_zone=profile_zone)
    if df.empty:
        _PROFILE_PRESET_CACHE.set(cache_key, copy.deepcopy(response))
        return response

    rth_duration = pd.to_timedelta(RTH_SESSION_END_MINUTES - RTH_SESSION_START_MINUTES, unit="m")
    one_day = pd.to_timedelta(1, unit="D")

    segment_rows: list[tuple[str, pd.DataFrame, datetime, datetime, str]] = []
    if normalized_preset == "day":
        # "Day" follows a session day from 16:00 ET to next-day 15:59:59 ET.
        for session_start_exchange, group in df.groupby("exchange_day_session_start", sort=True):
            session_start = pd.Timestamp(session_start_exchange)
            key = session_start.strftime("%Y-%m-%d")
            segment_start = session_start.tz_convert(profile_zone).to_pydatetime()
            segment_end = (session_start + one_day).tz_convert(profile_zone).to_pydatetime()
            segment_rows.append((key, group, segment_start, segment_end, "Day"))
    elif normalized_preset == "week":
        iso = df["exchange_ts"].dt.isocalendar()
        grouped = df.assign(_week_year=iso["year"].astype(int), _week_num=iso["week"].astype(int))
        for (year, week), group in grouped.groupby(["_week_year", "_week_num"], sort=True):
            key = f"{int(year)}-W{int(week):02d}"
            segment_start = pd.Timestamp(group["local_ts"].min()).to_pydatetime()
            segment_end = pd.Timestamp(group["local_ts"].max()).to_pydatetime()
            segment_rows.append((key, group, segment_start, segment_end, "Week"))
    else:
        minutes = (df["exchange_ts"].dt.hour * 60) + df["exchange_ts"].dt.minute
        rth_mask = (minutes >= RTH_SESSION_START_MINUTES) & (minutes < RTH_SESSION_END_MINUTES)
        filtered = df[rth_mask].copy() if normalized_preset == "rth" else df[~rth_mask].copy()
        for session_start_exchange, group in filtered.groupby("exchange_rth_session_start", sort=True):
            session_start = pd.Timestamp(session_start_exchange)
            key = session_start.strftime("%Y-%m-%d")
            if normalized_preset == "rth":
                segment_start = session_start.tz_convert(profile_zone).to_pydatetime()
                segment_end = (session_start + rth_duration).tz_convert(profile_zone).to_pydatetime()
                label_prefix = "RTH"
            else:
                segment_start = (session_start + rth_duration).tz_convert(profile_zone).to_pydatetime()
                segment_end = (session_start + one_day).tz_convert(profile_zone).to_pydatetime()
                label_prefix = "ETH"
            segment_rows.append((key, group, segment_start, segment_end, label_prefix))

    profiles: list[dict] = []
    for key, group, segment_start, segment_end, label_prefix in segment_rows:
        if group.empty:
            continue
        levels = _build_metric_profile_levels(
            ticks=group,
            metric=normalized_metric,
            tick_size=effective_tick_size,
        )
        profiles.append(
            _build_profile_payload(
                profile_id=f"{normalized_preset}-{key}",
                label=f"{label_prefix} {key}",
                start=segment_start,
                end=segment_end,
                levels=levels,
                metric=normalized_metric,
                value_area_enabled=value_area_flag,
                value_area_percent=float(value_area_percent),
            )
        )

    profiles = sorted(profiles, key=lambda row: row["end"])
    if len(profiles) > capped_segments:
        profiles = profiles[-capped_segments:]
    response["profiles"] = profiles
    _PROFILE_PRESET_CACHE.set(cache_key, copy.deepcopy(response))
    return response


def ensure_area_volume_profile(
    symbol_contract: str,
    start: datetime,
    end: datetime,
    price_min: float,
    price_max: float,
    area_id: str | None = None,
    profile_timezone: str | None = None,
    metric: str = "volume",
    tick_size: float | None = None,
    tick_aggregation: int = 1,
    value_area_enabled: bool = False,
    value_area_percent: float = 70.0,
) -> dict:
    normalized_metric = _normalize_profile_metric(metric)
    if not 0 < float(value_area_percent) <= 100:
        raise ValueError("value_area_percent must be > 0 and <= 100")
    try:
        profile_zone = _resolve_profile_zone(profile_timezone)
    except Exception as exc:
        raise ValueError(f"Invalid profile timezone: {profile_timezone}") from exc
    start_dt = _to_zone(start, profile_zone)
    end_dt = _to_zone(end, profile_zone)
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt

    low = float(min(price_min, price_max))
    high = float(max(price_min, price_max))
    effective_tick_size = _effective_profile_tick_size(
        symbol_contract=symbol_contract,
        tick_size=tick_size,
        tick_aggregation=tick_aggregation,
    )
    value_area_flag = bool(value_area_enabled and normalized_metric == "volume")

    cache_key = (
        "profile_area",
        _cache_db_namespace(),
        str(symbol_contract),
        _dt_cache_token(start_dt),
        _dt_cache_token(end_dt),
        float(low),
        float(high),
        str(area_id or ""),
        _zone_key(profile_zone),
        normalized_metric,
        float(effective_tick_size),
        int(tick_aggregation),
        value_area_flag,
        float(value_area_percent),
    )
    cached = _PROFILE_AREA_CACHE.get(cache_key)
    if cached is not None:
        return copy.deepcopy(cached)

    ticks = load_ticks_for_profile_window(symbol_contract=symbol_contract, start=start_dt, end=end_dt, profile_zone=profile_zone)
    levels = _build_metric_profile_levels(
        ticks=ticks,
        metric=normalized_metric,
        tick_size=effective_tick_size,
        price_min=low,
        price_max=high,
    )
    profile = _build_profile_payload(
        profile_id=area_id or f"vp-{int(pd.Timestamp.now().timestamp() * 1000)}",
        label="Area Profile",
        start=start_dt,
        end=end_dt,
        levels=levels,
        metric=normalized_metric,
        value_area_enabled=value_area_flag,
        value_area_percent=float(value_area_percent),
    )
    profile["price_min"] = low
    profile["price_max"] = high
    response = {
        "symbol_contract": symbol_contract,
        "mode": "area",
        "timezone": getattr(profile_zone, "key", str(profile_zone)),
        "metric": normalized_metric,
        "tick_size": effective_tick_size,
        "tick_aggregation": int(tick_aggregation),
        "value_area_enabled": value_area_flag,
        "value_area_percent": float(value_area_percent),
        "profile": profile,
    }
    _PROFILE_AREA_CACHE.set(cache_key, copy.deepcopy(response))
    return response


def ensure_large_orders(
    symbol_contract: str,
    start: datetime,
    end: datetime,
    method: str,
    fixed_threshold: float,
    percentile: float,
    *,
    persist: bool = True,
) -> pd.DataFrame:
    normalized_method = str(method).strip().lower()

    cache_key = (
        "large_orders",
        _cache_db_namespace(),
        str(symbol_contract),
        _dt_cache_token(start),
        _dt_cache_token(end),
        normalized_method,
        float(fixed_threshold),
        float(percentile),
        bool(persist),
    )
    if not persist:
        cached = _LARGE_ORDERS_CACHE.get(cache_key)
        if cached is not None:
            return cached.copy(deep=True)

    ticks = load_ticks(symbol_contract=symbol_contract, start=start, end=end)
    large_orders = detect_large_orders(
        ticks,
        method=normalized_method,
        fixed_threshold=fixed_threshold,
        percentile=percentile,
    )

    if not persist:
        _LARGE_ORDERS_CACHE.set(cache_key, large_orders.copy(deep=True))
        return large_orders

    con = get_duckdb_connection()
    try:
        con.execute(
            """
            DELETE FROM large_orders
            WHERE symbol_contract = ? AND method = ? AND threshold = ? AND ts >= ? AND ts <= ?
            """,
            [symbol_contract, normalized_method, float(fixed_threshold), start, end],
        )
        if not large_orders.empty:
            con.register("large_orders_df", large_orders)
            con.execute(
                """
                INSERT INTO large_orders (ts, session_date, symbol_contract, trade_price, trade_size, method, threshold)
                SELECT ts, session_date, symbol_contract, trade_price, trade_size, method, threshold
                FROM large_orders_df
                """
            )
            con.unregister("large_orders_df")
    finally:
        con.close()

    invalidate_runtime_symbol_caches(symbol_contract)
    return large_orders


def load_large_orders(
    symbol_contract: str,
    start: datetime,
    end: datetime,
    method: str,
    fixed_threshold: float,
) -> pd.DataFrame:
    normalized_method = str(method).strip().lower()
    con = get_duckdb_connection()
    try:
        df = con.execute(
            """
            SELECT ts, session_date, symbol_contract, trade_price, trade_size, method, threshold
            FROM large_orders
            WHERE symbol_contract = ?
              AND method = ?
              AND threshold = ?
              AND ts >= ?
              AND ts <= ?
            ORDER BY ts
            """,
            [symbol_contract, normalized_method, float(fixed_threshold), start, end],
        ).fetch_df()
    finally:
        con.close()

    if df.empty:
        return df

    parsed_ts = pd.to_datetime(df["ts"], errors="coerce")
    if parsed_ts.dt.tz is None:
        parsed_ts = parsed_ts.dt.tz_localize("UTC")
    df["ts"] = parsed_ts.dt.tz_convert(get_settings().dataset_tz)
    return df
