from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from app.db.duck import get_duckdb_connection
from app.services.aggregation import clear_runtime_caches, ensure_preset_volume_profiles

EXCHANGE_TZ = ZoneInfo("America/New_York")
SESSION_BOUNDARY_OFFSET = pd.Timedelta(hours=16)
SESSION_CHUNK_DAYS = 7
WEEK_CHUNK_WEEKS = 4
CHUNK_END_EPSILON = pd.Timedelta(microseconds=1)


@dataclass(frozen=True)
class ProfileBuildSpec:
    metric: str
    tick_aggregation: int
    preset: str


PROFILE_BUILD_SPECS: tuple[ProfileBuildSpec, ...] = (
    ProfileBuildSpec(metric="volume", tick_aggregation=1, preset="week"),
    ProfileBuildSpec(metric="volume", tick_aggregation=1, preset="day"),
    ProfileBuildSpec(metric="volume", tick_aggregation=1, preset="rth"),
    ProfileBuildSpec(metric="volume", tick_aggregation=1, preset="eth"),
    ProfileBuildSpec(metric="volume", tick_aggregation=4, preset="week"),
    ProfileBuildSpec(metric="volume", tick_aggregation=4, preset="day"),
    ProfileBuildSpec(metric="volume", tick_aggregation=4, preset="rth"),
    ProfileBuildSpec(metric="volume", tick_aggregation=4, preset="eth"),
    # Explicitly skip weekly delta profile persistence.
    *(
        ProfileBuildSpec(metric="delta", tick_aggregation=tick_aggregation, preset=preset)
        for tick_aggregation in range(1, 21)
        for preset in ("day", "rth", "eth")
    ),
)


def _normalize_ts(value: datetime | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts


def _session_start_1600_et(ts: pd.Timestamp) -> pd.Timestamp:
    exchange_ts = _normalize_ts(ts).tz_convert(EXCHANGE_TZ)
    return (exchange_ts - SESSION_BOUNDARY_OFFSET).floor("D") + SESSION_BOUNDARY_OFFSET


def _week_start_monday_0000_et(ts: pd.Timestamp) -> pd.Timestamp:
    exchange_ts = _normalize_ts(ts).tz_convert(EXCHANGE_TZ)
    midnight = exchange_ts.floor("D")
    return midnight - pd.Timedelta(days=int(midnight.weekday()))


def _iter_session_windows(min_ts: pd.Timestamp, max_ts: pd.Timestamp) -> list[tuple[datetime, datetime]]:
    first_start = _session_start_1600_et(min_ts)
    after_last = _session_start_1600_et(max_ts) + pd.Timedelta(days=1)
    step = pd.Timedelta(days=SESSION_CHUNK_DAYS)

    windows: list[tuple[datetime, datetime]] = []
    chunk_start = first_start
    while chunk_start < after_last:
        chunk_stop = min(chunk_start + step, after_last)
        chunk_end = chunk_stop - CHUNK_END_EPSILON
        windows.append((chunk_start.to_pydatetime(), chunk_end.to_pydatetime()))
        chunk_start = chunk_stop
    return windows


def _iter_week_windows(min_ts: pd.Timestamp, max_ts: pd.Timestamp) -> list[tuple[datetime, datetime]]:
    first_start = _week_start_monday_0000_et(min_ts)
    after_last = _week_start_monday_0000_et(max_ts) + pd.Timedelta(days=7)
    step = pd.Timedelta(days=7 * WEEK_CHUNK_WEEKS)

    windows: list[tuple[datetime, datetime]] = []
    chunk_start = first_start
    while chunk_start < after_last:
        chunk_stop = min(chunk_start + step, after_last)
        chunk_end = chunk_stop - CHUNK_END_EPSILON
        windows.append((chunk_start.to_pydatetime(), chunk_end.to_pydatetime()))
        chunk_start = chunk_stop
    return windows


def _list_symbols_with_ticks() -> list[str]:
    con = get_duckdb_connection()
    try:
        rows = con.execute(
            """
            SELECT DISTINCT symbol_contract
            FROM ticks
            WHERE symbol_contract IS NOT NULL
            ORDER BY symbol_contract
            """
        ).fetchall()
        return [str(row[0]) for row in rows if row and row[0]]
    finally:
        con.close()


def _symbol_bounds(symbol_contract: str) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    con = get_duckdb_connection()
    try:
        row = con.execute(
            """
            SELECT min(ts) AS min_ts, max(ts) AS max_ts
            FROM ticks
            WHERE symbol_contract = ?
            """,
            [symbol_contract],
        ).fetchone()
    finally:
        con.close()

    if row is None or row[0] is None or row[1] is None:
        return None
    return _normalize_ts(row[0]), _normalize_ts(row[1])


def _delete_combo(symbol_contract: str, profile_timezone: str, metric: str, preset: str, tick_aggregation: int) -> None:
    con = get_duckdb_connection()
    try:
        params = [symbol_contract, profile_timezone, metric, preset, int(tick_aggregation)]
        con.execute(
            """
            DELETE FROM preset_profile_levels
            WHERE symbol_contract = ?
              AND profile_timezone = ?
              AND metric = ?
              AND preset = ?
              AND tick_aggregation = ?
            """,
            params,
        )
        con.execute(
            """
            DELETE FROM preset_profile_segments
            WHERE symbol_contract = ?
              AND profile_timezone = ?
              AND metric = ?
              AND preset = ?
              AND tick_aggregation = ?
            """,
            params,
        )
    finally:
        con.close()


def _persist_preset_payload(
    payload: dict,
    *,
    symbol_contract: str,
    profile_timezone: str,
    metric: str,
    preset: str,
    tick_aggregation: int,
) -> tuple[int, int]:
    profiles = payload.get("profiles") or []
    if not profiles:
        return 0, 0

    segment_rows: list[dict] = []
    level_rows: list[dict] = []
    for profile in profiles:
        profile_id = str(profile.get("id") or "")
        levels = profile.get("levels") or []
        segment_rows.append(
            {
                "symbol_contract": symbol_contract,
                "profile_timezone": profile_timezone,
                "metric": metric,
                "preset": preset,
                "tick_aggregation": int(tick_aggregation),
                "tick_size": float(payload.get("tick_size") or 0.0),
                "profile_id": profile_id,
                "label": str(profile.get("label") or ""),
                "segment_start": pd.Timestamp(profile.get("start")).to_pydatetime(),
                "segment_end": pd.Timestamp(profile.get("end")).to_pydatetime(),
                "max_value": float(profile.get("max_value") or 0.0),
                "total_value": float(profile.get("total_value") or 0.0),
                "value_area_enabled": bool(profile.get("value_area_enabled") or False),
                "value_area_percent": float(profile.get("value_area_percent") or 70.0),
                "value_area_poc": profile.get("value_area_poc"),
                "value_area_low": profile.get("value_area_low"),
                "value_area_high": profile.get("value_area_high"),
                "value_area_volume": float(profile.get("value_area_volume") or 0.0),
                "level_count": int(len(levels)),
            }
        )
        for level in levels:
            level_rows.append(
                {
                    "symbol_contract": symbol_contract,
                    "profile_timezone": profile_timezone,
                    "metric": metric,
                    "preset": preset,
                    "tick_aggregation": int(tick_aggregation),
                    "profile_id": profile_id,
                    "price_level": float(level.get("price_level")),
                    "value": float(level.get("value") if level.get("value") is not None else level.get("volume") or 0.0),
                }
            )

    segments_df = pd.DataFrame(segment_rows).drop_duplicates(subset=["profile_id"], keep="last")
    levels_df = pd.DataFrame(level_rows)
    if not levels_df.empty:
        levels_df = levels_df.drop_duplicates(subset=["profile_id", "price_level"], keep="last")

    con = get_duckdb_connection()
    try:
        con.register("segments_df", segments_df)
        con.execute(
            """
            DELETE FROM preset_profile_levels
            WHERE symbol_contract = ?
              AND profile_timezone = ?
              AND metric = ?
              AND preset = ?
              AND tick_aggregation = ?
              AND profile_id IN (SELECT DISTINCT profile_id FROM segments_df)
            """,
            [symbol_contract, profile_timezone, metric, preset, int(tick_aggregation)],
        )
        con.execute(
            """
            DELETE FROM preset_profile_segments
            WHERE symbol_contract = ?
              AND profile_timezone = ?
              AND metric = ?
              AND preset = ?
              AND tick_aggregation = ?
              AND profile_id IN (SELECT DISTINCT profile_id FROM segments_df)
            """,
            [symbol_contract, profile_timezone, metric, preset, int(tick_aggregation)],
        )
        con.execute(
            """
            INSERT INTO preset_profile_segments (
                symbol_contract,
                profile_timezone,
                metric,
                preset,
                tick_aggregation,
                tick_size,
                profile_id,
                label,
                segment_start,
                segment_end,
                max_value,
                total_value,
                value_area_enabled,
                value_area_percent,
                value_area_poc,
                value_area_low,
                value_area_high,
                value_area_volume,
                level_count
            )
            SELECT
                symbol_contract,
                profile_timezone,
                metric,
                preset,
                tick_aggregation,
                tick_size,
                profile_id,
                label,
                segment_start,
                segment_end,
                max_value,
                total_value,
                value_area_enabled,
                value_area_percent,
                value_area_poc,
                value_area_low,
                value_area_high,
                value_area_volume,
                level_count
            FROM segments_df
            """
        )

        if not levels_df.empty:
            con.register("levels_df", levels_df)
            con.execute(
                """
                INSERT INTO preset_profile_levels (
                    symbol_contract,
                    profile_timezone,
                    metric,
                    preset,
                    tick_aggregation,
                    profile_id,
                    price_level,
                    value
                )
                SELECT
                    symbol_contract,
                    profile_timezone,
                    metric,
                    preset,
                    tick_aggregation,
                    profile_id,
                    price_level,
                    value
                FROM levels_df
                """
            )
            con.unregister("levels_df")
        con.unregister("segments_df")
    finally:
        con.close()

    return int(len(segments_df)), int(len(levels_df))


def run_preset_profile_backfill(
    symbols: list[str],
    profile_timezone: str,
    full_rebuild: bool = True,
) -> dict:
    ZoneInfo(profile_timezone)

    requested_symbols = [str(symbol).strip() for symbol in symbols if str(symbol).strip()]
    if not requested_symbols:
        requested_symbols = _list_symbols_with_ticks()

    symbol_stats: dict[str, dict] = {}
    total_segments = 0
    total_levels = 0
    total_chunks = 0

    for symbol_contract in requested_symbols:
        bounds = _symbol_bounds(symbol_contract)
        if bounds is None:
            symbol_stats[symbol_contract] = {
                "status": "skipped",
                "reason": "no_ticks",
                "segments": 0,
                "levels": 0,
                "chunks": 0,
                "combos": 0,
            }
            continue

        min_ts, max_ts = bounds
        symbol_segments = 0
        symbol_levels = 0
        symbol_chunks = 0
        symbol_combos = 0

        for spec in PROFILE_BUILD_SPECS:
            symbol_combos += 1
            if full_rebuild:
                _delete_combo(
                    symbol_contract=symbol_contract,
                    profile_timezone=profile_timezone,
                    metric=spec.metric,
                    preset=spec.preset,
                    tick_aggregation=spec.tick_aggregation,
                )

            windows = _iter_week_windows(min_ts, max_ts) if spec.preset == "week" else _iter_session_windows(min_ts, max_ts)
            for start_dt, end_dt in windows:
                payload = ensure_preset_volume_profiles(
                    symbol_contract=symbol_contract,
                    start=start_dt,
                    end=end_dt,
                    preset=spec.preset,
                    profile_timezone=profile_timezone,
                    metric=spec.metric,
                    tick_aggregation=spec.tick_aggregation,
                    max_segments=10000,
                )
                inserted_segments, inserted_levels = _persist_preset_payload(
                    payload,
                    symbol_contract=symbol_contract,
                    profile_timezone=profile_timezone,
                    metric=spec.metric,
                    preset=spec.preset,
                    tick_aggregation=spec.tick_aggregation,
                )
                symbol_segments += inserted_segments
                symbol_levels += inserted_levels
                symbol_chunks += 1
                clear_runtime_caches()

        symbol_stats[symbol_contract] = {
            "status": "backfilled",
            "segments": int(symbol_segments),
            "levels": int(symbol_levels),
            "chunks": int(symbol_chunks),
            "combos": int(symbol_combos),
            "min_ts": min_ts.isoformat(),
            "max_ts": max_ts.isoformat(),
        }
        total_segments += symbol_segments
        total_levels += symbol_levels
        total_chunks += symbol_chunks

    return {
        "status": "ok",
        "profile_timezone": profile_timezone,
        "full_rebuild": bool(full_rebuild),
        "symbols_requested": requested_symbols,
        "symbols_processed": [symbol for symbol, row in symbol_stats.items() if row["status"] == "backfilled"],
        "total_segments": int(total_segments),
        "total_levels": int(total_levels),
        "total_chunks": int(total_chunks),
        "build_specs": [
            {
                "metric": spec.metric,
                "tick_aggregation": spec.tick_aggregation,
                "preset": spec.preset,
            }
            for spec in PROFILE_BUILD_SPECS
        ],
        "symbol_stats": symbol_stats,
    }
