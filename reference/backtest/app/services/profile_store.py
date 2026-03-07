from __future__ import annotations

import math
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from app.config import get_settings
from app.db.duck import get_duckdb_connection
from app.services.market_processing import resolve_symbol_tick_size

CANONICAL_PROFILE_TIMEZONE = "America/New_York"
SUPPORTED_METRICS = {"volume", "delta"}
SUPPORTED_PRESETS = {"day", "week", "rth", "eth"}
VOLUME_TICK_AGGREGATIONS = (1, 4)
DELTA_TICK_AGGREGATION = 6


def _normalize_metric(metric: str) -> str:
    normalized = str(metric or "volume").strip().lower()
    if normalized not in SUPPORTED_METRICS:
        raise ValueError(f"Unsupported metric: {metric}")
    return normalized


def _normalize_preset(preset: str, metric: str) -> str:
    normalized = str(preset or "").strip().lower()
    if normalized not in SUPPORTED_PRESETS:
        raise ValueError(f"Unsupported preset: {preset}")
    if metric == "delta" and normalized == "week":
        raise ValueError("Unsupported preset: week")
    return normalized


def _normalize_tick_aggregation(metric: str, tick_aggregation: int) -> int:
    parsed = int(tick_aggregation)
    if metric == "delta":
        return DELTA_TICK_AGGREGATION
    if parsed in VOLUME_TICK_AGGREGATIONS:
        return parsed
    return min(VOLUME_TICK_AGGREGATIONS, key=lambda candidate: abs(candidate - parsed))


def _resolve_response_zone(profile_timezone: str | None) -> ZoneInfo:
    if profile_timezone:
        return ZoneInfo(profile_timezone)
    return ZoneInfo(get_settings().dataset_tz)


def _to_aware(value: datetime, default_zone: ZoneInfo) -> datetime:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize(default_zone)
    return ts.to_pydatetime()


def _to_float_or_none(value) -> float | None:
    if value is None:
        return None
    parsed = float(value)
    if math.isnan(parsed):
        return None
    return parsed


def _default_tick_size(symbol_contract: str, metric: str, tick_aggregation: int) -> float:
    base = resolve_symbol_tick_size(symbol_contract=symbol_contract, fallback=0.25)
    if metric == "delta":
        return float(base * DELTA_TICK_AGGREGATION)
    return float(base * tick_aggregation)


def load_persisted_preset_profiles(
    symbol_contract: str,
    start: datetime,
    end: datetime,
    preset: str,
    profile_timezone: str | None,
    metric: str,
    tick_aggregation: int,
    max_segments: int = 120,
) -> dict:
    normalized_metric = _normalize_metric(metric)
    normalized_preset = _normalize_preset(preset, normalized_metric)
    normalized_tick_aggregation = _normalize_tick_aggregation(normalized_metric, tick_aggregation)
    response_zone = _resolve_response_zone(profile_timezone)

    start_dt = _to_aware(start, default_zone=response_zone)
    end_dt = _to_aware(end, default_zone=response_zone)
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt
    capped_segments = max(1, int(max_segments))

    con = get_duckdb_connection()
    try:
        segments = con.execute(
            """
            SELECT
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
                level_count,
                tick_size
            FROM preset_profile_segments
            WHERE symbol_contract = ?
              AND profile_timezone = ?
              AND metric = ?
              AND preset = ?
              AND tick_aggregation = ?
              AND segment_end >= ?
              AND segment_start <= ?
            ORDER BY segment_end DESC
            LIMIT ?
            """,
            [
                symbol_contract,
                CANONICAL_PROFILE_TIMEZONE,
                normalized_metric,
                normalized_preset,
                normalized_tick_aggregation,
                start_dt,
                end_dt,
                capped_segments,
            ],
        ).fetch_df()
    finally:
        con.close()

    response = {
        "symbol_contract": symbol_contract,
        "timezone": getattr(response_zone, "key", str(response_zone)),
        "preset": normalized_preset,
        "metric": normalized_metric,
        "tick_size": _default_tick_size(symbol_contract, normalized_metric, normalized_tick_aggregation),
        "tick_aggregation": int(normalized_tick_aggregation),
        "value_area_enabled": False,
        "value_area_percent": 70.0,
        "profiles": [],
    }
    if segments.empty:
        return response

    segments = segments.sort_values("segment_end").reset_index(drop=True)
    profile_ids = [str(v) for v in segments["profile_id"].tolist()]
    placeholders = ",".join(["?"] * len(profile_ids))
    con = get_duckdb_connection()
    try:
        levels = con.execute(
            f"""
            SELECT profile_id, price_level, value
            FROM preset_profile_levels
            WHERE symbol_contract = ?
              AND profile_timezone = ?
              AND metric = ?
              AND preset = ?
              AND tick_aggregation = ?
              AND profile_id IN ({placeholders})
            ORDER BY profile_id, price_level
            """,
            [
                symbol_contract,
                CANONICAL_PROFILE_TIMEZONE,
                normalized_metric,
                normalized_preset,
                normalized_tick_aggregation,
                *profile_ids,
            ],
        ).fetch_df()
    finally:
        con.close()

    levels_by_profile: dict[str, list[dict]] = {profile_id: [] for profile_id in profile_ids}
    if not levels.empty:
        for row in levels.to_dict(orient="records"):
            pid = str(row.get("profile_id") or "")
            if pid not in levels_by_profile:
                continue
            price_level = _to_float_or_none(row.get("price_level"))
            level_value = _to_float_or_none(row.get("value"))
            if price_level is None or level_value is None:
                continue
            levels_by_profile[pid].append(
                {
                    "price_level": price_level,
                    "value": level_value,
                    # Backward compatibility for existing clients.
                    "volume": level_value,
                }
            )

    segment_rows = segments.to_dict(orient="records")
    profiles: list[dict] = []
    for row in segment_rows:
        profile_id = str(row.get("profile_id") or "")
        start_iso = pd.Timestamp(row["segment_start"]).tz_convert(response_zone).isoformat()
        end_iso = pd.Timestamp(row["segment_end"]).tz_convert(response_zone).isoformat()
        max_value = float(_to_float_or_none(row.get("max_value")) or 0.0)
        total_value = float(_to_float_or_none(row.get("total_value")) or 0.0)
        value_area_enabled = bool(row.get("value_area_enabled") or False)
        value_area_percent = float(_to_float_or_none(row.get("value_area_percent")) or 70.0)
        value_area_poc = _to_float_or_none(row.get("value_area_poc"))
        value_area_low = _to_float_or_none(row.get("value_area_low"))
        value_area_high = _to_float_or_none(row.get("value_area_high"))
        value_area_volume = float(_to_float_or_none(row.get("value_area_volume")) or 0.0)

        profiles.append(
            {
                "id": profile_id,
                "label": str(row.get("label") or ""),
                "start": start_iso,
                "end": end_iso,
                "max_value": max_value,
                "total_value": total_value,
                # Backward compatibility for existing clients.
                "max_volume": max_value,
                "total_volume": total_value,
                "value_area": {
                    "enabled": value_area_enabled,
                    "percent": value_area_percent,
                    "poc": value_area_poc,
                    "low": value_area_low,
                    "high": value_area_high,
                    "volume": value_area_volume,
                },
                "value_area_enabled": value_area_enabled,
                "value_area_percent": value_area_percent,
                "value_area_poc": value_area_poc,
                "value_area_low": value_area_low,
                "value_area_high": value_area_high,
                "value_area_volume": value_area_volume,
                "levels": levels_by_profile.get(profile_id, []),
            }
        )

    first = segment_rows[0]
    response["profiles"] = profiles
    response["tick_size"] = float(_to_float_or_none(first.get("tick_size")) or response["tick_size"])
    response["value_area_enabled"] = bool(first.get("value_area_enabled") or False)
    response["value_area_percent"] = float(_to_float_or_none(first.get("value_area_percent")) or 70.0)
    return response
