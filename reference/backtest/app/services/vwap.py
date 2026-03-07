from __future__ import annotations

import copy
from datetime import datetime

import numpy as np
import pandas as pd

from app.services.aggregation import (
    RTH_SESSION_END_MINUTES,
    RTH_SESSION_START_MINUTES,
    _cache_db_namespace,
    _dt_cache_token,
    _resolve_profile_zone,
    _to_zone,
    _zone_key,
    load_ticks_for_profile_window,
)
from app.services.runtime_cache import TTLCache

VWAP_PRESETS = {"week", "day", "rth"}

_VWAP_PRESET_CACHE = TTLCache(ttl_seconds=15.0, max_entries=160)


def clear_vwap_runtime_caches() -> None:
    _VWAP_PRESET_CACHE.clear()


def _segment_points(group: pd.DataFrame) -> list[dict]:
    points = group[["local_ts", "trade_price", "trade_size"]].copy()
    points["trade_price"] = pd.to_numeric(points["trade_price"], errors="coerce")
    points["trade_size"] = pd.to_numeric(points["trade_size"], errors="coerce")
    points = points.dropna(subset=["local_ts", "trade_price", "trade_size"])
    points = points[np.isfinite(points["trade_price"]) & np.isfinite(points["trade_size"])].copy()
    points = points[points["trade_size"] > 0].copy()
    if points.empty:
        return []

    points = points.sort_values("local_ts").reset_index(drop=True)
    price = points["trade_price"].to_numpy(dtype=float)
    volume = points["trade_size"].to_numpy(dtype=float)

    cum_volume = np.cumsum(volume)
    cum_pv = np.cumsum(price * volume)
    vwap = cum_pv / cum_volume

    cum_p2v = np.cumsum((price * price) * volume)
    variance = (cum_p2v / cum_volume) - (vwap * vwap)
    variance = np.maximum(variance, 0.0)
    offset = np.sqrt(variance)

    ts_values = pd.to_datetime(points["local_ts"], errors="coerce").tolist()
    return [
        {
            "ts": ts.isoformat(),
            "vwap": float(center),
            "upper_1": float(center + dev),
            "lower_1": float(center - dev),
            "upper_2": float(center + (2.0 * dev)),
            "lower_2": float(center - (2.0 * dev)),
        }
        for ts, center, dev in zip(ts_values, vwap.tolist(), offset.tolist())
        if pd.notna(ts)
    ]


def ensure_preset_vwap(
    symbol_contract: str,
    start: datetime,
    end: datetime,
    preset: str,
    profile_timezone: str | None = None,
    max_segments: int = 120,
) -> dict:
    normalized_preset = str(preset or "").strip().lower()
    if normalized_preset not in VWAP_PRESETS:
        raise ValueError(f"Unsupported preset: {preset}")

    try:
        profile_zone = _resolve_profile_zone(profile_timezone)
    except Exception as exc:
        raise ValueError(f"Invalid profile timezone: {profile_timezone}") from exc

    start_dt = _to_zone(start, profile_zone)
    end_dt = _to_zone(end, profile_zone)
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt

    capped_segments = max(1, int(max_segments))
    cache_key = (
        "vwap_preset",
        _cache_db_namespace(),
        str(symbol_contract),
        _dt_cache_token(start_dt),
        _dt_cache_token(end_dt),
        normalized_preset,
        _zone_key(profile_zone),
        int(capped_segments),
    )
    cached = _VWAP_PRESET_CACHE.get(cache_key)
    if cached is not None:
        return copy.deepcopy(cached)

    response = {
        "symbol_contract": symbol_contract,
        "timezone": getattr(profile_zone, "key", str(profile_zone)),
        "preset": normalized_preset,
        "segments": [],
    }

    df = load_ticks_for_profile_window(
        symbol_contract=symbol_contract,
        start=start_dt,
        end=end_dt,
        profile_zone=profile_zone,
    )
    if df.empty:
        _VWAP_PRESET_CACHE.set(cache_key, copy.deepcopy(response))
        return response

    one_day = pd.to_timedelta(1, unit="D")
    rth_offset = pd.to_timedelta(RTH_SESSION_START_MINUTES, unit="m")
    rth_duration = pd.to_timedelta(RTH_SESSION_END_MINUTES - RTH_SESSION_START_MINUTES, unit="m")

    segment_rows: list[tuple[str, str, pd.DataFrame, datetime, datetime]] = []

    if normalized_preset == "day":
        for session_start_exchange, group in df.groupby("exchange_day_session_start", sort=True):
            session_start = pd.Timestamp(session_start_exchange)
            key = session_start.strftime("%Y-%m-%d")
            segment_start = session_start.tz_convert(profile_zone).to_pydatetime()
            segment_end = (session_start + one_day).tz_convert(profile_zone).to_pydatetime()
            segment_rows.append((key, "Day", group, segment_start, segment_end))
    elif normalized_preset == "week":
        iso = df["exchange_ts"].dt.isocalendar()
        grouped = df.assign(_week_year=iso["year"].astype(int), _week_num=iso["week"].astype(int))
        for (year, week), group in grouped.groupby(["_week_year", "_week_num"], sort=True):
            key = f"{int(year)}-W{int(week):02d}"
            segment_start = pd.Timestamp(group["local_ts"].min()).to_pydatetime()
            segment_end = pd.Timestamp(group["local_ts"].max()).to_pydatetime()
            segment_rows.append((key, "Week", group, segment_start, segment_end))
    else:
        # RTH session boundaries are anchored in ET.
        session_starts = (df["exchange_ts"] - rth_offset).dt.floor("D") + rth_offset
        minutes = (df["exchange_ts"].dt.hour * 60) + df["exchange_ts"].dt.minute
        rth_mask = (minutes >= RTH_SESSION_START_MINUTES) & (minutes < RTH_SESSION_END_MINUTES)
        filtered = df[rth_mask].copy()
        if not filtered.empty:
            filtered["exchange_rth_session_start"] = session_starts[rth_mask]
            for session_start_exchange, group in filtered.groupby("exchange_rth_session_start", sort=True):
                session_start = pd.Timestamp(session_start_exchange)
                key = session_start.strftime("%Y-%m-%d")
                segment_start = session_start.tz_convert(profile_zone).to_pydatetime()
                segment_end = (session_start + rth_duration).tz_convert(profile_zone).to_pydatetime()
                segment_rows.append((key, "RTH", group, segment_start, segment_end))

    segments_with_end: list[tuple[datetime, dict]] = []
    for key, label_prefix, group, segment_start, segment_end in segment_rows:
        points = _segment_points(group)
        if not points:
            continue
        segments_with_end.append(
            (
                segment_end,
                {
                    "id": f"{normalized_preset}-{key}",
                    "label": f"{label_prefix} {key}",
                    "start": segment_start.isoformat(),
                    "end": segment_end.isoformat(),
                    "points": points,
                },
            )
        )

    segments_with_end.sort(key=lambda row: row[0])
    if len(segments_with_end) > capped_segments:
        segments_with_end = segments_with_end[-capped_segments:]

    response["segments"] = [row[1] for row in segments_with_end]
    _VWAP_PRESET_CACHE.set(cache_key, copy.deepcopy(response))
    return response
