import math
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query, status

from app.config import get_settings
from app.services.aggregation import (
    ensure_area_volume_profile,
    ensure_bars,
    ensure_session_profile,
    load_large_orders,
    load_ticks,
)
from app.services.market_processing import TIMEFRAME_RULES
from app.services.profile_store import load_persisted_preset_profiles

router = APIRouter(prefix="/chart", tags=["chart"])
PROFILE_METRICS = {"volume", "delta"}
BAR_TYPES = {"time", "tick", "volume", "range"}


def _normalize_timezone(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    try:
        ZoneInfo(normalized)
    except Exception as exc:  # pragma: no cover - defensive guard
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid timezone: {value}") from exc
    return normalized


def _parse_dt(v: str) -> datetime:
    try:
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo(get_settings().dataset_tz))
        return dt
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid datetime: {v}") from exc


def _normalize_timeframe(value: str) -> str:
    normalized = value.strip().lower()
    aliases = {
        "1h": "60m",
        "60min": "60m",
        "1d": "1d",
        "1day": "1d",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in TIMEFRAME_RULES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported timeframe: {value}",
        )
    return normalized


def _normalize_bar_type(value: str | None) -> str:
    normalized = str(value or "time").strip().lower()
    if normalized not in BAR_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported bar_type: {value}",
        )
    return normalized


def _parse_bar_size(value: int | None) -> int | None:
    if value is None:
        return None
    parsed = int(value)
    if parsed < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="bar_size must be >= 1",
        )
    return parsed


def _ensure_bars_or_400(
    symbol_contract: str,
    timeframe: str,
    start_dt: datetime,
    end_dt: datetime,
    bar_type: str = "time",
    bar_size: int | None = None,
):
    try:
        return ensure_bars(
            symbol_contract=symbol_contract,
            timeframe=timeframe,
            start=start_dt,
            end=end_dt,
            bar_type=bar_type,
            bar_size=bar_size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


def _records(df):
    out = df.copy()
    if "ts" in out.columns:
        out["ts"] = out["ts"].astype(str)
    if "session_date" in out.columns:
        out["session_date"] = out["session_date"].astype(str)
    return out.where(out.notna(), None).to_dict(orient="records")


@router.get("/bars")
def get_bars(
    symbol_contract: str,
    timeframe: str,
    start: str,
    end: str,
    bar_type: str = "time",
    bar_size: int | None = Query(default=None, ge=1),
):
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    normalized_bar_type = _normalize_bar_type(bar_type)
    normalized_bar_size = _parse_bar_size(bar_size)
    normalized_timeframe = _normalize_timeframe(timeframe) if normalized_bar_type == "time" else "1m"
    if normalized_bar_type != "time" and normalized_bar_size is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="bar_size is required for non-time bar_type")
    bars = _ensure_bars_or_400(
        symbol_contract=symbol_contract,
        timeframe=normalized_timeframe,
        start_dt=start_dt,
        end_dt=end_dt,
        bar_type=normalized_bar_type,
        bar_size=normalized_bar_size,
    )
    return _records(bars)


@router.get("/ticks")
def get_ticks(symbol_contract: str, start: str, end: str):
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    ticks = load_ticks(symbol_contract=symbol_contract, start=start_dt, end=end_dt)
    return _records(ticks)


@router.get("/overlays/volume")
def get_volume_overlay(
    symbol_contract: str,
    timeframe: str,
    start: str,
    end: str,
    bar_type: str = "time",
    bar_size: int | None = Query(default=None, ge=1),
):
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    normalized_bar_type = _normalize_bar_type(bar_type)
    normalized_bar_size = _parse_bar_size(bar_size)
    normalized_timeframe = _normalize_timeframe(timeframe) if normalized_bar_type == "time" else "1m"
    if normalized_bar_type != "time" and normalized_bar_size is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="bar_size is required for non-time bar_type")
    bars = _ensure_bars_or_400(
        symbol_contract=symbol_contract,
        timeframe=normalized_timeframe,
        start_dt=start_dt,
        end_dt=end_dt,
        bar_type=normalized_bar_type,
        bar_size=normalized_bar_size,
    )
    return _records(bars[["ts", "volume"]])


@router.get("/overlays/volume-profile")
def get_volume_profile_overlay(
    symbol_contract: str,
    session_date: str,
):
    profile = ensure_session_profile(symbol_contract=symbol_contract, session_date=session_date)
    return _records(profile)


@router.get("/overlays/volume-profiles/preset")
def get_volume_profiles_preset_overlay(
    symbol_contract: str,
    start: str,
    end: str,
    preset: str,
    timezone: str | None = None,
    metric: str = "volume",
    tick_size: float | None = None,
    tick_aggregation: int = Query(default=1, ge=1, le=1000),
    value_area_enabled: bool = False,
    value_area_percent: float = Query(default=70.0, gt=0.0, le=100.0),
    max_segments: int = Query(default=120, ge=1, le=1000),
):
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    normalized_metric = str(metric).strip().lower()
    if normalized_metric not in PROFILE_METRICS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported metric: {metric}",
        )
    if tick_size is not None and (not math.isfinite(tick_size) or tick_size <= 0):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="tick_size must be a finite value greater than 0",
        )
    if normalized_metric != "volume":
        value_area_enabled = False
    normalized_timezone = _normalize_timezone(timezone)
    try:
        return load_persisted_preset_profiles(
            symbol_contract=symbol_contract,
            start=start_dt,
            end=end_dt,
            preset=preset,
            profile_timezone=normalized_timezone,
            metric=normalized_metric,
            tick_aggregation=tick_aggregation,
            max_segments=max_segments,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/overlays/volume-profiles/area")
def get_volume_profiles_area_overlay(
    symbol_contract: str,
    start: str,
    end: str,
    price_min: float,
    price_max: float,
    area_id: str | None = None,
    timezone: str | None = None,
    metric: str = "volume",
    tick_size: float | None = None,
    tick_aggregation: int = Query(default=1, ge=1, le=1000),
    value_area_enabled: bool = False,
    value_area_percent: float = Query(default=70.0, gt=0.0, le=100.0),
):
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    normalized_metric = str(metric).strip().lower()
    if normalized_metric not in PROFILE_METRICS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported metric: {metric}",
        )
    if not math.isfinite(price_min) or not math.isfinite(price_max):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="price_min and price_max must be finite numeric values",
        )
    if tick_size is not None and (not math.isfinite(tick_size) or tick_size <= 0):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="tick_size must be a finite value greater than 0",
        )
    if normalized_metric != "volume":
        value_area_enabled = False
    normalized_timezone = _normalize_timezone(timezone)
    try:
        return ensure_area_volume_profile(
            symbol_contract=symbol_contract,
            start=start_dt,
            end=end_dt,
            price_min=price_min,
            price_max=price_max,
            area_id=area_id,
            profile_timezone=normalized_timezone,
            metric=normalized_metric,
            tick_size=tick_size,
            tick_aggregation=tick_aggregation,
            value_area_enabled=value_area_enabled,
            value_area_percent=value_area_percent,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/overlays/large-orders")
def get_large_orders_overlay(
    symbol_contract: str,
    start: str,
    end: str,
    method: str = Query(default="relative", pattern="^(fixed|relative)$"),
    fixed_threshold: float = 25.0,
    percentile: float = 99.0,
):
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    overlay = load_large_orders(
        symbol_contract=symbol_contract,
        start=start_dt,
        end=end_dt,
        method=method,
        fixed_threshold=fixed_threshold,
    )
    return _records(overlay)
