from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query, status

from app.config import get_settings
from app.services.vwap import ensure_preset_vwap

router = APIRouter(prefix="/chart", tags=["chart"])


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


def _parse_dt(value: str) -> datetime:
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo(get_settings().dataset_tz))
        return dt
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid datetime: {value}") from exc


@router.get("/overlays/vwap/preset")
def get_vwap_preset_overlay(
    symbol_contract: str,
    start: str,
    end: str,
    preset: str,
    timezone: str | None = None,
    max_segments: int = Query(default=120, ge=1, le=1000),
):
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    normalized_timezone = _normalize_timezone(timezone)
    try:
        return ensure_preset_vwap(
            symbol_contract=symbol_contract,
            start=start_dt,
            end=end_dt,
            preset=preset,
            profile_timezone=normalized_timezone,
            max_segments=max_segments,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
