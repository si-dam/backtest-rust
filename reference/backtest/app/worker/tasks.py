from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select

from app.db.models import Job, JobStatus
from app.db.postgres import SessionLocal
from app.services.backtest import run_backtest_scaffold, run_sweep_scaffold
from app.services.aggregation import (
    list_symbols_with_ticks,
    precompute_non_time_bar_presets,
    resolve_non_time_presets,
)
from app.services.ingest import process_csv_ingest
from app.services.large_orders_backfill import run_large_orders_backfill
from app.services.profile_backfill import run_preset_profile_backfill


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _get_job_or_raise(job_id: str, db) -> Job:
    job_uuid = UUID(job_id)
    job = db.scalar(select(Job).where(Job.id == job_uuid))
    if not job:
        raise ValueError(f"Job {job_id} not found")
    return job


def ingest_csv_job(job_id: str, file_path: str, symbol_contract: str | None, rebuild: bool) -> dict:
    db = SessionLocal()
    try:
        job = _get_job_or_raise(job_id, db)
        job.status = JobStatus.running
        job.updated_at = _utc_now()
        db.commit()

        result = process_csv_ingest(db=db, job=job, file_path=file_path, symbol_contract=symbol_contract, rebuild=rebuild)

        job.status = JobStatus.succeeded
        job.result = result
        job.updated_at = _utc_now()
        db.commit()
        return result
    except Exception as exc:
        db.rollback()
        job = _get_job_or_raise(job_id, db)
        job.status = JobStatus.failed
        job.error = {"message": str(exc), "type": type(exc).__name__}
        job.updated_at = _utc_now()
        db.commit()
        raise
    finally:
        db.close()


def run_backtest_job(job_id: str, name: str, strategy_id: str, params: dict) -> dict:
    db = SessionLocal()
    try:
        job = _get_job_or_raise(job_id, db)
        job.status = JobStatus.running
        job.updated_at = _utc_now()
        db.commit()

        result = run_backtest_scaffold(db=db, job=job, name=name, strategy_id=strategy_id, params=params)

        job.status = JobStatus.succeeded
        job.result = result
        job.updated_at = _utc_now()
        db.commit()
        return result
    except Exception as exc:
        db.rollback()
        job = _get_job_or_raise(job_id, db)
        job.status = JobStatus.failed
        job.error = {"message": str(exc), "type": type(exc).__name__}
        job.updated_at = _utc_now()
        db.commit()
        raise
    finally:
        db.close()


def run_sweep_job(job_id: str, name: str, strategy_id: str, params: dict) -> dict:
    db = SessionLocal()
    try:
        job = _get_job_or_raise(job_id, db)
        job.status = JobStatus.running
        job.updated_at = _utc_now()
        db.commit()

        result = run_sweep_scaffold(db=db, job=job, name=name, strategy_id=strategy_id, params=params)

        job.status = JobStatus.succeeded
        job.result = result
        job.updated_at = _utc_now()
        db.commit()
        return result
    except Exception as exc:
        db.rollback()
        job = _get_job_or_raise(job_id, db)
        job.status = JobStatus.failed
        job.error = {"message": str(exc), "type": type(exc).__name__}
        job.updated_at = _utc_now()
        db.commit()
        raise
    finally:
        db.close()


def build_preset_profiles_backfill_job(
    job_id: str,
    symbols: list[str],
    profile_timezone: str,
    full_rebuild: bool,
) -> dict:
    db = SessionLocal()
    try:
        job = _get_job_or_raise(job_id, db)
        job.status = JobStatus.running
        job.updated_at = _utc_now()
        db.commit()

        result = run_preset_profile_backfill(
            symbols=symbols,
            profile_timezone=profile_timezone,
            full_rebuild=full_rebuild,
        )

        job.status = JobStatus.succeeded
        job.result = result
        job.updated_at = _utc_now()
        db.commit()
        return result
    except Exception as exc:
        db.rollback()
        job = _get_job_or_raise(job_id, db)
        job.status = JobStatus.failed
        job.error = {"message": str(exc), "type": type(exc).__name__}
        job.updated_at = _utc_now()
        db.commit()
        raise
    finally:
        db.close()


def build_large_orders_backfill_job(
    job_id: str,
    symbols: list[str],
    threshold: float = 20.0,
) -> dict:
    db = SessionLocal()
    try:
        job = _get_job_or_raise(job_id, db)
        job.status = JobStatus.running
        job.updated_at = _utc_now()
        db.commit()

        result = run_large_orders_backfill(
            symbols=symbols,
            threshold=threshold,
        )

        job.status = JobStatus.succeeded
        job.result = result
        job.updated_at = _utc_now()
        db.commit()
        return result
    except Exception as exc:
        db.rollback()
        job = _get_job_or_raise(job_id, db)
        job.status = JobStatus.failed
        job.error = {"message": str(exc), "type": type(exc).__name__}
        job.updated_at = _utc_now()
        db.commit()
        raise
    finally:
        db.close()


def build_non_time_bars_backfill_job(
    job_id: str,
    symbols: list[str],
    full_rebuild: bool,
    preset_keys: list[str] | None = None,
) -> dict:
    db = SessionLocal()
    try:
        job = _get_job_or_raise(job_id, db)
        job.status = JobStatus.running
        job.updated_at = _utc_now()
        db.commit()

        normalized_symbols = [str(symbol).strip() for symbol in symbols if str(symbol).strip()]
        if not normalized_symbols:
            normalized_symbols = list_symbols_with_ticks()
        resolved_presets = resolve_non_time_presets(preset_keys)

        per_symbol = [
            precompute_non_time_bar_presets(
                symbol_contract=symbol_contract,
                full_rebuild=bool(full_rebuild),
                presets=resolved_presets,
            )
            for symbol_contract in normalized_symbols
        ]
        result = {
            "symbols_requested": normalized_symbols,
            "symbol_count": len(normalized_symbols),
            "full_rebuild": bool(full_rebuild),
            "preset_keys": [f"{bar_type}:{bar_size}" for bar_type, bar_size in resolved_presets],
            "results": per_symbol,
        }

        job.status = JobStatus.succeeded
        job.result = result
        job.updated_at = _utc_now()
        db.commit()
        return result
    except Exception as exc:
        db.rollback()
        job = _get_job_or_raise(job_id, db)
        job.status = JobStatus.failed
        job.error = {"message": str(exc), "type": type(exc).__name__}
        job.updated_at = _utc_now()
        db.commit()
        raise
    finally:
        db.close()
