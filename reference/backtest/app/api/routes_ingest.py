from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db.models import Job, JobType
from app.db.postgres import get_db_session
from app.db.schemas import IngestJobRequest, JobRead, LargeOrdersBackfillJobRequest
from app.services.ingest import (
    SUPPORTED_INGEST_SUFFIXES,
    is_supported_ingest_file,
    list_watch_market_files,
    validate_market_data_file,
)
from app.services.queue import get_queue, get_queue_health

router = APIRouter(prefix="/ingest", tags=["ingest"])


def _enqueue_ingest_job(db: Session, file_path: Path, symbol_contract: str | None, rebuild: bool) -> Job:
    job = Job(
        job_type=JobType.ingest_csv,
        payload={
            "file_path": str(file_path),
            "symbol_contract": symbol_contract,
            "rebuild": rebuild,
            "source": "api",
        },
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    queue = get_queue()
    rq_job = queue.enqueue(
        "app.worker.tasks.ingest_csv_job",
        str(job.id),
        str(file_path),
        symbol_contract,
        rebuild,
        job_timeout="30m",
    )
    job.rq_job_id = rq_job.id
    db.commit()
    db.refresh(job)
    return job


def _enqueue_large_orders_backfill_job(db: Session, symbols: list[str], threshold: float) -> Job:
    normalized_symbols = [str(symbol).strip() for symbol in symbols if str(symbol).strip()]
    if not normalized_symbols:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="symbols must include at least one symbol")

    job = Job(
        job_type=JobType.build_large_orders,
        payload={
            "symbols": normalized_symbols,
            "threshold": float(threshold),
            "source": "api",
        },
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    queue = get_queue()
    rq_job = queue.enqueue(
        "app.worker.tasks.build_large_orders_backfill_job",
        str(job.id),
        normalized_symbols,
        float(threshold),
        job_timeout="6h",
    )
    job.rq_job_id = rq_job.id
    db.commit()
    db.refresh(job)
    return job


@router.post("/jobs")
def create_ingest_jobs(
    payload: IngestJobRequest,
    db: Session = Depends(get_db_session),
):
    settings = get_settings()
    watch_dir = Path(settings.watch_dir)
    watch_dir.mkdir(parents=True, exist_ok=True)
    watch_root = watch_dir.resolve()

    targets: list[Path] = []
    if payload.file_name:
        candidate = (watch_dir / payload.file_name).resolve()
        if watch_root not in candidate.parents:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Requested file must be inside watch dir")
        if not candidate.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Requested file not found in watch dir")
        if not is_supported_ingest_file(candidate):
            suffixes = ", ".join(SUPPORTED_INGEST_SUFFIXES)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported file extension. Accepted extensions: {suffixes}",
            )
        targets = [candidate]
    elif payload.scan_watch_dir:
        targets = list_watch_market_files(watch_dir)

    if not targets:
        suffixes = ", ".join(SUPPORTED_INGEST_SUFFIXES)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"No supported market data files found for ingest ({suffixes})",
        )

    preflight_errors: list[str] = []
    for file_path in targets:
        relative_name = str(file_path.resolve().relative_to(watch_root))
        try:
            validate_market_data_file(
                file_path,
                dataset_tz=settings.dataset_tz,
                symbol_contract=payload.symbol_contract,
            )
        except Exception as exc:
            preflight_errors.append(f"{relative_name}: {exc}")

    if preflight_errors:
        joined = "; ".join(preflight_errors)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Preflight failed for one or more files: {joined}",
        )

    jobs = [
        _enqueue_ingest_job(db, file_path=t, symbol_contract=payload.symbol_contract, rebuild=payload.rebuild)
        for t in targets
    ]
    return {"job_ids": [str(j.id) for j in jobs], "count": len(jobs)}


@router.post("/large-orders/jobs")
def create_large_orders_backfill_job(
    payload: LargeOrdersBackfillJobRequest,
    db: Session = Depends(get_db_session),
):
    job = _enqueue_large_orders_backfill_job(db, payload.symbols, float(payload.threshold))
    return {"job_id": str(job.id), "rq_job_id": str(job.rq_job_id)}


@router.get("/jobs/{job_id}", response_model=JobRead)
def get_ingest_job(job_id: str, db: Session = Depends(get_db_session)) -> Job:
    try:
        job_uuid = UUID(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid job ID") from exc

    job = db.scalar(select(Job).where(Job.id == job_uuid, Job.job_type == JobType.ingest_csv))
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ingest job not found")
    return job


@router.get("/large-orders/jobs/{job_id}", response_model=JobRead)
def get_large_orders_backfill_job(job_id: str, db: Session = Depends(get_db_session)) -> Job:
    try:
        job_uuid = UUID(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid job ID") from exc

    job = db.scalar(select(Job).where(Job.id == job_uuid, Job.job_type == JobType.build_large_orders))
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Large-orders backfill job not found")
    return job


@router.get("/jobs")
def list_ingest_jobs(
    limit: int = Query(default=25, ge=1, le=200),
    db: Session = Depends(get_db_session),
):
    jobs = db.scalars(
        select(Job)
        .where(Job.job_type == JobType.ingest_csv)
        .order_by(desc(Job.created_at))
        .limit(limit)
    ).all()
    return [
        {
            "id": str(j.id),
            "status": j.status.value,
            "payload": j.payload,
            "result": j.result,
            "error": j.error,
            "created_at": j.created_at.isoformat(),
        }
        for j in jobs
    ]


@router.get("/large-orders/jobs")
def list_large_orders_backfill_jobs(
    limit: int = Query(default=25, ge=1, le=200),
    db: Session = Depends(get_db_session),
):
    jobs = db.scalars(
        select(Job)
        .where(Job.job_type == JobType.build_large_orders)
        .order_by(desc(Job.created_at))
        .limit(limit)
    ).all()
    return [
        {
            "id": str(j.id),
            "status": j.status.value,
            "payload": j.payload,
            "result": j.result,
            "error": j.error,
            "created_at": j.created_at.isoformat(),
        }
        for j in jobs
    ]


@router.get("/watch-files")
def list_watch_files():
    settings = get_settings()
    watch_dir = Path(settings.watch_dir)
    watch_root = watch_dir.resolve()
    files = list_watch_market_files(watch_dir)
    return [
        {"name": str(p.resolve().relative_to(watch_root)), "size_bytes": p.stat().st_size}
        for p in files
    ]


@router.get("/queue-health")
def queue_health():
    try:
        return get_queue_health()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Queue health unavailable: {exc}",
        ) from exc
