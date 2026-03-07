from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from sqlalchemy.orm import Session

from app.config import get_settings
from app.db.models import Job, JobStatus, JobType
from app.services.ingest import list_watch_market_files
from app.services.queue import get_queue


_seen_paths: set[str] = set()
logger = logging.getLogger(__name__)


def scan_watch_files() -> list[Path]:
    settings = get_settings()
    watch_dir = Path(settings.watch_dir)
    return list_watch_market_files(watch_dir)


def enqueue_ingest_file(db: Session, file_path: Path, symbol_contract: str | None = None, rebuild: bool = False) -> Job:
    job = Job(
        job_type=JobType.ingest_csv,
        status=JobStatus.queued,
        payload={
            "file_path": str(file_path),
            "symbol_contract": symbol_contract,
            "rebuild": rebuild,
            "source": "watcher",
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


async def watcher_loop(db_factory):
    settings = get_settings()
    while True:
        try:
            files = scan_watch_files()
            for file_path in files:
                key = str(file_path.resolve())
                if key in _seen_paths:
                    continue
                db = db_factory()
                try:
                    enqueue_ingest_file(db, file_path)
                    _seen_paths.add(key)
                finally:
                    db.close()
        except Exception:
            logger.exception("Watcher loop iteration failed")
        await asyncio.sleep(settings.watcher_interval_seconds)
