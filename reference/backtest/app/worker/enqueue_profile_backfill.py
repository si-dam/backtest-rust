from __future__ import annotations

import argparse

from app.db.models import Job, JobType
from app.db.postgres import SessionLocal
from app.services.queue import get_queue


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enqueue preset profile backfill worker job.")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["NQH26"],
        help="One or more symbol_contract values (default: NQH26).",
    )
    parser.add_argument(
        "--profile-timezone",
        default="America/New_York",
        help="Canonical profile timezone for persisted segments.",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Delete existing persisted rows for selected symbol/combo keys before rebuilding.",
    )
    return parser.parse_args(argv)


def enqueue_profile_backfill_job(
    *,
    symbols: list[str],
    profile_timezone: str,
    full_rebuild: bool,
) -> dict:
    db = SessionLocal()
    try:
        normalized_symbols = [str(symbol).strip() for symbol in symbols if str(symbol).strip()]
        if not normalized_symbols:
            normalized_symbols = ["NQH26"]

        job = Job(
            job_type=JobType.build_preset_profiles_backfill,
            payload={
                "symbols": normalized_symbols,
                "profile_timezone": profile_timezone,
                "full_rebuild": bool(full_rebuild),
                "source": "cli",
            },
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        queue = get_queue()
        rq_job = queue.enqueue(
            "app.worker.tasks.build_preset_profiles_backfill_job",
            str(job.id),
            normalized_symbols,
            profile_timezone,
            bool(full_rebuild),
            job_timeout="24h",
        )

        job.rq_job_id = rq_job.id
        db.commit()
        db.refresh(job)

        return {
            "job_id": str(job.id),
            "rq_job_id": str(rq_job.id),
            "symbols": normalized_symbols,
            "profile_timezone": profile_timezone,
            "full_rebuild": bool(full_rebuild),
        }
    finally:
        db.close()


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    result = enqueue_profile_backfill_job(
        symbols=list(args.symbols),
        profile_timezone=str(args.profile_timezone),
        full_rebuild=bool(args.full_rebuild),
    )
    print(result)


if __name__ == "__main__":
    main()
