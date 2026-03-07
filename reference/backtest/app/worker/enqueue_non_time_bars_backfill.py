from __future__ import annotations

import argparse

from app.db.models import Job, JobType
from app.db.postgres import SessionLocal
from app.services.aggregation import list_symbols_with_ticks
from app.services.queue import get_queue


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enqueue non-time bar preset backfill worker job.")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=[],
        help="Optional symbol_contract list. Defaults to all symbols with ticks.",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Rebuild preset bars for selected symbols from full tick history.",
    )
    parser.add_argument(
        "--presets",
        nargs="+",
        default=[],
        help="Optional preset keys such as tick:1500 volume:750 range:40. Defaults to built-in non-time presets.",
    )
    return parser.parse_args(argv)


def enqueue_non_time_bars_backfill_job(
    *,
    symbols: list[str],
    full_rebuild: bool,
    preset_keys: list[str] | None = None,
) -> dict:
    db = SessionLocal()
    try:
        normalized_symbols = [str(symbol).strip() for symbol in symbols if str(symbol).strip()]
        if not normalized_symbols:
            normalized_symbols = list_symbols_with_ticks()
        normalized_preset_keys = [str(key).strip().lower() for key in (preset_keys or []) if str(key).strip()]

        job = Job(
            job_type=JobType.build_bars,
            payload={
                "symbols": normalized_symbols,
                "full_rebuild": bool(full_rebuild),
                "source": "cli",
                "bar_presets": normalized_preset_keys if normalized_preset_keys else "non_time_defaults",
            },
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        queue = get_queue()
        rq_job = queue.enqueue(
            "app.worker.tasks.build_non_time_bars_backfill_job",
            str(job.id),
            normalized_symbols,
            bool(full_rebuild),
            normalized_preset_keys,
            job_timeout="24h",
        )

        job.rq_job_id = rq_job.id
        db.commit()
        db.refresh(job)

        return {
            "job_id": str(job.id),
            "rq_job_id": str(rq_job.id),
            "symbols": normalized_symbols,
            "full_rebuild": bool(full_rebuild),
            "preset_keys": normalized_preset_keys,
        }
    finally:
        db.close()


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    result = enqueue_non_time_bars_backfill_job(
        symbols=list(args.symbols),
        full_rebuild=bool(args.full_rebuild),
        preset_keys=list(args.presets),
    )
    print(result)


if __name__ == "__main__":
    main()
