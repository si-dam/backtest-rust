import csv
import io
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db.models import BacktestRun, BacktestTrade, Job, JobType
from app.db.postgres import get_db_session
from app.db.schemas import BacktestJobRequest
from app.services.backtest import list_backtest_strategies
from app.services.backtest_analytics import build_backtest_analytics, parse_trade_notes
from app.services.queue import get_queue

router = APIRouter(prefix="/backtests", tags=["backtests"])


def _parse_uuid_or_400(raw: str, field: str) -> UUID:
    try:
        return UUID(raw)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid {field}") from exc


def _serialize_run(run: BacktestRun, trade_count: int | None = None) -> dict:
    payload = {
        "id": str(run.id),
        "name": run.name,
        "strategy_id": run.strategy_id,
        "params": run.params,
        "metrics": run.metrics,
        "status": run.status,
        "created_at": run.created_at.isoformat(),
    }
    if trade_count is not None:
        payload["trade_count"] = int(trade_count)
    return payload


def _serialize_trade(trade: BacktestTrade) -> dict:
    return {
        "id": str(trade.id),
        "symbol_contract": trade.symbol_contract,
        "entry_ts": trade.entry_ts.isoformat() if trade.entry_ts else None,
        "exit_ts": trade.exit_ts.isoformat() if trade.exit_ts else None,
        "entry_price": trade.entry_price,
        "exit_price": trade.exit_price,
        "qty": trade.qty,
        "pnl": trade.pnl,
        "side": "long" if (trade.qty or 0) > 0 else "short" if (trade.qty or 0) < 0 else "flat",
        "notes": trade.notes,
    }


@router.post("/jobs")
def create_backtest_job(
    payload: BacktestJobRequest,
    db: Session = Depends(get_db_session),
):
    job_type = JobType.run_backtest if payload.mode == "run" else JobType.run_sweep
    job = Job(
        job_type=job_type,
        payload={
            "mode": payload.mode,
            "name": payload.name,
            "strategy_id": payload.strategy_id,
            "params": payload.params,
        },
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    queue = get_queue()
    if payload.mode == "run":
        rq_job = queue.enqueue(
            "app.worker.tasks.run_backtest_job",
            str(job.id),
            payload.name,
            payload.strategy_id,
            payload.params,
            job_timeout="20m",
        )
    else:
        rq_job = queue.enqueue(
            "app.worker.tasks.run_sweep_job",
            str(job.id),
            payload.name,
            payload.strategy_id,
            payload.params,
            job_timeout="40m",
        )

    job.rq_job_id = rq_job.id
    db.commit()
    db.refresh(job)
    return {"job_id": str(job.id), "rq_job_id": rq_job.id}


@router.get("/jobs/{job_id}")
def get_backtest_job(job_id: str, db: Session = Depends(get_db_session)):
    job_uuid = _parse_uuid_or_400(job_id, "job ID")
    job = db.scalar(select(Job).where(Job.id == job_uuid))
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backtest job not found")
    return {
        "id": str(job.id),
        "job_type": job.job_type.value,
        "status": job.status.value,
        "payload": job.payload,
        "result": job.result,
        "error": job.error,
        "created_at": job.created_at.isoformat(),
        "updated_at": job.updated_at.isoformat(),
    }


@router.get("/runs")
def list_runs(db: Session = Depends(get_db_session)):
    runs = db.scalars(select(BacktestRun).order_by(desc(BacktestRun.created_at))).all()
    if not runs:
        return []

    run_ids = [r.id for r in runs]
    count_rows = db.execute(
        select(BacktestTrade.run_id, func.count(BacktestTrade.id))
        .where(BacktestTrade.run_id.in_(run_ids))
        .group_by(BacktestTrade.run_id)
    ).all()
    trade_count_by_run = {run_id: int(count) for run_id, count in count_rows}

    return [_serialize_run(r, trade_count=trade_count_by_run.get(r.id, 0)) for r in runs]


@router.get("/strategies")
def list_strategies():
    return list_backtest_strategies()


@router.get("/runs/{run_id}")
def get_run(run_id: str, db: Session = Depends(get_db_session)):
    run_uuid = _parse_uuid_or_400(run_id, "run ID")
    run = db.scalar(select(BacktestRun).where(BacktestRun.id == run_uuid))
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backtest run not found")

    trade_count = db.scalar(select(func.count(BacktestTrade.id)).where(BacktestTrade.run_id == run_uuid)) or 0
    return _serialize_run(run, trade_count=int(trade_count))


@router.get("/runs/{run_id}/trades")
def list_run_trades(run_id: str, db: Session = Depends(get_db_session)):
    run_uuid = _parse_uuid_or_400(run_id, "run ID")
    trades = db.scalars(select(BacktestTrade).where(BacktestTrade.run_id == run_uuid)).all()
    return [_serialize_trade(t) for t in trades]


@router.get("/runs/{run_id}/analytics")
def get_run_analytics(run_id: str, db: Session = Depends(get_db_session)):
    run_uuid = _parse_uuid_or_400(run_id, "run ID")
    run = db.scalar(select(BacktestRun).where(BacktestRun.id == run_uuid))
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backtest run not found")

    trades = db.scalars(select(BacktestTrade).where(BacktestTrade.run_id == run_uuid)).all()
    analytics = build_backtest_analytics(trades=trades, timezone=get_settings().dataset_tz)
    return {
        "run": _serialize_run(run, trade_count=len(trades)),
        **analytics,
    }


@router.get("/runs/{run_id}/export/config.json")
def export_run_config(run_id: str, db: Session = Depends(get_db_session)):
    run_uuid = _parse_uuid_or_400(run_id, "run ID")
    run = db.scalar(select(BacktestRun).where(BacktestRun.id == run_uuid))
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backtest run not found")

    return {
        "run_id": str(run.id),
        "name": run.name,
        "strategy_id": run.strategy_id,
        "params": run.params,
        "status": run.status,
        "created_at": run.created_at.isoformat(),
    }


@router.get("/runs/{run_id}/export/trades.csv")
def export_run_trades_csv(run_id: str, db: Session = Depends(get_db_session)):
    run_uuid = _parse_uuid_or_400(run_id, "run ID")
    run = db.scalar(select(BacktestRun).where(BacktestRun.id == run_uuid))
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backtest run not found")

    trades = db.scalars(select(BacktestTrade).where(BacktestTrade.run_id == run_uuid)).all()

    base_rows: list[dict] = []
    notes_keys: set[str] = set()
    for trade in trades:
        notes = parse_trade_notes(trade.notes)
        notes_keys.update(str(key) for key in notes.keys())
        base_rows.append(
            {
                "id": str(trade.id),
                "run_id": str(trade.run_id),
                "symbol_contract": trade.symbol_contract,
                "entry_ts": trade.entry_ts.isoformat() if trade.entry_ts else "",
                "exit_ts": trade.exit_ts.isoformat() if trade.exit_ts else "",
                "entry_price": trade.entry_price if trade.entry_price is not None else "",
                "exit_price": trade.exit_price if trade.exit_price is not None else "",
                "qty": trade.qty if trade.qty is not None else "",
                "pnl": trade.pnl if trade.pnl is not None else "",
                "side": "long" if (trade.qty or 0) > 0 else "short" if (trade.qty or 0) < 0 else "flat",
                "notes": trade.notes or "",
                "_decoded_notes": notes,
            }
        )

    note_columns = sorted(notes_keys)
    fieldnames = [
        "id",
        "run_id",
        "symbol_contract",
        "entry_ts",
        "exit_ts",
        "entry_price",
        "exit_price",
        "qty",
        "pnl",
        "side",
        "notes",
        *note_columns,
    ]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in base_rows:
        notes = row.pop("_decoded_notes")
        for key in note_columns:
            value = notes.get(key)
            row[key] = "" if value is None else value
        writer.writerow(row)

    csv_text = output.getvalue()
    filename = f"backtest-trades-{run.id}.csv"
    return Response(
        content=csv_text,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
