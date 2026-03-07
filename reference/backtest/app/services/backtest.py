from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy.orm import Session

from app.db.models import BacktestRun, BacktestTrade, Job
from app.services.aggregation import ensure_bars
from app.strategy.orb_breakout_v1 import (
    merge_params,
    simulate_orb_breakout_strategy,
    strategy_metadata,
    summarize_breakout_trades,
)

RTH_SESSION_START = "09:30:00"
RTH_SESSION_END = "16:00:00"
FULL_DAY_SESSION_START = "00:00:00"
FULL_DAY_SESSION_END = "23:59:59"


def _parse_iso_datetime(value: str, field_name: str) -> datetime:
    ts = pd.to_datetime(value, errors="raise")
    if ts.tzinfo is None:
        raise ValueError(f"{field_name} must include timezone offset")
    return ts.to_pydatetime()


def _coerce_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value}")


def _parse_split_config(payload: dict, start_dt: datetime, end_dt: datetime) -> dict | None:
    split_raw = payload.get("split")
    if not isinstance(split_raw, dict):
        return None
    enabled = _coerce_bool(split_raw.get("enabled"), default=False)
    if not enabled:
        return None

    split_at_raw = split_raw.get("split_at")
    if not split_at_raw:
        raise ValueError("split.split_at is required when split.enabled is true")

    split_at_dt = _parse_iso_datetime(str(split_at_raw), "split.split_at")
    if split_at_dt <= start_dt or split_at_dt >= end_dt:
        raise ValueError("split.split_at must be strictly between start and end")

    return {
        "enabled": True,
        "split_at": pd.Timestamp(split_at_dt).isoformat(),
        "split_at_dt": split_at_dt,
    }


def _resolve_session_window(payload: dict, strategy_params: dict) -> tuple[str, str, bool]:
    if "rth_only" in payload:
        rth_only = _coerce_bool(payload.get("rth_only"), default=True)
        if rth_only:
            return RTH_SESSION_START, RTH_SESSION_END, True
        return FULL_DAY_SESSION_START, FULL_DAY_SESSION_END, False

    # Backward compatibility for legacy payloads that explicitly carry session_* fields.
    if "session_start" in payload or "session_end" in payload:
        return (
            str(strategy_params.get("session_start") or RTH_SESSION_START),
            str(strategy_params.get("session_end") or RTH_SESSION_END),
            _coerce_bool(strategy_params.get("rth_only"), default=True),
        )

    rth_default = _coerce_bool(strategy_params.get("rth_only"), default=True)
    if rth_default:
        return RTH_SESSION_START, RTH_SESSION_END, True
    return FULL_DAY_SESSION_START, FULL_DAY_SESSION_END, False


def list_backtest_strategies() -> list[dict]:
    return [
        {
            "id": "scaffold",
            "label": "Scaffold",
            "description": "Placeholder strategy used for smoke tests.",
            "defaults": {
                "name": "Scaffold Backtest",
                "timeframe": "1m",
            },
            "params": [
                {"name": "symbol_contract", "type": "string", "required": False},
                {"name": "start", "type": "datetime", "required": False},
                {"name": "end", "type": "datetime", "required": False},
            ],
        },
        strategy_metadata(),
    ]


def _run_scaffold_backtest(db: Session, job: Job, name: str, strategy_id: str, params: dict) -> dict:
    run = BacktestRun(
        job_id=job.id,
        owner_id=job.owner_id,
        name=name,
        strategy_id=strategy_id,
        params=params,
        metrics={
            "net_pnl": 0.0,
            "win_rate": 0.0,
            "max_drawdown": 0.0,
            "notes": "Scaffold run. No strategy logic executed yet.",
        },
        status="completed",
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return {
        "run_id": str(run.id),
        "run_ids": [str(run.id)],
        "mode": "run",
        "strategy_id": strategy_id,
        "trade_count": 0,
        "created_runs": 1,
        "failed_runs": 0,
    }


def _build_orb_segment(
    db: Session,
    job: Job,
    name: str,
    payload: dict,
    *,
    segment_name: str | None,
    segment_start: datetime,
    segment_end: datetime,
    session_start: str,
    session_end: str,
    rth_only: bool,
    split_meta: dict | None,
    batch_meta: dict | None,
) -> dict:
    symbol_contract = str(payload.get("symbol_contract") or "").strip()
    strategy_params = merge_params(payload)
    strategy_params["strategy_mode"] = "breakout_only"

    normalized_params = {
        **strategy_params,
        "symbol_contract": symbol_contract,
        "start": pd.Timestamp(segment_start).isoformat(),
        "end": pd.Timestamp(segment_end).isoformat(),
        "session_start": session_start,
        "session_end": session_end,
        "rth_only": rth_only,
    }
    if split_meta:
        normalized_params["split"] = split_meta
    if batch_meta:
        normalized_params["batch"] = batch_meta

    bars = ensure_bars(
        symbol_contract=symbol_contract,
        timeframe=str(strategy_params["timeframe"]),
        start=segment_start,
        end=segment_end,
    )

    run_name = name if not segment_name else f"{name} [{segment_name.upper()}]"
    run = BacktestRun(
        job_id=job.id,
        owner_id=job.owner_id,
        name=run_name,
        strategy_id="orb_breakout_v1",
        params=normalized_params,
        metrics={},
        status="completed",
    )
    db.add(run)
    db.flush()

    trades_df = pd.DataFrame()
    contracts = int(strategy_params.get("contracts") or 1)
    if not bars.empty:
        bars_for_strategy = bars[["ts", "open", "high", "low", "close", "volume"]].copy()
        trades_df = simulate_orb_breakout_strategy(
            bars=bars_for_strategy,
            timeframe=str(strategy_params["timeframe"]),
            ib_minutes=int(strategy_params["ib_minutes"]),
            session_start=session_start,
            session_end=session_end,
            stop_mode=str(strategy_params["stop_mode"]),
            tp_r_multiple=float(strategy_params["tp_r_multiple"]),
            entry_mode=str(strategy_params["entry_mode"]),
        )
        if not trades_df.empty and contracts != 1 and "pnl" in trades_df.columns:
            trades_df = trades_df.copy()
            trades_df["pnl"] = pd.to_numeric(trades_df["pnl"], errors="coerce") * float(contracts)

    metrics = summarize_breakout_trades(trades_df)
    metrics["notes"] = "ORB breakout backtest completed." if not bars.empty else "No bars in selected window."
    if segment_name:
        metrics["segment"] = segment_name
    run.metrics = metrics

    for _, trade in trades_df.iterrows():
        side = str(trade.get("side") or "long")
        qty = float(contracts) if side == "long" else -float(contracts)
        trade_notes = {
            "session_date": str(trade.get("session_date") or ""),
            "timeframe": str(trade.get("timeframe") or strategy_params["timeframe"]),
            "ib_minutes": int(trade.get("ib_minutes") or strategy_params["ib_minutes"]),
            "stop_price": float(trade.get("stop_price") or 0.0),
            "target_price": float(trade.get("target_price") or 0.0),
            "exit_reason": str(trade.get("exit_reason") or ""),
            "r_multiple": float(trade.get("r_multiple") or 0.0),
            "contracts": contracts,
        }
        if segment_name:
            trade_notes["segment"] = segment_name
        if batch_meta:
            trade_notes["batch"] = batch_meta

        db.add(
            BacktestTrade(
                run_id=run.id,
                symbol_contract=symbol_contract,
                entry_ts=pd.Timestamp(trade["entry_time"]).to_pydatetime() if pd.notna(trade.get("entry_time")) else None,
                exit_ts=pd.Timestamp(trade["exit_time"]).to_pydatetime() if pd.notna(trade.get("exit_time")) else None,
                entry_price=float(trade.get("entry_price")) if pd.notna(trade.get("entry_price")) else None,
                exit_price=float(trade.get("exit_price")) if pd.notna(trade.get("exit_price")) else None,
                qty=qty,
                pnl=float(trade.get("pnl")) if pd.notna(trade.get("pnl")) else None,
                notes=json.dumps(trade_notes),
            )
        )

    db.commit()
    db.refresh(run)
    return {
        "run_id": str(run.id),
        "trade_count": int(len(trades_df)),
    }


def _run_orb_breakout_backtest(
    db: Session,
    job: Job,
    name: str,
    params: dict,
    *,
    batch_meta: dict | None = None,
) -> dict:
    payload = params or {}

    symbol_contract = str(payload.get("symbol_contract") or "").strip()
    if not symbol_contract:
        raise ValueError("symbol_contract is required for orb_breakout_v1")

    start_raw = payload.get("start")
    end_raw = payload.get("end")
    if not start_raw or not end_raw:
        raise ValueError("start and end are required for orb_breakout_v1")

    start_dt = _parse_iso_datetime(str(start_raw), "start")
    end_dt = _parse_iso_datetime(str(end_raw), "end")
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt

    strategy_params = merge_params(payload)
    session_start, session_end, rth_only = _resolve_session_window(payload, strategy_params)

    split_cfg = _parse_split_config(payload, start_dt=start_dt, end_dt=end_dt)
    if split_cfg:
        split_at_dt = split_cfg["split_at_dt"]
        split_at_iso = split_cfg["split_at"]
        split_group_id = str(uuid.uuid4())
        segments = [
            ("is", start_dt, split_at_dt),
            ("oos", split_at_dt, end_dt),
        ]
    else:
        split_at_iso = None
        split_group_id = None
        segments = [(None, start_dt, end_dt)]

    created_run_ids: list[str] = []
    total_trades = 0

    for segment_label, seg_start, seg_end in segments:
        split_meta = None
        if split_cfg:
            split_meta = {
                "enabled": True,
                "split_at": split_at_iso,
                "segment": segment_label,
                "group_id": split_group_id,
            }

        segment_result = _build_orb_segment(
            db=db,
            job=job,
            name=name,
            payload=payload,
            segment_name=segment_label,
            segment_start=seg_start,
            segment_end=seg_end,
            session_start=session_start,
            session_end=session_end,
            rth_only=rth_only,
            split_meta=split_meta,
            batch_meta=batch_meta,
        )
        created_run_ids.append(segment_result["run_id"])
        total_trades += int(segment_result["trade_count"])

    result = {
        "run_id": created_run_ids[0],
        "run_ids": created_run_ids,
        "mode": "run",
        "strategy_id": "orb_breakout_v1",
        "trade_count": int(total_trades),
        "created_runs": len(created_run_ids),
        "failed_runs": 0,
    }
    if split_group_id:
        result["split_group_id"] = split_group_id
    return result


def run_backtest_scaffold(db: Session, job: Job, name: str, strategy_id: str, params: dict) -> dict:
    strategy_key = (strategy_id or "scaffold").strip().lower()
    if strategy_key == "scaffold":
        return _run_scaffold_backtest(db=db, job=job, name=name, strategy_id="scaffold", params=params or {})
    if strategy_key == "orb_breakout_v1":
        return _run_orb_breakout_backtest(db=db, job=job, name=name, params=params or {})
    raise ValueError(f"Unsupported strategy_id: {strategy_id}")


def _run_scaffold_sweep(db: Session, job: Job, name: str, strategy_id: str, params: dict) -> dict:
    seeds = params.get("seeds", [1, 2, 3])
    created = []
    for seed in seeds:
        pnl = round(random.uniform(-500, 500), 2)
        run = BacktestRun(
            job_id=job.id,
            owner_id=job.owner_id,
            name=f"{name}-seed-{seed}",
            strategy_id=strategy_id,
            params={**params, "seed": seed},
            metrics={
                "net_pnl": pnl,
                "win_rate": 0.0,
                "max_drawdown": 0.0,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "notes": "Scaffold sweep run. Placeholder metrics.",
            },
            status="completed",
        )
        db.add(run)
        db.flush()
        created.append(str(run.id))

    db.commit()
    return {
        "run_ids": created,
        "mode": "sweep",
        "strategy_id": strategy_id,
        "created_runs": len(created),
        "failed_runs": 0,
        "trade_count": 0,
    }


def _run_orb_sweep(db: Session, job: Job, name: str, params: dict) -> dict:
    payload = params or {}
    batch = payload.get("batch") if isinstance(payload.get("batch"), dict) else {}

    symbols = [str(s).strip() for s in batch.get("symbols", []) if str(s).strip()]
    if not symbols:
        symbol = str(payload.get("symbol_contract") or "").strip()
        if symbol:
            symbols = [symbol]
    if not symbols:
        raise ValueError("batch.symbols or params.symbol_contract is required for orb sweep")

    periods = batch.get("periods") if isinstance(batch.get("periods"), list) else []
    if not periods:
        periods = [{
            "label": "range",
            "start": payload.get("start"),
            "end": payload.get("end"),
        }]

    created_run_ids: list[str] = []
    failures: list[dict] = []
    trade_count = 0

    for symbol_contract in symbols:
        for idx, raw_period in enumerate(periods):
            period = raw_period if isinstance(raw_period, dict) else {}
            period_start = period.get("start")
            period_end = period.get("end")
            if not period_start or not period_end:
                failures.append(
                    {
                        "symbol_contract": symbol_contract,
                        "period_index": idx,
                        "error": "period.start and period.end are required",
                    }
                )
                continue

            period_label = str(period.get("label") or f"period-{idx + 1}").strip() or f"period-{idx + 1}"
            run_name = f"{name} | {symbol_contract} | {period_label}"
            run_params = {
                **payload,
                "symbol_contract": symbol_contract,
                "start": period_start,
                "end": period_end,
            }

            batch_meta = {
                "symbol_contract": symbol_contract,
                "period_label": period_label,
                "period_start": str(period_start),
                "period_end": str(period_end),
            }

            try:
                run_result = _run_orb_breakout_backtest(
                    db=db,
                    job=job,
                    name=run_name,
                    params=run_params,
                    batch_meta=batch_meta,
                )
                created_run_ids.extend(run_result.get("run_ids", []))
                trade_count += int(run_result.get("trade_count") or 0)
            except Exception as exc:  # pragma: no cover - defensive against data-specific issues
                failures.append(
                    {
                        "symbol_contract": symbol_contract,
                        "period_label": period_label,
                        "error": str(exc),
                    }
                )

    if not created_run_ids and failures:
        first = failures[0]
        raise ValueError(f"Sweep produced no runs: {first.get('error')}")

    return {
        "run_ids": created_run_ids,
        "mode": "sweep",
        "strategy_id": "orb_breakout_v1",
        "created_runs": len(created_run_ids),
        "failed_runs": len(failures),
        "failures": failures,
        "trade_count": trade_count,
    }


def run_sweep_scaffold(db: Session, job: Job, name: str, strategy_id: str, params: dict) -> dict:
    strategy_key = (strategy_id or "scaffold").strip().lower()
    if strategy_key == "orb_breakout_v1":
        return _run_orb_sweep(db=db, job=job, name=name, params=params or {})
    return _run_scaffold_sweep(db=db, job=job, name=name, strategy_id=strategy_id, params=params or {})
