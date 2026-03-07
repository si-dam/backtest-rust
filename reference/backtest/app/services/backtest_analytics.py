from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class TradeLike:
    id: object
    entry_ts: datetime | None
    exit_ts: datetime | None
    pnl: float | None
    notes: str | None


def parse_trade_notes(raw_notes: str | None) -> dict:
    if not isinstance(raw_notes, str) or not raw_notes.strip():
        return {}
    try:
        decoded = json.loads(raw_notes)
    except json.JSONDecodeError:
        return {}
    if not isinstance(decoded, dict):
        return {}
    return decoded


def _to_zone_iso(value: datetime | None, tz: ZoneInfo) -> str | None:
    if value is None:
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    ts = ts.tz_convert(tz)
    return ts.isoformat()


def _to_zone_series(values: pd.Series, tz: ZoneInfo) -> pd.Series:
    if values.empty:
        return values
    parsed = pd.to_datetime(values, errors="coerce", utc=True)
    return parsed.dt.tz_convert(tz)


def _empty_payload() -> dict:
    return {
        "summary": {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "net_pnl": 0.0,
            "total_pnl": 0.0,
            "avg_pnl": 0.0,
            "expectancy": 0.0,
            "profit_factor": None,
            "avg_win": None,
            "avg_loss": None,
            "largest_loser": None,
            "max_consecutive_losses": 0,
            "max_drawdown": 0.0,
        },
        "equity_curve": [],
        "drawdown_curve": [],
        "pnl_by_time_of_day": [],
        "pnl_by_day": [],
        "outliers": {
            "best_10_days": [],
            "worst_10_days": [],
        },
    }


def _max_consecutive_losses(pnls: list[float]) -> int:
    longest = 0
    current = 0
    for pnl in pnls:
        if pnl < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _bucket_hhmm(ts: pd.Timestamp) -> str:
    minute = 30 * (int(ts.minute) // 30)
    return f"{int(ts.hour):02d}:{minute:02d}"


def build_backtest_analytics(trades: list[TradeLike], timezone: str) -> dict:
    payload = _empty_payload()
    if not trades:
        return payload

    tz = ZoneInfo(timezone)
    rows: list[dict] = []
    for trade in trades:
        pnl_value = float(trade.pnl) if trade.pnl is not None else None
        if pnl_value is None or not np.isfinite(pnl_value):
            continue
        notes = parse_trade_notes(trade.notes)
        rows.append(
            {
                "id": str(trade.id),
                "entry_ts": trade.entry_ts,
                "exit_ts": trade.exit_ts,
                "pnl": float(pnl_value),
                "notes": notes,
            }
        )

    if not rows:
        return payload

    df = pd.DataFrame(rows)
    df["entry_ts"] = _to_zone_series(df["entry_ts"], tz)
    df["exit_ts"] = _to_zone_series(df["exit_ts"], tz)
    df["ts"] = df["exit_ts"].where(df["exit_ts"].notna(), df["entry_ts"])
    df = df.sort_values(["entry_ts", "exit_ts", "id"], na_position="last").reset_index(drop=True)

    total = int(len(df))
    wins = int((df["pnl"] > 0).sum())
    losses = int((df["pnl"] < 0).sum())
    gross_profit = float(df.loc[df["pnl"] > 0, "pnl"].sum())
    gross_loss = float(df.loc[df["pnl"] < 0, "pnl"].sum())
    total_pnl = float(df["pnl"].sum())
    avg_pnl = float(df["pnl"].mean()) if total else 0.0
    expectancy = avg_pnl
    profit_factor = float(gross_profit / abs(gross_loss)) if gross_loss < 0 else None
    avg_win = float(df.loc[df["pnl"] > 0, "pnl"].mean()) if wins else None
    avg_loss = float(df.loc[df["pnl"] < 0, "pnl"].mean()) if losses else None
    largest_loser = float(df["pnl"].min()) if total else None
    max_consecutive_losses = _max_consecutive_losses(df["pnl"].astype(float).tolist())

    equity = df["pnl"].cumsum().astype(float)
    running_max = np.maximum.accumulate(equity.to_numpy())
    drawdown = running_max - equity.to_numpy()
    max_drawdown = float(drawdown.max()) if len(drawdown) else 0.0

    payload["summary"] = {
        "trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": float(wins / total) if total else 0.0,
        "net_pnl": total_pnl,
        "total_pnl": total_pnl,
        "avg_pnl": avg_pnl,
        "expectancy": expectancy,
        "profit_factor": profit_factor,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "largest_loser": largest_loser,
        "max_consecutive_losses": max_consecutive_losses,
        "max_drawdown": max_drawdown,
    }

    equity_curve = []
    drawdown_curve = []
    for idx, row in df.iterrows():
        curve_ts = row["ts"]
        iso_ts = curve_ts.isoformat() if pd.notna(curve_ts) else None
        equity_curve.append(
            {
                "idx": int(idx),
                "ts": iso_ts,
                "equity_pnl": float(equity.iloc[idx]),
            }
        )
        drawdown_curve.append(
            {
                "idx": int(idx),
                "ts": iso_ts,
                "drawdown_pnl": float(drawdown[idx]),
            }
        )

    payload["equity_curve"] = equity_curve
    payload["drawdown_curve"] = drawdown_curve

    time_source = df["entry_ts"].where(df["entry_ts"].notna(), df["exit_ts"])
    time_df = pd.DataFrame({"bucket_hhmm": time_source.dropna().map(_bucket_hhmm), "pnl": df.loc[time_source.notna(), "pnl"]})
    if not time_df.empty:
        pnl_by_time = (
            time_df.groupby("bucket_hhmm", sort=True)
            .agg(trades=("pnl", "count"), pnl=("pnl", "sum"))
            .reset_index()
        )
        payload["pnl_by_time_of_day"] = [
            {
                "bucket_hhmm": str(row["bucket_hhmm"]),
                "trades": int(row["trades"]),
                "pnl": float(row["pnl"]),
            }
            for _, row in pnl_by_time.iterrows()
        ]

    day_source = df["exit_ts"].where(df["exit_ts"].notna(), df["entry_ts"])
    with_day = df.copy()
    with_day["day"] = day_source.dt.date
    with_day = with_day.dropna(subset=["day"])
    if with_day.empty:
        return payload

    by_day = (
        with_day.groupby("day", sort=True)
        .agg(
            trades=("pnl", "count"),
            pnl=("pnl", "sum"),
            first_entry_ts=("entry_ts", "min"),
            last_exit_ts=("exit_ts", "max"),
        )
        .reset_index()
    )

    payload["pnl_by_day"] = [
        {
            "date": row["day"].isoformat(),
            "trades": int(row["trades"]),
            "pnl": float(row["pnl"]),
        }
        for _, row in by_day.sort_values("day").iterrows()
    ]

    def _day_rows(source: pd.DataFrame) -> list[dict]:
        return [
            {
                "date": row["day"].isoformat(),
                "trades": int(row["trades"]),
                "pnl": float(row["pnl"]),
                "first_entry_ts": _to_zone_iso(row["first_entry_ts"], tz),
                "last_exit_ts": _to_zone_iso(row["last_exit_ts"], tz),
            }
            for _, row in source.iterrows()
        ]

    payload["outliers"] = {
        "best_10_days": _day_rows(by_day.sort_values(["pnl", "day"], ascending=[False, True]).head(10)),
        "worst_10_days": _day_rows(by_day.sort_values(["pnl", "day"], ascending=[True, True]).head(10)),
    }

    return payload
