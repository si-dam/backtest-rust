from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

import numpy as np
import pandas as pd


Side = Literal["long", "short"]
StopMode = Literal["or_boundary", "or_mid"]
EntryMode = Literal["first_outside", "reentry_after_stop"]

ALLOWED_TIMEFRAMES = ("1m", "3m", "5m", "15m", "30m", "60m")

DEFAULT_PARAMS: dict[str, str | int | float | bool] = {
    "timeframe": "1m",
    "ib_minutes": 15,
    "rth_only": True,
    "session_start": "09:30:00",
    "session_end": "16:00:00",
    "stop_mode": "or_boundary",
    "tp_r_multiple": 2.0,
    "entry_mode": "first_outside",
    "strategy_mode": "breakout_only",
    "big_trade_threshold": 25,
    "contracts": 1,
}


@dataclass(frozen=True)
class ORLevels:
    or_high: float
    or_low: float
    or_mid: float
    ib_end: pd.Timestamp


@dataclass(frozen=True)
class Trade:
    session_date: str
    timeframe: str
    ib_minutes: int
    side: Side
    entry_time: pd.Timestamp
    entry_price: float
    stop_price: float
    target_price: float
    exit_time: pd.Timestamp
    exit_price: float
    exit_reason: str
    pnl: float
    r_multiple: float


def strategy_metadata() -> dict:
    return {
        "id": "orb_breakout_v1",
        "label": "ORB Breakout V1",
        "description": "Opening-range breakout strategy using first candle close outside OR.",
        "defaults": {
            "name": "ORB Breakout V1",
            **DEFAULT_PARAMS,
        },
        "params": [
            {"name": "symbol_contract", "type": "string", "required": True},
            {"name": "start", "type": "datetime", "required": True},
            {"name": "end", "type": "datetime", "required": True},
            {
                "name": "timeframe",
                "type": "enum",
                "required": True,
                "default": DEFAULT_PARAMS["timeframe"],
                "options": list(ALLOWED_TIMEFRAMES),
            },
            {
                "name": "ib_minutes",
                "type": "integer",
                "required": True,
                "default": DEFAULT_PARAMS["ib_minutes"],
            },
            {
                "name": "rth_only",
                "type": "boolean",
                "required": True,
                "default": DEFAULT_PARAMS["rth_only"],
            },
            {
                "name": "stop_mode",
                "type": "enum",
                "required": True,
                "default": DEFAULT_PARAMS["stop_mode"],
                "options": ["or_boundary", "or_mid"],
            },
            {
                "name": "tp_r_multiple",
                "type": "number",
                "required": True,
                "default": DEFAULT_PARAMS["tp_r_multiple"],
            },
            {
                "name": "entry_mode",
                "type": "enum",
                "required": True,
                "default": DEFAULT_PARAMS["entry_mode"],
                "options": ["first_outside", "reentry_after_stop"],
            },
            {
                "name": "strategy_mode",
                "type": "enum",
                "required": True,
                "default": DEFAULT_PARAMS["strategy_mode"],
                "options": ["breakout_only", "big_order_required"],
            },
            {
                "name": "big_trade_threshold",
                "type": "integer",
                "required": True,
                "default": DEFAULT_PARAMS["big_trade_threshold"],
            },
            {
                "name": "contracts",
                "type": "integer",
                "required": False,
                "default": DEFAULT_PARAMS["contracts"],
            },
        ],
    }


def merge_params(raw_params: dict) -> dict:
    merged = {**DEFAULT_PARAMS, **(raw_params or {})}
    merged["timeframe"] = str(merged.get("timeframe") or DEFAULT_PARAMS["timeframe"]).lower()
    if merged["timeframe"] not in ALLOWED_TIMEFRAMES:
        raise ValueError(f"Unsupported strategy timeframe: {merged['timeframe']}")

    merged["ib_minutes"] = int(merged.get("ib_minutes") or DEFAULT_PARAMS["ib_minutes"])
    if merged["ib_minutes"] <= 0:
        raise ValueError("ib_minutes must be greater than 0")

    merged["rth_only"] = _coerce_bool(merged.get("rth_only"), default=bool(DEFAULT_PARAMS["rth_only"]))
    merged["session_start"] = str(merged.get("session_start") or DEFAULT_PARAMS["session_start"])
    merged["session_end"] = str(merged.get("session_end") or DEFAULT_PARAMS["session_end"])
    _parse_hhmmss(merged["session_start"])
    _parse_hhmmss(merged["session_end"])

    merged["stop_mode"] = str(merged.get("stop_mode") or DEFAULT_PARAMS["stop_mode"]).lower()
    if merged["stop_mode"] not in {"or_boundary", "or_mid"}:
        raise ValueError("stop_mode must be 'or_boundary' or 'or_mid'")

    merged["tp_r_multiple"] = float(merged.get("tp_r_multiple") or DEFAULT_PARAMS["tp_r_multiple"])
    if merged["tp_r_multiple"] <= 0:
        raise ValueError("tp_r_multiple must be greater than 0")

    merged["entry_mode"] = str(merged.get("entry_mode") or DEFAULT_PARAMS["entry_mode"]).lower()
    if merged["entry_mode"] not in {"first_outside", "reentry_after_stop"}:
        raise ValueError("entry_mode must be 'first_outside' or 'reentry_after_stop'")

    merged["strategy_mode"] = str(merged.get("strategy_mode") or DEFAULT_PARAMS["strategy_mode"]).lower()
    if merged["strategy_mode"] not in {"breakout_only", "big_order_required"}:
        raise ValueError("strategy_mode must be 'breakout_only' or 'big_order_required'")

    merged["big_trade_threshold"] = int(merged.get("big_trade_threshold") or DEFAULT_PARAMS["big_trade_threshold"])
    if merged["big_trade_threshold"] <= 0:
        raise ValueError("big_trade_threshold must be greater than 0")

    contracts_raw = merged.get("contracts")
    if contracts_raw in (None, ""):
        merged["contracts"] = int(DEFAULT_PARAMS["contracts"])
    else:
        merged["contracts"] = int(contracts_raw)
    if merged["contracts"] <= 0:
        raise ValueError("contracts must be greater than 0")

    return merged


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


def _parse_hhmmss(value: str):
    return datetime.strptime(value, "%H:%M:%S").time()


def _session_dt(day: pd.Timestamp, hhmmss: str) -> pd.Timestamp:
    out = pd.Timestamp.combine(day.date(), _parse_hhmmss(hhmmss))
    if getattr(day, "tzinfo", None) is not None:
        out = out.tz_localize(day.tzinfo)
    return out


def compute_opening_range_for_day(
    day_bars: pd.DataFrame,
    ib_minutes: int,
    session_start: str,
) -> ORLevels | None:
    if day_bars.empty:
        return None

    day_anchor = day_bars.index[0]
    session_start_dt = _session_dt(pd.Timestamp(day_anchor), session_start)
    ib_end = session_start_dt + timedelta(minutes=ib_minutes)

    ib_slice = day_bars[(day_bars.index > session_start_dt) & (day_bars.index <= ib_end)]
    if ib_slice.empty:
        return None

    or_high = float(ib_slice["high"].max())
    or_low = float(ib_slice["low"].min())
    return ORLevels(or_high=or_high, or_low=or_low, or_mid=(or_high + or_low) / 2.0, ib_end=ib_end)


def find_first_breakout_signal(
    day_bars: pd.DataFrame,
    levels: ORLevels,
    session_end: str,
    start_after: pd.Timestamp | None = None,
) -> tuple[pd.Timestamp, Side] | None:
    session_end_t = _parse_hhmmss(session_end)
    start_ts = levels.ib_end if start_after is None else max(levels.ib_end, pd.Timestamp(start_after))
    candidates = day_bars[(day_bars.index > start_ts) & (day_bars.index.time <= session_end_t)]

    for ts, row in candidates.iterrows():
        if float(row["close"]) > levels.or_high:
            return pd.Timestamp(ts), "long"
        if float(row["close"]) < levels.or_low:
            return pd.Timestamp(ts), "short"
    return None


def _stop_price(side: Side, mode: StopMode, or_high: float, or_low: float, or_mid: float) -> float:
    if mode == "or_mid":
        return or_mid
    if side == "long":
        return or_low
    return or_high


def _simulate_exit(
    future_bars: pd.DataFrame,
    side: Side,
    stop: float,
    target: float,
    session_end: str,
) -> tuple[pd.Timestamp, float, str]:
    session_end_t = _parse_hhmmss(session_end)
    scoped = future_bars[future_bars.index.time <= session_end_t]

    if scoped.empty:
        raise ValueError("No bars available after entry to simulate exit")

    for ts, row in scoped.iterrows():
        high = float(row["high"])
        low = float(row["low"])

        if side == "long":
            hit_stop = low <= stop
            hit_target = high >= target
            if hit_stop and hit_target:
                return pd.Timestamp(ts), stop, "stop"
            if hit_stop:
                return pd.Timestamp(ts), stop, "stop"
            if hit_target:
                return pd.Timestamp(ts), target, "target"
        else:
            hit_stop = high >= stop
            hit_target = low <= target
            if hit_stop and hit_target:
                return pd.Timestamp(ts), stop, "stop"
            if hit_stop:
                return pd.Timestamp(ts), stop, "stop"
            if hit_target:
                return pd.Timestamp(ts), target, "target"

    last_ts = pd.Timestamp(scoped.index[-1])
    last_price = float(scoped.iloc[-1]["close"])
    return last_ts, last_price, "session_close"


def simulate_orb_breakout_strategy(
    bars: pd.DataFrame,
    timeframe: str,
    ib_minutes: int,
    session_start: str,
    session_end: str,
    stop_mode: StopMode,
    tp_r_multiple: float,
    entry_mode: EntryMode = "first_outside",
) -> pd.DataFrame:
    if bars.empty:
        return pd.DataFrame()

    out = bars.copy()
    if "ts" in out.columns:
        out["ts"] = pd.to_datetime(out["ts"], errors="coerce")
        out = out.dropna(subset=["ts"]).set_index("ts")
    if not isinstance(out.index, pd.DatetimeIndex):
        raise ValueError("Bars must have a DatetimeIndex or a 'ts' column")

    out = out.sort_index()
    start_t = _parse_hhmmss(session_start)
    end_t = _parse_hhmmss(session_end)

    trades: list[Trade] = []

    for day, day_bars in out.groupby(out.index.date, sort=True):
        in_session = day_bars[(day_bars.index.time >= start_t) & (day_bars.index.time <= end_t)]
        if in_session.empty:
            continue

        levels = compute_opening_range_for_day(
            in_session,
            ib_minutes=ib_minutes,
            session_start=session_start,
        )
        if levels is None:
            continue

        search_after: pd.Timestamp | None = None
        while True:
            signal = find_first_breakout_signal(
                in_session,
                levels=levels,
                session_end=session_end,
                start_after=search_after,
            )
            if signal is None:
                break

            entry_ts, side = signal
            entry_price = float(in_session.loc[entry_ts, "close"])
            stop = _stop_price(
                side=side,
                mode=stop_mode,
                or_high=levels.or_high,
                or_low=levels.or_low,
                or_mid=levels.or_mid,
            )

            risk = abs(entry_price - stop)
            if risk <= 0:
                break

            target = entry_price + (tp_r_multiple * risk) if side == "long" else entry_price - (tp_r_multiple * risk)
            future = in_session[in_session.index > entry_ts]
            if future.empty:
                break

            exit_ts, exit_price, exit_reason = _simulate_exit(
                future_bars=future,
                side=side,
                stop=stop,
                target=target,
                session_end=session_end,
            )

            signed = 1.0 if side == "long" else -1.0
            pnl = (exit_price - entry_price) * signed
            r_multiple = pnl / risk

            trades.append(
                Trade(
                    session_date=str(day),
                    timeframe=timeframe,
                    ib_minutes=ib_minutes,
                    side=side,
                    entry_time=entry_ts,
                    entry_price=entry_price,
                    stop_price=stop,
                    target_price=target,
                    exit_time=exit_ts,
                    exit_price=exit_price,
                    exit_reason=exit_reason,
                    pnl=float(pnl),
                    r_multiple=float(r_multiple),
                )
            )

            if entry_mode != "reentry_after_stop":
                break
            if exit_reason != "stop":
                break
            search_after = exit_ts

    return pd.DataFrame([t.__dict__ for t in trades])


def summarize_breakout_trades(trades: pd.DataFrame) -> dict[str, int | float]:
    if trades.empty:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "full_tp_wins": 0,
            "full_losses": 0,
            "win_rate": 0.0,
            "net_pnl": 0.0,
            "total_pnl": 0.0,
            "avg_pnl": 0.0,
            "total_r": 0.0,
            "avg_r": 0.0,
            "max_drawdown": 0.0,
        }

    wins = int((trades["pnl"] > 0).sum())
    losses = int((trades["pnl"] < 0).sum())
    full_tp_wins = int(((trades["exit_reason"] == "target") & (trades["pnl"] > 0)).sum())
    full_losses = int(((trades["exit_reason"] == "stop") & (trades["pnl"] < 0)).sum())
    total = int(len(trades))
    total_pnl = float(trades["pnl"].sum())

    equity = trades["pnl"].cumsum().astype(float)
    running_max = np.maximum.accumulate(equity.to_numpy())
    drawdowns = running_max - equity.to_numpy()
    max_drawdown = float(drawdowns.max()) if len(drawdowns) else 0.0

    return {
        "trades": total,
        "wins": wins,
        "losses": losses,
        "full_tp_wins": full_tp_wins,
        "full_losses": full_losses,
        "win_rate": float(wins / total) if total else 0.0,
        "net_pnl": total_pnl,
        "total_pnl": total_pnl,
        "avg_pnl": float(trades["pnl"].mean()),
        "total_r": float(trades["r_multiple"].sum()),
        "avg_r": float(trades["r_multiple"].mean()),
        "max_drawdown": max_drawdown,
    }
