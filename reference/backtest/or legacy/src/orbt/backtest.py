from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import numpy as np
import pandas as pd

from orbt.signals import annotate_bars_with_or, compute_opening_range_for_day, find_first_breakout_signal

Side = Literal["long", "short"]
StopMode = Literal["or_boundary", "or_mid"]
EntryMode = Literal["first_outside", "reentry_after_stop"]
StrategyMode = Literal["big_order_required", "breakout_only"]


@dataclass(frozen=True)
class Trade:
    session_date: str
    timeframe_min: int
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


def _session_time(hhmmss: str):
    return datetime.strptime(hhmmss, "%H:%M:%S").time()


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
    session_end_t = _session_time(session_end)
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
                return ts, stop, "stop"
            if hit_stop:
                return ts, stop, "stop"
            if hit_target:
                return ts, target, "target"
        else:
            hit_stop = high >= stop
            hit_target = low <= target
            if hit_stop and hit_target:
                return ts, stop, "stop"
            if hit_stop:
                return ts, stop, "stop"
            if hit_target:
                return ts, target, "target"

    last_ts = scoped.index[-1]
    last_price = float(scoped.iloc[-1]["close"])
    return last_ts, last_price, "session_close"


def simulate_orb_big_trade_strategy(
    bars: pd.DataFrame,
    timeframe_min: int,
    ib_minutes: int,
    session_start: str,
    session_end: str,
    stop_mode: StopMode,
    tp_r_multiple: float,
    entry_mode: EntryMode = "first_outside",
    strategy_mode: StrategyMode = "big_order_required",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if bars.empty:
        return pd.DataFrame(), bars.copy()

    annotated = annotate_bars_with_or(
        bars=bars,
        ib_minutes=ib_minutes,
        session_start=session_start,
        session_end=session_end,
    )

    annotated["signal_long"] = False
    annotated["signal_short"] = False
    annotated["entry_marker"] = np.nan
    annotated["exit_marker"] = np.nan

    trades: list[Trade] = []
    start_t = _session_time(session_start)
    end_t = _session_time(session_end)

    for day, day_bars in annotated.groupby(annotated.index.date, sort=True):
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
                require_big_trade=strategy_mode == "big_order_required",
            )
            if signal is None:
                break

            entry_ts, side = signal
            annotated.loc[entry_ts, "signal_long"] = side == "long"
            annotated.loc[entry_ts, "signal_short"] = side == "short"

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

            if side == "long":
                target = entry_price + (tp_r_multiple * risk)
            else:
                target = entry_price - (tp_r_multiple * risk)

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

            annotated.loc[entry_ts, "entry_marker"] = entry_price
            annotated.loc[exit_ts, "exit_marker"] = exit_price

            trades.append(
                Trade(
                    session_date=str(day),
                    timeframe_min=timeframe_min,
                    ib_minutes=ib_minutes,
                    side=side,
                    entry_time=entry_ts,
                    entry_price=entry_price,
                    stop_price=stop,
                    target_price=target,
                    exit_time=exit_ts,
                    exit_price=exit_price,
                    exit_reason=exit_reason,
                    pnl=pnl,
                    r_multiple=r_multiple,
                )
            )

            if entry_mode != "reentry_after_stop":
                break
            if exit_reason != "stop":
                break
            search_after = exit_ts
    trades_df = pd.DataFrame([t.__dict__ for t in trades])
    return trades_df, annotated


def build_vectorbt_portfolio(bars: pd.DataFrame, trades: pd.DataFrame):
    try:
        import vectorbt as vbt
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("vectorbt is required. Install dependencies with `uv sync`.") from exc

    close = bars["close"].astype(float)
    size = pd.Series(0.0, index=close.index)
    price = pd.Series(np.nan, index=close.index)

    for _, trade in trades.iterrows():
        entry_ts = pd.Timestamp(trade["entry_time"])
        exit_ts = pd.Timestamp(trade["exit_time"])
        side = trade["side"]
        entry_price = float(trade["entry_price"])
        exit_price = float(trade["exit_price"])

        if side == "long":
            size.loc[entry_ts] += 1.0
            size.loc[exit_ts] -= 1.0
        else:
            size.loc[entry_ts] -= 1.0
            size.loc[exit_ts] += 1.0

        price.loc[entry_ts] = entry_price
        price.loc[exit_ts] = exit_price

    return vbt.Portfolio.from_orders(close=close, size=size, price=price, freq=None)
