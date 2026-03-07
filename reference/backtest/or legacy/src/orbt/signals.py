from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd


@dataclass(frozen=True)
class ORLevels:
    or_high: float
    or_low: float
    or_mid: float
    ib_end: pd.Timestamp


def _session_dt(day: pd.Timestamp, hhmmss: str) -> pd.Timestamp:
    t = datetime.strptime(hhmmss, "%H:%M:%S").time()
    return pd.Timestamp.combine(day.date(), t)


def compute_opening_range_for_day(
    day_bars: pd.DataFrame,
    ib_minutes: int,
    session_start: str,
) -> ORLevels | None:
    if day_bars.empty:
        return None

    day_anchor = day_bars.index[0]
    session_start_dt = _session_dt(day_anchor, session_start)
    ib_end = session_start_dt + timedelta(minutes=ib_minutes)

    ib_slice = day_bars[(day_bars.index > session_start_dt) & (day_bars.index <= ib_end)]
    if ib_slice.empty:
        return None

    or_high = float(ib_slice["high"].max())
    or_low = float(ib_slice["low"].min())
    return ORLevels(or_high=or_high, or_low=or_low, or_mid=(or_high + or_low) / 2.0, ib_end=ib_end)


def annotate_bars_with_or(
    bars: pd.DataFrame,
    ib_minutes: int,
    session_start: str,
    session_end: str,
) -> pd.DataFrame:
    out = bars.copy()
    out["or_high"] = pd.NA
    out["or_low"] = pd.NA
    out["or_mid"] = pd.NA
    out["ib_end"] = pd.NaT

    for _, day_bars in out.groupby(out.index.date, sort=True):
        day_frame = day_bars[
            (day_bars.index.time >= datetime.strptime(session_start, "%H:%M:%S").time())
            & (day_bars.index.time <= datetime.strptime(session_end, "%H:%M:%S").time())
        ]
        if day_frame.empty:
            continue
        levels = compute_opening_range_for_day(day_frame, ib_minutes=ib_minutes, session_start=session_start)
        if levels is None:
            continue
        idx = day_frame.index
        out.loc[idx, "or_high"] = levels.or_high
        out.loc[idx, "or_low"] = levels.or_low
        out.loc[idx, "or_mid"] = levels.or_mid
        out.loc[idx, "ib_end"] = levels.ib_end

    out["or_high"] = pd.to_numeric(out["or_high"], errors="coerce")
    out["or_low"] = pd.to_numeric(out["or_low"], errors="coerce")
    out["or_mid"] = pd.to_numeric(out["or_mid"], errors="coerce")
    return out


def find_first_breakout_signal(
    day_bars: pd.DataFrame,
    levels: ORLevels,
    session_end: str,
    start_after: pd.Timestamp | None = None,
    require_big_trade: bool = True,
) -> tuple[pd.Timestamp, str] | None:
    session_end_t = datetime.strptime(session_end, "%H:%M:%S").time()
    start_ts = levels.ib_end if start_after is None else max(levels.ib_end, pd.Timestamp(start_after))
    candidates = day_bars[(day_bars.index > start_ts) & (day_bars.index.time <= session_end_t)]

    # Only the first candle that closes outside the opening range can trigger.
    for ts, row in candidates.iterrows():
        if row["close"] > levels.or_high:
            if (not require_big_trade) or bool(row.get("has_big_buy", False)):
                return ts, "long"
            return None
        if row["close"] < levels.or_low:
            if (not require_big_trade) or bool(row.get("has_big_sell", False)):
                return ts, "short"
            return None
    return None
