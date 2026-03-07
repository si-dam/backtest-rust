from __future__ import annotations

from datetime import date, datetime, timedelta, time
from pathlib import Path

import pandas as pd


def _parse_time(value: str) -> time:
    return datetime.strptime(value, "%H:%M:%S").time()


def _pandas_offset_from_time(value: str) -> str:
    t = _parse_time(value)
    return f"{t.hour}h{t.minute}min"


def _find_col(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    by_lower = {c.lower(): c for c in df.columns}
    for key in candidates:
        col = by_lower.get(key.lower())
        if col is not None:
            return col
    return None


def filter_session(df: pd.DataFrame, session_start: str, session_end: str) -> pd.DataFrame:
    start_t = _parse_time(session_start)
    end_t = _parse_time(session_end)
    ts_time = df["timestamp"].dt.time
    mask = (ts_time >= start_t) & (ts_time <= end_t)
    cols = ["timestamp", "last", "volume", "bid_volume", "ask_volume"]
    if "seq" in df.columns:
        cols.append("seq")
    return df.loc[mask, cols].copy()


def _normalize_ohlc_bars(raw: pd.DataFrame) -> pd.DataFrame:
    if "timestamp" in raw.columns:
        ts = pd.to_datetime(raw["timestamp"], errors="raise")
    elif "Date" in raw.columns and "Time" in raw.columns:
        dt_str = raw["Date"].astype(str).str.strip() + " " + raw["Time"].astype(str).str.strip()
        ts = pd.to_datetime(dt_str, format="mixed", errors="raise")
    elif isinstance(raw.index, pd.DatetimeIndex):
        ts = pd.to_datetime(raw.index, errors="raise")
    else:
        raise ValueError("OHLC source must include 'timestamp' or 'Date'/'Time'")

    open_col = _find_col(raw, ("open",))
    high_col = _find_col(raw, ("high",))
    low_col = _find_col(raw, ("low",))
    close_col = _find_col(raw, ("close", "last"))
    volume_col = _find_col(raw, ("volume",))

    if open_col is None or high_col is None or low_col is None or close_col is None:
        raise ValueError("OHLC source must include Open/High/Low and Close/Last columns")

    out = pd.DataFrame(
        {
            "timestamp": ts,
            "open": pd.to_numeric(raw[open_col], errors="raise"),
            "high": pd.to_numeric(raw[high_col], errors="raise"),
            "low": pd.to_numeric(raw[low_col], errors="raise"),
            "close": pd.to_numeric(raw[close_col], errors="raise"),
        }
    )
    if volume_col is not None:
        out["volume"] = pd.to_numeric(raw[volume_col], errors="coerce").fillna(0)

    out = out.dropna(subset=["timestamp", "open", "high", "low", "close"])
    out = out.sort_values("timestamp", kind="stable")
    out = out.drop_duplicates(subset=["timestamp"], keep="last")
    out = out.reset_index(drop=True)
    return out


def _maybe_shift_ohlc_to_right_edge(bars: pd.DataFrame, session_start: str) -> pd.DataFrame:
    if bars.empty:
        return bars

    start_t = _parse_time(session_start)
    start_plus_1 = (datetime.combine(date.today(), start_t) + timedelta(minutes=1)).time()

    by_day_min = bars.groupby(bars["timestamp"].dt.date)["timestamp"].min().dt.time
    frac_at_start = float((by_day_min == start_t).mean())
    frac_at_start_plus_1 = float((by_day_min == start_plus_1).mean())

    # Sierra minute bars are often start-labeled (09:30..15:59). Shift to right-edge labels.
    if frac_at_start > 0.60 and frac_at_start_plus_1 < 0.40:
        shifted = bars.copy()
        shifted["timestamp"] = shifted["timestamp"] + pd.Timedelta(minutes=1)
        return shifted
    return bars


def load_sierra_ohlc_1m(path: str | Path, session_start: str, session_end: str) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() in {".txt", ".csv"}:
        raw = pd.read_csv(path, skipinitialspace=True)
    elif path.suffix.lower() == ".parquet":
        raw = pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported OHLC file type: {path.suffix}")

    bars = _normalize_ohlc_bars(raw)
    bars = _maybe_shift_ohlc_to_right_edge(bars, session_start=session_start)

    start_t = _parse_time(session_start)
    end_t = _parse_time(session_end)
    t = bars["timestamp"].dt.time
    bars = bars[(t > start_t) & (t <= end_t)].copy()

    bars = bars.sort_values("timestamp", kind="stable")
    bars = bars.drop_duplicates(subset=["timestamp"], keep="last")
    bars = bars.set_index("timestamp")
    bars["session_date"] = bars.index.date.astype("str")
    return bars


def _resample_price_bars_from_ohlc(
    ohlc_1m: pd.DataFrame,
    timeframe_min: int,
    session_start: str,
    session_end: str,
) -> pd.DataFrame:
    if ohlc_1m.empty:
        return pd.DataFrame()

    src = ohlc_1m.copy()
    if not isinstance(src.index, pd.DatetimeIndex):
        raise ValueError("OHLC bars must be indexed by timestamp")

    start_t = _parse_time(session_start)
    end_t = _parse_time(session_end)
    src = src[(src.index.time > start_t) & (src.index.time <= end_t)].copy()
    if src.empty:
        return pd.DataFrame()

    if timeframe_min == 1:
        out = src.copy()
        out["session_date"] = out.index.date.astype("str")
        return out

    agg: dict[str, tuple[str, str]] = {
        "open": ("open", "first"),
        "high": ("high", "max"),
        "low": ("low", "min"),
        "close": ("close", "last"),
    }
    if "volume" in src.columns:
        agg["volume"] = ("volume", "sum")

    bars = src.resample(
        rule=f"{timeframe_min}min",
        label="right",
        closed="left",
        origin="start_day",
        offset=_pandas_offset_from_time(session_start),
    ).agg(**agg)

    bars = bars.dropna(subset=["open", "high", "low", "close"])
    if bars.empty:
        return bars

    bars["session_date"] = bars.index.date.astype("str")
    return bars


def _resample_big_order_features(
    ticks: pd.DataFrame,
    timeframe_min: int,
    threshold: int,
    session_start: str,
    session_end: str,
) -> pd.DataFrame:
    session_ticks = filter_session(ticks, session_start=session_start, session_end=session_end)
    if session_ticks.empty:
        return pd.DataFrame(columns=["has_big_buy", "has_big_sell", "max_big_buy", "max_big_sell"])

    session_ticks["is_big_buy"] = session_ticks["ask_volume"] >= threshold
    session_ticks["is_big_sell"] = session_ticks["bid_volume"] >= threshold
    session_ticks["big_buy_size"] = session_ticks["ask_volume"].where(session_ticks["is_big_buy"], 0)
    session_ticks["big_sell_size"] = session_ticks["bid_volume"].where(session_ticks["is_big_sell"], 0)

    sort_cols = ["timestamp", "seq"] if "seq" in session_ticks.columns else ["timestamp"]
    session_ticks = session_ticks.sort_values(sort_cols, kind="stable")
    indexed = session_ticks.set_index("timestamp")

    features = indexed.resample(
        rule=f"{timeframe_min}min",
        label="right",
        closed="left",
        origin="start_day",
        offset=_pandas_offset_from_time(session_start),
    ).agg(
        has_big_buy=("is_big_buy", "max"),
        has_big_sell=("is_big_sell", "max"),
        max_big_buy=("big_buy_size", "max"),
        max_big_sell=("big_sell_size", "max"),
    )

    features = features.dropna(how="all")
    if features.empty:
        return features

    features["has_big_buy"] = features["has_big_buy"].fillna(False).astype(bool)
    features["has_big_sell"] = features["has_big_sell"].fillna(False).astype(bool)
    features["max_big_buy"] = pd.to_numeric(features["max_big_buy"], errors="coerce").fillna(0)
    features["max_big_sell"] = pd.to_numeric(features["max_big_sell"], errors="coerce").fillna(0)
    return features


def build_layered_bars(
    ticks: pd.DataFrame,
    timeframe_min: int,
    threshold: int,
    session_start: str,
    session_end: str,
    ohlc_1m: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if ohlc_1m is None:
        return resample_ticks_to_bars(
            ticks=ticks,
            timeframe_min=timeframe_min,
            threshold=threshold,
            session_start=session_start,
            session_end=session_end,
        )

    price_bars = _resample_price_bars_from_ohlc(
        ohlc_1m=ohlc_1m,
        timeframe_min=timeframe_min,
        session_start=session_start,
        session_end=session_end,
    )
    if price_bars.empty:
        return price_bars

    features = _resample_big_order_features(
        ticks=ticks,
        timeframe_min=timeframe_min,
        threshold=threshold,
        session_start=session_start,
        session_end=session_end,
    )

    out = price_bars.join(features, how="left")
    for c in ("has_big_buy", "has_big_sell"):
        if c not in out.columns:
            out[c] = False
        out[c] = out[c].astype("boolean").fillna(False).astype(bool)

    for c in ("max_big_buy", "max_big_sell"):
        if c not in out.columns:
            out[c] = 0
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)

    if "session_date" not in out.columns:
        out["session_date"] = out.index.date.astype("str")

    return out


def resample_ticks_to_bars(
    ticks: pd.DataFrame,
    timeframe_min: int,
    threshold: int,
    session_start: str,
    session_end: str,
) -> pd.DataFrame:
    if timeframe_min <= 0:
        raise ValueError("timeframe_min must be positive")

    session_ticks = filter_session(ticks, session_start=session_start, session_end=session_end)
    if session_ticks.empty:
        return pd.DataFrame()

    session_ticks["is_big_buy"] = session_ticks["ask_volume"] >= threshold
    session_ticks["is_big_sell"] = session_ticks["bid_volume"] >= threshold
    session_ticks["big_buy_size"] = session_ticks["ask_volume"].where(session_ticks["is_big_buy"], 0)
    session_ticks["big_sell_size"] = session_ticks["bid_volume"].where(session_ticks["is_big_sell"], 0)

    sort_cols = ["timestamp", "seq"] if "seq" in session_ticks.columns else ["timestamp"]
    session_ticks = session_ticks.sort_values(sort_cols, kind="stable")

    indexed = session_ticks.set_index("timestamp")

    bars = indexed.resample(
        rule=f"{timeframe_min}min",
        label="right",
        closed="left",
        origin="start_day",
        offset=_pandas_offset_from_time(session_start),
    ).agg(
        open=("last", "first"),
        high=("last", "max"),
        low=("last", "min"),
        close=("last", "last"),
        volume=("volume", "sum"),
        bid_volume=("bid_volume", "sum"),
        ask_volume=("ask_volume", "sum"),
        tick_count=("last", "size"),
        has_big_buy=("is_big_buy", "max"),
        has_big_sell=("is_big_sell", "max"),
        max_big_buy=("big_buy_size", "max"),
        max_big_sell=("big_sell_size", "max"),
    )

    bars = bars.dropna(subset=["open", "high", "low", "close"])
    if bars.empty:
        return bars

    bars["has_big_buy"] = bars["has_big_buy"].astype(bool)
    bars["has_big_sell"] = bars["has_big_sell"].astype(bool)
    bars["session_date"] = bars.index.date.astype("str")
    return bars
