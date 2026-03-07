from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo
import re

import numpy as np
import pandas as pd


REQUIRED_STD_COLUMNS = ["ts", "trade_price", "trade_size", "bid_price", "ask_price"]
BAR_1M_REQUIRED_COLUMNS = ["ts", "open", "high", "low", "close", "volume", "trade_count"]

TIMEFRAME_RULES = {
    "1s": "1s",
    "5s": "5s",
    "15s": "15s",
    "1m": "1min",
    "2m": "2min",
    "3m": "3min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "4h": "4h",
    "60m": "60min",
    "1d": "1D",
}

BAR_FRAME_COLUMNS = ["ts", "session_date", "timeframe", "symbol_contract", "open", "high", "low", "close", "volume", "trade_count"]

SYMBOL_TICK_SIZES = {
    "MNQ": 0.25,
    "MES": 0.25,
    "MYM": 1.0,
    "MCL": 0.01,
    "MGC": 0.1,
    "NQ": 0.25,
    "ES": 0.25,
    "YM": 1.0,
    "RTY": 0.1,
    "CL": 0.01,
    "GC": 0.1,
    "SI": 0.005,
    "NG": 0.001,
    "ZN": 0.015625,
    "ZB": 0.03125,
    "ZF": 0.0078125,
    "ZT": 0.00390625,
    "6E": 0.00005,
    "6B": 0.0001,
    "6J": 0.0000005,
    "6A": 0.0001,
    "6C": 0.0001,
    "6S": 0.0001,
}


def _find_column(columns: list[str], aliases: list[str]) -> str | None:
    normalized = {c.lower().strip(): c for c in columns}
    for alias in aliases:
        candidate = normalized.get(alias.lower().strip())
        if candidate:
            return candidate
    return None


def normalize_sierra_csv(df: pd.DataFrame, dataset_tz: str, symbol_contract: str | None = None) -> pd.DataFrame:
    cols = list(df.columns)

    ts_col = _find_column(cols, ["timestamp", "date time", "datetime", "date_time", "ts"])
    date_col = _find_column(cols, ["date"])
    time_col = _find_column(cols, ["time"])

    price_col = _find_column(cols, ["trade price", "price", "last", "last price"])
    size_col = _find_column(cols, ["trade size", "size", "volume", "qty", "quantity"])
    bid_col = _find_column(cols, ["bid", "bid price", "bidprice"])
    ask_col = _find_column(cols, ["ask", "ask price", "askprice"])
    symbol_col = _find_column(cols, ["symbol", "symbol_contract", "contract"])

    if ts_col is None and (date_col is None or time_col is None):
        raise ValueError("Timestamp column not found. Expected timestamp or date+time fields.")
    if price_col is None:
        raise ValueError("Trade price column not found.")
    if size_col is None:
        raise ValueError("Trade size/volume column not found.")
    if bid_col is None or ask_col is None:
        raise ValueError("Bid and ask columns are required.")

    out = pd.DataFrame()

    if ts_col:
        out["ts"] = pd.to_datetime(df[ts_col], errors="coerce")
    else:
        out["ts"] = pd.to_datetime(df[date_col].astype(str) + " " + df[time_col].astype(str), errors="coerce")

    tz = ZoneInfo(dataset_tz)
    if out["ts"].dt.tz is None:
        out["ts"] = out["ts"].dt.tz_localize(tz, nonexistent="shift_forward", ambiguous="NaT")
    else:
        out["ts"] = out["ts"].dt.tz_convert(tz)

    out["trade_price"] = pd.to_numeric(df[price_col], errors="coerce")
    out["trade_size"] = pd.to_numeric(df[size_col], errors="coerce")
    out["bid_price"] = pd.to_numeric(df[bid_col], errors="coerce")
    out["ask_price"] = pd.to_numeric(df[ask_col], errors="coerce")

    if symbol_col:
        out["symbol_contract"] = df[symbol_col].astype(str)
    elif symbol_contract:
        out["symbol_contract"] = symbol_contract
    else:
        out["symbol_contract"] = "UNKNOWN"

    out = out.dropna(subset=REQUIRED_STD_COLUMNS)
    out["session_date"] = out["ts"].dt.tz_convert(tz).dt.date
    out = out.sort_values("ts").reset_index(drop=True)
    return out


def normalize_sierra_ohlc_1m(df: pd.DataFrame, dataset_tz: str, symbol_contract: str | None = None) -> pd.DataFrame:
    cols = list(df.columns)

    ts_col = _find_column(cols, ["timestamp", "date time", "datetime", "date_time", "ts"])
    date_col = _find_column(cols, ["date"])
    time_col = _find_column(cols, ["time"])

    open_col = _find_column(cols, ["open"])
    high_col = _find_column(cols, ["high"])
    low_col = _find_column(cols, ["low"])
    close_col = _find_column(cols, ["last", "close", "last price"])
    volume_col = _find_column(cols, ["volume", "vol"])
    trade_count_col = _find_column(cols, ["numberoftrades", "number of trades", "trades", "trade_count", "trade count"])
    symbol_col = _find_column(cols, ["symbol", "symbol_contract", "contract"])

    if ts_col is None and (date_col is None or time_col is None):
        raise ValueError("Timestamp column not found. Expected timestamp or date+time fields.")
    if open_col is None or high_col is None or low_col is None or close_col is None:
        raise ValueError("OHLC columns are required for 1m bar ingest.")
    if volume_col is None:
        raise ValueError("Volume column not found.")
    if trade_count_col is None:
        raise ValueError("NumberOfTrades column not found.")

    out = pd.DataFrame()
    if ts_col:
        out["ts"] = pd.to_datetime(df[ts_col], errors="coerce")
    else:
        out["ts"] = pd.to_datetime(df[date_col].astype(str) + " " + df[time_col].astype(str), errors="coerce")

    tz = ZoneInfo(dataset_tz)
    if out["ts"].dt.tz is None:
        out["ts"] = out["ts"].dt.tz_localize(tz, nonexistent="shift_forward", ambiguous="NaT")
    else:
        out["ts"] = out["ts"].dt.tz_convert(tz)

    out["open"] = pd.to_numeric(df[open_col], errors="coerce")
    out["high"] = pd.to_numeric(df[high_col], errors="coerce")
    out["low"] = pd.to_numeric(df[low_col], errors="coerce")
    out["close"] = pd.to_numeric(df[close_col], errors="coerce")
    out["volume"] = pd.to_numeric(df[volume_col], errors="coerce")
    out["trade_count"] = pd.to_numeric(df[trade_count_col], errors="coerce")

    if symbol_col:
        out["symbol_contract"] = df[symbol_col].astype(str)
    elif symbol_contract:
        out["symbol_contract"] = symbol_contract
    else:
        out["symbol_contract"] = "UNKNOWN"

    out = out.dropna(subset=BAR_1M_REQUIRED_COLUMNS)
    if out.empty:
        raise ValueError("No valid 1m OHLC rows after normalization.")

    aligned = (out["ts"].dt.second == 0) & (out["ts"].dt.microsecond == 0)
    if not bool(aligned.all()):
        misaligned_count = int((~aligned).sum())
        raise ValueError(f"OHLC timestamps must be minute-aligned; found {misaligned_count} non-aligned rows.")

    out["trade_count"] = out["trade_count"].astype(int)
    out = out.sort_values("ts").reset_index(drop=True)
    out = (
        out.groupby(["symbol_contract", "ts"], as_index=False, sort=True)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            trade_count=("trade_count", "sum"),
        )
        .sort_values("ts")
        .reset_index(drop=True)
    )
    out["timeframe"] = "1m"
    out["session_date"] = out["ts"].dt.tz_convert(tz).dt.date
    return out[
        [
            "ts",
            "session_date",
            "timeframe",
            "symbol_contract",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
        ]
    ]


def normalize_sierra_market_data(
    df: pd.DataFrame,
    dataset_tz: str,
    symbol_contract: str | None = None,
) -> tuple[str, pd.DataFrame]:
    tick_error: ValueError | None = None
    try:
        ticks = normalize_sierra_csv(df, dataset_tz=dataset_tz, symbol_contract=symbol_contract)
        return "ticks", ticks
    except ValueError as exc:
        tick_error = exc

    try:
        bars_1m = normalize_sierra_ohlc_1m(df, dataset_tz=dataset_tz, symbol_contract=symbol_contract)
        return "ohlc_1m", bars_1m
    except ValueError as exc:
        tick_msg = str(tick_error) if tick_error else "n/a"
        raise ValueError(f"Unsupported market data schema. Tick parse error: {tick_msg}; OHLC parse error: {exc}") from exc


def resolve_symbol_tick_size(symbol_contract: str | None, fallback: float = 0.25) -> float:
    if not symbol_contract:
        return float(fallback)
    normalized = str(symbol_contract).upper().strip()
    letters_prefix = re.match(r"^[A-Z0-9]+", normalized)
    candidate = letters_prefix.group(0) if letters_prefix else normalized
    for prefix in sorted(SYMBOL_TICK_SIZES.keys(), key=len, reverse=True):
        if candidate.startswith(prefix):
            return float(SYMBOL_TICK_SIZES[prefix])
    return float(fallback)


def build_bars_from_ticks(ticks: pd.DataFrame, timeframe: str, dataset_tz: str) -> pd.DataFrame:
    rule = TIMEFRAME_RULES.get(timeframe)
    if not rule:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    if ticks.empty:
        return pd.DataFrame(columns=["ts", "session_date", "timeframe", "symbol_contract", "open", "high", "low", "close", "volume", "trade_count"])

    df = ticks.copy()
    df = df.sort_values("ts")
    df = df.set_index("ts")

    rows: list[pd.DataFrame] = []
    for symbol, group in df.groupby("symbol_contract"):
        agg = group.resample(rule).agg(
            open=("trade_price", "first"),
            high=("trade_price", "max"),
            low=("trade_price", "min"),
            close=("trade_price", "last"),
            volume=("trade_size", "sum"),
            trade_count=("trade_size", "count"),
        )
        agg = agg.dropna(subset=["open", "high", "low", "close"]).reset_index()
        agg["symbol_contract"] = symbol
        agg["timeframe"] = timeframe
        agg["session_date"] = agg["ts"].dt.tz_convert(ZoneInfo(dataset_tz)).dt.date
        rows.append(agg)

    if not rows:
        return pd.DataFrame(columns=["ts", "session_date", "timeframe", "symbol_contract", "open", "high", "low", "close", "volume", "trade_count"])
    return pd.concat(rows, ignore_index=True)


def _empty_bars_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=BAR_FRAME_COLUMNS)


def _append_bar_row(
    rows: list[dict],
    *,
    ts: pd.Timestamp,
    symbol_contract: str,
    timeframe: str,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    volume: float,
    trade_count: int,
    dataset_zone: ZoneInfo,
) -> None:
    rows.append(
        {
            "ts": ts,
            "session_date": ts.tz_convert(dataset_zone).date(),
            "timeframe": timeframe,
            "symbol_contract": symbol_contract,
            "open": float(open_price),
            "high": float(high_price),
            "low": float(low_price),
            "close": float(close_price),
            "volume": float(volume),
            "trade_count": int(trade_count),
        }
    )


def build_tick_bars_from_ticks(
    ticks: pd.DataFrame,
    bar_size: int,
    dataset_tz: str,
    timeframe: str | None = None,
) -> pd.DataFrame:
    size = int(bar_size)
    if size < 1:
        raise ValueError("bar_size must be >= 1")
    if ticks.empty:
        return _empty_bars_frame()

    timeframe_value = timeframe or f"tick:{size}"
    rows: list[dict] = []
    dataset_zone = ZoneInfo(dataset_tz)
    df = ticks.sort_values("ts")
    for symbol, group in df.groupby("symbol_contract", sort=False):
        prices = pd.to_numeric(group["trade_price"], errors="coerce").to_numpy(dtype=float)
        qtys = pd.to_numeric(group["trade_size"], errors="coerce").to_numpy(dtype=float)
        ts = pd.to_datetime(group["ts"], errors="coerce")
        if ts.dt.tz is None:
            ts = ts.dt.tz_localize("UTC")
        ts_values = ts.tolist()
        n = len(prices)
        if n == 0:
            continue

        for start_idx in range(0, n, size):
            stop_idx = min(start_idx + size, n)
            if stop_idx <= start_idx:
                continue
            segment_prices = prices[start_idx:stop_idx]
            segment_qtys = qtys[start_idx:stop_idx]
            close_idx = stop_idx - 1
            _append_bar_row(
                rows,
                ts=pd.Timestamp(ts_values[close_idx]),
                symbol_contract=str(symbol),
                timeframe=timeframe_value,
                open_price=float(segment_prices[0]),
                high_price=float(np.max(segment_prices)),
                low_price=float(np.min(segment_prices)),
                close_price=float(segment_prices[-1]),
                volume=float(np.sum(segment_qtys)),
                trade_count=int(stop_idx - start_idx),
                dataset_zone=dataset_zone,
            )
    if not rows:
        return _empty_bars_frame()
    return pd.DataFrame(rows, columns=BAR_FRAME_COLUMNS)


def build_volume_bars_from_ticks(
    ticks: pd.DataFrame,
    bar_size: int,
    dataset_tz: str,
    timeframe: str | None = None,
) -> pd.DataFrame:
    size = int(bar_size)
    if size < 1:
        raise ValueError("bar_size must be >= 1")
    if ticks.empty:
        return _empty_bars_frame()

    timeframe_value = timeframe or f"volume:{size}"
    rows: list[dict] = []
    dataset_zone = ZoneInfo(dataset_tz)
    df = ticks.sort_values("ts")
    for symbol, group in df.groupby("symbol_contract", sort=False):
        prices = pd.to_numeric(group["trade_price"], errors="coerce").to_numpy(dtype=float)
        qtys = pd.to_numeric(group["trade_size"], errors="coerce").to_numpy(dtype=float)
        ts = pd.to_datetime(group["ts"], errors="coerce")
        if ts.dt.tz is None:
            ts = ts.dt.tz_localize("UTC")
        ts_values = ts.tolist()
        n = len(prices)
        if n == 0:
            continue

        open_price = high_price = low_price = close_price = None
        volume = 0.0
        trade_count = 0
        close_ts: pd.Timestamp | None = None
        for idx in range(n):
            price = float(prices[idx])
            qty = float(qtys[idx])
            current_ts = pd.Timestamp(ts_values[idx])
            if open_price is None:
                open_price = high_price = low_price = close_price = price
                volume = qty
                trade_count = 1
                close_ts = current_ts
            else:
                high_price = max(float(high_price), price)
                low_price = min(float(low_price), price)
                close_price = price
                volume += qty
                trade_count += 1
                close_ts = current_ts

            if volume >= size:
                _append_bar_row(
                    rows,
                    ts=close_ts,
                    symbol_contract=str(symbol),
                    timeframe=timeframe_value,
                    open_price=float(open_price),
                    high_price=float(high_price),
                    low_price=float(low_price),
                    close_price=float(close_price),
                    volume=volume,
                    trade_count=trade_count,
                    dataset_zone=dataset_zone,
                )
                open_price = high_price = low_price = close_price = None
                volume = 0.0
                trade_count = 0
                close_ts = None
        if trade_count > 0 and close_ts is not None and open_price is not None:
            _append_bar_row(
                rows,
                ts=close_ts,
                symbol_contract=str(symbol),
                timeframe=timeframe_value,
                open_price=float(open_price),
                high_price=float(high_price),
                low_price=float(low_price),
                close_price=float(close_price),
                volume=volume,
                trade_count=trade_count,
                dataset_zone=dataset_zone,
            )
    if not rows:
        return _empty_bars_frame()
    return pd.DataFrame(rows, columns=BAR_FRAME_COLUMNS)


def build_range_bars_from_ticks(
    ticks: pd.DataFrame,
    bar_size: int,
    tick_size: float,
    dataset_tz: str,
    timeframe: str | None = None,
) -> pd.DataFrame:
    size = int(bar_size)
    if size < 1:
        raise ValueError("bar_size must be >= 1")
    resolved_tick = float(tick_size)
    if resolved_tick <= 0:
        raise ValueError("tick_size must be > 0")
    if ticks.empty:
        return _empty_bars_frame()

    timeframe_value = timeframe or f"range:{size}"
    range_span = resolved_tick * size
    rows: list[dict] = []
    dataset_zone = ZoneInfo(dataset_tz)
    df = ticks.sort_values("ts")
    for symbol, group in df.groupby("symbol_contract", sort=False):
        prices = pd.to_numeric(group["trade_price"], errors="coerce").to_numpy(dtype=float)
        qtys = pd.to_numeric(group["trade_size"], errors="coerce").to_numpy(dtype=float)
        ts = pd.to_datetime(group["ts"], errors="coerce")
        if ts.dt.tz is None:
            ts = ts.dt.tz_localize("UTC")
        ts_values = ts.tolist()
        n = len(prices)
        if n == 0:
            continue

        open_price = high_price = low_price = close_price = None
        volume = 0.0
        trade_count = 0
        close_ts: pd.Timestamp | None = None

        for idx in range(n):
            price = float(prices[idx])
            qty = float(qtys[idx])
            current_ts = pd.Timestamp(ts_values[idx])

            if open_price is None:
                open_price = high_price = low_price = close_price = price
                volume = qty
                trade_count = 1
                close_ts = current_ts
            else:
                high_price = max(float(high_price), price)
                low_price = min(float(low_price), price)
                close_price = price
                volume += qty
                trade_count += 1
                close_ts = current_ts

            if close_price >= (float(open_price) + range_span):
                fixed_close = float(open_price) + range_span
            elif close_price <= (float(open_price) - range_span):
                fixed_close = float(open_price) - range_span
            else:
                fixed_close = None

            if fixed_close is not None:
                _append_bar_row(
                    rows,
                    ts=close_ts,
                    symbol_contract=str(symbol),
                    timeframe=timeframe_value,
                    open_price=float(open_price),
                    high_price=max(float(high_price), fixed_close),
                    low_price=min(float(low_price), fixed_close),
                    close_price=fixed_close,
                    volume=volume,
                    trade_count=trade_count,
                    dataset_zone=dataset_zone,
                )
                # Fixed-span no-overlap: next bar starts at prior close boundary.
                open_price = high_price = low_price = close_price = fixed_close
                volume = 0.0
                trade_count = 0
                close_ts = current_ts

        if trade_count > 0 and close_ts is not None and open_price is not None:
            _append_bar_row(
                rows,
                ts=close_ts,
                symbol_contract=str(symbol),
                timeframe=timeframe_value,
                open_price=float(open_price),
                high_price=float(high_price),
                low_price=float(low_price),
                close_price=float(close_price),
                volume=volume,
                trade_count=trade_count,
                dataset_zone=dataset_zone,
            )

    if not rows:
        return _empty_bars_frame()
    return pd.DataFrame(rows, columns=BAR_FRAME_COLUMNS)


def build_volume_profile_levels(
    ticks: pd.DataFrame,
    tick_size: float,
    group_columns: list[str] | None = None,
    price_min: float | None = None,
    price_max: float | None = None,
) -> pd.DataFrame:
    group_columns = group_columns or []
    out_columns = [*group_columns, "price_level", "volume"]
    if ticks.empty:
        return pd.DataFrame(columns=out_columns)

    df = ticks.copy()
    if price_min is not None:
        df = df[df["trade_price"] >= float(price_min)]
    if price_max is not None:
        df = df[df["trade_price"] <= float(price_max)]
    if df.empty:
        return pd.DataFrame(columns=out_columns)

    df["price_level"] = _round_price_to_tick(df["trade_price"], tick_size)
    profile = (
        df.groupby([*group_columns, "price_level"], as_index=False)["trade_size"]
        .sum()
        .rename(columns={"trade_size": "volume"})
    )
    if group_columns:
        profile = profile.sort_values([*group_columns, "price_level"]).reset_index(drop=True)
    else:
        profile = profile.sort_values(["price_level"]).reset_index(drop=True)
    return profile


def build_session_profile(ticks: pd.DataFrame, tick_size: float = 0.25) -> pd.DataFrame:
    if ticks.empty:
        return pd.DataFrame(columns=["session_date", "symbol_contract", "price_level", "volume"])
    return build_volume_profile_levels(
        ticks=ticks[["session_date", "symbol_contract", "trade_price", "trade_size"]].copy(),
        tick_size=tick_size,
        group_columns=["session_date", "symbol_contract"],
    )


def _round_price_to_tick(prices: pd.Series, tick_size: float) -> pd.Series:
    tick = float(tick_size)
    if tick <= 0:
        raise ValueError("tick_size must be > 0")
    scaled = pd.to_numeric(prices, errors="coerce") / tick
    # Use half-up behavior (not bankers' rounding) to avoid alternating bucket bias
    # when prices frequently land on exact half-tick boundaries.
    rounded = np.floor(scaled + 0.5 + 1e-12)
    return rounded * tick


def classify_trade_side(ticks: pd.DataFrame) -> pd.Series:
    if ticks.empty:
        return pd.Series(dtype="object", index=ticks.index)

    trade_price = pd.to_numeric(ticks["trade_price"], errors="coerce")
    bid_price = (
        pd.to_numeric(ticks["bid_price"], errors="coerce")
        if "bid_price" in ticks.columns
        else pd.Series(np.nan, index=ticks.index, dtype="float64")
    )
    ask_price = (
        pd.to_numeric(ticks["ask_price"], errors="coerce")
        if "ask_price" in ticks.columns
        else pd.Series(np.nan, index=ticks.index, dtype="float64")
    )
    side = pd.Series("unknown", index=ticks.index, dtype="object")
    quotes_valid = (
        bid_price.notna()
        & ask_price.notna()
        & np.isfinite(bid_price)
        & np.isfinite(ask_price)
        & (ask_price >= bid_price)
    )
    unlocked_quotes = quotes_valid & (ask_price > bid_price)
    buy_touch = unlocked_quotes & (trade_price >= ask_price)
    sell_touch = unlocked_quotes & (trade_price <= bid_price)
    side.loc[buy_touch] = "buy"
    side.loc[sell_touch] = "sell"

    midpoint = (bid_price + ask_price) / 2.0
    in_spread = unlocked_quotes & ~(buy_touch | sell_touch)
    side.loc[in_spread & (trade_price > midpoint)] = "buy"
    side.loc[in_spread & (trade_price < midpoint)] = "sell"

    # Locked quotes (bid == ask) cannot indicate side from touch logic; use tick rule.
    unresolved_with_quotes = (side == "unknown") & quotes_valid
    price_delta = trade_price.diff()
    side.loc[unresolved_with_quotes & (price_delta > 0)] = "buy"
    side.loc[unresolved_with_quotes & (price_delta < 0)] = "sell"

    # If price is unchanged on locked quotes, inherit last resolved side for continuity.
    known_side = side.where(side != "unknown")
    carried_side = known_side.ffill()
    side.loc[unresolved_with_quotes & side.eq("unknown") & carried_side.notna()] = carried_side[
        unresolved_with_quotes & side.eq("unknown") & carried_side.notna()
    ]
    return side


def build_delta_profile_levels(
    ticks: pd.DataFrame,
    tick_size: float,
    group_columns: list[str] | None = None,
    price_min: float | None = None,
    price_max: float | None = None,
) -> pd.DataFrame:
    group_columns = group_columns or []
    out_columns = [*group_columns, "price_level", "delta"]
    if ticks.empty:
        return pd.DataFrame(columns=out_columns)

    df = ticks.copy()
    if price_min is not None:
        df = df[df["trade_price"] >= float(price_min)]
    if price_max is not None:
        df = df[df["trade_price"] <= float(price_max)]
    if df.empty:
        return pd.DataFrame(columns=out_columns)

    if "side" not in df.columns:
        df["side"] = classify_trade_side(df)

    side = df["side"].astype(str).str.lower()
    trade_size = pd.to_numeric(df["trade_size"], errors="coerce").fillna(0.0)
    sign = np.where(side.eq("buy"), 1.0, np.where(side.eq("sell"), -1.0, 0.0))
    df["delta"] = trade_size * sign
    df["price_level"] = _round_price_to_tick(df["trade_price"], tick_size)
    profile = df.groupby([*group_columns, "price_level"], as_index=False)["delta"].sum()
    profile = profile[~np.isclose(profile["delta"], 0.0)]
    if group_columns:
        profile = profile.sort_values([*group_columns, "price_level"]).reset_index(drop=True)
    else:
        profile = profile.sort_values(["price_level"]).reset_index(drop=True)
    return profile


def detect_large_orders(
    ticks: pd.DataFrame,
    method: str,
    fixed_threshold: float = 25.0,
    percentile: float = 99.0,
) -> pd.DataFrame:
    if ticks.empty:
        return pd.DataFrame(
            columns=["ts", "session_date", "symbol_contract", "trade_price", "trade_size", "method", "threshold", "side"]
        )

    method = method.lower()
    df = ticks[["ts", "session_date", "symbol_contract", "trade_price", "trade_size", "bid_price", "ask_price"]].copy()
    df["side"] = classify_trade_side(df)

    if method == "fixed":
        threshold = float(fixed_threshold)
        out = df[df["trade_size"] >= threshold].copy()
        out["method"] = "fixed"
        out["threshold"] = threshold
        return out.drop(columns=["bid_price", "ask_price"])

    if method == "relative":
        thresholds = (
            df.groupby(["session_date", "symbol_contract"], as_index=False)["trade_size"]
            .quantile(percentile / 100.0)
            .rename(columns={"trade_size": "threshold"})
        )
        out = df.merge(thresholds, on=["session_date", "symbol_contract"], how="left")
        out = out[out["trade_size"] >= out["threshold"]].copy()
        out["method"] = "relative"
        return out.drop(columns=["bid_price", "ask_price"])

    raise ValueError("method must be 'fixed' or 'relative'")


def parse_timestamp(value: str, dataset_tz: str) -> datetime:
    ts = pd.to_datetime(value)
    if ts.tzinfo is None:
        return ts.tz_localize(ZoneInfo(dataset_tz)).to_pydatetime()
    return ts.tz_convert(ZoneInfo(dataset_tz)).to_pydatetime()
