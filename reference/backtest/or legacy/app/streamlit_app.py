from __future__ import annotations

import argparse
import gc
import json
import shutil
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Literal
from urllib.parse import urlencode

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
except Exception:  # pragma: no cover - optional import during partial env setup
    AgGrid = None
    GridOptionsBuilder = None
    GridUpdateMode = None
    JsCode = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from orbt.backtest import build_vectorbt_portfolio, simulate_orb_big_trade_strategy
from orbt.ingest_sierra import read_sierra_export
from orbt.metrics import summarize_trades
from orbt.resample import build_layered_bars, load_sierra_ohlc_1m

StopMode = Literal["or_boundary", "or_mid"]
EntryMode = Literal["first_outside", "reentry_after_stop"]
StrategyMode = Literal["big_order_required", "breakout_only"]

RUNS_ROOT = PROJECT_ROOT / "runs"

HUMAN_COLUMN_NAMES = {
    "combo": "Scenario",
    "timeframe_min": "Timeframe (Min)",
    "ib_minutes": "Opening Range",
    "trades": "Trades",
    "wins": "Wins",
    "losses": "Losses",
    "full_tp_wins": "Full TP Wins",
    "full_losses": "Full Losses",
    "win_rate": "Win Rate",
    "total_pnl": "Total PnL (Points)",
    "avg_pnl": "Avg PnL (Points)",
    "total_r": "Total R",
    "avg_r": "Avg R",
    "stop_mode": "Stop Mode",
    "tp_r_multiple": "TP-R Multiple",
    "entry_mode": "Entry Mode",
    "strategy_mode": "Strategy",
    "vectorbt_total_return": "VectorBT Total Return",
    "session_date": "Session Date",
    "side": "Side",
    "entry_time": "Entry Time",
    "entry_price": "Entry Price",
    "stop_price": "Stop Price",
    "target_price": "Target Price",
    "exit_time": "Exit Time",
    "exit_price": "Exit Price",
    "exit_reason": "Exit Reason",
    "pnl": "PnL (Points)",
    "r_multiple": "R Multiple",
    "trade_result": "Win/Loss",
}


@st.cache_data
def load_run_artifacts(run_dir: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    base = Path(run_dir)
    metrics = pd.read_csv(base / "metrics.csv")
    trades = pd.read_csv(base / "trades.csv")
    if not trades.empty:
        trades["entry_time"] = pd.to_datetime(trades["entry_time"])
        trades["exit_time"] = pd.to_datetime(trades["exit_time"])
    return metrics, trades


@st.cache_data
def load_bars(run_dir: str, combo: str) -> pd.DataFrame:
    base = Path(run_dir)
    path = base / "bars" / f"bars_{combo}.parquet"
    bars = pd.read_parquet(path)
    bars.index = pd.to_datetime(bars.index)
    return bars


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--run-dir", default="runs/latest")
    return parser.parse_known_args()[0]


DISPLAY_FILTER_COLUMNS_METRICS = {
    "combo",
    "timeframe_min",
    "ib_minutes",
    "stop_mode",
    "tp_r_multiple",
    "entry_mode",
    "strategy_mode",
}
DISPLAY_FILTER_COLUMNS_TRADES = {"combo", "timeframe_min", "ib_minutes"}
TRADES_PAGE_SIZE = 10


def _humanize_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={c: HUMAN_COLUMN_NAMES.get(c, c) for c in df.columns})


def _drop_display_columns(df: pd.DataFrame, columns_to_hide: set[str]) -> pd.DataFrame:
    keep = [c for c in df.columns if c not in columns_to_hide]
    if not keep:
        return df
    return df[keep].copy()


def _reorder_metrics_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(df.columns)

    if "wins" in cols and "full_tp_wins" in cols:
        cols.remove("full_tp_wins")
        cols.insert(cols.index("wins") + 1, "full_tp_wins")

    if "losses" in cols and "full_losses" in cols:
        cols.remove("full_losses")
        cols.insert(cols.index("losses") + 1, "full_losses")

    return df[cols].copy()


def _ensure_full_tp_wins(metrics: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    if "full_tp_wins" in metrics.columns:
        return metrics

    out = metrics.copy()
    if trades.empty:
        out["full_tp_wins"] = 0
        return out

    target_mask = (trades["exit_reason"] == "target") & (trades["pnl"] > 0)
    if "combo" in out.columns and "combo" in trades.columns:
        by_combo = trades.loc[target_mask].groupby("combo").size()
        out["full_tp_wins"] = out["combo"].map(by_combo).fillna(0).astype(int)
    else:
        out["full_tp_wins"] = int(target_mask.sum())
    return out


def _ensure_full_losses(metrics: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    if "full_losses" in metrics.columns:
        return metrics

    out = metrics.copy()
    if trades.empty:
        out["full_losses"] = 0
        return out

    full_loss_mask = (trades["exit_reason"] == "stop") & (trades["pnl"] < 0)
    if "combo" in out.columns and "combo" in trades.columns:
        by_combo = trades.loc[full_loss_mask].groupby("combo").size()
        out["full_losses"] = out["combo"].map(by_combo).fillna(0).astype(int)
    else:
        out["full_losses"] = int(full_loss_mask.sum())
    return out


def default_ui_params() -> dict[str, int | float | str]:
    return {
        "ib_minutes": 15,
        "timeframe_min": 1,
        "tp_r_multiple": 2.0,
        "big_trade_threshold": 25,
        "stop_mode": "or_boundary",
        "entry_mode": "first_outside",
        "strategy_mode": "big_order_required",
    }


def build_combo_id(timeframe_min: int, ib_minutes: int) -> str:
    return f"tf{timeframe_min}_ib{ib_minutes}"


def _qp_get(key: str) -> str | None:
    value = st.query_params.get(key)
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _clear_trade_selection_query_params() -> None:
    for key in ("trade_id", "trade_combo", "trade_page", "date"):
        if key in st.query_params:
            del st.query_params[key]


def _clear_external_trades_filters() -> None:
    st.session_state.trades_external_result_filter = "All"
    st.session_state.trades_external_side_filter = "All"
    st.session_state.trades_external_exit_reason_filter = "All"
    st.session_state.trades_page_input = 1
    _clear_trade_selection_query_params()


def _build_trade_link(
    trade_id: int,
    combo: str,
    session_date: str,
    ib_minutes: int,
    timeframe_min: int,
    tp_r_multiple: float,
    big_trade_threshold: int,
    stop_mode: str,
    entry_mode: str,
    strategy_mode: str,
) -> str:
    query = urlencode(
        {
            "trade_id": trade_id,
            "trade_combo": combo,
            "date": session_date,
            "ib_minutes": ib_minutes,
            "timeframe_min": timeframe_min,
            "tp_r_multiple": tp_r_multiple,
            "big_trade_threshold": big_trade_threshold,
            "stop_mode": stop_mode,
            "entry_mode": entry_mode,
            "strategy_mode": strategy_mode,
        }
    )
    return f"?{query}#price-signals-anchor"


def _optimize_ticks(df: pd.DataFrame) -> pd.DataFrame:
    keep_cols = ["timestamp", "last", "volume", "bid_volume", "ask_volume"]
    if "seq" in df.columns:
        keep_cols.append("seq")
    out = df[keep_cols].copy()

    out["last"] = pd.to_numeric(out["last"], downcast="float")
    out["volume"] = pd.to_numeric(out["volume"], downcast="integer")
    out["bid_volume"] = pd.to_numeric(out["bid_volume"], downcast="integer")
    out["ask_volume"] = pd.to_numeric(out["ask_volume"], downcast="integer")
    if "seq" in out.columns:
        out["seq"] = pd.to_numeric(out["seq"], downcast="integer")

    return out


def _load_ticks(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".txt", ".csv"}:
        return _optimize_ticks(read_sierra_export(path))

    columns = ["timestamp", "last", "volume", "bid_volume", "ask_volume", "seq"]
    try:
        df = pd.read_parquet(path, columns=columns)
    except Exception:
        df = pd.read_parquet(path)
    return _optimize_ticks(df)



@st.cache_data
def load_data_date_bounds(data_path: str) -> tuple[date, date]:
    path = Path(data_path)
    if not path.exists():
        raise FileNotFoundError(f"Tick data path does not exist: {path}")

    if path.suffix.lower() in {".parquet"}:
        try:
            ts_df = pd.read_parquet(path, columns=["timestamp"])
        except Exception:
            ts_df = pd.read_parquet(path)
            if "timestamp" not in ts_df.columns:
                raise ValueError("Parquet data is missing required 'timestamp' column")
            ts_df = ts_df[["timestamp"]]
        ts = pd.to_datetime(ts_df["timestamp"], errors="coerce")
    elif path.suffix.lower() in {".txt", ".csv"}:
        raw = pd.read_csv(path, usecols=["Date", "Time"])
        ts = pd.to_datetime(
            raw["Date"].astype(str).str.strip() + " " + raw["Time"].astype(str).str.strip(),
            errors="coerce",
        )
    else:
        raise ValueError(f"Unsupported data file type for date bounds: {path.suffix}")

    valid = ts.dropna()
    if valid.empty:
        raise ValueError(f"No valid timestamps found in data: {path}")

    min_date = valid.dt.date.min()
    max_date = valid.dt.date.max()
    return min_date, max_date

def _resolve_data_path(path_str: str, project_root: Path) -> Path:
    p = Path(path_str)
    if p.is_absolute():
        return p
    return project_root / p


def _load_source_config(run_dir: Path) -> dict:
    config_path = run_dir / "config_used.json"
    if not config_path.exists():
        raise FileNotFoundError(f"Missing config file: {config_path}")
    return json.loads(config_path.read_text(encoding="utf-8"))


def _create_run_dir(runs_root: Path) -> Path:
    runs_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = runs_root / f"run_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir



def recompute_and_persist(
    source_run_dir: str | Path,
    ib_minutes: int,
    timeframe_min: int,
    tp_r_multiple: float,
    big_trade_threshold: int,
    stop_mode: StopMode,
    entry_mode: EntryMode = "first_outside",
    strategy_mode: StrategyMode = "big_order_required",
    start_date: date | None = None,
    end_date: date | None = None,
    project_root: str | Path = PROJECT_ROOT,
) -> tuple[Path, str]:
    source_run_dir = Path(source_run_dir)
    project_root = Path(project_root)

    source_cfg = _load_source_config(source_run_dir)

    data_path_raw = source_cfg.get("data_path")
    if not isinstance(data_path_raw, str) or not data_path_raw.strip():
        raise ValueError("Invalid data_path in config_used.json")

    data_path = _resolve_data_path(data_path_raw, project_root=project_root)
    if not data_path.exists():
        raise FileNotFoundError(f"Tick data path does not exist: {data_path}")

    session_start = str(source_cfg.get("session_start", "09:30:00"))
    session_end = str(source_cfg.get("session_end", "16:00:00"))

    ohlc_1m: pd.DataFrame | None = None
    ohlc_path_raw = source_cfg.get("ohlc_1m_path")
    if isinstance(ohlc_path_raw, str) and ohlc_path_raw.strip():
        ohlc_path = _resolve_data_path(ohlc_path_raw, project_root=project_root)
        if not ohlc_path.exists():
            raise FileNotFoundError(f"OHLC data path does not exist: {ohlc_path}")
        ohlc_1m = load_sierra_ohlc_1m(
            ohlc_path,
            session_start=session_start,
            session_end=session_end,
        )

    selected_start = start_date
    selected_end = end_date
    if selected_start is not None and selected_end is not None and selected_start > selected_end:
        raise ValueError("Start date must be on or before end date")

    ticks = _load_ticks(data_path)
    if selected_start is not None or selected_end is not None:
        tick_dates = pd.to_datetime(ticks["timestamp"], errors="coerce").dt.date
        if selected_start is None:
            selected_start = tick_dates.min()
        if selected_end is None:
            selected_end = tick_dates.max()

        mask = tick_dates.between(selected_start, selected_end, inclusive="both")
        ticks = ticks.loc[mask].copy()
        if ticks.empty:
            raise ValueError(
                f"No ticks in selected date range: {selected_start.isoformat()} to {selected_end.isoformat()}"
            )

        if ohlc_1m is not None:
            ohlc_dates = pd.Series(ohlc_1m.index.date, index=ohlc_1m.index)
            ohlc_mask = ohlc_dates.between(selected_start, selected_end, inclusive="both")
            ohlc_1m = ohlc_1m.loc[ohlc_mask].copy()
            if ohlc_1m.empty:
                raise ValueError(
                    f"No OHLC bars in selected date range: {selected_start.isoformat()} to {selected_end.isoformat()}"
                )

    bars = build_layered_bars(
        ticks=ticks,
        timeframe_min=timeframe_min,
        threshold=big_trade_threshold,
        session_start=session_start,
        session_end=session_end,
        ohlc_1m=ohlc_1m,
    )

    trades, annotated_bars = simulate_orb_big_trade_strategy(
        bars=bars,
        timeframe_min=timeframe_min,
        ib_minutes=ib_minutes,
        session_start=session_start,
        session_end=session_end,
        stop_mode=stop_mode,
        tp_r_multiple=tp_r_multiple,
        entry_mode=entry_mode,
        strategy_mode=strategy_mode,
    )

    combo_id = build_combo_id(timeframe_min=timeframe_min, ib_minutes=ib_minutes)
    run_dir = _create_run_dir(project_root / "runs")
    bars_dir = run_dir / "bars"
    bars_dir.mkdir(parents=True, exist_ok=True)

    annotated_bars.to_parquet(bars_dir / f"bars_{combo_id}.parquet")

    metric = summarize_trades(trades)
    metric.update(
        {
            "combo": combo_id,
            "timeframe_min": timeframe_min,
            "ib_minutes": ib_minutes,
            "stop_mode": stop_mode,
            "tp_r_multiple": tp_r_multiple,
            "entry_mode": entry_mode,
            "strategy_mode": strategy_mode,
            "vectorbt_total_return": 0.0,
        }
    )

    if not trades.empty:
        trades = trades.copy()
        trades["combo"] = combo_id
        try:
            pf = build_vectorbt_portfolio(annotated_bars, trades)
            metric["vectorbt_total_return"] = float(pf.total_return())
        except Exception:
            metric["vectorbt_total_return"] = 0.0

    metrics_df = pd.DataFrame([metric])
    metrics_df.to_csv(run_dir / "metrics.csv", index=False)

    if trades.empty:
        trades_df = pd.DataFrame(
            columns=[
                "session_date",
                "timeframe_min",
                "ib_minutes",
                "side",
                "entry_time",
                "entry_price",
                "stop_price",
                "target_price",
                "exit_time",
                "exit_price",
                "exit_reason",
                "pnl",
                "r_multiple",
                "combo",
            ]
        )
    else:
        trades_df = trades
    trades_df.to_csv(run_dir / "trades.csv", index=False)

    try:
        data_path_for_cfg = str(data_path.relative_to(project_root))
    except ValueError:
        data_path_for_cfg = str(data_path)

    ohlc_path_for_cfg = source_cfg.get("ohlc_1m_path")
    if isinstance(ohlc_path_raw, str) and ohlc_path_raw.strip():
        resolved_ohlc = _resolve_data_path(ohlc_path_raw, project_root=project_root)
        try:
            ohlc_path_for_cfg = str(resolved_ohlc.relative_to(project_root))
        except ValueError:
            ohlc_path_for_cfg = str(resolved_ohlc)

    out_cfg = dict(source_cfg)
    out_cfg.update(
        {
            "data_path": data_path_for_cfg,
            "ohlc_1m_path": ohlc_path_for_cfg,
            "ib_windows_min": [ib_minutes],
            "timeframes_min": [timeframe_min],
            "ib_minutes": ib_minutes,
            "timeframe_min": timeframe_min,
            "tp_r_multiple": tp_r_multiple,
            "big_trade_threshold": big_trade_threshold,
            "stop_mode": stop_mode,
            "entry_mode": entry_mode,
            "strategy_mode": strategy_mode,
        }
    )
    if selected_start is not None:
        out_cfg["date_start"] = selected_start.isoformat()
    if selected_end is not None:
        out_cfg["date_end"] = selected_end.isoformat()
    (run_dir / "config_used.json").write_text(json.dumps(out_cfg, indent=2), encoding="utf-8")

    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "created_at": datetime.now().isoformat(),
                "metrics_path": "metrics.csv",
                "trades_path": "trades.csv",
                "bars_dir": "bars",
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    latest_link = project_root / "runs" / "latest"
    if latest_link.is_symlink() or latest_link.is_file():
        latest_link.unlink()
    elif latest_link.exists() and latest_link.is_dir():
        shutil.rmtree(latest_link)
    latest_link.symlink_to(run_dir.resolve(), target_is_directory=True)

    del ticks
    del bars
    del annotated_bars
    gc.collect()

    return run_dir, combo_id



def _scaled_bubble_sizes(
    values: pd.Series, min_diameter: float = 24.0, max_diameter: float = 56.0
) -> list[float]:
    s = pd.to_numeric(values, errors="coerce").fillna(0.0).astype(float)
    if s.empty:
        return []

    max_val = float(s.max())
    if max_val <= 0:
        return [min_diameter] * len(s)

    return (min_diameter + ((s / max_val) * (max_diameter - min_diameter))).tolist()


def _build_signed_line_segments(x: pd.Series, y: pd.Series) -> tuple[list, list, list, list]:
    red_x: list = []
    red_y: list = []
    green_x: list = []
    green_y: list = []

    if len(y) < 2:
        return red_x, red_y, green_x, green_y

    for i in range(1, len(y)):
        x0 = pd.Timestamp(x.iloc[i - 1])
        x1 = pd.Timestamp(x.iloc[i])
        y0 = y.iloc[i - 1]
        y1 = y.iloc[i]

        if pd.isna(y0) or pd.isna(y1):
            continue

        y0f = float(y0)
        y1f = float(y1)

        if y0f > 0 and y1f > 0:
            green_x.extend([x0, x1, None])
            green_y.extend([y0f, y1f, None])
            continue

        if y0f <= 0 and y1f <= 0:
            red_x.extend([x0, x1, None])
            red_y.extend([y0f, y1f, None])
            continue

        # Segment crosses zero: split exactly at the zero intercept so color never bleeds.
        if y1f == y0f:
            continue

        frac = (-y0f) / (y1f - y0f)
        frac = max(0.0, min(1.0, frac))
        x0_ns = x0.value
        x1_ns = x1.value
        cross_ns = int(x0_ns + frac * (x1_ns - x0_ns))
        x_cross = pd.Timestamp(cross_ns)

        if y0f > 0 and y1f <= 0:
            green_x.extend([x0, x_cross, None])
            green_y.extend([y0f, 0.0, None])
            red_x.extend([x_cross, x1, None])
            red_y.extend([0.0, y1f, None])
        else:
            red_x.extend([x0, x_cross, None])
            red_y.extend([y0f, 0.0, None])
            green_x.extend([x_cross, x1, None])
            green_y.extend([0.0, y1f, None])

    return red_x, red_y, green_x, green_y


def _build_negative_fill_segments(x: pd.Series, y: pd.Series) -> list[tuple[list, list]]:
    segments: list[tuple[list, list]] = []
    current_x: list = []
    current_y: list = []

    if len(y) < 2:
        return segments

    for i in range(1, len(y)):
        x0 = pd.Timestamp(x.iloc[i - 1])
        x1 = pd.Timestamp(x.iloc[i])
        y0 = y.iloc[i - 1]
        y1 = y.iloc[i]

        if pd.isna(y0) or pd.isna(y1):
            continue

        y0f = float(y0)
        y1f = float(y1)

        if y0f <= 0 and y1f <= 0:
            if not current_x:
                current_x = [x0]
                current_y = [y0f]
            current_x.append(x1)
            current_y.append(y1f)
            continue

        if y0f > 0 and y1f > 0:
            if current_x:
                segments.append((current_x, current_y))
                current_x, current_y = [], []
            continue

        if y1f == y0f:
            continue

        frac = (-y0f) / (y1f - y0f)
        frac = max(0.0, min(1.0, frac))
        x_cross = pd.Timestamp(int(x0.value + frac * (x1.value - x0.value)))

        if y0f > 0 and y1f <= 0:
            current_x = [x_cross, x1]
            current_y = [0.0, y1f]
        else:
            if not current_x:
                current_x = [x0]
                current_y = [y0f]
            current_x.append(x_cross)
            current_y.append(0.0)
            segments.append((current_x, current_y))
            current_x, current_y = [], []

    if current_x:
        segments.append((current_x, current_y))

    return segments


def _build_positive_fill_segments(x: pd.Series, y: pd.Series) -> list[tuple[list, list]]:
    segments: list[tuple[list, list]] = []
    current_x: list = []
    current_y: list = []

    if len(y) < 2:
        return segments

    for i in range(1, len(y)):
        x0 = pd.Timestamp(x.iloc[i - 1])
        x1 = pd.Timestamp(x.iloc[i])
        y0 = y.iloc[i - 1]
        y1 = y.iloc[i]

        if pd.isna(y0) or pd.isna(y1):
            continue

        y0f = float(y0)
        y1f = float(y1)

        if y0f > 0 and y1f > 0:
            if not current_x:
                current_x = [x0]
                current_y = [y0f]
            current_x.append(x1)
            current_y.append(y1f)
            continue

        if y0f <= 0 and y1f <= 0:
            if current_x:
                segments.append((current_x, current_y))
                current_x, current_y = [], []
            continue

        if y1f == y0f:
            continue

        frac = (-y0f) / (y1f - y0f)
        frac = max(0.0, min(1.0, frac))
        x_cross = pd.Timestamp(int(x0.value + frac * (x1.value - x0.value)))

        if y0f <= 0 and y1f > 0:
            current_x = [x_cross, x1]
            current_y = [0.0, y1f]
        else:
            if not current_x:
                current_x = [x0]
                current_y = [y0f]
            current_x.append(x_cross)
            current_y.append(0.0)
            segments.append((current_x, current_y))
            current_x, current_y = [], []

    if current_x:
        segments.append((current_x, current_y))

    return segments


def _build_equity_streak_labels(x: pd.Series, y: pd.Series) -> tuple[list, list, list]:
    label_x: list = []
    label_y: list = []
    label_text: list[str] = []

    y_num = pd.to_numeric(y, errors="coerce")
    if y_num.empty:
        return label_x, label_y, label_text

    # Red zone is <= 0, green zone is > 0 to match line coloring.
    is_green = y_num > 0

    start = 0
    for i in range(1, len(y_num) + 1):
        changed = i == len(y_num) or bool(is_green.iloc[i]) != bool(is_green.iloc[start])
        if not changed:
            continue

        segment = y_num.iloc[start:i]
        segment_count = int(len(segment))
        if segment_count <= 0:
            start = i
            continue

        mid = start + (segment_count - 1) // 2
        y_mid = float(y_num.iloc[mid]) if pd.notna(y_num.iloc[mid]) else 0.0
        in_green = bool(is_green.iloc[start])
        y_text = y_mid * 0.5

        if in_green and y_text <= 0:
            y_text = max(1.0, abs(y_mid) * 0.5)
        if (not in_green) and y_text >= 0:
            y_text = -max(1.0, abs(y_mid) * 0.5)

        label_x.append(pd.Timestamp(x.iloc[mid]))
        label_y.append(y_text)
        label_text.append(f"{segment_count} trades")

        start = i

    return label_x, label_y, label_text


def _densify_bars_for_chart(
    bars: pd.DataFrame,
    timeframe_min: int,
    session_start: str | None = None,
    session_end: str | None = None,
) -> pd.DataFrame:
    if bars.empty or timeframe_min <= 0:
        return bars

    out = bars.sort_index().copy()

    full_index: pd.DatetimeIndex
    same_day = out.index.date.min() == out.index.date.max()
    if same_day and session_start and session_end:
        day = pd.Timestamp(out.index.min()).date()
        start_time = datetime.strptime(session_start, "%H:%M:%S").time()
        end_time = datetime.strptime(session_end, "%H:%M:%S").time()
        session_open = pd.Timestamp.combine(day, start_time)
        session_close = pd.Timestamp.combine(day, end_time)
        first_bar_label = session_open + pd.Timedelta(minutes=int(timeframe_min))
        full_index = pd.date_range(
            start=first_bar_label,
            end=session_close,
            freq=f"{int(timeframe_min)}min",
        )
    else:
        full_index = pd.date_range(
            start=pd.Timestamp(out.index.min()),
            end=pd.Timestamp(out.index.max()),
            freq=f"{int(timeframe_min)}min",
        )

    out = out.reindex(full_index)

    synthetic_mask = out["open"].isna() if "open" in out.columns else out.isna().all(axis=1)

    if "close" in out.columns:
        out["close"] = pd.to_numeric(out["close"], errors="coerce").ffill().bfill()

    for col in ("open", "high", "low"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    if {"open", "close"}.issubset(out.columns):
        out["open"] = out["open"].fillna(out["close"])
    if {"high", "open", "close"}.issubset(out.columns):
        oc_max = pd.concat([out["open"], out["close"]], axis=1).max(axis=1)
        out["high"] = out["high"].fillna(oc_max)
    if {"low", "open", "close"}.issubset(out.columns):
        oc_min = pd.concat([out["open"], out["close"]], axis=1).min(axis=1)
        out["low"] = out["low"].fillna(oc_min)

    for col in ("volume", "bid_volume", "ask_volume", "tick_count", "max_big_buy", "max_big_sell"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)

    for col in ("has_big_buy", "has_big_sell", "signal_long", "signal_short"):
        if col in out.columns:
            out[col] = out[col].astype("boolean").fillna(False).astype(bool)

    if bool(synthetic_mask.any()) and "close" in out.columns:
        close_num = pd.to_numeric(out["close"], errors="coerce")
        diffs = close_num.diff().abs()
        diffs = diffs[diffs > 0]
        if not diffs.empty:
            eps = float(diffs.min())
        else:
            ref = float(close_num.dropna().iloc[0]) if close_num.notna().any() else 1.0
            eps = max(0.01, abs(ref) * 0.00001)

        eps = max(0.01, min(eps, 1.0))
        if "high" in out.columns:
            out.loc[synthetic_mask, "high"] = close_num.loc[synthetic_mask] + (eps * 0.5)
        if "low" in out.columns:
            out.loc[synthetic_mask, "low"] = close_num.loc[synthetic_mask] - (eps * 0.5)

    for col in ("or_high", "or_low", "or_mid", "ib_end", "session_date"):
        if col in out.columns:
            out[col] = out[col].ffill().bfill()

    return out


def _prepare_trade_chart_bars(
    bars: pd.DataFrame,
    selected_trade: pd.DataFrame,
    timeframe_min: int,
    session_start: str,
    session_end: str,
) -> tuple[pd.DataFrame, int, int]:
    trade_bars = bars
    if not selected_trade.empty:
        session_date = selected_trade.iloc[0].get("session_date")
        if pd.notna(session_date):
            try:
                day = pd.to_datetime(session_date).date()
                scoped = bars[bars.index.date == day]
                if not scoped.empty:
                    trade_bars = scoped
            except Exception:
                pass

    chart_bars = _densify_bars_for_chart(
        trade_bars,
        int(timeframe_min),
        session_start=session_start,
        session_end=session_end,
    )
    raw_count = len(trade_bars)
    displayed_count = len(chart_bars)
    synthetic_count = max(displayed_count - raw_count, 0)
    return chart_bars, raw_count, synthetic_count


def make_candle_chart(
    bars: pd.DataFrame, trades: pd.DataFrame, timeframe_min: int, show_big_orders: bool = True
) -> go.Figure:
    fig = go.Figure()
    big_orders_trace_indexes: list[int] = []
    fig.add_trace(
        go.Candlestick(
            x=bars.index,
            open=bars["open"],
            high=bars["high"],
            low=bars["low"],
            close=bars["close"],
            name="Price",
        )
    )

    if "or_high" in bars.columns:
        fig.add_trace(go.Scatter(x=bars.index, y=bars["or_high"], mode="lines", name="OR High"))
    if "or_low" in bars.columns:
        fig.add_trace(go.Scatter(x=bars.index, y=bars["or_low"], mode="lines", name="OR Low"))
    if "or_mid" in bars.columns:
        fig.add_trace(
            go.Scatter(
                x=bars.index,
                y=bars["or_mid"],
                mode="lines",
                name="OR Mid",
                line=dict(color="yellow", dash="dash"),
            )
        )

    if show_big_orders and "has_big_buy" in bars.columns:
        bb = bars[bars["has_big_buy"]].copy()
        buy_size_col = "max_big_buy" if "max_big_buy" in bb.columns else "ask_volume"
        if not bb.empty and buy_size_col in bb.columns:
            buy_sizes_raw = pd.to_numeric(bb[buy_size_col], errors="coerce").fillna(0.0)
            buy_sizes = _scaled_bubble_sizes(buy_sizes_raw)
            fig.add_trace(
                go.Scatter(
                    x=bb.index,
                    y=bb["close"],
                    mode="markers+text",
                    marker=dict(
                        size=buy_sizes,
                        color="rgba(34, 197, 94, 0.65)",
                        symbol="circle",
                    ),
                    text=[str(int(v)) for v in buy_sizes_raw],
                    textposition="middle center",
                    textfont=dict(color="white", size=12, family="Arial Black, Arial"),
                    name="Big Buy",
                )
            )
            big_orders_trace_indexes.append(len(fig.data) - 1)

    if show_big_orders and "has_big_sell" in bars.columns:
        bs = bars[bars["has_big_sell"]].copy()
        sell_size_col = "max_big_sell" if "max_big_sell" in bs.columns else "bid_volume"
        if not bs.empty and sell_size_col in bs.columns:
            sell_sizes_raw = pd.to_numeric(bs[sell_size_col], errors="coerce").fillna(0.0)
            sell_sizes = _scaled_bubble_sizes(sell_sizes_raw)
            fig.add_trace(
                go.Scatter(
                    x=bs.index,
                    y=bs["close"],
                    mode="markers+text",
                    marker=dict(
                        size=sell_sizes,
                        color="rgba(239, 68, 68, 0.65)",
                        symbol="circle",
                    ),
                    text=[str(int(v)) for v in sell_sizes_raw],
                    textposition="middle center",
                    textfont=dict(color="white", size=12, family="Arial Black, Arial"),
                    name="Big Sell",
                )
            )
            big_orders_trace_indexes.append(len(fig.data) - 1)

    if not trades.empty:
        bar_spacing = pd.Timedelta(minutes=int(timeframe_min))
        if len(bars.index) > 1:
            observed_deltas = bars.index.to_series().sort_values().diff().dropna()
            if not observed_deltas.empty:
                bar_spacing = observed_deltas.median()
        marker_time_offset = max(pd.Timedelta(seconds=20), bar_spacing * 0.4)
        # Connect each trade from entry to exit for fast visual inspection of trade path.
        for _, trade in trades.iterrows():
            line_color = (
                "rgba(34, 197, 94, 0.8)"
                if float(trade.get("pnl", 0.0)) >= 0
                else "rgba(239, 68, 68, 0.8)"
            )
            fig.add_trace(
                go.Scatter(
                    x=[trade["entry_time"], trade["exit_time"]],
                    y=[trade["entry_price"], trade["exit_price"]],
                    mode="lines",
                    line=dict(color=line_color, width=2),
                    name="Trade Path",
                    showlegend=False,
                )
            )

        long_trades = (
            trades[trades["side"] == "long"] if "side" in trades.columns else pd.DataFrame()
        )
        short_trades = (
            trades[trades["side"] == "short"] if "side" in trades.columns else pd.DataFrame()
        )

        if not long_trades.empty:
            long_entry_x = pd.to_datetime(long_trades["entry_time"], errors="coerce") - marker_time_offset
            long_exit_x = pd.to_datetime(long_trades["exit_time"], errors="coerce") + marker_time_offset
            fig.add_trace(
                go.Scatter(
                    x=long_entry_x,
                    y=long_trades["entry_price"],
                    mode="markers",
                    marker=dict(size=12, symbol="triangle-right", color="rgba(34, 197, 94, 1.0)"),
                    name="Long Entry",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=long_exit_x,
                    y=long_trades["exit_price"],
                    mode="markers",
                    marker=dict(size=12, symbol="triangle-left", color="rgba(239, 68, 68, 1.0)"),
                    name="Long Exit",
                )
            )

        if not short_trades.empty:
            short_entry_x = pd.to_datetime(short_trades["entry_time"], errors="coerce") - marker_time_offset
            short_exit_x = pd.to_datetime(short_trades["exit_time"], errors="coerce") + marker_time_offset
            fig.add_trace(
                go.Scatter(
                    x=short_entry_x,
                    y=short_trades["entry_price"],
                    mode="markers",
                    marker=dict(size=12, symbol="triangle-right", color="rgba(239, 68, 68, 1.0)"),
                    name="Short Entry",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=short_exit_x,
                    y=short_trades["exit_price"],
                    mode="markers",
                    marker=dict(size=12, symbol="triangle-left", color="rgba(34, 197, 94, 1.0)"),
                    name="Short Exit",
                )
            )

    fig.update_layout(
        height=650,
        xaxis_rangeslider_visible=False,
        paper_bgcolor="#F1F3F5",
        plot_bgcolor="#F1F3F5",
        font=dict(color="#111827"),
    )
    if big_orders_trace_indexes:
        vis_show = [True] * len(fig.data)
        vis_hide = [True] * len(fig.data)
        for idx in big_orders_trace_indexes:
            vis_hide[idx] = False
        fig.update_layout(
            updatemenus=[
                dict(
                    type="buttons",
                    direction="left",
                    x=1.0,
                    y=1.15,
                    xanchor="right",
                    yanchor="top",
                    showactive=True,
                    buttons=[
                        dict(
                            label="Big Orders: On",
                            method="update",
                            args=[{"visible": vis_show}],
                        ),
                        dict(
                            label="Big Orders: Off",
                            method="update",
                            args=[{"visible": vis_hide}],
                        ),
                    ],
                )
            ]
        )
    fig.update_yaxes(tickformat=",.2f", separatethousands=True, dtick=100, showgrid=True)
    return fig


def _render_heatmap(metrics: pd.DataFrame) -> None:
    st.subheader("Opening Range x Timeframe Heatmap")
    if metrics.empty:
        st.info("No metrics available.")
        return

    if len(metrics) == 1:
        one = metrics[["timeframe_min", "ib_minutes", "total_pnl"]].copy()
        st.dataframe(_humanize_columns(one), use_container_width=True)
        return

    heat = metrics.pivot(index="timeframe_min", columns="ib_minutes", values="total_pnl")
    heat.index.name = "Timeframe (Min)"
    heat.columns.name = "Opening Range"
    st.dataframe(heat, use_container_width=True)


def _inject_centered_dataframe_css() -> None:
    st.markdown(
        """
        <style>
        [data-testid=\"stDataFrame\"] [role=\"columnheader\"] {
            text-align: center !important;
        }
        [data-testid=\"stDataFrame\"] [role=\"columnheader\"] > div {
            justify-content: center !important;
            width: 100% !important;
        }
        [data-testid=\"stDataFrame\"] [role=\"gridcell\"] {
            text-align: center !important;
        }
        [data-testid=\"stDataFrame\"] [role=\"gridcell\"] > div {
            text-align: center !important;
            width: 100% !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_centered_metrics_table(df: pd.DataFrame) -> None:
    table_html = df.to_html(index=False, classes="metrics-table", border=0, escape=True)
    style_html = (
        "<style>"
        "table.metrics-table {width: 100%; border-collapse: collapse;}"
        "table.metrics-table th, table.metrics-table td {text-align: center; padding: 0.4rem 0.5rem;}"
        "table.metrics-table thead tr {border-bottom: 1px solid rgba(49, 51, 63, 0.2);}"
        "</style>"
    )
    st.markdown(style_html + table_html, unsafe_allow_html=True)



def _render_trades_table(display_trades: pd.DataFrame) -> None:
    if display_trades.empty:
        st.caption("No trades to display.")
        return

    if AgGrid is None or GridOptionsBuilder is None or JsCode is None:
        st.warning("AG Grid is unavailable. Install `streamlit-aggrid` for header filters + pagination.")
        fallback_df = display_trades.drop(columns=["trade_link"], errors="ignore")
        st.dataframe(fallback_df, use_container_width=True, hide_index=True)
        return

    date_link_renderer = JsCode(
        """
        function(params) {
            const url = (params.data && params.data.trade_link) ? params.data.trade_link : "";
            const label = (params.value === undefined || params.value === null) ? "" : String(params.value);
            if (!url) return label;
            return `<a href="${url}" target="_self" style="font-weight:600;">${label}</a>`;
        }
        """
    )
    gb = GridOptionsBuilder.from_dataframe(display_trades)
    gb.configure_default_column(
        sortable=True,
        filter=True,
        floatingFilter=False,
        resizable=True,
    )
    gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=TRADES_PAGE_SIZE)

    if "trade_link" in display_trades.columns:
        gb.configure_column("trade_link", hide=True, filter=False, sortable=False)
    if "Session Date" in display_trades.columns:
        gb.configure_column("Session Date", cellRenderer=date_link_renderer, width=120)

    gb.configure_grid_options(
        domLayout="normal",
        rowHeight=34,
        headerHeight=34,
        animateRows=False,
        suppressMenuHide=True,
    )
    grid_options = gb.build()

    grid_kwargs = {
        "gridOptions": grid_options,
        "allow_unsafe_jscode": True,
        "theme": "streamlit",
        "fit_columns_on_grid_load": True,
        "height": 420,
    }
    if GridUpdateMode is not None:
        grid_kwargs["update_mode"] = GridUpdateMode.NO_UPDATE

    AgGrid(display_trades, **grid_kwargs)



def _render_trades_table_classic(
    combo_trades: pd.DataFrame,
    selected_combo: str,
    ib_minutes: int,
    timeframe_min: int,
    tp_r_multiple: float,
    big_trade_threshold: int,
    stop_mode: str,
    entry_mode: str,
    strategy_mode: str,
) -> None:
    total_trades = len(combo_trades)
    total_pages = max(1, (total_trades + TRADES_PAGE_SIZE - 1) // TRADES_PAGE_SIZE)

    if st.session_state.get("trades_page_combo") != selected_combo:
        st.session_state.trades_page_combo = selected_combo
        st.session_state.trades_page_input = 1

    if "trades_page_input" not in st.session_state:
        st.session_state.trades_page_input = 1

    st.session_state.trades_page_input = min(
        max(int(st.session_state.trades_page_input), 1), total_pages
    )
    current_page = int(st.session_state.trades_page_input)
    page_options = list(range(1, total_pages + 1))

    instruction_col, controls_col = st.columns([7, 5], vertical_alignment="center")
    with instruction_col:
        st.markdown("Click on a trade (date column) to see it on a chart.")

    with controls_col:
        spacer_col, prev_col, page_col, next_col = st.columns(
            [2.2, 1, 1.4, 1], vertical_alignment="center"
        )
        with spacer_col:
            st.empty()
        with prev_col:
            prev_clicked = st.button(
                "Prev", disabled=current_page <= 1, key="trades_prev_btn", use_container_width=True
            )
        with page_col:
            selected_page = st.selectbox(
                "Trades Page",
                options=page_options,
                index=current_page - 1,
                format_func=lambda p: f"Page {p} of {total_pages}",
                label_visibility="collapsed",
            )
        with next_col:
            next_clicked = st.button(
                "Next",
                disabled=current_page >= total_pages,
                key="trades_next_btn",
                use_container_width=True,
            )

    next_page: int | None = None
    if prev_clicked:
        next_page = current_page - 1
    elif next_clicked:
        next_page = current_page + 1
    elif int(selected_page) != current_page:
        next_page = int(selected_page)

    if next_page is not None:
        clamped_page = min(max(next_page, 1), total_pages)
        st.session_state.trades_page_input = clamped_page
        _clear_trade_selection_query_params()
        st.rerun()

    start_row = (current_page - 1) * TRADES_PAGE_SIZE
    end_row = min(start_row + TRADES_PAGE_SIZE, total_trades)

    if total_trades == 0:
        st.caption("No trades to display.")

    paged_trades = combo_trades.iloc[start_row:end_row].copy()

    display_trades = _drop_display_columns(paged_trades, DISPLAY_FILTER_COLUMNS_TRADES).copy()
    if "trade_id" in display_trades.columns:
        display_trades = display_trades.drop(columns=["trade_id"])

    if "session_date" in display_trades.columns and "trade_id" in paged_trades.columns:
        display_trades["session_date"] = [
            _build_trade_link(
                trade_id=int(trade_id),
                combo=selected_combo,
                session_date=str(session_date),
                ib_minutes=int(ib_minutes),
                timeframe_min=int(timeframe_min),
                tp_r_multiple=float(tp_r_multiple),
                big_trade_threshold=int(big_trade_threshold),
                stop_mode=str(stop_mode),
                entry_mode=str(entry_mode),
                strategy_mode=str(strategy_mode),
            )
            for trade_id, session_date in zip(
                paged_trades["trade_id"], paged_trades["session_date"], strict=False
            )
        ]

    display_trades = _humanize_columns(display_trades)
    trade_column_config: dict[str, object] = {}
    if "Session Date" in display_trades.columns:
        trade_column_config["Session Date"] = st.column_config.LinkColumn(
            "Session Date",
            display_text=r".*date=([0-9\-]+).*",
            width="small",
        )

    st.dataframe(
        display_trades,
        use_container_width=True,
        hide_index=True,
        column_config=trade_column_config,
    )

def _resolved_run_dir(run_dir: str) -> str:
    path = Path(run_dir)
    try:
        return str(path.resolve(strict=True))
    except FileNotFoundError:
        return str(path)


def _sync_active_run_dir(requested_run_dir: str) -> None:
    resolved = _resolved_run_dir(requested_run_dir)
    if st.session_state.get("active_run_dir") != resolved:
        st.session_state.active_run_dir = resolved


def _defaults_with_query_overrides(
    defaults: dict[str, int | float | str],
) -> dict[str, int | float | str]:
    out = dict(defaults)

    ib_raw = _qp_get("ib_minutes")
    if ib_raw in {"15", "30", "60"}:
        out["ib_minutes"] = int(ib_raw)

    tf_raw = _qp_get("timeframe_min")
    if tf_raw in {"1", "3", "5"}:
        out["timeframe_min"] = int(tf_raw)

    tp_raw = _qp_get("tp_r_multiple")
    if tp_raw is not None:
        try:
            tp = float(tp_raw)
            if tp > 0:
                out["tp_r_multiple"] = tp
        except ValueError:
            pass

    big_raw = _qp_get("big_trade_threshold")
    if big_raw is not None:
        try:
            big = int(big_raw)
            if big > 0:
                out["big_trade_threshold"] = big
        except ValueError:
            pass

    stop_raw = _qp_get("stop_mode")
    if stop_raw in {"or_boundary", "or_mid"}:
        out["stop_mode"] = stop_raw

    entry_mode_raw = _qp_get("entry_mode")
    if entry_mode_raw in {"first_outside", "reentry_after_stop"}:
        out["entry_mode"] = entry_mode_raw

    strategy_mode_raw = _qp_get("strategy_mode")
    if strategy_mode_raw in {"big_order_required", "breakout_only"}:
        out["strategy_mode"] = strategy_mode_raw

    return out


def _init_control_state(defaults: dict[str, int | float | str], run_dir: str) -> None:
    if "active_run_dir" not in st.session_state:
        st.session_state.active_run_dir = _resolved_run_dir(run_dir)
    if "ib_minutes_input" not in st.session_state:
        st.session_state.ib_minutes_input = int(defaults["ib_minutes"])
    if "timeframe_min_input" not in st.session_state:
        st.session_state.timeframe_min_input = int(defaults["timeframe_min"])
    if "tp_r_multiple_input" not in st.session_state:
        st.session_state.tp_r_multiple_input = float(defaults["tp_r_multiple"])
    if "big_trade_threshold_input" not in st.session_state:
        st.session_state.big_trade_threshold_input = int(defaults["big_trade_threshold"])
    if "stop_mode_input" not in st.session_state:
        st.session_state.stop_mode_input = str(defaults["stop_mode"])
    if "entry_mode_input" not in st.session_state:
        st.session_state.entry_mode_input = str(defaults["entry_mode"])
    if "strategy_mode_input" not in st.session_state:
        st.session_state.strategy_mode_input = str(defaults["strategy_mode"])

def main() -> None:
    args = parse_args()
    defaults = _defaults_with_query_overrides(default_ui_params())

    st.set_page_config(layout="wide", page_title="ORB Backtest")
    st.title("ORB Backtester Results")
    _inject_centered_dataframe_css()

    _init_control_state(defaults=defaults, run_dir=args.run_dir)
    _sync_active_run_dir(args.run_dir)

    active_run_path = Path(st.session_state.active_run_dir)
    if not active_run_path.exists():
        st.error(f"Run directory not found: {active_run_path}")
        return

    try:
        source_cfg_for_controls = _load_source_config(active_run_path)
        session_start_for_controls = str(source_cfg_for_controls.get("session_start", "09:30:00"))
        session_end_for_controls = str(source_cfg_for_controls.get("session_end", "16:00:00"))
        data_path_raw = source_cfg_for_controls.get("data_path")
        if not isinstance(data_path_raw, str) or not data_path_raw.strip():
            raise ValueError("Invalid data_path in config_used.json")
        data_path_for_controls = _resolve_data_path(data_path_raw, project_root=PROJECT_ROOT)
        dataset_min_date, dataset_max_date = load_data_date_bounds(str(data_path_for_controls))
    except Exception as exc:
        st.error(f"Unable to load dataset date range: {exc}")
        return

    bounds_key = (
        f"{str(data_path_for_controls.resolve())}|"
        f"{dataset_min_date.isoformat()}|{dataset_max_date.isoformat()}"
    )
    if st.session_state.get("backtest_date_bounds_key") != bounds_key:
        st.session_state.backtest_date_bounds_key = bounds_key
        st.session_state.backtest_start_date_input = dataset_min_date
        st.session_state.backtest_end_date_input = dataset_max_date

    st.session_state.backtest_start_date_input = max(
        dataset_min_date,
        min(st.session_state.backtest_start_date_input, dataset_max_date),
    )
    st.session_state.backtest_end_date_input = max(
        dataset_min_date,
        min(st.session_state.backtest_end_date_input, dataset_max_date),
    )

    l1, l2, l3, l4, l5, l6, l7, l8, l9, l10 = st.columns(
        [0.95, 0.9, 0.95, 1.0, 1.25, 1.0, 1.15, 1.2, 1.2, 1.0]
    )
    with l1:
        st.caption("Opening Range")
    with l2:
        st.caption("Timeframe")
    with l3:
        st.caption("TP-R multiple")
    with l4:
        st.caption("Large order size")
    with l5:
        st.caption("Strategy")
    with l6:
        st.caption("Stop mode")
    with l7:
        st.caption("Entry mode")
    with l8:
        st.caption("Start date")
    with l9:
        st.caption("End date")
    with l10:
        st.caption(" ")

    c1, c2, c3, c4, c5, c6, c7, c8, c9, c10 = st.columns(
        [0.95, 0.9, 0.95, 1.0, 1.25, 1.0, 1.15, 1.2, 1.2, 1.0]
    )
    with c1:
        ib_minutes = st.selectbox(
            "Opening Range",
            options=[15, 30, 60],
            key="ib_minutes_input",
            label_visibility="collapsed",
        )
    with c2:
        timeframe_min = st.selectbox(
            "Timeframe",
            options=[1, 3, 5],
            key="timeframe_min_input",
            label_visibility="collapsed",
        )
    with c3:
        tp_r_multiple = st.number_input(
            "TP-R multiple",
            min_value=0.1,
            step=0.1,
            key="tp_r_multiple_input",
            label_visibility="collapsed",
        )
    with c4:
        big_trade_threshold = st.number_input(
            "Large order size",
            min_value=1,
            step=1,
            key="big_trade_threshold_input",
            label_visibility="collapsed",
        )
    with c5:
        strategy_mode = st.selectbox(
            "Strategy",
            options=["big_order_required", "breakout_only"],
            format_func=lambda v: "Big Order Required" if v == "big_order_required" else "Breakout Only",
            key="strategy_mode_input",
            label_visibility="collapsed",
        )
    with c6:
        stop_mode = st.selectbox(
            "Stop mode",
            options=["or_boundary", "or_mid"],
            key="stop_mode_input",
            label_visibility="collapsed",
        )
    with c7:
        entry_mode = st.selectbox(
            "Entry mode",
            options=["first_outside", "reentry_after_stop"],
            key="entry_mode_input",
            label_visibility="collapsed",
        )
    with c8:
        start_date_input = st.date_input(
            "Start date",
            min_value=dataset_min_date,
            max_value=dataset_max_date,
            key="backtest_start_date_input",
            format="YYYY-MM-DD",
            label_visibility="collapsed",
        )
    with c9:
        end_date_input = st.date_input(
            "End date",
            min_value=dataset_min_date,
            max_value=dataset_max_date,
            key="backtest_end_date_input",
            format="YYYY-MM-DD",
            label_visibility="collapsed",
        )
    with c10:
        run_clicked = st.button("Run backtest", use_container_width=True)

    if start_date_input > end_date_input:
        st.error("Start date must be on or before end date.")

    if run_clicked and start_date_input <= end_date_input:
        try:
            with st.spinner("Running backtest..."):
                new_run_dir, new_combo = recompute_and_persist(
                    source_run_dir=st.session_state.active_run_dir,
                    ib_minutes=int(ib_minutes),
                    timeframe_min=int(timeframe_min),
                    tp_r_multiple=float(tp_r_multiple),
                    big_trade_threshold=int(big_trade_threshold),
                    stop_mode=stop_mode,
                    entry_mode=entry_mode,
                    strategy_mode=strategy_mode,
                    start_date=start_date_input,
                    end_date=end_date_input,
                    project_root=PROJECT_ROOT,
                )
            st.cache_data.clear()
            st.session_state.active_run_dir = str(new_run_dir)
            st.session_state.selected_combo = new_combo
            st.success(f"Run complete: {new_run_dir}")
            st.rerun()
        except Exception as exc:
            st.error(str(exc))

    active_run_path = Path(st.session_state.active_run_dir)
    if not active_run_path.exists():
        st.error(f"Run directory not found: {active_run_path}")
        return
    st.caption(f"Viewing run: {active_run_path}")

    try:
        metrics, trades = load_run_artifacts(str(active_run_path))
    except FileNotFoundError as exc:
        st.error(str(exc))
        return

    metrics = _ensure_full_tp_wins(metrics, trades)
    metrics = _ensure_full_losses(metrics, trades)

    filtered_metrics = metrics
    if "ib_minutes" in metrics.columns:
        filtered_metrics = metrics[metrics["ib_minutes"] == int(ib_minutes)].copy()

    st.subheader("Metrics")
    if filtered_metrics.empty:
        st.info("No metrics for selected Opening Range; showing all metrics.")
        display_metrics = _humanize_columns(
            _reorder_metrics_columns(_drop_display_columns(metrics, DISPLAY_FILTER_COLUMNS_METRICS))
        )
        _render_centered_metrics_table(display_metrics)
    else:
        display_metrics = _humanize_columns(
            _reorder_metrics_columns(
                _drop_display_columns(filtered_metrics, DISPLAY_FILTER_COLUMNS_METRICS)
            )
        )
        _render_centered_metrics_table(display_metrics)

    combos = metrics["combo"].tolist() if "combo" in metrics.columns else []
    preferred_combo = build_combo_id(timeframe_min=int(timeframe_min), ib_minutes=int(ib_minutes))
    selected_combo = preferred_combo if preferred_combo in combos else (combos[0] if combos else "")

    if not selected_combo:
        st.warning("No combo artifacts found for this run.")
        return

    try:
        bars = load_bars(str(active_run_path), selected_combo)
    except FileNotFoundError:
        st.warning(f"Bars artifact missing for combo: {selected_combo}")
        return

    combo_trades = trades[trades["combo"] == selected_combo].copy() if not trades.empty else trades
    combo_trades = combo_trades.reset_index(drop=True)
    combo_trades["trade_id"] = combo_trades.index
    if "pnl" in combo_trades.columns:
        pnl_num = pd.to_numeric(combo_trades["pnl"], errors="coerce").fillna(0.0)
        combo_trades["trade_result"] = pnl_num.apply(
            lambda v: "Win" if v > 0 else ("Loss" if v < 0 else "Flat")
        )
    st.subheader("Trades")


    filter_cols = st.columns([1, 1, 1, 0.8], vertical_alignment="bottom")

    result_options = ["All"]
    if "trade_result" in combo_trades.columns:
        result_options.extend(
            [v for v in ["Win", "Loss", "Flat"] if v in set(combo_trades["trade_result"])]
        )

    side_options = ["All"]
    if "side" in combo_trades.columns:
        side_norm = combo_trades["side"].astype(str).str.lower()
        if side_norm.isin(["long", "buy"]).any():
            side_options.append("Buy")
        if side_norm.isin(["short", "sell"]).any():
            side_options.append("Sell")

    exit_reason_options = ["All"]
    if "exit_reason" in combo_trades.columns:
        exit_reason_options.extend(sorted(combo_trades["exit_reason"].dropna().astype(str).unique()))

    if st.session_state.get("trades_external_result_filter") not in result_options:
        st.session_state.trades_external_result_filter = "All"
    if st.session_state.get("trades_external_side_filter") not in side_options:
        st.session_state.trades_external_side_filter = "All"
    if st.session_state.get("trades_external_exit_reason_filter") not in exit_reason_options:
        st.session_state.trades_external_exit_reason_filter = "All"

    with filter_cols[0]:
        result_filter = st.selectbox("Result", result_options, key="trades_external_result_filter")
    with filter_cols[1]:
        side_filter = st.selectbox("Side", side_options, key="trades_external_side_filter")
    with filter_cols[2]:
        exit_reason_filter = st.selectbox(
            "Exit Reason", exit_reason_options, key="trades_external_exit_reason_filter"
        )
    with filter_cols[3]:
        st.button(
            "Clear Filters",
            key="trades_clear_filters_btn",
            use_container_width=True,
            on_click=_clear_external_trades_filters,
        )

    filtered_for_table = combo_trades.copy()
    if result_filter != "All" and "trade_result" in filtered_for_table.columns:
        filtered_for_table = filtered_for_table[filtered_for_table["trade_result"] == result_filter]
    if side_filter != "All" and "side" in filtered_for_table.columns:
        side_norm = filtered_for_table["side"].astype(str).str.lower()
        if side_filter == "Buy":
            filtered_for_table = filtered_for_table[side_norm.isin(["long", "buy"])]
        elif side_filter == "Sell":
            filtered_for_table = filtered_for_table[side_norm.isin(["short", "sell"])]
    if exit_reason_filter != "All" and "exit_reason" in filtered_for_table.columns:
        filtered_for_table = filtered_for_table[
            filtered_for_table["exit_reason"] == exit_reason_filter
        ]

    st.caption(f"Total trades after filters: {len(filtered_for_table)}")

    _render_trades_table_classic(
        combo_trades=filtered_for_table,
        selected_combo=selected_combo,
        ib_minutes=int(ib_minutes),
        timeframe_min=int(timeframe_min),
        tp_r_multiple=float(tp_r_multiple),
        big_trade_threshold=int(big_trade_threshold),
        stop_mode=str(stop_mode),
        entry_mode=str(entry_mode),
        strategy_mode=str(strategy_mode),
    )

    selected_trade = pd.DataFrame()
    selected_trade_combo = _qp_get("trade_combo")
    selected_trade_id_raw = _qp_get("trade_id")
    if selected_trade_combo == selected_combo and selected_trade_id_raw is not None:
        try:
            selected_trade_id = int(selected_trade_id_raw)
        except ValueError:
            selected_trade_id = -1

        if 0 <= selected_trade_id < len(combo_trades):
            selected_trade = combo_trades.iloc[[selected_trade_id]].copy()

    if not selected_trade.empty:
        st.markdown('<div id="price-signals-anchor"></div>', unsafe_allow_html=True)
        st.subheader("Chart")
        chart_bars, raw_count, synthetic_count = _prepare_trade_chart_bars(
            bars=bars,
            selected_trade=selected_trade,
            timeframe_min=int(timeframe_min),
            session_start=session_start_for_controls,
            session_end=session_end_for_controls,
        )
        st.caption(
            f"Chart bars: raw={raw_count} displayed={len(chart_bars)} synthetic={synthetic_count}"
        )
        st.plotly_chart(
            make_candle_chart(
                chart_bars,
                selected_trade,
                int(timeframe_min),
            ),
            use_container_width=True,
        )
        components.html(
            """
            <script>
            const anchor = window.parent.document.getElementById("price-signals-anchor");
            if (anchor) {
                anchor.scrollIntoView({behavior: "smooth", block: "start"});
            }
            </script>
            """,
            height=0,
            width=0,
        )

    st.subheader("Equity Curve")
    if combo_trades.empty:
        st.info("No trades for selected combo.")
    else:
        eq = combo_trades.sort_values("exit_time").copy()
        eq["equity"] = eq["pnl"].cumsum()

        x = eq["exit_time"]
        y = pd.to_numeric(eq["equity"], errors="coerce")

        fig_eq = go.Figure()
        fig_eq.add_hline(y=0.0, line=dict(color="rgba(148, 163, 184, 0.8)", dash="dot", width=1))

        for seg_x, seg_y in _build_positive_fill_segments(x, y):
            fig_eq.add_trace(
                go.Scatter(
                    x=seg_x,
                    y=seg_y,
                    mode="lines",
                    line=dict(width=0, color="rgba(34, 197, 94, 0.0)"),
                    fill="tozeroy",
                    fillcolor="rgba(34, 197, 94, 0.18)",
                    hoverinfo="skip",
                    showlegend=False,
                    name="Positive Area",
                )
            )

        for seg_x, seg_y in _build_negative_fill_segments(x, y):
            fig_eq.add_trace(
                go.Scatter(
                    x=seg_x,
                    y=seg_y,
                    mode="lines",
                    line=dict(width=0, color="rgba(239, 68, 68, 0.0)"),
                    fill="tozeroy",
                    fillcolor="rgba(239, 68, 68, 0.22)",
                    hoverinfo="skip",
                    showlegend=False,
                    name="Negative Area",
                )
            )

        red_x, red_y, green_x, green_y = _build_signed_line_segments(x, y)
        if red_x:
            fig_eq.add_trace(
                go.Scatter(
                    x=red_x,
                    y=red_y,
                    mode="lines",
                    line=dict(color="rgba(239, 68, 68, 1.0)", width=2),
                    name="Equity (<= 0)",
                )
            )
        if green_x:
            fig_eq.add_trace(
                go.Scatter(
                    x=green_x,
                    y=green_y,
                    mode="lines",
                    line=dict(color="rgba(34, 197, 94, 1.0)", width=2),
                    name="Equity (> 0)",
                )
            )

        if not red_x and not green_x and len(y) > 0:
            point_color = (
                "rgba(34, 197, 94, 1.0)" if float(y.iloc[-1]) > 0 else "rgba(239, 68, 68, 1.0)"
            )
            fig_eq.add_trace(
                go.Scatter(
                    x=x,
                    y=y,
                    mode="lines+markers",
                    line=dict(color=point_color, width=2),
                    marker=dict(size=5, color=point_color),
                    name="Equity",
                )
            )
        label_x, label_y, label_text = _build_equity_streak_labels(x, y)
        theme_base = str(st.get_option("theme.base") or "light").lower()
        streak_text_color = "white" if theme_base == "dark" else "rgba(107, 114, 128, 1.0)"
        if label_x:
            fig_eq.add_trace(
                go.Scatter(
                    x=label_x,
                    y=label_y,
                    mode="text",
                    text=label_text,
                    textfont=dict(color=streak_text_color, size=12, family="Arial Black, Arial"),
                    hoverinfo="skip",
                    showlegend=False,
                    name="Streak Labels",
                )
            )

        fig_eq.update_layout(height=450, showlegend=False)
        fig_eq.update_yaxes(dtick=100, showgrid=True)
        st.plotly_chart(fig_eq, use_container_width=True)

    _render_heatmap(metrics)


if __name__ == "__main__":
    main()
