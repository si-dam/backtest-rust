from __future__ import annotations

import gc
import json
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

import pandas as pd
import typer

from orbt.backtest import build_vectorbt_portfolio, simulate_orb_big_trade_strategy
from orbt.config import StrategyConfig, load_config
from orbt.ingest_sierra import ingest_to_parquet, read_sierra_export
from orbt.metrics import summarize_trades
from orbt.resample import build_layered_bars, load_sierra_ohlc_1m

app = typer.Typer(help="ORB + big-trade backtesting CLI")


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


def _run_dir(base_output: Path) -> Path:
    if base_output.suffix:
        base_output = base_output.parent
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = base_output / f"run_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


@app.command()
def ingest(input: Path = typer.Option(..., exists=True), output: Path = typer.Option(...)) -> None:
    """Ingest Sierra text export and write normalized parquet."""
    out = ingest_to_parquet(input_path=input, output_path=output)
    typer.echo(f"Wrote parquet: {out}")


@app.command()
def run(config: Path = typer.Option(..., exists=True), output: Path = typer.Option(Path("runs"))) -> None:
    """Run full parameter sweep backtest from config."""
    cfg: StrategyConfig = load_config(config)
    ticks = _load_ticks(Path(cfg.data_path))

    ohlc_1m: pd.DataFrame | None = None
    if isinstance(cfg.ohlc_1m_path, str) and cfg.ohlc_1m_path.strip():
        ohlc_1m = load_sierra_ohlc_1m(
            Path(cfg.ohlc_1m_path),
            session_start=cfg.session_start,
            session_end=cfg.session_end,
        )

    run_dir = _run_dir(output)
    bars_dir = run_dir / "bars"
    bars_dir.mkdir(parents=True, exist_ok=True)

    all_trades: list[pd.DataFrame] = []
    all_metrics: list[dict] = []

    for timeframe in cfg.timeframes_min:
        bars = build_layered_bars(
            ticks=ticks,
            timeframe_min=timeframe,
            threshold=cfg.big_trade_threshold,
            session_start=cfg.session_start,
            session_end=cfg.session_end,
            ohlc_1m=ohlc_1m,
        )

        for ib in cfg.ib_windows_min:
            trades, annotated_bars = simulate_orb_big_trade_strategy(
                bars=bars,
                timeframe_min=timeframe,
                ib_minutes=ib,
                session_start=cfg.session_start,
                session_end=cfg.session_end,
                stop_mode=cfg.stop_mode,
                tp_r_multiple=cfg.tp_r_multiple,
                entry_mode=cfg.entry_mode,
                strategy_mode=cfg.strategy_mode,
            )

            combo_id = f"tf{timeframe}_ib{ib}"
            bar_path = bars_dir / f"bars_{combo_id}.parquet"
            annotated_bars.to_parquet(bar_path)

            metric = summarize_trades(trades)
            metric.update(
                {
                    "combo": combo_id,
                    "timeframe_min": timeframe,
                    "ib_minutes": ib,
                    "stop_mode": cfg.stop_mode,
                    "tp_r_multiple": cfg.tp_r_multiple,
                    "entry_mode": cfg.entry_mode,
                    "strategy_mode": cfg.strategy_mode,
                    "vectorbt_total_return": 0.0,
                }
            )

            if not trades.empty:
                trades = trades.copy()
                trades["combo"] = combo_id
                all_trades.append(trades)

                try:
                    pf = build_vectorbt_portfolio(annotated_bars, trades)
                    metric["vectorbt_total_return"] = float(pf.total_return())
                except Exception:
                    metric["vectorbt_total_return"] = 0.0

            all_metrics.append(metric)

            del annotated_bars
            gc.collect()

        del bars
        gc.collect()

    metrics_df = pd.DataFrame(all_metrics).sort_values(["timeframe_min", "ib_minutes"])
    metrics_df.to_csv(run_dir / "metrics.csv", index=False)

    if all_trades:
        trades_df = pd.concat(all_trades, ignore_index=True)
    else:
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
    trades_df.to_csv(run_dir / "trades.csv", index=False)

    (run_dir / "config_used.json").write_text(cfg.model_dump_json(indent=2), encoding="utf-8")
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

    latest_link = output / "latest"
    if latest_link.is_symlink() or latest_link.is_file():
        latest_link.unlink()
    elif latest_link.exists() and latest_link.is_dir():
        shutil.rmtree(latest_link)
    latest_link.symlink_to(run_dir.resolve(), target_is_directory=True)

    typer.echo(f"Run complete: {run_dir}")


@app.command()
def serve(run_dir: Path = typer.Option(Path("runs/latest"), help="Path to run artifact directory")) -> None:
    """Launch Streamlit results UI."""
    cmd = [
        "streamlit",
        "run",
        "app/streamlit_app.py",
        "--",
        f"--run-dir={run_dir}",
    ]
    raise SystemExit(subprocess.call(cmd))


if __name__ == "__main__":
    app()
