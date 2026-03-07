from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd


def test_ui_loaders_smoke(tmp_path: Path) -> None:
    __import__("streamlit")

    run_dir = tmp_path / "run"
    bars_dir = run_dir / "bars"
    bars_dir.mkdir(parents=True)

    metrics = pd.DataFrame(
        [
            {
                "combo": "tf1_ib15",
                "timeframe_min": 1,
                "ib_minutes": 15,
                "total_pnl": 10.0,
                "trades": 1,
            }
        ]
    )
    metrics.to_csv(run_dir / "metrics.csv", index=False)

    trades = pd.DataFrame(
        [
            {
                "combo": "tf1_ib15",
                "entry_time": "2026-02-18 09:31:00",
                "entry_price": 100.0,
                "exit_time": "2026-02-18 09:32:00",
                "exit_price": 102.0,
                "pnl": 2.0,
            }
        ]
    )
    trades.to_csv(run_dir / "trades.csv", index=False)

    bars = pd.DataFrame(
        {
            "open": [100.0],
            "high": [102.0],
            "low": [99.0],
            "close": [101.0],
            "or_high": [101.0],
            "or_low": [99.0],
            "has_big_buy": [True],
            "has_big_sell": [False],
        },
        index=pd.to_datetime(["2026-02-18 09:31:00"]),
    )
    bars.to_parquet(bars_dir / "bars_tf1_ib15.parquet")

    from app.streamlit_app import load_bars, load_run_artifacts

    m, t = load_run_artifacts(str(run_dir))
    b = load_bars(str(run_dir), "tf1_ib15")

    assert len(m) == 1
    assert len(t) == 1
    assert len(b) == 1


def test_ui_defaults_and_combo_id() -> None:
    from app.streamlit_app import build_combo_id, default_ui_params

    defaults = default_ui_params()
    assert defaults["ib_minutes"] == 15
    assert defaults["tp_r_multiple"] == 2.0
    assert defaults["big_trade_threshold"] == 25
    assert defaults["stop_mode"] == "or_boundary"
    assert defaults["entry_mode"] == "first_outside"
    assert defaults["strategy_mode"] == "big_order_required"

    assert build_combo_id(timeframe_min=3, ib_minutes=30) == "tf3_ib30"


def test_recompute_and_persist_writes_artifacts(tmp_path: Path) -> None:
    from app.streamlit_app import recompute_and_persist

    project_root = tmp_path
    data_dir = project_root / "data" / "processed"
    data_dir.mkdir(parents=True)

    ticks = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-02-18 09:30:00",
                    "2026-02-18 09:31:00",
                    "2026-02-18 09:45:00",
                    "2026-02-18 09:46:00",
                    "2026-02-18 09:47:00",
                ]
            ),
            "last": [100.0, 101.0, 102.0, 103.0, 103.5],
            "volume": [1, 1, 30, 1, 1],
            "bid_volume": [1, 1, 0, 0, 0],
            "ask_volume": [0, 0, 30, 1, 1],
            "seq": [0, 0, 0, 0, 0],
        }
    )
    ticks_path = data_dir / "ticks.parquet"
    ticks.to_parquet(ticks_path, index=False)

    source_run_dir = project_root / "runs" / "source"
    source_run_dir.mkdir(parents=True)
    (source_run_dir / "config_used.json").write_text(
        json.dumps(
            {
                "data_path": "data/processed/ticks.parquet",
                "session_start": "09:30:00",
                "session_end": "16:00:00",
                "symbol": "NQ",
                "cost_model": "none",
            }
        ),
        encoding="utf-8",
    )

    run_dir, combo = recompute_and_persist(
        source_run_dir=source_run_dir,
        ib_minutes=15,
        timeframe_min=1,
        tp_r_multiple=2.0,
        big_trade_threshold=25,
        stop_mode="or_boundary",
        project_root=project_root,
    )

    assert combo == "tf1_ib15"
    assert (run_dir / "metrics.csv").exists()
    assert (run_dir / "trades.csv").exists()
    assert (run_dir / "config_used.json").exists()
    assert (run_dir / "manifest.json").exists()
    assert (run_dir / "bars" / "bars_tf1_ib15.parquet").exists()

    cfg = json.loads((run_dir / "config_used.json").read_text(encoding="utf-8"))
    assert cfg["entry_mode"] == "first_outside"
    assert cfg["strategy_mode"] == "big_order_required"

    latest = project_root / "runs" / "latest"
    assert latest.exists() and latest.is_symlink()


def test_backfill_full_tp_wins_for_legacy_metrics() -> None:
    from app.streamlit_app import _ensure_full_losses, _ensure_full_tp_wins

    metrics = pd.DataFrame(
        [
            {"combo": "tf1_ib15", "trades": 3},
            {"combo": "tf3_ib15", "trades": 2},
        ]
    )
    trades = pd.DataFrame(
        [
            {"combo": "tf1_ib15", "exit_reason": "target", "pnl": 1.0},
            {"combo": "tf1_ib15", "exit_reason": "session_close", "pnl": 0.5},
            {"combo": "tf3_ib15", "exit_reason": "target", "pnl": 2.0},
            {"combo": "tf3_ib15", "exit_reason": "stop", "pnl": -1.0},
        ]
    )

    out = _ensure_full_tp_wins(metrics, trades)
    out = _ensure_full_losses(out, trades)

    assert "full_tp_wins" in out.columns
    assert out.set_index("combo").loc["tf1_ib15", "full_tp_wins"] == 1
    assert out.set_index("combo").loc["tf3_ib15", "full_tp_wins"] == 1
    assert "full_losses" in out.columns
    assert out.set_index("combo").loc["tf1_ib15", "full_losses"] == 0
    assert out.set_index("combo").loc["tf3_ib15", "full_losses"] == 1


def test_load_data_date_bounds_from_parquet(tmp_path: Path) -> None:
    from app.streamlit_app import load_data_date_bounds

    ticks = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-02-18 09:30:00",
                    "2026-02-19 10:00:00",
                    "2026-02-20 11:00:00",
                ]
            ),
            "last": [100.0, 101.0, 102.0],
        }
    )
    path = tmp_path / "ticks.parquet"
    ticks.to_parquet(path, index=False)

    dmin, dmax = load_data_date_bounds(str(path))

    assert dmin.isoformat() == "2026-02-18"
    assert dmax.isoformat() == "2026-02-20"


def test_recompute_and_persist_applies_date_range(tmp_path: Path) -> None:
    from app.streamlit_app import recompute_and_persist

    project_root = tmp_path
    data_dir = project_root / "data" / "processed"
    data_dir.mkdir(parents=True)

    ticks = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-02-18 09:30:00",
                    "2026-02-18 09:31:00",
                    "2026-02-18 09:45:00",
                    "2026-02-18 09:46:00",
                    "2026-02-18 09:47:00",
                    "2026-02-19 09:30:00",
                    "2026-02-19 09:31:00",
                    "2026-02-19 09:45:00",
                    "2026-02-19 09:46:00",
                    "2026-02-19 09:47:00",
                ]
            ),
            "last": [100.0, 101.0, 102.0, 103.0, 103.5, 104.0, 105.0, 106.0, 107.0, 107.5],
            "volume": [1, 1, 30, 1, 1, 1, 1, 30, 1, 1],
            "bid_volume": [1, 1, 0, 0, 0, 1, 1, 0, 0, 0],
            "ask_volume": [0, 0, 30, 1, 1, 0, 0, 30, 1, 1],
            "seq": [0] * 10,
        }
    )
    ticks_path = data_dir / "ticks.parquet"
    ticks.to_parquet(ticks_path, index=False)

    source_run_dir = project_root / "runs" / "source"
    source_run_dir.mkdir(parents=True)
    (source_run_dir / "config_used.json").write_text(
        json.dumps(
            {
                "data_path": "data/processed/ticks.parquet",
                "session_start": "09:30:00",
                "session_end": "16:00:00",
                "symbol": "NQ",
                "cost_model": "none",
            }
        ),
        encoding="utf-8",
    )

    run_dir, combo = recompute_and_persist(
        source_run_dir=source_run_dir,
        ib_minutes=15,
        timeframe_min=1,
        tp_r_multiple=2.0,
        big_trade_threshold=25,
        stop_mode="or_boundary",
        start_date=date(2026, 2, 19),
        end_date=date(2026, 2, 19),
        project_root=project_root,
    )

    assert combo == "tf1_ib15"
    trades = pd.read_csv(run_dir / "trades.csv")
    if not trades.empty:
        assert set(trades["session_date"].astype(str).unique()) == {"2026-02-19"}

    cfg = json.loads((run_dir / "config_used.json").read_text(encoding="utf-8"))
    assert cfg["date_start"] == "2026-02-19"
    assert cfg["date_end"] == "2026-02-19"


def test_densify_bars_for_chart_fills_time_gaps() -> None:
    from app.streamlit_app import _densify_bars_for_chart

    bars = pd.DataFrame(
        {
            "open": [100.0, 102.0],
            "high": [101.0, 103.0],
            "low": [99.5, 101.5],
            "close": [100.5, 102.5],
            "volume": [10, 12],
            "has_big_buy": [True, False],
        },
        index=pd.to_datetime(["2026-02-18 09:31:00", "2026-02-18 09:33:00"]),
    )

    out = _densify_bars_for_chart(bars, timeframe_min=1)

    assert list(out.index.astype(str)) == [
        "2026-02-18 09:31:00",
        "2026-02-18 09:32:00",
        "2026-02-18 09:33:00",
    ]
    assert float(out.loc[pd.Timestamp("2026-02-18 09:32:00"), "open"]) == 100.5
    assert float(out.loc[pd.Timestamp("2026-02-18 09:32:00"), "close"]) == 100.5
    assert bool(out.loc[pd.Timestamp("2026-02-18 09:32:00"), "has_big_buy"]) is False
    assert float(out.loc[pd.Timestamp("2026-02-18 09:32:00"), "high"]) > float(out.loc[pd.Timestamp("2026-02-18 09:32:00"), "low"])


def test_densify_bars_for_chart_uses_full_session_window() -> None:
    from app.streamlit_app import _densify_bars_for_chart

    bars = pd.DataFrame(
        {
            "open": [100.0, 102.0],
            "high": [101.0, 103.0],
            "low": [99.5, 101.5],
            "close": [100.5, 102.5],
        },
        index=pd.to_datetime(["2026-02-18 09:35:00", "2026-02-18 09:37:00"]),
    )

    out = _densify_bars_for_chart(
        bars,
        timeframe_min=1,
        session_start="09:30:00",
        session_end="09:40:00",
    )

    assert out.index.min() == pd.Timestamp("2026-02-18 09:31:00")
    assert out.index.max() == pd.Timestamp("2026-02-18 09:40:00")
    assert len(out) == 10


def test_prepare_trade_chart_bars_is_strategy_agnostic_and_scopes_day() -> None:
    import inspect

    from app.streamlit_app import _prepare_trade_chart_bars

    sig = inspect.signature(_prepare_trade_chart_bars)
    assert "strategy_mode" not in sig.parameters

    bars = pd.DataFrame(
        {
            "open": [100.0, 101.0, 200.0],
            "high": [101.0, 102.0, 201.0],
            "low": [99.0, 100.0, 199.0],
            "close": [100.5, 101.5, 200.5],
        },
        index=pd.to_datetime(
            [
                "2026-02-18 09:31:00",
                "2026-02-18 09:33:00",
                "2026-02-19 09:31:00",
            ]
        ),
    )

    selected_trade = pd.DataFrame([{"session_date": "2026-02-18"}])

    chart_bars, raw_count, synthetic_count = _prepare_trade_chart_bars(
        bars=bars,
        selected_trade=selected_trade,
        timeframe_min=1,
        session_start="09:30:00",
        session_end="09:35:00",
    )

    assert raw_count == 2
    assert len(chart_bars) == 5
    assert synthetic_count == 3
    assert chart_bars.index.min() == pd.Timestamp("2026-02-18 09:31:00")
    assert chart_bars.index.max() == pd.Timestamp("2026-02-18 09:35:00")


def test_recompute_and_persist_layers_ohlc_with_tick_features(tmp_path: Path) -> None:
    from app.streamlit_app import recompute_and_persist

    project_root = tmp_path
    data_dir = project_root / "data" / "processed"
    data_dir.mkdir(parents=True)

    ticks = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-02-18 09:31:10",
                    "2026-02-18 09:32:10",
                ]
            ),
            "last": [100.2, 100.8],
            "volume": [1, 30],
            "bid_volume": [1, 0],
            "ask_volume": [0, 30],
            "seq": [0, 0],
        }
    )
    ticks_path = data_dir / "ticks.parquet"
    ticks.to_parquet(ticks_path, index=False)

    ohlc = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-02-18 09:31:00",
                    "2026-02-18 09:32:00",
                    "2026-02-18 09:33:00",
                ]
            ),
            "open": [100.0, 100.4, 100.6],
            "high": [100.5, 100.9, 101.2],
            "low": [99.8, 100.2, 100.5],
            "close": [100.4, 100.6, 101.0],
            "volume": [10, 12, 15],
        }
    )
    ohlc_path = data_dir / "ohlc_1m.parquet"
    ohlc.to_parquet(ohlc_path, index=False)

    source_run_dir = project_root / "runs" / "source"
    source_run_dir.mkdir(parents=True)
    (source_run_dir / "config_used.json").write_text(
        json.dumps(
            {
                "data_path": "data/processed/ticks.parquet",
                "ohlc_1m_path": "data/processed/ohlc_1m.parquet",
                "session_start": "09:30:00",
                "session_end": "16:00:00",
                "symbol": "NQ",
                "cost_model": "none",
            }
        ),
        encoding="utf-8",
    )

    run_dir, _ = recompute_and_persist(
        source_run_dir=source_run_dir,
        ib_minutes=1,
        timeframe_min=1,
        tp_r_multiple=1.0,
        big_trade_threshold=25,
        stop_mode="or_boundary",
        project_root=project_root,
    )

    bars = pd.read_parquet(run_dir / "bars" / "bars_tf1_ib1.parquet")
    bars.index = pd.to_datetime(bars.index)

    assert len(bars) == 3
    assert bool(bars.loc[pd.Timestamp("2026-02-18 09:33:00"), "has_big_buy"]) is True
    assert int(bars.loc[pd.Timestamp("2026-02-18 09:33:00"), "max_big_buy"]) == 30
