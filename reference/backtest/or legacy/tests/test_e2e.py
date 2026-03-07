from __future__ import annotations

from pathlib import Path

import pandas as pd

from orbt.backtest import simulate_orb_big_trade_strategy
from orbt.ingest_sierra import ingest_to_parquet
from orbt.resample import resample_ticks_to_bars


def test_end_to_end_small_pipeline(tmp_path: Path) -> None:
    raw = tmp_path / "sample.txt"
    raw.write_text(
        """Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume
2026/2/18, 09:30:00, 100, 100, 100, 100, 1, 1, 1, 0
2026/2/18, 09:30:30, 101, 101, 101, 101, 1, 1, 0, 1
2026/2/18, 09:31:10, 102, 102, 102, 102, 30, 1, 0, 30
2026/2/18, 09:32:10, 106, 106, 106, 106, 1, 1, 0, 1
""",
        encoding="utf-8",
    )

    pq = tmp_path / "ticks.parquet"
    ingest_to_parquet(raw, pq)
    ticks = pd.read_parquet(pq)

    bars = resample_ticks_to_bars(
        ticks=ticks,
        timeframe_min=1,
        threshold=25,
        session_start="09:30:00",
        session_end="16:00:00",
    )

    trades, _ = simulate_orb_big_trade_strategy(
        bars=bars,
        timeframe_min=1,
        ib_minutes=1,
        session_start="09:30:00",
        session_end="16:00:00",
        stop_mode="or_boundary",
        tp_r_multiple=2.0,
    )

    assert not bars.empty
    assert len(trades) == 1
