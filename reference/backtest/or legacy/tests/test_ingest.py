from __future__ import annotations

from pathlib import Path

from orbt.ingest_sierra import read_sierra_export


def test_parse_mixed_time_and_duplicate_seq(tmp_path: Path) -> None:
    data = """Date, Time, Open, High, Low, Last, Volume, NumberOfTrades, BidVolume, AskVolume
2026/2/18, 09:30:00, 100, 100, 100, 100, 1, 1, 1, 0
2026/2/18, 09:30:00.001, 101, 101, 101, 101, 2, 1, 0, 2
2026/2/18, 09:30:00.001, 102, 102, 102, 102, 1, 1, 1, 0
"""
    path = tmp_path / "sample.txt"
    path.write_text(data, encoding="utf-8")

    df = read_sierra_export(path)

    assert len(df) == 3
    assert list(df.columns) == [
        "timestamp",
        "open",
        "high",
        "low",
        "last",
        "volume",
        "number_of_trades",
        "bid_volume",
        "ask_volume",
        "seq",
    ]

    dup = df[df["timestamp"] == df["timestamp"].iloc[1]]
    assert dup["seq"].tolist() == [0, 1]
