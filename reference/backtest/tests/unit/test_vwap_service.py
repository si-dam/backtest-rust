import pandas as pd
import pytest

from app.config import get_settings
from app.db.duck import get_duckdb_connection, init_duckdb
from app.services.vwap import ensure_preset_vwap


def _insert_ticks(rows: pd.DataFrame) -> None:
    con = get_duckdb_connection()
    try:
        con.register("ticks_df", rows)
        con.execute(
            """
            INSERT INTO ticks (ts, session_date, symbol_contract, trade_price, trade_size, bid_price, ask_price, source_file)
            SELECT ts, session_date, symbol_contract, trade_price, trade_size, bid_price, ask_price, source_file
            FROM ticks_df
            """
        )
        con.unregister("ticks_df")
    finally:
        con.close()


def test_ensure_preset_vwap_segments_day_week_rth(temp_duckdb):
    init_duckdb()
    dataset_tz = get_settings().dataset_tz
    ts = pd.to_datetime(
        [
            "2026-02-24T14:25:00Z",  # 09:25 ET (ETH)
            "2026-02-24T14:35:00Z",  # 09:35 ET (RTH)
            "2026-02-24T20:59:00Z",  # 15:59 ET (RTH)
            "2026-02-24T21:05:00Z",  # 16:05 ET (ETH)
            "2026-02-25T14:20:00Z",  # 09:20 ET (ETH)
            "2026-02-25T14:31:00Z",  # 09:31 ET (RTH)
            "2026-02-25T21:01:00Z",  # 16:01 ET (ETH)
            "2026-03-02T20:00:00Z",  # 15:00 ET (RTH)
            "2026-03-02T21:00:00Z",  # 16:00 ET (ETH, RTH boundary excluded)
        ]
    )
    rows = pd.DataFrame(
        {
            "ts": ts,
            "session_date": ts.tz_convert(dataset_tz).date,
            "symbol_contract": ["NQH6"] * len(ts),
            "trade_price": [100.0, 100.25, 100.5, 100.75, 100.0, 100.25, 100.5, 100.75, 101.0],
            "trade_size": [1, 2, 3, 4, 5, 6, 7, 8, 9],
            "bid_price": [99.75] * len(ts),
            "ask_price": [100.0] * len(ts),
            "source_file": ["fixture-vwap.csv"] * len(ts),
        }
    )
    _insert_ticks(rows)

    start = rows["ts"].min().to_pydatetime()
    end = rows["ts"].max().to_pydatetime()

    day_payload = ensure_preset_vwap("NQH6", start, end, preset="day")
    assert day_payload["preset"] == "day"
    assert len(day_payload["segments"]) == 5

    week_payload = ensure_preset_vwap("NQH6", start, end, preset="week")
    assert week_payload["preset"] == "week"
    assert len(week_payload["segments"]) == 2

    rth_payload = ensure_preset_vwap("NQH6", start, end, preset="rth")
    assert rth_payload["preset"] == "rth"
    assert len(rth_payload["segments"]) == 3
    assert all(segment["label"].startswith("RTH ") for segment in rth_payload["segments"])


def test_ensure_preset_vwap_sierra_running_values(temp_duckdb):
    init_duckdb()
    dataset_tz = get_settings().dataset_tz
    ts = pd.to_datetime(
        [
            "2026-02-24T14:31:00Z",
            "2026-02-24T14:32:00Z",
            "2026-02-24T14:33:00Z",
        ]
    )
    rows = pd.DataFrame(
        {
            "ts": ts,
            "session_date": ts.tz_convert(dataset_tz).date,
            "symbol_contract": ["NQH6"] * 3,
            "trade_price": [100.0, 101.0, 102.0],
            "trade_size": [1.0, 1.0, 1.0],
            "bid_price": [99.75, 100.75, 101.75],
            "ask_price": [100.0, 101.0, 102.0],
            "source_file": ["fixture-vwap-values.csv"] * 3,
        }
    )
    _insert_ticks(rows)

    start = rows["ts"].min().to_pydatetime()
    end = rows["ts"].max().to_pydatetime()
    payload = ensure_preset_vwap("NQH6", start, end, preset="rth", profile_timezone="America/New_York")

    assert len(payload["segments"]) == 1
    points = payload["segments"][0]["points"]
    assert len(points) == 3

    assert points[0]["vwap"] == pytest.approx(100.0)
    assert points[0]["upper_1"] == pytest.approx(100.0)
    assert points[0]["lower_1"] == pytest.approx(100.0)

    assert points[1]["vwap"] == pytest.approx(100.5)
    assert points[1]["upper_1"] == pytest.approx(101.0)
    assert points[1]["lower_1"] == pytest.approx(100.0)

    assert points[2]["vwap"] == pytest.approx(101.0)
    assert points[2]["upper_1"] == pytest.approx(101.81649658)
    assert points[2]["lower_1"] == pytest.approx(100.18350342)
    assert points[2]["upper_2"] == pytest.approx(102.63299316)
    assert points[2]["lower_2"] == pytest.approx(99.36700684)


def test_ensure_preset_vwap_rejects_invalid_preset_and_timezone(temp_duckdb):
    init_duckdb()
    start = pd.Timestamp("2026-02-24T14:00:00Z").to_pydatetime()
    end = pd.Timestamp("2026-02-24T15:00:00Z").to_pydatetime()

    with pytest.raises(ValueError, match="Unsupported preset"):
        ensure_preset_vwap("NQH6", start, end, preset="eth")

    with pytest.raises(ValueError, match="Invalid profile timezone"):
        ensure_preset_vwap("NQH6", start, end, preset="day", profile_timezone="Mars/Phobos")
