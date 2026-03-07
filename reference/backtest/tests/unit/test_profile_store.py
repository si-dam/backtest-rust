from __future__ import annotations

from datetime import datetime

import pandas as pd

from app.db.duck import get_duckdb_connection, init_duckdb
from app.services.profile_store import load_persisted_preset_profiles


def _insert_segments(rows: pd.DataFrame) -> None:
    con = get_duckdb_connection()
    try:
        con.register("segments_df", rows)
        con.execute(
            """
            INSERT INTO preset_profile_segments (
                symbol_contract,
                profile_timezone,
                metric,
                preset,
                tick_aggregation,
                tick_size,
                profile_id,
                label,
                segment_start,
                segment_end,
                max_value,
                total_value,
                value_area_enabled,
                value_area_percent,
                value_area_poc,
                value_area_low,
                value_area_high,
                value_area_volume,
                level_count
            )
            SELECT
                symbol_contract,
                profile_timezone,
                metric,
                preset,
                tick_aggregation,
                tick_size,
                profile_id,
                label,
                segment_start,
                segment_end,
                max_value,
                total_value,
                value_area_enabled,
                value_area_percent,
                value_area_poc,
                value_area_low,
                value_area_high,
                value_area_volume,
                level_count
            FROM segments_df
            """
        )
        con.unregister("segments_df")
    finally:
        con.close()


def _insert_levels(rows: pd.DataFrame) -> None:
    con = get_duckdb_connection()
    try:
        con.register("levels_df", rows)
        con.execute(
            """
            INSERT INTO preset_profile_levels (
                symbol_contract,
                profile_timezone,
                metric,
                preset,
                tick_aggregation,
                profile_id,
                price_level,
                value
            )
            SELECT
                symbol_contract,
                profile_timezone,
                metric,
                preset,
                tick_aggregation,
                profile_id,
                price_level,
                value
            FROM levels_df
            """
        )
        con.unregister("levels_df")
    finally:
        con.close()


def _seed_profiles() -> None:
    segments = pd.DataFrame(
        [
            {
                "symbol_contract": "NQH26",
                "profile_timezone": "America/New_York",
                "metric": "volume",
                "preset": "day",
                "tick_aggregation": 1,
                "tick_size": 0.25,
                "profile_id": "day-2026-02-24",
                "label": "Day 2026-02-24",
                "segment_start": pd.Timestamp("2026-02-24T16:00:00-05:00"),
                "segment_end": pd.Timestamp("2026-02-25T15:59:59-05:00"),
                "max_value": 120.0,
                "total_value": 450.0,
                "value_area_enabled": False,
                "value_area_percent": 70.0,
                "value_area_poc": None,
                "value_area_low": None,
                "value_area_high": None,
                "value_area_volume": 0.0,
                "level_count": 2,
            },
            {
                "symbol_contract": "NQH26",
                "profile_timezone": "America/New_York",
                "metric": "volume",
                "preset": "day",
                "tick_aggregation": 1,
                "tick_size": 0.25,
                "profile_id": "day-2026-02-25",
                "label": "Day 2026-02-25",
                "segment_start": pd.Timestamp("2026-02-25T16:00:00-05:00"),
                "segment_end": pd.Timestamp("2026-02-26T15:59:59-05:00"),
                "max_value": 110.0,
                "total_value": 350.0,
                "value_area_enabled": False,
                "value_area_percent": 70.0,
                "value_area_poc": None,
                "value_area_low": None,
                "value_area_high": None,
                "value_area_volume": 0.0,
                "level_count": 1,
            },
        ]
    )
    _insert_segments(segments)

    levels = pd.DataFrame(
        [
            {
                "symbol_contract": "NQH26",
                "profile_timezone": "America/New_York",
                "metric": "volume",
                "preset": "day",
                "tick_aggregation": 1,
                "profile_id": "day-2026-02-24",
                "price_level": 22000.25,
                "value": 30.0,
            },
            {
                "symbol_contract": "NQH26",
                "profile_timezone": "America/New_York",
                "metric": "volume",
                "preset": "day",
                "tick_aggregation": 1,
                "profile_id": "day-2026-02-24",
                "price_level": 22000.50,
                "value": 20.0,
            },
            {
                "symbol_contract": "NQH26",
                "profile_timezone": "America/New_York",
                "metric": "volume",
                "preset": "day",
                "tick_aggregation": 1,
                "profile_id": "day-2026-02-25",
                "price_level": 22001.00,
                "value": 15.0,
            },
        ]
    )
    _insert_levels(levels)


def test_load_persisted_profiles_reads_rows_and_levels(temp_duckdb):
    init_duckdb()
    _seed_profiles()

    payload = load_persisted_preset_profiles(
        symbol_contract="NQH26",
        start=pd.Timestamp("2026-02-24T00:00:00-05:00").to_pydatetime(),
        end=pd.Timestamp("2026-02-26T23:00:00-05:00").to_pydatetime(),
        preset="day",
        profile_timezone="America/New_York",
        metric="volume",
        tick_aggregation=1,
        max_segments=10,
    )

    assert payload["symbol_contract"] == "NQH26"
    assert payload["preset"] == "day"
    assert payload["metric"] == "volume"
    assert payload["tick_aggregation"] == 1
    assert payload["tick_size"] == 0.25
    assert len(payload["profiles"]) == 2
    assert payload["profiles"][0]["id"] == "day-2026-02-24"
    assert payload["profiles"][1]["id"] == "day-2026-02-25"
    assert len(payload["profiles"][0]["levels"]) == 2
    assert payload["profiles"][0]["levels"][0]["price_level"] == 22000.25
    assert payload["profiles"][0]["levels"][0]["value"] == 30.0
    assert payload["profiles"][0]["levels"][0]["volume"] == 30.0


def test_load_persisted_profiles_applies_overlap_and_max_segments(temp_duckdb):
    init_duckdb()
    _seed_profiles()

    payload = load_persisted_preset_profiles(
        symbol_contract="NQH26",
        start=pd.Timestamp("2026-02-25T16:00:00-05:00").to_pydatetime(),
        end=pd.Timestamp("2026-02-26T16:00:00-05:00").to_pydatetime(),
        preset="day",
        profile_timezone="America/New_York",
        metric="volume",
        tick_aggregation=1,
        max_segments=1,
    )
    assert len(payload["profiles"]) == 1
    assert payload["profiles"][0]["id"] == "day-2026-02-25"


def test_load_persisted_profiles_converts_segment_times_to_response_timezone(temp_duckdb):
    init_duckdb()
    _seed_profiles()

    payload = load_persisted_preset_profiles(
        symbol_contract="NQH26",
        start=pd.Timestamp("2026-02-24T00:00:00-05:00").to_pydatetime(),
        end=pd.Timestamp("2026-02-25T23:59:59-05:00").to_pydatetime(),
        preset="day",
        profile_timezone="UTC",
        metric="volume",
        tick_aggregation=1,
        max_segments=10,
    )

    profile = payload["profiles"][0]
    assert payload["timezone"] == "UTC"
    assert profile["start"].endswith("+00:00")
    assert profile["end"].endswith("+00:00")


def test_load_persisted_profiles_returns_empty_payload_when_rows_missing(temp_duckdb):
    init_duckdb()

    payload = load_persisted_preset_profiles(
        symbol_contract="NQH26",
        start=pd.Timestamp("2026-02-24T00:00:00-05:00").to_pydatetime(),
        end=pd.Timestamp("2026-02-25T23:59:59-05:00").to_pydatetime(),
        preset="day",
        profile_timezone="America/New_York",
        metric="volume",
        tick_aggregation=1,
        max_segments=10,
    )

    assert payload["profiles"] == []
    assert payload["symbol_contract"] == "NQH26"
    assert payload["preset"] == "day"
    assert payload["metric"] == "volume"


def test_load_persisted_profiles_coerces_unsupported_tick_aggregation(temp_duckdb):
    init_duckdb()
    _seed_profiles()

    payload = load_persisted_preset_profiles(
        symbol_contract="NQH26",
        start=pd.Timestamp("2026-02-24T00:00:00-05:00").to_pydatetime(),
        end=pd.Timestamp("2026-02-26T23:00:00-05:00").to_pydatetime(),
        preset="day",
        profile_timezone="America/New_York",
        metric="volume",
        tick_aggregation=2,
        max_segments=10,
    )

    assert payload["tick_aggregation"] == 1
    assert len(payload["profiles"]) == 2
