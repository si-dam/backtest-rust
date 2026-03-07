from pathlib import Path

import duckdb

from app.config import get_settings


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    settings = get_settings()
    db_path = Path(settings.duckdb_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def init_duckdb() -> None:
    con = get_duckdb_connection()
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ticks (
                ts TIMESTAMPTZ,
                session_date DATE,
                symbol_contract VARCHAR,
                trade_price DOUBLE,
                trade_size DOUBLE,
                bid_price DOUBLE,
                ask_price DOUBLE,
                source_file VARCHAR,
                ingested_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS bars (
                ts TIMESTAMPTZ,
                session_date DATE,
                timeframe VARCHAR,
                symbol_contract VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                trade_count BIGINT,
                updated_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS session_profiles (
                session_date DATE,
                symbol_contract VARCHAR,
                price_level DOUBLE,
                volume DOUBLE,
                updated_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS large_orders (
                ts TIMESTAMPTZ,
                session_date DATE,
                symbol_contract VARCHAR,
                trade_price DOUBLE,
                trade_size DOUBLE,
                method VARCHAR,
                threshold DOUBLE,
                updated_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS large_order_backfill_state (
                symbol_contract VARCHAR,
                session_date DATE,
                method VARCHAR,
                threshold DOUBLE,
                processed_at TIMESTAMPTZ DEFAULT now(),
                row_count BIGINT
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS preset_profile_segments (
                symbol_contract VARCHAR,
                profile_timezone VARCHAR,
                metric VARCHAR,
                preset VARCHAR,
                tick_aggregation INTEGER,
                tick_size DOUBLE,
                profile_id VARCHAR,
                label VARCHAR,
                segment_start TIMESTAMPTZ,
                segment_end TIMESTAMPTZ,
                max_value DOUBLE,
                total_value DOUBLE,
                value_area_enabled BOOLEAN,
                value_area_percent DOUBLE,
                value_area_poc DOUBLE,
                value_area_low DOUBLE,
                value_area_high DOUBLE,
                value_area_volume DOUBLE,
                level_count BIGINT,
                updated_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS preset_profile_levels (
                symbol_contract VARCHAR,
                profile_timezone VARCHAR,
                metric VARCHAR,
                preset VARCHAR,
                tick_aggregation INTEGER,
                profile_id VARCHAR,
                price_level DOUBLE,
                value DOUBLE,
                updated_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks(symbol_contract, ts)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_bars_symbol_timeframe_ts ON bars(symbol_contract, timeframe, ts)")
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_large_orders_symbol_method_threshold_ts ON large_orders(symbol_contract, method, threshold, ts)"
        )
        con.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_large_order_backfill_state_unique
            ON large_order_backfill_state(symbol_contract, session_date, method, threshold)
            """
        )
        con.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_preset_profile_segments_unique
            ON preset_profile_segments(symbol_contract, profile_timezone, metric, preset, tick_aggregation, profile_id)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_preset_profile_segments_lookup
            ON preset_profile_segments(symbol_contract, profile_timezone, metric, preset, tick_aggregation, segment_end)
            """
        )
        con.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_preset_profile_levels_unique
            ON preset_profile_levels(symbol_contract, profile_timezone, metric, preset, tick_aggregation, profile_id, price_level)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_preset_profile_levels_lookup
            ON preset_profile_levels(symbol_contract, profile_timezone, metric, preset, tick_aggregation, profile_id)
            """
        )
    finally:
        con.close()
