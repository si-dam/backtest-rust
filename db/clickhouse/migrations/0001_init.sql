CREATE TABLE IF NOT EXISTS ticks (
    ts DateTime64(6, 'UTC'),
    trading_day Date,
    session_date Date,
    symbol_contract String,
    trade_price Float64,
    trade_size Float64,
    bid_price Nullable(Float64),
    ask_price Nullable(Float64),
    source_file String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trading_day)
ORDER BY (symbol_contract, ts);

CREATE TABLE IF NOT EXISTS bars_time (
    ts DateTime64(6, 'UTC'),
    trading_day Date,
    session_date Date,
    symbol_contract String,
    timeframe String,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    trade_count UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trading_day)
ORDER BY (symbol_contract, timeframe, ts);

CREATE TABLE IF NOT EXISTS bars_non_time (
    ts DateTime64(6, 'UTC'),
    trading_day Date,
    session_date Date,
    symbol_contract String,
    bar_type String,
    bar_size UInt32,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    trade_count UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trading_day)
ORDER BY (symbol_contract, bar_type, bar_size, ts);

CREATE TABLE IF NOT EXISTS profile_segments (
    segment_id UUID,
    symbol_contract String,
    preset String,
    metric String,
    profile_timezone String,
    label String,
    segment_start DateTime64(6, 'UTC'),
    segment_end DateTime64(6, 'UTC'),
    base_tick_size Float64,
    total_value Float64,
    max_value Float64,
    value_area_enabled Bool,
    value_area_percent Float64,
    value_area_poc Nullable(Float64),
    value_area_low Nullable(Float64),
    value_area_high Nullable(Float64),
    value_area_volume Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(segment_start)
ORDER BY (symbol_contract, preset, metric, profile_timezone, segment_start, segment_id);

CREATE TABLE IF NOT EXISTS profile_levels_base (
    segment_id UUID,
    symbol_contract String,
    price_level Float64,
    total_volume Float64,
    buy_volume Float64,
    sell_volume Float64,
    delta Float64
)
ENGINE = MergeTree
ORDER BY (segment_id, price_level);

CREATE TABLE IF NOT EXISTS large_orders (
    ts DateTime64(6, 'UTC'),
    trading_day Date,
    session_date Date,
    symbol_contract String,
    method String,
    threshold Float64,
    trade_price Float64,
    trade_size Float64,
    side String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trading_day)
ORDER BY (symbol_contract, method, ts);
