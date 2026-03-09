mod bars;
mod ingest;
mod profiles;

use std::collections::BTreeMap;

use anyhow::Context;
use app_core::error::ApiError;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use clickhouse::{error::Error as ClickHouseError, Client, Row};
use profiles::{bucket_price, build_preset_profiles, build_value_area, classify_side};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const DEFAULT_TICKS_LIMIT: u32 = 10_000;
const DEFAULT_BARS_LIMIT: u32 = 5_000;
const DEFAULT_LARGE_ORDERS_LIMIT: u32 = 2_000;
const MAX_TICKS_LIMIT: u32 = 50_000;
const MAX_BARS_LIMIT: u32 = 20_000;
const MAX_LARGE_ORDERS_LIMIT: u32 = 10_000;

pub use bars::{
    build_bar_records, build_non_time_bars_from_ticks, build_time_bars_from_ticks,
    timeframe_to_seconds,
};
pub use ingest::{
    parse_market_data_file, summarize_parsed_data, ParsedFileSummary, ParsedMarketData,
};

#[derive(Clone)]
pub struct ClickHouseMarketStore {
    client: Client,
    http: HttpClient,
    base_url: String,
    database: String,
}

impl ClickHouseMarketStore {
    pub fn new(base_url: &str, database: &str) -> Self {
        let client = Client::default()
            .with_url(base_url)
            .with_database(database)
            .with_option("async_insert", "0")
            .with_option("wait_for_async_insert", "0");
        Self {
            client,
            http: HttpClient::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            database: database.to_string(),
        }
    }

    pub async fn ping(&self) -> Result<(), ApiError> {
        self.client
            .query("SELECT 1 AS value")
            .fetch_one::<ClickHousePingRow>()
            .await
            .map(|_| ())
            .map_err(map_clickhouse_err)
    }

    pub async fn list_symbols(&self) -> Result<Vec<SymbolRecord>, ApiError> {
        self.client
            .query(
                "SELECT symbol_contract FROM ticks GROUP BY symbol_contract ORDER BY symbol_contract",
            )
            .fetch_all::<SymbolRecord>()
            .await
            .map_err(map_clickhouse_err)
    }

    pub async fn load_ticks(
        &self,
        symbol: &str,
        query: &TicksQuery,
    ) -> Result<Vec<TickRecord>, ApiError> {
        self.client
            .query(
                r#"
                SELECT ts, session_date, symbol_contract, trade_price, trade_size, bid_price, ask_price
                FROM ticks
                WHERE symbol_contract = ?
                  AND ts >= parseDateTime64BestEffort(?)
                  AND ts <= parseDateTime64BestEffort(?)
                ORDER BY ts
                LIMIT ?
                "#,
            )
            .bind(symbol)
            .bind(format_clickhouse_timestamp(query.start))
            .bind(format_clickhouse_timestamp(query.end))
            .bind(query.limit as u64)
            .fetch_all::<TickRecord>()
            .await
            .map_err(map_clickhouse_err)
    }

    pub async fn load_bars(
        &self,
        symbol: &str,
        query: &BarsQuery,
    ) -> Result<Vec<BarRecord>, ApiError> {
        if query.bar_type == "time" {
            return self
                .client
                .query(
                    r#"
                    SELECT ts, session_date, symbol_contract, timeframe, open, high, low, close, volume, trade_count
                    FROM bars_time
                    WHERE symbol_contract = ?
                      AND timeframe = ?
                      AND ts >= parseDateTime64BestEffort(?)
                      AND ts <= parseDateTime64BestEffort(?)
                    ORDER BY ts
                    LIMIT ?
                    "#,
                )
                .bind(symbol)
                .bind(query.timeframe.clone())
                .bind(format_clickhouse_timestamp(query.start))
                .bind(format_clickhouse_timestamp(query.end))
                .bind(query.limit as u64)
                .fetch_all::<BarRecord>()
                .await
                .map_err(map_clickhouse_err);
        }

        let size = query
            .bar_size
            .ok_or_else(|| ApiError::bad_request("bar_size is required for non-time bars"))?;

        self.client
            .query(
                r#"
                SELECT
                    ts,
                    session_date,
                    symbol_contract,
                    concat(bar_type, ':', toString(bar_size)) AS timeframe,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    trade_count
                FROM bars_non_time
                WHERE symbol_contract = ?
                  AND bar_type = ?
                  AND bar_size = ?
                  AND ts >= parseDateTime64BestEffort(?)
                  AND ts <= parseDateTime64BestEffort(?)
                ORDER BY ts
                LIMIT ?
                "#,
            )
            .bind(symbol)
            .bind(query.bar_type.clone())
            .bind(size)
            .bind(format_clickhouse_timestamp(query.start))
            .bind(format_clickhouse_timestamp(query.end))
            .bind(query.limit as u64)
            .fetch_all::<BarRecord>()
            .await
            .map_err(map_clickhouse_err)
    }

    pub async fn load_large_orders(
        &self,
        symbol: &str,
        query: &LargeOrdersQuery,
    ) -> Result<Vec<LargeOrderRecord>, ApiError> {
        self.client
            .query(
                r#"
                SELECT ts, session_date, symbol_contract, method, threshold, trade_price, trade_size, side
                FROM large_orders
                WHERE symbol_contract = ?
                  AND method = ?
                  AND threshold = ?
                  AND ts >= parseDateTime64BestEffort(?)
                  AND ts <= parseDateTime64BestEffort(?)
                ORDER BY ts
                LIMIT ?
                "#,
            )
            .bind(symbol)
            .bind(query.method.clone())
            .bind(query.fixed_threshold)
            .bind(format_clickhouse_timestamp(query.start))
            .bind(format_clickhouse_timestamp(query.end))
            .bind(query.limit as u64)
            .fetch_all::<LargeOrderRecord>()
            .await
            .map_err(map_clickhouse_err)
    }

    pub async fn load_preset_profiles(
        &self,
        symbol: &str,
        query: &PresetProfileQuery,
    ) -> Result<PresetProfilesResponse, ApiError> {
        let mut segments = self
            .client
            .query(
                r#"
                SELECT
                    segment_id,
                    symbol_contract,
                    preset,
                    metric,
                    profile_timezone,
                    label,
                    segment_start,
                    segment_end,
                    base_tick_size,
                    total_value,
                    max_value,
                    value_area_enabled,
                    value_area_percent,
                    value_area_poc,
                    value_area_low,
                    value_area_high,
                    value_area_volume
                FROM profile_segments
                WHERE symbol_contract = ?
                  AND preset = ?
                  AND metric = ?
                  AND profile_timezone = ?
                  AND segment_end >= parseDateTime64BestEffort(?)
                  AND segment_start <= parseDateTime64BestEffort(?)
                ORDER BY segment_end
                LIMIT ?
                "#,
            )
            .bind(symbol)
            .bind(query.preset.clone())
            .bind(query.metric.clone())
            .bind(query.timezone.clone())
            .bind(format_clickhouse_timestamp(query.start))
            .bind(format_clickhouse_timestamp(query.end))
            .bind(query.max_segments as u64)
            .fetch_all::<ProfileSegmentRow>()
            .await
            .map_err(map_clickhouse_err)?;

        if segments.is_empty() {
            return Ok(PresetProfilesResponse {
                symbol_contract: symbol.to_string(),
                timezone: query.timezone.clone(),
                preset: query.preset.clone(),
                metric: query.metric.clone(),
                tick_aggregation: query.tick_aggregation,
                profiles: Vec::new(),
            });
        }

        segments.sort_by_key(|segment| segment.segment_end);
        let mut profiles = Vec::with_capacity(segments.len());
        for segment in segments {
            let levels = self.load_segment_levels(segment.segment_id).await?;
            let aggregation = query.tick_aggregation.max(1);
            let grouped =
                aggregate_levels(&levels, aggregation, segment.base_tick_size, &query.metric);
            let value_area = if query.value_area_enabled && query.metric == "volume" {
                build_value_area(&grouped, query.value_area_percent)
            } else {
                ValueAreaSummary::disabled(query.value_area_percent)
            };
            profiles.push(ProfilePayload {
                id: segment.segment_id,
                label: segment.label,
                start: segment.segment_start,
                end: segment.segment_end,
                max_value: grouped
                    .iter()
                    .map(|row| row.value.abs())
                    .fold(0.0, f64::max),
                total_value: grouped.iter().map(|row| row.value).sum(),
                value_area_enabled: value_area.enabled,
                value_area_percent: value_area.percent,
                value_area_poc: value_area.poc,
                value_area_low: value_area.low,
                value_area_high: value_area.high,
                value_area_volume: value_area.volume,
                levels: grouped,
            });
        }

        Ok(PresetProfilesResponse {
            symbol_contract: symbol.to_string(),
            timezone: query.timezone.clone(),
            preset: query.preset.clone(),
            metric: query.metric.clone(),
            tick_aggregation: query.tick_aggregation,
            profiles,
        })
    }

    pub async fn load_area_profile(
        &self,
        symbol: &str,
        query: &AreaProfileQuery,
    ) -> Result<AreaProfileResponse, ApiError> {
        let ticks = self
            .load_ticks(
                symbol,
                &TicksQuery {
                    start: query.start,
                    end: query.end,
                    limit: MAX_TICKS_LIMIT,
                },
            )
            .await?;

        let tick_size = query.tick_size.unwrap_or(0.25);
        let aggregation = query.tick_aggregation.max(1);
        let mut values: BTreeMap<i64, f64> = BTreeMap::new();

        for tick in ticks {
            if tick.trade_price < query.price_min || tick.trade_price > query.price_max {
                continue;
            }

            let base_level = bucket_price(tick.trade_price, tick_size);
            let aggregated_level = bucket_price(base_level, tick_size * aggregation as f64);
            let value = if query.metric == "delta" {
                tick.trade_size * classify_side(tick.trade_price, tick.bid_price, tick.ask_price)
            } else {
                tick.trade_size
            };
            let key = (aggregated_level * 10_000.0).round() as i64;
            values
                .entry(key)
                .and_modify(|entry| *entry += value)
                .or_insert(value);
        }

        let levels = values
            .into_iter()
            .map(|(key, value)| ProfileLevelValue {
                price_level: key as f64 / 10_000.0,
                value,
                volume: value,
            })
            .collect::<Vec<_>>();
        let value_area = if query.value_area_enabled && query.metric == "volume" {
            build_value_area(&levels, query.value_area_percent)
        } else {
            ValueAreaSummary::disabled(query.value_area_percent)
        };

        Ok(AreaProfileResponse {
            symbol_contract: symbol.to_string(),
            timezone: query.timezone.clone(),
            metric: query.metric.clone(),
            tick_aggregation: aggregation,
            profile: ProfilePayload {
                id: query.area_id.unwrap_or_else(Uuid::new_v4),
                label: "Area Profile".to_string(),
                start: query.start,
                end: query.end,
                max_value: levels.iter().map(|row| row.value.abs()).fold(0.0, f64::max),
                total_value: levels.iter().map(|row| row.value).sum(),
                value_area_enabled: value_area.enabled,
                value_area_percent: value_area.percent,
                value_area_poc: value_area.poc,
                value_area_low: value_area.low,
                value_area_high: value_area.high,
                value_area_volume: value_area.volume,
                levels,
            },
        })
    }

    async fn load_segment_levels(
        &self,
        segment_id: Uuid,
    ) -> Result<Vec<ProfileLevelBaseRow>, ApiError> {
        self.client
            .query(
                r#"
                SELECT segment_id, symbol_contract, price_level, total_volume, buy_volume, sell_volume, delta
                FROM profile_levels_base
                WHERE segment_id = ?
                ORDER BY price_level
                "#,
            )
            .bind(segment_id)
            .fetch_all::<ProfileLevelBaseRow>()
            .await
            .map_err(map_clickhouse_err)
    }

    pub async fn insert_ticks(&self, rows: &[CanonicalTickRow]) -> anyhow::Result<()> {
        let tick_rows = rows
            .iter()
            .map(|row| {
                json!({
                    "ts": format_clickhouse_timestamp(row.ts),
                    "trading_day": format_clickhouse_date(row.trading_day),
                    "session_date": format_clickhouse_date(row.session_date),
                    "symbol_contract": row.symbol_contract,
                    "trade_price": row.trade_price,
                    "trade_size": row.trade_size,
                    "bid_price": row.bid_price,
                    "ask_price": row.ask_price,
                    "source_file": row.source_file,
                })
            })
            .collect::<Vec<_>>();
        self.insert_json_each_row("ticks", &tick_rows).await
    }

    pub async fn delete_ticks_by_source(&self, source_file: &str) -> anyhow::Result<()> {
        self.client
            .query("ALTER TABLE ticks DELETE WHERE source_file = ?")
            .bind(source_file)
            .execute()
            .await
            .context("failed to delete ticks by source file")?;
        Ok(())
    }

    pub async fn insert_time_bars(&self, rows: &[TimeBarRow]) -> anyhow::Result<()> {
        let bar_rows = rows
            .iter()
            .map(|row| {
                json!({
                    "ts": format_clickhouse_timestamp(row.ts),
                    "trading_day": format_clickhouse_date(row.trading_day),
                    "session_date": format_clickhouse_date(row.session_date),
                    "symbol_contract": row.symbol_contract,
                    "timeframe": row.timeframe,
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": row.volume,
                    "trade_count": row.trade_count,
                })
            })
            .collect::<Vec<_>>();
        self.insert_json_each_row("bars_time", &bar_rows).await
    }

    pub async fn clear_time_bars(
        &self,
        symbol_contract: &str,
        timeframe: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.client
            .query(
                "ALTER TABLE bars_time DELETE WHERE symbol_contract = ? AND timeframe = ? AND ts >= parseDateTime64BestEffort(?) AND ts <= parseDateTime64BestEffort(?)",
            )
            .bind(symbol_contract)
            .bind(timeframe)
            .bind(format_clickhouse_timestamp(start))
            .bind(format_clickhouse_timestamp(end))
            .execute()
            .await
            .context("failed to clear time bars")?;
        Ok(())
    }

    pub async fn replace_time_bars(
        &self,
        symbol_contract: &str,
        timeframe: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        rows: &[TimeBarRow],
    ) -> anyhow::Result<()> {
        self.clear_time_bars(symbol_contract, timeframe, start, end)
            .await?;
        self.insert_time_bars(rows).await
    }

    pub async fn insert_non_time_bars(&self, rows: &[NonTimeBarRow]) -> anyhow::Result<()> {
        let bar_rows = rows
            .iter()
            .map(|row| {
                json!({
                    "ts": format_clickhouse_timestamp(row.ts),
                    "trading_day": format_clickhouse_date(row.trading_day),
                    "session_date": format_clickhouse_date(row.session_date),
                    "symbol_contract": row.symbol_contract,
                    "bar_type": row.bar_type,
                    "bar_size": row.bar_size,
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": row.volume,
                    "trade_count": row.trade_count,
                })
            })
            .collect::<Vec<_>>();
        self.insert_json_each_row("bars_non_time", &bar_rows).await
    }

    pub async fn clear_non_time_bars(
        &self,
        symbol_contract: &str,
        bar_type: &str,
        bar_size: u32,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.client
            .query(
                "ALTER TABLE bars_non_time DELETE WHERE symbol_contract = ? AND bar_type = ? AND bar_size = ? AND ts >= parseDateTime64BestEffort(?) AND ts <= parseDateTime64BestEffort(?)",
            )
            .bind(symbol_contract)
            .bind(bar_type)
            .bind(bar_size)
            .bind(format_clickhouse_timestamp(start))
            .bind(format_clickhouse_timestamp(end))
            .execute()
            .await
            .context("failed to clear non-time bars")?;
        Ok(())
    }

    pub async fn replace_non_time_bars(
        &self,
        symbol_contract: &str,
        bar_type: &str,
        bar_size: u32,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        rows: &[NonTimeBarRow],
    ) -> anyhow::Result<()> {
        self.clear_non_time_bars(symbol_contract, bar_type, bar_size, start, end)
            .await?;
        self.insert_non_time_bars(rows).await
    }

    pub async fn clear_profiles_in_range(
        &self,
        symbol_contract: &str,
        profile_timezone: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let segment_ids = self
            .client
            .query(
                r#"
                SELECT segment_id
                FROM profile_segments
                WHERE symbol_contract = ?
                  AND profile_timezone = ?
                  AND segment_end >= parseDateTime64BestEffort(?)
                  AND segment_start <= parseDateTime64BestEffort(?)
                "#,
            )
            .bind(symbol_contract)
            .bind(profile_timezone)
            .bind(format_clickhouse_timestamp(start))
            .bind(format_clickhouse_timestamp(end))
            .fetch_all::<SegmentIdRow>()
            .await
            .context("failed to load overlapping profile segments")?;

        if !segment_ids.is_empty() {
            let placeholders = std::iter::repeat_n("?", segment_ids.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "ALTER TABLE profile_levels_base DELETE WHERE segment_id IN ({placeholders})"
            );
            let mut query = self.client.query(&sql);
            for segment_id in &segment_ids {
                query = query.bind(segment_id.segment_id);
            }
            query
                .execute()
                .await
                .context("failed to clear profile levels")?;
        }

        self.client
            .query(
                "ALTER TABLE profile_segments DELETE WHERE symbol_contract = ? AND profile_timezone = ? AND segment_end >= parseDateTime64BestEffort(?) AND segment_start <= parseDateTime64BestEffort(?)",
            )
            .bind(symbol_contract)
            .bind(profile_timezone)
            .bind(format_clickhouse_timestamp(start))
            .bind(format_clickhouse_timestamp(end))
            .execute()
            .await
            .context("failed to clear profile segments")?;

        Ok(())
    }

    pub async fn replace_profiles_in_range(
        &self,
        symbol_contract: &str,
        profile_timezone: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        profiles: &[PersistedProfile],
    ) -> anyhow::Result<()> {
        self.clear_profiles_in_range(symbol_contract, profile_timezone, start, end)
            .await?;

        if profiles.is_empty() {
            return Ok(());
        }

        let segments = profiles.iter().map(|profile| profile.segment.clone()).collect::<Vec<_>>();
        let levels = profiles
            .iter()
            .flat_map(|profile| profile.levels.clone())
            .collect::<Vec<_>>();

        let segment_rows = segments
            .iter()
            .map(|row| {
                json!({
                    "segment_id": row.segment_id,
                    "symbol_contract": row.symbol_contract,
                    "preset": row.preset,
                    "metric": row.metric,
                    "profile_timezone": row.profile_timezone,
                    "label": row.label,
                    "segment_start": format_clickhouse_timestamp(row.segment_start),
                    "segment_end": format_clickhouse_timestamp(row.segment_end),
                    "base_tick_size": row.base_tick_size,
                    "total_value": row.total_value,
                    "max_value": row.max_value,
                    "value_area_enabled": row.value_area_enabled,
                    "value_area_percent": row.value_area_percent,
                    "value_area_poc": row.value_area_poc,
                    "value_area_low": row.value_area_low,
                    "value_area_high": row.value_area_high,
                    "value_area_volume": row.value_area_volume,
                })
            })
            .collect::<Vec<_>>();
        let level_rows = levels
            .iter()
            .map(|row| {
                json!({
                    "segment_id": row.segment_id,
                    "symbol_contract": row.symbol_contract,
                    "price_level": row.price_level,
                    "total_volume": row.total_volume,
                    "buy_volume": row.buy_volume,
                    "sell_volume": row.sell_volume,
                    "delta": row.delta,
                })
            })
            .collect::<Vec<_>>();

        self.insert_json_each_row("profile_segments", &segment_rows)
            .await
            .context("failed to insert profile segments")?;
        self.insert_json_each_row("profile_levels_base", &level_rows)
            .await
            .context("failed to insert profile levels")?;
        Ok(())
    }

    pub async fn insert_large_orders(&self, rows: &[LargeOrderRow]) -> anyhow::Result<()> {
        let order_rows = rows
            .iter()
            .map(|row| {
                json!({
                    "ts": format_clickhouse_timestamp(row.ts),
                    "trading_day": format_clickhouse_date(row.trading_day),
                    "session_date": format_clickhouse_date(row.session_date),
                    "symbol_contract": row.symbol_contract,
                    "method": row.method,
                    "threshold": row.threshold,
                    "trade_price": row.trade_price,
                    "trade_size": row.trade_size,
                    "side": row.side,
                })
            })
            .collect::<Vec<_>>();
        self.insert_json_each_row("large_orders", &order_rows).await
    }

    async fn insert_json_each_row(&self, table: &str, rows: &[Value]) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut body = String::new();
        for row in rows {
            body.push_str(&serde_json::to_string(row).context("failed to serialize clickhouse row")?);
            body.push('\n');
        }

        let query = format!("INSERT INTO {table} FORMAT JSONEachRow");
        let response = self
            .http
            .post(format!("{}/", self.base_url))
            .query(&[
                ("database", self.database.as_str()),
                ("query", query.as_str()),
                ("async_insert", "0"),
                ("wait_for_async_insert", "0"),
            ])
            .body(body)
            .send()
            .await
            .context("failed to submit clickhouse insert request")?;

        let status = response.status();
        if !status.is_success() {
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| format!("clickhouse insert failed with status {status}"));
            anyhow::bail!("clickhouse insert failed: {message}");
        }

        Ok(())
    }

    pub async fn clear_large_orders(
        &self,
        symbol_contract: &str,
        method: &str,
        threshold: f64,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.client
            .query(
                "ALTER TABLE large_orders DELETE WHERE symbol_contract = ? AND method = ? AND threshold = ? AND ts >= parseDateTime64BestEffort(?) AND ts <= parseDateTime64BestEffort(?)",
            )
            .bind(symbol_contract)
            .bind(method)
            .bind(threshold)
            .bind(format_clickhouse_timestamp(start))
            .bind(format_clickhouse_timestamp(end))
            .execute()
            .await
            .context("failed to clear large orders")?;
        Ok(())
    }

    pub async fn replace_large_orders(
        &self,
        symbol_contract: &str,
        method: &str,
        threshold: f64,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        rows: &[LargeOrderRow],
    ) -> anyhow::Result<()> {
        self.clear_large_orders(symbol_contract, method, threshold, start, end)
            .await?;
        self.insert_large_orders(rows).await
    }
}

fn format_clickhouse_timestamp(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

fn format_clickhouse_date(value: chrono::NaiveDate) -> String {
    value.format("%Y-%m-%d").to_string()
}

#[derive(Clone, Debug, Deserialize)]
pub struct TicksQuery {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    #[serde(default = "default_ticks_limit")]
    pub limit: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BarsQuery {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub timeframe: String,
    #[serde(default = "default_bar_type")]
    pub bar_type: String,
    pub bar_size: Option<u32>,
    #[serde(default = "default_bars_limit")]
    pub limit: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PresetProfileQuery {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub preset: String,
    pub timezone: String,
    #[serde(default = "default_metric")]
    pub metric: String,
    #[serde(default = "default_tick_aggregation")]
    pub tick_aggregation: u32,
    #[serde(default)]
    pub value_area_enabled: bool,
    #[serde(default = "default_value_area_percent")]
    pub value_area_percent: f64,
    #[serde(default = "default_max_segments")]
    pub max_segments: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AreaProfileQuery {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub price_min: f64,
    pub price_max: f64,
    pub timezone: String,
    #[serde(default = "default_metric")]
    pub metric: String,
    pub tick_size: Option<f64>,
    #[serde(default = "default_tick_aggregation")]
    pub tick_aggregation: u32,
    #[serde(default)]
    pub value_area_enabled: bool,
    #[serde(default = "default_value_area_percent")]
    pub value_area_percent: f64,
    pub area_id: Option<Uuid>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct LargeOrdersQuery {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    #[serde(default = "default_large_orders_method")]
    pub method: String,
    #[serde(default = "default_large_orders_threshold")]
    pub fixed_threshold: f64,
    #[serde(default = "default_large_orders_limit")]
    pub limit: u32,
}

impl TicksQuery {
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.end < self.start {
            return Err(ApiError::bad_request("end must be after start"));
        }
        if self.limit == 0 || self.limit > MAX_TICKS_LIMIT {
            return Err(ApiError::bad_request(format!(
                "limit must be between 1 and {MAX_TICKS_LIMIT}"
            )));
        }
        Ok(())
    }
}

impl BarsQuery {
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.end < self.start {
            return Err(ApiError::bad_request("end must be after start"));
        }
        if self.limit == 0 || self.limit > MAX_BARS_LIMIT {
            return Err(ApiError::bad_request(format!(
                "limit must be between 1 and {MAX_BARS_LIMIT}"
            )));
        }
        match self.bar_type.as_str() {
            "time" => {
                timeframe_to_seconds(&self.timeframe).map_err(|error| {
                    ApiError::bad_request(format!("unsupported timeframe: {error}"))
                })?;
            }
            "tick" | "volume" | "range" => {
                if self.bar_size.unwrap_or(0) == 0 {
                    return Err(ApiError::bad_request(
                        "bar_size is required for non-time bars",
                    ));
                }
            }
            _ => {
                return Err(ApiError::bad_request(
                    "bar_type must be one of time, tick, volume, range",
                ))
            }
        }
        Ok(())
    }
}

impl PresetProfileQuery {
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.end < self.start {
            return Err(ApiError::bad_request("end must be after start"));
        }
        if !matches!(self.preset.as_str(), "day" | "week" | "rth" | "eth") {
            return Err(ApiError::bad_request(
                "preset must be one of day, week, rth, eth",
            ));
        }
        if !matches!(self.metric.as_str(), "volume" | "delta") {
            return Err(ApiError::bad_request("metric must be volume or delta"));
        }
        self.timezone
            .parse::<Tz>()
            .map_err(|_| ApiError::bad_request("timezone must be a valid IANA timezone"))?;
        if self.tick_aggregation == 0 {
            return Err(ApiError::bad_request(
                "tick_aggregation must be greater than 0",
            ));
        }
        if self.max_segments == 0 {
            return Err(ApiError::bad_request("max_segments must be greater than 0"));
        }
        Ok(())
    }
}

impl AreaProfileQuery {
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.end < self.start {
            return Err(ApiError::bad_request("end must be after start"));
        }
        if self.price_max < self.price_min {
            return Err(ApiError::bad_request("price_max must be >= price_min"));
        }
        if !matches!(self.metric.as_str(), "volume" | "delta") {
            return Err(ApiError::bad_request("metric must be volume or delta"));
        }
        self.timezone
            .parse::<Tz>()
            .map_err(|_| ApiError::bad_request("timezone must be a valid IANA timezone"))?;
        if self.tick_aggregation == 0 {
            return Err(ApiError::bad_request(
                "tick_aggregation must be greater than 0",
            ));
        }
        Ok(())
    }
}

impl LargeOrdersQuery {
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.end < self.start {
            return Err(ApiError::bad_request("end must be after start"));
        }
        if self.method != "fixed" {
            return Err(ApiError::bad_request("method must be fixed"));
        }
        if self.fixed_threshold <= 0.0 {
            return Err(ApiError::bad_request(
                "fixed_threshold must be greater than 0",
            ));
        }
        if self.limit == 0 || self.limit > MAX_LARGE_ORDERS_LIMIT {
            return Err(ApiError::bad_request(format!(
                "limit must be between 1 and {MAX_LARGE_ORDERS_LIMIT}"
            )));
        }
        Ok(())
    }
}

fn default_ticks_limit() -> u32 {
    DEFAULT_TICKS_LIMIT
}

fn default_bars_limit() -> u32 {
    DEFAULT_BARS_LIMIT
}

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct SymbolRecord {
    pub symbol_contract: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct TickRecord {
    pub ts: DateTime<Utc>,
    pub session_date: chrono::NaiveDate,
    pub symbol_contract: String,
    pub trade_price: f64,
    pub trade_size: f64,
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CanonicalTick {
    pub ts: DateTime<Utc>,
    pub trading_day: chrono::NaiveDate,
    pub session_date: chrono::NaiveDate,
    pub symbol_contract: String,
    pub trade_price: f64,
    pub trade_size: f64,
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
    pub source_file: Option<String>,
}

impl CanonicalTick {
    pub fn new(
        ts: DateTime<Utc>,
        symbol_contract: &str,
        trade_price: f64,
        trade_size: f64,
        bid_price: Option<f64>,
        ask_price: Option<f64>,
    ) -> Self {
        Self {
            ts,
            trading_day: ts.date_naive(),
            session_date: ts.date_naive(),
            symbol_contract: symbol_contract.to_string(),
            trade_price,
            trade_size,
            bid_price,
            ask_price,
            source_file: None,
        }
    }

    pub fn with_dataset_timezone(mut self, dataset_timezone: Tz) -> Self {
        self.session_date = self.ts.with_timezone(&dataset_timezone).date_naive();
        self
    }

    pub fn with_source_file(mut self, source_file: impl Into<String>) -> Self {
        self.source_file = Some(source_file.into());
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct CanonicalTickRow {
    pub ts: DateTime<Utc>,
    pub trading_day: chrono::NaiveDate,
    pub session_date: chrono::NaiveDate,
    pub symbol_contract: String,
    pub trade_price: f64,
    pub trade_size: f64,
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
    pub source_file: String,
}

impl From<CanonicalTick> for CanonicalTickRow {
    fn from(value: CanonicalTick) -> Self {
        Self {
            ts: value.ts,
            trading_day: value.trading_day,
            session_date: value.session_date,
            symbol_contract: value.symbol_contract,
            trade_price: value.trade_price,
            trade_size: value.trade_size,
            bid_price: value.bid_price,
            ask_price: value.ask_price,
            source_file: value
                .source_file
                .unwrap_or_else(|| "unknown.csv".to_string()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct BarRecord {
    pub ts: DateTime<Utc>,
    pub session_date: chrono::NaiveDate,
    pub symbol_contract: String,
    pub timeframe: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub trade_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct LargeOrderRecord {
    pub ts: DateTime<Utc>,
    pub session_date: chrono::NaiveDate,
    pub symbol_contract: String,
    pub method: String,
    pub threshold: f64,
    pub trade_price: f64,
    pub trade_size: f64,
    pub side: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct LargeOrderRow {
    pub ts: DateTime<Utc>,
    pub trading_day: chrono::NaiveDate,
    pub session_date: chrono::NaiveDate,
    pub symbol_contract: String,
    pub method: String,
    pub threshold: f64,
    pub trade_price: f64,
    pub trade_size: f64,
    pub side: String,
}

#[derive(Clone, Debug)]
pub enum TradingBarKind {
    Time(String),
    NonTime(String, u32),
}

#[derive(Clone, Debug)]
pub struct TradingBar {
    pub ts: DateTime<Utc>,
    pub trading_day: chrono::NaiveDate,
    pub session_date: chrono::NaiveDate,
    pub symbol_contract: String,
    pub kind: TradingBarKind,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub trade_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct TimeBarRow {
    pub ts: DateTime<Utc>,
    pub trading_day: chrono::NaiveDate,
    pub session_date: chrono::NaiveDate,
    pub symbol_contract: String,
    pub timeframe: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub trade_count: u64,
}

impl TimeBarRow {
    pub fn from_bar(value: TradingBar) -> Self {
        let timeframe = match value.kind {
            TradingBarKind::Time(timeframe) => timeframe,
            TradingBarKind::NonTime(bar_type, bar_size) => format!("{bar_type}:{bar_size}"),
        };
        Self {
            ts: value.ts,
            trading_day: value.trading_day,
            session_date: value.session_date,
            symbol_contract: value.symbol_contract,
            timeframe,
            open: value.open,
            high: value.high,
            low: value.low,
            close: value.close,
            volume: value.volume,
            trade_count: value.trade_count,
        }
    }
}

impl From<TimeBarRow> for BarRecord {
    fn from(value: TimeBarRow) -> Self {
        Self {
            ts: value.ts,
            session_date: value.session_date,
            symbol_contract: value.symbol_contract,
            timeframe: value.timeframe,
            open: value.open,
            high: value.high,
            low: value.low,
            close: value.close,
            volume: value.volume,
            trade_count: value.trade_count,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct NonTimeBarRow {
    pub ts: DateTime<Utc>,
    pub trading_day: chrono::NaiveDate,
    pub session_date: chrono::NaiveDate,
    pub symbol_contract: String,
    pub bar_type: String,
    pub bar_size: u32,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub trade_count: u64,
}

impl NonTimeBarRow {
    pub fn from_bar(value: TradingBar) -> Self {
        let (bar_type, bar_size) = match value.kind {
            TradingBarKind::Time(timeframe) => {
                ("time".to_string(), timeframe.parse::<u32>().unwrap_or(0))
            }
            TradingBarKind::NonTime(bar_type, bar_size) => (bar_type, bar_size),
        };
        Self {
            ts: value.ts,
            trading_day: value.trading_day,
            session_date: value.session_date,
            symbol_contract: value.symbol_contract,
            bar_type,
            bar_size,
            open: value.open,
            high: value.high,
            low: value.low,
            close: value.close,
            volume: value.volume,
            trade_count: value.trade_count,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct ProfileSegmentRow {
    pub segment_id: Uuid,
    pub symbol_contract: String,
    pub preset: String,
    pub metric: String,
    pub profile_timezone: String,
    pub label: String,
    pub segment_start: DateTime<Utc>,
    pub segment_end: DateTime<Utc>,
    pub base_tick_size: f64,
    pub total_value: f64,
    pub max_value: f64,
    pub value_area_enabled: bool,
    pub value_area_percent: f64,
    pub value_area_poc: Option<f64>,
    pub value_area_low: Option<f64>,
    pub value_area_high: Option<f64>,
    pub value_area_volume: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Row)]
pub struct ProfileLevelBaseRow {
    pub segment_id: Uuid,
    pub symbol_contract: String,
    pub price_level: f64,
    pub total_volume: f64,
    pub buy_volume: f64,
    pub sell_volume: f64,
    pub delta: f64,
}

#[derive(Clone, Debug, Deserialize, Row)]
struct SegmentIdRow {
    segment_id: Uuid,
}

#[derive(Clone, Debug, Serialize)]
pub struct PresetProfilesResponse {
    pub symbol_contract: String,
    pub timezone: String,
    pub preset: String,
    pub metric: String,
    pub tick_aggregation: u32,
    pub profiles: Vec<ProfilePayload>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AreaProfileResponse {
    pub symbol_contract: String,
    pub timezone: String,
    pub metric: String,
    pub tick_aggregation: u32,
    pub profile: ProfilePayload,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProfilePayload {
    pub id: Uuid,
    pub label: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub max_value: f64,
    pub total_value: f64,
    pub value_area_enabled: bool,
    pub value_area_percent: f64,
    pub value_area_poc: Option<f64>,
    pub value_area_low: Option<f64>,
    pub value_area_high: Option<f64>,
    pub value_area_volume: f64,
    pub levels: Vec<ProfileLevelValue>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProfileLevelValue {
    pub price_level: f64,
    pub value: f64,
    pub volume: f64,
}

#[derive(Clone, Debug)]
pub struct ValueAreaSummary {
    pub enabled: bool,
    pub percent: f64,
    pub poc: Option<f64>,
    pub low: Option<f64>,
    pub high: Option<f64>,
    pub volume: f64,
}

#[derive(Clone, Debug)]
pub struct PersistedProfile {
    pub segment: ProfileSegmentRow,
    pub levels: Vec<ProfileLevelBaseRow>,
}

impl ValueAreaSummary {
    pub fn disabled(percent: f64) -> Self {
        Self {
            enabled: false,
            percent,
            poc: None,
            low: None,
            high: None,
            volume: 0.0,
        }
    }
}

fn aggregate_levels(
    levels: &[ProfileLevelBaseRow],
    tick_aggregation: u32,
    base_tick_size: f64,
    metric: &str,
) -> Vec<ProfileLevelValue> {
    let mut grouped: BTreeMap<i64, f64> = BTreeMap::new();
    let bucket_size = base_tick_size * tick_aggregation.max(1) as f64;

    for row in levels {
        let bucket = bucket_price(row.price_level, bucket_size);
        let key = (bucket * 10_000.0).round() as i64;
        let value = if metric == "delta" {
            row.delta
        } else {
            row.total_volume
        };
        grouped
            .entry(key)
            .and_modify(|existing| *existing += value)
            .or_insert(value);
    }

    grouped
        .into_iter()
        .map(|(key, value)| ProfileLevelValue {
            price_level: key as f64 / 10_000.0,
            value,
            volume: value,
        })
        .collect()
}

pub fn build_large_orders_from_ticks(
    symbol_contract: &str,
    ticks: &[CanonicalTick],
    method: &str,
    fixed_threshold: f64,
) -> Vec<LargeOrderRow> {
    if method != "fixed" || fixed_threshold <= 0.0 {
        return Vec::new();
    }

    ticks
        .iter()
        .filter(|tick| tick.trade_size >= fixed_threshold)
        .map(|tick| LargeOrderRow {
            ts: tick.ts,
            trading_day: tick.trading_day,
            session_date: tick.session_date,
            symbol_contract: symbol_contract.to_string(),
            method: "fixed".to_string(),
            threshold: fixed_threshold,
            trade_price: tick.trade_price,
            trade_size: tick.trade_size,
            side: match classify_side(tick.trade_price, tick.bid_price, tick.ask_price) {
                side if side > 0.0 => "buy".to_string(),
                side if side < 0.0 => "sell".to_string(),
                _ => "unknown".to_string(),
            },
        })
        .collect()
}

fn default_bar_type() -> String {
    "time".to_string()
}

fn default_large_orders_method() -> String {
    "fixed".to_string()
}

fn default_large_orders_threshold() -> f64 {
    25.0
}

fn default_large_orders_limit() -> u32 {
    DEFAULT_LARGE_ORDERS_LIMIT
}

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize, Row)]
struct ClickHousePingRow {
    value: u8,
}

fn default_metric() -> String {
    "volume".to_string()
}

fn default_tick_aggregation() -> u32 {
    1
}

fn default_value_area_percent() -> f64 {
    70.0
}

fn default_max_segments() -> u32 {
    120
}

fn map_clickhouse_err(error: ClickHouseError) -> ApiError {
    ApiError::internal(format!("clickhouse query failed: {error}"))
}

pub fn derive_symbol_root(symbol_contract: &str) -> String {
    symbol_contract
        .chars()
        .take_while(|value| value.is_ascii_alphabetic())
        .collect::<String>()
}

pub fn detect_tick_size(symbol_contract: &str) -> f64 {
    for (prefix, tick_size) in [
        ("MNQ", 0.25),
        ("MES", 0.25),
        ("NQ", 0.25),
        ("ES", 0.25),
        ("RTY", 0.1),
        ("CL", 0.01),
        ("GC", 0.1),
    ] {
        if symbol_contract.starts_with(prefix) {
            return tick_size;
        }
    }
    0.25
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

pub fn build_profiles_for_ticks(
    symbol_contract: &str,
    ticks: &[CanonicalTick],
    profile_timezone: Tz,
    tick_size: f64,
) -> Vec<PersistedProfile> {
    let mut profiles = Vec::new();
    for preset in ["day", "week", "rth", "eth"] {
        for metric in ["volume", "delta"] {
            profiles.extend(build_preset_profiles(
                symbol_contract,
                ticks,
                preset,
                metric,
                profile_timezone,
                tick_size,
                metric == "volume",
                70.0,
            ));
        }
    }
    profiles
}

#[cfg(test)]
mod tests {
    use super::{build_large_orders_from_ticks, BarsQuery, CanonicalTick, LargeOrdersQuery, TicksQuery};
    use chrono::{TimeZone, Utc};

    fn sample_ticks() -> Vec<CanonicalTick> {
        vec![
            CanonicalTick {
                ts: Utc.with_ymd_and_hms(2026, 3, 1, 14, 30, 0).unwrap(),
                trading_day: Utc
                    .with_ymd_and_hms(2026, 3, 1, 14, 30, 0)
                    .unwrap()
                    .date_naive(),
                session_date: Utc
                    .with_ymd_and_hms(2026, 3, 1, 14, 30, 0)
                    .unwrap()
                    .date_naive(),
                symbol_contract: "NQH6".to_string(),
                trade_price: 100.0,
                trade_size: 10.0,
                bid_price: Some(99.75),
                ask_price: Some(100.0),
                source_file: None,
            },
            CanonicalTick {
                ts: Utc.with_ymd_and_hms(2026, 3, 1, 14, 30, 1).unwrap(),
                trading_day: Utc
                    .with_ymd_and_hms(2026, 3, 1, 14, 30, 1)
                    .unwrap()
                    .date_naive(),
                session_date: Utc
                    .with_ymd_and_hms(2026, 3, 1, 14, 30, 1)
                    .unwrap()
                    .date_naive(),
                symbol_contract: "NQH6".to_string(),
                trade_price: 99.5,
                trade_size: 30.0,
                bid_price: Some(99.5),
                ask_price: Some(99.75),
                source_file: None,
            },
            CanonicalTick {
                ts: Utc.with_ymd_and_hms(2026, 3, 1, 14, 30, 2).unwrap(),
                trading_day: Utc
                    .with_ymd_and_hms(2026, 3, 1, 14, 30, 2)
                    .unwrap()
                    .date_naive(),
                session_date: Utc
                    .with_ymd_and_hms(2026, 3, 1, 14, 30, 2)
                    .unwrap()
                    .date_naive(),
                symbol_contract: "NQH6".to_string(),
                trade_price: 99.625,
                trade_size: 40.0,
                bid_price: Some(99.5),
                ask_price: Some(99.75),
                source_file: None,
            },
        ]
    }

    #[test]
    fn builds_fixed_large_orders_and_classifies_side() {
        let orders = build_large_orders_from_ticks("NQH6", &sample_ticks(), "fixed", 25.0);
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0].trade_size, 30.0);
        assert_eq!(orders[0].side, "sell");
        assert_eq!(orders[1].trade_size, 40.0);
        assert_eq!(orders[1].side, "unknown");
        assert!(orders.iter().all(|row| row.threshold == 25.0));
        assert!(orders.iter().all(|row| row.method == "fixed"));
    }

    #[test]
    fn validates_large_orders_queries() {
        let valid = LargeOrdersQuery {
            start: Utc.with_ymd_and_hms(2026, 3, 1, 14, 30, 0).unwrap(),
            end: Utc.with_ymd_and_hms(2026, 3, 1, 15, 30, 0).unwrap(),
            method: "fixed".to_string(),
            fixed_threshold: 25.0,
            limit: 500,
        };
        assert!(valid.validate().is_ok());

        let invalid_method = LargeOrdersQuery {
            method: "relative".to_string(),
            ..valid.clone()
        };
        assert!(invalid_method
            .validate()
            .unwrap_err()
            .to_string()
            .contains("method must be fixed"));

        let invalid_threshold = LargeOrdersQuery {
            fixed_threshold: 0.0,
            ..valid
        };
        assert!(invalid_threshold
            .validate()
            .unwrap_err()
            .to_string()
            .contains("fixed_threshold"));
    }

    #[test]
    fn validates_tick_query_limits() {
        let valid = TicksQuery {
            start: Utc.with_ymd_and_hms(2026, 3, 1, 14, 30, 0).unwrap(),
            end: Utc.with_ymd_and_hms(2026, 3, 1, 15, 30, 0).unwrap(),
            limit: 1000,
        };
        assert!(valid.validate().is_ok());

        let invalid = TicksQuery { limit: 0, ..valid };
        assert!(invalid.validate().unwrap_err().to_string().contains("limit must be between"));
    }

    #[test]
    fn validates_bar_query_limits() {
        let valid = BarsQuery {
            start: Utc.with_ymd_and_hms(2026, 3, 1, 14, 30, 0).unwrap(),
            end: Utc.with_ymd_and_hms(2026, 3, 1, 15, 30, 0).unwrap(),
            timeframe: "1m".to_string(),
            bar_type: "time".to_string(),
            bar_size: None,
            limit: 1000,
        };
        assert!(valid.validate().is_ok());

        let invalid = BarsQuery { limit: 25_000, ..valid };
        assert!(invalid.validate().unwrap_err().to_string().contains("limit must be between"));
    }
}
