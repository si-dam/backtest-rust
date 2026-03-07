mod profiles;

use std::collections::BTreeMap;

use app_core::error::ApiError;
use chrono::{DateTime, Utc};
use clickhouse::{error::Error as ClickHouseError, Client, Row};
use profiles::{build_value_area, bucket_price, classify_side};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone)]
pub struct ClickHouseMarketStore {
    client: Client,
}

impl ClickHouseMarketStore {
    pub fn new(base_url: &str, database: &str) -> Self {
        let client = Client::default().with_url(base_url).with_database(database);
        Self { client }
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

    pub async fn load_ticks(&self, symbol: &str, query: &TicksQuery) -> Result<Vec<TickRecord>, ApiError> {
        self.client
            .query(
                r#"
                SELECT ts, session_date, symbol_contract, trade_price, trade_size, bid_price, ask_price
                FROM ticks
                WHERE symbol_contract = ?
                  AND ts >= ?
                  AND ts <= ?
                ORDER BY ts
                "#,
            )
            .bind(symbol)
            .bind(query.start)
            .bind(query.end)
            .fetch_all::<TickRecord>()
            .await
            .map_err(map_clickhouse_err)
    }

    pub async fn load_bars(&self, symbol: &str, query: &BarsQuery) -> Result<Vec<BarRecord>, ApiError> {
        if query.bar_type == "time" {
            return self
                .client
                .query(
                    r#"
                    SELECT ts, session_date, symbol_contract, timeframe, open, high, low, close, volume, trade_count
                    FROM bars_time
                    WHERE symbol_contract = ?
                      AND timeframe = ?
                      AND ts >= ?
                      AND ts <= ?
                    ORDER BY ts
                    "#,
                )
                .bind(symbol)
                .bind(query.timeframe.clone())
                .bind(query.start)
                .bind(query.end)
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
                  AND ts >= ?
                  AND ts <= ?
                ORDER BY ts
                "#,
            )
            .bind(symbol)
            .bind(query.bar_type.clone())
            .bind(size)
            .bind(query.start)
            .bind(query.end)
            .fetch_all::<BarRecord>()
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
                  AND segment_end >= ?
                  AND segment_start <= ?
                ORDER BY segment_end
                LIMIT ?
                "#,
            )
            .bind(symbol)
            .bind(query.preset.clone())
            .bind(query.metric.clone())
            .bind(query.timezone.clone())
            .bind(query.start)
            .bind(query.end)
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
            let grouped = aggregate_levels(&levels, aggregation, segment.base_tick_size, &query.metric);
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
                max_value: grouped.iter().map(|row| row.value.abs()).fold(0.0, f64::max),
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
        let ticks = self.load_ticks(
            symbol,
            &TicksQuery {
                start: query.start,
                end: query.end,
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
            values.entry(key).and_modify(|entry| *entry += value).or_insert(value);
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

    async fn load_segment_levels(&self, segment_id: Uuid) -> Result<Vec<ProfileLevelBaseRow>, ApiError> {
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
}

#[derive(Clone, Debug, Deserialize)]
pub struct TicksQuery {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BarsQuery {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub timeframe: String,
    #[serde(default = "default_bar_type")]
    pub bar_type: String,
    pub bar_size: Option<u32>,
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

impl BarsQuery {
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.end < self.start {
            return Err(ApiError::bad_request("end must be after start"));
        }
        if self.bar_type != "time" && self.bar_size.is_none() {
            return Err(ApiError::bad_request("bar_size is required for non-time bars"));
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
            return Err(ApiError::bad_request("preset must be one of day, week, rth, eth"));
        }
        if !matches!(self.metric.as_str(), "volume" | "delta") {
            return Err(ApiError::bad_request("metric must be volume or delta"));
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
        Ok(())
    }
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
        let value = if metric == "delta" { row.delta } else { row.total_volume };
        grouped.entry(key).and_modify(|existing| *existing += value).or_insert(value);
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

fn default_bar_type() -> String {
    "time".to_string()
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
