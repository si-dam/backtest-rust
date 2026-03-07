use std::{cmp::Ordering, collections::BTreeMap};

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Timelike, Utc};
use chrono_tz::{America::New_York, Tz};
use uuid::Uuid;

use super::{CanonicalTick, PersistedProfile, ProfileLevelBaseRow, ProfileLevelValue, ProfileSegmentRow, ValueAreaSummary};

pub fn bucket_price(price: f64, bucket_size: f64) -> f64 {
    if bucket_size <= 0.0 {
        return price;
    }
    (price / bucket_size).floor() * bucket_size
}

pub fn classify_side(trade_price: f64, bid_price: Option<f64>, ask_price: Option<f64>) -> f64 {
    match (bid_price, ask_price) {
        (Some(bid), Some(ask)) if ask > bid => {
            let midpoint = bid + ((ask - bid) / 2.0);
            if trade_price >= ask {
                1.0
            } else if trade_price <= bid {
                -1.0
            } else if trade_price > midpoint {
                1.0
            } else if trade_price < midpoint {
                -1.0
            } else {
                0.0
            }
        }
        _ => 0.0,
    }
}

pub fn build_value_area(levels: &[ProfileLevelValue], percent: f64) -> ValueAreaSummary {
    if levels.is_empty() {
        return ValueAreaSummary::disabled(percent);
    }

    let total_volume = levels.iter().map(|row| row.value.max(0.0)).sum::<f64>();
    if total_volume <= 0.0 {
        return ValueAreaSummary::disabled(percent);
    }

    let target = total_volume * (percent / 100.0);
    let poc_index = levels
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| left.value.partial_cmp(&right.value).unwrap_or(Ordering::Equal))
        .map(|(index, _)| index)
        .unwrap_or(0);

    let mut low = poc_index;
    let mut high = poc_index;
    let mut collected = levels[poc_index].value.max(0.0);

    while collected < target && (low > 0 || high + 1 < levels.len()) {
        let left_index = low.checked_sub(1);
        let right_index = (high + 1 < levels.len()).then_some(high + 1);
        let left_value = left_index.map(|index| levels[index].value).unwrap_or(-1.0);
        let right_value = right_index.map(|index| levels[index].value).unwrap_or(-1.0);

        if right_value > left_value {
            if let Some(index) = right_index {
                high = index;
                collected += levels[index].value.max(0.0);
            }
        } else if let Some(index) = left_index {
            low = index;
            collected += levels[index].value.max(0.0);
        } else if let Some(index) = right_index {
            high = index;
            collected += levels[index].value.max(0.0);
        } else {
            break;
        }
    }

    ValueAreaSummary {
        enabled: true,
        percent,
        poc: Some(levels[poc_index].price_level),
        low: Some(levels[low].price_level),
        high: Some(levels[high].price_level),
        volume: collected,
    }
}

pub fn build_base_profile_levels(segment_id: Uuid, symbol_contract: &str, ticks: &[CanonicalTick], tick_size: f64) -> Vec<ProfileLevelBaseRow> {
    let mut grouped: BTreeMap<i64, (f64, f64, f64)> = BTreeMap::new();

    for tick in ticks {
        let level = bucket_price(tick.trade_price, tick_size);
        let key = (level * 10_000.0).round() as i64;
        let side = classify_side(tick.trade_price, tick.bid_price, tick.ask_price);
        let buy_volume = if side > 0.0 { tick.trade_size } else { 0.0 };
        let sell_volume = if side < 0.0 { tick.trade_size } else { 0.0 };
        grouped
            .entry(key)
            .and_modify(|entry| {
                entry.0 += tick.trade_size;
                entry.1 += buy_volume;
                entry.2 += sell_volume;
            })
            .or_insert((tick.trade_size, buy_volume, sell_volume));

        if side == 0.0 {
            grouped.entry(key).and_modify(|entry| entry.0 += 0.0);
        }
    }

    grouped
        .into_iter()
        .map(|(key, (total_volume, buy_volume, sell_volume))| ProfileLevelBaseRow {
            segment_id,
            symbol_contract: symbol_contract.to_string(),
            price_level: key as f64 / 10_000.0,
            total_volume,
            buy_volume,
            sell_volume,
            delta: buy_volume - sell_volume,
        })
        .collect()
}

pub fn build_preset_profiles(
    symbol_contract: &str,
    ticks: &[CanonicalTick],
    preset: &str,
    metric: &str,
    profile_timezone: Tz,
    tick_size: f64,
    value_area_enabled: bool,
    value_area_percent: f64,
) -> Vec<PersistedProfile> {
    let grouped = match preset {
        "day" => group_by_session_day(ticks),
        "week" => group_by_week(ticks),
        "rth" => group_by_rth(ticks, true),
        "eth" => group_by_rth(ticks, false),
        _ => return Vec::new(),
    };

    grouped
        .into_iter()
        .map(|(label, segment_start, segment_end, segment_ticks)| {
            let segment_id = Uuid::new_v4();
            let levels = build_base_profile_levels(segment_id, symbol_contract, &segment_ticks, tick_size);
            let response_levels = levels
                .iter()
                .map(|level| ProfileLevelValue {
                    price_level: level.price_level,
                    value: if metric == "delta" { level.delta } else { level.total_volume },
                    volume: level.total_volume,
                })
                .collect::<Vec<_>>();
            let value_area = if value_area_enabled && metric == "volume" {
                build_value_area(&response_levels, value_area_percent)
            } else {
                ValueAreaSummary::disabled(value_area_percent)
            };

            PersistedProfile {
                segment: ProfileSegmentRow {
                    segment_id,
                    symbol_contract: symbol_contract.to_string(),
                    preset: preset.to_string(),
                    metric: metric.to_string(),
                    profile_timezone: profile_timezone.name().to_string(),
                    label,
                    segment_start,
                    segment_end,
                    base_tick_size: tick_size,
                    total_value: response_levels.iter().map(|row| row.value).sum(),
                    max_value: response_levels.iter().map(|row| row.value.abs()).fold(0.0, f64::max),
                    value_area_enabled: value_area.enabled,
                    value_area_percent: value_area.percent,
                    value_area_poc: value_area.poc,
                    value_area_low: value_area.low,
                    value_area_high: value_area.high,
                    value_area_volume: value_area.volume,
                },
                levels,
            }
        })
        .collect()
}

fn group_by_session_day(ticks: &[CanonicalTick]) -> Vec<(String, DateTime<Utc>, DateTime<Utc>, Vec<CanonicalTick>)> {
    let mut grouped: BTreeMap<DateTime<Tz>, Vec<CanonicalTick>> = BTreeMap::new();

    for tick in ticks {
        let exchange = tick.ts.with_timezone(&New_York);
        let session_start = session_start_1600(exchange);
        grouped.entry(session_start).or_default().push(tick.clone());
    }

    grouped
        .into_iter()
        .map(|(session_start, rows)| {
            let label = format!("Day {}", session_start.date_naive());
            let segment_end = session_start + Duration::days(1);
            (label, session_start.with_timezone(&Utc), segment_end.with_timezone(&Utc), rows)
        })
        .collect()
}

fn group_by_week(ticks: &[CanonicalTick]) -> Vec<(String, DateTime<Utc>, DateTime<Utc>, Vec<CanonicalTick>)> {
    let mut grouped: BTreeMap<(i32, u32), Vec<CanonicalTick>> = BTreeMap::new();
    for tick in ticks {
        let exchange = tick.ts.with_timezone(&New_York);
        let week = exchange.iso_week();
        grouped.entry((week.year(), week.week())).or_default().push(tick.clone());
    }

    grouped
        .into_iter()
        .map(|((year, week), rows)| {
            let first = rows.first().expect("week rows exist").ts;
            let last = rows.last().expect("week rows exist").ts;
            (format!("Week {year}-W{week:02}"), first, last, rows)
        })
        .collect()
}

fn group_by_rth(
    ticks: &[CanonicalTick],
    include_rth: bool,
) -> Vec<(String, DateTime<Utc>, DateTime<Utc>, Vec<CanonicalTick>)> {
    let mut grouped: BTreeMap<NaiveDate, Vec<CanonicalTick>> = BTreeMap::new();

    for tick in ticks {
        let exchange = tick.ts.with_timezone(&New_York);
        let is_rth = is_rth(exchange);
        if is_rth != include_rth {
            continue;
        }
        let session_day = if exchange.time().hour() < 9 || (exchange.time().hour() == 9 && exchange.time().minute() < 30) {
            exchange.date_naive().pred_opt().unwrap_or(exchange.date_naive())
        } else {
            exchange.date_naive()
        };
        grouped.entry(session_day).or_default().push(tick.clone());
    }

    grouped
        .into_iter()
        .map(|(day, rows)| {
            let first = rows.first().expect("rows exist").ts;
            let last = rows.last().expect("rows exist").ts;
            let prefix = if include_rth { "RTH" } else { "ETH" };
            (format!("{prefix} {day}"), first, last, rows)
        })
        .collect()
}

fn is_rth(timestamp: DateTime<Tz>) -> bool {
    let hour = timestamp.time().hour();
    let minute = timestamp.time().minute();
    ((hour > 9) || (hour == 9 && minute >= 30)) && hour < 16
}

fn session_start_1600(timestamp: DateTime<Tz>) -> DateTime<Tz> {
    let date = if timestamp.hour() < 16 {
        timestamp.date_naive().pred_opt().unwrap_or(timestamp.date_naive())
    } else {
        timestamp.date_naive()
    };
    New_York
        .with_ymd_and_hms(date.year(), date.month(), date.day(), 16, 0, 0)
        .single()
        .expect("valid session start")
}
