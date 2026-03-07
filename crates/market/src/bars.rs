use std::collections::BTreeMap;

use chrono::{DateTime, Duration, TimeZone, Utc};
use chrono_tz::Tz;

use crate::{BarRecord, CanonicalTick, NonTimeBarRow, TimeBarRow, TradingBar, TradingBarKind};

pub fn build_time_bars_from_ticks(
    ticks: &[CanonicalTick],
    symbol_contract: &str,
    timeframe: &str,
    dataset_timezone: Tz,
) -> anyhow::Result<Vec<TimeBarRow>> {
    let bucket_seconds = timeframe_to_seconds(timeframe)?;
    let mut buckets: BTreeMap<i64, Vec<&CanonicalTick>> = BTreeMap::new();

    for tick in ticks {
        let bucket = tick.ts.timestamp().div_euclid(bucket_seconds) * bucket_seconds;
        buckets.entry(bucket).or_default().push(tick);
    }

    let mut bars = Vec::with_capacity(buckets.len());
    for (bucket, grouped_ticks) in buckets {
        if grouped_ticks.is_empty() {
            continue;
        }
        let ts = Utc.timestamp_opt(bucket, 0).single().expect("valid timestamp");
        let bar = build_bar(symbol_contract, TradingBarKind::Time(timeframe.to_string()), ts, grouped_ticks, dataset_timezone);
        bars.push(TimeBarRow::from_bar(bar));
    }

    Ok(bars)
}

pub fn build_non_time_bars_from_ticks(
    ticks: &[CanonicalTick],
    symbol_contract: &str,
    bar_type: &str,
    bar_size: u32,
    tick_size: f64,
    dataset_timezone: Tz,
) -> anyhow::Result<Vec<NonTimeBarRow>> {
    if bar_size == 0 {
        anyhow::bail!("bar_size must be greater than 0");
    }

    match bar_type {
        "tick" => Ok(build_tick_bars(ticks, symbol_contract, bar_size, dataset_timezone)),
        "volume" => Ok(build_volume_bars(ticks, symbol_contract, bar_size, dataset_timezone)),
        "range" => Ok(build_range_bars(ticks, symbol_contract, bar_size, tick_size, dataset_timezone)),
        other => anyhow::bail!("unsupported bar type: {other}"),
    }
}

pub fn build_bar_records(rows: &[TimeBarRow]) -> Vec<BarRecord> {
    rows.iter().cloned().map(BarRecord::from).collect()
}

fn build_tick_bars(
    ticks: &[CanonicalTick],
    symbol_contract: &str,
    bar_size: u32,
    dataset_timezone: Tz,
) -> Vec<NonTimeBarRow> {
    ticks.chunks(bar_size as usize)
        .filter(|chunk| !chunk.is_empty())
        .map(|chunk| {
            let ts = chunk.last().expect("chunk has value").ts;
            let refs = chunk.iter().collect::<Vec<_>>();
            let bar = build_bar(symbol_contract, TradingBarKind::NonTime("tick".to_string(), bar_size), ts, refs, dataset_timezone);
            NonTimeBarRow::from_bar(bar)
        })
        .collect()
}

fn build_volume_bars(
    ticks: &[CanonicalTick],
    symbol_contract: &str,
    bar_size: u32,
    dataset_timezone: Tz,
) -> Vec<NonTimeBarRow> {
    let mut rows = Vec::new();
    let mut window: Vec<&CanonicalTick> = Vec::new();
    let mut volume = 0.0_f64;

    for tick in ticks {
        volume += tick.trade_size;
        window.push(tick);
        if volume >= bar_size as f64 {
            let ts = window.last().expect("window has value").ts;
            let bar = build_bar(symbol_contract, TradingBarKind::NonTime("volume".to_string(), bar_size), ts, window.clone(), dataset_timezone);
            rows.push(NonTimeBarRow::from_bar(bar));
            window.clear();
            volume = 0.0;
        }
    }

    if !window.is_empty() {
        let ts = window.last().expect("window has value").ts;
        let bar = build_bar(symbol_contract, TradingBarKind::NonTime("volume".to_string(), bar_size), ts, window, dataset_timezone);
        rows.push(NonTimeBarRow::from_bar(bar));
    }

    rows
}

fn build_range_bars(
    ticks: &[CanonicalTick],
    symbol_contract: &str,
    bar_size: u32,
    tick_size: f64,
    dataset_timezone: Tz,
) -> Vec<NonTimeBarRow> {
    let mut rows = Vec::new();
    let mut window: Vec<&CanonicalTick> = Vec::new();
    let target_span = tick_size * bar_size as f64;

    for tick in ticks {
        window.push(tick);
        let open = window.first().expect("window has value").trade_price;
        let high = window.iter().map(|item| item.trade_price).fold(f64::MIN, f64::max);
        let low = window.iter().map(|item| item.trade_price).fold(f64::MAX, f64::min);
        if (high - open) >= target_span || (open - low) >= target_span {
            let ts = window.last().expect("window has value").ts;
            let bar = build_bar(symbol_contract, TradingBarKind::NonTime("range".to_string(), bar_size), ts, window.clone(), dataset_timezone);
            rows.push(NonTimeBarRow::from_bar(bar));
            window.clear();
        }
    }

    if !window.is_empty() {
        let ts = window.last().expect("window has value").ts;
        let bar = build_bar(symbol_contract, TradingBarKind::NonTime("range".to_string(), bar_size), ts, window, dataset_timezone);
        rows.push(NonTimeBarRow::from_bar(bar));
    }

    rows
}

fn build_bar(
    symbol_contract: &str,
    kind: TradingBarKind,
    ts: DateTime<Utc>,
    ticks: Vec<&CanonicalTick>,
    dataset_timezone: Tz,
) -> TradingBar {
    let open = ticks.first().expect("ticks must not be empty").trade_price;
    let close = ticks.last().expect("ticks must not be empty").trade_price;
    let high = ticks.iter().map(|item| item.trade_price).fold(f64::MIN, f64::max);
    let low = ticks.iter().map(|item| item.trade_price).fold(f64::MAX, f64::min);
    let volume = ticks.iter().map(|item| item.trade_size).sum::<f64>();
    let trade_count = ticks.len() as u64;
    let localized = ts.with_timezone(&dataset_timezone);

    TradingBar {
        ts,
        trading_day: ts.date_naive(),
        session_date: localized.date_naive(),
        symbol_contract: symbol_contract.to_string(),
        kind,
        open,
        high,
        low,
        close,
        volume,
        trade_count,
    }
}

pub fn timeframe_to_seconds(timeframe: &str) -> anyhow::Result<i64> {
    let normalized = timeframe.trim().to_lowercase();
    if let Some(value) = normalized.strip_suffix('s') {
        return Ok(value.parse::<i64>()?);
    }
    if let Some(value) = normalized.strip_suffix('m') {
        return Ok(value.parse::<i64>()? * Duration::minutes(1).num_seconds());
    }
    if let Some(value) = normalized.strip_suffix('h') {
        return Ok(value.parse::<i64>()? * Duration::hours(1).num_seconds());
    }
    if let Some(value) = normalized.strip_suffix('d') {
        return Ok(value.parse::<i64>()? * Duration::days(1).num_seconds());
    }
    anyhow::bail!("unsupported timeframe: {timeframe}")
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use chrono_tz::America::New_York;

    use crate::CanonicalTick;

    use super::{build_non_time_bars_from_ticks, build_time_bars_from_ticks};

    fn sample_ticks() -> Vec<CanonicalTick> {
        vec![
            CanonicalTick::new(Utc.with_ymd_and_hms(2026, 3, 1, 14, 30, 0).unwrap(), "NQH6", 100.0, 2.0, Some(99.75), Some(100.0)),
            CanonicalTick::new(Utc.with_ymd_and_hms(2026, 3, 1, 14, 30, 20).unwrap(), "NQH6", 101.0, 3.0, Some(100.75), Some(101.0)),
            CanonicalTick::new(Utc.with_ymd_and_hms(2026, 3, 1, 14, 31, 0).unwrap(), "NQH6", 99.5, 4.0, Some(99.25), Some(99.5)),
            CanonicalTick::new(Utc.with_ymd_and_hms(2026, 3, 1, 14, 31, 30).unwrap(), "NQH6", 102.0, 5.0, Some(101.75), Some(102.0)),
        ]
    }

    #[test]
    fn builds_time_bars() {
        let bars = build_time_bars_from_ticks(&sample_ticks(), "NQH6", "1m", New_York).unwrap();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].open, 100.0);
        assert_eq!(bars[0].close, 101.0);
        assert_eq!(bars[1].low, 99.5);
    }

    #[test]
    fn builds_tick_bars() {
        let bars = build_non_time_bars_from_ticks(&sample_ticks(), "NQH6", "tick", 2, 0.25, New_York).unwrap();
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].bar_type, "tick");
        assert_eq!(bars[0].trade_count, 2);
    }
}
