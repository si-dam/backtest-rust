use std::{
    collections::BTreeMap,
    env,
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use backtest::{merge_orb_params, simulate_orb_breakout_strategy, summarize_breakout_trades, StrategyBar};
use chrono_tz::Tz;
use market::{
    build_large_orders_from_ticks, build_non_time_bars_from_ticks, build_profiles_for_ticks,
    build_time_bars_from_ticks, detect_tick_size, parse_market_data_file, summarize_parsed_data,
    timeframe_to_seconds, CanonicalTick, LargeOrderRow, ParsedMarketData, TimeBarRow,
};
use serde::Serialize;
use serde_json::{json, Value};

fn main() -> Result<()> {
    let options = Options::parse(env::args().skip(1).collect())?;
    let started = Instant::now();
    let parse_started = Instant::now();
    let parsed = parse_market_data_file(&options.file_path, options.dataset_timezone, options.symbol.as_deref())
        .with_context(|| format!("failed to parse {}", options.file_path.display()))?;
    let parse_elapsed = parse_started.elapsed();
    let summary = summarize_parsed_data(&options.file_path, &parsed);

    let report = match parsed {
        ParsedMarketData::Ticks(ticks) => build_ticks_report(summary, ticks, &options, parse_elapsed, started.elapsed())?,
        ParsedMarketData::Ohlc1m(bars) => build_ohlc_report(summary, bars, &options, parse_elapsed, started.elapsed())?,
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

#[derive(Clone, Debug)]
struct Options {
    file_path: PathBuf,
    symbol: Option<String>,
    dataset_timezone: Tz,
    run_orb: bool,
    orb_timeframe: String,
    orb_ib_minutes: i64,
    orb_session_start: String,
    orb_session_end: String,
    orb_rth_only: bool,
    orb_stop_mode: String,
    orb_entry_mode: String,
    orb_strategy_mode: String,
    orb_big_trade_threshold: f64,
    orb_tp_multiple: f64,
    orb_contracts: i32,
}

impl Options {
    fn parse(args: Vec<String>) -> Result<Self> {
        if args.is_empty() {
            bail!(
                "usage: runtime-validate <file_path> [--symbol CONTRACT] [--timezone TZ] [--run-orb] [--timeframe 1m] [--ib-minutes 15] [--strategy-mode breakout_only|big_order_required] [--big-trade-threshold 25]"
            );
        }

        let mut file_path = None;
        let mut symbol = None;
        let mut dataset_timezone: Tz = "America/New_York".parse()?;
        let mut run_orb = false;
        let mut orb_timeframe = "1m".to_string();
        let mut orb_ib_minutes = 15_i64;
        let mut orb_session_start = "09:30:00".to_string();
        let mut orb_session_end = "16:00:00".to_string();
        let mut orb_rth_only = true;
        let mut orb_stop_mode = "or_boundary".to_string();
        let mut orb_entry_mode = "first_outside".to_string();
        let mut orb_strategy_mode = "breakout_only".to_string();
        let mut orb_big_trade_threshold = 25.0_f64;
        let mut orb_tp_multiple = 2.0_f64;
        let mut orb_contracts = 1_i32;

        let mut index = 0usize;
        while index < args.len() {
            let arg = &args[index];
            if !arg.starts_with("--") && file_path.is_none() {
                file_path = Some(PathBuf::from(arg));
                index += 1;
                continue;
            }

            let next = |index: &mut usize| -> Result<String> {
                *index += 1;
                args.get(*index)
                    .cloned()
                    .with_context(|| format!("missing value for {}", arg))
            };

            match arg.as_str() {
                "--symbol" => symbol = Some(next(&mut index)?),
                "--timezone" => dataset_timezone = next(&mut index)?.parse().context("invalid timezone")?,
                "--run-orb" => run_orb = true,
                "--timeframe" => orb_timeframe = next(&mut index)?,
                "--ib-minutes" => orb_ib_minutes = next(&mut index)?.parse().context("invalid ib-minutes")?,
                "--session-start" => orb_session_start = next(&mut index)?,
                "--session-end" => orb_session_end = next(&mut index)?,
                "--rth-only" => orb_rth_only = parse_bool(&next(&mut index)?)?,
                "--stop-mode" => orb_stop_mode = next(&mut index)?,
                "--entry-mode" => orb_entry_mode = next(&mut index)?,
                "--strategy-mode" => orb_strategy_mode = next(&mut index)?,
                "--big-trade-threshold" => {
                    orb_big_trade_threshold = next(&mut index)?.parse().context("invalid big-trade-threshold")?
                }
                "--tp-multiple" => orb_tp_multiple = next(&mut index)?.parse().context("invalid tp-multiple")?,
                "--contracts" => orb_contracts = next(&mut index)?.parse().context("invalid contracts")?,
                other => bail!("unknown argument: {other}"),
            }
            index += 1;
        }

        Ok(Self {
            file_path: file_path.context("file_path is required")?,
            symbol,
            dataset_timezone,
            run_orb,
            orb_timeframe,
            orb_ib_minutes,
            orb_session_start,
            orb_session_end,
            orb_rth_only,
            orb_stop_mode,
            orb_entry_mode,
            orb_strategy_mode,
            orb_big_trade_threshold,
            orb_tp_multiple,
            orb_contracts,
        })
    }
}

#[derive(Debug, Serialize)]
struct ValidationReport {
    source_path: String,
    schema_kind: String,
    symbol_contract: Option<String>,
    row_count: usize,
    timings_ms: BTreeMap<String, u128>,
    outputs: Value,
}

fn build_ticks_report(
    summary: market::ParsedFileSummary,
    ticks: Vec<CanonicalTick>,
    options: &Options,
    parse_elapsed: Duration,
    _elapsed: Duration,
) -> Result<ValidationReport> {
    let symbol_contract = summary
        .symbol_contract
        .clone()
        .or_else(|| options.symbol.clone())
        .unwrap_or_else(|| "UNKNOWN".to_string());
    let tick_size = detect_tick_size(&symbol_contract);

    let mut timings_ms = BTreeMap::new();
    timings_ms.insert("parse".to_string(), parse_elapsed.as_millis());

    let build_1m_started = Instant::now();
    let bars_1m = build_time_bars_from_ticks(&ticks, &symbol_contract, "1m", options.dataset_timezone)?;
    timings_ms.insert("build_time_bars_1m".to_string(), build_1m_started.elapsed().as_millis());

    let build_5m_started = Instant::now();
    let bars_5m = build_time_bars_from_ticks(&ticks, &symbol_contract, "5m", options.dataset_timezone)?;
    timings_ms.insert("build_time_bars_5m".to_string(), build_5m_started.elapsed().as_millis());

    let tick_bars_started = Instant::now();
    let tick_bars = build_non_time_bars_from_ticks(
        &ticks,
        &symbol_contract,
        "tick",
        1500,
        tick_size,
        options.dataset_timezone,
    )?;
    timings_ms.insert("build_tick_bars_1500".to_string(), tick_bars_started.elapsed().as_millis());

    let volume_bars_started = Instant::now();
    let volume_bars = build_non_time_bars_from_ticks(
        &ticks,
        &symbol_contract,
        "volume",
        500,
        tick_size,
        options.dataset_timezone,
    )?;
    timings_ms.insert("build_volume_bars_500".to_string(), volume_bars_started.elapsed().as_millis());

    let range_bars_started = Instant::now();
    let range_bars = build_non_time_bars_from_ticks(
        &ticks,
        &symbol_contract,
        "range",
        40,
        tick_size,
        options.dataset_timezone,
    )?;
    timings_ms.insert("build_range_bars_40".to_string(), range_bars_started.elapsed().as_millis());

    let large_orders_started = Instant::now();
    let large_orders = build_large_orders_from_ticks(
        &symbol_contract,
        &ticks,
        "fixed",
        options.orb_big_trade_threshold,
    );
    timings_ms.insert("build_large_orders".to_string(), large_orders_started.elapsed().as_millis());

    let profiles_started = Instant::now();
    let profiles = build_profiles_for_ticks(&symbol_contract, &ticks, options.dataset_timezone, tick_size);
    timings_ms.insert("build_profiles".to_string(), profiles_started.elapsed().as_millis());

    let orb = if options.run_orb {
        let orb_started = Instant::now();
        let orb_bars = build_time_bars_from_ticks(
            &ticks,
            &symbol_contract,
            &options.orb_timeframe,
            options.dataset_timezone,
        )?;
        let params = merge_orb_params(
            &json!({
                "symbol_contract": symbol_contract,
                "start": ticks.first().map(|row| row.ts).context("tick file is empty")?,
                "end": ticks.last().map(|row| row.ts).context("tick file is empty")?,
                "timeframe": options.orb_timeframe,
                "ib_minutes": options.orb_ib_minutes,
                "session_start": options.orb_session_start,
                "session_end": options.orb_session_end,
                "rth_only": options.orb_rth_only,
                "stop_mode": options.orb_stop_mode,
                "tp_r_multiple": options.orb_tp_multiple,
                "entry_mode": options.orb_entry_mode,
                "strategy_mode": options.orb_strategy_mode,
                "big_trade_threshold": options.orb_big_trade_threshold,
                "contracts": options.orb_contracts,
                "timezone": options.dataset_timezone.name(),
            }),
            options.dataset_timezone,
        )?;
        let mut strategy_bars = bars_to_strategy_bars(&orb_bars);
        if params.strategy_mode == backtest::StrategyMode::BigOrderRequired {
            annotate_strategy_bars_with_large_orders(
                &mut strategy_bars,
                &large_orders,
                timeframe_to_seconds(&options.orb_timeframe)?,
            );
        }
        let trades = simulate_orb_breakout_strategy(&strategy_bars, &params)?;
        timings_ms.insert("run_orb".to_string(), orb_started.elapsed().as_millis());
        Some(json!({
            "trade_count": trades.len(),
            "summary": summarize_breakout_trades(&trades),
        }))
    } else {
        None
    };

    let profile_counts = profiles.iter().fold(BTreeMap::<String, usize>::new(), |mut acc, profile| {
        let key = format!("{}:{}", profile.segment.preset, profile.segment.metric);
        *acc.entry(key).or_default() += 1;
        acc
    });

    Ok(ValidationReport {
        source_path: summary.source_path.display().to_string(),
        schema_kind: summary.schema_kind,
        symbol_contract: Some(symbol_contract),
        row_count: summary.row_count,
        timings_ms,
        outputs: json!({
            "tick_count": ticks.len(),
            "bars": {
                "time_1m": bars_1m.len(),
                "time_5m": bars_5m.len(),
                "tick_1500": tick_bars.len(),
                "volume_500": volume_bars.len(),
                "range_40": range_bars.len(),
            },
            "large_orders": {
                "threshold": options.orb_big_trade_threshold,
                "count": large_orders.len(),
            },
            "profiles": {
                "count": profiles.len(),
                "by_preset_metric": profile_counts,
            },
            "orb": orb,
        }),
    })
}

fn build_ohlc_report(
    summary: market::ParsedFileSummary,
    bars: Vec<TimeBarRow>,
    options: &Options,
    parse_elapsed: Duration,
    _elapsed: Duration,
) -> Result<ValidationReport> {
    let symbol_contract = summary
        .symbol_contract
        .clone()
        .or_else(|| options.symbol.clone())
        .unwrap_or_else(|| "UNKNOWN".to_string());
    let mut timings_ms = BTreeMap::new();
    timings_ms.insert("parse".to_string(), parse_elapsed.as_millis());

    let orb = if options.run_orb {
        if options.orb_timeframe != "1m" {
            bail!("OHLC validation only supports --timeframe 1m because no finer-grained source is available")
        }
        let orb_started = Instant::now();
        let params = merge_orb_params(
            &json!({
                "symbol_contract": symbol_contract,
                "start": bars.first().map(|row| row.ts).context("ohlc file is empty")?,
                "end": bars.last().map(|row| row.ts).context("ohlc file is empty")?,
                "timeframe": options.orb_timeframe,
                "ib_minutes": options.orb_ib_minutes,
                "session_start": options.orb_session_start,
                "session_end": options.orb_session_end,
                "rth_only": options.orb_rth_only,
                "stop_mode": options.orb_stop_mode,
                "tp_r_multiple": options.orb_tp_multiple,
                "entry_mode": options.orb_entry_mode,
                "strategy_mode": "breakout_only",
                "contracts": options.orb_contracts,
                "timezone": options.dataset_timezone.name(),
            }),
            options.dataset_timezone,
        )?;
        let strategy_bars = bars_to_strategy_bars(&bars);
        let trades = simulate_orb_breakout_strategy(&strategy_bars, &params)?;
        timings_ms.insert("run_orb".to_string(), orb_started.elapsed().as_millis());
        Some(json!({
            "trade_count": trades.len(),
            "summary": summarize_breakout_trades(&trades),
            "note": "OHLC validation runs ORB in breakout_only mode because big-order confirmation requires ticks.",
        }))
    } else {
        None
    };

    Ok(ValidationReport {
        source_path: summary.source_path.display().to_string(),
        schema_kind: summary.schema_kind,
        symbol_contract: Some(symbol_contract),
        row_count: summary.row_count,
        timings_ms,
        outputs: json!({
            "bars": {
                "time_1m": bars.len(),
            },
            "orb": orb,
        }),
    })
}

fn bars_to_strategy_bars(rows: &[TimeBarRow]) -> Vec<StrategyBar> {
    rows.iter()
        .map(|row| StrategyBar {
            ts: row.ts,
            open: row.open,
            high: row.high,
            low: row.low,
            close: row.close,
            volume: row.volume,
            has_big_buy: false,
            has_big_sell: false,
        })
        .collect()
}

fn annotate_strategy_bars_with_large_orders(
    bars: &mut [StrategyBar],
    large_orders: &[LargeOrderRow],
    timeframe_seconds: i64,
) {
    let mut order_index = 0usize;
    for bar in bars.iter_mut() {
        let window_end = bar.ts + chrono::Duration::seconds(timeframe_seconds);
        while order_index < large_orders.len() && large_orders[order_index].ts < bar.ts {
            order_index += 1;
        }
        let mut scan_index = order_index;
        while scan_index < large_orders.len() && large_orders[scan_index].ts < window_end {
            match large_orders[scan_index].side.as_str() {
                "buy" => bar.has_big_buy = true,
                "sell" => bar.has_big_sell = true,
                _ => {}
            }
            scan_index += 1;
        }
    }
}

fn parse_bool(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Ok(true),
        "0" | "false" | "no" | "n" | "off" => Ok(false),
        _ => bail!("invalid boolean value: {raw}"),
    }
}
