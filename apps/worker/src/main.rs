use anyhow::Result;
use app_core::{config::Settings, telemetry::init_tracing};
use jobs::{CreateJobInput, JobType, PgJobStore};
use market::{
    build_non_time_bars_from_ticks, build_profiles_for_ticks, build_time_bars_from_ticks, derive_symbol_root,
    detect_tick_size, parse_market_data_file, sha256_hex, summarize_parsed_data, CanonicalTick, CanonicalTickRow,
    ClickHouseMarketStore, ParsedMarketData, TicksQuery,
};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use tokio::fs;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::from_env()?;
    init_tracing(&settings);

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_lazy(&settings.database_url)?;
    let jobs = PgJobStore::new(pool);
    let market = ClickHouseMarketStore::new(&settings.clickhouse_url, &settings.clickhouse_database);
    let worker_id = format!("runtime-worker-{}", std::process::id());
    let poll_interval = Duration::from_millis(settings.worker_poll_interval_ms);

    tracing::info!(worker_id = %worker_id, "runtime-worker started");

    loop {
        if let Some(job) = jobs.claim_next_job(&worker_id, 30).await? {
            tracing::info!(job_id = %job.id, job_type = %job.job_type, "claimed job");
            jobs.heartbeat(job.id, &worker_id, 30, "dispatching").await?;
            let result = dispatch_job(&settings, &jobs, &market, &job.job_type, &job.payload_json).await;
            match result {
                Ok(summary) => jobs.complete_job(job.id, summary).await?,
                Err(error) => jobs.fail_job(job.id, &error).await?,
            }
            continue;
        }

        sleep(poll_interval).await;
    }
}

async fn dispatch_job(
    settings: &Settings,
    jobs: &PgJobStore,
    market: &ClickHouseMarketStore,
    job_type: &str,
    payload: &Value,
) -> std::result::Result<Value, String> {
    match job_type {
        "ingestion" => handle_ingestion(settings, jobs, market, payload).await,
        "build_bars" => handle_build_bars(settings, market, payload).await,
        "build_profiles" => handle_build_profiles(settings, market, payload).await,
        "backtest_run" => Err(format!("backtest worker not implemented yet: {payload}")),
        "dataset_export" => Ok(json!({ "status": "queued_for_export_implementation", "payload": payload })),
        other => Err(format!("unknown job type: {other}")),
    }
}

async fn handle_ingestion(
    settings: &Settings,
    jobs: &PgJobStore,
    market: &ClickHouseMarketStore,
    payload: &Value,
) -> std::result::Result<Value, String> {
    let file_path = payload
        .get("file_path")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing file_path".to_string())?;
    let symbol_contract = payload.get("symbol_contract").and_then(Value::as_str);
    let rebuild = payload.get("rebuild").and_then(Value::as_bool).unwrap_or(false);
    let bytes = fs::read(file_path).await.map_err(|error| format!("failed to read file: {error}"))?;
    let file_hash = sha256_hex(&bytes);
    if let Some(existing) = jobs
        .get_ingested_file(file_path)
        .await
        .map_err(|error| format!("failed to load prior ingest metadata: {error}"))?
    {
        if !rebuild && existing.source_hash == file_hash {
            return Ok(json!({
                "status": "skipped",
                "reason": "matching_source_already_ingested",
                "source_path": existing.source_path,
                "hash": file_hash,
            }));
        }
        if !rebuild && existing.source_hash != file_hash {
            return Err("source path already exists with a different hash; resubmit with rebuild=true".to_string());
        }
    }
    let parsed = parse_market_data_file(std::path::Path::new(file_path), settings.dataset_timezone, symbol_contract)
        .map_err(|error| format!("failed to parse market data file: {error}"))?;
    let summary = summarize_parsed_data(std::path::Path::new(file_path), &parsed);
    let tick_size = summary
        .symbol_contract
        .as_deref()
        .map(detect_tick_size)
        .unwrap_or(0.25);

    match parsed {
        ParsedMarketData::Ticks(ticks) => {
            let source_name = file_path.to_string();
            let canonical_rows = ticks
                .iter()
                .cloned()
                .map(|tick| tick.with_dataset_timezone(settings.dataset_timezone).with_source_file(source_name.clone()))
                .map(CanonicalTickRow::from)
                .collect::<Vec<_>>();
            if rebuild {
                market
                    .delete_ticks_by_source(&source_name)
                    .await
                    .map_err(|error| format!("failed to clear prior ticks for source: {error}"))?;
            }
            market
                .insert_ticks(&canonical_rows)
                .await
                .map_err(|error| format!("failed to insert ticks: {error}"))?;

            let first = ticks.first().ok_or_else(|| "tick dataset was empty".to_string())?;
            let last = ticks.last().ok_or_else(|| "tick dataset was empty".to_string())?;
            let symbol = summary
                .symbol_contract
                .clone()
                .ok_or_else(|| "missing symbol_contract".to_string())?;

            jobs.upsert_symbol(&symbol, &derive_symbol_root(&symbol), None, Some(tick_size))
                .await
                .map_err(|error| format!("failed to upsert symbol: {error}"))?;
            jobs.record_ingested_file(file_path, &file_hash, &summary.schema_kind, Some(&symbol), summary.row_count as i64)
                .await
                .map_err(|error| format!("failed to record ingested file: {error}"))?;

            jobs.create_job(CreateJobInput {
                job_type: JobType::BuildBars,
                payload_json: json!({
                    "symbol_contract": symbol,
                    "start": first.ts,
                    "end": last.ts,
                    "tick_size": tick_size,
                    "rebuild": true,
                }),
                max_attempts: 3,
            })
            .await
            .map_err(|error| format!("failed to enqueue build_bars job: {error}"))?;
            jobs.create_job(CreateJobInput {
                job_type: JobType::BuildProfiles,
                payload_json: json!({
                    "symbol_contract": summary.symbol_contract,
                    "start": first.ts,
                    "end": last.ts,
                    "tick_size": tick_size,
                    "profile_timezone": settings.dataset_timezone.name(),
                    "rebuild": true,
                }),
                max_attempts: 3,
            })
            .await
            .map_err(|error| format!("failed to enqueue build_profiles job: {error}"))?;
        }
        ParsedMarketData::Ohlc1m(bars) => {
            if let (Some(first), Some(last), Some(symbol)) = (bars.first(), bars.last(), summary.symbol_contract.as_deref()) {
                market
                    .replace_time_bars(symbol, "1m", first.ts, last.ts, &bars)
                    .await
                    .map_err(|error| format!("failed to replace 1m bars: {error}"))?;
            } else {
                market
                    .insert_time_bars(&bars)
                    .await
                    .map_err(|error| format!("failed to insert 1m bars: {error}"))?;
            }
            if let Some(symbol) = summary.symbol_contract.as_deref() {
                jobs.upsert_symbol(symbol, &derive_symbol_root(symbol), None, Some(tick_size))
                    .await
                    .map_err(|error| format!("failed to upsert symbol: {error}"))?;
                jobs.record_ingested_file(file_path, &file_hash, &summary.schema_kind, Some(symbol), summary.row_count as i64)
                    .await
                    .map_err(|error| format!("failed to record ingested file: {error}"))?;
            }
        }
    }

    Ok(json!({
        "status": "ingested",
        "summary": summary,
        "hash": file_hash,
    }))
}

async fn handle_build_bars(
    settings: &Settings,
    market: &ClickHouseMarketStore,
    payload: &Value,
) -> std::result::Result<Value, String> {
    let symbol_contract = payload
        .get("symbol_contract")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing symbol_contract".to_string())?;
    let start = serde_json::from_value(payload.get("start").cloned().ok_or_else(|| "missing start".to_string())?)
        .map_err(|error| format!("invalid start: {error}"))?;
    let end = serde_json::from_value(payload.get("end").cloned().ok_or_else(|| "missing end".to_string())?)
        .map_err(|error| format!("invalid end: {error}"))?;
    let tick_size = payload.get("tick_size").and_then(Value::as_f64).unwrap_or(0.25);

    let tick_rows = market
        .load_ticks(symbol_contract, &TicksQuery { start, end })
        .await
        .map_err(|error| error.to_string())?;
    let ticks = tick_rows_to_canonical(&tick_rows);

    let mut inserted_timeframes = Vec::new();
    for timeframe in ["1m", "3m", "5m", "15m", "30m", "60m"] {
        let bars = build_time_bars_from_ticks(&ticks, symbol_contract, timeframe, settings.dataset_timezone)
            .map_err(|error| format!("failed to build {timeframe} bars: {error}"))?;
        market
            .replace_time_bars(symbol_contract, timeframe, start, end, &bars)
            .await
            .map_err(|error| format!("failed to replace {timeframe} bars: {error}"))?;
        inserted_timeframes.push(json!({ "timeframe": timeframe, "rows": bars.len() }));
    }

    let mut inserted_non_time = Vec::new();
    for (bar_type, bar_size) in [("tick", 1500_u32), ("volume", 500_u32), ("volume", 1000_u32), ("range", 40_u32)] {
        let bars = build_non_time_bars_from_ticks(&ticks, symbol_contract, bar_type, bar_size, tick_size, settings.dataset_timezone)
            .map_err(|error| format!("failed to build {bar_type}:{bar_size} bars: {error}"))?;
        market
            .replace_non_time_bars(symbol_contract, bar_type, bar_size, start, end, &bars)
            .await
            .map_err(|error| format!("failed to replace {bar_type}:{bar_size} bars: {error}"))?;
        inserted_non_time.push(json!({ "bar_type": bar_type, "bar_size": bar_size, "rows": bars.len() }));
    }

    Ok(json!({
        "status": "bars_built",
        "symbol_contract": symbol_contract,
        "time_bars": inserted_timeframes,
        "non_time_bars": inserted_non_time,
    }))
}

async fn handle_build_profiles(
    settings: &Settings,
    market: &ClickHouseMarketStore,
    payload: &Value,
) -> std::result::Result<Value, String> {
    let symbol_contract = payload
        .get("symbol_contract")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing symbol_contract".to_string())?;
    let start = serde_json::from_value(payload.get("start").cloned().ok_or_else(|| "missing start".to_string())?)
        .map_err(|error| format!("invalid start: {error}"))?;
    let end = serde_json::from_value(payload.get("end").cloned().ok_or_else(|| "missing end".to_string())?)
        .map_err(|error| format!("invalid end: {error}"))?;
    let profile_timezone = payload
        .get("profile_timezone")
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
        .unwrap_or(settings.dataset_timezone);
    let tick_size = payload.get("tick_size").and_then(Value::as_f64).unwrap_or(0.25);

    let tick_rows = market
        .load_ticks(symbol_contract, &TicksQuery { start, end })
        .await
        .map_err(|error| error.to_string())?;
    let ticks = tick_rows_to_canonical(&tick_rows);
    let profiles = build_profiles_for_ticks(symbol_contract, &ticks, profile_timezone, tick_size);
    market
        .replace_profiles_in_range(symbol_contract, profile_timezone.name(), start, end, &profiles)
        .await
        .map_err(|error| format!("failed to persist profiles: {error}"))?;

    Ok(json!({
        "status": "profiles_built",
        "symbol_contract": symbol_contract,
        "profiles": profiles.len(),
    }))
}

fn tick_rows_to_canonical(rows: &[market::TickRecord]) -> Vec<CanonicalTick> {
    rows.iter()
        .cloned()
        .map(|row| CanonicalTick {
            ts: row.ts,
            trading_day: row.ts.date_naive(),
            session_date: row.session_date,
            symbol_contract: row.symbol_contract,
            trade_price: row.trade_price,
            trade_size: row.trade_size,
            bid_price: row.bid_price,
            ask_price: row.ask_price,
            source_file: None,
        })
        .collect()
}
