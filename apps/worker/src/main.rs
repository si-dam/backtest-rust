use anyhow::Result;
use app_core::{config::Settings, telemetry::init_tracing};
use backtest::{
    build_trade_records, merge_orb_params, simulate_orb_breakout_strategy, summarize_breakout_trades, NewRunInput,
    OrbRunSummary, PgBacktestStore, StrategyBar,
};
use jobs::{CreateJobInput, JobProgressUpdate, JobType, PgJobStore};
use market::{
    build_non_time_bars_from_ticks, build_profiles_for_ticks, build_time_bars_from_ticks, derive_symbol_root,
    detect_tick_size, parse_market_data_file, sha256_hex, summarize_parsed_data, BarRecord, BarsQuery, CanonicalTick,
    CanonicalTickRow, ClickHouseMarketStore, ParsedMarketData, TicksQuery,
};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use tokio::fs;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::from_env()?;
    init_tracing(&settings);

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_lazy(&settings.database_url)?;
    let jobs = PgJobStore::new(pool.clone());
    let market = ClickHouseMarketStore::new(&settings.clickhouse_url, &settings.clickhouse_database);
    let backtests = PgBacktestStore::new(pool);
    let worker_id = format!("runtime-worker-{}", std::process::id());
    let poll_interval = Duration::from_millis(settings.worker_poll_interval_ms);

    tracing::info!(worker_id = %worker_id, "runtime-worker started");

    loop {
        if let Some(job) = jobs.claim_next_job(&worker_id, 30).await? {
            tracing::info!(job_id = %job.id, job_type = %job.job_type, "claimed job");
            jobs.update_progress(
                job.id,
                &worker_id,
                30,
                JobProgressUpdate {
                    stage: "dispatching".to_string(),
                    detail_json: json!({ "job_type": job.job_type, "attempt": job.attempt }),
                },
            )
            .await?;
            let result = dispatch_job(
                &settings,
                &jobs,
                &market,
                &backtests,
                job.id,
                &worker_id,
                &job.job_type,
                &job.payload_json,
            )
            .await;
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
    backtests: &PgBacktestStore,
    job_id: Uuid,
    worker_id: &str,
    job_type: &str,
    payload: &Value,
) -> std::result::Result<Value, String> {
    match job_type {
        "ingestion" => handle_ingestion(settings, jobs, market, job_id, worker_id, payload).await,
        "build_bars" => handle_build_bars(settings, jobs, market, job_id, worker_id, payload).await,
        "build_profiles" => handle_build_profiles(settings, jobs, market, job_id, worker_id, payload).await,
        "backtest_run" => handle_backtest_run(settings, jobs, market, backtests, job_id, worker_id, payload).await,
        "dataset_export" => Ok(json!({ "status": "queued_for_export_implementation", "payload": payload })),
        other => Err(format!("unknown job type: {other}")),
    }
}

async fn handle_ingestion(
    settings: &Settings,
    jobs: &PgJobStore,
    market: &ClickHouseMarketStore,
    job_id: Uuid,
    worker_id: &str,
    payload: &Value,
) -> std::result::Result<Value, String> {
    update_stage(
        jobs,
        job_id,
        worker_id,
        "reading_source",
        json!({ "file_path": payload.get("file_path") }),
    )
    .await?;
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
    update_stage(jobs, job_id, worker_id, "parsing_source", json!({ "hash": file_hash })).await?;
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
            update_stage(
                jobs,
                job_id,
                worker_id,
                "persisting_ticks",
                json!({ "rows": ticks.len(), "rebuild": rebuild }),
            )
            .await?;
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

            update_stage(
                jobs,
                job_id,
                worker_id,
                "queueing_derivatives",
                json!({ "symbol_contract": symbol, "start": first.ts, "end": last.ts }),
            )
            .await?;
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
            update_stage(
                jobs,
                job_id,
                worker_id,
                "persisting_bars",
                json!({ "rows": bars.len(), "timeframe": "1m" }),
            )
            .await?;
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
    jobs: &PgJobStore,
    market: &ClickHouseMarketStore,
    job_id: Uuid,
    worker_id: &str,
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

    update_stage(
        jobs,
        job_id,
        worker_id,
        "loading_ticks",
        json!({ "symbol_contract": symbol_contract, "start": start, "end": end }),
    )
    .await?;
    let tick_rows = market
        .load_ticks(symbol_contract, &TicksQuery { start, end })
        .await
        .map_err(|error| error.to_string())?;
    let ticks = tick_rows_to_canonical(&tick_rows);

    let mut inserted_timeframes = Vec::new();
    for timeframe in ["1m", "3m", "5m", "15m", "30m", "60m"] {
        update_stage(
            jobs,
            job_id,
            worker_id,
            "building_time_bars",
            json!({ "symbol_contract": symbol_contract, "timeframe": timeframe }),
        )
        .await?;
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
        update_stage(
            jobs,
            job_id,
            worker_id,
            "building_non_time_bars",
            json!({ "symbol_contract": symbol_contract, "bar_type": bar_type, "bar_size": bar_size }),
        )
        .await?;
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
    jobs: &PgJobStore,
    market: &ClickHouseMarketStore,
    job_id: Uuid,
    worker_id: &str,
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

    update_stage(
        jobs,
        job_id,
        worker_id,
        "loading_ticks",
        json!({ "symbol_contract": symbol_contract, "start": start, "end": end }),
    )
    .await?;
    let tick_rows = market
        .load_ticks(symbol_contract, &TicksQuery { start, end })
        .await
        .map_err(|error| error.to_string())?;
    let ticks = tick_rows_to_canonical(&tick_rows);
    update_stage(
        jobs,
        job_id,
        worker_id,
        "building_profiles",
        json!({ "symbol_contract": symbol_contract, "timezone": profile_timezone.name() }),
    )
    .await?;
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

async fn handle_backtest_run(
    settings: &Settings,
    jobs: &PgJobStore,
    market: &ClickHouseMarketStore,
    backtests: &PgBacktestStore,
    job_id: Uuid,
    worker_id: &str,
    payload: &Value,
) -> std::result::Result<Value, String> {
    let name = payload
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("ORB Backtest")
        .to_string();
    let strategy_id = payload
        .get("strategy_id")
        .and_then(Value::as_str)
        .unwrap_or("scaffold");
    if strategy_id != "orb_breakout_v1" {
        return Err(format!("unsupported strategy_id: {strategy_id}"));
    }

    let params = payload.get("params").cloned().unwrap_or_else(|| json!({}));
    let orb_params = merge_orb_params(&params, settings.dataset_timezone)
        .map_err(|error| format!("invalid ORB params: {error}"))?;

    update_stage(
        jobs,
        job_id,
        worker_id,
        "creating_backtest_run",
        json!({ "strategy_id": strategy_id, "symbol_contract": orb_params.symbol_contract }),
    )
    .await?;
    let run = backtests
        .create_run(NewRunInput {
            job_id: Some(job_id),
            strategy_id: strategy_id.to_string(),
            name,
            params_json: json!(orb_params),
        })
        .await
        .map_err(|error| format!("failed to create backtest run: {error}"))?;

    let result = run_orb_backtest(settings, jobs, market, backtests, job_id, worker_id, run.id, orb_params).await;
    if let Err(error) = &result {
        backtests
            .fail_run(run.id, error)
            .await
            .map_err(|store_error| format!("failed to mark backtest run as failed: {store_error}"))?;
    }
    result
}

async fn run_orb_backtest(
    settings: &Settings,
    jobs: &PgJobStore,
    market: &ClickHouseMarketStore,
    backtests: &PgBacktestStore,
    job_id: Uuid,
    worker_id: &str,
    run_id: Uuid,
    params: backtest::OrbStrategyParams,
) -> std::result::Result<Value, String> {
    update_stage(
        jobs,
        job_id,
        worker_id,
        "loading_backtest_bars",
        json!({
            "run_id": run_id,
            "symbol_contract": params.symbol_contract,
            "timeframe": params.timeframe,
            "start": params.start,
            "end": params.end,
        }),
    )
    .await?;
    let bars = market
        .load_bars(
            &params.symbol_contract,
            &BarsQuery {
                start: params.start,
                end: params.end,
                timeframe: params.timeframe.clone(),
                bar_type: "time".to_string(),
                bar_size: None,
            },
        )
        .await
        .map_err(|error| format!("failed to load backtest bars: {error}"))?;
    let strategy_bars = bars_to_strategy_bars(&bars);

    update_stage(
        jobs,
        job_id,
        worker_id,
        "simulating_strategy",
        json!({ "run_id": run_id, "bar_count": strategy_bars.len(), "timezone": settings.dataset_timezone.name() }),
    )
    .await?;
    let trades = simulate_orb_breakout_strategy(&strategy_bars, &params)
        .map_err(|error| format!("failed to simulate ORB breakout: {error}"))?;
    let metrics = summarize_breakout_trades(&trades);
    let trade_records = build_trade_records(run_id, &params, &trades);

    update_stage(
        jobs,
        job_id,
        worker_id,
        "persisting_backtest_results",
        json!({ "run_id": run_id, "trade_count": trades.len() }),
    )
    .await?;
    backtests
        .insert_trades(run_id, &trade_records)
        .await
        .map_err(|error| format!("failed to insert backtest trades: {error}"))?;
    backtests
        .complete_run(run_id, metrics.clone())
        .await
        .map_err(|error| format!("failed to complete backtest run: {error}"))?;

    Ok(json!(OrbRunSummary {
        strategy_id: "orb_breakout_v1".to_string(),
        run_id,
        trade_count: trades.len(),
        metrics,
    }))
}

async fn update_stage(
    jobs: &PgJobStore,
    job_id: Uuid,
    worker_id: &str,
    stage: &str,
    detail_json: Value,
) -> std::result::Result<(), String> {
    jobs.update_progress(
        job_id,
        worker_id,
        30,
        JobProgressUpdate {
            stage: stage.to_string(),
            detail_json,
        },
    )
    .await
    .map_err(|error| format!("failed to update job progress: {error}"))
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

fn bars_to_strategy_bars(rows: &[BarRecord]) -> Vec<StrategyBar> {
    rows.iter()
        .map(|row| StrategyBar {
            ts: row.ts,
            open: row.open,
            high: row.high,
            low: row.low,
            close: row.close,
            volume: row.volume,
        })
        .collect()
}
