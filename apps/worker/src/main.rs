use std::{fs::File, path::PathBuf, sync::Arc};

use anyhow::Result;
use app_core::{config::Settings, telemetry::init_tracing};
use arrow_array::{
    ArrayRef, Float64Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use backtest::{
    build_trade_records, merge_orb_params, parse_orb_split_config, simulate_orb_breakout_strategy,
    summarize_breakout_trades, BacktestTradeRecord, NewRunInput, OrbRunSummary, PgBacktestStore,
    StrategyBar,
};
use chrono::{DateTime, Utc};
use jobs::{CreateJobInput, JobProgressUpdate, JobType, PgJobStore};
use market::{
    build_large_orders_from_ticks, build_non_time_bars_from_ticks, build_profiles_for_ticks, build_time_bars_from_ticks,
    derive_symbol_root, detect_tick_size, parse_market_data_file, sha256_hex,
    summarize_parsed_data, BarRecord, BarsQuery, CanonicalTick, CanonicalTickRow,
    ClickHouseMarketStore, ParsedMarketData, TicksQuery,
};
use parquet::arrow::ArrowWriter;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use tokio::fs;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

#[derive(Clone, Debug)]
struct OrbRunSegment {
    label: Option<&'static str>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    split_meta: Option<Value>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::from_env()?;
    init_tracing(&settings);

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_lazy(&settings.database_url)?;
    let jobs = PgJobStore::new(pool.clone());
    let market =
        ClickHouseMarketStore::new(&settings.clickhouse_url, &settings.clickhouse_database);
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
        "build_profiles" => {
            handle_build_profiles(settings, jobs, market, job_id, worker_id, payload).await
        }
        "backtest_run" => {
            handle_backtest_run(
                settings, jobs, market, backtests, job_id, worker_id, payload,
            )
            .await
        }
        "dataset_export" => {
            handle_dataset_export(settings, jobs, market, backtests, job_id, worker_id, payload)
                .await
        }
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
    let rebuild = payload
        .get("rebuild")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let bytes = fs::read(file_path)
        .await
        .map_err(|error| format!("failed to read file: {error}"))?;
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
            return Err(
                "source path already exists with a different hash; resubmit with rebuild=true"
                    .to_string(),
            );
        }
    }
    update_stage(
        jobs,
        job_id,
        worker_id,
        "parsing_source",
        json!({ "hash": file_hash }),
    )
    .await?;
    let parsed = parse_market_data_file(
        std::path::Path::new(file_path),
        settings.dataset_timezone,
        symbol_contract,
    )
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
                .map(|tick| {
                    tick.with_dataset_timezone(settings.dataset_timezone)
                        .with_source_file(source_name.clone())
                })
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

            let first = ticks
                .first()
                .ok_or_else(|| "tick dataset was empty".to_string())?;
            let last = ticks
                .last()
                .ok_or_else(|| "tick dataset was empty".to_string())?;
            let symbol = summary
                .symbol_contract
                .clone()
                .ok_or_else(|| "missing symbol_contract".to_string())?;

            jobs.upsert_symbol(&symbol, &derive_symbol_root(&symbol), None, Some(tick_size))
                .await
                .map_err(|error| format!("failed to upsert symbol: {error}"))?;
            jobs.record_ingested_file(
                file_path,
                &file_hash,
                &summary.schema_kind,
                Some(&symbol),
                summary.row_count as i64,
            )
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
            if let (Some(first), Some(last), Some(symbol)) = (
                bars.first(),
                bars.last(),
                summary.symbol_contract.as_deref(),
            ) {
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
                jobs.record_ingested_file(
                    file_path,
                    &file_hash,
                    &summary.schema_kind,
                    Some(symbol),
                    summary.row_count as i64,
                )
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
    let start = serde_json::from_value(
        payload
            .get("start")
            .cloned()
            .ok_or_else(|| "missing start".to_string())?,
    )
    .map_err(|error| format!("invalid start: {error}"))?;
    let end = serde_json::from_value(
        payload
            .get("end")
            .cloned()
            .ok_or_else(|| "missing end".to_string())?,
    )
    .map_err(|error| format!("invalid end: {error}"))?;
    let tick_size = payload
        .get("tick_size")
        .and_then(Value::as_f64)
        .unwrap_or(0.25);

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
        let bars = build_time_bars_from_ticks(
            &ticks,
            symbol_contract,
            timeframe,
            settings.dataset_timezone,
        )
        .map_err(|error| format!("failed to build {timeframe} bars: {error}"))?;
        market
            .replace_time_bars(symbol_contract, timeframe, start, end, &bars)
            .await
            .map_err(|error| format!("failed to replace {timeframe} bars: {error}"))?;
        inserted_timeframes.push(json!({ "timeframe": timeframe, "rows": bars.len() }));
    }

    let mut inserted_non_time = Vec::new();
    for (bar_type, bar_size) in [
        ("tick", 1500_u32),
        ("volume", 500_u32),
        ("volume", 1000_u32),
        ("range", 40_u32),
    ] {
        update_stage(
            jobs,
            job_id,
            worker_id,
            "building_non_time_bars",
            json!({ "symbol_contract": symbol_contract, "bar_type": bar_type, "bar_size": bar_size }),
        )
        .await?;
        let bars = build_non_time_bars_from_ticks(
            &ticks,
            symbol_contract,
            bar_type,
            bar_size,
            tick_size,
            settings.dataset_timezone,
        )
        .map_err(|error| format!("failed to build {bar_type}:{bar_size} bars: {error}"))?;
        market
            .replace_non_time_bars(symbol_contract, bar_type, bar_size, start, end, &bars)
            .await
            .map_err(|error| format!("failed to replace {bar_type}:{bar_size} bars: {error}"))?;
        inserted_non_time
            .push(json!({ "bar_type": bar_type, "bar_size": bar_size, "rows": bars.len() }));
    }

    let large_order_threshold = 25.0_f64;
    update_stage(
        jobs,
        job_id,
        worker_id,
        "building_large_orders",
        json!({
            "symbol_contract": symbol_contract,
            "method": "fixed",
            "fixed_threshold": large_order_threshold,
        }),
    )
    .await?;
    let large_orders =
        build_large_orders_from_ticks(symbol_contract, &ticks, "fixed", large_order_threshold);
    market
        .replace_large_orders(
            symbol_contract,
            "fixed",
            large_order_threshold,
            start,
            end,
            &large_orders,
        )
        .await
        .map_err(|error| format!("failed to replace fixed large orders: {error}"))?;

    Ok(json!({
        "status": "bars_built",
        "symbol_contract": symbol_contract,
        "time_bars": inserted_timeframes,
        "non_time_bars": inserted_non_time,
        "large_orders": {
            "method": "fixed",
            "threshold": large_order_threshold,
            "rows": large_orders.len(),
        },
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
    let start = serde_json::from_value(
        payload
            .get("start")
            .cloned()
            .ok_or_else(|| "missing start".to_string())?,
    )
    .map_err(|error| format!("invalid start: {error}"))?;
    let end = serde_json::from_value(
        payload
            .get("end")
            .cloned()
            .ok_or_else(|| "missing end".to_string())?,
    )
    .map_err(|error| format!("invalid end: {error}"))?;
    let profile_timezone = payload
        .get("profile_timezone")
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
        .unwrap_or(settings.dataset_timezone);
    let tick_size = payload
        .get("tick_size")
        .and_then(Value::as_f64)
        .unwrap_or(0.25);

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
        .replace_profiles_in_range(
            symbol_contract,
            profile_timezone.name(),
            start,
            end,
            &profiles,
        )
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
    let split_config = parse_orb_split_config(&params, orb_params.start, orb_params.end)
        .map_err(|error| format!("invalid ORB params: {error}"))?;
    let split_group_id = split_config.as_ref().map(|_| Uuid::new_v4().to_string());
    let segments = orb_segments(
        &orb_params,
        split_config.as_ref(),
        split_group_id.as_deref(),
    );

    update_stage(
        jobs,
        job_id,
        worker_id,
        "creating_backtest_run",
        json!({
            "strategy_id": strategy_id,
            "symbol_contract": orb_params.symbol_contract,
            "segments": segments.len(),
        }),
    )
    .await?;

    let mut created_run_ids = Vec::with_capacity(segments.len());
    let mut total_trades = 0usize;

    for segment in segments {
        let mut segment_params = orb_params.clone();
        segment_params.start = segment.start;
        segment_params.end = segment.end;

        let run_name = segment
            .label
            .map(|label| format!("{name} [{}]", label.to_uppercase()))
            .unwrap_or_else(|| name.clone());
        let params_json = params_json_for_segment(&segment_params, segment.split_meta.clone());

        let run = backtests
            .create_run(NewRunInput {
                job_id: Some(job_id),
                strategy_id: strategy_id.to_string(),
                name: run_name,
                params_json,
            })
            .await
            .map_err(|error| format!("failed to create backtest run: {error}"))?;

        let result = run_orb_backtest(
            settings,
            jobs,
            market,
            backtests,
            job_id,
            worker_id,
            run.id,
            segment_params,
            segment.label,
        )
        .await;
        if let Err(error) = &result {
            backtests
                .fail_run(run.id, error)
                .await
                .map_err(|store_error| {
                    format!("failed to mark backtest run as failed: {store_error}")
                })?;
        }
        let summary = result?;
        created_run_ids.push(run.id);
        total_trades += summary.trade_count;
    }

    let mut response = json!({
        "run_id": created_run_ids.first(),
        "run_ids": created_run_ids,
        "mode": "run",
        "strategy_id": strategy_id,
        "trade_count": total_trades,
        "created_runs": created_run_ids.len(),
        "failed_runs": 0,
    });
    if let Some(group_id) = split_group_id {
        response
            .as_object_mut()
            .expect("backtest split response should be an object")
            .insert("split_group_id".to_string(), json!(group_id));
    }
    Ok(response)
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
    segment_label: Option<&str>,
) -> std::result::Result<OrbRunSummary, String> {
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
    let mut metrics = summarize_breakout_trades(&trades);
    if let Some(segment) = segment_label {
        metrics
            .as_object_mut()
            .expect("backtest metrics should be an object")
            .insert("segment".to_string(), json!(segment));
    }
    let mut trade_records = build_trade_records(run_id, &params, &trades);
    if let Some(segment) = segment_label {
        for trade in &mut trade_records {
            trade
                .notes_json
                .as_object_mut()
                .expect("trade notes should be an object")
                .insert("segment".to_string(), json!(segment));
        }
    }

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

    Ok(OrbRunSummary {
        strategy_id: "orb_breakout_v1".to_string(),
        run_id,
        trade_count: trades.len(),
        metrics,
    })
}

fn orb_segments(
    params: &backtest::OrbStrategyParams,
    split_config: Option<&backtest::OrbSplitConfig>,
    split_group_id: Option<&str>,
) -> Vec<OrbRunSegment> {
    match split_config {
        Some(split) => vec![
            OrbRunSegment {
                label: Some("is"),
                start: params.start,
                end: split.split_at,
                split_meta: Some(json!({
                    "enabled": true,
                    "split_at": split.split_at,
                    "segment": "is",
                    "group_id": split_group_id,
                })),
            },
            OrbRunSegment {
                label: Some("oos"),
                start: split.split_at,
                end: params.end,
                split_meta: Some(json!({
                    "enabled": true,
                    "split_at": split.split_at,
                    "segment": "oos",
                    "group_id": split_group_id,
                })),
            },
        ],
        None => vec![OrbRunSegment {
            label: None,
            start: params.start,
            end: params.end,
            split_meta: None,
        }],
    }
}

fn params_json_for_segment(
    params: &backtest::OrbStrategyParams,
    split_meta: Option<Value>,
) -> Value {
    let mut payload = json!(params);
    if let Some(split_meta) = split_meta {
        payload
            .as_object_mut()
            .expect("segment params should serialize to an object")
            .insert("split".to_string(), split_meta);
    }
    payload
}

async fn handle_dataset_export(
    settings: &Settings,
    jobs: &PgJobStore,
    market: &ClickHouseMarketStore,
    backtests: &PgBacktestStore,
    job_id: Uuid,
    worker_id: &str,
    payload: &Value,
) -> std::result::Result<Value, String> {
    let export_kind = payload
        .get("export_kind")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing export_kind".to_string())?;
    let export_payload = payload.get("payload").cloned().unwrap_or_else(|| json!({}));
    let artifact_root = payload
        .get("artifact_root")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .unwrap_or_else(|| settings.artifact_root.clone());

    match export_kind {
        "bars" => {
            export_bars_dataset(
                jobs,
                market,
                job_id,
                worker_id,
                &artifact_root,
                export_payload,
            )
            .await
        }
        "ticks" => {
            export_ticks_dataset(
                jobs,
                market,
                job_id,
                worker_id,
                &artifact_root,
                export_payload,
            )
            .await
        }
        "backtest_trades" => {
            export_backtest_trades_dataset(
                jobs,
                backtests,
                job_id,
                worker_id,
                &artifact_root,
                export_payload,
            )
            .await
        }
        other => Err(format!("unsupported export_kind: {other}")),
    }
}

async fn export_bars_dataset(
    jobs: &PgJobStore,
    market: &ClickHouseMarketStore,
    job_id: Uuid,
    worker_id: &str,
    artifact_root: &PathBuf,
    payload: Value,
) -> std::result::Result<Value, String> {
    let symbol_contract = payload
        .get("symbol_contract")
        .and_then(Value::as_str)
        .ok_or_else(|| "bars export requires payload.symbol_contract".to_string())?;
    let timeframe = payload
        .get("timeframe")
        .and_then(Value::as_str)
        .unwrap_or("1m")
        .to_string();
    let bar_type = payload
        .get("bar_type")
        .and_then(Value::as_str)
        .unwrap_or("time")
        .to_string();
    let bar_size = payload
        .get("bar_size")
        .and_then(Value::as_u64)
        .map(|value| value as u32);
    let start = serde_json::from_value(
        payload
            .get("start")
            .cloned()
            .ok_or_else(|| "bars export requires payload.start".to_string())?,
    )
    .map_err(|error| format!("invalid export start: {error}"))?;
    let end = serde_json::from_value(
        payload
            .get("end")
            .cloned()
            .ok_or_else(|| "bars export requires payload.end".to_string())?,
    )
    .map_err(|error| format!("invalid export end: {error}"))?;

    update_stage(
        jobs,
        job_id,
        worker_id,
        "loading_export_data",
        json!({
            "export_kind": "bars",
            "symbol_contract": symbol_contract,
            "timeframe": timeframe,
            "bar_type": bar_type,
            "bar_size": bar_size,
        }),
    )
    .await?;
    let bars = market
        .load_bars(
            symbol_contract,
            &BarsQuery {
                start,
                end,
                timeframe: timeframe.clone(),
                bar_type: bar_type.clone(),
                bar_size,
            },
        )
        .await
        .map_err(|error| format!("failed to load bars for export: {error}"))?;

    let export_dir = artifact_root
        .join("dataset_exports")
        .join(job_id.to_string());
    fs::create_dir_all(&export_dir)
        .await
        .map_err(|error| format!("failed to create export directory: {error}"))?;
    let data_path = export_dir.join("bars.parquet");
    let manifest_path = export_dir.join("manifest.json");

    update_stage(
        jobs,
        job_id,
        worker_id,
        "writing_export_files",
        json!({ "data_path": data_path, "row_count": bars.len() }),
    )
    .await?;
    write_bars_parquet(&data_path, &bars)
        .map_err(|error| format!("failed to write bars parquet: {error}"))?;

    let schema_version = "bars.v1";
    let manifest = json!({
        "job_id": job_id,
        "export_kind": "bars",
        "schema_version": schema_version,
        "files": [
            {
                "kind": "parquet",
                "path": data_path,
                "rows": bars.len(),
            }
        ],
        "request": {
            "symbol_contract": symbol_contract,
            "timeframe": timeframe,
            "bar_type": bar_type,
            "bar_size": bar_size,
            "start": start,
            "end": end,
        },
        "row_count": bars.len(),
    });

    let manifest_string = serde_json::to_string_pretty(&manifest)
        .map_err(|error| format!("failed to encode manifest: {error}"))?;
    fs::write(&manifest_path, manifest_string)
        .await
        .map_err(|error| format!("failed to write manifest: {error}"))?;

    update_stage(
        jobs,
        job_id,
        worker_id,
        "recording_export",
        json!({ "manifest_path": manifest_path }),
    )
    .await?;
    let export_record = jobs
        .record_dataset_export(
            Some(job_id),
            "bars",
            manifest_path
                .to_str()
                .ok_or_else(|| "manifest path is not valid UTF-8".to_string())?,
            schema_version,
            manifest.clone(),
        )
        .await
        .map_err(|error| format!("failed to record dataset export: {error}"))?;

    Ok(json!({
        "status": "dataset_exported",
        "export_id": export_record.id,
        "export_kind": "bars",
        "manifest_path": manifest_path,
        "row_count": bars.len(),
    }))
}

async fn export_ticks_dataset(
    jobs: &PgJobStore,
    market: &ClickHouseMarketStore,
    job_id: Uuid,
    worker_id: &str,
    artifact_root: &PathBuf,
    payload: Value,
) -> std::result::Result<Value, String> {
    let symbol_contract = payload
        .get("symbol_contract")
        .and_then(Value::as_str)
        .ok_or_else(|| "ticks export requires payload.symbol_contract".to_string())?;
    let start = serde_json::from_value(
        payload
            .get("start")
            .cloned()
            .ok_or_else(|| "ticks export requires payload.start".to_string())?,
    )
    .map_err(|error| format!("invalid export start: {error}"))?;
    let end = serde_json::from_value(
        payload
            .get("end")
            .cloned()
            .ok_or_else(|| "ticks export requires payload.end".to_string())?,
    )
    .map_err(|error| format!("invalid export end: {error}"))?;

    update_stage(
        jobs,
        job_id,
        worker_id,
        "loading_export_data",
        json!({
            "export_kind": "ticks",
            "symbol_contract": symbol_contract,
            "start": start,
            "end": end,
        }),
    )
    .await?;
    let ticks = market
        .load_ticks(symbol_contract, &TicksQuery { start, end })
        .await
        .map_err(|error| format!("failed to load ticks for export: {error}"))?;

    let export_dir = artifact_root
        .join("dataset_exports")
        .join(job_id.to_string());
    fs::create_dir_all(&export_dir)
        .await
        .map_err(|error| format!("failed to create export directory: {error}"))?;
    let data_path = export_dir.join("ticks.parquet");
    let manifest_path = export_dir.join("manifest.json");

    update_stage(
        jobs,
        job_id,
        worker_id,
        "writing_export_files",
        json!({ "data_path": data_path, "row_count": ticks.len() }),
    )
    .await?;
    write_ticks_parquet(&data_path, &ticks)
        .map_err(|error| format!("failed to write ticks parquet: {error}"))?;

    let schema_version = "ticks.v1";
    let manifest = json!({
        "job_id": job_id,
        "export_kind": "ticks",
        "schema_version": schema_version,
        "files": [
            {
                "kind": "parquet",
                "path": data_path,
                "rows": ticks.len(),
            }
        ],
        "request": {
            "symbol_contract": symbol_contract,
            "start": start,
            "end": end,
        },
        "row_count": ticks.len(),
    });

    let manifest_string = serde_json::to_string_pretty(&manifest)
        .map_err(|error| format!("failed to encode manifest: {error}"))?;
    fs::write(&manifest_path, manifest_string)
        .await
        .map_err(|error| format!("failed to write manifest: {error}"))?;

    update_stage(
        jobs,
        job_id,
        worker_id,
        "recording_export",
        json!({ "manifest_path": manifest_path }),
    )
    .await?;
    let export_record = jobs
        .record_dataset_export(
            Some(job_id),
            "ticks",
            manifest_path
                .to_str()
                .ok_or_else(|| "manifest path is not valid UTF-8".to_string())?,
            schema_version,
            manifest.clone(),
        )
        .await
        .map_err(|error| format!("failed to record dataset export: {error}"))?;

    Ok(json!({
        "status": "dataset_exported",
        "export_id": export_record.id,
        "export_kind": "ticks",
        "manifest_path": manifest_path,
        "row_count": ticks.len(),
    }))
}

async fn export_backtest_trades_dataset(
    jobs: &PgJobStore,
    backtests: &PgBacktestStore,
    job_id: Uuid,
    worker_id: &str,
    artifact_root: &PathBuf,
    payload: Value,
) -> std::result::Result<Value, String> {
    let run_id = serde_json::from_value::<Uuid>(
        payload
            .get("run_id")
            .cloned()
            .ok_or_else(|| "backtest_trades export requires payload.run_id".to_string())?,
    )
    .map_err(|error| format!("invalid export run_id: {error}"))?;

    update_stage(
        jobs,
        job_id,
        worker_id,
        "loading_export_data",
        json!({
            "export_kind": "backtest_trades",
            "run_id": run_id,
        }),
    )
    .await?;

    let run = backtests
        .get_run(run_id)
        .await
        .map_err(|error| format!("failed to load backtest run: {error}"))?
        .ok_or_else(|| format!("backtest run not found: {run_id}"))?;
    let trades = backtests
        .get_run_trades(run_id)
        .await
        .map_err(|error| format!("failed to load backtest trades: {error}"))?;

    let export_dir = artifact_root.join("dataset_exports").join(job_id.to_string());
    fs::create_dir_all(&export_dir)
        .await
        .map_err(|error| format!("failed to create export directory: {error}"))?;
    let data_path = export_dir.join("backtest_trades.parquet");
    let manifest_path = export_dir.join("manifest.json");

    update_stage(
        jobs,
        job_id,
        worker_id,
        "writing_export_files",
        json!({ "data_path": data_path, "row_count": trades.len() }),
    )
    .await?;
    write_backtest_trades_parquet(&data_path, &trades)
        .map_err(|error| format!("failed to write backtest trades parquet: {error}"))?;

    let schema_version = "backtest_trades.v1";
    let manifest = json!({
        "job_id": job_id,
        "export_kind": "backtest_trades",
        "schema_version": schema_version,
        "files": [
            {
                "kind": "parquet",
                "path": data_path,
                "rows": trades.len(),
            }
        ],
        "request": {
            "run_id": run_id,
        },
        "run": {
            "id": run.id,
            "strategy_id": run.strategy_id,
            "name": run.name,
            "status": run.status,
            "params_json": run.params_json,
            "metrics_json": run.metrics_json,
        },
        "row_count": trades.len(),
    });

    let manifest_string = serde_json::to_string_pretty(&manifest)
        .map_err(|error| format!("failed to encode manifest: {error}"))?;
    fs::write(&manifest_path, manifest_string)
        .await
        .map_err(|error| format!("failed to write manifest: {error}"))?;

    update_stage(
        jobs,
        job_id,
        worker_id,
        "recording_export",
        json!({ "manifest_path": manifest_path }),
    )
    .await?;
    let export_record = jobs
        .record_dataset_export(
            Some(job_id),
            "backtest_trades",
            manifest_path
                .to_str()
                .ok_or_else(|| "manifest path is not valid UTF-8".to_string())?,
            schema_version,
            manifest.clone(),
        )
        .await
        .map_err(|error| format!("failed to record dataset export: {error}"))?;

    Ok(json!({
        "status": "dataset_exported",
        "export_id": export_record.id,
        "export_kind": "backtest_trades",
        "manifest_path": manifest_path,
        "row_count": trades.len(),
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

fn write_bars_parquet(path: &PathBuf, rows: &[BarRecord]) -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::<str>::from("UTC"))),
            false,
        ),
        Field::new("session_date", DataType::Utf8, false),
        Field::new("symbol_contract", DataType::Utf8, false),
        Field::new("timeframe", DataType::Utf8, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("trade_count", DataType::UInt64, false),
    ]));

    let ts = TimestampMicrosecondArray::from(
        rows.iter()
            .map(|row| row.ts.timestamp_micros())
            .collect::<Vec<_>>(),
    );
    let session_dates = StringArray::from(
        rows.iter()
            .map(|row| row.session_date.to_string())
            .collect::<Vec<_>>(),
    );
    let symbol_contracts = StringArray::from(
        rows.iter()
            .map(|row| row.symbol_contract.clone())
            .collect::<Vec<_>>(),
    );
    let timeframes = StringArray::from(
        rows.iter()
            .map(|row| row.timeframe.clone())
            .collect::<Vec<_>>(),
    );
    let opens = Float64Array::from(rows.iter().map(|row| row.open).collect::<Vec<_>>());
    let highs = Float64Array::from(rows.iter().map(|row| row.high).collect::<Vec<_>>());
    let lows = Float64Array::from(rows.iter().map(|row| row.low).collect::<Vec<_>>());
    let closes = Float64Array::from(rows.iter().map(|row| row.close).collect::<Vec<_>>());
    let volumes = Float64Array::from(rows.iter().map(|row| row.volume).collect::<Vec<_>>());
    let trade_counts =
        UInt64Array::from(rows.iter().map(|row| row.trade_count).collect::<Vec<_>>());

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ts) as ArrayRef,
            Arc::new(session_dates),
            Arc::new(symbol_contracts),
            Arc::new(timeframes),
            Arc::new(opens),
            Arc::new(highs),
            Arc::new(lows),
            Arc::new(closes),
            Arc::new(volumes),
            Arc::new(trade_counts),
        ],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn write_ticks_parquet(path: &PathBuf, rows: &[market::TickRecord]) -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::<str>::from("UTC"))),
            false,
        ),
        Field::new("session_date", DataType::Utf8, false),
        Field::new("symbol_contract", DataType::Utf8, false),
        Field::new("trade_price", DataType::Float64, false),
        Field::new("trade_size", DataType::Float64, false),
        Field::new("bid_price", DataType::Float64, true),
        Field::new("ask_price", DataType::Float64, true),
    ]));

    let ts = TimestampMicrosecondArray::from(
        rows.iter()
            .map(|row| row.ts.timestamp_micros())
            .collect::<Vec<_>>(),
    );
    let session_dates = StringArray::from(
        rows.iter()
            .map(|row| row.session_date.to_string())
            .collect::<Vec<_>>(),
    );
    let symbol_contracts = StringArray::from(
        rows.iter()
            .map(|row| row.symbol_contract.clone())
            .collect::<Vec<_>>(),
    );
    let trade_prices =
        Float64Array::from(rows.iter().map(|row| row.trade_price).collect::<Vec<_>>());
    let trade_sizes = Float64Array::from(rows.iter().map(|row| row.trade_size).collect::<Vec<_>>());
    let bid_prices = Float64Array::from(rows.iter().map(|row| row.bid_price).collect::<Vec<_>>());
    let ask_prices = Float64Array::from(rows.iter().map(|row| row.ask_price).collect::<Vec<_>>());

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ts) as ArrayRef,
            Arc::new(session_dates),
            Arc::new(symbol_contracts),
            Arc::new(trade_prices),
            Arc::new(trade_sizes),
            Arc::new(bid_prices),
            Arc::new(ask_prices),
        ],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn write_backtest_trades_parquet(
    path: &PathBuf,
    rows: &[BacktestTradeRecord],
) -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, false),
        Field::new("symbol_contract", DataType::Utf8, false),
        Field::new("entry_ts", DataType::Utf8, true),
        Field::new("exit_ts", DataType::Utf8, true),
        Field::new("entry_price", DataType::Float64, true),
        Field::new("exit_price", DataType::Float64, true),
        Field::new("qty", DataType::Float64, true),
        Field::new("pnl", DataType::Float64, true),
        Field::new("notes_json", DataType::Utf8, false),
        Field::new("created_at", DataType::Utf8, false),
    ]));

    let ids = StringArray::from(rows.iter().map(|row| row.id.to_string()).collect::<Vec<_>>());
    let run_ids = StringArray::from(rows.iter().map(|row| row.run_id.to_string()).collect::<Vec<_>>());
    let symbol_contracts = StringArray::from(
        rows.iter()
            .map(|row| row.symbol_contract.clone())
            .collect::<Vec<_>>(),
    );
    let entry_ts = StringArray::from(
        rows.iter()
            .map(|row| row.entry_ts.map(|value| value.to_rfc3339()))
            .collect::<Vec<_>>(),
    );
    let exit_ts = StringArray::from(
        rows.iter()
            .map(|row| row.exit_ts.map(|value| value.to_rfc3339()))
            .collect::<Vec<_>>(),
    );
    let entry_prices = Float64Array::from(rows.iter().map(|row| row.entry_price).collect::<Vec<_>>());
    let exit_prices = Float64Array::from(rows.iter().map(|row| row.exit_price).collect::<Vec<_>>());
    let qty = Float64Array::from(rows.iter().map(|row| row.qty).collect::<Vec<_>>());
    let pnl = Float64Array::from(rows.iter().map(|row| row.pnl).collect::<Vec<_>>());
    let notes = StringArray::from(
        rows.iter()
            .map(|row| row.notes_json.to_string())
            .collect::<Vec<_>>(),
    );
    let created_at = StringArray::from(
        rows.iter()
            .map(|row| row.created_at.to_rfc3339())
            .collect::<Vec<_>>(),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(run_ids),
            Arc::new(symbol_contracts),
            Arc::new(entry_ts),
            Arc::new(exit_ts),
            Arc::new(entry_prices),
            Arc::new(exit_prices),
            Arc::new(qty),
            Arc::new(pnl),
            Arc::new(notes),
            Arc::new(created_at),
        ],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
