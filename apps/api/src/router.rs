use std::path::PathBuf;

use anyhow::Result;
use app_core::{
    config::Settings,
    error::{ApiError, ApiResult},
};
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderValue, StatusCode},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use backtest::{list_backtest_strategies, BacktestJobRequest, BacktestTradeRecord, PgBacktestStore};
use jobs::{CreateJobInput, JobSubmitted, JobType, ListDatasetExportsInput, ListJobsInput, PgJobStore};
use market::{AreaProfileQuery, BarsQuery, ClickHouseMarketStore, LargeOrdersQuery, PresetProfileQuery, TicksQuery};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::postgres::PgPoolOptions;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    settings: Settings,
    jobs: PgJobStore,
    market: ClickHouseMarketStore,
    backtests: PgBacktestStore,
}

#[derive(Debug, Deserialize)]
struct IngestionJobRequest {
    file_path: String,
    symbol_contract: Option<String>,
    #[serde(default)]
    rebuild: bool,
}

#[derive(Debug, Deserialize)]
struct DatasetJobRequest {
    export_kind: String,
    payload: Value,
}

#[derive(Debug, Deserialize)]
struct DatasetExportsQuery {
    export_kind: Option<String>,
    job_id: Option<Uuid>,
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ReplayJobRequest {
    #[serde(default)]
    allow_any_status: bool,
}

#[derive(Debug, Deserialize)]
struct RebuildMarketRequest {
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
    tick_size: Option<f64>,
    large_orders_threshold: Option<f64>,
    profile_timezone: Option<String>,
    #[serde(default = "default_rebuild_target")]
    target: String,
}

#[derive(Debug, Deserialize)]
struct JobsListQuery {
    status: Option<String>,
    job_type: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
}

pub async fn build_router(settings: Settings) -> Result<Router> {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_lazy(&settings.database_url)?;

    let state = AppState {
        settings: settings.clone(),
        jobs: PgJobStore::new(pool.clone()),
        market: ClickHouseMarketStore::new(&settings.clickhouse_url, &settings.clickhouse_database),
        backtests: PgBacktestStore::new(pool),
    };

    let api_v1 = Router::new()
        .route("/ingestion/jobs", post(create_ingestion_job))
        .route("/jobs", get(list_jobs))
        .route("/jobs/:job_id", get(get_job))
        .route("/jobs/:job_id/replay", post(replay_job))
        .route("/symbols", get(list_symbols))
        .route("/markets/:symbol/rebuild/jobs", post(create_market_rebuild_jobs))
        .route("/markets/:symbol/ticks", get(get_ticks))
        .route("/markets/:symbol/bars", get(get_bars))
        .route("/markets/:symbol/large-orders", get(get_large_orders))
        .route("/markets/:symbol/profiles/preset", get(get_preset_profiles))
        .route("/markets/:symbol/profiles/area", get(get_area_profile))
        .route("/backtests/jobs", post(create_backtest_job))
        .route("/backtests/strategies", get(list_strategies))
        .route("/backtests/runs", get(list_backtest_runs))
        .route("/backtests/runs/:run_id", get(get_backtest_run))
        .route("/backtests/runs/:run_id/trades", get(get_backtest_trades))
        .route("/backtests/runs/:run_id/analytics", get(get_backtest_analytics))
        .route("/backtests/runs/:run_id/export/config.json", get(export_backtest_config))
        .route("/backtests/runs/:run_id/export/trades.csv", get(export_backtest_trades_csv))
        .route("/datasets/jobs", post(create_dataset_job))
        .route("/datasets/exports", get(list_dataset_exports))
        .with_state(state.clone());

    Ok(Router::new()
        .route("/health", get(health))
        .nest("/api/v1", api_v1)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state))
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "runtime-api",
    })
}

async fn create_ingestion_job(
    State(state): State<AppState>,
    Json(payload): Json<IngestionJobRequest>,
) -> ApiResult<(StatusCode, Json<JobSubmitted>)> {
    let root = state.settings.ingest_root.clone();
    let requested = PathBuf::from(&payload.file_path);
    let normalized = if requested.is_absolute() {
        requested
    } else {
        root.join(requested)
    };

    if !normalized.starts_with(&root) {
        return Err(ApiError::bad_request("file_path must stay inside the ingest root"));
    }
    if !normalized.exists() {
        return Err(ApiError::not_found("requested ingest file does not exist"));
    }

    let job = state
        .jobs
        .create_job(CreateJobInput {
            job_type: JobType::Ingestion,
            payload_json: json!({
                "file_path": normalized,
                "symbol_contract": payload.symbol_contract,
                "rebuild": payload.rebuild,
            }),
            max_attempts: 5,
        })
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;

    Ok((StatusCode::ACCEPTED, Json(JobSubmitted { job_id: job.id })))
}

async fn get_job(State(state): State<AppState>, Path(job_id): Path<Uuid>) -> ApiResult<Json<Value>> {
    let job = state
        .jobs
        .get_job(job_id)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?
        .ok_or_else(|| ApiError::not_found("job not found"))?;

    Ok(Json(json!(job)))
}

async fn list_jobs(State(state): State<AppState>, Query(query): Query<JobsListQuery>) -> ApiResult<Json<Value>> {
    let jobs = state
        .jobs
        .list_jobs(ListJobsInput {
            status: query.status.as_deref(),
            job_type: query.job_type.as_deref(),
            limit: query.limit.unwrap_or(50),
        })
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;

    Ok(Json(json!({ "jobs": jobs })))
}

async fn replay_job(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    Json(payload): Json<ReplayJobRequest>,
) -> ApiResult<(StatusCode, Json<JobSubmitted>)> {
    let source = state
        .jobs
        .get_job(job_id)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?
        .ok_or_else(|| ApiError::not_found("job not found"))?;

    if !payload.allow_any_status && !matches!(source.status.as_str(), "failed" | "dead_letter") {
        return Err(ApiError::bad_request("only failed or dead_letter jobs can be replayed by default"));
    }

    let replay = state
        .jobs
        .replay_job(job_id)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;

    Ok((StatusCode::ACCEPTED, Json(JobSubmitted { job_id: replay.id })))
}

async fn create_market_rebuild_jobs(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Json(payload): Json<RebuildMarketRequest>,
) -> ApiResult<(StatusCode, Json<Value>)> {
    if payload.end < payload.start {
        return Err(ApiError::bad_request("end must be after start"));
    }
    if !matches!(payload.target.as_str(), "bars" | "profiles" | "large_orders" | "all") {
        return Err(ApiError::bad_request(
            "target must be one of bars, profiles, large_orders, all",
        ));
    }

    let tick_size = payload.tick_size.unwrap_or_else(|| market::detect_tick_size(&symbol));
    let large_orders_threshold = payload.large_orders_threshold.unwrap_or(25.0);
    if large_orders_threshold <= 0.0 {
        return Err(ApiError::bad_request("large_orders_threshold must be greater than 0"));
    }
    let profile_timezone = payload.profile_timezone.unwrap_or_else(|| state.settings.dataset_timezone.name().to_string());
    if payload.target != "bars" {
        profile_timezone
            .parse::<chrono_tz::Tz>()
            .map_err(|_| ApiError::bad_request("profile_timezone must be a valid IANA timezone"))?;
    }

    let mut submitted = Vec::new();
    if matches!(payload.target.as_str(), "bars" | "large_orders" | "all") {
        let include_bars = matches!(payload.target.as_str(), "bars" | "all");
        let include_large_orders = matches!(payload.target.as_str(), "large_orders" | "all");
        let job = state
            .jobs
            .create_job(CreateJobInput {
                job_type: JobType::BuildBars,
                payload_json: json!({
                    "symbol_contract": symbol.clone(),
                    "start": payload.start,
                    "end": payload.end,
                    "tick_size": tick_size,
                    "large_orders_threshold": large_orders_threshold,
                    "build_time_bars": include_bars,
                    "build_non_time_bars": include_bars,
                    "build_large_orders": include_large_orders,
                    "rebuild": true,
                }),
                max_attempts: 3,
            })
            .await
            .map_err(|error| ApiError::internal(error.to_string()))?;
        submitted.push(json!({ "job_id": job.id, "job_type": "build_bars" }));
    }

    if matches!(payload.target.as_str(), "profiles" | "all") {
        let job = state
            .jobs
            .create_job(CreateJobInput {
                job_type: JobType::BuildProfiles,
                payload_json: json!({
                    "symbol_contract": symbol.clone(),
                    "start": payload.start,
                    "end": payload.end,
                    "tick_size": tick_size,
                    "profile_timezone": profile_timezone,
                    "rebuild": true,
                }),
                max_attempts: 3,
            })
            .await
            .map_err(|error| ApiError::internal(error.to_string()))?;
        submitted.push(json!({ "job_id": job.id, "job_type": "build_profiles" }));
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "symbol_contract": symbol,
            "start": payload.start,
            "end": payload.end,
            "large_orders_threshold": large_orders_threshold,
            "jobs": submitted,
        })),
    ))
}

async fn list_symbols(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    let symbols = state.market.list_symbols().await?;
    Ok(Json(json!({ "symbols": symbols })))
}

async fn get_ticks(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(query): Query<TicksQuery>,
) -> ApiResult<Json<Value>> {
    let ticks = state.market.load_ticks(&symbol, &query).await?;
    Ok(Json(json!({ "symbol_contract": symbol, "ticks": ticks })))
}

async fn get_bars(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(query): Query<BarsQuery>,
) -> ApiResult<Json<Value>> {
    query.validate()?;
    let bars = state.market.load_bars(&symbol, &query).await?;
    Ok(Json(json!({ "symbol_contract": symbol, "bars": bars })))
}

async fn get_large_orders(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(query): Query<LargeOrdersQuery>,
) -> ApiResult<Json<Value>> {
    query.validate()?;
    let large_orders = state.market.load_large_orders(&symbol, &query).await?;
    Ok(Json(json!({ "symbol_contract": symbol, "large_orders": large_orders })))
}

async fn get_preset_profiles(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(query): Query<PresetProfileQuery>,
) -> ApiResult<Json<Value>> {
    query.validate()?;
    let response = state.market.load_preset_profiles(&symbol, &query).await?;
    Ok(Json(json!(response)))
}

async fn get_area_profile(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(query): Query<AreaProfileQuery>,
) -> ApiResult<Json<Value>> {
    query.validate()?;
    let response = state.market.load_area_profile(&symbol, &query).await?;
    Ok(Json(json!(response)))
}

async fn create_backtest_job(
    State(state): State<AppState>,
    Json(payload): Json<BacktestJobRequest>,
) -> ApiResult<(StatusCode, Json<JobSubmitted>)> {
    let job = state
        .jobs
        .create_job(CreateJobInput {
            job_type: JobType::BacktestRun,
            payload_json: json!(payload),
            max_attempts: 3,
        })
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;

    Ok((StatusCode::ACCEPTED, Json(JobSubmitted { job_id: job.id })))
}

async fn list_strategies(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    Ok(Json(json!(list_backtest_strategies(
        state.settings.dataset_timezone,
    ))))
}

async fn list_backtest_runs(State(state): State<AppState>) -> ApiResult<Json<Value>> {
    let runs = state
        .backtests
        .list_runs()
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(Json(json!({ "runs": runs })))
}

async fn get_backtest_run(State(state): State<AppState>, Path(run_id): Path<Uuid>) -> ApiResult<Json<Value>> {
    let run = state
        .backtests
        .get_run(run_id)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    Ok(Json(json!(run)))
}

async fn get_backtest_trades(State(state): State<AppState>, Path(run_id): Path<Uuid>) -> ApiResult<Json<Value>> {
    let trades = state
        .backtests
        .get_run_trades(run_id)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(Json(json!({ "run_id": run_id, "trades": trades })))
}

async fn get_backtest_analytics(State(state): State<AppState>, Path(run_id): Path<Uuid>) -> ApiResult<Json<Value>> {
    let analytics = state
        .backtests
        .build_analytics(run_id)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(Json(json!({ "run_id": run_id, "analytics": analytics })))
}

async fn export_backtest_config(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    let run = state
        .backtests
        .get_run(run_id)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?
        .ok_or_else(|| ApiError::not_found("run not found"))?;

    Ok(Json(json!({
        "run_id": run.id,
        "name": run.name,
        "strategy_id": run.strategy_id,
        "params": run.params_json,
        "status": run.status,
        "created_at": run.created_at,
    })))
}

async fn export_backtest_trades_csv(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
) -> ApiResult<Response> {
    let run = state
        .backtests
        .get_run(run_id)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    let trades = state
        .backtests
        .get_run_trades(run.id)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;

    let csv_body = encode_backtest_trades_csv(&trades)
        .map_err(|error| ApiError::internal(error.to_string()))?;

    let mut response = Response::new(csv_body.into());
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/csv; charset=utf-8"),
    );
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"backtest-run-{run_id}-trades.csv\""))
            .map_err(|error| ApiError::internal(error.to_string()))?,
    );

    Ok(response)
}

async fn create_dataset_job(
    State(state): State<AppState>,
    Json(payload): Json<DatasetJobRequest>,
) -> ApiResult<(StatusCode, Json<JobSubmitted>)> {
    let job = state
        .jobs
        .create_job(CreateJobInput {
            job_type: JobType::DatasetExport,
            payload_json: json!({
                "export_kind": payload.export_kind,
                "payload": payload.payload,
                "artifact_root": state.settings.artifact_root,
            }),
            max_attempts: 3,
        })
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;

    Ok((StatusCode::ACCEPTED, Json(JobSubmitted { job_id: job.id })))
}

async fn list_dataset_exports(
    State(state): State<AppState>,
    Query(query): Query<DatasetExportsQuery>,
) -> ApiResult<Json<Value>> {
    let exports = state
        .jobs
        .list_dataset_exports(ListDatasetExportsInput {
            export_kind: query.export_kind.as_deref(),
            job_id: query.job_id,
            limit: query.limit.unwrap_or(50),
        })
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;

    Ok(Json(json!({ "exports": exports })))
}

fn default_rebuild_target() -> String {
    "all".to_string()
}

fn encode_backtest_trades_csv(trades: &[BacktestTradeRecord]) -> anyhow::Result<String> {
    let mut note_columns = std::collections::BTreeSet::new();
    let decoded_notes = trades
        .iter()
        .map(|trade| {
            let notes = flattenable_notes(&trade.notes_json);
            note_columns.extend(notes.keys().cloned());
            notes
        })
        .collect::<Vec<_>>();

    let mut writer = csv::Writer::from_writer(Vec::new());
    let mut headers = vec![
        "id".to_string(),
        "run_id".to_string(),
        "symbol_contract".to_string(),
        "entry_ts".to_string(),
        "exit_ts".to_string(),
        "entry_price".to_string(),
        "exit_price".to_string(),
        "qty".to_string(),
        "pnl".to_string(),
        "side".to_string(),
        "notes_json".to_string(),
    ];
    headers.extend(note_columns.iter().cloned());
    writer.write_record(&headers)?;

    for (trade, notes) in trades.iter().zip(decoded_notes.iter()) {
        let mut row = vec![
            trade.id.to_string(),
            trade.run_id.to_string(),
            trade.symbol_contract.clone(),
            trade.entry_ts.map(|value| value.to_rfc3339()).unwrap_or_default(),
            trade.exit_ts.map(|value| value.to_rfc3339()).unwrap_or_default(),
            trade.entry_price.map(|value| value.to_string()).unwrap_or_default(),
            trade.exit_price.map(|value| value.to_string()).unwrap_or_default(),
            trade.qty.map(|value| value.to_string()).unwrap_or_default(),
            trade.pnl.map(|value| value.to_string()).unwrap_or_default(),
            trade_side(trade).to_string(),
            serde_json::to_string(&trade.notes_json)?,
        ];
        row.extend(
            note_columns
                .iter()
                .map(|key| value_to_csv_cell(notes.get(key))),
        );
        writer.write_record(&row)?;
    }

    let bytes = writer.into_inner().map_err(|error| error.into_error())?;
    String::from_utf8(bytes).map_err(Into::into)
}

fn flattenable_notes(notes: &Value) -> Map<String, Value> {
    match notes {
        Value::Object(values) => values.clone(),
        _ => Map::new(),
    }
}

fn trade_side(trade: &BacktestTradeRecord) -> &'static str {
    match trade.qty.unwrap_or_default().partial_cmp(&0.0) {
        Some(std::cmp::Ordering::Greater) => "long",
        Some(std::cmp::Ordering::Less) => "short",
        _ => "flat",
    }
}

fn value_to_csv_cell(value: Option<&Value>) -> String {
    match value {
        Some(Value::Null) | None => String::new(),
        Some(Value::String(inner)) => inner.clone(),
        Some(Value::Bool(inner)) => inner.to_string(),
        Some(Value::Number(inner)) => inner.to_string(),
        Some(other) => serde_json::to_string(other).unwrap_or_default(),
    }
}
