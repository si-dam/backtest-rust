use std::path::PathBuf;

use anyhow::Result;
use app_core::{
    config::Settings,
    error::{ApiError, ApiResult},
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use backtest::{BacktestJobRequest, PgBacktestStore};
use jobs::{CreateJobInput, JobSubmitted, JobType, ListDatasetExportsInput, ListJobsInput, PgJobStore};
use market::{AreaProfileQuery, BarsQuery, ClickHouseMarketStore, LargeOrdersQuery, PresetProfileQuery, TicksQuery};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
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
        .route("/backtests/runs", get(list_backtest_runs))
        .route("/backtests/runs/:run_id", get(get_backtest_run))
        .route("/backtests/runs/:run_id/trades", get(get_backtest_trades))
        .route("/backtests/runs/:run_id/analytics", get(get_backtest_analytics))
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
    if !matches!(payload.target.as_str(), "bars" | "profiles" | "all") {
        return Err(ApiError::bad_request("target must be one of bars, profiles, all"));
    }

    let tick_size = payload.tick_size.unwrap_or_else(|| market::detect_tick_size(&symbol));
    let profile_timezone = payload.profile_timezone.unwrap_or_else(|| state.settings.dataset_timezone.name().to_string());
    if payload.target != "bars" {
        profile_timezone
            .parse::<chrono_tz::Tz>()
            .map_err(|_| ApiError::bad_request("profile_timezone must be a valid IANA timezone"))?;
    }

    let mut submitted = Vec::new();
    if matches!(payload.target.as_str(), "bars" | "all") {
        let job = state
            .jobs
            .create_job(CreateJobInput {
                job_type: JobType::BuildBars,
                payload_json: json!({
                    "symbol_contract": symbol.clone(),
                    "start": payload.start,
                    "end": payload.end,
                    "tick_size": tick_size,
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
            limit: query.limit.unwrap_or(50),
        })
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;

    Ok(Json(json!({ "exports": exports })))
}

fn default_rebuild_target() -> String {
    "all".to_string()
}
