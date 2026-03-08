use std::env;

use anyhow::Result;
use app_core::config::Settings;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use backtest::{NewRunInput, NewTradeInput, PgBacktestStore};
use chrono::TimeZone;
use jobs::{JobType, PgJobStore};
use runtime_api::router::build_router;
use serde_json::{json, Value};
use sqlx::{migrate::Migrator, postgres::PgPoolOptions};
use tower::util::ServiceExt;
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("../../db/postgres/migrations");

async fn test_pool_and_settings() -> Result<Option<(sqlx::PgPool, Settings)>> {
    let Ok(database_url) = env::var("DATABASE_URL") else {
        eprintln!("Skipping backtests_api test: DATABASE_URL is not set");
        return Ok(None);
    };

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?;
    MIGRATOR.run(&pool).await?;

    let settings = Settings {
        app_name: "backtest-rust-test".to_string(),
        app_env: "test".to_string(),
        app_host: "127.0.0.1".to_string(),
        app_port: 8080,
        frontend_port: 5173,
        database_url,
        clickhouse_url: "http://127.0.0.1:8123".to_string(),
        clickhouse_database: "backtest".to_string(),
        ingest_root: env::temp_dir().join("backtest-rust-ingest"),
        artifact_root: env::temp_dir().join("backtest-rust-artifacts"),
        dataset_timezone: "America/New_York".parse()?,
        worker_poll_interval_ms: 250,
        rust_log: "error".to_string(),
    };

    Ok(Some((pool, settings)))
}

async fn json_body(response: axum::response::Response) -> Result<Value> {
    let bytes = to_bytes(response.into_body(), usize::MAX).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

#[tokio::test]
async fn create_backtest_job_enqueues_runtime_job() -> Result<()> {
    let Some((pool, settings)) = test_pool_and_settings().await? else {
        return Ok(());
    };

    let app = build_router(settings).await?;
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/backtests/jobs")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "name": format!("API Job {}", Uuid::new_v4()),
                "strategy_id": "orb_breakout_v1",
                "params": {
                    "symbol_contract": "NQM6",
                    "start": "2026-02-18T14:30:00Z",
                    "end": "2026-02-18T21:00:00Z"
                }
            })
            .to_string(),
        ))?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = json_body(response).await?;
    let job_id = serde_json::from_value::<Uuid>(body["job_id"].clone())?;

    let jobs = PgJobStore::new(pool);
    let job = jobs.get_job(job_id).await?.expect("job should exist");
    assert_eq!(job.job_type, JobType::BacktestRun.as_str());
    assert_eq!(job.status, "queued");
    assert_eq!(job.payload_json["strategy_id"], "orb_breakout_v1");

    Ok(())
}

#[tokio::test]
async fn list_backtest_strategies_returns_orb_metadata() -> Result<()> {
    let Some((_pool, settings)) = test_pool_and_settings().await? else {
        return Ok(());
    };

    let app = build_router(settings).await?;
    let response = app
        .oneshot(Request::builder().uri("/api/v1/backtests/strategies").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = json_body(response).await?;
    let strategies = body.as_array().expect("strategies response should be an array");
    let orb = strategies
        .iter()
        .find(|strategy| strategy["id"] == "orb_breakout_v1")
        .expect("orb strategy should exist");
    assert_eq!(orb["label"], "ORB Breakout V1");
    assert!(orb["params"]
        .as_array()
        .expect("strategy params should be an array")
        .iter()
        .any(|param| param["name"] == "strategy_mode"));

    Ok(())
}

#[tokio::test]
async fn backtest_routes_serve_persisted_run_trade_and_analytics_data() -> Result<()> {
    let Some((pool, settings)) = test_pool_and_settings().await? else {
        return Ok(());
    };

    let run_tag = Uuid::new_v4();
    let store = PgBacktestStore::new(pool.clone());
    let run = store
        .create_run(NewRunInput {
            job_id: Some(run_tag),
            strategy_id: "orb_breakout_v1".to_string(),
            name: format!("Persisted Run {}", run_tag),
            params_json: json!({
                "symbol_contract": "NQM6",
                "test_id": run_tag,
            }),
        })
        .await?;
    store
        .insert_trades(
            run.id,
            &[
                NewTradeInput {
                    symbol_contract: "NQM6".to_string(),
                    entry_ts: Some(chrono::Utc.with_ymd_and_hms(2026, 2, 18, 14, 33, 0).unwrap()),
                    exit_ts: Some(chrono::Utc.with_ymd_and_hms(2026, 2, 18, 14, 35, 0).unwrap()),
                    entry_price: Some(102.0),
                    exit_price: Some(106.0),
                    qty: Some(1.0),
                    pnl: Some(4.0),
                    notes_json: json!({ "test_id": run_tag, "exit_reason": "target" }),
                },
                NewTradeInput {
                    symbol_contract: "NQM6".to_string(),
                    entry_ts: Some(chrono::Utc.with_ymd_and_hms(2026, 2, 19, 14, 33, 0).unwrap()),
                    exit_ts: Some(chrono::Utc.with_ymd_and_hms(2026, 2, 19, 14, 34, 0).unwrap()),
                    entry_price: Some(103.0),
                    exit_price: Some(101.5),
                    qty: Some(-1.0),
                    pnl: Some(-1.5),
                    notes_json: json!({ "test_id": run_tag, "exit_reason": "stop" }),
                },
            ],
        )
        .await?;
    store
        .complete_run(run.id, json!({ "test_id": run_tag, "trades": 2, "net_pnl": 2.5 }))
        .await?;

    let app = build_router(settings).await?;

    let runs_response = app
        .clone()
        .oneshot(Request::builder().uri("/api/v1/backtests/runs").body(Body::empty())?)
        .await?;
    assert_eq!(runs_response.status(), StatusCode::OK);
    let runs_body = json_body(runs_response).await?;
    let runs = runs_body["runs"].as_array().expect("runs response should be an array");
    assert!(runs.iter().any(|value| value["id"] == json!(run.id)));

    let run_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/backtests/runs/{}", run.id))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(run_response.status(), StatusCode::OK);
    let run_body = json_body(run_response).await?;
    assert_eq!(run_body["id"], json!(run.id));
    assert_eq!(run_body["status"], "completed");

    let trades_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/backtests/runs/{}/trades", run.id))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(trades_response.status(), StatusCode::OK);
    let trades_body = json_body(trades_response).await?;
    assert_eq!(trades_body["run_id"], json!(run.id));
    assert_eq!(trades_body["trades"].as_array().map(Vec::len), Some(2));

    let analytics_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/backtests/runs/{}/analytics", run.id))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(analytics_response.status(), StatusCode::OK);
    let analytics_body = json_body(analytics_response).await?;
    assert_eq!(analytics_body["run_id"], json!(run.id));
    assert_eq!(analytics_body["analytics"]["trades"], 2);
    assert_eq!(analytics_body["analytics"]["wins"], 1);
    assert_eq!(analytics_body["analytics"]["losses"], 1);
    assert_eq!(analytics_body["analytics"]["total_pnl"], 2.5);

    let config_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/backtests/runs/{}/export/config.json", run.id))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(config_response.status(), StatusCode::OK);
    let config_body = json_body(config_response).await?;
    assert_eq!(config_body["run_id"], json!(run.id));
    assert_eq!(config_body["strategy_id"], "orb_breakout_v1");

    let csv_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/backtests/runs/{}/export/trades.csv", run.id))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(csv_response.status(), StatusCode::OK);
    let content_type = csv_response
        .headers()
        .get("content-type")
        .expect("csv response should have content type")
        .to_str()?;
    assert!(content_type.starts_with("text/csv"));
    let csv_bytes = to_bytes(csv_response.into_body(), usize::MAX).await?;
    let csv_text = String::from_utf8(csv_bytes.to_vec())?;
    assert!(csv_text.contains("symbol_contract"));
    assert!(csv_text.contains("exit_reason"));
    assert!(csv_text.contains("r_multiple"));

    Ok(())
}
