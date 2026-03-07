use anyhow::Result;
use app_core::{config::Settings, telemetry::init_tracing};
use jobs::PgJobStore;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::from_env()?;
    init_tracing(&settings);

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_lazy(&settings.database_url)?;
    let jobs = PgJobStore::new(pool);
    let worker_id = format!("runtime-worker-{}", std::process::id());
    let poll_interval = Duration::from_millis(settings.worker_poll_interval_ms);

    tracing::info!(worker_id = %worker_id, "runtime-worker started");

    loop {
        if let Some(job) = jobs.claim_next_job(&worker_id, 30).await? {
            tracing::info!(job_id = %job.id, job_type = %job.job_type, "claimed job");
            jobs.heartbeat(job.id, &worker_id, 30, "dispatching").await?;
            let result = dispatch_job(&job.job_type, &job.payload_json).await;
            match result {
                Ok(summary) => jobs.complete_job(job.id, summary).await?,
                Err(error) => jobs.fail_job(job.id, &error).await?,
            }
            continue;
        }

        sleep(poll_interval).await;
    }
}

async fn dispatch_job(job_type: &str, payload: &serde_json::Value) -> std::result::Result<serde_json::Value, String> {
    match job_type {
        "ingestion" => Err(format!("ingestion worker not implemented yet: {payload}")),
        "build_bars" => Err(format!("bar build worker not implemented yet: {payload}")),
        "build_profiles" => Err(format!("profile build worker not implemented yet: {payload}")),
        "backtest_run" => Err(format!("backtest worker not implemented yet: {payload}")),
        "dataset_export" => Ok(json!({ "status": "queued_for_export_implementation", "payload": payload })),
        other => Err(format!("unknown job type: {other}")),
    }
}
