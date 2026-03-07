use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    Ingestion,
    BuildBars,
    BuildProfiles,
    BacktestRun,
    DatasetExport,
}

impl JobType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ingestion => "ingestion",
            Self::BuildBars => "build_bars",
            Self::BuildProfiles => "build_profiles",
            Self::BacktestRun => "backtest_run",
            Self::DatasetExport => "dataset_export",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    DeadLetter,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::DeadLetter => "dead_letter",
        }
    }
}

#[derive(Clone, Debug, Serialize, sqlx::FromRow)]
pub struct JobRecord {
    pub id: Uuid,
    pub job_type: String,
    pub status: String,
    pub payload_json: Value,
    pub result_json: Value,
    pub error_json: Value,
    pub progress_json: Value,
    pub attempt: i32,
    pub max_attempts: i32,
    pub lease_until: Option<DateTime<Utc>>,
    pub locked_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct CreateJobInput {
    pub job_type: JobType,
    pub payload_json: Value,
    pub max_attempts: i32,
}

#[derive(Clone, Debug, Serialize)]
pub struct JobSubmitted {
    pub job_id: Uuid,
}

#[derive(Clone)]
pub struct PgJobStore {
    pool: PgPool,
}

impl PgJobStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn create_job(&self, input: CreateJobInput) -> anyhow::Result<JobRecord> {
        let record = sqlx::query_as::<_, JobRecord>(
            r#"
            INSERT INTO jobs (job_type, status, payload_json, result_json, error_json, progress_json, max_attempts)
            VALUES ($1, 'queued', $2, '{}'::jsonb, '{}'::jsonb, '{"stage":"queued"}'::jsonb, $3)
            RETURNING
                id,
                job_type,
                status,
                payload_json,
                result_json,
                error_json,
                progress_json,
                attempt,
                max_attempts,
                lease_until,
                locked_by,
                created_at,
                updated_at
            "#,
        )
        .bind(input.job_type.as_str())
        .bind(input.payload_json)
        .bind(input.max_attempts)
        .fetch_one(&self.pool)
        .await
        .context("failed to create job")?;

        Ok(record)
    }

    pub async fn get_job(&self, job_id: Uuid) -> anyhow::Result<Option<JobRecord>> {
        let record = sqlx::query_as::<_, JobRecord>(
            r#"
            SELECT
                id,
                job_type,
                status,
                payload_json,
                result_json,
                error_json,
                progress_json,
                attempt,
                max_attempts,
                lease_until,
                locked_by,
                created_at,
                updated_at
            FROM jobs
            WHERE id = $1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load job")?;

        Ok(record)
    }

    pub async fn claim_next_job(&self, worker_id: &str, lease_seconds: i64) -> anyhow::Result<Option<JobRecord>> {
        let record = sqlx::query_as::<_, JobRecord>(
            r#"
            WITH claimed AS (
                SELECT id
                FROM jobs
                WHERE status IN ('queued', 'failed')
                  AND (lease_until IS NULL OR lease_until < NOW())
                  AND attempt < max_attempts
                ORDER BY created_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE jobs
            SET
                status = 'running',
                locked_by = $1,
                lease_until = NOW() + make_interval(secs => $2),
                attempt = attempt + 1,
                progress_json = jsonb_build_object('stage', 'running', 'worker_id', $1),
                updated_at = NOW()
            WHERE id IN (SELECT id FROM claimed)
            RETURNING
                id,
                job_type,
                status,
                payload_json,
                result_json,
                error_json,
                progress_json,
                attempt,
                max_attempts,
                lease_until,
                locked_by,
                created_at,
                updated_at
            "#,
        )
        .bind(worker_id)
        .bind(lease_seconds as i32)
        .fetch_optional(&self.pool)
        .await
        .context("failed to claim job")?;

        if let Some(record) = &record {
            self.record_attempt(record.id, record.attempt, worker_id, record.status.clone(), json!({})).await?;
        }

        Ok(record)
    }

    pub async fn heartbeat(&self, job_id: Uuid, worker_id: &str, lease_seconds: i64, stage: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE jobs
            SET
                lease_until = NOW() + make_interval(secs => $3),
                progress_json = jsonb_set(progress_json, '{stage}', to_jsonb($4::text), true),
                updated_at = NOW()
            WHERE id = $1
              AND locked_by = $2
            "#,
        )
        .bind(job_id)
        .bind(worker_id)
        .bind(lease_seconds as i32)
        .bind(stage)
        .execute(&self.pool)
        .await
        .context("failed to heartbeat job")?;

        Ok(())
    }

    pub async fn complete_job(&self, job_id: Uuid, result_json: Value) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE jobs
            SET
                status = 'succeeded',
                result_json = $2,
                error_json = '{}'::jsonb,
                progress_json = jsonb_build_object('stage', 'completed'),
                lease_until = NULL,
                locked_by = NULL,
                updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(job_id)
        .bind(result_json)
        .execute(&self.pool)
        .await
        .context("failed to complete job")?;

        Ok(())
    }

    pub async fn fail_job(&self, job_id: Uuid, error_message: &str) -> anyhow::Result<()> {
        let current = self.get_job(job_id).await?;
        let terminal_status = current
            .as_ref()
            .map(|job| if job.attempt >= job.max_attempts { "dead_letter" } else { "failed" })
            .unwrap_or("failed");

        sqlx::query(
            r#"
            UPDATE jobs
            SET
                status = $2,
                error_json = jsonb_build_object('message', $3),
                lease_until = NULL,
                locked_by = NULL,
                updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(job_id)
        .bind(terminal_status)
        .bind(error_message)
        .execute(&self.pool)
        .await
        .context("failed to fail job")?;

        Ok(())
    }

    async fn record_attempt(
        &self,
        job_id: Uuid,
        attempt: i32,
        worker_id: &str,
        status: String,
        detail_json: Value,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO job_attempts (job_id, attempt, worker_id, status, detail_json)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(job_id)
        .bind(attempt)
        .bind(worker_id)
        .bind(status)
        .bind(detail_json)
        .execute(&self.pool)
        .await
        .context("failed to record job attempt")?;

        Ok(())
    }
}
