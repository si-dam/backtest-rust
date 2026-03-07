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

#[derive(Clone, Debug, Serialize, sqlx::FromRow)]
pub struct IngestedFileRecord {
    pub id: Uuid,
    pub source_path: String,
    pub source_hash: String,
    pub schema_kind: String,
    pub symbol_contract: Option<String>,
    pub row_count: i64,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, sqlx::FromRow)]
pub struct DatasetExportRecord {
    pub id: Uuid,
    pub job_id: Option<Uuid>,
    pub export_kind: String,
    pub manifest_path: String,
    pub schema_version: String,
    pub payload_json: Value,
    pub created_at: DateTime<Utc>,
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

#[derive(Clone, Debug, Default)]
pub struct ListJobsInput<'a> {
    pub status: Option<&'a str>,
    pub job_type: Option<&'a str>,
    pub limit: i64,
}

#[derive(Clone, Debug)]
pub struct JobProgressUpdate {
    pub stage: String,
    pub detail_json: Value,
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
            VALUES (
                $1,
                'queued',
                $2,
                '{}'::jsonb,
                '{}'::jsonb,
                jsonb_build_object('stage', 'queued', 'job_type', $1),
                $3
            )
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

    pub async fn list_jobs(&self, input: ListJobsInput<'_>) -> anyhow::Result<Vec<JobRecord>> {
        let limit = if input.limit <= 0 { 50 } else { input.limit.min(200) };
        let records = sqlx::query_as::<_, JobRecord>(
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
            WHERE ($1::text IS NULL OR status = $1)
              AND ($2::text IS NULL OR job_type = $2)
            ORDER BY updated_at DESC, created_at DESC
            LIMIT $3
            "#,
        )
        .bind(input.status)
        .bind(input.job_type)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("failed to list jobs")?;

        Ok(records)
    }

    pub async fn get_ingested_file(&self, source_path: &str) -> anyhow::Result<Option<IngestedFileRecord>> {
        let record = sqlx::query_as::<_, IngestedFileRecord>(
            r#"
            SELECT
                id,
                source_path,
                source_hash,
                schema_kind,
                symbol_contract,
                row_count,
                created_at
            FROM ingested_files
            WHERE source_path = $1
            "#,
        )
        .bind(source_path)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load ingested file")?;

        Ok(record)
    }

    pub async fn record_dataset_export(
        &self,
        job_id: Option<Uuid>,
        export_kind: &str,
        manifest_path: &str,
        schema_version: &str,
        payload_json: Value,
    ) -> anyhow::Result<DatasetExportRecord> {
        let record = sqlx::query_as::<_, DatasetExportRecord>(
            r#"
            INSERT INTO dataset_exports (job_id, export_kind, manifest_path, schema_version, payload_json)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, job_id, export_kind, manifest_path, schema_version, payload_json, created_at
            "#,
        )
        .bind(job_id)
        .bind(export_kind)
        .bind(manifest_path)
        .bind(schema_version)
        .bind(payload_json)
        .fetch_one(&self.pool)
        .await
        .context("failed to record dataset export")?;

        Ok(record)
    }

    pub async fn claim_next_job(&self, worker_id: &str, lease_seconds: i64) -> anyhow::Result<Option<JobRecord>> {
        let record = sqlx::query_as::<_, JobRecord>(
            r#"
            WITH claimed AS (
                SELECT id
                FROM jobs
                WHERE (
                    status = 'queued'
                    OR (status = 'failed' AND (lease_until IS NULL OR lease_until < NOW()))
                    OR (status = 'running' AND lease_until < NOW())
                )
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
                progress_json = jsonb_build_object(
                    'stage',
                    'running',
                    'worker_id',
                    $1,
                    'attempt',
                    attempt + 1
                ),
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

    pub async fn update_progress(
        &self,
        job_id: Uuid,
        worker_id: &str,
        lease_seconds: i64,
        progress: JobProgressUpdate,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE jobs
            SET
                lease_until = NOW() + make_interval(secs => $3),
                progress_json = jsonb_build_object(
                    'stage', $4,
                    'worker_id', $2,
                    'detail', $5::jsonb,
                    'updated_at', NOW()
                ),
                updated_at = NOW()
            WHERE id = $1
              AND locked_by = $2
            "#,
        )
        .bind(job_id)
        .bind(worker_id)
        .bind(lease_seconds as i32)
        .bind(progress.stage)
        .bind(progress.detail_json)
        .execute(&self.pool)
        .await
        .context("failed to update job progress")?;

        Ok(())
    }

    pub async fn heartbeat(&self, job_id: Uuid, worker_id: &str, lease_seconds: i64, stage: &str) -> anyhow::Result<()> {
        self.update_progress(
            job_id,
            worker_id,
            lease_seconds,
            JobProgressUpdate {
                stage: stage.to_string(),
                detail_json: json!({}),
            },
        )
        .await
    }

    pub async fn complete_job(&self, job_id: Uuid, result_json: Value) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE jobs
            SET
                status = 'succeeded',
                result_json = $2,
                error_json = '{}'::jsonb,
                progress_json = jsonb_build_object('stage', 'completed', 'result', $2),
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
                progress_json = jsonb_build_object('stage', $2, 'error', jsonb_build_object('message', $3)),
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

    pub async fn replay_job(&self, job_id: Uuid) -> anyhow::Result<JobRecord> {
        let source = self
            .get_job(job_id)
            .await?
            .context("job not found")?;

        let replay = self
            .create_job(CreateJobInput {
                job_type: match source.job_type.as_str() {
                    "ingestion" => JobType::Ingestion,
                    "build_bars" => JobType::BuildBars,
                    "build_profiles" => JobType::BuildProfiles,
                    "backtest_run" => JobType::BacktestRun,
                    "dataset_export" => JobType::DatasetExport,
                    other => anyhow::bail!("unsupported job type for replay: {other}"),
                },
                payload_json: source.payload_json,
                max_attempts: source.max_attempts,
            })
            .await?;

        Ok(replay)
    }

    pub async fn upsert_symbol(
        &self,
        symbol_contract: &str,
        root_symbol: &str,
        exchange: Option<&str>,
        tick_size: Option<f64>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO symbols (symbol_contract, root_symbol, exchange, tick_size)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (symbol_contract)
            DO UPDATE SET
                root_symbol = EXCLUDED.root_symbol,
                exchange = EXCLUDED.exchange,
                tick_size = EXCLUDED.tick_size,
                updated_at = NOW()
            "#,
        )
        .bind(symbol_contract)
        .bind(root_symbol)
        .bind(exchange)
        .bind(tick_size)
        .execute(&self.pool)
        .await
        .context("failed to upsert symbol")?;

        Ok(())
    }

    pub async fn record_ingested_file(
        &self,
        source_path: &str,
        source_hash: &str,
        schema_kind: &str,
        symbol_contract: Option<&str>,
        row_count: i64,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO ingested_files (source_path, source_hash, schema_kind, symbol_contract, row_count)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (source_path)
            DO UPDATE SET
                source_hash = EXCLUDED.source_hash,
                schema_kind = EXCLUDED.schema_kind,
                symbol_contract = EXCLUDED.symbol_contract,
                row_count = EXCLUDED.row_count
            "#,
        )
        .bind(source_path)
        .bind(source_hash)
        .bind(schema_kind)
        .bind(symbol_contract)
        .bind(row_count)
        .execute(&self.pool)
        .await
        .context("failed to record ingested file")?;

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
