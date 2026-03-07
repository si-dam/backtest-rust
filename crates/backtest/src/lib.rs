use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BacktestJobRequest {
    pub name: String,
    pub strategy_id: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Clone, Debug, Serialize, sqlx::FromRow)]
pub struct BacktestRunRecord {
    pub id: Uuid,
    pub job_id: Option<Uuid>,
    pub strategy_id: String,
    pub name: String,
    pub status: String,
    pub params_json: Value,
    pub metrics_json: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, sqlx::FromRow)]
pub struct BacktestTradeRecord {
    pub id: Uuid,
    pub run_id: Uuid,
    pub symbol_contract: String,
    pub entry_ts: Option<DateTime<Utc>>,
    pub exit_ts: Option<DateTime<Utc>>,
    pub entry_price: Option<f64>,
    pub exit_price: Option<f64>,
    pub qty: Option<f64>,
    pub pnl: Option<f64>,
    pub notes_json: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub struct BacktestAnalytics {
    pub trades: usize,
    pub wins: usize,
    pub losses: usize,
    pub total_pnl: f64,
    pub avg_pnl: f64,
    pub max_drawdown: f64,
}

#[derive(Clone)]
pub struct PgBacktestStore {
    pool: PgPool,
}

impl PgBacktestStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn list_runs(&self) -> anyhow::Result<Vec<BacktestRunRecord>> {
        let runs = sqlx::query_as::<_, BacktestRunRecord>(
            r#"
            SELECT id, job_id, strategy_id, name, status, params_json, metrics_json, created_at, updated_at
            FROM backtest_runs
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to list runs")?;

        Ok(runs)
    }

    pub async fn get_run(&self, run_id: Uuid) -> anyhow::Result<Option<BacktestRunRecord>> {
        let run = sqlx::query_as::<_, BacktestRunRecord>(
            r#"
            SELECT id, job_id, strategy_id, name, status, params_json, metrics_json, created_at, updated_at
            FROM backtest_runs
            WHERE id = $1
            "#,
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to fetch run")?;

        Ok(run)
    }

    pub async fn get_run_trades(&self, run_id: Uuid) -> anyhow::Result<Vec<BacktestTradeRecord>> {
        let trades = sqlx::query_as::<_, BacktestTradeRecord>(
            r#"
            SELECT id, run_id, symbol_contract, entry_ts, exit_ts, entry_price, exit_price, qty, pnl, notes_json, created_at
            FROM backtest_trades
            WHERE run_id = $1
            ORDER BY entry_ts NULLS LAST, created_at
            "#,
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to fetch run trades")?;

        Ok(trades)
    }

    pub async fn build_analytics(&self, run_id: Uuid) -> anyhow::Result<BacktestAnalytics> {
        let trades = self.get_run_trades(run_id).await?;
        let wins = trades.iter().filter(|trade| trade.pnl.unwrap_or_default() > 0.0).count();
        let losses = trades.iter().filter(|trade| trade.pnl.unwrap_or_default() < 0.0).count();
        let total_pnl = trades.iter().map(|trade| trade.pnl.unwrap_or_default()).sum::<f64>();
        let avg_pnl = if trades.is_empty() { 0.0 } else { total_pnl / trades.len() as f64 };

        let mut running = 0.0_f64;
        let mut peak = 0.0_f64;
        let mut max_drawdown = 0.0_f64;
        for trade in &trades {
            running += trade.pnl.unwrap_or_default();
            peak = peak.max(running);
            max_drawdown = max_drawdown.max(peak - running);
        }

        Ok(BacktestAnalytics {
            trades: trades.len(),
            wins,
            losses,
            total_pnl,
            avg_pnl,
            max_drawdown,
        })
    }
}
