use std::env;

use anyhow::Result;
use backtest::{NewRunInput, NewTradeInput, PgBacktestStore};
use chrono::{TimeZone, Utc};
use serde_json::json;
use sqlx::{migrate::Migrator, postgres::PgPoolOptions};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("../../db/postgres/migrations");

async fn test_store() -> Result<Option<PgBacktestStore>> {
    let Ok(database_url) = env::var("DATABASE_URL") else {
        eprintln!("Skipping runtime_store test: DATABASE_URL is not set");
        return Ok(None);
    };

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?;
    MIGRATOR.run(&pool).await?;
    Ok(Some(PgBacktestStore::new(pool)))
}

#[tokio::test]
async fn persists_runs_trades_and_analytics() -> Result<()> {
    let Some(store) = test_store().await? else {
        return Ok(());
    };

    let test_id = Uuid::new_v4();
    let run = store
        .create_run(NewRunInput {
            job_id: Some(test_id),
            strategy_id: "orb_breakout_v1".to_string(),
            name: format!("Integration Runtime {}", test_id),
            params_json: json!({
                "symbol_contract": "NQM6",
                "timeframe": "1m",
                "test_id": test_id,
            }),
        })
        .await?;

    store
        .insert_trades(
            run.id,
            &[
                NewTradeInput {
                    symbol_contract: "NQM6".to_string(),
                    entry_ts: Some(Utc.with_ymd_and_hms(2026, 2, 18, 14, 33, 0).unwrap()),
                    exit_ts: Some(Utc.with_ymd_and_hms(2026, 2, 18, 14, 35, 0).unwrap()),
                    entry_price: Some(102.0),
                    exit_price: Some(106.0),
                    qty: Some(1.0),
                    pnl: Some(4.0),
                    notes_json: json!({ "test_id": test_id, "exit_reason": "target" }),
                },
                NewTradeInput {
                    symbol_contract: "NQM6".to_string(),
                    entry_ts: Some(Utc.with_ymd_and_hms(2026, 2, 19, 14, 33, 0).unwrap()),
                    exit_ts: Some(Utc.with_ymd_and_hms(2026, 2, 19, 14, 34, 0).unwrap()),
                    entry_price: Some(103.0),
                    exit_price: Some(101.5),
                    qty: Some(-1.0),
                    pnl: Some(-1.5),
                    notes_json: json!({ "test_id": test_id, "exit_reason": "stop" }),
                },
            ],
        )
        .await?;
    store
        .complete_run(
            run.id,
            json!({
                "test_id": test_id,
                "trades": 2,
                "net_pnl": 2.5,
            }),
        )
        .await?;

    let loaded_run = store.get_run(run.id).await?.expect("run should exist");
    assert_eq!(loaded_run.status, "completed");
    assert_eq!(loaded_run.metrics_json["trades"], 2);

    let trades = store.get_run_trades(run.id).await?;
    assert_eq!(trades.len(), 2);
    assert_eq!(trades[0].notes_json["test_id"], json!(test_id));

    let analytics = store.build_analytics(run.id).await?;
    assert_eq!(analytics.trades, 2);
    assert_eq!(analytics.wins, 1);
    assert_eq!(analytics.losses, 1);
    assert!((analytics.total_pnl - 2.5).abs() < f64::EPSILON);
    assert!((analytics.avg_pnl - 1.25).abs() < f64::EPSILON);
    assert!((analytics.max_drawdown - 1.5).abs() < f64::EPSILON);

    Ok(())
}
