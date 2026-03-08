use anyhow::{anyhow, bail, Context};
use chrono::{DateTime, Datelike, Duration, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

const ALLOWED_TIMEFRAMES: &[&str] = &["1m", "3m", "5m", "15m", "30m", "60m"];

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BacktestJobRequest {
    #[serde(default = "default_backtest_mode")]
    pub mode: String,
    pub name: String,
    pub strategy_id: String,
    #[serde(default)]
    pub params: Value,
}

fn default_backtest_mode() -> String {
    "run".to_string()
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopMode {
    OrBoundary,
    OrMid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryMode {
    FirstOutside,
    ReentryAfterStop,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyMode {
    BreakoutOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Long,
    Short,
}

impl Side {
    fn as_str(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::Short => "short",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrbStrategyParams {
    pub symbol_contract: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub timeframe: String,
    pub ib_minutes: i64,
    pub rth_only: bool,
    pub session_start: String,
    pub session_end: String,
    pub stop_mode: StopMode,
    pub tp_r_multiple: f64,
    pub entry_mode: EntryMode,
    pub strategy_mode: StrategyMode,
    pub contracts: i32,
    pub timezone: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct StrategyMetadata {
    pub id: String,
    pub label: String,
    pub description: String,
    pub defaults: Value,
    pub params: Vec<StrategyParamMetadata>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StrategyParamMetadata {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyBar {
    pub ts: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyTrade {
    pub session_date: String,
    pub timeframe: String,
    pub ib_minutes: i64,
    pub side: String,
    pub entry_time: DateTime<Utc>,
    pub entry_price: f64,
    pub stop_price: f64,
    pub target_price: f64,
    pub exit_time: DateTime<Utc>,
    pub exit_price: f64,
    pub exit_reason: String,
    pub pnl: f64,
    pub r_multiple: f64,
}

#[derive(Clone, Debug)]
pub struct NewRunInput {
    pub job_id: Option<Uuid>,
    pub strategy_id: String,
    pub name: String,
    pub params_json: Value,
}

#[derive(Clone, Debug)]
pub struct NewTradeInput {
    pub symbol_contract: String,
    pub entry_ts: Option<DateTime<Utc>>,
    pub exit_ts: Option<DateTime<Utc>>,
    pub entry_price: Option<f64>,
    pub exit_price: Option<f64>,
    pub qty: Option<f64>,
    pub pnl: Option<f64>,
    pub notes_json: Value,
}

#[derive(Clone, Debug, Serialize)]
pub struct OrbRunSummary {
    pub strategy_id: String,
    pub run_id: Uuid,
    pub trade_count: usize,
    pub metrics: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrbSplitConfig {
    pub split_at: DateTime<Utc>,
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

    pub async fn create_run(&self, input: NewRunInput) -> anyhow::Result<BacktestRunRecord> {
        let run = sqlx::query_as::<_, BacktestRunRecord>(
            r#"
            INSERT INTO backtest_runs (job_id, strategy_id, name, status, params_json, metrics_json)
            VALUES ($1, $2, $3, 'running', $4, '{}'::jsonb)
            RETURNING id, job_id, strategy_id, name, status, params_json, metrics_json, created_at, updated_at
            "#,
        )
        .bind(input.job_id)
        .bind(input.strategy_id)
        .bind(input.name)
        .bind(input.params_json)
        .fetch_one(&self.pool)
        .await
        .context("failed to create backtest run")?;

        Ok(run)
    }

    pub async fn complete_run(&self, run_id: Uuid, metrics_json: Value) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE backtest_runs
            SET status = 'completed', metrics_json = $2, updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(run_id)
        .bind(metrics_json)
        .execute(&self.pool)
        .await
        .context("failed to complete run")?;
        Ok(())
    }

    pub async fn fail_run(&self, run_id: Uuid, error_message: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE backtest_runs
            SET status = 'failed', metrics_json = jsonb_build_object('error', $2), updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(run_id)
        .bind(error_message)
        .execute(&self.pool)
        .await
        .context("failed to fail run")?;
        Ok(())
    }

    pub async fn insert_trades(
        &self,
        run_id: Uuid,
        trades: &[NewTradeInput],
    ) -> anyhow::Result<()> {
        for trade in trades {
            sqlx::query(
                r#"
                INSERT INTO backtest_trades (run_id, symbol_contract, entry_ts, exit_ts, entry_price, exit_price, qty, pnl, notes_json)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                "#,
            )
            .bind(run_id)
            .bind(&trade.symbol_contract)
            .bind(trade.entry_ts)
            .bind(trade.exit_ts)
            .bind(trade.entry_price)
            .bind(trade.exit_price)
            .bind(trade.qty)
            .bind(trade.pnl)
            .bind(&trade.notes_json)
            .execute(&self.pool)
            .await
            .context("failed to insert backtest trade")?;
        }
        Ok(())
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
        let wins = trades
            .iter()
            .filter(|trade| trade.pnl.unwrap_or_default() > 0.0)
            .count();
        let losses = trades
            .iter()
            .filter(|trade| trade.pnl.unwrap_or_default() < 0.0)
            .count();
        let total_pnl = trades
            .iter()
            .map(|trade| trade.pnl.unwrap_or_default())
            .sum::<f64>();
        let avg_pnl = if trades.is_empty() {
            0.0
        } else {
            total_pnl / trades.len() as f64
        };

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

pub fn list_backtest_strategies(default_timezone: Tz) -> Vec<StrategyMetadata> {
    vec![orb_breakout_strategy_metadata(default_timezone)]
}

pub fn orb_breakout_strategy_metadata(default_timezone: Tz) -> StrategyMetadata {
    StrategyMetadata {
        id: "orb_breakout_v1".to_string(),
        label: "ORB Breakout V1".to_string(),
        description: "Opening-range breakout strategy using first candle close outside OR.".to_string(),
        defaults: json!({
            "name": "ORB Breakout V1",
            "timeframe": "1m",
            "ib_minutes": 15,
            "rth_only": true,
            "session_start": "09:30:00",
            "session_end": "16:00:00",
            "stop_mode": "or_boundary",
            "tp_r_multiple": 2.0,
            "entry_mode": "first_outside",
            "strategy_mode": "breakout_only",
            "contracts": 1,
            "timezone": default_timezone.name(),
        }),
        params: vec![
            string_param("symbol_contract", true, None),
            datetime_param("start"),
            datetime_param("end"),
            enum_param("timeframe", true, json!("1m"), ALLOWED_TIMEFRAMES),
            integer_param("ib_minutes", true, json!(15)),
            boolean_param("rth_only", true, json!(true)),
            string_param("session_start", true, Some(json!("09:30:00"))),
            string_param("session_end", true, Some(json!("16:00:00"))),
            enum_param("stop_mode", true, json!("or_boundary"), &["or_boundary", "or_mid"]),
            number_param("tp_r_multiple", true, json!(2.0)),
            enum_param(
                "entry_mode",
                true,
                json!("first_outside"),
                &["first_outside", "reentry_after_stop"],
            ),
            enum_param(
                "strategy_mode",
                true,
                json!("breakout_only"),
                &["breakout_only"],
            ),
            integer_param("contracts", false, json!(1)),
            string_param("timezone", false, Some(json!(default_timezone.name()))),
        ],
    }
}

pub fn merge_orb_params(params: &Value, default_timezone: Tz) -> anyhow::Result<OrbStrategyParams> {
    let get = |key: &str| params.get(key);
    let symbol_contract = get("symbol_contract")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    if symbol_contract.is_empty() {
        bail!("symbol_contract is required for orb_breakout_v1");
    }

    let start = parse_utc_datetime(get("start").context("start is required for orb_breakout_v1")?)?;
    let end = parse_utc_datetime(get("end").context("end is required for orb_breakout_v1")?)?;
    let (start, end) = if end < start {
        (end, start)
    } else {
        (start, end)
    };

    let timeframe = get("timeframe")
        .and_then(Value::as_str)
        .unwrap_or("1m")
        .to_lowercase();
    if !ALLOWED_TIMEFRAMES.contains(&timeframe.as_str()) {
        bail!("unsupported strategy timeframe: {timeframe}");
    }

    let ib_minutes = get("ib_minutes").and_then(Value::as_i64).unwrap_or(15);
    if ib_minutes <= 0 {
        bail!("ib_minutes must be greater than 0");
    }

    let rth_only = coerce_bool(get("rth_only"), true)?;
    let session_start = get("session_start")
        .and_then(Value::as_str)
        .unwrap_or(if rth_only { "09:30:00" } else { "00:00:00" })
        .to_string();
    let session_end = get("session_end")
        .and_then(Value::as_str)
        .unwrap_or(if rth_only { "16:00:00" } else { "23:59:59" })
        .to_string();
    parse_hhmmss(&session_start)?;
    parse_hhmmss(&session_end)?;

    let stop_mode = match get("stop_mode")
        .and_then(Value::as_str)
        .unwrap_or("or_boundary")
    {
        "or_boundary" => StopMode::OrBoundary,
        "or_mid" => StopMode::OrMid,
        other => bail!("stop_mode must be 'or_boundary' or 'or_mid', got {other}"),
    };

    let tp_r_multiple = get("tp_r_multiple").and_then(Value::as_f64).unwrap_or(2.0);
    if tp_r_multiple <= 0.0 {
        bail!("tp_r_multiple must be greater than 0");
    }

    let entry_mode = match get("entry_mode")
        .and_then(Value::as_str)
        .unwrap_or("first_outside")
    {
        "first_outside" => EntryMode::FirstOutside,
        "reentry_after_stop" => EntryMode::ReentryAfterStop,
        other => bail!("entry_mode must be 'first_outside' or 'reentry_after_stop', got {other}"),
    };

    let strategy_mode = match get("strategy_mode")
        .and_then(Value::as_str)
        .unwrap_or("breakout_only")
    {
        "breakout_only" => StrategyMode::BreakoutOnly,
        other => bail!("strategy_mode must be 'breakout_only', got {other}"),
    };

    let contracts = get("contracts").and_then(Value::as_i64).unwrap_or(1) as i32;
    if contracts <= 0 {
        bail!("contracts must be greater than 0");
    }

    let timezone = get("timezone")
        .and_then(Value::as_str)
        .unwrap_or(default_timezone.name())
        .to_string();
    timezone
        .parse::<Tz>()
        .map_err(|_| anyhow!("timezone must be a valid IANA timezone"))?;

    Ok(OrbStrategyParams {
        symbol_contract,
        start,
        end,
        timeframe,
        ib_minutes,
        rth_only,
        session_start,
        session_end,
        stop_mode,
        tp_r_multiple,
        entry_mode,
        strategy_mode,
        contracts,
        timezone,
    })
}

pub fn parse_orb_split_config(
    params: &Value,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> anyhow::Result<Option<OrbSplitConfig>> {
    let Some(split) = params.get("split") else {
        return Ok(None);
    };
    let Some(split) = split.as_object() else {
        return Ok(None);
    };
    if !coerce_bool(split.get("enabled"), false)? {
        return Ok(None);
    }

    let split_at = parse_utc_datetime(
        split
            .get("split_at")
            .context("split.split_at is required when split.enabled is true")?,
    )?;
    if split_at <= start || split_at >= end {
        bail!("split.split_at must be strictly between start and end");
    }

    Ok(Some(OrbSplitConfig { split_at }))
}

pub fn simulate_orb_breakout_strategy(
    bars: &[StrategyBar],
    params: &OrbStrategyParams,
) -> anyhow::Result<Vec<StrategyTrade>> {
    if bars.is_empty() {
        return Ok(Vec::new());
    }

    let timezone = params
        .timezone
        .parse::<Tz>()
        .map_err(|_| anyhow!("invalid strategy timezone: {}", params.timezone))?;
    let session_start = parse_hhmmss(&params.session_start)?;
    let session_end = parse_hhmmss(&params.session_end)?;

    let mut sorted = bars.to_vec();
    sorted.sort_by_key(|bar| bar.ts);
    let grouped = group_session_bars(&sorted, timezone, session_start, session_end);

    let mut trades = Vec::new();
    for day_bars in grouped {
        let Some(levels) =
            compute_opening_range(&day_bars, params.ib_minutes, session_start, timezone)
        else {
            continue;
        };

        let mut search_after: Option<DateTime<Utc>> = None;
        loop {
            let Some((entry_ts, side)) =
                find_first_breakout_signal(&day_bars, &levels, session_end, timezone, search_after)
            else {
                break;
            };

            let entry_bar = day_bars
                .iter()
                .find(|bar| bar.ts == entry_ts)
                .context("entry bar missing from grouped bars")?;
            let entry_price = entry_bar.close;
            let stop = stop_price(side, params.stop_mode, &levels);
            let risk = (entry_price - stop).abs();
            if risk <= 0.0 {
                break;
            }

            let target = match side {
                Side::Long => entry_price + params.tp_r_multiple * risk,
                Side::Short => entry_price - params.tp_r_multiple * risk,
            };

            let future = day_bars
                .iter()
                .filter(|bar| bar.ts > entry_ts)
                .cloned()
                .collect::<Vec<_>>();
            if future.is_empty() {
                break;
            }

            let (exit_ts, exit_price, exit_reason) =
                simulate_exit(&future, side, stop, target, session_end, timezone)?;
            let signed = if side == Side::Long { 1.0 } else { -1.0 };
            let pnl = (exit_price - entry_price) * signed * f64::from(params.contracts);
            let r_multiple = pnl / (risk * f64::from(params.contracts));

            let local_entry = entry_ts.with_timezone(&timezone);
            trades.push(StrategyTrade {
                session_date: local_entry.date_naive().to_string(),
                timeframe: params.timeframe.clone(),
                ib_minutes: params.ib_minutes,
                side: side.as_str().to_string(),
                entry_time: entry_ts,
                entry_price,
                stop_price: stop,
                target_price: target,
                exit_time: exit_ts,
                exit_price,
                exit_reason: exit_reason.to_string(),
                pnl,
                r_multiple,
            });

            if params.entry_mode != EntryMode::ReentryAfterStop || exit_reason != "stop" {
                break;
            }
            search_after = Some(exit_ts);
        }
    }

    Ok(trades)
}

pub fn summarize_breakout_trades(trades: &[StrategyTrade]) -> Value {
    if trades.is_empty() {
        return json!({
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "full_tp_wins": 0,
            "full_losses": 0,
            "win_rate": 0.0,
            "net_pnl": 0.0,
            "total_pnl": 0.0,
            "avg_pnl": 0.0,
            "total_r": 0.0,
            "avg_r": 0.0,
            "max_drawdown": 0.0,
        });
    }

    let wins = trades.iter().filter(|trade| trade.pnl > 0.0).count();
    let losses = trades.iter().filter(|trade| trade.pnl < 0.0).count();
    let full_tp_wins = trades
        .iter()
        .filter(|trade| trade.exit_reason == "target" && trade.pnl > 0.0)
        .count();
    let full_losses = trades
        .iter()
        .filter(|trade| trade.exit_reason == "stop" && trade.pnl < 0.0)
        .count();
    let total_pnl = trades.iter().map(|trade| trade.pnl).sum::<f64>();
    let total_r = trades.iter().map(|trade| trade.r_multiple).sum::<f64>();

    let mut running = 0.0_f64;
    let mut peak = 0.0_f64;
    let mut max_drawdown = 0.0_f64;
    for trade in trades {
        running += trade.pnl;
        peak = peak.max(running);
        max_drawdown = max_drawdown.max(peak - running);
    }

    json!({
        "trades": trades.len(),
        "wins": wins,
        "losses": losses,
        "full_tp_wins": full_tp_wins,
        "full_losses": full_losses,
        "win_rate": wins as f64 / trades.len() as f64,
        "net_pnl": total_pnl,
        "total_pnl": total_pnl,
        "avg_pnl": total_pnl / trades.len() as f64,
        "total_r": total_r,
        "avg_r": total_r / trades.len() as f64,
        "max_drawdown": max_drawdown,
    })
}

pub fn build_trade_records(
    run_id: Uuid,
    params: &OrbStrategyParams,
    trades: &[StrategyTrade],
) -> Vec<NewTradeInput> {
    trades
        .iter()
        .map(|trade| {
            let qty = if trade.side == "long" {
                f64::from(params.contracts)
            } else {
                -f64::from(params.contracts)
            };
            NewTradeInput {
                symbol_contract: params.symbol_contract.clone(),
                entry_ts: Some(trade.entry_time),
                exit_ts: Some(trade.exit_time),
                entry_price: Some(trade.entry_price),
                exit_price: Some(trade.exit_price),
                qty: Some(qty),
                pnl: Some(trade.pnl),
                notes_json: json!({
                    "run_id": run_id,
                    "session_date": trade.session_date,
                    "timeframe": trade.timeframe,
                    "ib_minutes": trade.ib_minutes,
                    "stop_price": trade.stop_price,
                    "target_price": trade.target_price,
                    "exit_reason": trade.exit_reason,
                    "r_multiple": trade.r_multiple,
                    "contracts": params.contracts,
                    "side": trade.side,
                }),
            }
        })
        .collect()
}

fn parse_utc_datetime(value: &Value) -> anyhow::Result<DateTime<Utc>> {
    let raw = value.as_str().context("datetime values must be strings")?;
    let parsed =
        DateTime::parse_from_rfc3339(raw).context("datetime must include timezone offset")?;
    Ok(parsed.with_timezone(&Utc))
}

fn coerce_bool(value: Option<&Value>, default: bool) -> anyhow::Result<bool> {
    let Some(value) = value else {
        return Ok(default);
    };
    if let Some(value) = value.as_bool() {
        return Ok(value);
    }
    match value
        .as_str()
        .unwrap_or_default()
        .trim()
        .to_lowercase()
        .as_str()
    {
        "1" | "true" | "t" | "yes" | "y" | "on" => Ok(true),
        "0" | "false" | "f" | "no" | "n" | "off" => Ok(false),
        _ => bail!("invalid boolean value: {value}"),
    }
}

fn parse_hhmmss(value: &str) -> anyhow::Result<NaiveTime> {
    NaiveTime::parse_from_str(value, "%H:%M:%S")
        .with_context(|| format!("invalid HH:MM:SS time: {value}"))
}

fn string_param(name: &str, required: bool, default: Option<Value>) -> StrategyParamMetadata {
    StrategyParamMetadata {
        name: name.to_string(),
        kind: "string".to_string(),
        required,
        default,
        options: None,
    }
}

fn datetime_param(name: &str) -> StrategyParamMetadata {
    StrategyParamMetadata {
        name: name.to_string(),
        kind: "datetime".to_string(),
        required: true,
        default: None,
        options: None,
    }
}

fn integer_param(name: &str, required: bool, default: Value) -> StrategyParamMetadata {
    StrategyParamMetadata {
        name: name.to_string(),
        kind: "integer".to_string(),
        required,
        default: Some(default),
        options: None,
    }
}

fn number_param(name: &str, required: bool, default: Value) -> StrategyParamMetadata {
    StrategyParamMetadata {
        name: name.to_string(),
        kind: "number".to_string(),
        required,
        default: Some(default),
        options: None,
    }
}

fn boolean_param(name: &str, required: bool, default: Value) -> StrategyParamMetadata {
    StrategyParamMetadata {
        name: name.to_string(),
        kind: "boolean".to_string(),
        required,
        default: Some(default),
        options: None,
    }
}

fn enum_param(
    name: &str,
    required: bool,
    default: Value,
    options: &[&str],
) -> StrategyParamMetadata {
    StrategyParamMetadata {
        name: name.to_string(),
        kind: "enum".to_string(),
        required,
        default: Some(default),
        options: Some(options.iter().map(|value| (*value).to_string()).collect()),
    }
}

#[derive(Clone)]
struct OrLevels {
    or_high: f64,
    or_low: f64,
    or_mid: f64,
    ib_end: DateTime<Utc>,
}

fn group_session_bars(
    bars: &[StrategyBar],
    timezone: Tz,
    session_start: NaiveTime,
    session_end: NaiveTime,
) -> Vec<Vec<StrategyBar>> {
    let mut grouped = std::collections::BTreeMap::<chrono::NaiveDate, Vec<StrategyBar>>::new();
    for bar in bars {
        let local = bar.ts.with_timezone(&timezone);
        let time = local.time();
        if time < session_start || time > session_end {
            continue;
        }
        grouped
            .entry(local.date_naive())
            .or_default()
            .push(bar.clone());
    }
    grouped.into_values().collect()
}

fn compute_opening_range(
    day_bars: &[StrategyBar],
    ib_minutes: i64,
    session_start: NaiveTime,
    timezone: Tz,
) -> Option<OrLevels> {
    let first = day_bars.first()?;
    let local_first = first.ts.with_timezone(&timezone);
    let session_start_dt = timezone
        .with_ymd_and_hms(
            local_first.year(),
            local_first.month(),
            local_first.day(),
            session_start.hour(),
            session_start.minute(),
            session_start.second(),
        )
        .single()?;
    let ib_end = session_start_dt + Duration::minutes(ib_minutes);

    let ib_slice = day_bars
        .iter()
        .filter(|bar| {
            let local = bar.ts.with_timezone(&timezone);
            local > session_start_dt && local <= ib_end
        })
        .collect::<Vec<_>>();
    if ib_slice.is_empty() {
        return None;
    }

    let or_high = ib_slice.iter().map(|bar| bar.high).fold(f64::MIN, f64::max);
    let or_low = ib_slice.iter().map(|bar| bar.low).fold(f64::MAX, f64::min);
    Some(OrLevels {
        or_high,
        or_low,
        or_mid: (or_high + or_low) / 2.0,
        ib_end: ib_end.with_timezone(&Utc),
    })
}

fn find_first_breakout_signal(
    day_bars: &[StrategyBar],
    levels: &OrLevels,
    session_end: NaiveTime,
    timezone: Tz,
    start_after: Option<DateTime<Utc>>,
) -> Option<(DateTime<Utc>, Side)> {
    let start_ts = start_after.map_or(levels.ib_end, |value| value.max(levels.ib_end));
    for bar in day_bars.iter().filter(|bar| {
        let local = bar.ts.with_timezone(&timezone);
        bar.ts > start_ts && local.time() <= session_end
    }) {
        if bar.close > levels.or_high {
            return Some((bar.ts, Side::Long));
        }
        if bar.close < levels.or_low {
            return Some((bar.ts, Side::Short));
        }
    }
    None
}

fn stop_price(side: Side, mode: StopMode, levels: &OrLevels) -> f64 {
    match mode {
        StopMode::OrMid => levels.or_mid,
        StopMode::OrBoundary => match side {
            Side::Long => levels.or_low,
            Side::Short => levels.or_high,
        },
    }
}

fn simulate_exit(
    future_bars: &[StrategyBar],
    side: Side,
    stop: f64,
    target: f64,
    session_end: NaiveTime,
    timezone: Tz,
) -> anyhow::Result<(DateTime<Utc>, f64, &'static str)> {
    let scoped = future_bars
        .iter()
        .filter(|bar| bar.ts.with_timezone(&timezone).time() <= session_end)
        .collect::<Vec<_>>();
    if scoped.is_empty() {
        bail!("no bars available after entry to simulate exit");
    }

    for bar in &scoped {
        match side {
            Side::Long => {
                let hit_stop = bar.low <= stop;
                let hit_target = bar.high >= target;
                if hit_stop {
                    return Ok((bar.ts, stop, "stop"));
                }
                if hit_target {
                    return Ok((bar.ts, target, "target"));
                }
            }
            Side::Short => {
                let hit_stop = bar.high >= stop;
                let hit_target = bar.low <= target;
                if hit_stop {
                    return Ok((bar.ts, stop, "stop"));
                }
                if hit_target {
                    return Ok((bar.ts, target, "target"));
                }
            }
        }
    }

    let last = scoped.last().context("scoped session bars missing")?;
    Ok((last.ts, last.close, "session_close"))
}

#[cfg(test)]
mod tests {
    use super::{
        list_backtest_strategies, merge_orb_params, parse_orb_split_config,
        simulate_orb_breakout_strategy, summarize_breakout_trades, EntryMode,
        OrbStrategyParams, StopMode, StrategyBar,
    };
    use chrono::{TimeZone, Utc};
    use chrono_tz::America::New_York;
    use serde_json::json;

    fn base_params() -> OrbStrategyParams {
        merge_orb_params(
            &json!({
                "symbol_contract": "NQH6",
                "start": "2026-02-18T14:30:00Z",
                "end": "2026-02-18T21:00:00Z",
                "timeframe": "1m",
                "ib_minutes": 1,
                "session_start": "09:30:00",
                "session_end": "16:00:00",
                "stop_mode": "or_boundary",
                "tp_r_multiple": 2.0,
            }),
            New_York,
        )
        .unwrap()
    }

    fn bars() -> Vec<StrategyBar> {
        vec![
            StrategyBar {
                ts: Utc.with_ymd_and_hms(2026, 2, 18, 14, 31, 0).unwrap(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
                volume: 10.0,
            },
            StrategyBar {
                ts: Utc.with_ymd_and_hms(2026, 2, 18, 14, 32, 0).unwrap(),
                open: 100.0,
                high: 103.0,
                low: 100.0,
                close: 102.0,
                volume: 20.0,
            },
            StrategyBar {
                ts: Utc.with_ymd_and_hms(2026, 2, 18, 14, 33, 0).unwrap(),
                open: 102.0,
                high: 108.0,
                low: 101.0,
                close: 106.0,
                volume: 20.0,
            },
            StrategyBar {
                ts: Utc.with_ymd_and_hms(2026, 2, 18, 14, 34, 0).unwrap(),
                open: 106.0,
                high: 107.0,
                low: 105.0,
                close: 106.0,
                volume: 20.0,
            },
        ]
    }

    fn bars_reentry_case() -> Vec<StrategyBar> {
        vec![
            StrategyBar {
                ts: Utc.with_ymd_and_hms(2026, 2, 18, 14, 31, 0).unwrap(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
                volume: 10.0,
            },
            StrategyBar {
                ts: Utc.with_ymd_and_hms(2026, 2, 18, 14, 32, 0).unwrap(),
                open: 100.0,
                high: 103.0,
                low: 101.0,
                close: 102.0,
                volume: 20.0,
            },
            StrategyBar {
                ts: Utc.with_ymd_and_hms(2026, 2, 18, 14, 33, 0).unwrap(),
                open: 102.0,
                high: 103.0,
                low: 98.0,
                close: 99.0,
                volume: 20.0,
            },
            StrategyBar {
                ts: Utc.with_ymd_and_hms(2026, 2, 18, 14, 34, 0).unwrap(),
                open: 98.0,
                high: 99.0,
                low: 97.0,
                close: 98.0,
                volume: 20.0,
            },
            StrategyBar {
                ts: Utc.with_ymd_and_hms(2026, 2, 18, 14, 35, 0).unwrap(),
                open: 98.0,
                high: 99.0,
                low: 95.0,
                close: 96.0,
                volume: 20.0,
            },
            StrategyBar {
                ts: Utc.with_ymd_and_hms(2026, 2, 18, 14, 36, 0).unwrap(),
                open: 95.0,
                high: 94.0,
                low: 91.0,
                close: 92.0,
                volume: 20.0,
            },
        ]
    }

    #[test]
    fn orb_boundary_hits_target() {
        let trades = simulate_orb_breakout_strategy(&bars(), &base_params()).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].side, "long");
        assert_eq!(trades[0].stop_price, 99.0);
        assert_eq!(trades[0].exit_reason, "target");
    }

    #[test]
    fn orb_mid_hits_target() {
        let mut params = base_params();
        params.stop_mode = StopMode::OrMid;
        let trades = simulate_orb_breakout_strategy(&bars(), &params).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].stop_price, 100.0);
        assert_eq!(trades[0].exit_reason, "target");
    }

    #[test]
    fn orb_reentry_after_stop() {
        let mut params = base_params();
        params.entry_mode = EntryMode::ReentryAfterStop;
        let trades = simulate_orb_breakout_strategy(&bars_reentry_case(), &params).unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].side, "long");
        assert_eq!(trades[0].exit_reason, "stop");
        assert_eq!(trades[1].side, "short");
        assert_eq!(trades[1].exit_reason, "target");
    }

    #[test]
    fn summary_counts_trade_outcomes() {
        let trades = simulate_orb_breakout_strategy(&bars(), &base_params()).unwrap();
        let summary = summarize_breakout_trades(&trades);
        assert_eq!(summary["trades"], 1);
        assert_eq!(summary["wins"], 1);
        assert_eq!(summary["full_tp_wins"], 1);
    }

    #[test]
    fn split_config_is_parsed_when_enabled() {
        let params = base_params();
        let split = parse_orb_split_config(
            &json!({
                "split": {
                    "enabled": true,
                    "split_at": "2026-02-18T18:00:00Z"
                }
            }),
            params.start,
            params.end,
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            split.split_at,
            Utc.with_ymd_and_hms(2026, 2, 18, 18, 0, 0).unwrap()
        );
    }

    #[test]
    fn split_config_rejects_out_of_range_boundary() {
        let params = base_params();
        let error = parse_orb_split_config(
            &json!({
                "split": {
                    "enabled": true,
                    "split_at": "2026-02-18T14:00:00Z"
                }
            }),
            params.start,
            params.end,
        )
        .unwrap_err();

        assert!(error.to_string().contains("split.split_at"));
    }

    #[test]
    fn invalid_strategy_mode_is_rejected() {
        let error = merge_orb_params(
            &json!({
                "symbol_contract": "NQH6",
                "start": "2026-02-18T14:30:00Z",
                "end": "2026-02-18T21:00:00Z",
                "strategy_mode": "something_else",
            }),
            New_York,
        )
        .unwrap_err();

        assert!(error.to_string().contains("strategy_mode"));
    }

    #[test]
    fn strategy_metadata_exposes_orb_params() {
        let strategies = list_backtest_strategies(New_York);
        assert_eq!(strategies.len(), 1);
        assert_eq!(strategies[0].id, "orb_breakout_v1");
        assert!(strategies[0].params.iter().any(|param| param.name == "strategy_mode"));
    }
}
