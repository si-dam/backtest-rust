use std::{env, net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result};

#[derive(Clone, Debug)]
pub struct Settings {
    pub app_name: String,
    pub app_env: String,
    pub app_host: String,
    pub app_port: u16,
    pub frontend_port: u16,
    pub database_url: String,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub ingest_root: PathBuf,
    pub artifact_root: PathBuf,
    pub worker_poll_interval_ms: u64,
    pub rust_log: String,
}

impl Settings {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            app_name: env_var("APP_NAME", "backtest-rust"),
            app_env: env_var("APP_ENV", "development"),
            app_host: env_var("APP_HOST", "0.0.0.0"),
            app_port: env_u16("APP_PORT", 8080)?,
            frontend_port: env_u16("FRONTEND_PORT", 5173)?,
            database_url: env_var("DATABASE_URL", "postgres://backtest:backtest@localhost:5432/backtest"),
            clickhouse_url: env_var("CLICKHOUSE_URL", "http://localhost:8123"),
            clickhouse_database: env_var("CLICKHOUSE_DATABASE", "backtest"),
            ingest_root: PathBuf::from(env_var("INGEST_ROOT", "./data/watch")),
            artifact_root: PathBuf::from(env_var("ARTIFACT_ROOT", "./artifacts")),
            worker_poll_interval_ms: env_u64("WORKER_POLL_INTERVAL_MS", 1_500)?,
            rust_log: env_var("RUST_LOG", "info"),
        })
    }

    pub fn api_socket_addr(&self) -> Result<SocketAddr> {
        format!("{}:{}", self.app_host, self.app_port)
            .parse()
            .with_context(|| format!("invalid socket address {}:{}", self.app_host, self.app_port))
    }
}

fn env_var(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_u16(name: &str, default: u16) -> Result<u16> {
    env::var(name)
        .ok()
        .map(|value| value.parse::<u16>())
        .transpose()
        .with_context(|| format!("invalid u16 value for {name}"))?
        .map_or(Ok(default), Ok)
}

fn env_u64(name: &str, default: u64) -> Result<u64> {
    env::var(name)
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()
        .with_context(|| format!("invalid u64 value for {name}"))?
        .map_or(Ok(default), Ok)
}
