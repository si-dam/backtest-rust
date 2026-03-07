# backtest-rust

Clean-slate rewrite of the backtest platform with:

- Rust runtime services
- ClickHouse for market data
- Postgres for jobs and metadata
- React/Vite frontend
- Python reserved for offline ML research and training

The imported Python application is preserved under `reference/backtest/` as the source system for parity checks and migration work.

## Workspace

- `apps/api`: Axum HTTP API serving `/api/v1`
- `apps/worker`: Rust worker that leases jobs from Postgres
- `crates/app-core`: shared config, errors, and telemetry
- `crates/market`: market-data queries, profile logic, and ingest kernels
- `crates/jobs`: Postgres-backed job model and queue primitives
- `crates/backtest`: backtest models and analytics scaffolding
- `frontend`: React/Vite shell for the new UI
- `db/postgres/migrations`: control-plane schema
- `db/clickhouse/migrations`: market-data schema
- `reference/backtest`: imported Python reference codebase

## Local development

1. Install the pinned toolchains with `mise`:
   - `mise trust`
   - `mise install`
   - If you already installed an older pinned Rust toolchain for this repo, rerun `mise install` after pulling changes.
2. Copy `.env.example` to `.env`.
3. Install frontend dependencies in `frontend/` with `npm install`.
4. Run:
   - `cargo run -p runtime-api`
   - `cargo run -p runtime-worker`
   - `npm run dev` from `frontend/`

This repo does not assume a local Docker stack. Postgres and ClickHouse can be attached later from whatever local or remote instances you prefer.

## Current state

This repo now contains the phase-1 foundation plus the first API/frontend scaffold. The Rust toolchain is not installed in this environment, so the Rust side was built manually and could not be compiled here.
