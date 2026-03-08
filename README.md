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
   - `cargo run -p runtime-validate -- <path-to-market-file.txt> --run-orb`
   - `npm run dev` from `frontend/`

This repo does not assume a local Docker stack. Postgres and ClickHouse can be attached later from whatever local or remote instances you prefer.

API liveness and readiness endpoints are separate:
- `GET /health` checks process liveness
- `GET /ready` checks Postgres and ClickHouse connectivity
- all API responses now include `x-request-id`
- `LOG_FORMAT=plain|json` controls human-readable versus structured JSON logs

Runtime startup now prepares `INGEST_ROOT` and `ARTIFACT_ROOT` automatically, and the API plus worker both shut down gracefully on `CTRL+C` or `SIGTERM`.

For local parity/performance checks on real files without a running DB, use `runtime-validate`. Example:

`cargo run -p runtime-validate -- data/NQ_sample.txt --symbol NQM6 --run-orb --strategy-mode big_order_required --big-trade-threshold 25`

It parses the source file and emits a JSON summary with row counts, timing for bar/profile/large-order construction, and optional ORB summary metrics.

## Current state

This repo now contains the phase-1 foundation and an in-progress phase-2 market-data slice:

- ingest jobs parse Sierra-style tick and 1m OHLC CSV inputs
- ingest parsing now accepts both comma-delimited `.csv` and tab-delimited `.txt` market exports
- the worker now handles duplicate source files intentionally:
  - identical re-submits are skipped
  - changed files require `rebuild=true`
  - rebuilds replace derived bars/profiles in-range instead of appending
- the job system is now harder to operate incorrectly:
  - expired `running` jobs can be reclaimed after lease expiry
  - workers publish stage-level progress into `progress_json`
  - jobs can be listed and filtered through `GET /api/v1/jobs`
  - failed or dead-letter jobs can be replayed through `/api/v1/jobs/:job_id/replay`
  - derived bars/profiles can be rebuilt by symbol and date range through `/api/v1/markets/:symbol/rebuild/jobs`
- the market crate can build time bars, non-time bars, and persisted base profile levels for both volume and delta profiles
- fixed-threshold large orders are also persisted during bar rebuilds, and rebuild controls can target `large_orders` directly or override the threshold used for those rows
- the frontend can now operate the control plane as well as the market views:
  - ingest files
  - inspect recent ingested file metadata
  - inspect/replay jobs
  - queue rebuilds
  - read market bars, preset profiles, and area profiles
- market read endpoints now enforce bounded result sets by default so large chart and overlay queries do not return unbounded rows
- the first Rust backtest path now exists for `orb_breakout_v1`:
  - the worker loads persisted time bars from ClickHouse
  - ORB simulation runs in `crates/backtest`
  - runs and trades persist in Postgres
  - `POST /api/v1/backtests/jobs` now supports both `mode: "run"` and `mode: "sweep"`
  - sweep jobs can fan out one ORB config across multiple symbols while keeping one job/result envelope
  - ORB now supports both `strategy_mode: "breakout_only"` and `strategy_mode: "big_order_required"`, with strict mode deriving big-print confirmation from ticks at the requested threshold during simulation
  - split runs are supported with paired `[IS]` and `[OOS]` segments from one backtest job payload
  - `GET /api/v1/backtests/strategies` now exposes strategy metadata/defaults for the current runtime
  - run configs and trades can be exported through `/api/v1/backtests/runs/{id}/export/config.json` and `/api/v1/backtests/runs/{id}/export/trades.csv`
  - the frontend can submit and inspect ORB runs or symbol sweeps, then download config/trade exports for the selected run
  - fixture-based parity cases for ORB now live under `crates/backtest/tests/`
  - Postgres-backed integration tests now cover persisted run/trade analytics in `crates/backtest/tests/runtime_store.rs` and the `/api/v1/backtests/*` read/write contract in `apps/api/tests/backtests_api.rs`
- the first dataset export path now exists:
  - `POST /api/v1/datasets/jobs` accepts `export_kind: "bars"`, `export_kind: "ticks"`, `export_kind: "preset_profiles"`, or `export_kind: "backtest_trades"`
  - the worker writes `bars.parquet`, `ticks.parquet`, `preset_profiles.parquet`, or `backtest_trades.parquet` plus `manifest.json` under `ARTIFACT_ROOT/dataset_exports/<job_id>/`
  - export metadata is recorded in `dataset_exports`
  - a minimal Python reader lives at `python/examples/read_bars_export.py`
- fixture-based golden tests now lock down time, tick, volume, and range bars plus both compact and multi-session preset-profile behavior in `crates/market/tests/`

The remaining work is to deepen backtest parity, expand dataset export coverage, and validate more of the rewrite against the imported Python reference app.
