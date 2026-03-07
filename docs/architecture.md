# Architecture

## Runtime split

- `runtime-api`: serves `/api/v1`, validates requests, and shapes responses.
- `runtime-worker`: leases jobs from Postgres and runs background tasks.
- `market`: owns ingest parsing, ClickHouse reads/writes, bar generation, and profile derivation.
- `jobs`: owns Postgres-backed queue primitives and job lifecycle.
- `backtest`: owns strategy execution and analytics.
- `frontend`: React/Vite application consuming `/api/v1`.

## Storage

- `Postgres` is the control plane for jobs, symbols, backtest metadata, and dataset exports.
- `ClickHouse` is the market-data plane for ticks, bars, profiles, and large orders.
- `artifacts/` is reserved for dataset exports and migration outputs.

## Migration stance

- The existing Python app under `reference/backtest` is the parity reference.
- The Rust API is versioned from day one under `/api/v1`.
- Python is intentionally removed from production request handling and kept only for offline ML and notebooks.

## Current vertical

- `POST /api/v1/ingestion/jobs` accepts a server-local file path and enqueues ingest work in Postgres.
- `runtime-worker` parses supported `.csv` and tab-delimited `.txt` inputs, persists canonical ticks or 1m bars, records ingest metadata, and enqueues derived jobs.
- Derived jobs materialize:
  - time bars into `bars_time`
  - non-time bars into `bars_non_time`
  - persisted preset profile segments and base levels into `profile_segments` and `profile_levels_base`
  - fixed-threshold large orders into `large_orders`
- Rebuild semantics are now explicit:
  - the same source path and hash is treated as a no-op unless `rebuild=true`
  - rebuilds clear and replace affected bars/profiles in-range instead of appending duplicates
  - preset profile persistence now covers both `volume` and `delta`
- Job control semantics are now explicit:
  - stale `running` jobs become reclaimable when their lease expires
  - workers write stage-level progress details into `jobs.progress_json`
  - operators can inspect recent jobs with `GET /api/v1/jobs`
  - operators can clone a failed/dead-letter job with `POST /api/v1/jobs/:job_id/replay`
  - operators can queue derived rebuild jobs with `POST /api/v1/markets/:symbol/rebuild/jobs`
  - rebuild jobs can target bars, profiles, or large-order overlays independently
- The first strategy path now exists in Rust:
  - `POST /api/v1/backtests/jobs` can queue `orb_breakout_v1`
  - `runtime-worker` loads persisted time bars from ClickHouse and runs the ORB engine in `crates/backtest`
  - a single ORB job can optionally split the requested window into `[IS]` and `[OOS]` runs with shared split metadata
  - run metadata and trades persist to Postgres and are exposed through `/api/v1/backtests/runs/*`
  - `crates/backtest/tests/fixtures/` contains reference-style ORB cases used to keep the Rust implementation aligned with the Python behavior
- The first dataset export path now exists:
  - `POST /api/v1/datasets/jobs` supports `export_kind = "bars"`, `export_kind = "ticks"`, `export_kind = "preset_profiles"`, and `export_kind = "backtest_trades"`
  - the worker reads persisted bars, ticks, or preset profile segments from ClickHouse, or persisted run trades from Postgres, writes Parquet plus a manifest under `artifacts/`, and records the export in Postgres
  - `python/examples/read_bars_export.py` is the first offline consumer example for the Python side
- The frontend currently exercises three workflows:
  - submit an ingest job and poll `/api/v1/jobs/:job_id`
  - inspect, replay, and rebuild jobs through the control-plane UI
  - read persisted bars for charting
  - read persisted fixed-threshold large-order rows
  - read persisted preset profiles and ad hoc area profiles with client-side filter controls
  - submit and inspect ORB backtest runs
- `crates/market/tests/fixtures/` contains the first golden dataset used to lock down bar/profile derivation behavior during the rewrite, including non-time bar variants and week/RTH/ETH profile cases.
