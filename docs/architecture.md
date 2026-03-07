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
- `runtime-worker` parses supported CSV inputs, persists canonical ticks or 1m bars, records ingest metadata, and enqueues derived jobs.
- Derived jobs materialize:
  - time bars into `bars_time`
  - non-time bars into `bars_non_time`
  - persisted preset profile segments and base levels into `profile_segments` and `profile_levels_base`
- The frontend currently exercises three workflows:
  - submit an ingest job and poll `/api/v1/jobs/:job_id`
  - read persisted bars for charting
  - read persisted preset profiles with client-side filter controls
