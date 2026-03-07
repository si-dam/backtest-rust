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
