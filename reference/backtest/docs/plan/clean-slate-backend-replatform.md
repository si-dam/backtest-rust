# Clean-Slate Backend Replatform Plan

## Rails API + Python Compute + ClickHouse/Postgres + DB-Backed Jobs (Single VM First)

## Summary
- Target architecture: `Rails API` as system-of-record and contract owner, `Python compute service` for ingest/strategy/analytics, `Postgres` for app/domain state, `ClickHouse` for time-series market data.
- Queue model: DB-backed only.
- Cutover model: big-bang, but only after feature parity and reliability gates.
- Goal: remove fragility from schema drift, weak operational boundaries, and ad-hoc job/data flow while preserving current frontend API behavior.

## Final Architecture
1. `Rails 8 API-only` (`ruby 3.3`) owns public API, auth/tenant boundaries, job lifecycle, and API contracts.
2. `Python 3.11 compute service` (FastAPI internal service) owns:
- CSV normalization
- bar/profile/overlay computation
- backtest/sweep execution
- analytics generation
3. Data stores:
- `Postgres 16`: users, tenants, jobs, run metadata, configuration, audit events.
- `ClickHouse 24.x`: ticks, bars, derived market overlays, high-volume trade/result facts.
4. DB-backed orchestration:
- Rails uses `Solid Queue` for API-originated workflow jobs.
- Cross-language compute queue is Postgres table `compute_tasks` consumed by Python workers via `FOR UPDATE SKIP LOCKED`.
5. Deployment:
- single VM with Docker Compose: `nginx`, `rails-api`, `python-compute`, `postgres`, `clickhouse`, `rails-worker`, `python-worker`.

## Public API and Interface Changes
- Keep existing frontend-facing paths and response envelopes for parity:
- `/ingest/*`
- `/backtests/*`
- `/chart/*`
- `/symbols/*`
- Add mandatory idempotency support on mutating endpoints:
- Header `Idempotency-Key` on `POST /ingest/jobs`, `POST /backtests/jobs`.
- Add auth endpoints for commercial readiness:
- `/auth/register`, `/auth/token`, `/auth/me`.
- Add internal interface (not public internet):
- `POST /internal/compute/tasks/:id/start`
- `POST /internal/compute/tasks/:id/heartbeat`
- `POST /internal/compute/tasks/:id/complete`
- `POST /internal/compute/tasks/:id/fail`
- Versioning policy:
- lock current contract as `v1` behavior-compatible.
- reject breaking payload changes unless behind explicit `v2`.

## Data Model and Storage Design
- Postgres tables:
- `users`, `tenants`, `memberships`, `jobs`, `compute_tasks`, `backtest_runs`, `backtest_run_exports`, `audit_events`.
- `compute_tasks` fields: `id`, `task_type`, `payload_json`, `status`, `attempt`, `max_attempts`, `lease_until`, `locked_by`, `error_json`, `created_at`, `updated_at`.
- ClickHouse tables:
- `ticks`, `bars`, `large_orders`, `session_profiles`, `backtest_trades_fact`, `backtest_equity_curve_fact`.
- Partition by trading date/month and order by `(symbol_contract, ts)` for chart latency.
- Migration from current system:
- export DuckDB data to Parquet.
- bulk load Parquet into ClickHouse.
- migrate Postgres metadata with deterministic ID preservation.
- validate row counts and checksum samples before cutover.

## Job Lifecycle and Failure Semantics
- State machine: `queued -> running -> succeeded|failed|dead_letter`.
- Lease + heartbeat required for running tasks.
- Retry policy:
- exponential backoff with jitter.
- max attempts by task type.
- dead-letter after exhaustion with preserved payload/error.
- Idempotency:
- dedupe by `(tenant_id, idempotency_key, endpoint)` for API jobs.
- dedupe compute task submission by `job_id + task_fingerprint`.

## Big-Bang Cutover Plan
1. Build target stack and implement full API parity in Rails.
2. Implement Python compute task consumer and internal callback contract.
3. Migrate historical data to ClickHouse/Postgres and validate.
4. Run full staging shadow validation against production-like replayed workloads.
5. Execute reliability gate suite (below) and freeze deploys.
6. Cutover window:
- enable maintenance mode
- final incremental data sync
- switch `nginx` upstream to Rails stack
- run smoke and release.
7. Rollback plan:
- preserve old stack images and DB snapshots
- revert DNS/upstream in <15 minutes if gate fails.

## Test Cases and Acceptance Scenarios
- Contract tests:
- existing React client flows pass with no frontend API code changes.
- Ingest/backtest/chart e2e:
- file ingest -> job completion -> bars visible -> backtest run -> analytics export.
- Concurrency/load:
- simultaneous ingest and chart reads under sustained load.
- Failure injection:
- kill python worker mid-task; task re-leases and retries correctly.
- restart Postgres/ClickHouse during queued workload; system recovers without orphaned states.
- Data correctness:
- old vs new analytics parity within defined numeric tolerance.
- Security:
- tenant isolation tests across all read/write endpoints.
- Performance gates (required before cutover):
- chart bars P95 < 300ms for common query windows.
- ingest enqueue P95 < 150ms.
- worker queue lag P95 < 30s under target peak.
- zero data-loss in replay tests.

## Observability and Operations
- Structured logs with `request_id`, `job_id`, `task_id`, `tenant_id`.
- Metrics:
- queue depth, lease expiry count, retry rate, task runtime, API latency percentiles.
- Alerts:
- task dead-letter spikes
- queue lag threshold breach
- ClickHouse/Postgres health and disk pressure.
- Backups:
- nightly Postgres logical + base backups.
- ClickHouse snapshot/backup schedule with restore drill monthly.

## Assumptions and Defaults
- You chose: `Rails API + Python compute`, `ClickHouse + Postgres`, `DB-backed jobs`, `single VM/Docker`, `big-bang`, `feature parity + reliability gates`.
- Because DB-backed jobs were selected despite scale priority, the plan hardens queue tables with leasing, retries, and dead-lettering; no Redis/Kafka in v1 target.
- Scope excludes live/paper trading changes and ORB strategy redesign; only platform/backend architecture hardening and migration.
