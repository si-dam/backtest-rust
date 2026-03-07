# Futures Backtest Platform (V1)

Chart-first modular futures backtesting platform for NQ/ES with:
- FastAPI APIs
- React + Lightweight Charts workspace
- Postgres metadata storage
- DuckDB market-data storage
- Redis + RQ async jobs

## Dev container

This repo includes a local VS Code dev container in `.devcontainer/` with:
- Python 3.11
- GitHub CLI (`gh`)
- Docker CLI access via host socket
- Codex auth reuse from host `~/.codex` (no repeated sign-in)
- The same extension IDs currently installed on host (project recommendations in `.vscode/extensions.json`)

### Open in container

1. In VS Code: `Dev Containers: Reopen in Container`
2. Wait for `post-create` dependency install to complete.
3. Verify tools:
```bash
python --version
gh --version
```

### Run stack from inside container
```bash
./scripts/devcontainer-run.sh
```
This starts `postgres` + `redis` with Docker, then runs API and worker directly in the dev container with live code reload.
Build frontend assets after dependency updates:
```bash
./scripts/build-dash-frontend.sh
```

If you prefer separate terminals:
```bash
./scripts/devcontainer-infra.sh up
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload --reload-dir app --reload-dir tests
# terminal 2
python -m app.worker.run_worker
```

Stop infra:
```bash
./scripts/devcontainer-infra.sh down
```

## Host macOS (no Docker)

If you want to run without Docker, use local Postgres and a pinned toolchain.

1. Install toolchain + Postgres:
```bash
brew install mise postgresql@16
```

2. Install repo runtimes (`.mise.toml`) and Python deps:
```bash
mise trust
mise install
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -e ".[dev]"
```

3. Start local Postgres:
```bash
brew services start postgresql@16
```

4. Use host env values and run app:
```bash
cp .env.host.example .env
./scripts/local-run.sh
```
This starts API on `http://localhost:8000` and auto-creates local `backtest` role/database if needed.
If you also want the worker, run with `LOCAL_RUN_WITH_WORKER=1 ./scripts/local-run.sh` (requires Redis).
To stop local processes started by that script, run `./scripts/local-stop.sh`.

## Quick start (Docker)

1. Copy env:
```bash
cp .env.example .env
```

2. Start services:
```bash
docker compose up --build
```

3. Open:
- API docs: `http://localhost:8000/docs`
- App UI: `http://localhost:8000/dash/`

4. Drop Sierra CSV files into `./data/watch`.

## Frontend dev (no rebuilds)

For React UI iteration with HMR, use the dev override which runs Vite in a `dash` service and proxies API calls to the FastAPI `web` service.

Start:
```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build
```

Open:
- UI (Vite/HMR): `http://localhost:5173/dash/`
- UI (FastAPI redirect): `http://localhost:8000/dash/` (redirects to 5173 in dev override)

## CSV requirements
Two ingest schemas are supported (case-insensitive aliases supported):

1. Tick schema:
- timestamp (or date+time)
- trade price
- trade size
- bid price
- ask price

2. Sierra 1-minute OHLC schema:
- date
- time
- open
- high
- low
- last/close
- volume
- number of trades

Notes:
- 1-minute OHLC ingest writes directly to `bars` (`timeframe=1m`) and is used as the candle source.
- Tick-dependent endpoints/features (`/chart/ticks`, large-orders, session/volume profiles) require tick data.

## Access model
V1 runs without authentication in local/dev environments.
