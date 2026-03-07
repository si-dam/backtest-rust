#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ "${DEVCONTAINER:-0}" == "1" ]]; then
  PYTHON_BIN="python"
else
  PYTHON_BIN="python3"
fi

if [[ -x ".venv/bin/${PYTHON_BIN}" ]]; then
  PYTHON_CMD=".venv/bin/${PYTHON_BIN}"
elif command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  PYTHON_CMD="${PYTHON_BIN}"
else
  echo "Missing ${PYTHON_BIN}. Install it (or create .venv) before running local-run." >&2
  exit 1
fi

PG_BIN_DIR=""
if command -v psql >/dev/null 2>&1; then
  PG_BIN_DIR="$(dirname "$(command -v psql)")"
elif command -v brew >/dev/null 2>&1; then
  for formula in postgresql@16 postgresql postgresql@18 postgresql@17; do
    BREW_PG_PREFIX="$(brew --prefix "${formula}" 2>/dev/null || true)"
    if [[ -n "${BREW_PG_PREFIX}" ]] && [[ -x "${BREW_PG_PREFIX}/bin/psql" ]]; then
      PG_BIN_DIR="${BREW_PG_PREFIX}/bin"
      break
    fi
  done

  if [[ -z "${PG_BIN_DIR}" ]]; then
    BREW_PG_FORMULA="$(brew list --formula 2>/dev/null | rg '^postgresql(@[0-9]+)?$' | sort -Vr | head -n 1 || true)"
    if [[ -n "${BREW_PG_FORMULA}" ]]; then
      BREW_PG_PREFIX="$(brew --prefix "${BREW_PG_FORMULA}" 2>/dev/null || true)"
      if [[ -n "${BREW_PG_PREFIX}" ]] && [[ -x "${BREW_PG_PREFIX}/bin/psql" ]]; then
        PG_BIN_DIR="${BREW_PG_PREFIX}/bin"
      fi
    fi
  fi
fi

if [[ -z "${PG_BIN_DIR}" ]]; then
  echo "Missing psql. Install Postgres client tools first." >&2
  exit 1
fi

PSQL_BIN="${PG_BIN_DIR}/psql"
CREATEUSER_BIN="${PG_BIN_DIR}/createuser"
CREATEDB_BIN="${PG_BIN_DIR}/createdb"
PG_ISREADY_BIN="${PG_BIN_DIR}/pg_isready"

mkdir -p data/watch data/datasets data/backups
RUN_DIR=".local/run"
mkdir -p "${RUN_DIR}"
API_PID_FILE="${RUN_DIR}/local-api.pid"
WORKER_PID_FILE="${RUN_DIR}/local-worker.pid"
rm -f "${API_PID_FILE}" "${WORKER_PID_FILE}"

LOCAL_PG_USER="${LOCAL_PG_USER:-backtest}"
LOCAL_PG_DB="${LOCAL_PG_DB:-backtest}"
LOCAL_PG_PASSWORD="${LOCAL_PG_PASSWORD:-backtest}"
LOCAL_PG_HOST="${LOCAL_PG_HOST:-localhost}"
LOCAL_PG_PORT="${LOCAL_PG_PORT:-5432}"
PSQL_CMD=("${PSQL_BIN}" -h "${LOCAL_PG_HOST}" -p "${LOCAL_PG_PORT}" postgres)
CREATEUSER_CMD=("${CREATEUSER_BIN}" -h "${LOCAL_PG_HOST}" -p "${LOCAL_PG_PORT}")
CREATEDB_CMD=("${CREATEDB_BIN}" -h "${LOCAL_PG_HOST}" -p "${LOCAL_PG_PORT}")

if [[ -x "${PG_ISREADY_BIN}" ]]; then
  if ! "${PG_ISREADY_BIN}" -h "${LOCAL_PG_HOST}" -p "${LOCAL_PG_PORT}" >/dev/null 2>&1; then
    echo "Postgres is not reachable at ${LOCAL_PG_HOST}:${LOCAL_PG_PORT}. Start it, then retry." >&2
    exit 1
  fi
fi

if [[ "$("${PSQL_CMD[@]}" -tAc "SELECT 1 FROM pg_roles WHERE rolname='${LOCAL_PG_USER}'" | tr -d '[:space:]')" != "1" ]]; then
  "${CREATEUSER_CMD[@]}" -s "${LOCAL_PG_USER}"
fi

"${PSQL_CMD[@]}" -v ON_ERROR_STOP=1 -c "ALTER USER \"${LOCAL_PG_USER}\" WITH PASSWORD '${LOCAL_PG_PASSWORD}';" >/dev/null

if [[ "$("${PSQL_CMD[@]}" -tAc "SELECT 1 FROM pg_database WHERE datname='${LOCAL_PG_DB}'" | tr -d '[:space:]')" != "1" ]]; then
  "${CREATEDB_CMD[@]}" -O "${LOCAL_PG_USER}" "${LOCAL_PG_DB}"
fi

export APP_ENV="${APP_ENV:-dev}"
export POSTGRES_DSN="${POSTGRES_DSN:-postgresql+psycopg://${LOCAL_PG_USER}:${LOCAL_PG_PASSWORD}@${LOCAL_PG_HOST}:${LOCAL_PG_PORT}/${LOCAL_PG_DB}}"
export REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"
export DUCKDB_PATH="${DUCKDB_PATH:-./data/datasets/market.duckdb}"
export WATCH_DIR="${WATCH_DIR:-./data/watch}"
export INTERNAL_API_BASE="${INTERNAL_API_BASE:-http://127.0.0.1:8000}"
export ENABLE_WATCHER="${ENABLE_WATCHER:-false}"

API_PID=""
WORKER_PID=""

cleanup() {
  if [[ -n "${API_PID}" ]] && kill -0 "${API_PID}" 2>/dev/null; then
    kill "${API_PID}" 2>/dev/null || true
    wait "${API_PID}" 2>/dev/null || true
  fi

  if [[ -n "${WORKER_PID}" ]] && kill -0 "${WORKER_PID}" 2>/dev/null; then
    kill "${WORKER_PID}" 2>/dev/null || true
    wait "${WORKER_PID}" 2>/dev/null || true
  fi

  rm -f "${WORKER_PID_FILE}"
  rm -f "${API_PID_FILE}"
}

trap cleanup EXIT INT TERM

if [[ "${LOCAL_RUN_WITH_WORKER:-0}" == "1" ]]; then
  echo "Starting worker (${PYTHON_CMD} -m app.worker.run_worker)..."
  "${PYTHON_CMD}" -m app.worker.run_worker &
  WORKER_PID=$!
  echo "${WORKER_PID}" > "${WORKER_PID_FILE}"
else
  echo "Skipping worker (LOCAL_RUN_WITH_WORKER=0). Redis is optional for read-only/API usage."
fi

echo "Starting API (${PYTHON_CMD} -m uvicorn app.main:app --reload)..."
"${PYTHON_CMD}" -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload --reload-dir app --reload-dir tests &
API_PID=$!
echo "${API_PID}" > "${API_PID_FILE}"
wait "${API_PID}"
