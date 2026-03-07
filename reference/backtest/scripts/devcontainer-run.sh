#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ "${DEVCONTAINER:-0}" != "1" ]]; then
  echo "This command is intended for the VS Code dev container (DEVCONTAINER=1)." >&2
fi

mkdir -p data/watch data/datasets data/backups

docker compose up -d postgres redis

if [[ -d frontend/dash ]] && command -v npm >/dev/null 2>&1; then
  ./scripts/build-dash-frontend.sh
fi

echo "Starting worker (python -m app.worker.run_worker)..."
python -m app.worker.run_worker &
WORKER_PID=$!

cleanup() {
  if kill -0 "${WORKER_PID}" 2>/dev/null; then
    echo "Stopping worker..."
    kill "${WORKER_PID}" 2>/dev/null || true
    wait "${WORKER_PID}" 2>/dev/null || true
  fi
}

trap cleanup EXIT INT TERM

echo "Starting API with reload (uvicorn app.main:app --reload)..."
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload --reload-dir app --reload-dir tests
