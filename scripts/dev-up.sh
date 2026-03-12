#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE" >&2
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

ARTIFACT_ROOT="${ARTIFACT_ROOT:-./artifacts}"
if [[ "$ARTIFACT_ROOT" != /* ]]; then
  ARTIFACT_ROOT="$ROOT/${ARTIFACT_ROOT#./}"
fi
LOG_DIR="$ARTIFACT_ROOT/logs"
mkdir -p "$LOG_DIR"

API_LOG="$LOG_DIR/runtime-api.log"
WORKER_LOG="$LOG_DIR/runtime-worker.log"
FRONTEND_LOG="$LOG_DIR/frontend.log"

declare -a PIDS=()

cleanup() {
  local code=$?

  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done

  wait >/dev/null 2>&1 || true
  exit "$code"
}

trap cleanup INT TERM EXIT

echo "Starting runtime-api on http://127.0.0.1:${APP_PORT:-8080}"
(cd "$ROOT" && exec mise exec -- cargo run -p runtime-api) >"$API_LOG" 2>&1 &
PIDS+=("$!")

echo "Starting runtime-worker"
(cd "$ROOT" && exec mise exec -- cargo run -p runtime-worker) >"$WORKER_LOG" 2>&1 &
PIDS+=("$!")

echo "Starting frontend on http://127.0.0.1:${FRONTEND_PORT:-5173}"
(cd "$ROOT/frontend" && exec mise exec -- npm run dev -- --host 127.0.0.1 --port "${FRONTEND_PORT:-5173}") >"$FRONTEND_LOG" 2>&1 &
PIDS+=("$!")

echo
echo "Logs:"
echo "  API:      $API_LOG"
echo "  Worker:   $WORKER_LOG"
echo "  Frontend: $FRONTEND_LOG"
echo
echo "Press Ctrl+C to stop all processes."

while true; do
  for pid in "${PIDS[@]}"; do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      echo "A process exited unexpectedly. Check logs in $LOG_DIR" >&2
      exit 1
    fi
  done
  sleep 2
done
