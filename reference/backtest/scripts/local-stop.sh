#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

RUN_DIR=".local/run"
API_PID_FILE="${RUN_DIR}/local-api.pid"
WORKER_PID_FILE="${RUN_DIR}/local-worker.pid"
STOPPED_ANY=0

stop_from_pid_file() {
  local label="$1"
  local pid_file="$2"

  if [[ ! -f "${pid_file}" ]]; then
    return
  fi

  local pid
  pid="$(tr -d '[:space:]' < "${pid_file}")"
  rm -f "${pid_file}"
  STOPPED_ANY=1

  if [[ -z "${pid}" ]]; then
    echo "${label}: cleared empty pid file."
    return
  fi

  if ! kill -0 "${pid}" 2>/dev/null; then
    echo "${label}: process ${pid} is not running."
    return
  fi

  kill "${pid}" 2>/dev/null || true
  for _ in {1..30}; do
    if ! kill -0 "${pid}" 2>/dev/null; then
      echo "${label}: stopped process ${pid}."
      return
    fi
    sleep 0.1
  done

  kill -9 "${pid}" 2>/dev/null || true
  echo "${label}: force-stopped process ${pid}."
}

stop_from_pid_file "API" "${API_PID_FILE}"
stop_from_pid_file "Worker" "${WORKER_PID_FILE}"

if [[ "${STOPPED_ANY}" == "0" ]]; then
  echo "No local-run PID files found. Start with ./scripts/local-run.sh first."
fi
