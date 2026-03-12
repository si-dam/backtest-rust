#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ASSUME_YES=0
BACKUP_DIR=""

usage() {
  cat <<'EOF'
Usage:
  scripts/restore-from-backup.sh <backup_dir> [--yes]

Example:
  scripts/restore-from-backup.sh "/Volumes/USB DISK/backtest-rust-backup-20260312_123926" --yes

This restores:
  - .env
  - artifacts/
  - data/watch/
  - Postgres from postgres/backtest.sql
  - ClickHouse from clickhouse/schema.sql + clickhouse/data/*.native

The script is destructive for the local app state. Stop the API/worker before running it.
EOF
}

find_bin() {
  local name="$1"
  shift

  if command -v "$name" >/dev/null 2>&1; then
    command -v "$name"
    return 0
  fi

  local candidate
  for candidate in "$@"; do
    if [[ -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  return 1
}

restore_path() {
  local source_path="$1"
  local target_path="$2"
  local safety_root="$3"

  if [[ ! -e "$source_path" ]]; then
    return 0
  fi

  if [[ -e "$target_path" ]]; then
    mkdir -p "$safety_root"
    mv "$target_path" "$safety_root/"
  fi

  mkdir -p "$(dirname "$target_path")"
  cp -R "$source_path" "$target_path"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --yes|-y)
      ASSUME_YES=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    -*)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
    *)
      if [[ -n "$BACKUP_DIR" ]]; then
        echo "Only one backup_dir can be provided." >&2
        usage >&2
        exit 1
      fi
      BACKUP_DIR="$1"
      shift
      ;;
  esac
done

if [[ -z "$BACKUP_DIR" ]]; then
  usage >&2
  exit 1
fi

if [[ ! -d "$BACKUP_DIR" ]]; then
  echo "Backup directory not found: $BACKUP_DIR" >&2
  exit 1
fi

BACKUP_DIR="$(cd "$BACKUP_DIR" && pwd)"
BACKUP_ENV="$BACKUP_DIR/project/.env"
ACTIVE_ENV="$ROOT/.env"

if [[ -f "$BACKUP_ENV" ]]; then
  set -a
  source "$BACKUP_ENV"
  set +a
elif [[ -f "$ACTIVE_ENV" ]]; then
  set -a
  source "$ACTIVE_ENV"
  set +a
else
  echo "Missing both $BACKUP_ENV and $ACTIVE_ENV" >&2
  exit 1
fi

DATABASE_URL="${DATABASE_URL:-postgres://backtest:backtest@localhost:5432/backtest}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-backtest}"

PSQL_BIN="$(
  find_bin \
    psql \
    /opt/homebrew/opt/postgresql@16/bin/psql \
    /opt/homebrew/opt/postgresql@15/bin/psql \
    /usr/local/opt/postgresql@16/bin/psql \
    /usr/local/opt/postgresql@15/bin/psql
)"

CLICKHOUSE_BIN="$(
  find_bin \
    clickhouse-client \
    "$HOME/.local/bin/clickhouse-client" \
    /opt/homebrew/bin/clickhouse-client \
    /usr/local/bin/clickhouse-client
)"

clickhouse_host_port="${CLICKHOUSE_URL#http://}"
clickhouse_host_port="${clickhouse_host_port#https://}"
clickhouse_host_port="${clickhouse_host_port%%/*}"
clickhouse_auth=""

if [[ "$clickhouse_host_port" == *"@"* ]]; then
  clickhouse_auth="${clickhouse_host_port%@*}"
  clickhouse_host_port="${clickhouse_host_port#*@}"
fi

CLICKHOUSE_HOST="${clickhouse_host_port%%:*}"
CLICKHOUSE_PORT="${clickhouse_host_port##*:}"

if [[ "$CLICKHOUSE_HOST" == "$CLICKHOUSE_PORT" ]]; then
  CLICKHOUSE_PORT="8123"
fi

CLICKHOUSE_ARGS=(--host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT")

if [[ -n "$clickhouse_auth" ]]; then
  CLICKHOUSE_USER="${clickhouse_auth%%:*}"
  CLICKHOUSE_PASSWORD="${clickhouse_auth#*:}"
  CLICKHOUSE_ARGS+=(--user "$CLICKHOUSE_USER")
  if [[ "$CLICKHOUSE_USER" != "$CLICKHOUSE_PASSWORD" ]]; then
    CLICKHOUSE_ARGS+=(--password "$CLICKHOUSE_PASSWORD")
  fi
fi

POSTGRES_DUMP="$BACKUP_DIR/postgres/backtest.sql"
CLICKHOUSE_SCHEMA="$BACKUP_DIR/clickhouse/schema.sql"
CLICKHOUSE_DATA_DIR="$BACKUP_DIR/clickhouse/data"

timestamp="$(date +%Y%m%d_%H%M%S)"
safety_root="$ROOT/.restore-backups/$timestamp"

echo "Restore source: $BACKUP_DIR"
echo "Repo root:       $ROOT"
echo "Postgres URL:    $DATABASE_URL"
echo "ClickHouse:      $CLICKHOUSE_URL / $CLICKHOUSE_DATABASE"
echo "Safety backup:   $safety_root"
echo
echo "This will overwrite local app state."

if [[ "$ASSUME_YES" -ne 1 ]]; then
  read -r -p "Type 'restore' to continue: " answer
  if [[ "$answer" != "restore" ]]; then
    echo "Cancelled."
    exit 1
  fi
fi

mkdir -p "$safety_root"

echo "Restoring repo files..."
if [[ -f "$BACKUP_ENV" ]]; then
  restore_path "$BACKUP_ENV" "$ROOT/.env" "$safety_root"
fi
if [[ -d "$BACKUP_DIR/project/artifacts" ]]; then
  restore_path "$BACKUP_DIR/project/artifacts" "$ROOT/artifacts" "$safety_root"
fi
if [[ -d "$BACKUP_DIR/project/watch" ]]; then
  restore_path "$BACKUP_DIR/project/watch" "$ROOT/data/watch" "$safety_root"
fi

if [[ -f "$POSTGRES_DUMP" ]]; then
  echo "Restoring Postgres..."
  "$PSQL_BIN" "$DATABASE_URL" -v ON_ERROR_STOP=1 -c \
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid();" \
    >/dev/null || true
  "$PSQL_BIN" "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$POSTGRES_DUMP"
fi

if [[ -f "$CLICKHOUSE_SCHEMA" ]]; then
  echo "Restoring ClickHouse schema..."
  "$CLICKHOUSE_BIN" "${CLICKHOUSE_ARGS[@]}" --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE} SYNC"
  "$CLICKHOUSE_BIN" "${CLICKHOUSE_ARGS[@]}" --multiquery < "$CLICKHOUSE_SCHEMA"
fi

if [[ -d "$CLICKHOUSE_DATA_DIR" ]]; then
  echo "Restoring ClickHouse data..."
  shopt -s nullglob
  for native_path in "$CLICKHOUSE_DATA_DIR"/*.native; do
    native_name="$(basename "$native_path")"
    if [[ "$native_name" == ._* ]]; then
      continue
    fi
    table_name="${native_name%.native}"
    echo "  -> ${CLICKHOUSE_DATABASE}.${table_name}"
    "$CLICKHOUSE_BIN" "${CLICKHOUSE_ARGS[@]}" --query \
      "INSERT INTO ${CLICKHOUSE_DATABASE}.${table_name} FORMAT Native" < "$native_path"
  done
  shopt -u nullglob
fi

echo
echo "Restore complete."
echo "If needed, start the app with: scripts/dev-up.sh"
