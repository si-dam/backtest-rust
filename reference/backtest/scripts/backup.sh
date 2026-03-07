#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BACKUP_DIR="${ROOT_DIR}/data/backups"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

mkdir -p "${BACKUP_DIR}/postgres"
mkdir -p "${BACKUP_DIR}/duckdb"

# Postgres dump
cd "${ROOT_DIR}"
docker compose exec -T postgres pg_dump -U backtest -d backtest > "${BACKUP_DIR}/postgres/backtest_${TIMESTAMP}.sql"

# DuckDB snapshot
if [[ -f "${ROOT_DIR}/data/datasets/market.duckdb" ]]; then
  cp "${ROOT_DIR}/data/datasets/market.duckdb" "${BACKUP_DIR}/duckdb/market_${TIMESTAMP}.duckdb"
fi

# Retention: keep last 14 backups per type
ls -1t "${BACKUP_DIR}/postgres"/*.sql 2>/dev/null | tail -n +15 | xargs -r rm -f
ls -1t "${BACKUP_DIR}/duckdb"/*.duckdb 2>/dev/null | tail -n +15 | xargs -r rm -f

echo "Backup completed: ${TIMESTAMP}"
