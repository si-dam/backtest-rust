#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

ACTION="${1:-up}"

case "${ACTION}" in
  up)
    docker compose up -d postgres redis
    ;;
  down)
    docker compose stop postgres redis
    ;;
  logs)
    docker compose logs -f postgres redis
    ;;
  *)
    echo "Usage: $0 {up|down|logs}" >&2
    exit 1
    ;;
esac
