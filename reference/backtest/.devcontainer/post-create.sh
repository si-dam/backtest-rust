#!/usr/bin/env bash
set -euo pipefail

python -m pip install --upgrade pip setuptools wheel
python -m pip install -e '.[dev]'

mkdir -p data/watch data/datasets data/backups

if [[ -d frontend/dash ]]; then
  if command -v npm >/dev/null 2>&1; then
    (cd frontend/dash && if [[ -f package-lock.json ]]; then npm ci; else npm install; fi)
  else
    echo "Skipping frontend dependency install: npm is not available in this container."
  fi
fi
