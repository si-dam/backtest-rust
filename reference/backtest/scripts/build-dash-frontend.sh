#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}/frontend/dash"

if [[ -f package-lock.json ]]; then
  npm ci
else
  npm install
fi
npm run build

cd "${ROOT_DIR}"
rm -rf app/static/dash
mkdir -p app/static/dash
cp -R frontend/dash/dist/. app/static/dash/

echo "Synced frontend/dash/dist -> app/static/dash"
