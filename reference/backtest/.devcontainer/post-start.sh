#!/usr/bin/env bash
set -euo pipefail

mkdir -p "${CODEX_HOME:-/home/vscode/.codex}"
mkdir -p data/watch data/datasets data/backups

if [[ -f .env.example && ! -f .env ]]; then
  cp .env.example .env
fi

if [[ -f .env.devcontainer.example && ! -f .env.devcontainer ]]; then
  cp .env.devcontainer.example .env.devcontainer
fi
