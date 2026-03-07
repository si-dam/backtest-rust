# AGENTS.md

## Runtime profiles (important)

This project is used in two runtimes:

1. Host macOS shell:
- Use `python3` and `python3 -m pip`.
- `python` may not exist.

2. VS Code dev container (`DEVCONTAINER=1`):
- Use `python` and `pip`.
- Toolchain includes `gh` CLI and Docker CLI.

When running commands, detect runtime first and choose the correct interpreter.

## Default command policy

- Prefer `rg`/`rg --files` for search.
- Use repo-root commands unless a subdirectory is explicitly needed.
- Prefer non-interactive commands.

## Workspace boundaries (strict)

- Treat the active repo root as the only writable project root:
  - Host macOS: `/Users/si-dam/dev/backtest`
  - Dev container: `/workspaces/backtest`
- Do not read, edit, run, delete, or clean up files in sibling/parent folders unless the user explicitly asks.
- Do not run git commands outside this repo.
- Do not run global cleanup commands that affect other projects (for example `docker system prune -a`, broad `docker image rm`, or deleting shared volumes) unless the user explicitly asks.
- Only operate on Docker resources with names tied to this project (`backtest-*`, compose project `backtest`) unless explicitly requested otherwise.
- If a task requires touching another folder or shared resource, stop and ask for confirmation first.

## Backtest project conventions

- API app: `app.main:app`
- Worker entrypoint: `python -m app.worker.run_worker`
- Tests: `python -m pytest -q` (container) or `python3 -m pytest -q` (host)
- Compile check: `python -m compileall app tests` (container) or `python3 -m compileall app tests` (host)

## Dev container + auth behavior

- Dev container mounts host `~/.codex` to `/home/vscode/.codex`.
- `CODEX_HOME` is set to `/home/vscode/.codex`.
- Do not overwrite or delete `auth.json` in that path.
- If Codex auth is missing inside container, verify mount availability before asking user to re-auth.

## Docker workflow

- Primary local orchestration uses `docker compose` from repo root.
- Expected services: `web`, `worker`, `redis`, `postgres`.
- CSV ingest watch directory: `data/watch/`.

## Scope guardrails

- V1 is chart-first and strategy-ready scaffolding.
- No live/paper trading changes unless explicitly requested.
- Keep ORB implementation out of scope unless user asks for it.
