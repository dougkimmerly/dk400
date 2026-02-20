# dk400

AS/400-inspired job queue and scheduling system. This is the **platform** repo — reusable engine that deployments consume.

## Architecture

Three processes run via supervisord:
- **Robot** (Celery Beat + Worker) — reads `qsys._jobscde`, runs programs on schedule
- **API** (FastAPI :8400) — HTTP endpoint to call programs
- **Web** (FastAPI :8500) — 5250 terminal emulator via WebSocket

## Key Directories

| Path | Purpose |
|------|---------|
| `dk400/` | Python package (platform code) |
| `dk400/config/` | Settings (DATABASE_URL, REDIS_URL, API_PORT, LOG_LEVEL only) |
| `dk400/db/` | Database connection management |
| `dk400/robot/` | Celery worker, tasks, database scheduler |
| `dk400/api/` | FastAPI API server |
| `dk400/web/` | 5250 terminal (server, screens, database, users, static files) |
| `dk400/programs/` | Built-in platform programs |
| `programs/` | User/deployment programs (searched first) |

## Program Loading

Programs are searched in order:
1. `programs.*` — deployment-specific (override)
2. `dk400.programs.*` — built-in platform

This happens in three places:
- `dk400/robot/tasks.py` — `_import_program()`
- `dk400/api/main.py` — `_import_program()`
- `dk400/web/server.py` — inline in `run_program()`

## Database

Uses PostgreSQL with `qsys` schema for system tables:
- `qsys._jobscde` — job schedule definitions
- `qsys.qausrprf` — user profiles
- `qsys._jobhst` — job history

## Design Principles

- **Platform knows nothing about deployments.** No homelab, NetBox, Telegram, or deployment-specific settings in this repo.
- **Programs are the extension point.** All business logic lives in programs.
- **Settings are minimal.** Platform only needs DATABASE_URL, REDIS_URL, API_PORT, LOG_LEVEL. Programs read their own env vars.
- **Dual namespace loading.** `programs.*` first, then `dk400.programs.*`. Deployments override built-ins.

## Deployment Pattern

Deployments use this repo as a git submodule at `engine/`:
```
my-deployment/
├── engine/          ← this repo (submodule)
├── programs/        ← deployment programs
├── Dockerfile       ← extends engine image
├── compose.yaml     ← deployment config
└── requirements.txt ← extra deps
```
