# DK/400

An AS/400-inspired job queue and scheduling system with a web-based 5250 terminal emulator.

## What It Does

DK/400 provides three things:

1. **Robot** — A Celery-based job scheduler that reads schedules from a database table (`qsys._jobscde`) and runs programs on schedule
2. **API** — A FastAPI endpoint that lets external systems call programs via HTTP
3. **Web Terminal** — A 5250-style terminal emulator in the browser, complete with green phosphor text, function keys, and AS/400-style screens

Programs are Python modules with a `run()` function. DK/400 schedules them, runs them, and provides a UI to manage them.

## Quick Start

```bash
git clone https://github.com/dougkimmerly/dk400.git
cd dk400
docker compose up
```

This starts dk400 + Redis. You'll need a PostgreSQL database — either add one to compose.yaml or point `DATABASE_URL` at an existing one.

**Ports:**
- `8400` — API (`/health`, `/pgm/{name}`)
- `8500` — Web terminal

## Writing Programs

A program is a Python module with a `run()` function. Put it in `programs/`:

```python
# programs/hello.py

async def run(**kwargs):
    """Say hello."""
    name = kwargs.get("name", "world")
    return {"message": f"Hello, {name}!"}
```

**Call it via API:**
```bash
curl -X POST http://localhost:8400/pgm/hello \
  -H "Content-Type: application/json" \
  -d '{"kwargs": {"name": "Doug"}}'
```

**Schedule it** by adding a row to `qsys._jobscde`:
```sql
INSERT INTO qsys._jobscde (name, text, command, frequency, status)
VALUES ('HELLO', 'Say hello every hour', 'hello', '*HOURLY', '*ACTIVE');
```

## Program Search Order

DK/400 searches for programs in this order:

1. `programs.*` — Your deployment-specific programs
2. `dk400.programs.*` — Built-in platform programs

This means deployment programs override built-ins with the same name.

## Scheduling

Jobs are defined in the `qsys._jobscde` table. The Robot scheduler reads this table every 60 seconds and picks up changes without a restart.

| Column | Purpose |
|--------|---------|
| `name` | Job identifier |
| `text` | Description |
| `command` | Program name (e.g., `hello`) |
| `frequency` | `*HOURLY`, `*DAILY`, `*WEEKLY`, `*MONTHLY`, or seconds |
| `schedule_time` | Time to run (for daily/weekly/monthly) |
| `days_of_week` | Days to run (for weekly) |
| `status` | `*ACTIVE` or `*HELD` |

**Command with kwargs:** `program_name|key=value,key2=value2`

## Architecture

```
┌──────────────────────────────────────────────┐
│                  DK/400                       │
├──────────────────────────────────────────────┤
│  supervisord                                  │
│  ├── Robot (Celery Beat + Worker)             │
│  │   └── reads qsys._jobscde → runs programs │
│  ├── API (FastAPI :8400)                      │
│  │   └── POST /pgm/{name} → runs programs    │
│  └── Web (FastAPI :8500)                      │
│      └── 5250 terminal via WebSocket          │
├──────────────────────────────────────────────┤
│  programs/         Your programs              │
│  dk400/programs/   Built-in programs          │
└──────────────────────────────────────────────┘
         │                    │
         ▼                    ▼
    PostgreSQL             Redis
    (qsys schema)       (Celery broker)
```

## Using as a Submodule

DK/400 is designed to be consumed as a git submodule by deployment repos:

```bash
# In your deployment repo
git submodule add https://github.com/dougkimmerly/dk400.git engine

# Your repo structure:
# my-deployment/
# ├── engine/          ← dk400 submodule
# ├── programs/        ← your programs
# ├── Dockerfile       ← extends engine
# ├── compose.yaml     ← your config
# └── requirements.txt ← additional deps
```

Your Dockerfile builds from the engine and adds your programs + extra dependencies.

## Configuration

All configuration via environment variables:

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | `postgresql://dk400:dk400@localhost:5432/dk400` | PostgreSQL connection |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis for Celery |
| `API_PORT` | `8400` | API listen port |
| `LOG_LEVEL` | `INFO` | Logging level |
| `TZ` | `America/New_York` | Timezone |

Programs read their own env vars — the platform doesn't need to know about them.

## License

MIT
