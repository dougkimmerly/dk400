# DK/400

AS/400-inspired job queue system with web-based 5250 terminal emulator.

```
  ____  _  ______ ___   ___   ___
 |  _ \| |/ / / // _ \ / _ \ / _ \
 | | | | ' / / /| | | | | | | | | |
 | |_| | . \/ /_| |_| | |_| | |_| |
 |____/|_|\_\____\___/ \___/ \___/
```

## Overview

DK/400 brings the reliability and simplicity of AS/400-style job management to modern infrastructure:

- **Celery** - Distributed task queue (jobs never lost)
- **Redis** - Fast, reliable message broker
- **PostgreSQL** - System database with AS/400-style schemas
- **Web Terminal** - Browser-based 5250 green screen emulator
- **Flower** - Web-based job monitoring

## Quick Start

```bash
# Clone the repo
git clone https://github.com/dougkimmerly/dk400.git
cd dk400

# Start all containers
docker compose up -d

# Check status
docker compose ps

# Access Web Terminal (5250 emulator)
open http://localhost:8400

# Access Flower UI (job monitoring)
open http://localhost:5555

# Default login: QSECOFR / QSECOFR
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                           DK/400                                 │
├─────────────────────────────────────────────────────────────────┤
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐   │
│  │  celery-  │  │  celery-  │  │  dk400-   │  │  dk400-   │   │
│  │  qbatch   │  │   beat    │  │  flower   │  │   web     │   │
│  │ (worker)  │  │(scheduler)│  │ (monitor) │  │(terminal) │   │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘   │
│        └──────────────┼──────────────┼──────────────┘         │
│                       │              │                         │
│                 ┌─────▼─────┐  ┌─────▼─────┐                  │
│                 │  dk400-   │  │  dk400-   │                  │
│                 │   redis   │  │ postgres  │                  │
│                 │  (queue)  │  │  (data)   │                  │
│                 └───────────┘  └───────────┘                  │
└─────────────────────────────────────────────────────────────────┘
```

## Containers

| Container | Port | Purpose |
|-----------|------|---------|
| dk400-web | 8400 | Web-based 5250 terminal emulator |
| dk400-flower | 5555 | Celery job monitoring |
| dk400-redis | 6379 | Job queue broker |
| dk400-postgres | 5432 | System database |
| celery-qbatch | - | Batch job worker (QBATCH) |
| celery-beat | - | Job scheduler |

## AS/400 Commands

The web terminal supports 37+ AS/400-style commands:

| Command | Description |
|---------|-------------|
| WRKACTJOB | Work with active jobs |
| WRKJOBQ | Work with job queues |
| WRKJOBSCDE | Work with job schedule entries |
| WRKSVC | Work with services |
| WRKHLTH | Work with health checks |
| DSPSYSSTS | Display system status |
| DSPLOG | Display system log |
| SBMJOB | Submit job |
| WRKUSRPRF | Work with user profiles |
| WRKMSGQ | Work with message queues |
| WRKQRY | Work with queries (SQL browser) |
| WRKLIB | Work with libraries |
| WRKSYSVAL | Work with system values |
| WRKOUTQ | Work with output queues |
| GO MENU | Display command menus |

## Test Tasks

```bash
# Ping test
docker exec celery-qbatch celery -A src.dk400.celery_app call dk400.ping

# Echo test
docker exec celery-qbatch celery -A src.dk400.celery_app call dk400.echo --args='["Hello DK/400"]'

# Delay test (5 seconds)
docker exec celery-qbatch celery -A src.dk400.celery_app call dk400.delay --args='[5]'
```

## Adding Your Own Tasks

1. Create a new file in `src/dk400/tasks/`:

```python
from src.dk400.celery_app import app

@app.task(name='dk400.mytask')
def my_task(param: str):
    """Your custom task."""
    return {'status': 'ok', 'result': param}
```

2. Add scheduled jobs in `src/dk400/celery_app.py`:

```python
from celery.schedules import crontab

app.conf.beat_schedule = {
    'my-daily-job': {
        'task': 'dk400.mytask',
        'schedule': crontab(hour=8, minute=0),
        'args': ('morning run',),
    },
}
```

3. Restart containers:

```bash
docker compose restart celery-qbatch celery-beat
```

## Database

DK/400 uses PostgreSQL with AS/400-style library (schema) organization:

- `qsys` - System library (users, jobs, messages, system values)
- `qgpl` - General purpose library
- User-created libraries map to PostgreSQL schemas

```sql
-- Example: Query users
SELECT * FROM qsys.users;

-- Example: Query job schedule
SELECT * FROM qsys.job_schedule;
```

## License

MIT
