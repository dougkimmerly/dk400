# DK/400

AS/400-inspired job queue system with green screen TUI.

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
- **Textual TUI** - Green screen terminal interface
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

# Access Flower UI (job monitoring)
open http://localhost:5555

# SSH to green screen TUI
ssh -p 2222 root@localhost
# Password: dk400
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                           DK/400                                 │
├─────────────────────────────────────────────────────────────────┤
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐   │
│  │  celery-  │  │  celery-  │  │  dk400-   │  │  dk400-   │   │
│  │  qbatch   │  │   beat    │  │  flower   │  │   tui     │   │
│  │ (worker)  │  │(scheduler)│  │ (web ui)  │  │(terminal) │   │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘   │
│        └──────────────┼──────────────┼──────────────┘         │
│                       │              │                         │
│                 ┌─────▼─────┐        │                         │
│                 │  dk400-   │        │                         │
│                 │   redis   │────────┘                         │
│                 │  (queue)  │                                  │
│                 └───────────┘                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Containers

| Container | Port | Purpose |
|-----------|------|---------|
| dk400-redis | 6379 | Job queue broker |
| celery-qbatch | - | Batch job worker (QBATCH) |
| celery-beat | - | Job scheduler |
| dk400-flower | 5555 | Web UI for job monitoring |
| dk400-tui | 2222 | SSH green screen terminal |

## AS/400 Commands

| Command | Description |
|---------|-------------|
| WRKACTJOB | Work with active jobs |
| WRKJOBQ | Work with job queues |
| WRKSVC | Work with services |
| DSPSYSSTS | Display system status |
| DSPLOG | Display log |
| SBMJOB | Submit job |

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

## License

MIT
