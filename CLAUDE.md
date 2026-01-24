# DK/400

AS/400-inspired job queue system with green screen web TUI.

## Quick Deploy

```bash
# Standard deploy (via homelab-dk400 wrapper)
ssh doug@192.168.20.19 "cd /home/doug/dkSRC/infrastructure/homelab-dk400 && git pull --recurse-submodules && docker compose up -d --build"

# Force rebuild (cache issues)
ssh doug@192.168.20.19 "cd /home/doug/dkSRC/infrastructure/homelab-dk400 && git pull --recurse-submodules && docker compose build --no-cache && docker compose up -d"
```

## URLs

- **Web UI:** http://192.168.20.19:8400
- **Flower:** http://192.168.20.19:5555

## Key Files

| File | Purpose | Lines |
|------|---------|-------|
| `src/dk400/web/screens.py` | ALL screen definitions | ~8500 |
| `src/dk400/web/database.py` | ALL database functions | ~4300 |
| `src/dk400/web/users.py` | User auth | ~430 |
| `src/dk400/web/main.py` | FastAPI + WebSocket | ~200 |

## Database

**Schema:** All system tables in `qsys` PostgreSQL schema.

```sql
-- ALWAYS use qsys prefix
SELECT * FROM qsys.users;
INSERT INTO qsys.system_values ...
```

**Library = PostgreSQL Schema:**
- QSYS → qsys (system)
- QGPL → qgpl (general purpose)
- User libraries → user schemas

## Common Gotchas

### psycopg2 % in LIKE
```python
# WRONG: % interpreted as placeholder
"NOT LIKE '\\_%'"

# RIGHT: escape % as %%
"NOT LIKE '\\_%%'"
```

### list_table_columns keys
Uses `name`, `max_length`, `precision`, `scale`, `nullable` (bool)
NOT `column_name`, `character_maximum_length`, `is_nullable`

### WebSocket disconnects
Silent exceptions cause disconnects. Test screen methods directly:
```bash
ssh doug@192.168.20.19 "docker exec dk400-web python3 -c '
from src.dk400.web.screens import ScreenManager
sm = ScreenManager()
# test your method
'"
```

## Testing

```bash
# Syntax check
python3 -m py_compile src/dk400/web/screens.py

# Direct DB query
ssh doug@192.168.20.19 "docker exec dk400-postgres psql -U dk400 -c 'SELECT * FROM qsys.users;'"

# Function test
ssh doug@192.168.20.19 "docker exec dk400-web python3 -c 'from src.dk400.web.database import list_schemas; print(list_schemas())'"

# Reset password
ssh doug@192.168.20.19 "docker exec dk400-web python3 -c 'from src.dk400.web.users import user_manager; print(user_manager.change_password(\"USER\", \"PASS\"))'"
```

## Fixer Module

Issue tracking and remediation toolkit built into dk400.

### Usage

```python
from dk400.fixer import report_issue, resolve_issue, send_telegram

# Report an issue (auto-remediation optional)
result = await report_issue(
    issue_type='container_down',
    target='nginx',
    error_message='Container not running',
    severity='error',           # critical, error, warning, info
    host='192.168.20.19',       # optional
    context={'exit_code': 1},   # optional metadata
    auto_remediate=True,        # attempt automatic fix
)

# Result contains:
# - issue_id: int
# - is_new: bool (new vs recurring)
# - remediated: bool
# - method: 'runbook', 'claude', 'escalated', 'none'
# - message: str

# Mark issue as resolved
await resolve_issue(issue_type='container_down', target='nginx')

# Send notification directly
await send_telegram("Service restored", target="nginx")
```

### In Celery Tasks (sync context)

```python
import asyncio
from dk400.fixer import report_issue

def my_task():
    result = asyncio.run(report_issue(
        issue_type='backup_failure',
        target='bitwarden',
        error_message='Backup command failed',
    ))
    if result['remediated']:
        print(f"Fixed via {result['method']}")
```

### Available Functions

| Function | Purpose |
|----------|---------|
| `report_issue()` | Create/update issue, optionally remediate |
| `resolve_issue()` | Mark issue as resolved |
| `get_issue(id)` | Get issue by ID |
| `get_open_issues()` | List all open issues |
| `attempt_remediation()` | Try to fix without creating issue |
| `execute_runbook()` | Run specific runbook |
| `ask_claude()` | Get Claude's analysis |
| `send_telegram()` | Send Telegram notification |

### Environment Variables

```bash
# Database (required)
FIXER_DB_HOST=dk400-postgres
FIXER_DB_PORT=5432
FIXER_DB_NAME=dk400
FIXER_DB_USER=fixer
FIXER_DB_PASSWORD=<password>

# Notifications (optional)
FIXER_COMMS_URL=http://192.168.20.19:8440/send

# Claude AI (optional, for auto-remediation)
ANTHROPIC_API_KEY=<key>
```

### Database

Fixer uses the `fixer` schema in the `dk400` database:

| Table | Purpose |
|-------|---------|
| `fixer.unified_issues` | All tracked issues |
| `fixer.issue_actions` | Actions taken on issues |
| `fixer.telegram_log` | Notification history |

---

## Full Documentation

See `.claude/skills/dk400/skill.md` for complete development guide.
