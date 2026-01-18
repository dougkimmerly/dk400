# DK/400

AS/400-inspired job queue system with green screen web TUI.

## Quick Deploy

```bash
# Standard deploy
ssh doug@192.168.20.19 "cd /home/doug/dkSRC/infrastructure/dk400 && git pull && docker compose up -d --build"

# Force rebuild (cache issues)
ssh doug@192.168.20.19 "cd /home/doug/dkSRC/infrastructure/dk400 && git pull && docker compose build --no-cache && docker compose up -d"
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

## Full Documentation

See `.claude/skills/dk400/skill.md` for complete development guide.
