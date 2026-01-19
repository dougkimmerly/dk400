# DK/400 Development Skill

AS/400-inspired job queue system with web-based 5250 terminal emulator running on PostgreSQL.

## Project Structure

```
dk400/
├── compose.yaml              # Docker services
├── Dockerfile               # Celery workers
├── Dockerfile.web           # Web UI (FastAPI + WebSocket)
├── src/dk400/
│   ├── celery_app.py        # Celery configuration
│   ├── tasks/               # Celery task definitions
│   └── web/
│       ├── main.py          # FastAPI app, WebSocket handler
│       ├── screens.py       # ALL screen definitions (~9000+ lines)
│       ├── database.py      # ALL database functions (~4500+ lines)
│       └── users.py         # User management, authentication
└── data/                    # Persistent data (postgres, redis)
```

---

## Database Architecture

### Schema: `qsys`

All system tables live in the `qsys` PostgreSQL schema (AS/400 QSYS library equivalent).

**CRITICAL:** Always use `qsys.tablename` in SQL queries, never just `tablename`.

```sql
-- System tables in qsys schema (AS/400-style names)
qsys.users              -- User profiles
qsys.system_values      -- System values (WRKSYSVAL)
qsys.qhst               -- Audit/history log

-- Internal object storage tables (underscore prefix = physical files)
qsys._lib               -- Library definitions
qsys._objaut            -- Object authorities
qsys._splf              -- Spool file metadata
qsys._cmd               -- Command definitions
qsys._cmdparm           -- Command parameters
qsys._prmval            -- F4 prompt values
qsys._jobhst            -- Job execution history
qsys._dtaara            -- Data areas
qsys._msgq              -- Message queues
qsys._msg               -- Messages
qsys._qrydfn            -- Query definitions
qsys._jobd              -- Job descriptions
qsys._jobscde           -- Job schedules
qsys._outq              -- Output queues
qsys._autl              -- Authorization lists
qsys._autle             -- Auth list entries
qsys._sbsd              -- Subsystem descriptions

-- Journaling tables
qsys._jrn               -- Journal definitions
qsys._jrnrcv            -- Journal receivers
qsys._jrne              -- Journal entries (before/after images)
qsys._jrnpf             -- Journaled files registry
```

### Library = PostgreSQL Schema

- `QSYS` = `qsys` schema (system library)
- `QGPL` = `qgpl` schema (general purpose)
- User libraries = user-created schemas

### Library List (*LIBL)

Each user has a library list stored in `qsys.users`:
- `current_library` - Where new objects are created when using *LIBL
- `library_list` - JSONB array of libraries to search (default: `["QGPL", "QSYS"]`)

**Functions:**
```python
get_user_library_list(username) -> list[str]  # Get user's searchable libraries
get_user_current_library(username) -> str     # Get where *LIBL creates objects
resolve_library(library, username) -> list[str]  # Expand *LIBL to library list
resolve_library_for_create(library, username) -> str  # Resolve for object creation
```

### Object Types

| AS/400 Type | PostgreSQL Implementation | Storage |
|-------------|---------------------------|---------|
| *FILE | Table | `information_schema.tables` |
| *DTAARA | Row in _dtaara | `{schema}._dtaara` |
| *MSGQ | Row in _msgq | `{schema}._msgq` |
| *QRYDFN | Row in _qrydfn | `{schema}._qrydfn` |
| *JOBD | Row in _jobd | `{schema}._jobd` |
| *OUTQ | Row in _outq | `{schema}._outq` |
| *AUTL | Row in _autl | `{schema}._autl` |
| *SBSD | Row in _sbsd | `{schema}._sbsd` |

---

## Adding New Features

### Adding a New Command

1. **Register in COMMANDS dict** (`screens.py` ~line 150):
```python
COMMANDS = {
    # ...existing...
    'NEWCMD': 'newscreen',  # Command -> screen mapping
}
```

2. **Add description** (`screens.py` COMMAND_DESCS):
```python
COMMAND_DESCS = {
    # ...existing...
    'NEWCMD': 'Description of new command',
}
```

3. **Create screen method** (`screens.py`):
```python
def _screen_newscreen(self, session: Session) -> dict:
    """New screen implementation."""
    hostname, date_str, time_str = get_system_info()

    content = [
        pad_line(f" {hostname:<20}   Screen Title                    {session.user:>10}"),
        pad_line(f"                                                          {date_str}  {time_str}"),
        pad_line(""),
        # ... screen content ...
    ]

    return {
        "type": "screen",
        "screen": "newscreen",
        "cols": 80,
        "content": content,
        "fields": [
            {"id": "field1", "row": 5, "col": 30, "len": 10, "value": ""},
        ],
        "activeField": 0,
    }
```

4. **Create submit handler** (if needed):
```python
def _submit_newscreen(self, session: Session, fields: dict) -> dict:
    """Handle Enter key on new screen."""
    # Process input
    return self.get_screen(session, 'next_screen')
```

5. **Add F12 cancel handler** (`screens.py` in handle_function_key F12 section ~line 540):
```python
elif screen == 'newscreen':
    return self.get_screen(session, 'parent_screen')
```

### Adding a New Database Table

1. **Add to SCHEMA_SQL** (`database.py` ~line 30):
```python
SCHEMA_SQL = """
-- ... existing tables ...

CREATE TABLE IF NOT EXISTS qsys.new_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(10) NOT NULL,
    -- ... columns ...
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
```

2. **Create CRUD functions** (`database.py`):
```python
def create_new_thing(name: str, ...) -> tuple[bool, str]:
    """Create a new thing."""
    try:
        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO qsys.new_table (name, ...)
                VALUES (%s, ...)
            """, (name, ...))
        return True, f"Thing {name} created"
    except Exception as e:
        return False, str(e)

def get_new_thing(name: str) -> dict | None:
    """Get a thing by name."""
    with get_cursor() as cursor:
        cursor.execute(
            "SELECT * FROM qsys.new_table WHERE name = %s",
            (name,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None
```

3. **Export in database.py imports** (at top of screens.py):
```python
from src.dk400.web.database import (
    # ...existing...
    create_new_thing, get_new_thing,
)
```

### Adding a New Object Type to WRKOBJ

1. **Add query in get_library_objects** (`database.py` ~line 740):
```python
# New Object Type
if obj_type in ('*ALL', '*NEWTYPE'):
    try:
        cursor.execute(sql.SQL("""
            SELECT name, 'NEWTYPE' as type, text, created, created_by
            FROM {}._newtype ORDER BY name
        """).format(sql.Identifier(lib_safe)))
        objects.extend([{**dict(r), 'library': lib} for r in cursor.fetchall()])
    except:
        pass
```

2. **Add display handler in _display_object** (`screens.py` ~line 8352):
```python
elif obj_type == 'NEWTYPE':
    session.field_values['selected_newtype'] = name
    session.field_values['selected_newtype_lib'] = library
    return self.get_screen(session, 'dspnewtype')
```

3. **Add delete handler in _delete_object** (`screens.py`):
```python
elif obj_type == 'NEWTYPE':
    return delete_newtype(library, name)
```

---

## Common Issues & Fixes

### psycopg2 % Escape in LIKE Patterns

**Problem:** `%` in SQL LIKE patterns gets interpreted as psycopg2 placeholder.

**Wrong:**
```python
cursor.execute("SELECT * FROM t WHERE name NOT LIKE '\\_%'", ())
# Error: tuple index out of range
```

**Correct:**
```python
cursor.execute("SELECT * FROM t WHERE name NOT LIKE '\\_%%'", ())
# %% becomes literal % in the SQL
```

### Column Field Name Mappings

`list_table_columns()` returns different keys than information_schema:

| list_table_columns | information_schema |
|-------------------|-------------------|
| `name` | `column_name` |
| `max_length` | `character_maximum_length` |
| `precision` | `numeric_precision` |
| `scale` | `numeric_scale` |
| `nullable` (bool) | `is_nullable` ('YES'/'NO') |

### get_cursor() Dict Mode

```python
# Default: returns RealDictRow (can use dict(row))
with get_cursor() as cursor:
    cursor.execute("SELECT * FROM qsys.users")
    row = cursor.fetchone()  # RealDictRow

# Tuple mode: returns plain tuples
with get_cursor(dict_cursor=False) as cursor:
    cursor.execute("SELECT * FROM qsys.users")
    row = cursor.fetchone()  # tuple
```

### Dict Cursor with SELECT EXISTS

**Problem:** `SELECT EXISTS(...)` returns a column named `exists`, not index 0.

**Wrong:**
```python
with get_cursor() as cursor:  # Dict cursor (default)
    cursor.execute("SELECT EXISTS (SELECT 1 FROM ...)")
    if not cursor.fetchone()[0]:  # ERROR: dict doesn't support [0]
```

**Correct:**
```python
with get_cursor() as cursor:
    cursor.execute("SELECT EXISTS (SELECT 1 FROM ...)")
    if not cursor.fetchone()['exists']:  # Use column name
```

### WebSocket Disconnects

Silent errors in screen methods cause WebSocket disconnects. Always check:
1. KeyError in dict access (use `.get()` with defaults)
2. Missing imports
3. Exceptions in try/except blocks being swallowed

---

## Screen Patterns

### Standard Screen Layout

```
Line 1:  {hostname:<20}   {title:^30}   {user:>10}
Line 2:  {empty:50}                     {date}  {time}
Line 3:  (empty)
Line 4+: Content/fields
Line 20: Message line
Line 21: (empty)
Line 22: F-keys
```

### Option List Pattern (WRKXXX)

```python
def _screen_wrkxxx(self, session: Session) -> dict:
    items = get_xxx_list()
    offset = session.get_offset('wrkxxx')
    page_size = PAGE_SIZES.get('wrkxxx', 10)

    content = [
        # Header lines...
        pad_line(" Type options, press Enter."),
        pad_line("   2=Change   4=Delete   5=Display"),
        pad_line(""),
        [{"type": "text", "text": pad_line(" Opt  Name       Description"), "class": "field-reverse"}],
    ]

    fields = []
    page_items = items[offset:offset + page_size]
    for i, item in enumerate(page_items):
        fields.append({"id": f"opt_{i}", "row": 8 + i, "col": 2, "len": 2, "value": ""})
        content.append(pad_line(f"      {item['name']:<10} {item['desc']}"))

    # Pagination indicator
    more = "More..." if len(items) > offset + page_size else "Bottom"
    content.append(pad_line(f"{'':>70}{more}"))

    return {
        "type": "screen",
        "screen": "wrkxxx",
        "cols": 80,
        "content": content,
        "fields": fields,
        "activeField": 0,
    }
```

### Submit Handler for Options

```python
def _submit_wrkxxx(self, session: Session, fields: dict) -> dict:
    items = get_xxx_list()
    offset = session.get_offset('wrkxxx')
    page_size = PAGE_SIZES.get('wrkxxx', 10)
    page_items = items[offset:offset + page_size]

    for i, item in enumerate(page_items):
        opt = fields.get(f'opt_{i}', '').strip()
        if opt == '2':  # Change
            session.field_values['selected_xxx'] = item['name']
            return self.get_screen(session, 'chgxxx')
        elif opt == '4':  # Delete
            success, msg = delete_xxx(item['name'])
            session.message = msg
            break
        elif opt == '5':  # Display
            session.field_values['selected_xxx'] = item['name']
            return self.get_screen(session, 'dspxxx')

    return self.get_screen(session, 'wrkxxx')
```

---

## Key Constants

### PAGE_SIZES (`screens.py` ~line 200)
```python
PAGE_SIZES = {
    'wrklib': 10,
    'wrkobj': 12,
    'wrkusrprf': 10,
    'wrksysval': 12,
    'wrkqry': 10,
    'qryfields': 15,
    'qrywhere': 8,
    'qrysort': 8,
    'qryrun': 18,
    'dspfd': 12,
    # ...
}
```

### USER_CLASS_GRANTS (`database.py` ~line 80)
Defines table permissions by user class (*SECOFR, *SECADM, *PGMR, *SYSOPR, *USER).

### QUERY_OPERATORS (`database.py`)
```python
QUERY_OPERATORS = {
    'EQ': ('=', 'Equal'),
    'NE': ('<>', 'Not Equal'),
    'GT': ('>', 'Greater Than'),
    'LT': ('<', 'Less Than'),
    'GE': ('>=', 'Greater/Equal'),
    'LE': ('<=', 'Less/Equal'),
    'CT': ('LIKE', 'Contains'),
    'SW': ('LIKE', 'Starts With'),
    'NL': ('IS NULL', 'Is Null'),
    'NN': ('IS NOT NULL', 'Not Null'),
}
```

---

## WRKQRY - Query/400 Implementation

### Overview

WRKQRY provides AS/400 Query/400-style prompted SQL interface. Users build queries through menu-driven screens without writing SQL.

### Screen Flow

```
WRKQRY (list)
    │
    ├── F6=Create / Opt 1=Create / Opt 2=Change
    │   └── qrydefine (Define Query menu)
    │           ├── Opt 1: qryfiles (Select file - schema/table)
    │           ├── Opt 2: qryfields (Select columns)
    │           ├── Opt 3: qrywhere (WHERE conditions)
    │           │           └── F6=Add / Opt 2=Change → qrycond
    │           ├── Opt 4: qrysort (ORDER BY)
    │           ├── F5=Run → qryrun (preview results)
    │           └── F10=Save
    │
    ├── Opt 4=Delete (confirm)
    │
    └── Opt 5=Run → qryrun (execute and display)
```

### Session State Keys

```python
# Query definition being built/edited
'qry_name': 'CUSTQRY'           # Query name (max 10 chars)
'qry_library': 'QGPL'           # Library to save in
'qry_desc': 'Customer list'     # Description (max 50 chars)
'qry_mode': 'create'            # 'create' or 'change'

# Source file
'qry_schema': 'public'          # PostgreSQL schema
'qry_table': 'customers'        # Table name

# Query components
'qry_columns': [                # Selected columns with sequence
    {'name': 'ID', 'seq': 10},
    {'name': 'NAME', 'seq': 20},
]
'qry_conditions': [             # WHERE conditions
    {'field': 'STATUS', 'op': 'EQ', 'value': 'ACTIVE', 'and_or': 'AND'},
]
'qry_orderby': [                # ORDER BY fields
    {'field': 'NAME', 'seq': 1, 'dir': 'ASC'},
]
```

### Database Table: `{library}._qrydfn`

Query definitions are stored per-library in `_qrydfn` tables:

```sql
CREATE TABLE IF NOT EXISTS {schema}._qrydfn (
    name VARCHAR(10) PRIMARY KEY,
    text VARCHAR(50) DEFAULT '',
    source_schema VARCHAR(128),
    source_table VARCHAR(128),
    selected_columns JSONB DEFAULT '[]',
    where_conditions JSONB DEFAULT '[]',
    order_by_fields JSONB DEFAULT '[]',
    summary_functions JSONB DEFAULT '[]',
    group_by_fields JSONB DEFAULT '[]',
    output_type VARCHAR(10) DEFAULT '*DISPLAY',
    row_limit INTEGER DEFAULT 0,
    created_by VARCHAR(10),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_run TIMESTAMP
);
```

### Key Functions (`database.py`)

```python
# Query Definition CRUD
create_query_definition(name, library, description, source_schema, source_table,
                       selected_columns, where_conditions, order_by_fields, ...) -> tuple[bool, str]
get_query_definition(name, library) -> dict | None
update_query_definition(name, library, **kwargs) -> tuple[bool, str]
delete_query_definition(name, library) -> tuple[bool, str]
list_query_definitions(library=None, created_by=None) -> list[dict]

# Query Execution
execute_query(schema, table, columns, conditions, order_by, limit, offset) -> tuple[bool, list|str, list]
```

---

## AS/400 Screen Design Style Guide

### Screen Fundamentals

#### Display Size
- **Standard:** 80 columns x 24 rows (1920 characters)
- Row numbering: 1-24 (top to bottom)
- Column numbering: 1-80 (left to right)

#### The 24-Line Grid
```
Row 1:   System identifier line (hostname, title, user)
Row 2:   Date/time line
Row 3:   (usually blank separator)
Rows 4-6: Instructions or parameter prompts
Rows 7-19: Main content area (list data, form fields)
Row 20:  Message line (error/informational messages)
Row 21:  (usually blank separator)
Rows 22-24: Function key legend
```

### Screen Types

#### 1. Menu Screens
Used for selection menus (like main menu after signon).

```
Row 4:  "Select one of the following:"
Row 5:  (blank)
Row 6+: " n. Option description" (n = number, 2 spaces before number)
...
Row 18: (end options)
Row 19: (blank)
Row 20: Message line
Row 21: "Selection or command"
Row 22: "===>" followed by input field
Row 23: Function keys
```

#### 2. Entry/Prompt Screens (DSPXXX/CHGXXX)
Used for data entry forms and parameter prompts.

```
Row 4:  "Type choices, press Enter."
Row 5:  (blank)
Row 6+: "  Field label . . . . . . . :  [input field]"
...     (labels right-aligned with dots, colon before value)
Row 20: Message line
Row 21-22: (blank or continuation)
Row 23-24: Function keys
```

#### 3. List/Work-With Screens (WRKXXX)
Used for listing objects with options.

```
Row 4:  "Type options, press Enter."
Row 5:  "  2=Change   4=Delete   5=Display   ..." (option legend)
Row 6:  (blank)
Row 7:  Column headers (reverse video)
Row 8+: Data rows with option column
...
Row 19: "More..." or "Bottom" indicator (right-justified)
Row 20: Message line
Row 21: (blank)
Row 22-24: Function keys
```

### Function Key Standards

#### Universal Keys (must be consistent across ALL screens)
| Key | Action | Notes |
|-----|--------|-------|
| F1 | Help | Display context-sensitive help |
| F3 | Exit | Exit program/function entirely |
| F4 | Prompt | Show valid values for current field |
| F5 | Refresh | Reload current display |
| F12 | Cancel | Return to previous screen |

#### Common Optional Keys
| Key | Action | Typical Use |
|-----|--------|-------------|
| F6 | Create/Add | Add new item to list |
| F9 | Retrieve | Retrieve previous command |
| F10 | Save | Save changes (entry screens) |
| F11 | Toggle view | Alternate view/more details |

### Color and Attribute Conventions

#### Display Attributes
| Attribute | CSS Class | Usage |
|-----------|-----------|-------|
| Normal (GRN) | `field-normal` | Regular text, protected fields |
| High Intensity (WHT) | `field-high` | Emphasis, column headers |
| Reverse Image (RI) | `field-reverse` | Column headers, selection bars |
| Underline (UL) | `field-input` | Input fields |
| Red/Blink | `field-error` | Error messages |
| Blue/Column Sep | `field-info` | Informational text |

### Cursor Navigation

| Key | Action |
|-----|--------|
| Tab | Next input field (tab order) |
| Shift+Tab | Previous input field (tab order) |
| Arrow Up | Move to field above (spatial) |
| Arrow Down | Move to field below (spatial) |
| Home | Start of current field |
| End | End of current field |
| Enter | Submit screen |
| PageUp | Roll Up |
| PageDown | Roll Down |

---

## Clickable Hotspots

DK/400 supports clickable hotspots for mouse-based navigation.

### Hotspot Types

| Type | Action | Usage |
|------|--------|-------|
| `page_up` / `roll_up` | Roll up (previous page) | `<Prev>` indicators |
| `page_down` / `roll_down` | Roll down (next page) | `<More>` indicators |
| `fkey_F3`, `fkey_F5`, etc. | Trigger function key | Clickable F-key labels |

### Function Key Lines with fkey_line()

```python
# Instead of:
content.append(pad_line(" F3=Exit  F5=Refresh  F12=Cancel"))

# Use:
content.append(fkey_line("F3=Exit  F5=Refresh  F12=Cancel"))

# With PageUp/PageDown:
content.append(fkey_line("F3=Exit  F5=Refresh  F12=Cancel  PageDown=Roll Down  PageUp=Roll Up"))
```

---

## Journaling System

DK/400 implements AS/400-style journaling for change tracking and recovery.

### Journal Objects

| Object | Table | Purpose |
|--------|-------|---------|
| Journal (*JRN) | `qsys._jrn` | Journal configuration |
| Journal Receiver (*JRNRCV) | `qsys._jrnrcv` | Physical storage for entries |
| Journal Entries | `qsys._jrne` | Before/after images of changes |
| Journaled Files | `qsys._jrnpf` | Which tables are being journaled |

### Entry Types (Journal Code 'F' - File Operations)

| Code | Meaning | Data Stored |
|------|---------|-------------|
| PT | Record Added (Put) | After-image |
| UP | Record Updated (After) | After-image |
| UB | Record Updated (Before) | Before-image |
| DL | Record Deleted | Before-image |

### Commands

| Command | Screen | Purpose |
|---------|--------|---------|
| WRKJRN | wrkjrn | Work with Journals |
| DSPJRN | dspjrn | Display Journal Entries |
| CRTJRN | crtjrn | Create Journal |
| CRTJRNRCV | crtjrnrcv | Create Journal Receiver |
| STRJRNPF | strjrnpf | Start Journaling Physical File |
| ENDJRNPF | endjrnpf | End Journaling Physical File |

### Database Functions

```python
# Journal CRUD
create_journal(name, library, receiver, text, images='*AFTER') -> tuple[bool, str]
get_journal(name, library) -> dict | None
list_journals(library=None) -> list[dict]

# Receiver CRUD
create_journal_receiver(name, library, journal, journal_library, text) -> tuple[bool, str]
attach_receiver(journal, receiver, library) -> tuple[bool, str]
detach_receiver(journal, library) -> tuple[bool, str]

# Start/End journaling
start_journal_pf(schema, table, journal, library, images='*AFTER') -> tuple[bool, str]
end_journal_pf(schema, table) -> tuple[bool, str]

# Query entries
get_journal_entries(journal, library, object_name, entry_type, from_time, to_time, limit) -> list[dict]
```

---

## User Profiles

User profiles are stored in `qsys.qausrprf` with AS/400 field compatibility.

### Key Columns

```sql
username VARCHAR(10) PRIMARY KEY    -- User ID
password_hash, salt                  -- Authentication
user_class VARCHAR(10)               -- *SECOFR, *SECADM, *PGMR, *SYSOPR, *USER
status VARCHAR(10)                   -- *ENABLED, *DISABLED
description VARCHAR(50)              -- Text description
spcaut JSONB                         -- Special authorities array
group_profile VARCHAR(10)            -- Primary group
current_library VARCHAR(10)          -- *CURLIB
inllibl JSONB                        -- Initial library list array
inlpgm, inlpgm_lib VARCHAR(10)       -- Initial program
inlmnu, inlmnu_lib VARCHAR(10)       -- Initial menu
```

### User Management Functions

```python
from src.dk400.web.users import user_manager

# Create user
success, msg = user_manager.create_user(
    username="NEWUSER",
    password="password",
    user_class="*USER",
    description="New user"
)

# Get user profile
profile = user_manager.get_user("DOUG")  # Returns UserProfile or None

# Update user
success, msg = user_manager.update_user(
    username="DOUG",
    user_class="*PGMR",
    description="Developer"
)

# Change password
success, msg = user_manager.change_password("DOUG", "newpass")
```

---

### Reference Sources

- [5250 Terminal Overview](https://as400i.com/2013/03/07/overview-of-the-green-screen-5250-terminal/)
- [Display File Design Standards](https://techdocs.broadcom.com/us/en/ca-enterprise-software/it-operations-management/ca-2e/8-7/Standards/ibm-i-general-design-standards/design-standards-for-display-files.html)
- [5250 Colors and Display Attributes](https://try-as400.pocnet.net/wiki/5250_colors_and_display_attributes)
- [DSPATR Keyword Reference](https://www.ibm.com/docs/en/i/7.3.0?topic=80-dspatr-display-attribute-keyword-display-files)
- [Query/400 Getting Started](https://www.mcpressonline.com/analytics-cognitive/business-intelligence/getting-started-with-query400)
