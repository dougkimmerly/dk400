"""
DK/400 Database Module

PostgreSQL database connection and schema management.
"""
import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Optional, Generator
import logging

logger = logging.getLogger(__name__)


# Database configuration from environment
DB_CONFIG = {
    'host': os.environ.get('DK400_DB_HOST', 'localhost'),
    'port': int(os.environ.get('DK400_DB_PORT', 5432)),
    'dbname': os.environ.get('DK400_DB_NAME', 'dk400'),
    'user': os.environ.get('DK400_DB_USER', 'dk400'),
    'password': os.environ.get('DK400_DB_PASSWORD', 'dk400secret'),
}


# Schema definitions
SCHEMA_SQL = """
-- Users table for authentication
CREATE TABLE IF NOT EXISTS users (
    username VARCHAR(10) PRIMARY KEY,
    password_hash VARCHAR(128) NOT NULL,
    salt VARCHAR(64) NOT NULL,
    user_class VARCHAR(10) DEFAULT '*USER',
    status VARCHAR(10) DEFAULT '*ENABLED',
    description VARCHAR(50) DEFAULT '',
    group_profile VARCHAR(10) DEFAULT '*NONE',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_signon TIMESTAMP,
    signon_attempts INTEGER DEFAULT 0,
    password_expires VARCHAR(10) DEFAULT '*NOMAX'
);

-- Add group_profile column if it doesn't exist (for existing databases)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'users' AND column_name = 'group_profile') THEN
        ALTER TABLE users ADD COLUMN group_profile VARCHAR(10) DEFAULT '*NONE';
    END IF;
END $$;

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);
CREATE INDEX IF NOT EXISTS idx_users_group ON users(group_profile);

-- Job history table (for future use)
CREATE TABLE IF NOT EXISTS job_history (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(50) NOT NULL,
    job_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    submitted_by VARCHAR(10),
    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    result TEXT,
    error TEXT
);

-- Audit log table (for future use)
CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    username VARCHAR(10),
    action VARCHAR(50) NOT NULL,
    details TEXT,
    ip_address VARCHAR(45)
);

-- Index for audit log queries
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_username ON audit_log(username);

-- Object authorities table (AS/400-style object permissions)
CREATE TABLE IF NOT EXISTS object_authorities (
    id SERIAL PRIMARY KEY,
    object_type VARCHAR(20) NOT NULL,   -- TABLE, VIEW, FUNCTION, etc.
    object_name VARCHAR(128) NOT NULL,
    username VARCHAR(10) NOT NULL,
    authority VARCHAR(10) NOT NULL,     -- *ALL, *CHANGE, *USE, *EXCLUDE
    granted_by VARCHAR(10),
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(object_type, object_name, username)
);

-- Index for object authority lookups
CREATE INDEX IF NOT EXISTS idx_objauth_object ON object_authorities(object_type, object_name);
CREATE INDEX IF NOT EXISTS idx_objauth_user ON object_authorities(username);

-- System values table (AS/400-style WRKSYSVAL)
CREATE TABLE IF NOT EXISTS system_values (
    name VARCHAR(20) PRIMARY KEY,
    value VARCHAR(256) NOT NULL,
    description VARCHAR(100) DEFAULT '',
    category VARCHAR(20) DEFAULT 'SYSTEM',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(10) DEFAULT 'SYSTEM'
);

-- Default system values
INSERT INTO system_values (name, value, description, category) VALUES
    ('QSYSNAME', 'DK400', 'System name', 'SYSTEM'),
    ('QLOGOSIZE', '*SMALL', 'Logo display size (*FULL, *SMALL, *NONE)', 'DISPLAY'),
    ('QDATFMT', '*MDY', 'Date format (*MDY, *DMY, *YMD, *ISO)', 'DATETIME'),
    ('QTIMSEP', ':', 'Time separator character', 'DATETIME'),
    ('QDATSEP', '/', 'Date separator character', 'DATETIME')
ON CONFLICT (name) DO NOTHING;

-- =============================================================================
-- Message Queues (AS/400 MSGQ)
-- =============================================================================

-- Message queues table
CREATE TABLE IF NOT EXISTS message_queues (
    name VARCHAR(10) PRIMARY KEY,
    description VARCHAR(50) DEFAULT '',
    queue_type VARCHAR(10) DEFAULT '*STD',     -- *STD, *SYSOPR
    max_size INTEGER DEFAULT 1000,
    created_by VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Messages in queues
CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    queue_name VARCHAR(10) NOT NULL REFERENCES message_queues(name) ON DELETE CASCADE,
    msg_id VARCHAR(7) DEFAULT '',              -- Message ID (e.g., CPF1234)
    msg_type VARCHAR(10) DEFAULT '*INFO',      -- *INFO, *INQ, *COMP, *DIAG, *ESCAPE
    msg_text VARCHAR(512) NOT NULL,
    msg_data TEXT,                             -- Additional data/parameters
    severity INTEGER DEFAULT 0,                -- 0-99
    sent_by VARCHAR(10),
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reply VARCHAR(256),                        -- For *INQ messages
    replied_at TIMESTAMP,
    status VARCHAR(10) DEFAULT '*NEW'          -- *NEW, *OLD, *ANSWERED
);

CREATE INDEX IF NOT EXISTS idx_messages_queue ON messages(queue_name, status);
CREATE INDEX IF NOT EXISTS idx_messages_sent ON messages(sent_at);

-- Default message queues
INSERT INTO message_queues (name, description, queue_type, created_by) VALUES
    ('QSYSOPR', 'System Operator Message Queue', '*SYSOPR', 'SYSTEM'),
    ('QSYSMSG', 'System Message Queue', '*STD', 'SYSTEM')
ON CONFLICT (name) DO NOTHING;

-- =============================================================================
-- Data Areas (AS/400 DTAARA)
-- =============================================================================

CREATE TABLE IF NOT EXISTS data_areas (
    name VARCHAR(10) NOT NULL,
    library VARCHAR(10) NOT NULL DEFAULT '*LIBL',
    type VARCHAR(10) DEFAULT '*CHAR',          -- *CHAR, *DEC, *LGL
    length INTEGER DEFAULT 2000,
    decimal_positions INTEGER DEFAULT 0,
    value TEXT DEFAULT '',
    description VARCHAR(50) DEFAULT '',
    locked_by VARCHAR(10),                     -- User who has exclusive lock
    locked_at TIMESTAMP,
    created_by VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(10),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (name, library)
);

CREATE INDEX IF NOT EXISTS idx_dtaara_library ON data_areas(library);

-- Default data areas
INSERT INTO data_areas (name, library, type, length, value, description, created_by) VALUES
    ('QDATE', '*LIBL', '*CHAR', 8, '', 'System date data area', 'SYSTEM'),
    ('QTIME', '*LIBL', '*CHAR', 6, '', 'System time data area', 'SYSTEM')
ON CONFLICT (name, library) DO NOTHING;

-- =============================================================================
-- Job Descriptions (AS/400 JOBD)
-- =============================================================================

CREATE TABLE IF NOT EXISTS job_descriptions (
    name VARCHAR(10) NOT NULL,
    library VARCHAR(10) NOT NULL DEFAULT '*LIBL',
    description VARCHAR(50) DEFAULT '',
    job_queue VARCHAR(10) DEFAULT 'QBATCH',    -- Default job queue
    job_priority INTEGER DEFAULT 5,            -- 1-9
    output_queue VARCHAR(10) DEFAULT '*USRPRF',
    print_device VARCHAR(10) DEFAULT '*USRPRF',
    routing_data VARCHAR(80) DEFAULT 'QCMDB',
    request_data VARCHAR(256) DEFAULT '',
    user_profile VARCHAR(10) DEFAULT '*RQD',   -- User to run as
    accounting_code VARCHAR(15) DEFAULT '',
    log_level INTEGER DEFAULT 4,               -- 0-4
    log_severity INTEGER DEFAULT 20,           -- 0-99
    log_text VARCHAR(10) DEFAULT '*MSG',       -- *MSG, *SECLVL, *NOLIST
    hold_on_jobq VARCHAR(10) DEFAULT '*NO',    -- *YES, *NO
    created_by VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (name, library)
);

-- Default job descriptions
INSERT INTO job_descriptions (name, library, description, job_queue, job_priority, created_by) VALUES
    ('QBATCH', '*LIBL', 'Default batch job description', 'QBATCH', 5, 'SYSTEM'),
    ('QINTER', '*LIBL', 'Interactive job description', 'QINTER', 2, 'SYSTEM'),
    ('QSPL', '*LIBL', 'Spooling job description', 'QSPL', 5, 'SYSTEM')
ON CONFLICT (name, library) DO NOTHING;

-- =============================================================================
-- Output Queues and Spooled Files (AS/400 OUTQ/SPLF)
-- =============================================================================

-- Output queues
CREATE TABLE IF NOT EXISTS output_queues (
    name VARCHAR(10) NOT NULL,
    library VARCHAR(10) NOT NULL DEFAULT '*LIBL',
    description VARCHAR(50) DEFAULT '',
    status VARCHAR(10) DEFAULT '*RLS',         -- *RLS, *HLD
    max_size INTEGER DEFAULT 0,                -- 0 = unlimited
    authority VARCHAR(10) DEFAULT '*USE',
    created_by VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (name, library)
);

-- Spooled files (job output)
CREATE TABLE IF NOT EXISTS spooled_files (
    id SERIAL PRIMARY KEY,
    name VARCHAR(10) NOT NULL,
    file_number INTEGER NOT NULL DEFAULT 1,
    job_name VARCHAR(28) NOT NULL,             -- job/user/number format
    job_id VARCHAR(36),                        -- Celery task ID
    output_queue VARCHAR(10) DEFAULT 'QPRINT',
    status VARCHAR(10) DEFAULT '*RDY',         -- *RDY, *HLD, *PND, *PRT, *SAV
    user_data VARCHAR(10) DEFAULT '',
    form_type VARCHAR(10) DEFAULT '*STD',
    copies INTEGER DEFAULT 1,
    pages INTEGER DEFAULT 0,
    total_records INTEGER DEFAULT 0,
    content TEXT,                              -- Actual spooled output
    created_by VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_splf_job ON spooled_files(job_name);
CREATE INDEX IF NOT EXISTS idx_splf_outq ON spooled_files(output_queue, status);
CREATE INDEX IF NOT EXISTS idx_splf_user ON spooled_files(created_by);

-- Default output queues
INSERT INTO output_queues (name, library, description, created_by) VALUES
    ('QPRINT', '*LIBL', 'Default print output queue', 'SYSTEM'),
    ('QPRINT2', '*LIBL', 'Secondary print output queue', 'SYSTEM')
ON CONFLICT (name, library) DO NOTHING;

-- =============================================================================
-- Job Schedule Entries (AS/400 WRKJOBSCDE)
-- =============================================================================

CREATE TABLE IF NOT EXISTS job_schedule_entries (
    name VARCHAR(20) PRIMARY KEY,
    description VARCHAR(50) DEFAULT '',
    command TEXT NOT NULL,                     -- Command/task to run
    job_description VARCHAR(10) DEFAULT 'QBATCH',
    frequency VARCHAR(10) DEFAULT '*ONCE',     -- *ONCE, *WEEKLY, *MONTHLY, *DAILY
    schedule_date DATE,                        -- For *ONCE
    schedule_time TIME,
    days_of_week VARCHAR(20),                  -- For *WEEKLY: 'MON,TUE,WED' etc
    day_of_month INTEGER,                      -- For *MONTHLY: 1-31
    relative_day VARCHAR(10),                  -- *FIRST, *SECOND, *THIRD, *FOURTH, *LAST
    relative_weekday VARCHAR(10),              -- Day for relative (MON, TUE, etc)
    status VARCHAR(10) DEFAULT '*ACTIVE',      -- *ACTIVE, *HELD
    last_run_at TIMESTAMP,
    last_run_status VARCHAR(10),               -- *SUCCESS, *FAILED
    next_run_at TIMESTAMP,
    created_by VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(10),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_jobscde_status ON job_schedule_entries(status);
CREATE INDEX IF NOT EXISTS idx_jobscde_next ON job_schedule_entries(next_run_at);

-- =============================================================================
-- Authorization Lists (AS/400 AUTL)
-- =============================================================================

CREATE TABLE IF NOT EXISTS authorization_lists (
    name VARCHAR(10) PRIMARY KEY,
    description VARCHAR(50) DEFAULT '',
    created_by VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Users in authorization lists
CREATE TABLE IF NOT EXISTS authorization_list_entries (
    autl_name VARCHAR(10) NOT NULL REFERENCES authorization_lists(name) ON DELETE CASCADE,
    username VARCHAR(10) NOT NULL,
    authority VARCHAR(10) NOT NULL DEFAULT '*USE',  -- *USE, *CHANGE, *ALL, *EXCLUDE
    added_by VARCHAR(10),
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (autl_name, username)
);

-- Objects secured by authorization lists
CREATE TABLE IF NOT EXISTS authorization_list_objects (
    autl_name VARCHAR(10) NOT NULL REFERENCES authorization_lists(name) ON DELETE CASCADE,
    object_type VARCHAR(20) NOT NULL,
    object_name VARCHAR(128) NOT NULL,
    added_by VARCHAR(10),
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (autl_name, object_type, object_name)
);

CREATE INDEX IF NOT EXISTS idx_autle_user ON authorization_list_entries(username);
CREATE INDEX IF NOT EXISTS idx_autlo_object ON authorization_list_objects(object_type, object_name);

-- =============================================================================
-- Subsystem Descriptions (AS/400 SBSD)
-- =============================================================================

CREATE TABLE IF NOT EXISTS subsystem_descriptions (
    name VARCHAR(10) PRIMARY KEY,
    description VARCHAR(50) DEFAULT '',
    status VARCHAR(10) DEFAULT '*INACTIVE',    -- *ACTIVE, *INACTIVE
    max_active_jobs INTEGER DEFAULT 0,         -- 0 = no limit
    current_jobs INTEGER DEFAULT 0,
    pool_id VARCHAR(10) DEFAULT '*BASE',
    celery_queue VARCHAR(50),                  -- Maps to Celery queue name
    worker_concurrency INTEGER DEFAULT 4,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    stopped_at TIMESTAMP
);

-- Job queue entries for subsystems
CREATE TABLE IF NOT EXISTS subsystem_job_queues (
    subsystem_name VARCHAR(10) NOT NULL REFERENCES subsystem_descriptions(name) ON DELETE CASCADE,
    job_queue VARCHAR(10) NOT NULL,
    sequence INTEGER DEFAULT 10,               -- Processing order
    max_active INTEGER DEFAULT 0,              -- Max jobs from this queue
    PRIMARY KEY (subsystem_name, job_queue)
);

-- Default subsystems (map to Celery workers)
INSERT INTO subsystem_descriptions (name, description, status, celery_queue, worker_concurrency) VALUES
    ('QBATCH', 'Batch Subsystem', '*ACTIVE', 'celery', 4),
    ('QINTER', 'Interactive Subsystem', '*ACTIVE', 'interactive', 2),
    ('QSPL', 'Spooling Subsystem', '*ACTIVE', 'spooling', 1),
    ('QCTL', 'Controlling Subsystem', '*ACTIVE', NULL, 1)
ON CONFLICT (name) DO NOTHING;

-- Default job queue assignments
INSERT INTO subsystem_job_queues (subsystem_name, job_queue, sequence, max_active) VALUES
    ('QBATCH', 'QBATCH', 10, 4),
    ('QINTER', 'QINTER', 10, 10),
    ('QSPL', 'QSPL', 10, 2)
ON CONFLICT (subsystem_name, job_queue) DO NOTHING;

-- =============================================================================
-- Commands (AS/400 *CMD objects)
-- Naming follows QSYS2.COMMAND_INFO pattern from IBM i
-- =============================================================================

-- Command definitions (matches QSYS2.COMMAND_INFO structure)
CREATE TABLE IF NOT EXISTS COMMAND_INFO (
    COMMAND_NAME VARCHAR(10) PRIMARY KEY,
    COMMAND_LIBRARY VARCHAR(10) DEFAULT 'QSYS',
    TEXT_DESCRIPTION VARCHAR(50) DEFAULT '',
    SCREEN_NAME VARCHAR(30),                    -- DK/400 extension: screen to display
    PROCESSING_PROGRAM VARCHAR(100),            -- Python module/function (future)
    ALLOW_RUN_INTERACTIVE VARCHAR(3) DEFAULT 'YES',
    ALLOW_RUN_BATCH VARCHAR(3) DEFAULT 'YES',
    THREADSAFE VARCHAR(5) DEFAULT '*NO',
    CREATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CREATED_BY VARCHAR(10) DEFAULT 'SYSTEM'
);

-- Command parameters (DK/400 extension - no AS/400 SQL equivalent)
-- Column naming follows QSYS2.SYSPARMS pattern where applicable
CREATE TABLE IF NOT EXISTS COMMAND_PARM_INFO (
    COMMAND_NAME VARCHAR(10) NOT NULL REFERENCES COMMAND_INFO(COMMAND_NAME) ON DELETE CASCADE,
    PARM_NAME VARCHAR(10) NOT NULL,             -- Keyword (USER, USRCLS, etc.)
    ORDINAL_POSITION INTEGER NOT NULL,          -- Parameter order (matches SYSPARMS)
    DATA_TYPE VARCHAR(10) DEFAULT '*CHAR',      -- *CHAR, *DEC, *NAME, *CMD, *LGL
    LENGTH INTEGER DEFAULT 10,
    DECIMAL_POSITIONS INTEGER DEFAULT 0,
    DEFAULT_VALUE VARCHAR(100),
    PROMPT_TEXT VARCHAR(40) NOT NULL,           -- F4 prompt label
    IS_REQUIRED VARCHAR(3) DEFAULT 'NO',        -- YES/NO (AS/400 style)
    MIN_VALUE VARCHAR(50),                      -- For numeric validation
    MAX_VALUE VARCHAR(50),                      -- For numeric validation
    PRIMARY KEY (COMMAND_NAME, PARM_NAME)
);

-- Index for parameter ordering
CREATE INDEX IF NOT EXISTS idx_cmd_parm_ord ON COMMAND_PARM_INFO(COMMAND_NAME, ORDINAL_POSITION);

-- Valid values for parameters (DK/400 extension for F4 prompts)
CREATE TABLE IF NOT EXISTS PARM_VALID_VALUES (
    COMMAND_NAME VARCHAR(10) NOT NULL,
    PARM_NAME VARCHAR(10) NOT NULL,
    VALID_VALUE VARCHAR(50) NOT NULL,
    TEXT_DESCRIPTION VARCHAR(50) DEFAULT '',
    ORDINAL_POSITION INTEGER DEFAULT 0,         -- Display order
    PRIMARY KEY (COMMAND_NAME, PARM_NAME, VALID_VALUE),
    FOREIGN KEY (COMMAND_NAME, PARM_NAME)
        REFERENCES COMMAND_PARM_INFO(COMMAND_NAME, PARM_NAME) ON DELETE CASCADE
);

-- Index for value lookups
CREATE INDEX IF NOT EXISTS idx_parm_val_ord ON PARM_VALID_VALUES(COMMAND_NAME, PARM_NAME, ORDINAL_POSITION);
"""


def get_connection() -> psycopg2.extensions.connection:
    """Get a database connection."""
    return psycopg2.connect(**DB_CONFIG)


@contextmanager
def get_cursor(dict_cursor: bool = True) -> Generator:
    """Context manager for database cursor."""
    conn = get_connection()
    try:
        cursor_factory = RealDictCursor if dict_cursor else None
        cursor = conn.cursor(cursor_factory=cursor_factory)
        yield cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def init_database() -> bool:
    """Initialize the database schema."""
    try:
        with get_cursor(dict_cursor=False) as cursor:
            cursor.execute(SCHEMA_SQL)
        logger.info("Database schema initialized successfully")
        # Populate default commands after schema creation
        populate_default_commands()
        return True
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return False


def check_connection() -> bool:
    """Check if database is accessible."""
    try:
        with get_cursor() as cursor:
            cursor.execute("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False


def migrate_from_json(json_file: str) -> tuple[bool, str]:
    """
    Migrate users from JSON file to PostgreSQL.
    Used for one-time migration from old storage.
    """
    import json
    from pathlib import Path

    json_path = Path(json_file)
    if not json_path.exists():
        return False, f"JSON file not found: {json_file}"

    try:
        with open(json_path, 'r') as f:
            users_data = json.load(f)

        migrated = 0
        skipped = 0

        with get_cursor() as cursor:
            for username, user in users_data.items():
                # Check if user already exists
                cursor.execute(
                    "SELECT 1 FROM users WHERE username = %s",
                    (username,)
                )
                if cursor.fetchone():
                    skipped += 1
                    continue

                # Insert user
                cursor.execute("""
                    INSERT INTO users (
                        username, password_hash, salt, user_class, status,
                        description, created, last_signon, signon_attempts,
                        password_expires
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    user['username'],
                    user['password_hash'],
                    user['salt'],
                    user.get('user_class', '*USER'),
                    user.get('status', '*ENABLED'),
                    user.get('description', ''),
                    user.get('created'),
                    user.get('last_signon'),
                    user.get('signon_attempts', 0),
                    user.get('password_expires', '*NOMAX'),
                ))
                migrated += 1

        return True, f"Migrated {migrated} users, skipped {skipped} existing"

    except Exception as e:
        return False, f"Migration failed: {e}"


# =============================================================================
# PostgreSQL Role Management (AS/400-style Object Authority)
# =============================================================================

# Map AS/400 user classes to PostgreSQL grants
# Each class defines what tables/operations the role can access
USER_CLASS_GRANTS = {
    '*SECOFR': {
        # Security Officer - full access to everything
        'tables': {
            'users': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            'job_history': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            'audit_log': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            'object_authorities': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
        },
        'special': ['CREATEROLE'],  # Can manage other roles
    },
    '*SECADM': {
        # Security Admin - can manage users but not full system access
        'tables': {
            'users': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            'job_history': ['SELECT'],
            'audit_log': ['SELECT', 'INSERT'],
            'object_authorities': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
        },
        'special': [],
    },
    '*PGMR': {
        # Programmer - full access to app tables, read users
        'tables': {
            'users': ['SELECT'],
            'job_history': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            'audit_log': ['SELECT', 'INSERT'],
            'object_authorities': ['SELECT'],
        },
        'special': [],
    },
    '*SYSOPR': {
        # System Operator - operational access
        'tables': {
            'users': ['SELECT'],
            'job_history': ['SELECT', 'INSERT', 'UPDATE'],
            'audit_log': ['SELECT', 'INSERT'],
            'object_authorities': ['SELECT'],
        },
        'special': [],
    },
    '*USER': {
        # Regular user - read-only on most tables
        'tables': {
            'users': [],  # No direct access to users table
            'job_history': ['SELECT'],
            'audit_log': [],  # No access to audit log
            'object_authorities': ['SELECT'],
        },
        'special': [],
    },
}


def role_exists(username: str) -> bool:
    """Check if a PostgreSQL role exists."""
    try:
        with get_cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s",
                (username.lower(),)
            )
            return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"Failed to check role existence: {e}")
        return False


def create_role(username: str, password: str, user_class: str = '*USER') -> tuple[bool, str]:
    """
    Create a PostgreSQL role for a DK/400 user.
    Role name is lowercase version of username.
    """
    role_name = username.lower()

    if role_exists(username):
        return False, f"Role {role_name} already exists"

    try:
        conn = get_connection()
        conn.autocommit = True  # CREATE ROLE can't run in transaction
        cursor = conn.cursor()

        try:
            # Create the role with login capability
            cursor.execute(
                sql.SQL("CREATE ROLE {} WITH LOGIN PASSWORD %s").format(
                    sql.Identifier(role_name)
                ),
                (password,)
            )

            # Apply grants based on user class
            _apply_role_grants(cursor, role_name, user_class)

            logger.info(f"Created PostgreSQL role: {role_name}")
            return True, f"Role {role_name} created"

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"Failed to create role {role_name}: {e}")
        return False, f"Failed to create role: {e}"


def drop_role(username: str) -> tuple[bool, str]:
    """Drop a PostgreSQL role."""
    role_name = username.lower()

    # Prevent dropping system roles
    if role_name in ('dk400', 'postgres'):
        return False, f"Cannot drop system role {role_name}"

    if not role_exists(username):
        return True, f"Role {role_name} does not exist"

    try:
        conn = get_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        try:
            # Revoke all privileges first
            cursor.execute(
                sql.SQL("REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {}").format(
                    sql.Identifier(role_name)
                )
            )

            # Drop the role
            cursor.execute(
                sql.SQL("DROP ROLE IF EXISTS {}").format(
                    sql.Identifier(role_name)
                )
            )

            logger.info(f"Dropped PostgreSQL role: {role_name}")
            return True, f"Role {role_name} dropped"

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"Failed to drop role {role_name}: {e}")
        return False, f"Failed to drop role: {e}"


def update_role_password(username: str, password: str) -> tuple[bool, str]:
    """Update a PostgreSQL role's password."""
    role_name = username.lower()

    if not role_exists(username):
        return False, f"Role {role_name} does not exist"

    try:
        conn = get_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        try:
            cursor.execute(
                sql.SQL("ALTER ROLE {} WITH PASSWORD %s").format(
                    sql.Identifier(role_name)
                ),
                (password,)
            )

            logger.info(f"Updated password for role: {role_name}")
            return True, f"Password updated for {role_name}"

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"Failed to update password for {role_name}: {e}")
        return False, f"Failed to update password: {e}"


def update_role_class(username: str, user_class: str) -> tuple[bool, str]:
    """Update a PostgreSQL role's grants based on new user class."""
    role_name = username.lower()

    if not role_exists(username):
        return False, f"Role {role_name} does not exist"

    try:
        conn = get_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        try:
            # Revoke all existing grants
            cursor.execute(
                sql.SQL("REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {}").format(
                    sql.Identifier(role_name)
                )
            )

            # Apply new grants
            _apply_role_grants(cursor, role_name, user_class)

            logger.info(f"Updated grants for role {role_name} to {user_class}")
            return True, f"Grants updated for {role_name}"

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"Failed to update grants for {role_name}: {e}")
        return False, f"Failed to update grants: {e}"


def set_role_enabled(username: str, enabled: bool) -> tuple[bool, str]:
    """Enable or disable a PostgreSQL role's login capability."""
    role_name = username.lower()

    if not role_exists(username):
        return False, f"Role {role_name} does not exist"

    try:
        conn = get_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        try:
            login_opt = "LOGIN" if enabled else "NOLOGIN"
            cursor.execute(
                sql.SQL("ALTER ROLE {} WITH {}").format(
                    sql.Identifier(role_name),
                    sql.SQL(login_opt)
                )
            )

            status = "enabled" if enabled else "disabled"
            logger.info(f"Role {role_name} {status}")
            return True, f"Role {role_name} {status}"

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"Failed to set role enabled for {role_name}: {e}")
        return False, f"Failed to update role: {e}"


def _apply_role_grants(cursor, role_name: str, user_class: str):
    """Apply table grants to a role based on user class."""
    grants = USER_CLASS_GRANTS.get(user_class, USER_CLASS_GRANTS['*USER'])

    for table, privileges in grants.get('tables', {}).items():
        if privileges:
            # Grant specified privileges
            privs = ', '.join(privileges)
            cursor.execute(
                sql.SQL("GRANT {} ON {} TO {}").format(
                    sql.SQL(privs),
                    sql.Identifier(table),
                    sql.Identifier(role_name)
                )
            )


def sync_user_to_role(username: str, password: str, user_class: str,
                      status: str) -> tuple[bool, str]:
    """
    Ensure PostgreSQL role is in sync with DK/400 user.
    Creates role if missing, updates if exists.
    """
    role_name = username.lower()

    if not role_exists(username):
        # Create new role
        success, msg = create_role(username, password, user_class)
        if not success:
            return success, msg
    else:
        # Update existing role
        update_role_password(username, password)
        update_role_class(username, user_class)

    # Set enabled/disabled based on status
    enabled = (status == '*ENABLED')
    set_role_enabled(username, enabled)

    return True, f"Role {role_name} synced"


def init_role_management() -> bool:
    """
    Initialize role management by ensuring the dk400 user can create roles.
    This should be run once during database setup.
    """
    try:
        # Check if we already have CREATEROLE
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT rolcreaterole FROM pg_roles
                WHERE rolname = current_user
            """)
            row = cursor.fetchone()
            if row and row.get('rolcreaterole'):
                logger.info("Role management already initialized")
                return True

        logger.warning("dk400 user does not have CREATEROLE privilege")
        logger.warning("Run as postgres: ALTER ROLE dk400 WITH CREATEROLE;")
        return False

    except Exception as e:
        logger.error(f"Failed to check role management: {e}")
        return False


# =============================================================================
# Schema Management (AS/400-style Library concept)
# =============================================================================

def create_schema(schema_name: str, owner: str = None, description: str = '') -> tuple[bool, str]:
    """
    Create a PostgreSQL schema (AS/400 library equivalent).
    Optionally assign an owner who can manage objects in the schema.
    """
    schema_name = schema_name.lower().strip()

    if not schema_name:
        return False, "Schema name is required"

    if schema_name in ('public', 'pg_catalog', 'information_schema'):
        return False, f"Cannot create system schema {schema_name}"

    try:
        conn = get_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        try:
            # Check if schema exists
            cursor.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                (schema_name,)
            )
            if cursor.fetchone():
                return False, f"Schema {schema_name} already exists"

            # Create the schema
            cursor.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
            )

            # Set owner if specified
            if owner:
                owner_role = owner.lower()
                cursor.execute(
                    sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(owner_role)
                    )
                )

            # Store schema info in object_authorities for tracking
            cursor.execute("""
                INSERT INTO object_authorities (object_type, object_name, username, authority, granted_by)
                VALUES ('SCHEMA', %s, %s, '*OWNER', %s)
                ON CONFLICT (object_type, object_name, username) DO UPDATE SET authority = '*OWNER'
            """, (schema_name.upper(), owner.upper() if owner else 'DK400', 'DK400'))

            # Grant *SECOFR users (security officers) full access to new schemas
            cursor.execute("""
                SELECT username FROM users WHERE user_class = '*SECOFR'
            """)
            secofr_users = cursor.fetchall()
            for row in secofr_users:
                secofr_role = row['username'].lower()
                # Check if role exists before granting
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (secofr_role,))
                if cursor.fetchone():
                    cursor.execute(
                        sql.SQL("GRANT ALL ON SCHEMA {} TO {}").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(secofr_role)
                        )
                    )
                    cursor.execute(
                        sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT ALL ON TABLES TO {}").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(secofr_role)
                        )
                    )
                    cursor.execute(
                        sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT ALL ON SEQUENCES TO {}").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(secofr_role)
                        )
                    )

            logger.info(f"Created schema: {schema_name}")
            return True, f"Schema {schema_name.upper()} created"

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"Failed to create schema {schema_name}: {e}")
        return False, f"Failed to create schema: {e}"


def drop_schema(schema_name: str, cascade: bool = False) -> tuple[bool, str]:
    """Drop a PostgreSQL schema."""
    schema_name = schema_name.lower().strip()

    if schema_name in ('public', 'pg_catalog', 'information_schema'):
        return False, f"Cannot drop system schema {schema_name}"

    try:
        conn = get_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        try:
            # Check if schema exists
            cursor.execute(
                "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                (schema_name,)
            )
            if not cursor.fetchone():
                return False, f"Schema {schema_name} not found"

            # Drop the schema
            if cascade:
                cursor.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name))
                )
            else:
                cursor.execute(
                    sql.SQL("DROP SCHEMA {}").format(sql.Identifier(schema_name))
                )

            # Remove from object_authorities
            cursor.execute(
                "DELETE FROM object_authorities WHERE object_type = 'SCHEMA' AND object_name = %s",
                (schema_name.upper(),)
            )

            logger.info(f"Dropped schema: {schema_name}")
            return True, f"Schema {schema_name.upper()} dropped"

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"Failed to drop schema {schema_name}: {e}")
        return False, f"Failed to drop schema: {e}"


def list_schemas() -> list[dict]:
    """List all user-created schemas."""
    schemas = []
    try:
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT
                    s.schema_name,
                    s.schema_owner,
                    COALESCE(oa.authority, '') as authority,
                    (SELECT COUNT(*) FROM information_schema.tables t
                     WHERE t.table_schema = s.schema_name) as table_count
                FROM information_schema.schemata s
                LEFT JOIN object_authorities oa
                    ON oa.object_type = 'SCHEMA'
                    AND oa.object_name = UPPER(s.schema_name)
                WHERE s.schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                ORDER BY s.schema_name
            """)
            for row in cursor.fetchall():
                schemas.append({
                    'name': row['schema_name'].upper(),
                    'owner': row['schema_owner'].upper(),
                    'authority': row['authority'] or '',
                    'table_count': row['table_count'],
                })
    except Exception as e:
        logger.error(f"Failed to list schemas: {e}")
    return schemas


# =============================================================================
# Object Authority Management (AS/400-style GRTOBJAUT/RVKOBJAUT)
# =============================================================================

# Authority types and their PostgreSQL grant mappings
AUTHORITY_GRANTS = {
    '*USE': {
        'SCHEMA': ['USAGE'],
        'TABLE': ['SELECT'],
    },
    '*CHANGE': {
        'SCHEMA': ['USAGE'],
        'TABLE': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
    },
    '*ALL': {
        'SCHEMA': ['USAGE', 'CREATE'],
        'TABLE': ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER'],
    },
    '*OBJMGT': {
        # Object management - can create/alter/drop objects in schema
        'SCHEMA': ['USAGE', 'CREATE'],
        'TABLE': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
    },
    '*OWNER': {
        # Full ownership - transferred via ALTER OWNER
        'SCHEMA': ['ALL'],
        'TABLE': ['ALL'],
    },
    '*EXCLUDE': {
        # Revoke all access
        'SCHEMA': [],
        'TABLE': [],
    },
}


def grant_object_authority(
    object_type: str,
    object_name: str,
    username: str,
    authority: str,
    granted_by: str = 'DK400'
) -> tuple[bool, str]:
    """
    Grant authority on an object to a user (AS/400 GRTOBJAUT equivalent).

    object_type: 'SCHEMA' or 'TABLE'
    object_name: Schema name or 'schema.table' for tables
    username: User to grant authority to
    authority: *USE, *CHANGE, *ALL, *OBJMGT, *OWNER, *EXCLUDE
    """
    object_type = object_type.upper().strip()
    authority = authority.upper().strip()
    username = username.upper().strip()
    role_name = username.lower()

    if authority not in AUTHORITY_GRANTS:
        return False, f"Invalid authority {authority}. Valid: {', '.join(AUTHORITY_GRANTS.keys())}"

    if object_type not in ('SCHEMA', 'TABLE'):
        return False, f"Invalid object type {object_type}. Valid: SCHEMA, TABLE"

    # Check if role exists
    if not role_exists(username):
        return False, f"User {username} does not exist"

    grants = AUTHORITY_GRANTS[authority].get(object_type, [])

    try:
        conn = get_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        try:
            if object_type == 'SCHEMA':
                schema_name = object_name.lower().strip()

                # Verify schema exists
                cursor.execute(
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
                    (schema_name,)
                )
                if not cursor.fetchone():
                    return False, f"Schema {object_name} not found"

                if authority == '*OWNER':
                    # Transfer ownership
                    cursor.execute(
                        sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(role_name)
                        )
                    )
                    # Also grant on existing tables
                    cursor.execute("""
                        SELECT table_name FROM information_schema.tables
                        WHERE table_schema = %s
                    """, (schema_name,))
                    for row in cursor.fetchall():
                        cursor.execute(
                            sql.SQL("ALTER TABLE {}.{} OWNER TO {}").format(
                                sql.Identifier(schema_name),
                                sql.Identifier(row['table_name']),
                                sql.Identifier(role_name)
                            )
                        )
                elif authority == '*EXCLUDE':
                    # Revoke all
                    cursor.execute(
                        sql.SQL("REVOKE ALL ON SCHEMA {} FROM {}").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(role_name)
                        )
                    )
                    # Also revoke on all tables in schema
                    cursor.execute(
                        sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA {} FROM {}").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(role_name)
                        )
                    )
                else:
                    # Grant schema privileges
                    for grant in grants:
                        cursor.execute(
                            sql.SQL("GRANT {} ON SCHEMA {} TO {}").format(
                                sql.SQL(grant),
                                sql.Identifier(schema_name),
                                sql.Identifier(role_name)
                            )
                        )

                    # For *ALL and *OBJMGT, also grant on existing and future tables
                    if authority in ('*ALL', '*OBJMGT', '*CHANGE'):
                        table_grants = AUTHORITY_GRANTS[authority].get('TABLE', [])
                        if table_grants:
                            privs = ', '.join(table_grants)
                            cursor.execute(
                                sql.SQL("GRANT {} ON ALL TABLES IN SCHEMA {} TO {}").format(
                                    sql.SQL(privs),
                                    sql.Identifier(schema_name),
                                    sql.Identifier(role_name)
                                )
                            )
                            # Default privileges for future tables
                            cursor.execute(
                                sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT {} ON TABLES TO {}").format(
                                    sql.Identifier(schema_name),
                                    sql.SQL(privs),
                                    sql.Identifier(role_name)
                                )
                            )

            elif object_type == 'TABLE':
                # Parse schema.table format
                if '.' in object_name:
                    schema_name, table_name = object_name.lower().split('.', 1)
                else:
                    schema_name = 'public'
                    table_name = object_name.lower()

                # Verify table exists
                cursor.execute("""
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                """, (schema_name, table_name))
                if not cursor.fetchone():
                    return False, f"Table {object_name} not found"

                if authority == '*OWNER':
                    cursor.execute(
                        sql.SQL("ALTER TABLE {}.{} OWNER TO {}").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                            sql.Identifier(role_name)
                        )
                    )
                elif authority == '*EXCLUDE':
                    cursor.execute(
                        sql.SQL("REVOKE ALL ON {}.{} FROM {}").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                            sql.Identifier(role_name)
                        )
                    )
                else:
                    for grant in grants:
                        cursor.execute(
                            sql.SQL("GRANT {} ON {}.{} TO {}").format(
                                sql.SQL(grant),
                                sql.Identifier(schema_name),
                                sql.Identifier(table_name),
                                sql.Identifier(role_name)
                            )
                        )

            # Record in object_authorities table
            if authority == '*EXCLUDE':
                cursor.execute("""
                    DELETE FROM object_authorities
                    WHERE object_type = %s AND object_name = %s AND username = %s
                """, (object_type, object_name.upper(), username))
            else:
                cursor.execute("""
                    INSERT INTO object_authorities (object_type, object_name, username, authority, granted_by)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (object_type, object_name, username)
                    DO UPDATE SET authority = %s, granted_by = %s, granted_at = CURRENT_TIMESTAMP
                """, (object_type, object_name.upper(), username, authority, granted_by, authority, granted_by))

            logger.info(f"Granted {authority} on {object_type} {object_name} to {username}")
            return True, f"Authority {authority} granted to {username} on {object_name}"

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"Failed to grant authority: {e}")
        return False, f"Failed to grant authority: {e}"


def revoke_object_authority(
    object_type: str,
    object_name: str,
    username: str
) -> tuple[bool, str]:
    """
    Revoke all authority on an object from a user (AS/400 RVKOBJAUT equivalent).
    """
    return grant_object_authority(object_type, object_name, username, '*EXCLUDE')


def get_object_authorities(object_type: str = None, object_name: str = None, username: str = None) -> list[dict]:
    """
    Get object authorities, optionally filtered.
    """
    authorities = []
    try:
        with get_cursor() as cursor:
            query = "SELECT * FROM object_authorities WHERE 1=1"
            params = []

            if object_type:
                query += " AND object_type = %s"
                params.append(object_type.upper())
            if object_name:
                query += " AND object_name = %s"
                params.append(object_name.upper())
            if username:
                query += " AND username = %s"
                params.append(username.upper())

            query += " ORDER BY object_type, object_name, username"

            cursor.execute(query, params)
            for row in cursor.fetchall():
                authorities.append({
                    'object_type': row['object_type'],
                    'object_name': row['object_name'],
                    'username': row['username'],
                    'authority': row['authority'],
                    'granted_by': row['granted_by'],
                    'granted_at': str(row['granted_at']) if row['granted_at'] else '',
                })
    except Exception as e:
        logger.error(f"Failed to get object authorities: {e}")
    return authorities


def list_schema_tables(schema_name: str) -> list[dict]:
    """List all tables in a schema."""
    tables = []
    schema_name = schema_name.lower().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT
                    t.table_name,
                    t.table_type,
                    (SELECT COUNT(*) FROM information_schema.columns c
                     WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) as column_count
                FROM information_schema.tables t
                WHERE t.table_schema = %s
                ORDER BY t.table_name
            """, (schema_name,))
            for row in cursor.fetchall():
                tables.append({
                    'name': row['table_name'].upper(),
                    'type': row['table_type'],
                    'columns': row['column_count'],
                })
    except Exception as e:
        logger.error(f"Failed to list tables in {schema_name}: {e}")
    return tables


# =============================================================================
# Group Profile Management (AS/400-style GRPPRF)
# =============================================================================

def set_group_profile(username: str, group_profile: str) -> tuple[bool, str]:
    """
    Set a user's group profile. The user inherits all authorities from the group.
    Uses PostgreSQL role inheritance (GRANT role TO role).
    """
    username = username.upper().strip()
    group_profile = group_profile.upper().strip()
    role_name = username.lower()
    group_role = group_profile.lower()

    if group_profile == '*NONE':
        # Remove from any current group
        return remove_from_group(username)

    # Verify the group user exists
    try:
        with get_cursor() as cursor:
            cursor.execute("SELECT 1 FROM users WHERE username = %s", (group_profile,))
            if not cursor.fetchone():
                return False, f"Group profile {group_profile} not found"
    except Exception as e:
        return False, f"Failed to verify group profile: {e}"

    # Check if roles exist
    if not role_exists(username):
        return False, f"User {username} does not have a PostgreSQL role"

    if not role_exists(group_profile):
        return False, f"Group profile {group_profile} does not have a PostgreSQL role"

    try:
        conn = get_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        try:
            # First, revoke any existing group membership
            cursor.execute("""
                SELECT r.rolname
                FROM pg_roles r
                JOIN pg_auth_members m ON r.oid = m.roleid
                JOIN pg_roles member ON member.oid = m.member
                WHERE member.rolname = %s AND r.rolname != 'dk400'
            """, (role_name,))
            for row in cursor.fetchall():
                old_group = row['rolname']
                cursor.execute(
                    sql.SQL("REVOKE {} FROM {}").format(
                        sql.Identifier(old_group),
                        sql.Identifier(role_name)
                    )
                )

            # Grant new group membership (role inherits from group)
            cursor.execute(
                sql.SQL("GRANT {} TO {}").format(
                    sql.Identifier(group_role),
                    sql.Identifier(role_name)
                )
            )

            # Update users table
            cursor.execute(
                "UPDATE users SET group_profile = %s WHERE username = %s",
                (group_profile, username)
            )

            logger.info(f"Set group profile for {username} to {group_profile}")
            return True, f"User {username} now inherits from {group_profile}"

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"Failed to set group profile: {e}")
        return False, f"Failed to set group profile: {e}"


def remove_from_group(username: str) -> tuple[bool, str]:
    """Remove a user from their current group profile."""
    username = username.upper().strip()
    role_name = username.lower()

    try:
        conn = get_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        try:
            # Find and revoke current group memberships
            cursor.execute("""
                SELECT r.rolname
                FROM pg_roles r
                JOIN pg_auth_members m ON r.oid = m.roleid
                JOIN pg_roles member ON member.oid = m.member
                WHERE member.rolname = %s AND r.rolname != 'dk400'
            """, (role_name,))

            for row in cursor.fetchall():
                old_group = row['rolname']
                cursor.execute(
                    sql.SQL("REVOKE {} FROM {}").format(
                        sql.Identifier(old_group),
                        sql.Identifier(role_name)
                    )
                )

            # Update users table
            cursor.execute(
                "UPDATE users SET group_profile = '*NONE' WHERE username = %s",
                (username,)
            )

            return True, f"User {username} removed from group"

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"Failed to remove from group: {e}")
        return False, f"Failed to remove from group: {e}"


def copy_authorities_from(source_user: str, target_user: str) -> tuple[bool, str]:
    """
    Copy all object authorities from one user to another.
    This copies the entries from object_authorities table and applies the grants.
    """
    source_user = source_user.upper().strip()
    target_user = target_user.upper().strip()

    if not role_exists(source_user):
        return False, f"Source user {source_user} does not have a PostgreSQL role"

    if not role_exists(target_user):
        return False, f"Target user {target_user} does not have a PostgreSQL role"

    try:
        # Get all authorities for source user
        authorities = get_object_authorities(username=source_user)

        if not authorities:
            return True, f"No authorities to copy from {source_user}"

        copied = 0
        for auth in authorities:
            success, msg = grant_object_authority(
                object_type=auth['object_type'],
                object_name=auth['object_name'],
                username=target_user,
                authority=auth['authority'],
                granted_by=f"COPY:{source_user}"
            )
            if success:
                copied += 1

        return True, f"Copied {copied} authorities from {source_user} to {target_user}"

    except Exception as e:
        logger.error(f"Failed to copy authorities: {e}")
        return False, f"Failed to copy authorities: {e}"


def get_user_group(username: str) -> str:
    """Get a user's group profile."""
    username = username.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "SELECT group_profile FROM users WHERE username = %s",
                (username,)
            )
            row = cursor.fetchone()
            if row:
                return row['group_profile'] or '*NONE'
    except Exception as e:
        logger.error(f"Failed to get group profile: {e}")

    return '*NONE'


def get_group_members(group_profile: str) -> list[str]:
    """Get all users that belong to a group profile."""
    group_profile = group_profile.upper().strip()
    members = []

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "SELECT username FROM users WHERE group_profile = %s ORDER BY username",
                (group_profile,)
            )
            for row in cursor.fetchall():
                members.append(row['username'])
    except Exception as e:
        logger.error(f"Failed to get group members: {e}")

    return members


def get_effective_authorities(username: str) -> list[dict]:
    """
    Get all effective authorities for a user, including inherited from group profile.
    """
    username = username.upper().strip()

    # Get direct authorities
    direct = get_object_authorities(username=username)

    # Get group profile
    group = get_user_group(username)

    if group and group != '*NONE':
        # Get inherited authorities from group
        inherited = get_object_authorities(username=group)
        for auth in inherited:
            auth['inherited_from'] = group
            # Check if not already in direct (direct overrides inherited)
            exists = any(
                d['object_type'] == auth['object_type'] and
                d['object_name'] == auth['object_name']
                for d in direct
            )
            if not exists:
                direct.append(auth)

    return direct


# =============================================================================
# System Values (AS/400-style WRKSYSVAL)
# =============================================================================

# Cache for system values to avoid repeated DB queries
_sysval_cache: dict[str, str] = {}


def get_system_value(name: str, default: str = '') -> str:
    """
    Get a system value by name.
    Values are cached for performance.
    """
    name = name.upper().strip()

    # Check cache first
    if name in _sysval_cache:
        return _sysval_cache[name]

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "SELECT value FROM system_values WHERE name = %s",
                (name,)
            )
            row = cursor.fetchone()
            if row:
                _sysval_cache[name] = row['value']
                return row['value']
    except Exception as e:
        logger.error(f"Failed to get system value {name}: {e}")

    return default


def set_system_value(name: str, value: str, updated_by: str = 'SYSTEM') -> tuple[bool, str]:
    """
    Set a system value.
    Clears the cache for this value.
    """
    name = name.upper().strip()
    updated_by = updated_by.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                UPDATE system_values
                SET value = %s, updated_at = CURRENT_TIMESTAMP, updated_by = %s
                WHERE name = %s
            """, (value, updated_by, name))

            if cursor.rowcount == 0:
                return False, f"System value {name} not found"

        # Clear cache
        if name in _sysval_cache:
            del _sysval_cache[name]

        logger.info(f"System value {name} changed to {value} by {updated_by}")
        return True, f"System value {name} changed"

    except Exception as e:
        logger.error(f"Failed to set system value {name}: {e}")
        return False, f"Failed to set system value: {e}"


def list_system_values(category: str = None) -> list[dict]:
    """
    List all system values, optionally filtered by category.
    """
    values = []
    try:
        with get_cursor() as cursor:
            if category:
                cursor.execute("""
                    SELECT name, value, description, category, updated_at, updated_by
                    FROM system_values
                    WHERE category = %s
                    ORDER BY name
                """, (category.upper(),))
            else:
                cursor.execute("""
                    SELECT name, value, description, category, updated_at, updated_by
                    FROM system_values
                    ORDER BY category, name
                """)

            for row in cursor.fetchall():
                values.append({
                    'name': row['name'],
                    'value': row['value'],
                    'description': row['description'],
                    'category': row['category'],
                    'updated_at': str(row['updated_at']) if row['updated_at'] else '',
                    'updated_by': row['updated_by'],
                })
    except Exception as e:
        logger.error(f"Failed to list system values: {e}")

    return values


def clear_sysval_cache():
    """Clear the system value cache."""
    global _sysval_cache
    _sysval_cache = {}


# =============================================================================
# Message Queues (AS/400-style MSGQ)
# =============================================================================

def create_message_queue(name: str, description: str = '', queue_type: str = '*STD',
                         created_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Create a message queue."""
    name = name.upper().strip()[:10]

    if not name:
        return False, "Queue name is required"

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO message_queues (name, description, queue_type, created_by)
                VALUES (%s, %s, %s, %s)
            """, (name, description, queue_type, created_by))
        return True, f"Message queue {name} created"
    except psycopg2.IntegrityError:
        return False, f"Message queue {name} already exists"
    except Exception as e:
        logger.error(f"Failed to create message queue: {e}")
        return False, f"Failed to create message queue: {e}"


def delete_message_queue(name: str) -> tuple[bool, str]:
    """Delete a message queue."""
    name = name.upper().strip()

    if name in ('QSYSOPR', 'QSYSMSG'):
        return False, f"Cannot delete system queue {name}"

    try:
        with get_cursor() as cursor:
            cursor.execute("DELETE FROM message_queues WHERE name = %s", (name,))
            if cursor.rowcount == 0:
                return False, f"Message queue {name} not found"
        return True, f"Message queue {name} deleted"
    except Exception as e:
        logger.error(f"Failed to delete message queue: {e}")
        return False, f"Failed to delete message queue: {e}"


def list_message_queues() -> list[dict]:
    """List all message queues."""
    queues = []
    try:
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT mq.*,
                    (SELECT COUNT(*) FROM messages m WHERE m.queue_name = mq.name AND m.status = '*NEW') as new_count,
                    (SELECT COUNT(*) FROM messages m WHERE m.queue_name = mq.name) as total_count
                FROM message_queues mq
                ORDER BY mq.name
            """)
            for row in cursor.fetchall():
                queues.append(dict(row))
    except Exception as e:
        logger.error(f"Failed to list message queues: {e}")
    return queues


def send_message(queue_name: str, msg_text: str, msg_type: str = '*INFO',
                 msg_id: str = '', severity: int = 0, sent_by: str = 'SYSTEM',
                 msg_data: str = None) -> tuple[bool, str]:
    """Send a message to a queue (SNDMSG)."""
    queue_name = queue_name.upper().strip()

    try:
        with get_cursor() as cursor:
            # Verify queue exists
            cursor.execute("SELECT 1 FROM message_queues WHERE name = %s", (queue_name,))
            if not cursor.fetchone():
                return False, f"Message queue {queue_name} not found"

            cursor.execute("""
                INSERT INTO messages (queue_name, msg_id, msg_type, msg_text, msg_data, severity, sent_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (queue_name, msg_id, msg_type, msg_text, msg_data, severity, sent_by))
        return True, "Message sent"
    except Exception as e:
        logger.error(f"Failed to send message: {e}")
        return False, f"Failed to send message: {e}"


def get_messages(queue_name: str, status: str = None, limit: int = 50) -> list[dict]:
    """Get messages from a queue (DSPMSG)."""
    queue_name = queue_name.upper().strip()
    messages = []

    try:
        with get_cursor() as cursor:
            if status:
                cursor.execute("""
                    SELECT * FROM messages
                    WHERE queue_name = %s AND status = %s
                    ORDER BY sent_at DESC LIMIT %s
                """, (queue_name, status, limit))
            else:
                cursor.execute("""
                    SELECT * FROM messages
                    WHERE queue_name = %s
                    ORDER BY sent_at DESC LIMIT %s
                """, (queue_name, limit))
            for row in cursor.fetchall():
                messages.append(dict(row))
    except Exception as e:
        logger.error(f"Failed to get messages: {e}")
    return messages


def mark_message_old(message_id: int) -> tuple[bool, str]:
    """Mark a message as old/read."""
    try:
        with get_cursor() as cursor:
            cursor.execute(
                "UPDATE messages SET status = '*OLD' WHERE id = %s AND status = '*NEW'",
                (message_id,)
            )
        return True, "Message marked as read"
    except Exception as e:
        return False, f"Failed to update message: {e}"


def reply_to_message(message_id: int, reply: str, replied_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Reply to an inquiry message."""
    try:
        with get_cursor() as cursor:
            cursor.execute("""
                UPDATE messages
                SET reply = %s, replied_at = CURRENT_TIMESTAMP, status = '*ANSWERED'
                WHERE id = %s AND msg_type = '*INQ'
            """, (reply, message_id))
            if cursor.rowcount == 0:
                return False, "Message not found or not an inquiry"
        return True, "Reply sent"
    except Exception as e:
        return False, f"Failed to reply: {e}"


def delete_message(message_id: int) -> tuple[bool, str]:
    """Delete a message."""
    try:
        with get_cursor() as cursor:
            cursor.execute("DELETE FROM messages WHERE id = %s", (message_id,))
        return True, "Message deleted"
    except Exception as e:
        return False, f"Failed to delete message: {e}"


def clear_message_queue(queue_name: str) -> tuple[bool, str]:
    """Clear all messages from a queue."""
    queue_name = queue_name.upper().strip()
    try:
        with get_cursor() as cursor:
            cursor.execute("DELETE FROM messages WHERE queue_name = %s", (queue_name,))
            count = cursor.rowcount
        return True, f"Cleared {count} messages from {queue_name}"
    except Exception as e:
        return False, f"Failed to clear queue: {e}"


# =============================================================================
# Data Areas (AS/400-style DTAARA)
# =============================================================================

def create_data_area(name: str, library: str = '*LIBL', type: str = '*CHAR',
                     length: int = 2000, decimal_positions: int = 0,
                     value: str = '', description: str = '',
                     created_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Create a data area (CRTDTAARA)."""
    name = name.upper().strip()[:10]
    library = library.upper().strip()[:10] or '*LIBL'

    if not name:
        return False, "Data area name is required"

    if type not in ('*CHAR', '*DEC', '*LGL'):
        return False, "Type must be *CHAR, *DEC, or *LGL"

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO data_areas (name, library, type, length, decimal_positions,
                                        value, description, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (name, library, type, length, decimal_positions, value, description, created_by))
        return True, f"Data area {library}/{name} created"
    except psycopg2.IntegrityError:
        return False, f"Data area {library}/{name} already exists"
    except Exception as e:
        logger.error(f"Failed to create data area: {e}")
        return False, f"Failed to create data area: {e}"


def delete_data_area(name: str, library: str = '*LIBL') -> tuple[bool, str]:
    """Delete a data area (DLTDTAARA)."""
    name = name.upper().strip()
    library = library.upper().strip() or '*LIBL'

    if name in ('QDATE', 'QTIME'):
        return False, f"Cannot delete system data area {name}"

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "DELETE FROM data_areas WHERE name = %s AND library = %s",
                (name, library)
            )
            if cursor.rowcount == 0:
                return False, f"Data area {library}/{name} not found"
        return True, f"Data area {library}/{name} deleted"
    except Exception as e:
        return False, f"Failed to delete data area: {e}"


def get_data_area(name: str, library: str = '*LIBL') -> dict | None:
    """Get a data area (RTVDTAARA)."""
    name = name.upper().strip()
    library = library.upper().strip() or '*LIBL'

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "SELECT * FROM data_areas WHERE name = %s AND library = %s",
                (name, library)
            )
            row = cursor.fetchone()
            if row:
                return dict(row)
    except Exception as e:
        logger.error(f"Failed to get data area: {e}")
    return None


def change_data_area(name: str, library: str = '*LIBL', value: str = None,
                     updated_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Change a data area value (CHGDTAARA)."""
    name = name.upper().strip()
    library = library.upper().strip() or '*LIBL'

    try:
        with get_cursor() as cursor:
            # Check if locked by another user
            cursor.execute(
                "SELECT locked_by FROM data_areas WHERE name = %s AND library = %s",
                (name, library)
            )
            row = cursor.fetchone()
            if not row:
                return False, f"Data area {library}/{name} not found"

            if row['locked_by'] and row['locked_by'] != updated_by:
                return False, f"Data area locked by {row['locked_by']}"

            cursor.execute("""
                UPDATE data_areas
                SET value = %s, updated_by = %s, updated_at = CURRENT_TIMESTAMP
                WHERE name = %s AND library = %s
            """, (value, updated_by, name, library))
        return True, f"Data area {library}/{name} changed"
    except Exception as e:
        return False, f"Failed to change data area: {e}"


def lock_data_area(name: str, library: str = '*LIBL', locked_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Lock a data area for exclusive use."""
    name = name.upper().strip()
    library = library.upper().strip() or '*LIBL'

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "SELECT locked_by FROM data_areas WHERE name = %s AND library = %s",
                (name, library)
            )
            row = cursor.fetchone()
            if not row:
                return False, f"Data area {library}/{name} not found"

            if row['locked_by'] and row['locked_by'] != locked_by:
                return False, f"Data area already locked by {row['locked_by']}"

            cursor.execute("""
                UPDATE data_areas
                SET locked_by = %s, locked_at = CURRENT_TIMESTAMP
                WHERE name = %s AND library = %s
            """, (locked_by, name, library))
        return True, f"Data area {library}/{name} locked"
    except Exception as e:
        return False, f"Failed to lock data area: {e}"


def unlock_data_area(name: str, library: str = '*LIBL', unlocked_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Unlock a data area."""
    name = name.upper().strip()
    library = library.upper().strip() or '*LIBL'

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "SELECT locked_by FROM data_areas WHERE name = %s AND library = %s",
                (name, library)
            )
            row = cursor.fetchone()
            if not row:
                return False, f"Data area {library}/{name} not found"

            # Only the locker or QSECOFR can unlock
            if row['locked_by'] and row['locked_by'] != unlocked_by:
                # Check if unlocked_by is SECOFR
                cursor.execute(
                    "SELECT user_class FROM users WHERE username = %s",
                    (unlocked_by,)
                )
                user_row = cursor.fetchone()
                if not user_row or user_row['user_class'] != '*SECOFR':
                    return False, f"Data area locked by {row['locked_by']}"

            cursor.execute("""
                UPDATE data_areas SET locked_by = NULL, locked_at = NULL
                WHERE name = %s AND library = %s
            """, (name, library))
        return True, f"Data area {library}/{name} unlocked"
    except Exception as e:
        return False, f"Failed to unlock data area: {e}"


def list_data_areas(library: str = None) -> list[dict]:
    """List data areas (WRKDTAARA)."""
    areas = []
    try:
        with get_cursor() as cursor:
            if library:
                cursor.execute(
                    "SELECT * FROM data_areas WHERE library = %s ORDER BY name",
                    (library.upper(),)
                )
            else:
                cursor.execute("SELECT * FROM data_areas ORDER BY library, name")
            for row in cursor.fetchall():
                areas.append(dict(row))
    except Exception as e:
        logger.error(f"Failed to list data areas: {e}")
    return areas


# =============================================================================
# Job Descriptions (AS/400-style JOBD)
# =============================================================================

def create_job_description(name: str, library: str = '*LIBL', description: str = '',
                           job_queue: str = 'QBATCH', job_priority: int = 5,
                           output_queue: str = '*USRPRF', user_profile: str = '*RQD',
                           hold_on_jobq: str = '*NO', created_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Create a job description (CRTJOBD)."""
    name = name.upper().strip()[:10]
    library = library.upper().strip()[:10] or '*LIBL'

    if not name:
        return False, "Job description name is required"

    if job_priority < 1 or job_priority > 9:
        return False, "Job priority must be 1-9"

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO job_descriptions (name, library, description, job_queue,
                                              job_priority, output_queue, user_profile,
                                              hold_on_jobq, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (name, library, description, job_queue, job_priority,
                  output_queue, user_profile, hold_on_jobq, created_by))
        return True, f"Job description {library}/{name} created"
    except psycopg2.IntegrityError:
        return False, f"Job description {library}/{name} already exists"
    except Exception as e:
        return False, f"Failed to create job description: {e}"


def delete_job_description(name: str, library: str = '*LIBL') -> tuple[bool, str]:
    """Delete a job description (DLTJOBD)."""
    name = name.upper().strip()
    library = library.upper().strip() or '*LIBL'

    if name in ('QBATCH', 'QINTER', 'QSPL'):
        return False, f"Cannot delete system job description {name}"

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "DELETE FROM job_descriptions WHERE name = %s AND library = %s",
                (name, library)
            )
            if cursor.rowcount == 0:
                return False, f"Job description {library}/{name} not found"
        return True, f"Job description {library}/{name} deleted"
    except Exception as e:
        return False, f"Failed to delete job description: {e}"


def get_job_description(name: str, library: str = '*LIBL') -> dict | None:
    """Get a job description."""
    name = name.upper().strip()
    library = library.upper().strip() or '*LIBL'

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "SELECT * FROM job_descriptions WHERE name = %s AND library = %s",
                (name, library)
            )
            row = cursor.fetchone()
            if row:
                return dict(row)
    except Exception as e:
        logger.error(f"Failed to get job description: {e}")
    return None


def list_job_descriptions(library: str = None) -> list[dict]:
    """List job descriptions (WRKJOBD)."""
    jobds = []
    try:
        with get_cursor() as cursor:
            if library:
                cursor.execute(
                    "SELECT * FROM job_descriptions WHERE library = %s ORDER BY name",
                    (library.upper(),)
                )
            else:
                cursor.execute("SELECT * FROM job_descriptions ORDER BY library, name")
            for row in cursor.fetchall():
                jobds.append(dict(row))
    except Exception as e:
        logger.error(f"Failed to list job descriptions: {e}")
    return jobds


def change_job_description(name: str, library: str = '*LIBL', **kwargs) -> tuple[bool, str]:
    """Change a job description (CHGJOBD)."""
    name = name.upper().strip()
    library = library.upper().strip() or '*LIBL'

    allowed_fields = ['description', 'job_queue', 'job_priority', 'output_queue',
                      'user_profile', 'hold_on_jobq', 'log_level', 'log_severity']

    updates = {k: v for k, v in kwargs.items() if k in allowed_fields and v is not None}

    if not updates:
        return False, "No changes specified"

    try:
        with get_cursor() as cursor:
            set_clause = ', '.join(f"{k} = %s" for k in updates.keys())
            values = list(updates.values()) + [name, library]
            cursor.execute(f"""
                UPDATE job_descriptions SET {set_clause}
                WHERE name = %s AND library = %s
            """, values)
            if cursor.rowcount == 0:
                return False, f"Job description {library}/{name} not found"
        return True, f"Job description {library}/{name} changed"
    except Exception as e:
        return False, f"Failed to change job description: {e}"


# =============================================================================
# Output Queues and Spooled Files (AS/400-style OUTQ/SPLF)
# =============================================================================

def create_output_queue(name: str, library: str = '*LIBL', description: str = '',
                        created_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Create an output queue (CRTOUTQ)."""
    name = name.upper().strip()[:10]
    library = library.upper().strip()[:10] or '*LIBL'

    if not name:
        return False, "Output queue name is required"

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO output_queues (name, library, description, created_by)
                VALUES (%s, %s, %s, %s)
            """, (name, library, description, created_by))
        return True, f"Output queue {library}/{name} created"
    except psycopg2.IntegrityError:
        return False, f"Output queue {library}/{name} already exists"
    except Exception as e:
        return False, f"Failed to create output queue: {e}"


def delete_output_queue(name: str, library: str = '*LIBL') -> tuple[bool, str]:
    """Delete an output queue (DLTOUTQ)."""
    name = name.upper().strip()
    library = library.upper().strip() or '*LIBL'

    if name in ('QPRINT', 'QPRINT2'):
        return False, f"Cannot delete system output queue {name}"

    try:
        with get_cursor() as cursor:
            # Check for spooled files
            cursor.execute(
                "SELECT COUNT(*) as cnt FROM spooled_files WHERE output_queue = %s",
                (name,)
            )
            if cursor.fetchone()['cnt'] > 0:
                return False, f"Output queue {name} contains spooled files"

            cursor.execute(
                "DELETE FROM output_queues WHERE name = %s AND library = %s",
                (name, library)
            )
            if cursor.rowcount == 0:
                return False, f"Output queue {library}/{name} not found"
        return True, f"Output queue {library}/{name} deleted"
    except Exception as e:
        return False, f"Failed to delete output queue: {e}"


def list_output_queues(library: str = None) -> list[dict]:
    """List output queues (WRKOUTQ)."""
    queues = []
    try:
        with get_cursor() as cursor:
            if library:
                cursor.execute("""
                    SELECT oq.*,
                        (SELECT COUNT(*) FROM spooled_files sf WHERE sf.output_queue = oq.name) as file_count
                    FROM output_queues oq
                    WHERE oq.library = %s
                    ORDER BY oq.name
                """, (library.upper(),))
            else:
                cursor.execute("""
                    SELECT oq.*,
                        (SELECT COUNT(*) FROM spooled_files sf WHERE sf.output_queue = oq.name) as file_count
                    FROM output_queues oq
                    ORDER BY oq.library, oq.name
                """)
            for row in cursor.fetchall():
                queues.append(dict(row))
    except Exception as e:
        logger.error(f"Failed to list output queues: {e}")
    return queues


def hold_output_queue(name: str, library: str = '*LIBL') -> tuple[bool, str]:
    """Hold an output queue (HLDOUTQ)."""
    name = name.upper().strip()
    library = library.upper().strip() or '*LIBL'

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                UPDATE output_queues SET status = '*HLD'
                WHERE name = %s AND library = %s
            """, (name, library))
            if cursor.rowcount == 0:
                return False, f"Output queue {library}/{name} not found"
        return True, f"Output queue {library}/{name} held"
    except Exception as e:
        return False, f"Failed to hold output queue: {e}"


def release_output_queue(name: str, library: str = '*LIBL') -> tuple[bool, str]:
    """Release an output queue (RLSOUTQ)."""
    name = name.upper().strip()
    library = library.upper().strip() or '*LIBL'

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                UPDATE output_queues SET status = '*RLS'
                WHERE name = %s AND library = %s
            """, (name, library))
            if cursor.rowcount == 0:
                return False, f"Output queue {library}/{name} not found"
        return True, f"Output queue {library}/{name} released"
    except Exception as e:
        return False, f"Failed to release output queue: {e}"


def create_spooled_file(name: str, job_name: str, content: str, job_id: str = None,
                        output_queue: str = 'QPRINT', user_data: str = '',
                        created_by: str = 'SYSTEM') -> tuple[bool, str, int]:
    """Create a spooled file (job output)."""
    name = name.upper().strip()[:10]

    try:
        with get_cursor() as cursor:
            # Get next file number for this job
            cursor.execute(
                "SELECT COALESCE(MAX(file_number), 0) + 1 as next_num FROM spooled_files WHERE job_name = %s",
                (job_name,)
            )
            file_number = cursor.fetchone()['next_num']

            # Count pages (lines / 60)
            pages = max(1, len(content.split('\n')) // 60)
            total_records = len(content.split('\n'))

            cursor.execute("""
                INSERT INTO spooled_files (name, file_number, job_name, job_id, output_queue,
                                           user_data, pages, total_records, content, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (name, file_number, job_name, job_id, output_queue, user_data,
                  pages, total_records, content, created_by))
            splf_id = cursor.fetchone()['id']
        return True, f"Spooled file {name} created", splf_id
    except Exception as e:
        return False, f"Failed to create spooled file: {e}", 0


def get_spooled_file(splf_id: int) -> dict | None:
    """Get a spooled file by ID."""
    try:
        with get_cursor() as cursor:
            cursor.execute("SELECT * FROM spooled_files WHERE id = %s", (splf_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
    except Exception as e:
        logger.error(f"Failed to get spooled file: {e}")
    return None


def list_spooled_files(user: str = None, output_queue: str = None, job_name: str = None) -> list[dict]:
    """List spooled files (WRKSPLF)."""
    files = []
    try:
        with get_cursor() as cursor:
            query = "SELECT * FROM spooled_files WHERE 1=1"
            params = []

            if user:
                query += " AND created_by = %s"
                params.append(user.upper())
            if output_queue:
                query += " AND output_queue = %s"
                params.append(output_queue.upper())
            if job_name:
                query += " AND job_name = %s"
                params.append(job_name)

            query += " ORDER BY created_at DESC"

            cursor.execute(query, params)
            for row in cursor.fetchall():
                result = dict(row)
                # Don't include full content in list
                result.pop('content', None)
                files.append(result)
    except Exception as e:
        logger.error(f"Failed to list spooled files: {e}")
    return files


def delete_spooled_file(splf_id: int) -> tuple[bool, str]:
    """Delete a spooled file (DLTSPLF)."""
    try:
        with get_cursor() as cursor:
            cursor.execute("DELETE FROM spooled_files WHERE id = %s", (splf_id,))
            if cursor.rowcount == 0:
                return False, "Spooled file not found"
        return True, "Spooled file deleted"
    except Exception as e:
        return False, f"Failed to delete spooled file: {e}"


def hold_spooled_file(splf_id: int) -> tuple[bool, str]:
    """Hold a spooled file (HLDSPLF)."""
    try:
        with get_cursor() as cursor:
            cursor.execute(
                "UPDATE spooled_files SET status = '*HLD' WHERE id = %s",
                (splf_id,)
            )
            if cursor.rowcount == 0:
                return False, "Spooled file not found"
        return True, "Spooled file held"
    except Exception as e:
        return False, f"Failed to hold spooled file: {e}"


def release_spooled_file(splf_id: int) -> tuple[bool, str]:
    """Release a spooled file (RLSSPLF)."""
    try:
        with get_cursor() as cursor:
            cursor.execute(
                "UPDATE spooled_files SET status = '*RDY' WHERE id = %s",
                (splf_id,)
            )
            if cursor.rowcount == 0:
                return False, "Spooled file not found"
        return True, "Spooled file released"
    except Exception as e:
        return False, f"Failed to release spooled file: {e}"


# =============================================================================
# Job Schedule Entries (AS/400-style WRKJOBSCDE)
# =============================================================================

def add_job_schedule_entry(name: str, command: str, description: str = '',
                           frequency: str = '*DAILY', schedule_time: str = '00:00',
                           days_of_week: str = None, day_of_month: int = None,
                           schedule_date: str = None, job_description: str = 'QBATCH',
                           created_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Add a job schedule entry (ADDJOBSCDE)."""
    name = name.upper().strip()[:20]

    if not name:
        return False, "Schedule entry name is required"

    if not command:
        return False, "Command is required"

    if frequency not in ('*ONCE', '*DAILY', '*WEEKLY', '*MONTHLY'):
        return False, "Frequency must be *ONCE, *DAILY, *WEEKLY, or *MONTHLY"

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO job_schedule_entries (name, description, command, job_description,
                                                  frequency, schedule_time, days_of_week,
                                                  day_of_month, schedule_date, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (name, description, command, job_description, frequency,
                  schedule_time, days_of_week, day_of_month, schedule_date, created_by))
        return True, f"Job schedule entry {name} added"
    except psycopg2.IntegrityError:
        return False, f"Job schedule entry {name} already exists"
    except Exception as e:
        return False, f"Failed to add job schedule entry: {e}"


def remove_job_schedule_entry(name: str) -> tuple[bool, str]:
    """Remove a job schedule entry (RMVJOBSCDE)."""
    name = name.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute("DELETE FROM job_schedule_entries WHERE name = %s", (name,))
            if cursor.rowcount == 0:
                return False, f"Job schedule entry {name} not found"
        return True, f"Job schedule entry {name} removed"
    except Exception as e:
        return False, f"Failed to remove job schedule entry: {e}"


def get_job_schedule_entry(name: str) -> dict | None:
    """Get a job schedule entry."""
    name = name.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute("SELECT * FROM job_schedule_entries WHERE name = %s", (name,))
            row = cursor.fetchone()
            if row:
                return dict(row)
    except Exception as e:
        logger.error(f"Failed to get job schedule entry: {e}")
    return None


def list_job_schedule_entries(status: str = None) -> list[dict]:
    """List job schedule entries (WRKJOBSCDE)."""
    entries = []
    try:
        with get_cursor() as cursor:
            if status:
                cursor.execute(
                    "SELECT * FROM job_schedule_entries WHERE status = %s ORDER BY name",
                    (status.upper(),)
                )
            else:
                cursor.execute("SELECT * FROM job_schedule_entries ORDER BY name")
            for row in cursor.fetchall():
                entries.append(dict(row))
    except Exception as e:
        logger.error(f"Failed to list job schedule entries: {e}")
    return entries


def hold_job_schedule_entry(name: str) -> tuple[bool, str]:
    """Hold a job schedule entry (HLDJOBSCDE)."""
    name = name.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "UPDATE job_schedule_entries SET status = '*HELD' WHERE name = %s",
                (name,)
            )
            if cursor.rowcount == 0:
                return False, f"Job schedule entry {name} not found"
        return True, f"Job schedule entry {name} held"
    except Exception as e:
        return False, f"Failed to hold job schedule entry: {e}"


def release_job_schedule_entry(name: str) -> tuple[bool, str]:
    """Release a job schedule entry (RLSJOBSCDE)."""
    name = name.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "UPDATE job_schedule_entries SET status = '*ACTIVE' WHERE name = %s",
                (name,)
            )
            if cursor.rowcount == 0:
                return False, f"Job schedule entry {name} not found"
        return True, f"Job schedule entry {name} released"
    except Exception as e:
        return False, f"Failed to release job schedule entry: {e}"


def change_job_schedule_entry(name: str, **kwargs) -> tuple[bool, str]:
    """Change a job schedule entry (CHGJOBSCDE)."""
    name = name.upper().strip()

    allowed_fields = ['description', 'command', 'frequency', 'schedule_time',
                      'days_of_week', 'day_of_month', 'schedule_date', 'job_description']

    updates = {k: v for k, v in kwargs.items() if k in allowed_fields and v is not None}

    if not updates:
        return False, "No changes specified"

    updates['updated_at'] = 'CURRENT_TIMESTAMP'
    updates['updated_by'] = kwargs.get('updated_by', 'SYSTEM')

    try:
        with get_cursor() as cursor:
            set_parts = []
            values = []
            for k, v in updates.items():
                if v == 'CURRENT_TIMESTAMP':
                    set_parts.append(f"{k} = CURRENT_TIMESTAMP")
                else:
                    set_parts.append(f"{k} = %s")
                    values.append(v)

            values.append(name)
            cursor.execute(f"""
                UPDATE job_schedule_entries SET {', '.join(set_parts)}
                WHERE name = %s
            """, values)
            if cursor.rowcount == 0:
                return False, f"Job schedule entry {name} not found"
        return True, f"Job schedule entry {name} changed"
    except Exception as e:
        return False, f"Failed to change job schedule entry: {e}"


# =============================================================================
# Authorization Lists (AS/400-style AUTL)
# =============================================================================

def create_authorization_list(name: str, description: str = '',
                              created_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Create an authorization list (CRTAUTL)."""
    name = name.upper().strip()[:10]

    if not name:
        return False, "Authorization list name is required"

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO authorization_lists (name, description, created_by)
                VALUES (%s, %s, %s)
            """, (name, description, created_by))
        return True, f"Authorization list {name} created"
    except psycopg2.IntegrityError:
        return False, f"Authorization list {name} already exists"
    except Exception as e:
        return False, f"Failed to create authorization list: {e}"


def delete_authorization_list(name: str) -> tuple[bool, str]:
    """Delete an authorization list (DLTAUTL)."""
    name = name.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute("DELETE FROM authorization_lists WHERE name = %s", (name,))
            if cursor.rowcount == 0:
                return False, f"Authorization list {name} not found"
        return True, f"Authorization list {name} deleted"
    except Exception as e:
        return False, f"Failed to delete authorization list: {e}"


def list_authorization_lists() -> list[dict]:
    """List authorization lists (WRKAUTL)."""
    lists = []
    try:
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT al.*,
                    (SELECT COUNT(*) FROM authorization_list_entries e WHERE e.autl_name = al.name) as user_count,
                    (SELECT COUNT(*) FROM authorization_list_objects o WHERE o.autl_name = al.name) as object_count
                FROM authorization_lists al
                ORDER BY al.name
            """)
            for row in cursor.fetchall():
                lists.append(dict(row))
    except Exception as e:
        logger.error(f"Failed to list authorization lists: {e}")
    return lists


def add_authorization_list_entry(autl_name: str, username: str, authority: str = '*USE',
                                  added_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Add a user to an authorization list (ADDAUTLE)."""
    autl_name = autl_name.upper().strip()
    username = username.upper().strip()
    authority = authority.upper().strip()

    if authority not in ('*USE', '*CHANGE', '*ALL', '*EXCLUDE'):
        return False, "Authority must be *USE, *CHANGE, *ALL, or *EXCLUDE"

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO authorization_list_entries (autl_name, username, authority, added_by)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (autl_name, username) DO UPDATE SET authority = %s
            """, (autl_name, username, authority, added_by, authority))
        return True, f"User {username} added to {autl_name} with {authority}"
    except psycopg2.IntegrityError:
        return False, f"Authorization list {autl_name} not found"
    except Exception as e:
        return False, f"Failed to add entry: {e}"


def remove_authorization_list_entry(autl_name: str, username: str) -> tuple[bool, str]:
    """Remove a user from an authorization list (RMVAUTLE)."""
    autl_name = autl_name.upper().strip()
    username = username.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "DELETE FROM authorization_list_entries WHERE autl_name = %s AND username = %s",
                (autl_name, username)
            )
            if cursor.rowcount == 0:
                return False, f"Entry not found"
        return True, f"User {username} removed from {autl_name}"
    except Exception as e:
        return False, f"Failed to remove entry: {e}"


def get_authorization_list_entries(autl_name: str) -> list[dict]:
    """Get users in an authorization list."""
    autl_name = autl_name.upper().strip()
    entries = []

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM authorization_list_entries
                WHERE autl_name = %s ORDER BY username
            """, (autl_name,))
            for row in cursor.fetchall():
                entries.append(dict(row))
    except Exception as e:
        logger.error(f"Failed to get authorization list entries: {e}")
    return entries


def add_object_to_authorization_list(autl_name: str, object_type: str, object_name: str,
                                      added_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Add an object to an authorization list."""
    autl_name = autl_name.upper().strip()
    object_type = object_type.upper().strip()
    object_name = object_name.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO authorization_list_objects (autl_name, object_type, object_name, added_by)
                VALUES (%s, %s, %s, %s)
            """, (autl_name, object_type, object_name, added_by))
        return True, f"Object {object_name} added to {autl_name}"
    except psycopg2.IntegrityError:
        return False, f"Object already in list or authorization list not found"
    except Exception as e:
        return False, f"Failed to add object: {e}"


def remove_object_from_authorization_list(autl_name: str, object_type: str,
                                           object_name: str) -> tuple[bool, str]:
    """Remove an object from an authorization list."""
    autl_name = autl_name.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                DELETE FROM authorization_list_objects
                WHERE autl_name = %s AND object_type = %s AND object_name = %s
            """, (autl_name, object_type.upper(), object_name.upper()))
            if cursor.rowcount == 0:
                return False, "Object not found in list"
        return True, f"Object removed from {autl_name}"
    except Exception as e:
        return False, f"Failed to remove object: {e}"


def get_authorization_list_objects(autl_name: str) -> list[dict]:
    """Get objects secured by an authorization list."""
    autl_name = autl_name.upper().strip()
    objects = []

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM authorization_list_objects
                WHERE autl_name = %s ORDER BY object_type, object_name
            """, (autl_name,))
            for row in cursor.fetchall():
                objects.append(dict(row))
    except Exception as e:
        logger.error(f"Failed to get authorization list objects: {e}")
    return objects


# =============================================================================
# Subsystem Descriptions (AS/400-style SBSD)
# =============================================================================

def create_subsystem_description(name: str, description: str = '',
                                  celery_queue: str = None, worker_concurrency: int = 4,
                                  max_active_jobs: int = 0) -> tuple[bool, str]:
    """Create a subsystem description (CRTSBSD)."""
    name = name.upper().strip()[:10]

    if not name:
        return False, "Subsystem name is required"

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO subsystem_descriptions (name, description, celery_queue,
                                                    worker_concurrency, max_active_jobs)
                VALUES (%s, %s, %s, %s, %s)
            """, (name, description, celery_queue, worker_concurrency, max_active_jobs))
        return True, f"Subsystem description {name} created"
    except psycopg2.IntegrityError:
        return False, f"Subsystem {name} already exists"
    except Exception as e:
        return False, f"Failed to create subsystem: {e}"


def delete_subsystem_description(name: str) -> tuple[bool, str]:
    """Delete a subsystem description (DLTSBSD)."""
    name = name.upper().strip()

    if name in ('QBATCH', 'QINTER', 'QSPL', 'QCTL'):
        return False, f"Cannot delete system subsystem {name}"

    try:
        with get_cursor() as cursor:
            cursor.execute("DELETE FROM subsystem_descriptions WHERE name = %s", (name,))
            if cursor.rowcount == 0:
                return False, f"Subsystem {name} not found"
        return True, f"Subsystem {name} deleted"
    except Exception as e:
        return False, f"Failed to delete subsystem: {e}"


def get_subsystem_description(name: str) -> dict | None:
    """Get a subsystem description."""
    name = name.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute("SELECT * FROM subsystem_descriptions WHERE name = %s", (name,))
            row = cursor.fetchone()
            if row:
                return dict(row)
    except Exception as e:
        logger.error(f"Failed to get subsystem: {e}")
    return None


def list_subsystem_descriptions() -> list[dict]:
    """List subsystem descriptions (WRKSBSD)."""
    subsystems = []
    try:
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT sd.*,
                    (SELECT COUNT(*) FROM subsystem_job_queues jq WHERE jq.subsystem_name = sd.name) as jobq_count
                FROM subsystem_descriptions sd
                ORDER BY sd.name
            """)
            for row in cursor.fetchall():
                subsystems.append(dict(row))
    except Exception as e:
        logger.error(f"Failed to list subsystems: {e}")
    return subsystems


def start_subsystem(name: str) -> tuple[bool, str]:
    """Start a subsystem (STRSBS)."""
    name = name.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                UPDATE subsystem_descriptions
                SET status = '*ACTIVE', started_at = CURRENT_TIMESTAMP, stopped_at = NULL
                WHERE name = %s
            """, (name,))
            if cursor.rowcount == 0:
                return False, f"Subsystem {name} not found"
        return True, f"Subsystem {name} started"
    except Exception as e:
        return False, f"Failed to start subsystem: {e}"


def end_subsystem(name: str, option: str = '*CNTRLD') -> tuple[bool, str]:
    """End a subsystem (ENDSBS)."""
    name = name.upper().strip()

    if name == 'QCTL':
        return False, "Cannot end controlling subsystem"

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                UPDATE subsystem_descriptions
                SET status = '*INACTIVE', stopped_at = CURRENT_TIMESTAMP
                WHERE name = %s
            """, (name,))
            if cursor.rowcount == 0:
                return False, f"Subsystem {name} not found"
        return True, f"Subsystem {name} ended"
    except Exception as e:
        return False, f"Failed to end subsystem: {e}"


def add_job_queue_entry(subsystem_name: str, job_queue: str, sequence: int = 10,
                        max_active: int = 0) -> tuple[bool, str]:
    """Add a job queue entry to a subsystem (ADDJOBQE)."""
    subsystem_name = subsystem_name.upper().strip()
    job_queue = job_queue.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO subsystem_job_queues (subsystem_name, job_queue, sequence, max_active)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (subsystem_name, job_queue) DO UPDATE
                SET sequence = %s, max_active = %s
            """, (subsystem_name, job_queue, sequence, max_active, sequence, max_active))
        return True, f"Job queue {job_queue} added to {subsystem_name}"
    except psycopg2.IntegrityError:
        return False, f"Subsystem {subsystem_name} not found"
    except Exception as e:
        return False, f"Failed to add job queue entry: {e}"


def remove_job_queue_entry(subsystem_name: str, job_queue: str) -> tuple[bool, str]:
    """Remove a job queue entry from a subsystem (RMVJOBQE)."""
    subsystem_name = subsystem_name.upper().strip()
    job_queue = job_queue.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "DELETE FROM subsystem_job_queues WHERE subsystem_name = %s AND job_queue = %s",
                (subsystem_name, job_queue)
            )
            if cursor.rowcount == 0:
                return False, "Job queue entry not found"
        return True, f"Job queue {job_queue} removed from {subsystem_name}"
    except Exception as e:
        return False, f"Failed to remove job queue entry: {e}"


def get_subsystem_job_queues(subsystem_name: str) -> list[dict]:
    """Get job queues for a subsystem."""
    subsystem_name = subsystem_name.upper().strip()
    queues = []

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM subsystem_job_queues
                WHERE subsystem_name = %s ORDER BY sequence
            """, (subsystem_name,))
            for row in cursor.fetchall():
                queues.append(dict(row))
    except Exception as e:
        logger.error(f"Failed to get subsystem job queues: {e}")
    return queues


# =============================================================================
# Command Functions (AS/400 *CMD)
# Uses QSYS2.COMMAND_INFO naming convention
# =============================================================================

def list_commands(filter_prefix: str = '') -> list[dict]:
    """List all commands, optionally filtered by prefix."""
    commands = []
    filter_prefix = filter_prefix.upper().strip()

    try:
        with get_cursor() as cursor:
            if filter_prefix:
                cursor.execute("""
                    SELECT COMMAND_NAME, COMMAND_LIBRARY, TEXT_DESCRIPTION, SCREEN_NAME
                    FROM COMMAND_INFO
                    WHERE COMMAND_NAME LIKE %s
                    ORDER BY COMMAND_NAME
                """, (f"{filter_prefix}%",))
            else:
                cursor.execute("""
                    SELECT COMMAND_NAME, COMMAND_LIBRARY, TEXT_DESCRIPTION, SCREEN_NAME
                    FROM COMMAND_INFO
                    ORDER BY COMMAND_NAME
                """)
            for row in cursor.fetchall():
                commands.append(dict(row))
    except Exception as e:
        logger.error(f"Failed to list commands: {e}")
    return commands


def get_command(command_name: str) -> Optional[dict]:
    """Get a command definition."""
    command_name = command_name.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM COMMAND_INFO WHERE COMMAND_NAME = %s
            """, (command_name,))
            row = cursor.fetchone()
            if row:
                return dict(row)
    except Exception as e:
        logger.error(f"Failed to get command {command_name}: {e}")
    return None


def get_command_parameters(command_name: str) -> list[dict]:
    """Get parameters for a command."""
    command_name = command_name.upper().strip()
    params = []

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM COMMAND_PARM_INFO
                WHERE COMMAND_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (command_name,))
            for row in cursor.fetchall():
                params.append(dict(row))
    except Exception as e:
        logger.error(f"Failed to get parameters for {command_name}: {e}")
    return params


def get_parameter_valid_values(command_name: str, parm_name: str) -> list[dict]:
    """Get valid values for a command parameter."""
    command_name = command_name.upper().strip()
    parm_name = parm_name.upper().strip()
    values = []

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT VALID_VALUE, TEXT_DESCRIPTION
                FROM PARM_VALID_VALUES
                WHERE COMMAND_NAME = %s AND PARM_NAME = %s
                ORDER BY ORDINAL_POSITION, VALID_VALUE
            """, (command_name, parm_name))
            for row in cursor.fetchall():
                values.append(dict(row))
    except Exception as e:
        logger.error(f"Failed to get valid values for {command_name}.{parm_name}: {e}")
    return values


def create_command(
    command_name: str,
    text_description: str,
    screen_name: str = None,
    command_library: str = 'QSYS',
    processing_program: str = None,
    allow_interactive: str = 'YES',
    allow_batch: str = 'YES'
) -> tuple[bool, str]:
    """Create a new command definition."""
    command_name = command_name.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO COMMAND_INFO (COMMAND_NAME, COMMAND_LIBRARY, TEXT_DESCRIPTION,
                                          SCREEN_NAME, PROCESSING_PROGRAM,
                                          ALLOW_RUN_INTERACTIVE, ALLOW_RUN_BATCH)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (COMMAND_NAME) DO UPDATE SET
                    TEXT_DESCRIPTION = EXCLUDED.TEXT_DESCRIPTION,
                    SCREEN_NAME = EXCLUDED.SCREEN_NAME,
                    PROCESSING_PROGRAM = EXCLUDED.PROCESSING_PROGRAM,
                    ALLOW_RUN_INTERACTIVE = EXCLUDED.ALLOW_RUN_INTERACTIVE,
                    ALLOW_RUN_BATCH = EXCLUDED.ALLOW_RUN_BATCH
            """, (command_name, command_library, text_description, screen_name,
                  processing_program, allow_interactive, allow_batch))
        return True, f"Command {command_name} created"
    except Exception as e:
        return False, f"Failed to create command: {e}"


def add_command_parameter(
    command_name: str,
    parm_name: str,
    ordinal_position: int,
    prompt_text: str,
    data_type: str = '*CHAR',
    length: int = 10,
    default_value: str = None,
    is_required: str = 'NO'
) -> tuple[bool, str]:
    """Add a parameter to a command."""
    command_name = command_name.upper().strip()
    parm_name = parm_name.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO COMMAND_PARM_INFO
                    (COMMAND_NAME, PARM_NAME, ORDINAL_POSITION, DATA_TYPE, LENGTH,
                     DEFAULT_VALUE, PROMPT_TEXT, IS_REQUIRED)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (COMMAND_NAME, PARM_NAME) DO UPDATE SET
                    ORDINAL_POSITION = EXCLUDED.ORDINAL_POSITION,
                    DATA_TYPE = EXCLUDED.DATA_TYPE,
                    LENGTH = EXCLUDED.LENGTH,
                    DEFAULT_VALUE = EXCLUDED.DEFAULT_VALUE,
                    PROMPT_TEXT = EXCLUDED.PROMPT_TEXT,
                    IS_REQUIRED = EXCLUDED.IS_REQUIRED
            """, (command_name, parm_name, ordinal_position, data_type, length,
                  default_value, prompt_text, is_required))
        return True, f"Parameter {parm_name} added to {command_name}"
    except Exception as e:
        return False, f"Failed to add parameter: {e}"


def add_parameter_valid_value(
    command_name: str,
    parm_name: str,
    valid_value: str,
    text_description: str = '',
    ordinal_position: int = 0
) -> tuple[bool, str]:
    """Add a valid value for a parameter."""
    command_name = command_name.upper().strip()
    parm_name = parm_name.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO PARM_VALID_VALUES
                    (COMMAND_NAME, PARM_NAME, VALID_VALUE, TEXT_DESCRIPTION, ORDINAL_POSITION)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (COMMAND_NAME, PARM_NAME, VALID_VALUE) DO UPDATE SET
                    TEXT_DESCRIPTION = EXCLUDED.TEXT_DESCRIPTION,
                    ORDINAL_POSITION = EXCLUDED.ORDINAL_POSITION
            """, (command_name, parm_name, valid_value, text_description, ordinal_position))
        return True, f"Valid value {valid_value} added"
    except Exception as e:
        return False, f"Failed to add valid value: {e}"


def populate_default_commands() -> None:
    """Populate the commands table with default system commands."""
    # Command definitions: (name, description, screen_name)
    commands = [
        # Work with commands
        ('WRKACTJOB', 'Work with Active Jobs', 'wrkactjob'),
        ('WRKJOBQ', 'Work with Job Queues', 'wrkjobq'),
        ('WRKSVC', 'Work with Services (Docker)', 'wrksvc'),
        ('WRKHLTH', 'Work with Health Checks', 'wrkhlth'),
        ('WRKBKP', 'Work with Backups', 'wrkbkp'),
        ('WRKALR', 'Work with Alerts', 'wrkalr'),
        ('WRKNETDEV', 'Work with Network Devices', 'wrknetdev'),
        ('WRKUSRPRF', 'Work with User Profiles', 'wrkusrprf'),
        ('WRKMSGQ', 'Work with Message Queues', 'wrkmsgq'),
        ('WRKDTAARA', 'Work with Data Areas', 'wrkdtaara'),
        ('WRKJOBD', 'Work with Job Descriptions', 'wrkjobd'),
        ('WRKOUTQ', 'Work with Output Queues', 'wrkoutq'),
        ('WRKSPLF', 'Work with Spooled Files', 'wrksplf'),
        ('WRKJOBSCDE', 'Work with Job Schedule Entries', 'wrkjobscde'),
        ('WRKAUTL', 'Work with Authorization Lists', 'wrkautl'),
        ('WRKSBSD', 'Work with Subsystem Descriptions', 'wrksbsd'),
        ('WRKSCHEMA', 'Work with Schemas', 'wrkschema'),
        ('WRKLIB', 'Work with Libraries', 'wrkschema'),
        ('WRKSYSVAL', 'Work with System Values', 'wrksysval'),
        # Display commands
        ('DSPSYSSTS', 'Display System Status', 'dspsyssts'),
        ('DSPLOG', 'Display Log', 'dsplog'),
        ('DSPMSG', 'Display Messages', 'dspmsg'),
        ('DSPDTAARA', 'Display Data Area', 'dspdtaara'),
        ('DSPJOBD', 'Display Job Description', 'dspjobd'),
        ('DSPSPLF', 'Display Spooled File', 'dspsplf'),
        ('DSPAUTL', 'Display Authorization List', 'dspautl'),
        ('DSPOBJAUT', 'Display Object Authority', 'dspobjaut'),
        ('DSPUSRAUT', 'Display User Authorities', 'user_authorities'),
        ('DSPSYSVAL', 'Display System Values', 'wrksysval'),
        # Create commands
        ('CRTUSRPRF', 'Create User Profile', 'user_create'),
        ('CRTSCHEMA', 'Create Schema', 'schema_create'),
        ('CRTLIB', 'Create Library', 'schema_create'),
        ('CRTDTAARA', 'Create Data Area', 'crtdtaara'),
        ('CRTJOBD', 'Create Job Description', 'crtjobd'),
        ('CRTAUTL', 'Create Authorization List', 'crtautl'),
        # Change commands
        ('CHGDTAARA', 'Change Data Area', 'chgdtaara'),
        ('CHGSYSVAL', 'Change System Value', 'chgsysval'),
        # Add/Remove commands
        ('ADDJOBSCDE', 'Add Job Schedule Entry', 'addjobscde'),
        # Authority commands
        ('GRTOBJAUT', 'Grant Object Authority', 'grtobjaut'),
        ('RVKOBJAUT', 'Revoke Object Authority', 'rvkobjaut'),
        # Submit commands
        ('SBMJOB', 'Submit Job', 'sbmjob'),
        # Send commands
        ('SNDMSG', 'Send Message', 'sndmsg'),
        # Subsystem commands
        ('STRSBS', 'Start Subsystem', 'strsbs'),
        ('ENDSBS', 'End Subsystem', 'endsbs'),
        # Navigation
        ('GO', 'Go to Menu', 'main'),
        ('SIGNOFF', 'Sign Off', 'signon'),
    ]

    for cmd_name, description, screen_name in commands:
        create_command(cmd_name, description, screen_name)

    # Add parameters for key commands
    _populate_crtusrprf_parameters()
    _populate_sbmjob_parameters()
    _populate_sndmsg_parameters()
    _populate_crtdtaara_parameters()
    _populate_chgsysval_parameters()
    _populate_grtobjaut_parameters()
    _populate_addjobscde_parameters()

    logger.info("Default commands populated")


def _populate_crtusrprf_parameters():
    """Populate CRTUSRPRF command parameters."""
    cmd = 'CRTUSRPRF'

    # Parameters
    add_command_parameter(cmd, 'USRPRF', 1, 'User profile', '*NAME', 10, None, 'YES')
    add_command_parameter(cmd, 'PASSWORD', 2, 'Password', '*CHAR', 128, '*USRPRF', 'NO')
    add_command_parameter(cmd, 'USRCLS', 3, 'User class', '*CHAR', 10, '*USER', 'NO')
    add_command_parameter(cmd, 'TEXT', 4, 'Text description', '*CHAR', 50, None, 'NO')
    add_command_parameter(cmd, 'GRPPRF', 5, 'Group profile', '*NAME', 10, '*NONE', 'NO')
    add_command_parameter(cmd, 'CPYUSRPRF', 6, 'Copy from user', '*NAME', 10, None, 'NO')

    # Valid values for USRCLS
    add_parameter_valid_value(cmd, 'USRCLS', '*SECOFR', 'Security Officer', 1)
    add_parameter_valid_value(cmd, 'USRCLS', '*SECADM', 'Security Administrator', 2)
    add_parameter_valid_value(cmd, 'USRCLS', '*PGMR', 'Programmer', 3)
    add_parameter_valid_value(cmd, 'USRCLS', '*SYSOPR', 'System Operator', 4)
    add_parameter_valid_value(cmd, 'USRCLS', '*USER', 'User', 5)

    # Valid values for PASSWORD
    add_parameter_valid_value(cmd, 'PASSWORD', '*USRPRF', 'Same as user profile', 1)
    add_parameter_valid_value(cmd, 'PASSWORD', '*NONE', 'No password', 2)

    # Valid values for GRPPRF
    add_parameter_valid_value(cmd, 'GRPPRF', '*NONE', 'No group profile', 1)


def _populate_sbmjob_parameters():
    """Populate SBMJOB command parameters."""
    cmd = 'SBMJOB'

    add_command_parameter(cmd, 'CMD', 1, 'Command to run', '*CMD', 256, None, 'YES')
    add_command_parameter(cmd, 'JOB', 2, 'Job name', '*NAME', 10, '*JOBD', 'NO')
    add_command_parameter(cmd, 'JOBQ', 3, 'Job queue', '*NAME', 10, '*JOBD', 'NO')
    add_command_parameter(cmd, 'JOBD', 4, 'Job description', '*NAME', 10, 'QBATCH', 'NO')
    add_command_parameter(cmd, 'SCDDATE', 5, 'Scheduled date', '*CHAR', 10, '*CURRENT', 'NO')
    add_command_parameter(cmd, 'SCDTIME', 6, 'Scheduled time', '*CHAR', 8, '*CURRENT', 'NO')

    # Valid values
    add_parameter_valid_value(cmd, 'JOB', '*JOBD', 'Use job description', 1)
    add_parameter_valid_value(cmd, 'JOBQ', '*JOBD', 'Use job description', 1)
    add_parameter_valid_value(cmd, 'JOBQ', 'QBATCH', 'Batch job queue', 2)
    add_parameter_valid_value(cmd, 'JOBQ', 'QINTER', 'Interactive job queue', 3)
    add_parameter_valid_value(cmd, 'SCDDATE', '*CURRENT', 'Current date', 1)
    add_parameter_valid_value(cmd, 'SCDTIME', '*CURRENT', 'Current time', 1)


def _populate_sndmsg_parameters():
    """Populate SNDMSG command parameters."""
    cmd = 'SNDMSG'

    add_command_parameter(cmd, 'MSG', 1, 'Message text', '*CHAR', 256, None, 'YES')
    add_command_parameter(cmd, 'TOMSGQ', 2, 'To message queue', '*NAME', 10, '*SYSOPR', 'NO')
    add_command_parameter(cmd, 'MSGTYPE', 3, 'Message type', '*CHAR', 10, '*INFO', 'NO')

    add_parameter_valid_value(cmd, 'TOMSGQ', '*SYSOPR', 'System operator queue', 1)
    add_parameter_valid_value(cmd, 'TOMSGQ', '*ALLACT', 'All active users', 2)
    add_parameter_valid_value(cmd, 'MSGTYPE', '*INFO', 'Informational', 1)
    add_parameter_valid_value(cmd, 'MSGTYPE', '*INQ', 'Inquiry (requires reply)', 2)
    add_parameter_valid_value(cmd, 'MSGTYPE', '*COMP', 'Completion', 3)


def _populate_crtdtaara_parameters():
    """Populate CRTDTAARA command parameters."""
    cmd = 'CRTDTAARA'

    add_command_parameter(cmd, 'DTAARA', 1, 'Data area name', '*NAME', 10, None, 'YES')
    add_command_parameter(cmd, 'TYPE', 2, 'Data type', '*CHAR', 10, '*CHAR', 'NO')
    add_command_parameter(cmd, 'LEN', 3, 'Length', '*DEC', 5, '50', 'NO')
    add_command_parameter(cmd, 'VALUE', 4, 'Initial value', '*CHAR', 2000, None, 'NO')
    add_command_parameter(cmd, 'TEXT', 5, 'Text description', '*CHAR', 50, None, 'NO')

    add_parameter_valid_value(cmd, 'TYPE', '*CHAR', 'Character', 1)
    add_parameter_valid_value(cmd, 'TYPE', '*DEC', 'Decimal', 2)
    add_parameter_valid_value(cmd, 'TYPE', '*LGL', 'Logical', 3)


def _populate_chgsysval_parameters():
    """Populate CHGSYSVAL command parameters."""
    cmd = 'CHGSYSVAL'

    add_command_parameter(cmd, 'SYSVAL', 1, 'System value', '*NAME', 20, None, 'YES')
    add_command_parameter(cmd, 'VALUE', 2, 'New value', '*CHAR', 256, None, 'YES')


def _populate_grtobjaut_parameters():
    """Populate GRTOBJAUT command parameters."""
    cmd = 'GRTOBJAUT'

    add_command_parameter(cmd, 'OBJ', 1, 'Object', '*NAME', 128, None, 'YES')
    add_command_parameter(cmd, 'OBJTYPE', 2, 'Object type', '*CHAR', 10, '*FILE', 'NO')
    add_command_parameter(cmd, 'USER', 3, 'User', '*NAME', 10, None, 'YES')
    add_command_parameter(cmd, 'AUT', 4, 'Authority', '*CHAR', 10, '*USE', 'NO')

    add_parameter_valid_value(cmd, 'OBJTYPE', '*FILE', 'File/Table', 1)
    add_parameter_valid_value(cmd, 'OBJTYPE', '*PGM', 'Program', 2)
    add_parameter_valid_value(cmd, 'OBJTYPE', '*DTAARA', 'Data Area', 3)
    add_parameter_valid_value(cmd, 'OBJTYPE', '*MSGQ', 'Message Queue', 4)
    add_parameter_valid_value(cmd, 'OBJTYPE', '*OUTQ', 'Output Queue', 5)

    add_parameter_valid_value(cmd, 'AUT', '*ALL', 'All authority', 1)
    add_parameter_valid_value(cmd, 'AUT', '*CHANGE', 'Change authority', 2)
    add_parameter_valid_value(cmd, 'AUT', '*USE', 'Use authority', 3)
    add_parameter_valid_value(cmd, 'AUT', '*EXCLUDE', 'Exclude', 4)

    add_parameter_valid_value(cmd, 'USER', '*PUBLIC', 'All users', 1)


def _populate_addjobscde_parameters():
    """Populate ADDJOBSCDE command parameters."""
    cmd = 'ADDJOBSCDE'

    add_command_parameter(cmd, 'JOB', 1, 'Job name', '*NAME', 10, None, 'YES')
    add_command_parameter(cmd, 'CMD', 2, 'Command to run', '*CMD', 256, None, 'YES')
    add_command_parameter(cmd, 'FRQ', 3, 'Frequency', '*CHAR', 10, '*WEEKLY', 'NO')
    add_command_parameter(cmd, 'SCDDATE', 4, 'Scheduled date', '*CHAR', 10, '*NONE', 'NO')
    add_command_parameter(cmd, 'SCDDAY', 5, 'Scheduled day', '*CHAR', 10, '*NONE', 'NO')
    add_command_parameter(cmd, 'SCDTIME', 6, 'Scheduled time', '*CHAR', 8, '00:00', 'NO')
    add_command_parameter(cmd, 'JOBD', 7, 'Job description', '*NAME', 10, 'QBATCH', 'NO')
    add_command_parameter(cmd, 'TEXT', 8, 'Text description', '*CHAR', 50, None, 'NO')

    add_parameter_valid_value(cmd, 'FRQ', '*ONCE', 'Run once', 1)
    add_parameter_valid_value(cmd, 'FRQ', '*DAILY', 'Every day', 2)
    add_parameter_valid_value(cmd, 'FRQ', '*WEEKLY', 'Every week', 3)
    add_parameter_valid_value(cmd, 'FRQ', '*MONTHLY', 'Every month', 4)

    add_parameter_valid_value(cmd, 'SCDDAY', '*NONE', 'Not specified', 0)
    add_parameter_valid_value(cmd, 'SCDDAY', '*SUN', 'Sunday', 1)
    add_parameter_valid_value(cmd, 'SCDDAY', '*MON', 'Monday', 2)
    add_parameter_valid_value(cmd, 'SCDDAY', '*TUE', 'Tuesday', 3)
    add_parameter_valid_value(cmd, 'SCDDAY', '*WED', 'Wednesday', 4)
    add_parameter_valid_value(cmd, 'SCDDAY', '*THU', 'Thursday', 5)
    add_parameter_valid_value(cmd, 'SCDDAY', '*FRI', 'Friday', 6)
    add_parameter_valid_value(cmd, 'SCDDAY', '*SAT', 'Saturday', 7)

    add_parameter_valid_value(cmd, 'SCDDATE', '*NONE', 'Not specified', 1)
    add_parameter_valid_value(cmd, 'SCDDATE', '*CURRENT', 'Current date', 2)
