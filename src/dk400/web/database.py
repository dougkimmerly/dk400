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
from datetime import datetime
from zoneinfo import ZoneInfo
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


# Schema definitions - All system tables live in QSYS schema (authentic AS/400)
SCHEMA_SQL = """
-- =============================================================================
-- QSYS Schema - System Library (AS/400 authentic)
-- All system tables live here, not in public
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS qsys;

-- Users table (*USRPRF objects)
CREATE TABLE IF NOT EXISTS qsys.users (
    username VARCHAR(10) PRIMARY KEY,
    password_hash VARCHAR(128) NOT NULL,
    salt VARCHAR(64) NOT NULL,
    user_class VARCHAR(10) DEFAULT '*USER',
    status VARCHAR(10) DEFAULT '*ENABLED',
    description VARCHAR(50) DEFAULT '',
    group_profile VARCHAR(10) DEFAULT '*NONE',
    current_library VARCHAR(10) DEFAULT 'QGPL',
    library_list JSONB DEFAULT '["QGPL", "QSYS"]',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_signon TIMESTAMP,
    signon_attempts INTEGER DEFAULT 0,
    password_expires VARCHAR(10) DEFAULT '*NOMAX'
);

CREATE INDEX IF NOT EXISTS idx_users_status ON qsys.users(status);
CREATE INDEX IF NOT EXISTS idx_users_group ON qsys.users(group_profile);

-- Job history table (QJOBHST)
CREATE TABLE IF NOT EXISTS qsys._jobhst (
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

-- Audit/History log table (QHST)
CREATE TABLE IF NOT EXISTS qsys.qhst (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    username VARCHAR(10),
    action VARCHAR(50) NOT NULL,
    details TEXT,
    ip_address VARCHAR(45)
);

CREATE INDEX IF NOT EXISTS idx_qhst_timestamp ON qsys.qhst(timestamp);
CREATE INDEX IF NOT EXISTS idx_qhst_username ON qsys.qhst(username);

-- Object authorities table (QOBJAUT)
CREATE TABLE IF NOT EXISTS qsys._objaut (
    id SERIAL PRIMARY KEY,
    object_type VARCHAR(20) NOT NULL,
    object_name VARCHAR(128) NOT NULL,
    username VARCHAR(10) NOT NULL,
    authority VARCHAR(10) NOT NULL,
    granted_by VARCHAR(10),
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(object_type, object_name, username)
);

CREATE INDEX IF NOT EXISTS idx_objaut_object ON qsys._objaut(object_type, object_name);
CREATE INDEX IF NOT EXISTS idx_objaut_user ON qsys._objaut(username);

-- =============================================================================
-- Libraries Registry (QLIB)
-- =============================================================================
CREATE TABLE IF NOT EXISTS qsys._lib (
    name VARCHAR(10) PRIMARY KEY,
    type VARCHAR(10) DEFAULT '*PROD',
    text VARCHAR(50) DEFAULT '',
    asp_number INTEGER DEFAULT 1,
    create_authority VARCHAR(10) DEFAULT '*SYSVAL',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(10) DEFAULT 'QSECOFR'
);

-- System values table (WRKSYSVAL)
CREATE TABLE IF NOT EXISTS qsys.system_values (
    name VARCHAR(20) PRIMARY KEY,
    value VARCHAR(256) NOT NULL,
    description VARCHAR(100) DEFAULT '',
    category VARCHAR(20) DEFAULT 'SYSTEM',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(10) DEFAULT 'SYSTEM'
);

-- Default system values
INSERT INTO qsys.system_values (name, value, description, category) VALUES
    ('QSYSNAME', 'DK400', 'System name', 'SYSTEM'),
    ('QLOGOSIZE', '*SMALL', 'Logo display size (*FULL, *SMALL, *NONE)', 'DISPLAY'),
    ('QDATFMT', '*MDY', 'Date format (*MDY, *DMY, *YMD, *ISO)', 'DATETIME'),
    ('QTIMSEP', ':', 'Time separator character', 'DATETIME'),
    ('QDATSEP', '/', 'Date separator character', 'DATETIME'),
    ('QTIMZON', 'America/Toronto', 'System timezone (IANA format)', 'DATETIME'),
    ('QDSTADJ', '*YES', 'Daylight saving time adjustment (*YES, *NO)', 'DATETIME')
ON CONFLICT (name) DO NOTHING;

-- =============================================================================
-- Spooled Files (QSPLF - centralized, references library-based output queues)
-- =============================================================================
CREATE TABLE IF NOT EXISTS qsys._splf (
    id SERIAL PRIMARY KEY,
    name VARCHAR(10) NOT NULL,
    file_number INTEGER NOT NULL DEFAULT 1,
    job_name VARCHAR(28) NOT NULL,
    job_id VARCHAR(36),
    output_queue VARCHAR(10) DEFAULT 'QPRINT',
    output_queue_lib VARCHAR(10) DEFAULT 'QGPL',
    status VARCHAR(10) DEFAULT '*RDY',
    user_data VARCHAR(10) DEFAULT '',
    form_type VARCHAR(10) DEFAULT '*STD',
    copies INTEGER DEFAULT 1,
    pages INTEGER DEFAULT 0,
    total_records INTEGER DEFAULT 0,
    content TEXT,
    created_by VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_splf_job ON qsys._splf(job_name);
CREATE INDEX IF NOT EXISTS idx_splf_outq ON qsys._splf(output_queue, output_queue_lib, status);
CREATE INDEX IF NOT EXISTS idx_splf_user ON qsys._splf(created_by);

-- =============================================================================
-- Commands (QCMD - AS/400 *CMD objects in QSYS)
-- =============================================================================
CREATE TABLE IF NOT EXISTS qsys._cmd (
    command_name VARCHAR(10) PRIMARY KEY,
    command_library VARCHAR(10) DEFAULT 'QSYS',
    text_description VARCHAR(50) DEFAULT '',
    screen_name VARCHAR(30),
    processing_program VARCHAR(100),
    allow_run_interactive VARCHAR(3) DEFAULT 'YES',
    allow_run_batch VARCHAR(3) DEFAULT 'YES',
    threadsafe VARCHAR(5) DEFAULT '*NO',
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(10) DEFAULT 'SYSTEM'
);

CREATE TABLE IF NOT EXISTS qsys._cmdparm (
    command_name VARCHAR(10) NOT NULL REFERENCES qsys._cmd(command_name) ON DELETE CASCADE,
    parm_name VARCHAR(10) NOT NULL,
    ordinal_position INTEGER NOT NULL,
    data_type VARCHAR(10) DEFAULT '*CHAR',
    length INTEGER DEFAULT 10,
    decimal_positions INTEGER DEFAULT 0,
    default_value VARCHAR(100),
    prompt_text VARCHAR(40) NOT NULL,
    is_required VARCHAR(3) DEFAULT 'NO',
    min_value VARCHAR(50),
    max_value VARCHAR(50),
    PRIMARY KEY (command_name, parm_name)
);

CREATE INDEX IF NOT EXISTS idx_cmdparm_ord ON qsys._cmdparm(command_name, ordinal_position);

CREATE TABLE IF NOT EXISTS qsys._prmval (
    command_name VARCHAR(10) NOT NULL,
    parm_name VARCHAR(10) NOT NULL,
    valid_value VARCHAR(50) NOT NULL,
    text_description VARCHAR(50) DEFAULT '',
    ordinal_position INTEGER DEFAULT 0,
    PRIMARY KEY (command_name, parm_name, valid_value),
    FOREIGN KEY (command_name, parm_name)
        REFERENCES qsys._cmdparm(command_name, parm_name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_prmval_ord ON qsys._prmval(command_name, parm_name, ordinal_position);

-- =============================================================================
-- Default Libraries
-- =============================================================================
INSERT INTO qsys._lib (name, type, text, created_by) VALUES
    ('QSYS', '*PROD', 'System Library', 'QSECOFR'),
    ('QGPL', '*PROD', 'General Purpose Library', 'QSECOFR'),
    ('QUSRSYS', '*PROD', 'User System Library', 'QSECOFR')
ON CONFLICT (name) DO NOTHING;
"""

# SQL template for creating object tables within a library schema
LIBRARY_OBJECT_TABLES_SQL = '''
-- Data Areas in library {lib}
CREATE TABLE IF NOT EXISTS {lib}._dtaara (
    name VARCHAR(10) NOT NULL PRIMARY KEY,
    type VARCHAR(10) DEFAULT '*CHAR',
    length INTEGER DEFAULT 2000,
    decimal_positions INTEGER DEFAULT 0,
    value TEXT DEFAULT '',
    text VARCHAR(50) DEFAULT '',
    locked_by VARCHAR(10),
    locked_at TIMESTAMP,
    created_by VARCHAR(10),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by VARCHAR(10),
    changed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Message Queues in library {lib}
CREATE TABLE IF NOT EXISTS {lib}._msgq (
    name VARCHAR(10) NOT NULL PRIMARY KEY,
    text VARCHAR(50) DEFAULT '',
    delivery VARCHAR(10) DEFAULT '*HOLD',
    severity INTEGER DEFAULT 0,
    created_by VARCHAR(10),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Messages in message queues
CREATE TABLE IF NOT EXISTS {lib}._msg (
    id SERIAL PRIMARY KEY,
    msgq VARCHAR(10) NOT NULL REFERENCES {lib}._msgq(name) ON DELETE CASCADE,
    msg_id VARCHAR(7),
    msg_type VARCHAR(10) DEFAULT '*INFO',
    severity INTEGER DEFAULT 0,
    msg_text TEXT NOT NULL,
    msg_data TEXT,
    sender VARCHAR(10),
    sent TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(10) DEFAULT '*NEW'
);
CREATE INDEX IF NOT EXISTS idx_{lib_safe}_msg_q ON {lib}._msg(msgq, status);

-- Query Definitions in library {lib}
CREATE TABLE IF NOT EXISTS {lib}._qrydfn (
    name VARCHAR(10) NOT NULL PRIMARY KEY,
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
    changed_by VARCHAR(10),
    changed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_run TIMESTAMP
);

-- Job Descriptions in library {lib}
CREATE TABLE IF NOT EXISTS {lib}._jobd (
    name VARCHAR(10) NOT NULL PRIMARY KEY,
    text VARCHAR(50) DEFAULT '',
    job_queue VARCHAR(21) DEFAULT 'QGPL/QBATCH',
    job_priority INTEGER DEFAULT 5,
    output_queue VARCHAR(21) DEFAULT '*USRPRF',
    print_device VARCHAR(10) DEFAULT '*USRPRF',
    routing_data VARCHAR(80) DEFAULT 'QCMDB',
    request_data VARCHAR(256) DEFAULT '',
    user_profile VARCHAR(10) DEFAULT '*RQD',
    accounting_code VARCHAR(15) DEFAULT '',
    log_level INTEGER DEFAULT 4,
    log_severity INTEGER DEFAULT 20,
    log_text VARCHAR(10) DEFAULT '*MSG',
    hold_on_jobq VARCHAR(10) DEFAULT '*NO',
    created_by VARCHAR(10),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Output Queues in library {lib}
CREATE TABLE IF NOT EXISTS {lib}._outq (
    name VARCHAR(10) NOT NULL PRIMARY KEY,
    text VARCHAR(50) DEFAULT '',
    status VARCHAR(10) DEFAULT '*RLS',
    max_size INTEGER DEFAULT 0,
    authority VARCHAR(10) DEFAULT '*USE',
    created_by VARCHAR(10),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Authorization Lists in library {lib}
CREATE TABLE IF NOT EXISTS {lib}._autl (
    name VARCHAR(10) NOT NULL PRIMARY KEY,
    text VARCHAR(50) DEFAULT '',
    created_by VARCHAR(10),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Authorization List Entries
CREATE TABLE IF NOT EXISTS {lib}._autle (
    autl VARCHAR(10) NOT NULL REFERENCES {lib}._autl(name) ON DELETE CASCADE,
    user_profile VARCHAR(10) NOT NULL,
    authority VARCHAR(10) DEFAULT '*USE',
    PRIMARY KEY (autl, user_profile)
);

-- Subsystem Descriptions in library {lib}
CREATE TABLE IF NOT EXISTS {lib}._sbsd (
    name VARCHAR(10) NOT NULL PRIMARY KEY,
    text VARCHAR(50) DEFAULT '',
    max_active_jobs INTEGER DEFAULT 0,
    status VARCHAR(10) DEFAULT '*INACTIVE',
    created_by VARCHAR(10),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Job Schedule Entries in library {lib}
CREATE TABLE IF NOT EXISTS {lib}._jobscde (
    name VARCHAR(20) NOT NULL PRIMARY KEY,
    text VARCHAR(50) DEFAULT '',
    command TEXT NOT NULL,
    frequency VARCHAR(10) DEFAULT '*ONCE',
    schedule_date DATE,
    schedule_time TIME,
    days_of_week VARCHAR(20) DEFAULT '',
    status VARCHAR(10) DEFAULT '*ACTIVE',
    last_run TIMESTAMP,
    next_run TIMESTAMP,
    created_by VARCHAR(10),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
'''


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

        # Create default library schemas (QSYS, QGPL, QUSRSYS)
        for lib in ['QSYS', 'QGPL', 'QUSRSYS']:
            _ensure_library_schema(lib)
        logger.info("Default library schemas created")

        # Migrate data from public to qsys (for upgrades from older versions)
        _migrate_public_to_qsys()

        # Rename tables to AS/400-style names (for upgrades)
        _rename_tables_to_as400_style()

        # Add library list columns to users (for upgrades)
        _add_library_list_columns()

        # Migrate existing objects to library schemas
        _migrate_objects_to_libraries()

        # Create default system objects in QGPL
        _create_default_system_objects()

        # Populate default commands after schema creation
        populate_default_commands()
        return True
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return False


def _migrate_public_to_qsys():
    """
    Migrate existing data from public schema tables to qsys schema.
    This handles upgrades from older versions that had system tables in public.
    """
    migrations = [
        # (source_table, dest_table, columns)
        ('public.users', 'qsys.users',
         'username, password_hash, salt, user_class, status, description, group_profile, created, last_signon, signon_attempts, password_expires'),
        ('public.system_values', 'qsys.system_values',
         'name, value, description, category, updated_at, updated_by'),
        ('public.libraries', 'qsys._lib',
         'name, type, text, asp_number, create_authority, created, created_by'),
        ('public.object_authorities', 'qsys._objaut',
         'id, object_type, object_name, username, authority, granted_by, granted_at'),
        ('public.job_history', 'qsys._jobhst',
         'id, job_name, job_type, status, submitted_by, submitted_at, started_at, completed_at, result, error'),
        ('public.audit_log', 'qsys.qhst',
         'id, timestamp, username, action, details, ip_address'),
        ('public.spooled_files', 'qsys._splf',
         'id, name, file_number, job_name, job_id, output_queue, output_queue_lib, status, user_data, form_type, copies, pages, total_records, content, created_by, created_at'),
        ('public.command_info', 'qsys._cmd',
         'command_name, command_library, text_description, screen_name, processing_program, allow_run_interactive, allow_run_batch, threadsafe, created, created_by'),
        ('public.command_parm_info', 'qsys._cmdparm',
         'command_name, parm_name, ordinal_position, data_type, length, decimal_positions, default_value, prompt_text, is_required, min_value, max_value'),
        ('public.parm_valid_values', 'qsys._prmval',
         'command_name, parm_name, valid_value, text_description, ordinal_position'),
    ]

    try:
        with get_cursor(dict_cursor=False) as cursor:
            for source, dest, columns in migrations:
                # Check if source table exists
                source_schema, source_table = source.split('.')
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                    )
                """, (source_schema, source_table))

                if not cursor.fetchone()[0]:
                    continue  # Source table doesn't exist, skip

                # Check if source has data
                cursor.execute(f"SELECT COUNT(*) FROM {source}")
                if cursor.fetchone()[0] == 0:
                    continue  # No data to migrate

                # Migrate data (ON CONFLICT DO NOTHING to avoid duplicates)
                try:
                    # Use INSERT ... SELECT with ON CONFLICT for tables with primary keys
                    cursor.execute(f"""
                        INSERT INTO {dest} ({columns})
                        SELECT {columns} FROM {source}
                        ON CONFLICT DO NOTHING
                    """)
                    logger.info(f"Migrated data from {source} to {dest}")
                except Exception as e:
                    logger.warning(f"Could not migrate {source} to {dest}: {e}")

        logger.info("Public to qsys migration completed")
    except Exception as e:
        logger.warning(f"Public to qsys migration: {e}")


def _add_library_list_columns():
    """Add current_library and library_list columns to existing users table."""
    try:
        with get_cursor(dict_cursor=False) as cursor:
            # Add current_library column if not exists
            cursor.execute("""
                ALTER TABLE qsys.users
                ADD COLUMN IF NOT EXISTS current_library VARCHAR(10) DEFAULT 'QGPL'
            """)
            # Add library_list column if not exists
            cursor.execute("""
                ALTER TABLE qsys.users
                ADD COLUMN IF NOT EXISTS library_list JSONB DEFAULT '["QGPL", "QSYS"]'
            """)
        logger.info("Library list columns added to users table")
    except Exception as e:
        logger.warning(f"Adding library list columns: {e}")


def _rename_tables_to_as400_style():
    """
    Rename existing qsys tables to AS/400-style names.
    This handles upgrades from older versions with non-AS/400 names.
    """
    renames = [
        # (old_name, new_name)
        ('qsys.job_history', 'qsys._jobhst'),
        ('qsys.audit_log', 'qsys.qhst'),
        ('qsys.object_authorities', 'qsys._objaut'),
        ('qsys.libraries', 'qsys._lib'),
        ('qsys.spooled_files', 'qsys._splf'),
        ('qsys.command_info', 'qsys._cmd'),
        ('qsys.command_parm_info', 'qsys._cmdparm'),
        ('qsys.parm_valid_values', 'qsys._prmval'),
    ]

    try:
        with get_cursor(dict_cursor=False) as cursor:
            for old_name, new_name in renames:
                old_schema, old_table = old_name.split('.')
                new_schema, new_table = new_name.split('.')

                # Check if old table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                    )
                """, (old_schema, old_table))

                if not cursor.fetchone()[0]:
                    continue  # Old table doesn't exist, skip

                # Check if new table already exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                    )
                """, (new_schema, new_table))

                if cursor.fetchone()[0]:
                    # New table exists - migrate data and drop old
                    logger.info(f"New table {new_name} exists, migrating data from {old_name}")
                    try:
                        cursor.execute(f"INSERT INTO {new_name} SELECT * FROM {old_name} ON CONFLICT DO NOTHING")
                        cursor.execute(f"DROP TABLE {old_name} CASCADE")
                        logger.info(f"Migrated data and dropped {old_name}")
                    except Exception as e:
                        logger.warning(f"Could not migrate from {old_name}: {e}")
                else:
                    # Rename old table to new name
                    try:
                        cursor.execute(f"ALTER TABLE {old_name} RENAME TO {new_table}")
                        logger.info(f"Renamed {old_name} to {new_name}")
                    except Exception as e:
                        logger.warning(f"Could not rename {old_name} to {new_name}: {e}")

        logger.info("AS/400-style table rename completed")
    except Exception as e:
        logger.warning(f"AS/400-style table rename: {e}")


def _create_default_system_objects():
    """Create default system objects in library schemas."""
    try:
        with get_cursor(dict_cursor=False) as cursor:
            # Create default output queues in QGPL
            cursor.execute("""
                INSERT INTO qgpl._outq (name, text, status, created_by)
                VALUES ('QPRINT', 'Default print output queue', '*RLS', 'SYSTEM')
                ON CONFLICT (name) DO NOTHING
            """)
            cursor.execute("""
                INSERT INTO qgpl._outq (name, text, status, created_by)
                VALUES ('QPRINT2', 'Secondary print output queue', '*RLS', 'SYSTEM')
                ON CONFLICT (name) DO NOTHING
            """)

            # Create default message queues in QSYS
            cursor.execute("""
                INSERT INTO qsys._msgq (name, text, delivery, created_by)
                VALUES ('QSYSOPR', 'System operator message queue', '*BREAK', 'SYSTEM')
                ON CONFLICT (name) DO NOTHING
            """)
            cursor.execute("""
                INSERT INTO qsys._msgq (name, text, delivery, created_by)
                VALUES ('QSYSMSG', 'System message queue', '*HOLD', 'SYSTEM')
                ON CONFLICT (name) DO NOTHING
            """)

            # Create default data areas in QSYS
            cursor.execute("""
                INSERT INTO qsys._dtaara (name, type, length, value, text, created_by)
                VALUES ('QDATE', '*CHAR', 8, TO_CHAR(CURRENT_DATE, 'YYYYMMDD'), 'System date', 'SYSTEM')
                ON CONFLICT (name) DO UPDATE SET value = TO_CHAR(CURRENT_DATE, 'YYYYMMDD')
            """)
            cursor.execute("""
                INSERT INTO qsys._dtaara (name, type, length, value, text, created_by)
                VALUES ('QTIME', '*CHAR', 6, TO_CHAR(CURRENT_TIME::TIME, 'HH24MISS'), 'System time', 'SYSTEM')
                ON CONFLICT (name) DO UPDATE SET value = TO_CHAR(CURRENT_TIME::TIME, 'HH24MISS')
            """)

        logger.info("Default system objects created")
    except Exception as e:
        logger.warning(f"Creating default system objects: {e}")


def _ensure_library_schema(library: str) -> bool:
    """Ensure a library schema exists with all object tables."""
    lib = library.upper()
    lib_safe = lib.lower().replace('-', '_')  # Safe for identifiers

    try:
        with get_cursor(dict_cursor=False) as cursor:
            # Create schema if not exists
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(lib_safe)
            ))

            # Create object tables in the schema
            # Use string formatting for table creation (schema qualified)
            lib_sql = LIBRARY_OBJECT_TABLES_SQL.format(lib=lib_safe, lib_safe=lib_safe)
            cursor.execute(lib_sql)

        logger.info(f"Library schema {lib} ensured")
        return True
    except Exception as e:
        logger.error(f"Failed to ensure library schema {lib}: {e}")
        return False


def _migrate_objects_to_libraries():
    """Migrate existing objects from old tables to library schemas (one-time)."""
    try:
        with get_cursor(dict_cursor=False) as cursor:
            # Check if migration is needed (old tables have data, new don't)
            cursor.execute("SELECT COUNT(*) FROM data_areas WHERE library != '*LIBL'")
            old_dtaara_count = cursor.fetchone()[0]

            if old_dtaara_count > 0:
                # Check if already migrated
                cursor.execute("""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = 'qgpl' AND table_name = '_dtaara'
                """)
                if cursor.fetchone()[0] > 0:
                    cursor.execute("SELECT COUNT(*) FROM qgpl._dtaara")
                    if cursor.fetchone()[0] > 0:
                        logger.info("Objects already migrated to library schemas")
                        return

            # Migrate data areas
            cursor.execute("""
                INSERT INTO qgpl._dtaara (name, type, length, decimal_positions, value, text, created_by, created)
                SELECT name, type, length, decimal_positions, value, description, created_by, created_at
                FROM data_areas
                WHERE library IN ('*LIBL', 'QGPL')
                ON CONFLICT (name) DO NOTHING
            """)

            # Migrate message queues (old table has 'queue_type', new has 'delivery')
            cursor.execute("""
                INSERT INTO qgpl._msgq (name, text, delivery, created_by, created)
                SELECT name, description,
                    CASE queue_type WHEN '*BREAK' THEN '*BREAK' ELSE '*HOLD' END,
                    COALESCE(created_by, 'SYSTEM'), COALESCE(created_at, CURRENT_TIMESTAMP)
                FROM message_queues
                ON CONFLICT (name) DO NOTHING
            """)

            # Migrate query definitions
            cursor.execute("""
                INSERT INTO qgpl._qrydfn (name, text, source_schema, source_table, selected_columns,
                    where_conditions, order_by_fields, summary_functions, group_by_fields,
                    output_type, row_limit, created_by, created, changed_by, changed, last_run)
                SELECT name, description, source_schema, source_table, selected_columns,
                    where_conditions, order_by_fields,
                    COALESCE(summary_functions, '[]'::jsonb),
                    COALESCE(group_by_fields, '[]'::jsonb),
                    output_type, row_limit, created_by, created_at, updated_by, updated_at, last_run_at
                FROM query_definitions
                WHERE library IN ('*LIBL', 'QGPL')
                ON CONFLICT (name) DO NOTHING
            """)

            # Migrate job descriptions
            cursor.execute("""
                INSERT INTO qgpl._jobd (name, text, job_queue, job_priority, output_queue,
                    print_device, routing_data, request_data, user_profile, accounting_code,
                    log_level, log_severity, log_text, hold_on_jobq, created_by, created)
                SELECT name, description, job_queue, job_priority, output_queue,
                    print_device, routing_data, request_data, user_profile, accounting_code,
                    log_level, log_severity, log_text, hold_on_jobq, created_by, created_at
                FROM job_descriptions
                WHERE library IN ('*LIBL', 'QGPL')
                ON CONFLICT (name) DO NOTHING
            """)

            # Migrate output queues
            cursor.execute("""
                INSERT INTO qgpl._outq (name, text, status, max_size, authority, created_by, created)
                SELECT name, description, status, max_size, authority, created_by, created_at
                FROM output_queues
                WHERE library IN ('*LIBL', 'QGPL')
                ON CONFLICT (name) DO NOTHING
            """)

            logger.info("Migrated existing objects to library schemas")

    except Exception as e:
        logger.warning(f"Migration to library schemas: {e}")


# =============================================================================
# Library Management Functions (CRTLIB/DLTLIB/WRKLIB)
# =============================================================================

def create_library(name: str, text: str = '', lib_type: str = '*PROD',
                   asp_number: int = 1, create_authority: str = '*SYSVAL',
                   created_by: str = 'QSECOFR') -> tuple[bool, str]:
    """
    Create a library (AS/400 CRTLIB).
    Creates the library entry and PostgreSQL schema with object tables.
    """
    lib = name.upper()
    if len(lib) > 10:
        return False, "Library name must be 10 characters or less"

    try:
        with get_cursor() as cursor:
            # Check if library already exists
            cursor.execute("SELECT name FROM qsys._lib WHERE name = %s", (lib,))
            if cursor.fetchone():
                return False, f"Library {lib} already exists"

            # Insert library record
            cursor.execute("""
                INSERT INTO qsys._lib (name, type, text, asp_number, create_authority, created_by)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (lib, lib_type, text, asp_number, create_authority, created_by))

        # Create the schema with object tables
        if not _ensure_library_schema(lib):
            return False, f"Failed to create schema for library {lib}"

        return True, f"Library {lib} created"

    except Exception as e:
        logger.error(f"Failed to create library {lib}: {e}")
        return False, str(e)


def delete_library(name: str) -> tuple[bool, str]:
    """
    Delete a library (AS/400 DLTLIB).
    Drops the schema and all objects within it.
    """
    lib = name.upper()

    # Protect system libraries
    if lib in ('QSYS', 'QGPL', 'QUSRSYS', 'QTEMP'):
        return False, f"Cannot delete system library {lib}"

    try:
        lib_safe = lib.lower().replace('-', '_')

        with get_cursor(dict_cursor=False) as cursor:
            # Check if library exists
            cursor.execute("SELECT name FROM qsys._lib WHERE name = %s", (lib,))
            if not cursor.fetchone():
                return False, f"Library {lib} not found"

            # Drop the schema (CASCADE drops all objects)
            cursor.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(lib_safe)
            ))

            # Delete library record
            cursor.execute("DELETE FROM qsys._lib WHERE name = %s", (lib,))

        return True, f"Library {lib} deleted"

    except Exception as e:
        logger.error(f"Failed to delete library {lib}: {e}")
        return False, str(e)


def list_libraries() -> list[dict]:
    """List all libraries (AS/400 WRKLIB)."""
    try:
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT name, type, text, asp_number, created, created_by
                FROM qsys._lib
                ORDER BY name
            """)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Failed to list libraries: {e}")
        return []


def get_library(name: str) -> Optional[dict]:
    """Get library details."""
    try:
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT name, type, text, asp_number, created, created_by
                FROM qsys._lib
                WHERE name = %s
            """, (name.upper(),))
            row = cursor.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"Failed to get library {name}: {e}")
        return None


def library_exists(name: str) -> bool:
    """Check if a library exists."""
    try:
        with get_cursor() as cursor:
            cursor.execute("SELECT 1 FROM qsys._lib WHERE name = %s", (name.upper(),))
            return cursor.fetchone() is not None
    except Exception:
        return False


def get_library_objects(library: str, obj_type: str = '*ALL') -> list[dict]:
    """
    List objects in a library (AS/400 WRKOBJ).
    obj_type: *ALL, *DTAARA, *MSGQ, *QRYDFN, *JOBD, *OUTQ, *FILE, etc.
    """
    lib = library.upper()
    lib_safe = lib.lower().replace('-', '_')

    objects = []

    try:
        with get_cursor() as cursor:
            # Data Areas
            if obj_type in ('*ALL', '*DTAARA'):
                try:
                    cursor.execute(sql.SQL("""
                        SELECT name, 'DTAARA' as type, text, created, created_by
                        FROM {}._dtaara ORDER BY name
                    """).format(sql.Identifier(lib_safe)))
                    objects.extend([{**dict(r), 'library': lib} for r in cursor.fetchall()])
                except:
                    pass

            # Message Queues
            if obj_type in ('*ALL', '*MSGQ'):
                try:
                    cursor.execute(sql.SQL("""
                        SELECT name, 'MSGQ' as type, text, created, created_by
                        FROM {}._msgq ORDER BY name
                    """).format(sql.Identifier(lib_safe)))
                    objects.extend([{**dict(r), 'library': lib} for r in cursor.fetchall()])
                except:
                    pass

            # Query Definitions
            if obj_type in ('*ALL', '*QRYDFN'):
                try:
                    cursor.execute(sql.SQL("""
                        SELECT name, 'QRYDFN' as type, text, created, created_by
                        FROM {}._qrydfn ORDER BY name
                    """).format(sql.Identifier(lib_safe)))
                    objects.extend([{**dict(r), 'library': lib} for r in cursor.fetchall()])
                except:
                    pass

            # Job Descriptions
            if obj_type in ('*ALL', '*JOBD'):
                try:
                    cursor.execute(sql.SQL("""
                        SELECT name, 'JOBD' as type, text, created, created_by
                        FROM {}._jobd ORDER BY name
                    """).format(sql.Identifier(lib_safe)))
                    objects.extend([{**dict(r), 'library': lib} for r in cursor.fetchall()])
                except:
                    pass

            # Output Queues
            if obj_type in ('*ALL', '*OUTQ'):
                try:
                    cursor.execute(sql.SQL("""
                        SELECT name, 'OUTQ' as type, text, created, created_by
                        FROM {}._outq ORDER BY name
                    """).format(sql.Identifier(lib_safe)))
                    objects.extend([{**dict(r), 'library': lib} for r in cursor.fetchall()])
                except:
                    pass

            # Physical Files (Tables) - actual PostgreSQL tables in the schema
            if obj_type in ('*ALL', '*FILE', '*PF'):
                try:
                    cursor.execute("""
                        SELECT table_name as name, 'FILE' as type, '' as text,
                               NULL as created, NULL as created_by
                        FROM information_schema.tables
                        WHERE table_schema = %s
                          AND table_type = 'BASE TABLE'
                          AND table_name NOT LIKE '\\_%%'
                        ORDER BY table_name
                    """, (lib_safe,))
                    objects.extend([{**dict(r), 'library': lib} for r in cursor.fetchall()])
                except:
                    pass

        return objects

    except Exception as e:
        logger.error(f"Failed to get objects in library {lib}: {e}")
        return []


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
                    "SELECT 1 FROM qsys.users WHERE username = %s",
                    (username,)
                )
                if cursor.fetchone():
                    skipped += 1
                    continue

                # Insert user
                cursor.execute("""
                    INSERT INTO qsys.users (
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
            'qsys.users': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            'qsys._jobhst': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            'qsys.qhst': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            'qsys._objaut': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
        },
        'special': ['CREATEROLE'],  # Can manage other roles
    },
    '*SECADM': {
        # Security Admin - can manage users but not full system access
        'tables': {
            'qsys.users': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            'qsys._jobhst': ['SELECT'],
            'qsys.qhst': ['SELECT', 'INSERT'],
            'qsys._objaut': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
        },
        'special': [],
    },
    '*PGMR': {
        # Programmer - full access to app tables, read users
        'tables': {
            'qsys.users': ['SELECT'],
            'qsys._jobhst': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            'qsys.qhst': ['SELECT', 'INSERT'],
            'qsys._objaut': ['SELECT'],
        },
        'special': [],
    },
    '*SYSOPR': {
        # System Operator - operational access
        'tables': {
            'qsys.users': ['SELECT'],
            'qsys._jobhst': ['SELECT', 'INSERT', 'UPDATE'],
            'qsys.qhst': ['SELECT', 'INSERT'],
            'qsys._objaut': ['SELECT'],
        },
        'special': [],
    },
    '*USER': {
        # Regular user - read-only on most tables
        'tables': {
            'qsys.users': [],  # No direct access to users table
            'qsys._jobhst': ['SELECT'],
            'qsys.qhst': [],  # No access to audit log
            'qsys._objaut': ['SELECT'],
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
            # Handle schema-qualified table names (e.g., 'qsys.users')
            if '.' in table:
                schema, table_name = table.split('.', 1)
                table_ref = sql.SQL("{}.{}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name)
                )
            else:
                table_ref = sql.Identifier(table)
            cursor.execute(
                sql.SQL("GRANT {} ON {} TO {}").format(
                    sql.SQL(privs),
                    table_ref,
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
                INSERT INTO qsys._objaut (object_type, object_name, username, authority, granted_by)
                VALUES ('SCHEMA', %s, %s, '*OWNER', %s)
                ON CONFLICT (object_type, object_name, username) DO UPDATE SET authority = '*OWNER'
            """, (schema_name.upper(), owner.upper() if owner else 'DK400', 'DK400'))

            # Grant *SECOFR users (security officers) full access to new schemas
            cursor.execute("""
                SELECT username FROM qsys.users WHERE user_class = '*SECOFR'
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
                "DELETE FROM qsys._objaut WHERE object_type = 'SCHEMA' AND object_name = %s",
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
                LEFT JOIN qsys._objaut oa
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
                    DELETE FROM qsys._objaut
                    WHERE object_type = %s AND object_name = %s AND username = %s
                """, (object_type, object_name.upper(), username))
            else:
                cursor.execute("""
                    INSERT INTO qsys._objaut (object_type, object_name, username, authority, granted_by)
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
            query = "SELECT * FROM qsys._objaut WHERE 1=1"
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
            cursor.execute("SELECT 1 FROM qsys.users WHERE username = %s", (group_profile,))
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
                "UPDATE qsys.users SET group_profile = %s WHERE username = %s",
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
                "UPDATE qsys.users SET group_profile = '*NONE' WHERE username = %s",
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
                "SELECT group_profile FROM qsys.users WHERE username = %s",
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
                "SELECT username FROM qsys.users WHERE group_profile = %s ORDER BY username",
                (group_profile,)
            )
            for row in cursor.fetchall():
                members.append(row['username'])
    except Exception as e:
        logger.error(f"Failed to get group members: {e}")

    return members


# =============================================================================
# Library List Functions (*LIBL support)
# =============================================================================

DEFAULT_LIBRARY_LIST = ['QGPL', 'QSYS']


def get_user_library_list(username: str) -> list[str]:
    """Get a user's library list. Returns default if not set."""
    username = username.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "SELECT library_list, current_library FROM qsys.users WHERE username = %s",
                (username,)
            )
            row = cursor.fetchone()
            if row and row.get('library_list'):
                lib_list = row['library_list']
                if isinstance(lib_list, list):
                    return lib_list
                # Handle JSON string
                import json
                return json.loads(lib_list) if isinstance(lib_list, str) else DEFAULT_LIBRARY_LIST
    except Exception as e:
        logger.error(f"Failed to get library list: {e}")

    return DEFAULT_LIBRARY_LIST.copy()


def get_user_current_library(username: str) -> str:
    """Get a user's current library (where new objects are created with *LIBL)."""
    username = username.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "SELECT current_library FROM qsys.users WHERE username = %s",
                (username,)
            )
            row = cursor.fetchone()
            if row and row.get('current_library'):
                return row['current_library']
    except Exception as e:
        logger.error(f"Failed to get current library: {e}")

    return 'QGPL'


def set_user_library_list(username: str, library_list: list[str]) -> tuple[bool, str]:
    """Set a user's library list."""
    username = username.upper().strip()
    # Uppercase all library names
    library_list = [lib.upper().strip() for lib in library_list]

    try:
        import json
        with get_cursor() as cursor:
            cursor.execute(
                "UPDATE qsys.users SET library_list = %s WHERE username = %s",
                (json.dumps(library_list), username)
            )
        return True, f"Library list updated for {username}"
    except Exception as e:
        logger.error(f"Failed to set library list: {e}")
        return False, str(e)


def set_user_current_library(username: str, library: str) -> tuple[bool, str]:
    """Set a user's current library."""
    username = username.upper().strip()
    library = library.upper().strip()

    try:
        with get_cursor() as cursor:
            cursor.execute(
                "UPDATE qsys.users SET current_library = %s WHERE username = %s",
                (library, username)
            )
        return True, f"Current library set to {library} for {username}"
    except Exception as e:
        logger.error(f"Failed to set current library: {e}")
        return False, str(e)


def resolve_library(library: str, username: str) -> list[str]:
    """
    Resolve a library specification to actual library names.

    Args:
        library: Library name or '*LIBL' to search library list
        username: User whose library list to use for *LIBL

    Returns:
        List of library names to search (single item for specific library,
        multiple for *LIBL)
    """
    library = library.upper().strip() if library else '*LIBL'

    if library == '*LIBL':
        return get_user_library_list(username)
    else:
        return [library]


def resolve_library_for_create(library: str, username: str) -> str:
    """
    Resolve a library specification for creating a new object.

    Args:
        library: Library name or '*LIBL'
        username: User whose current library to use for *LIBL

    Returns:
        Single library name where object should be created
    """
    library = library.upper().strip() if library else '*LIBL'

    if library == '*LIBL':
        return get_user_current_library(username)
    else:
        return library


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
                "SELECT value FROM qsys.system_values WHERE name = %s",
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
                UPDATE qsys.system_values
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
                    FROM qsys.system_values
                    WHERE category = %s
                    ORDER BY name
                """, (category.upper(),))
            else:
                cursor.execute("""
                    SELECT name, value, description, category, updated_at, updated_by
                    FROM qsys.system_values
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


def get_system_timezone() -> ZoneInfo:
    """
    Get the system timezone from QTIMZON system value.
    Returns ZoneInfo object for timezone-aware datetime operations.
    Falls back to America/Toronto if timezone is invalid.
    """
    tz_name = get_system_value('QTIMZON', 'America/Toronto')
    try:
        return ZoneInfo(tz_name)
    except Exception:
        logger.warning(f"Invalid timezone {tz_name}, using America/Toronto")
        return ZoneInfo('America/Toronto')


def get_system_datetime() -> datetime:
    """
    Get current datetime in system timezone.
    Uses QTIMZON system value for timezone.
    """
    tz = get_system_timezone()
    return datetime.now(tz)


def get_system_timezone_name() -> str:
    """
    Get the system timezone name for Celery and other configs.
    Returns the IANA timezone string.
    """
    return get_system_value('QTIMZON', 'America/Toronto')


# =============================================================================
# Message Queues (AS/400-style MSGQ)
# =============================================================================

def create_message_queue(name: str, library: str = 'QGPL', description: str = '',
                         delivery: str = '*HOLD', created_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Create a message queue (CRTMSGQ)."""
    name = name.upper().strip()[:10]
    library = library.upper().strip()[:10] if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    if not name:
        return False, "Queue name is required"

    # Ensure library exists
    if not library_exists(library):
        return False, f"Library {library} does not exist"
    _ensure_library_schema(library)

    try:
        with get_cursor() as cursor:
            query = sql.SQL("""
                INSERT INTO {}._msgq (name, text, delivery, created_by)
                VALUES (%s, %s, %s, %s)
            """).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name, description, delivery, created_by))
        return True, f"Message queue {library}/{name} created"
    except psycopg2.IntegrityError:
        return False, f"Message queue {library}/{name} already exists"
    except Exception as e:
        logger.error(f"Failed to create message queue: {e}")
        return False, f"Failed to create message queue: {e}"


def delete_message_queue(name: str, library: str = 'QGPL') -> tuple[bool, str]:
    """Delete a message queue (DLTMSGQ)."""
    name = name.upper().strip()
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    if name in ('QSYSOPR', 'QSYSMSG'):
        return False, f"Cannot delete system queue {name}"

    try:
        with get_cursor() as cursor:
            query = sql.SQL("DELETE FROM {}._msgq WHERE name = %s").format(
                sql.Identifier(lib_schema)
            )
            cursor.execute(query, (name,))
            if cursor.rowcount == 0:
                return False, f"Message queue {library}/{name} not found"
        return True, f"Message queue {library}/{name} deleted"
    except Exception as e:
        logger.error(f"Failed to delete message queue: {e}")
        return False, f"Failed to delete message queue: {e}"


def list_message_queues(library: str = None) -> list[dict]:
    """List message queues (WRKMSGQ).

    If library is specified, queries only that library's _msgq table.
    If library is None, queries all libraries' _msgq tables.
    """
    queues = []

    # Get list of libraries to query
    if library:
        libraries = [library.upper()]
    else:
        libraries = [lib['name'] for lib in list_libraries()]

    try:
        with get_cursor() as cursor:
            for lib in libraries:
                lib_schema = lib.lower().replace('-', '_')

                # Check if library schema and table exist
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = '_msgq'
                    )
                """, (lib_schema,))
                if not cursor.fetchone()['exists']:
                    continue

                query = sql.SQL("SELECT * FROM {}._msgq ORDER BY name").format(
                    sql.Identifier(lib_schema)
                )
                cursor.execute(query)

                for row in cursor.fetchall():
                    # Get message counts from _msg table
                    count_query = sql.SQL("""
                        SELECT
                            COUNT(*) FILTER (WHERE status = '*NEW') as new_count,
                            COUNT(*) as total_count
                        FROM {}._msg WHERE msgq = %s
                    """).format(sql.Identifier(lib_schema))
                    cursor.execute(count_query, (row['name'],))
                    counts = cursor.fetchone()

                    queues.append({
                        'name': row['name'],
                        'library': lib,  # Add library from loop
                        'description': row['text'],  # Column is 'text'
                        'delivery': row.get('delivery', '*HOLD'),
                        'created_by': row['created_by'],
                        'created_at': row['created'],  # Column is 'created'
                        'new_count': counts['new_count'] if counts else 0,
                        'total_count': counts['total_count'] if counts else 0,
                    })

        # Sort by library, then name if we queried multiple libraries
        if not library:
            queues.sort(key=lambda q: (q['library'], q['name']))

    except Exception as e:
        logger.error(f"Failed to list message queues: {e}")
    return queues


def send_message(queue_name: str, library: str = 'QGPL', msg_text: str = '',
                 msg_type: str = '*INFO', msg_id: str = '', severity: int = 0,
                 sent_by: str = 'SYSTEM', msg_data: str = None) -> tuple[bool, str]:
    """Send a message to a queue (SNDMSG)."""
    queue_name = queue_name.upper().strip()
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    try:
        with get_cursor() as cursor:
            # Verify queue exists
            verify_query = sql.SQL("SELECT 1 FROM {}._msgq WHERE name = %s").format(
                sql.Identifier(lib_schema)
            )
            cursor.execute(verify_query, (queue_name,))
            if not cursor.fetchone():
                return False, f"Message queue {library}/{queue_name} not found"

            query = sql.SQL("""
                INSERT INTO {}._msg (msgq, msg_id, msg_type, msg_text, msg_data, severity, sender)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """).format(sql.Identifier(lib_schema))
            cursor.execute(query, (queue_name, msg_id, msg_type, msg_text, msg_data, severity, sent_by))
        return True, "Message sent"
    except Exception as e:
        logger.error(f"Failed to send message: {e}")
        return False, f"Failed to send message: {e}"


def get_messages(queue_name: str, library: str = 'QGPL', status: str = None, limit: int = 50) -> list[dict]:
    """Get messages from a queue (DSPMSG)."""
    queue_name = queue_name.upper().strip()
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')
    messages = []

    try:
        with get_cursor() as cursor:
            if status:
                query = sql.SQL("""
                    SELECT * FROM {}._msg
                    WHERE msgq = %s AND status = %s
                    ORDER BY sent DESC LIMIT %s
                """).format(sql.Identifier(lib_schema))
                cursor.execute(query, (queue_name, status, limit))
            else:
                query = sql.SQL("""
                    SELECT * FROM {}._msg
                    WHERE msgq = %s
                    ORDER BY sent DESC LIMIT %s
                """).format(sql.Identifier(lib_schema))
                cursor.execute(query, (queue_name, limit))

            for row in cursor.fetchall():
                messages.append({
                    'id': row['id'],
                    'queue_name': row['msgq'],
                    'library': library,
                    'msg_id': row['msg_id'],
                    'msg_type': row['msg_type'],
                    'msg_text': row['msg_text'],
                    'msg_data': row['msg_data'],
                    'severity': row['severity'],
                    'sent_by': row['sender'],
                    'sent_at': row['sent'],
                    'status': row['status'],
                })
    except Exception as e:
        logger.error(f"Failed to get messages: {e}")
    return messages


def mark_message_old(message_id: int, library: str = 'QGPL') -> tuple[bool, str]:
    """Mark a message as old/read."""
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    try:
        with get_cursor() as cursor:
            query = sql.SQL(
                "UPDATE {}._msg SET status = '*OLD' WHERE id = %s AND status = '*NEW'"
            ).format(sql.Identifier(lib_schema))
            cursor.execute(query, (message_id,))
        return True, "Message marked as read"
    except Exception as e:
        return False, f"Failed to update message: {e}"


def reply_to_message(message_id: int, library: str = 'QGPL', reply: str = '',
                     replied_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Reply to an inquiry message.

    Note: Reply functionality requires additional columns in _msg table.
    For now, this just marks the message as answered.
    """
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    try:
        with get_cursor() as cursor:
            # Just mark as answered (reply column not in _msg table yet)
            query = sql.SQL("""
                UPDATE {}._msg
                SET status = '*ANSWERED'
                WHERE id = %s AND msg_type = '*INQ'
            """).format(sql.Identifier(lib_schema))
            cursor.execute(query, (message_id,))
            if cursor.rowcount == 0:
                return False, "Message not found or not an inquiry"
        return True, "Reply sent"
    except Exception as e:
        return False, f"Failed to reply: {e}"


def delete_message(message_id: int, library: str = 'QGPL') -> tuple[bool, str]:
    """Delete a message."""
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    try:
        with get_cursor() as cursor:
            query = sql.SQL("DELETE FROM {}._msg WHERE id = %s").format(
                sql.Identifier(lib_schema)
            )
            cursor.execute(query, (message_id,))
        return True, "Message deleted"
    except Exception as e:
        return False, f"Failed to delete message: {e}"


def clear_message_queue(queue_name: str, library: str = 'QGPL') -> tuple[bool, str]:
    """Clear all messages from a queue (CLRMSGQ)."""
    queue_name = queue_name.upper().strip()
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    try:
        with get_cursor() as cursor:
            query = sql.SQL("DELETE FROM {}._msg WHERE msgq = %s").format(
                sql.Identifier(lib_schema)
            )
            cursor.execute(query, (queue_name,))
            count = cursor.rowcount
        return True, f"Cleared {count} messages from {library}/{queue_name}"
    except Exception as e:
        return False, f"Failed to clear queue: {e}"


# =============================================================================
# Data Areas (AS/400-style DTAARA)
# =============================================================================

def create_data_area(name: str, library: str = 'QGPL', type: str = '*CHAR',
                     length: int = 2000, decimal_positions: int = 0,
                     value: str = '', description: str = '',
                     created_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Create a data area (CRTDTAARA)."""
    name = name.upper().strip()[:10]
    library = library.upper().strip()[:10] if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    if not name:
        return False, "Data area name is required"

    if type not in ('*CHAR', '*DEC', '*LGL'):
        return False, "Type must be *CHAR, *DEC, or *LGL"

    # Ensure library exists
    if not library_exists(library):
        return False, f"Library {library} does not exist"
    _ensure_library_schema(library)

    try:
        with get_cursor() as cursor:
            query = sql.SQL("""
                INSERT INTO {}._dtaara (name, type, length, decimal_positions,
                                        value, text, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name, type, length, decimal_positions, value, description, created_by))
        return True, f"Data area {library}/{name} created"
    except psycopg2.IntegrityError:
        return False, f"Data area {library}/{name} already exists"
    except Exception as e:
        logger.error(f"Failed to create data area: {e}")
        return False, f"Failed to create data area: {e}"


def delete_data_area(name: str, library: str = 'QGPL') -> tuple[bool, str]:
    """Delete a data area (DLTDTAARA)."""
    name = name.upper().strip()
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    if name in ('QDATE', 'QTIME'):
        return False, f"Cannot delete system data area {name}"

    try:
        with get_cursor() as cursor:
            query = sql.SQL(
                "DELETE FROM {}._dtaara WHERE name = %s"
            ).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name,))
            if cursor.rowcount == 0:
                return False, f"Data area {library}/{name} not found"
        return True, f"Data area {library}/{name} deleted"
    except Exception as e:
        return False, f"Failed to delete data area: {e}"


def get_data_area(name: str, library: str = 'QGPL') -> dict | None:
    """Get a data area (RTVDTAARA)."""
    name = name.upper().strip()
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    try:
        with get_cursor() as cursor:
            query = sql.SQL(
                "SELECT * FROM {}._dtaara WHERE name = %s"
            ).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name,))
            row = cursor.fetchone()
            if row:
                return {
                    'name': row['name'],
                    'library': library,  # From parameter
                    'type': row['type'],
                    'length': row['length'],
                    'decimal_positions': row['decimal_positions'],
                    'value': row['value'],
                    'description': row['text'],  # Column is 'text'
                    'locked_by': row['locked_by'],
                    'locked_at': row['locked_at'],
                    'created_by': row['created_by'],
                    'created_at': row['created'],  # Column is 'created'
                    'updated_by': row['changed_by'],  # Column is 'changed_by'
                    'updated_at': row['changed'],  # Column is 'changed'
                }
    except Exception as e:
        logger.error(f"Failed to get data area {library}/{name}: {e}")
    return None


def change_data_area(name: str, library: str = 'QGPL', value: str = None,
                     updated_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Change a data area value (CHGDTAARA)."""
    name = name.upper().strip()
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    try:
        with get_cursor() as cursor:
            # Check if locked by another user
            query = sql.SQL(
                "SELECT locked_by FROM {}._dtaara WHERE name = %s"
            ).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name,))
            row = cursor.fetchone()
            if not row:
                return False, f"Data area {library}/{name} not found"

            if row['locked_by'] and row['locked_by'] != updated_by:
                return False, f"Data area locked by {row['locked_by']}"

            query = sql.SQL("""
                UPDATE {}._dtaara
                SET value = %s, changed_by = %s, changed = CURRENT_TIMESTAMP
                WHERE name = %s
            """).format(sql.Identifier(lib_schema))
            cursor.execute(query, (value, updated_by, name))
        return True, f"Data area {library}/{name} changed"
    except Exception as e:
        return False, f"Failed to change data area: {e}"


def lock_data_area(name: str, library: str = 'QGPL', locked_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Lock a data area for exclusive use."""
    name = name.upper().strip()
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    try:
        with get_cursor() as cursor:
            query = sql.SQL(
                "SELECT locked_by FROM {}._dtaara WHERE name = %s"
            ).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name,))
            row = cursor.fetchone()
            if not row:
                return False, f"Data area {library}/{name} not found"

            if row['locked_by'] and row['locked_by'] != locked_by:
                return False, f"Data area already locked by {row['locked_by']}"

            query = sql.SQL("""
                UPDATE {}._dtaara
                SET locked_by = %s, locked_at = CURRENT_TIMESTAMP
                WHERE name = %s
            """).format(sql.Identifier(lib_schema))
            cursor.execute(query, (locked_by, name))
        return True, f"Data area {library}/{name} locked"
    except Exception as e:
        return False, f"Failed to lock data area: {e}"


def unlock_data_area(name: str, library: str = 'QGPL', unlocked_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Unlock a data area."""
    name = name.upper().strip()
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    try:
        with get_cursor() as cursor:
            query = sql.SQL(
                "SELECT locked_by FROM {}._dtaara WHERE name = %s"
            ).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name,))
            row = cursor.fetchone()
            if not row:
                return False, f"Data area {library}/{name} not found"

            # Only the locker or QSECOFR can unlock
            if row['locked_by'] and row['locked_by'] != unlocked_by:
                # Check if unlocked_by is SECOFR
                cursor.execute(
                    "SELECT user_class FROM qsys.users WHERE username = %s",
                    (unlocked_by,)
                )
                user_row = cursor.fetchone()
                if not user_row or user_row['user_class'] != '*SECOFR':
                    return False, f"Data area locked by {row['locked_by']}"

            query = sql.SQL("""
                UPDATE {}._dtaara SET locked_by = NULL, locked_at = NULL
                WHERE name = %s
            """).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name,))
        return True, f"Data area {library}/{name} unlocked"
    except Exception as e:
        return False, f"Failed to unlock data area: {e}"


def list_data_areas(library: str = None) -> list[dict]:
    """List data areas (WRKDTAARA).

    If library is specified, queries only that library's _dtaara table.
    If library is None, queries all libraries' _dtaara tables.
    """
    areas = []

    # Get list of libraries to query
    if library:
        libraries = [library.upper()]
    else:
        libraries = [lib['name'] for lib in list_libraries()]

    try:
        with get_cursor() as cursor:
            for lib in libraries:
                lib_schema = lib.lower().replace('-', '_')

                # Check if library schema and table exist
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = '_dtaara'
                    )
                """, (lib_schema,))
                if not cursor.fetchone()['exists']:
                    continue

                query = sql.SQL("SELECT * FROM {}._dtaara ORDER BY name").format(
                    sql.Identifier(lib_schema)
                )
                cursor.execute(query)

                for row in cursor.fetchall():
                    areas.append({
                        'name': row['name'],
                        'library': lib,  # Add library from loop
                        'type': row['type'],
                        'length': row['length'],
                        'decimal_positions': row['decimal_positions'],
                        'value': row['value'],
                        'description': row['text'],  # Column is 'text'
                        'locked_by': row['locked_by'],
                        'locked_at': row['locked_at'],
                        'created_by': row['created_by'],
                        'created_at': row['created'],  # Column is 'created'
                        'updated_by': row['changed_by'],  # Column is 'changed_by'
                        'updated_at': row['changed'],  # Column is 'changed'
                    })

        # Sort by library, then name if we queried multiple libraries
        if not library:
            areas.sort(key=lambda a: (a['library'], a['name']))

    except Exception as e:
        logger.error(f"Failed to list data areas: {e}")
    return areas


# =============================================================================
# Job Descriptions (AS/400-style JOBD)
# =============================================================================

def create_job_description(name: str, library: str = 'QGPL', description: str = '',
                           job_queue: str = 'QGPL/QBATCH', job_priority: int = 5,
                           output_queue: str = '*USRPRF', user_profile: str = '*RQD',
                           hold_on_jobq: str = '*NO', created_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Create a job description (CRTJOBD)."""
    name = name.upper().strip()[:10]
    library = library.upper().strip()[:10] if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    if not name:
        return False, "Job description name is required"

    if job_priority < 1 or job_priority > 9:
        return False, "Job priority must be 1-9"

    # Ensure library exists
    if not library_exists(library):
        return False, f"Library {library} does not exist"
    _ensure_library_schema(library)

    try:
        with get_cursor() as cursor:
            query = sql.SQL("""
                INSERT INTO {}._jobd (name, text, job_queue,
                                      job_priority, output_queue, user_profile,
                                      hold_on_jobq, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name, description, job_queue, job_priority,
                  output_queue, user_profile, hold_on_jobq, created_by))
        return True, f"Job description {library}/{name} created"
    except psycopg2.IntegrityError:
        return False, f"Job description {library}/{name} already exists"
    except Exception as e:
        return False, f"Failed to create job description: {e}"


def delete_job_description(name: str, library: str = 'QGPL') -> tuple[bool, str]:
    """Delete a job description (DLTJOBD)."""
    name = name.upper().strip()
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    if name in ('QBATCH', 'QINTER', 'QSPL'):
        return False, f"Cannot delete system job description {name}"

    try:
        with get_cursor() as cursor:
            query = sql.SQL(
                "DELETE FROM {}._jobd WHERE name = %s"
            ).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name,))
            if cursor.rowcount == 0:
                return False, f"Job description {library}/{name} not found"
        return True, f"Job description {library}/{name} deleted"
    except Exception as e:
        return False, f"Failed to delete job description: {e}"


def get_job_description(name: str, library: str = 'QGPL') -> dict | None:
    """Get a job description."""
    name = name.upper().strip()
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    try:
        with get_cursor() as cursor:
            query = sql.SQL(
                "SELECT * FROM {}._jobd WHERE name = %s"
            ).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name,))
            row = cursor.fetchone()
            if row:
                return {
                    'name': row['name'],
                    'library': library,  # From parameter
                    'description': row['text'],  # Column is 'text'
                    'job_queue': row['job_queue'],
                    'job_priority': row['job_priority'],
                    'output_queue': row['output_queue'],
                    'user_profile': row['user_profile'],
                    'hold_on_jobq': row['hold_on_jobq'],
                    'log_level': row.get('log_level', 4),
                    'log_severity': row.get('log_severity', 20),
                    'created_by': row['created_by'],
                    'created_at': row['created'],  # Column is 'created'
                }
    except Exception as e:
        logger.error(f"Failed to get job description {library}/{name}: {e}")
    return None


def list_job_descriptions(library: str = None) -> list[dict]:
    """List job descriptions (WRKJOBD).

    If library is specified, queries only that library's _jobd table.
    If library is None, queries all libraries' _jobd tables.
    """
    jobds = []

    # Get list of libraries to query
    if library:
        libraries = [library.upper()]
    else:
        libraries = [lib['name'] for lib in list_libraries()]

    try:
        with get_cursor() as cursor:
            for lib in libraries:
                lib_schema = lib.lower().replace('-', '_')

                # Check if library schema and table exist
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = '_jobd'
                    )
                """, (lib_schema,))
                if not cursor.fetchone()['exists']:
                    continue

                query = sql.SQL("SELECT * FROM {}._jobd ORDER BY name").format(
                    sql.Identifier(lib_schema)
                )
                cursor.execute(query)

                for row in cursor.fetchall():
                    jobds.append({
                        'name': row['name'],
                        'library': lib,  # Add library from loop
                        'description': row['text'],  # Column is 'text'
                        'job_queue': row['job_queue'],
                        'job_priority': row['job_priority'],
                        'output_queue': row['output_queue'],
                        'user_profile': row['user_profile'],
                        'hold_on_jobq': row['hold_on_jobq'],
                        'created_by': row['created_by'],
                        'created_at': row['created'],  # Column is 'created'
                    })

        # Sort by library, then name if we queried multiple libraries
        if not library:
            jobds.sort(key=lambda j: (j['library'], j['name']))

    except Exception as e:
        logger.error(f"Failed to list job descriptions: {e}")
    return jobds


def change_job_description(name: str, library: str = 'QGPL', **kwargs) -> tuple[bool, str]:
    """Change a job description (CHGJOBD)."""
    name = name.upper().strip()
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    # Map API field names to DB column names
    field_mapping = {
        'description': 'text',  # API uses 'description', DB uses 'text'
        'job_queue': 'job_queue',
        'job_priority': 'job_priority',
        'output_queue': 'output_queue',
        'user_profile': 'user_profile',
        'hold_on_jobq': 'hold_on_jobq',
        'log_level': 'log_level',
        'log_severity': 'log_severity'
    }

    updates = []
    values = []
    for api_field, db_field in field_mapping.items():
        if api_field in kwargs and kwargs[api_field] is not None:
            updates.append(f"{db_field} = %s")
            values.append(kwargs[api_field])

    if not updates:
        return False, "No changes specified"

    values.append(name)

    try:
        with get_cursor() as cursor:
            query = sql.SQL("UPDATE {}._jobd SET {} WHERE name = %s").format(
                sql.Identifier(lib_schema),
                sql.SQL(', ').join([sql.SQL(u) for u in updates])
            )
            cursor.execute(query, values)
            if cursor.rowcount == 0:
                return False, f"Job description {library}/{name} not found"
        return True, f"Job description {library}/{name} changed"
    except Exception as e:
        return False, f"Failed to change job description: {e}"


# =============================================================================
# Output Queues and Spooled Files (AS/400-style OUTQ/SPLF)
# =============================================================================

def create_output_queue(name: str, library: str = 'QGPL', description: str = '',
                        created_by: str = 'SYSTEM') -> tuple[bool, str]:
    """Create an output queue (CRTOUTQ)."""
    name = name.upper().strip()[:10]
    library = library.upper().strip()[:10] if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    if not name:
        return False, "Output queue name is required"

    # Ensure library exists
    if not library_exists(library):
        return False, f"Library {library} does not exist"
    _ensure_library_schema(library)

    try:
        with get_cursor() as cursor:
            query = sql.SQL("""
                INSERT INTO {}._outq (name, text, created_by)
                VALUES (%s, %s, %s)
            """).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name, description, created_by))
        return True, f"Output queue {library}/{name} created"
    except psycopg2.IntegrityError:
        return False, f"Output queue {library}/{name} already exists"
    except Exception as e:
        return False, f"Failed to create output queue: {e}"


def delete_output_queue(name: str, library: str = 'QGPL') -> tuple[bool, str]:
    """Delete an output queue (DLTOUTQ)."""
    name = name.upper().strip()
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    if name in ('QPRINT', 'QPRINT2'):
        return False, f"Cannot delete system output queue {name}"

    try:
        with get_cursor() as cursor:
            # Check for spooled files (spooled_files table stays centralized)
            cursor.execute(
                "SELECT COUNT(*) as cnt FROM qsys._splf WHERE output_queue = %s AND output_queue_lib = %s",
                (name, library)
            )
            result = cursor.fetchone()
            if result and result['cnt'] > 0:
                return False, f"Output queue {library}/{name} contains spooled files"

            query = sql.SQL(
                "DELETE FROM {}._outq WHERE name = %s"
            ).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name,))
            if cursor.rowcount == 0:
                return False, f"Output queue {library}/{name} not found"
        return True, f"Output queue {library}/{name} deleted"
    except Exception as e:
        return False, f"Failed to delete output queue: {e}"


def list_output_queues(library: str = None) -> list[dict]:
    """List output queues (WRKOUTQ).

    If library is specified, queries only that library's _outq table.
    If library is None, queries all libraries' _outq tables.
    """
    queues = []

    # Get list of libraries to query
    if library:
        libraries = [library.upper()]
    else:
        libraries = [lib['name'] for lib in list_libraries()]

    try:
        with get_cursor() as cursor:
            for lib in libraries:
                lib_schema = lib.lower().replace('-', '_')

                # Check if library schema and table exist
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = '_outq'
                    )
                """, (lib_schema,))
                if not cursor.fetchone()['exists']:
                    continue

                query = sql.SQL("SELECT * FROM {}._outq ORDER BY name").format(
                    sql.Identifier(lib_schema)
                )
                cursor.execute(query)

                for row in cursor.fetchall():
                    # Get spooled file count (centralized table)
                    cursor.execute(
                        "SELECT COUNT(*) as cnt FROM qsys._splf WHERE output_queue = %s AND output_queue_lib = %s",
                        (row['name'], lib)
                    )
                    cnt_result = cursor.fetchone()
                    file_count = cnt_result['cnt'] if cnt_result else 0

                    queues.append({
                        'name': row['name'],
                        'library': lib,  # Add library from loop
                        'description': row['text'],  # Column is 'text'
                        'status': row.get('status', '*RLS'),
                        'created_by': row['created_by'],
                        'created_at': row['created'],  # Column is 'created'
                        'file_count': file_count,
                    })

        # Sort by library, then name if we queried multiple libraries
        if not library:
            queues.sort(key=lambda q: (q['library'], q['name']))

    except Exception as e:
        logger.error(f"Failed to list output queues: {e}")
    return queues


def hold_output_queue(name: str, library: str = 'QGPL') -> tuple[bool, str]:
    """Hold an output queue (HLDOUTQ)."""
    name = name.upper().strip()
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    try:
        with get_cursor() as cursor:
            query = sql.SQL("""
                UPDATE {}._outq SET status = '*HLD'
                WHERE name = %s
            """).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name,))
            if cursor.rowcount == 0:
                return False, f"Output queue {library}/{name} not found"
        return True, f"Output queue {library}/{name} held"
    except Exception as e:
        return False, f"Failed to hold output queue: {e}"


def release_output_queue(name: str, library: str = 'QGPL') -> tuple[bool, str]:
    """Release an output queue (RLSOUTQ)."""
    name = name.upper().strip()
    library = library.upper().strip() if library and library != '*LIBL' else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    try:
        with get_cursor() as cursor:
            query = sql.SQL("""
                UPDATE {}._outq SET status = '*RLS'
                WHERE name = %s
            """).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name,))
            if cursor.rowcount == 0:
                return False, f"Output queue {library}/{name} not found"
        return True, f"Output queue {library}/{name} released"
    except Exception as e:
        return False, f"Failed to release output queue: {e}"


def create_spooled_file(name: str, job_name: str, content: str, job_id: str = None,
                        output_queue: str = 'QPRINT', output_queue_lib: str = 'QGPL',
                        user_data: str = '', created_by: str = 'SYSTEM') -> tuple[bool, str, int]:
    """Create a spooled file (job output)."""
    name = name.upper().strip()[:10]
    output_queue_lib = output_queue_lib.upper().strip() if output_queue_lib else 'QGPL'

    try:
        with get_cursor() as cursor:
            # Get next file number for this job
            cursor.execute(
                "SELECT COALESCE(MAX(file_number), 0) + 1 as next_num FROM qsys._splf WHERE job_name = %s",
                (job_name,)
            )
            file_number = cursor.fetchone()['next_num']

            # Count pages (lines / 60)
            pages = max(1, len(content.split('\n')) // 60)
            total_records = len(content.split('\n'))

            cursor.execute("""
                INSERT INTO qsys._splf (name, file_number, job_name, job_id, output_queue,
                                           output_queue_lib, user_data, pages, total_records, content, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (name, file_number, job_name, job_id, output_queue, output_queue_lib, user_data,
                  pages, total_records, content, created_by))
            splf_id = cursor.fetchone()['id']
        return True, f"Spooled file {name} created", splf_id
    except Exception as e:
        return False, f"Failed to create spooled file: {e}", 0


def get_spooled_file(splf_id: int) -> dict | None:
    """Get a spooled file by ID."""
    try:
        with get_cursor() as cursor:
            cursor.execute("SELECT * FROM qsys._splf WHERE id = %s", (splf_id,))
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
            query = "SELECT * FROM qsys._splf WHERE 1=1"
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
            cursor.execute("DELETE FROM qsys._splf WHERE id = %s", (splf_id,))
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
                "UPDATE qsys._splf SET status = '*HLD' WHERE id = %s",
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
                "UPDATE qsys._splf SET status = '*RDY' WHERE id = %s",
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
                    FROM qsys._cmd
                    WHERE COMMAND_NAME LIKE %s
                    ORDER BY COMMAND_NAME
                """, (f"{filter_prefix}%",))
            else:
                cursor.execute("""
                    SELECT COMMAND_NAME, COMMAND_LIBRARY, TEXT_DESCRIPTION, SCREEN_NAME
                    FROM qsys._cmd
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
                SELECT * FROM qsys._cmd WHERE COMMAND_NAME = %s
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
                SELECT * FROM qsys._cmdparm
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
                FROM qsys._prmval
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
                INSERT INTO qsys._cmd (COMMAND_NAME, COMMAND_LIBRARY, TEXT_DESCRIPTION,
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
                INSERT INTO qsys._cmdparm
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
                INSERT INTO qsys._prmval
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


# =============================================================================
# Query Definitions (AS/400 Query/400 - WRKQRY)
# =============================================================================

# Operators for WHERE clause building
QUERY_OPERATORS = {
    'EQ': ('=', 'Equal'),
    'NE': ('<>', 'Not Equal'),
    'GT': ('>', 'Greater Than'),
    'LT': ('<', 'Less Than'),
    'GE': ('>=', 'Greater/Equal'),
    'LE': ('<=', 'Less/Equal'),
    'CT': ('LIKE', 'Contains'),       # Wraps value in %...%
    'SW': ('LIKE', 'Starts With'),    # Appends % to value
    'EW': ('LIKE', 'Ends With'),      # Prepends % to value
    'NL': ('IS NULL', 'Is Null'),
    'NN': ('IS NOT NULL', 'Not Null'),
}

# Aggregate functions for summary queries (Query/400 style)
AGGREGATE_FUNCTIONS = {
    '': ('', 'None'),                 # No aggregate
    'COUNT': ('COUNT', 'Count'),
    'SUM': ('SUM', 'Sum'),
    'AVG': ('AVG', 'Average'),
    'MIN': ('MIN', 'Minimum'),
    'MAX': ('MAX', 'Maximum'),
}


def list_table_columns(schema_name: str, table_name: str) -> list[dict]:
    """
    List all columns in a table with metadata.
    Used by Query/400 for column selection.
    """
    columns = []
    try:
        with get_cursor() as cursor:
            cursor.execute("""
                SELECT
                    column_name,
                    data_type,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema_name.lower(), table_name.lower()))

            for row in cursor.fetchall():
                columns.append({
                    'name': row['column_name'].upper(),
                    'data_type': row['data_type'],
                    'max_length': row['character_maximum_length'],
                    'precision': row['numeric_precision'],
                    'scale': row['numeric_scale'],
                    'nullable': row['is_nullable'] == 'YES',
                    'default': row['column_default'],
                    'ordinal': row['ordinal_position'],
                })
    except Exception as e:
        logger.error(f"Failed to list columns for {schema_name}.{table_name}: {e}")

    return columns


def create_query_definition(
    name: str,
    library: str = 'QGPL',
    description: str = '',
    source_schema: str = None,
    source_table: str = None,
    selected_columns: list = None,
    where_conditions: list = None,
    order_by_fields: list = None,
    summary_functions: list = None,
    group_by_fields: list = None,
    output_type: str = '*DISPLAY',
    row_limit: int = 0,
    created_by: str = 'SYSTEM'
) -> tuple[bool, str]:
    """Create a new query definition (CRTQRY equivalent)."""
    import json

    name = name.upper().strip()[:10]
    library = library.upper().strip()[:10] if library else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    if not name:
        return False, "Query name is required"

    # Ensure library schema exists
    if not library_exists(library):
        return False, f"Library {library} does not exist"
    _ensure_library_schema(library)

    try:
        with get_cursor() as cursor:
            # Use psycopg2.sql for safe schema/table reference
            query = sql.SQL("""
                INSERT INTO {}._qrydfn (
                    name, text, source_schema, source_table,
                    selected_columns, where_conditions, order_by_fields,
                    summary_functions, group_by_fields,
                    output_type, row_limit, created_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """).format(sql.Identifier(lib_schema))

            cursor.execute(query, (
                name, description,
                source_schema.lower() if source_schema else None,
                source_table.lower() if source_table else None,
                json.dumps(selected_columns or []),
                json.dumps(where_conditions or []),
                json.dumps(order_by_fields or []),
                json.dumps(summary_functions or []),
                json.dumps(group_by_fields or []),
                output_type, row_limit, created_by
            ))

        return True, f"Query {library}/{name} created"
    except psycopg2.IntegrityError:
        return False, f"Query {library}/{name} already exists"
    except Exception as e:
        logger.error(f"Failed to create query {name}: {e}")
        return False, f"Failed to create query: {e}"


def get_query_definition(name: str, library: str = 'QGPL') -> dict | None:
    """Get a query definition by name."""
    name = name.upper().strip()
    library = library.upper().strip() if library else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    try:
        with get_cursor() as cursor:
            query = sql.SQL(
                "SELECT * FROM {}._qrydfn WHERE name = %s"
            ).format(sql.Identifier(lib_schema))

            cursor.execute(query, (name,))
            row = cursor.fetchone()
            if row:
                return {
                    'name': row['name'],
                    'library': library,  # From parameter, not stored in table
                    'description': row['text'],  # Column is 'text' in library schema
                    'source_schema': row['source_schema'],
                    'source_table': row['source_table'],
                    'selected_columns': row['selected_columns'] or [],
                    'where_conditions': row['where_conditions'] or [],
                    'order_by_fields': row['order_by_fields'] or [],
                    'summary_functions': row['summary_functions'] or [],
                    'group_by_fields': row['group_by_fields'] or [],
                    'output_type': row['output_type'],
                    'row_limit': row['row_limit'],
                    'created_by': row['created_by'],
                    'created_at': str(row['created']) if row['created'] else '',
                    'updated_by': row['changed_by'],
                    'updated_at': str(row['changed']) if row['changed'] else '',
                    'last_run_at': str(row['last_run']) if row['last_run'] else '',
                }
    except Exception as e:
        logger.error(f"Failed to get query {library}/{name}: {e}")

    return None


def update_query_definition(
    name: str,
    library: str = 'QGPL',
    updated_by: str = 'SYSTEM',
    **kwargs
) -> tuple[bool, str]:
    """Update an existing query definition."""
    import json

    name = name.upper().strip()
    library = library.upper().strip() if library else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    if not get_query_definition(name, library):
        return False, f"Query {library}/{name} not found"

    # Build dynamic update - map API field names to DB column names
    field_mapping = {
        'description': 'text',  # API uses 'description', DB uses 'text'
        'source_schema': 'source_schema',
        'source_table': 'source_table',
        'selected_columns': 'selected_columns',
        'where_conditions': 'where_conditions',
        'order_by_fields': 'order_by_fields',
        'summary_functions': 'summary_functions',
        'group_by_fields': 'group_by_fields',
        'output_type': 'output_type',
        'row_limit': 'row_limit'
    }

    # Fields that need JSON serialization
    json_fields = ('selected_columns', 'where_conditions', 'order_by_fields',
                   'summary_functions', 'group_by_fields')

    updates = []
    values = []

    for api_field, db_field in field_mapping.items():
        if api_field in kwargs:
            val = kwargs[api_field]
            if api_field in json_fields:
                val = json.dumps(val or [])
            elif api_field in ('source_schema', 'source_table') and val:
                val = val.lower()
            updates.append(f"{db_field} = %s")
            values.append(val)

    if not updates:
        return False, "No fields to update"

    updates.append("changed_by = %s")
    values.append(updated_by)
    updates.append("changed = CURRENT_TIMESTAMP")

    values.append(name)

    try:
        with get_cursor() as cursor:
            # Build query with safe schema reference
            query = sql.SQL("UPDATE {}._qrydfn SET {} WHERE name = %s").format(
                sql.Identifier(lib_schema),
                sql.SQL(', ').join([sql.SQL(u) for u in updates])
            )
            cursor.execute(query, values)

        return True, f"Query {library}/{name} updated"
    except Exception as e:
        logger.error(f"Failed to update query {library}/{name}: {e}")
        return False, f"Failed to update query: {e}"


def delete_query_definition(name: str, library: str = 'QGPL') -> tuple[bool, str]:
    """Delete a query definition (DLTQRY equivalent)."""
    name = name.upper().strip()
    library = library.upper().strip() if library else 'QGPL'
    lib_schema = library.lower().replace('-', '_')

    if not get_query_definition(name, library):
        return False, f"Query {library}/{name} not found"

    try:
        with get_cursor() as cursor:
            query = sql.SQL(
                "DELETE FROM {}._qrydfn WHERE name = %s"
            ).format(sql.Identifier(lib_schema))
            cursor.execute(query, (name,))

        return True, f"Query {library}/{name} deleted"
    except Exception as e:
        logger.error(f"Failed to delete query {library}/{name}: {e}")
        return False, f"Failed to delete query: {e}"


def list_query_definitions(library: str = None, created_by: str = None, username: str = None) -> list[dict]:
    """List query definitions with optional filters.

    Args:
        library: Library to search. Can be:
            - None: Search all libraries
            - '*LIBL': Search user's library list (requires username)
            - Specific name: Search only that library
        created_by: Filter by creator username
        username: User for *LIBL resolution

    Returns:
        List of query definition dicts
    """
    queries = []

    # Get list of libraries to query
    if library:
        library = library.upper().strip()
        if library == '*LIBL' and username:
            # Resolve *LIBL to user's library list
            libraries = get_user_library_list(username)
        else:
            libraries = [library]
    else:
        libraries = [lib['name'] for lib in list_libraries()]

    try:
        with get_cursor() as cursor:
            for lib in libraries:
                lib_schema = lib.lower().replace('-', '_')

                # Check if library schema and table exist
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = '_qrydfn'
                    )
                """, (lib_schema,))
                if not cursor.fetchone()['exists']:
                    continue

                # Build query for this library
                query = sql.SQL("SELECT * FROM {}._qrydfn WHERE 1=1").format(
                    sql.Identifier(lib_schema)
                )
                params = []

                if created_by:
                    query = sql.SQL("{} AND created_by = %s").format(query)
                    params.append(created_by.upper())

                query = sql.SQL("{} ORDER BY name").format(query)

                cursor.execute(query, params)

                for row in cursor.fetchall():
                    queries.append({
                        'name': row['name'],
                        'library': lib,  # Add library from loop
                        'description': row['text'],  # Column is 'text'
                        'source_schema': row['source_schema'],
                        'source_table': row['source_table'],
                        'created_by': row['created_by'],
                        'created_at': str(row['created']) if row['created'] else '',
                        'last_run_at': str(row['last_run']) if row['last_run'] else '',
                    })

        # Sort by library, then name if we queried multiple libraries
        if not library:
            queries.sort(key=lambda q: (q['library'], q['name']))

    except Exception as e:
        logger.error(f"Failed to list queries: {e}")

    return queries


def build_query_sql(
    schema: str,
    table: str,
    columns: list = None,
    conditions: list = None,
    order_by: list = None,
    summary_functions: list = None,
    group_by_fields: list = None,
    limit: int = 100,
    offset: int = 0
) -> tuple:
    """
    Build a parameterized SQL query from query definition components.
    Uses psycopg2.sql module for safe identifier handling.

    Supports:
    - Column selection with aliases
    - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
    - GROUP BY clause
    - WHERE conditions
    - ORDER BY clause

    Returns: (sql_composed_object, params_list)
    """
    from psycopg2 import sql as psql

    # Build table reference
    table_ref = psql.SQL("{}.{}").format(
        psql.Identifier(schema.lower()),
        psql.Identifier(table.lower())
    )

    # Build summary function lookup: column_name -> function
    summary_map = {}
    if summary_functions:
        for sf in summary_functions:
            col = sf.get('column', '').lower()
            func = sf.get('function', '').upper()
            if col and func and func in AGGREGATE_FUNCTIONS:
                summary_map[col] = func

    # Build column list with aggregates and aliases
    if columns and len(columns) > 0:
        col_parts = []
        for c in sorted(columns, key=lambda x: x.get('seq', 0)):
            col_name = c['name'].lower()
            alias = c.get('alias', '').strip()
            func = c.get('function', '').upper() or summary_map.get(col_name, '')

            if func and func in AGGREGATE_FUNCTIONS and func != '':
                # Apply aggregate function
                if alias:
                    col_expr = psql.SQL("{}({}) AS {}").format(
                        psql.SQL(func),
                        psql.Identifier(col_name),
                        psql.Identifier(alias)
                    )
                else:
                    # Auto-alias: e.g., SUM(amount) AS sum_amount
                    auto_alias = f"{func.lower()}_{col_name}"
                    col_expr = psql.SQL("{}({}) AS {}").format(
                        psql.SQL(func),
                        psql.Identifier(col_name),
                        psql.Identifier(auto_alias)
                    )
            elif alias:
                # Column with alias, no aggregate
                col_expr = psql.SQL("{} AS {}").format(
                    psql.Identifier(col_name),
                    psql.Identifier(alias)
                )
            else:
                # Plain column
                col_expr = psql.Identifier(col_name)
            col_parts.append(col_expr)
        col_list = psql.SQL(", ").join(col_parts)
    else:
        col_list = psql.SQL("*")

    # Start building query
    query = psql.SQL("SELECT {} FROM {}").format(col_list, table_ref)

    # Build WHERE clause
    params = []
    if conditions and len(conditions) > 0:
        where_parts = []
        for i, cond in enumerate(conditions):
            col = psql.Identifier(cond['field'].lower())
            op_code = cond.get('op', 'EQ')
            op_sql, _ = QUERY_OPERATORS.get(op_code, ('=', 'Equal'))
            value = cond.get('value', '')

            if op_code == 'NL':
                where_parts.append(psql.SQL("{} IS NULL").format(col))
            elif op_code == 'NN':
                where_parts.append(psql.SQL("{} IS NOT NULL").format(col))
            elif op_code == 'CT':
                where_parts.append(psql.SQL("{} LIKE %s").format(col))
                params.append(f"%{value}%")
            elif op_code == 'SW':
                where_parts.append(psql.SQL("{} LIKE %s").format(col))
                params.append(f"{value}%")
            elif op_code == 'EW':
                where_parts.append(psql.SQL("{} LIKE %s").format(col))
                params.append(f"%{value}")
            else:
                where_parts.append(psql.SQL("{} {} %s").format(col, psql.SQL(op_sql)))
                params.append(value)

        # Join with AND/OR connectors
        if len(where_parts) == 1:
            query = query + psql.SQL(" WHERE ") + where_parts[0]
        else:
            # Build with connectors
            where_sql = where_parts[0]
            for i in range(1, len(where_parts)):
                connector = conditions[i - 1].get('and_or', 'AND').upper()
                if connector not in ('AND', 'OR'):
                    connector = 'AND'
                where_sql = where_sql + psql.SQL(" {} ").format(psql.SQL(connector)) + where_parts[i]
            query = query + psql.SQL(" WHERE ") + where_sql

    # Build GROUP BY clause (required when using aggregate functions)
    if group_by_fields and len(group_by_fields) > 0:
        group_parts = [psql.Identifier(f.lower()) for f in group_by_fields]
        query = query + psql.SQL(" GROUP BY ") + psql.SQL(", ").join(group_parts)

    # Build ORDER BY clause
    if order_by and len(order_by) > 0:
        order_parts = []
        for o in sorted(order_by, key=lambda x: x.get('seq', 0)):
            direction = 'DESC' if o.get('dir', 'ASC').upper() == 'DESC' else 'ASC'
            order_parts.append(
                psql.SQL("{} {}").format(
                    psql.Identifier(o['field'].lower()),
                    psql.SQL(direction)
                )
            )
        query = query + psql.SQL(" ORDER BY ") + psql.SQL(", ").join(order_parts)

    # Add LIMIT/OFFSET
    if limit > 0:
        query = query + psql.SQL(" LIMIT %s OFFSET %s")
        params.extend([limit, offset])

    return query, params


def execute_query_definition(
    name: str,
    library: str = 'QGPL',
    limit: int = 100,
    offset: int = 0
) -> tuple[bool, list | str, list]:
    """
    Execute a saved query definition.

    Returns: (success, rows_or_error_message, column_names)
    """
    qry = get_query_definition(name, library)
    if not qry:
        return False, f"Query {library}/{name} not found", []

    if not qry['source_schema'] or not qry['source_table']:
        return False, "Query has no table specified", []

    try:
        query_sql, params = build_query_sql(
            schema=qry['source_schema'],
            table=qry['source_table'],
            columns=qry['selected_columns'],
            conditions=qry['where_conditions'],
            order_by=qry['order_by_fields'],
            summary_functions=qry.get('summary_functions'),
            group_by_fields=qry.get('group_by_fields'),
            limit=qry['row_limit'] if qry['row_limit'] > 0 else limit,
            offset=offset
        )

        with get_cursor() as cursor:
            cursor.execute(query_sql, params)
            rows = cursor.fetchall()

            # Get column names from cursor description
            col_names = [desc[0].upper() for desc in cursor.description] if cursor.description else []

            # Update last_run_at in the library's _qrydfn table
            try:
                cursor.execute(f"""
                    UPDATE {library.lower()}._qrydfn
                    SET last_run = CURRENT_TIMESTAMP
                    WHERE name = %s
                """, (name.upper(),))
            except Exception:
                pass  # Ignore if table doesn't exist

        # Convert dict keys to uppercase to match col_names
        return True, [{k.upper(): v for k, v in dict(row).items()} for row in rows], col_names

    except Exception as e:
        logger.error(f"Failed to execute query {name}: {e}")
        return False, str(e), []


def run_adhoc_query(
    schema: str,
    table: str,
    columns: list = None,
    conditions: list = None,
    order_by: list = None,
    summary_functions: list = None,
    group_by_fields: list = None,
    limit: int = 100,
    offset: int = 0
) -> tuple[bool, list | str, list]:
    """
    Run an ad-hoc query without saving definition.
    Used for query preview (F5) before saving.

    Returns: (success, rows_or_error_message, column_names)
    """
    if not schema or not table:
        return False, "Schema and table are required", []

    try:
        query_sql, params = build_query_sql(
            schema=schema,
            table=table,
            columns=columns,
            conditions=conditions,
            order_by=order_by,
            summary_functions=summary_functions,
            group_by_fields=group_by_fields,
            limit=limit,
            offset=offset
        )

        with get_cursor() as cursor:
            cursor.execute(query_sql, params)
            rows = cursor.fetchall()

            col_names = [desc[0].upper() for desc in cursor.description] if cursor.description else []

        # Convert dict keys to uppercase to match col_names
        return True, [{k.upper(): v for k, v in dict(row).items()} for row in rows], col_names

    except Exception as e:
        logger.error(f"Failed to run ad-hoc query: {e}")
        return False, str(e), []
