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
