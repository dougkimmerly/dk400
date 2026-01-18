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
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_signon TIMESTAMP,
    signon_attempts INTEGER DEFAULT 0,
    password_expires VARCHAR(10) DEFAULT '*NOMAX'
);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);

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
