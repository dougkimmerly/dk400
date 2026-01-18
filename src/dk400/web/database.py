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
