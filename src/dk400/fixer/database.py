"""Database connection pool for fixer.

Uses asyncpg for PostgreSQL connections.
All fixer data is stored in the 'fixer' schema within the dk400 database.
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)

# Connection pool (singleton)
_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    """Get or create the database connection pool.

    Environment variables:
        FIXER_DB_HOST: PostgreSQL host (default: dk400-postgres)
        FIXER_DB_PORT: PostgreSQL port (default: 5432)
        FIXER_DB_NAME: Database name (default: dk400)
        FIXER_DB_USER: Database user (default: fixer)
        FIXER_DB_PASSWORD: Database password
    """
    global _pool
    if _pool is None:
        host = os.environ.get("FIXER_DB_HOST", "dk400-postgres")
        port = int(os.environ.get("FIXER_DB_PORT", "5432"))
        database = os.environ.get("FIXER_DB_NAME", "dk400")
        user = os.environ.get("FIXER_DB_USER", "fixer")
        password = os.environ.get("FIXER_DB_PASSWORD", "")

        logger.info(f"Creating PostgreSQL connection pool to {host}:{port}/{database}")

        _pool = await asyncpg.create_pool(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            min_size=2,
            max_size=10,
            command_timeout=60,
        )

        # Set search path to fixer schema
        async with _pool.acquire() as conn:
            await conn.execute("SET search_path TO fixer, public")

    return _pool


async def close_pool():
    """Close the database connection pool."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Database connection pool closed")


@asynccontextmanager
async def get_connection():
    """Get a connection from the pool with fixer schema set."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("SET search_path TO fixer, public")
        yield conn
