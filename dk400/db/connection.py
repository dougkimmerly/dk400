"""Database connection management."""

import logging
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg

from dk400.config import settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def get_connection():
    """
    Get a database connection.

    Creates a fresh connection each time - safer for Celery workers
    where a corrupted connection could poison the whole pool.
    """
    conn = None
    try:
        conn = await asyncpg.connect(settings.database_url)
        yield conn
    finally:
        if conn:
            await conn.close()


# Legacy alias for compatibility
async def pool():
    """Legacy - just returns None, use get_connection() instead."""
    return None


async def close_pool():
    """Legacy - no-op, connections close automatically."""
    pass
