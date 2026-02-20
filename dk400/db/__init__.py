"""Database module."""

from .connection import get_connection, pool

__all__ = ["get_connection", "pool"]
