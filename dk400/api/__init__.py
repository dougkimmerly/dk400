"""API - External access to programs.

Thin layer that allows external systems to call programs via HTTP.
"""

from .main import app

__all__ = ["app"]
