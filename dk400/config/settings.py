"""dk400 Configuration.

All configuration comes from environment variables.
Platform only — deployment-specific settings belong in programs.
"""

import os
from dataclasses import dataclass


@dataclass
class Settings:
    """Platform settings from environment."""

    # Database
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql://dk400:dk400@localhost:5432/dk400"
    )

    # Redis (for Celery)
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # API
    api_port: int = int(os.getenv("API_PORT", "8400"))

    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()
