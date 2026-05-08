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

    # NetBox (used cross-cuttingly by docker_sync, netalertx_sync,
    # netbox_maint, audit_netbox_duplicates, service_reconciliation).
    # Programs were already reading settings.netbox_url/token; without
    # these declarations they hit AttributeError. Pragmatic exception
    # to "platform only" — every dk400 instance talks to a NetBox.
    netbox_url: str = os.getenv("NETBOX_URL", "")
    netbox_token: str = os.getenv("NETBOX_TOKEN", "")


settings = Settings()
