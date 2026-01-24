"""Configuration for fixer package.

All configuration is loaded from environment variables.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class FixerConfig:
    """Fixer configuration loaded from environment."""

    # Database
    db_host: str = "dk400-postgres"
    db_port: int = 5432
    db_name: str = "dk400"
    db_user: str = "fixer"
    db_password: str = ""

    # API Keys
    anthropic_api_key: str = ""

    # NetBox
    netbox_url: str = "http://192.168.20.19:8000"
    netbox_token: str = ""

    # Notifications
    comms_url: str = "http://192.168.20.19:3500/api/send"
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # Runbooks
    runbook_dir: str = "/app/runbooks"

    @classmethod
    def from_env(cls) -> "FixerConfig":
        """Load configuration from environment variables."""
        return cls(
            db_host=os.environ.get("FIXER_DB_HOST", "dk400-postgres"),
            db_port=int(os.environ.get("FIXER_DB_PORT", "5432")),
            db_name=os.environ.get("FIXER_DB_NAME", "dk400"),
            db_user=os.environ.get("FIXER_DB_USER", "fixer"),
            db_password=os.environ.get("FIXER_DB_PASSWORD", ""),
            anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
            netbox_url=os.environ.get("NETBOX_URL", "http://192.168.20.19:8000"),
            netbox_token=os.environ.get("NETBOX_TOKEN", ""),
            comms_url=os.environ.get("COMMS_URL", "http://192.168.20.19:3500/api/send"),
            telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", ""),
            telegram_chat_id=os.environ.get("TELEGRAM_CHAT_ID", ""),
            runbook_dir=os.environ.get("RUNBOOK_DIR", "/app/runbooks"),
        )


# Global config instance
config = FixerConfig.from_env()
