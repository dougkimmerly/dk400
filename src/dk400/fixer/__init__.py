"""DK/400 Fixer - Issue tracking and remediation toolkit.

This package provides issue tracking, remediation, and notification
capabilities that can be imported directly into DK/400 Celery tasks.

Usage:
    from dk400.fixer import report_issue, attempt_remediation, send_telegram

    # Report an issue and attempt auto-remediation
    result = await report_issue(
        issue_type='container_down',
        target='nginx',
        error_message='Container not running',
        auto_remediate=True,
    )

    # Send a notification
    await send_telegram("Service restored", target="nginx")
"""

from .issues import report_issue, resolve_issue, get_issue, get_open_issues
from .actions import attempt_remediation, execute_runbook
from .reasoning import ask_claude
from .notifications import send_telegram
from .database import get_pool, close_pool

__version__ = "1.0.0"

__all__ = [
    # Issues
    "report_issue",
    "resolve_issue",
    "get_issue",
    "get_open_issues",
    # Actions
    "attempt_remediation",
    "execute_runbook",
    # Reasoning
    "ask_claude",
    # Notifications
    "send_telegram",
    # Database
    "get_pool",
    "close_pool",
]
