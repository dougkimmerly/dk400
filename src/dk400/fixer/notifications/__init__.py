"""Notification module.

Provides functions for sending notifications via Telegram and other channels.
"""

from .telegram import send_telegram

__all__ = [
    "send_telegram",
]
