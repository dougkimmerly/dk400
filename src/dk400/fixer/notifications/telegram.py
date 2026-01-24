"""Telegram notification functions."""

import hashlib
import logging
import secrets
import string
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import httpx

from ..config import config
from ..database import get_pool

logger = logging.getLogger(__name__)

# Throttle settings
THROTTLE_HOURS = 6
MAX_SENDS_BEFORE_SUPPRESS = 3


def _generate_msg_id() -> str:
    """Generate unique message ID like TG-A1B2C3."""
    chars = string.ascii_uppercase + string.digits
    random_part = "".join(secrets.choice(chars) for _ in range(6))
    return f"TG-{random_part}"


def _hash_message(message: str) -> str:
    """Create hash for deduplication."""
    return hashlib.sha256(message.encode()).hexdigest()[:16]


async def _get_notification_history(message_hash: str) -> Dict:
    """Get notification history for throttling."""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    MAX(sent_at) as last_sent,
                    COUNT(*) as total_sends
                FROM fixer.notification_log
                WHERE fingerprint = $1 AND success = true
                """,
                message_hash,
            )
            if row:
                return {
                    "last_sent": row["last_sent"],
                    "total_sends": row["total_sends"] or 0,
                }
    except Exception as e:
        logger.warning(f"Could not check notification history: {e}")

    return {"last_sent": None, "total_sends": 0}


async def _log_notification(
    message_hash: str,
    message: str,
    success: bool,
) -> None:
    """Log notification to database."""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO fixer.notification_log
                    (fingerprint, channel, message, success)
                VALUES ($1, $2, $3, $4)
                """,
                message_hash,
                "telegram",
                message[:500],
                success,
            )
    except Exception as e:
        logger.warning(f"Could not log notification: {e}")


async def send_telegram(
    message: str,
    parse_mode: Optional[str] = "Markdown",
    bypass_throttle: bool = False,
    message_type: str = "alert",
    target: Optional[str] = None,
    host: Optional[str] = None,
    issue_id: Optional[int] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Send message via Telegram through comms service.

    Args:
        message: Message text
        parse_mode: Telegram parse mode (Markdown, HTML, None)
        bypass_throttle: Skip throttle checks
        message_type: Type of message (alert, investigation, escalation, etc)
        target: What service/device this is about
        host: Host IP/name
        issue_id: Related issue ID
        context: Additional context

    Returns:
        Message ID if sent, None if throttled/failed
    """
    message_hash = _hash_message(message)

    # Check throttle
    if not bypass_throttle:
        history = await _get_notification_history(message_hash)

        if history["total_sends"] >= MAX_SENDS_BEFORE_SUPPRESS:
            logger.info(f"Notification suppressed (sent {history['total_sends']} times)")
            await _log_notification(message_hash, message, False)
            return None

        if history["last_sent"]:
            time_since = datetime.utcnow() - history["last_sent"].replace(tzinfo=None)
            if time_since < timedelta(hours=THROTTLE_HOURS):
                logger.info(f"Notification throttled (sent {time_since.seconds // 3600}h ago)")
                return None

    # Generate tracking ID
    msg_id = _generate_msg_id()

    # Append tracking ID
    if parse_mode == "HTML":
        final_message = f"{message}\n\n<i>Ref: {msg_id}</i>"
    elif parse_mode == "Markdown":
        final_message = f"{message}\n\n_Ref: {msg_id}_"
    else:
        final_message = f"{message}\n\nRef: {msg_id}"

    # Send via comms service
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                config.comms_url,
                json={
                    "message": final_message,
                    "from": "fixer",
                    "channel": "telegram",
                    "parse_mode": parse_mode,
                },
            )
            response.raise_for_status()

            logger.info(f"Telegram message sent: {msg_id}")
            await _log_notification(message_hash, final_message, True)
            return msg_id

    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")
        await _log_notification(message_hash, message, False)
        raise
