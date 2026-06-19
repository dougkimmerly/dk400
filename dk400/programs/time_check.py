"""time_check - Example dk400 program.

Checks NTP time sync and returns the offset.
Demonstrates the program pattern: module with a run() function.
"""

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


async def run(**kwargs):
    """Check NTP time synchronization.

    Returns:
        dict with local time, NTP time, and offset
    """
    try:
        import ntplib

        ntp_client = ntplib.NTPClient()
        response = ntp_client.request('pool.ntp.org', version=3, timeout=5)

        ntp_time = datetime.fromtimestamp(response.tx_time, tz=ZoneInfo('UTC'))
        local_time = datetime.now(tz=ZoneInfo('UTC'))
        offset = response.offset

        status = "ok" if abs(offset) < 1.0 else "drift"

        return {
            "status": status,
            "local_time": local_time.isoformat(),
            "ntp_time": ntp_time.isoformat(),
            "offset_seconds": round(offset, 3),
            "server": "pool.ntp.org",
        }

    except Exception as e:
        logger.error(f"NTP check failed: {e}")
        return {
            "status": "error",
            "error": str(e),
            "local_time": datetime.now(tz=ZoneInfo('UTC')).isoformat(),
        }
