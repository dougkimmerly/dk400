"""Robot - Job Scheduler.

Named after Robot Scheduler for AS/400.
Calls programs on schedule.
"""

from .worker import app

__all__ = ["app"]
