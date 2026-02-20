"""Robot Worker - Celery configuration.

This is the Celery app that runs scheduled programs.
Uses DatabaseScheduler to read schedules from qsys._jobscde table.
"""

from celery import Celery

from dk400.config import settings

# Create Celery app
app = Celery(
    "dk400",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["dk400.robot.tasks"],
)

# Celery configuration
app.conf.update(
    # Timezone
    timezone="America/New_York",
    enable_utc=True,

    # Task settings
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",

    # Worker settings
    worker_prefetch_multiplier=1,
    worker_concurrency=4,

    # Beat settings - use database scheduler
    beat_scheduler='dk400.robot.db_scheduler:DatabaseScheduler',
    beat_schedule_filename="/tmp/celerybeat-schedule",
)

# Empty beat_schedule - scheduler reads from database
app.conf.beat_schedule = {}


# App startup logging
@app.on_after_configure.connect
def setup_logging(sender, **kwargs):
    import logging
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
