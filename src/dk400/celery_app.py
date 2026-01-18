"""
DK/400 Celery Application

AS/400-inspired job queue system using Celery + Redis.
"""
from celery import Celery
import os

app = Celery('dk400')

app.config_from_object({
    'broker_url': os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0'),
    'result_backend': os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379/1'),
    'task_serializer': 'json',
    'result_serializer': 'json',
    'accept_content': ['json'],
    'timezone': 'America/Toronto',
    'enable_utc': True,
    'task_track_started': True,
    'task_time_limit': 600,  # 10 minute hard limit
    'task_soft_time_limit': 540,  # 9 minute soft limit
    'worker_prefetch_multiplier': 1,  # Fair scheduling
    'task_acks_late': True,  # Acknowledge after completion
    'task_reject_on_worker_lost': True,  # Requeue if worker dies
})

# Autodiscover tasks in src/dk400/tasks/
app.autodiscover_tasks(['src.dk400.tasks'])

# Empty beat schedule - will add jobs later
# Format: 'job-name': {'task': 'task.name', 'schedule': crontab(...)}
app.conf.beat_schedule = {}
