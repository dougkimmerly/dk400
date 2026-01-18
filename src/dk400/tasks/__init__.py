"""
DK/400 Tasks

Import all task modules here to ensure they are registered with Celery.
"""
from src.dk400.tasks import sample  # noqa: F401
