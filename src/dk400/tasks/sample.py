"""
DK/400 Sample Tasks

Test tasks for verifying the job queue infrastructure.
"""
from src.dk400.celery_app import app
import time


@app.task(name='dk400.ping')
def ping():
    """Simple ping task for testing connectivity."""
    return {'status': 'pong', 'message': 'DK/400 is alive'}


@app.task(name='dk400.delay')
def delay(seconds: int = 5):
    """Task that takes time - for testing job monitoring."""
    time.sleep(seconds)
    return {'status': 'complete', 'delayed': seconds}


@app.task(name='dk400.echo')
def echo(message: str):
    """Echo a message back."""
    return {'status': 'ok', 'echo': message}


@app.task(name='dk400.add')
def add(x: int, y: int):
    """Add two numbers - classic Celery test task."""
    return {'status': 'ok', 'result': x + y}
