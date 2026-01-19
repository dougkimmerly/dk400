"""
DK/400 In-Process Job Scheduler

AS/400-style job scheduler using APScheduler.
Runs within the web process for Heroku/serverless deployments.
Checks _jobscde table and executes due jobs.
"""
import os
import logging
import ntplib
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Callable

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)

# Job registry - maps job names to their execution functions
JOB_REGISTRY: dict[str, Callable] = {}

# Scheduler instance
scheduler: Optional[AsyncIOScheduler] = None


def register_job(name: str):
    """Decorator to register a job function."""
    def decorator(func: Callable):
        JOB_REGISTRY[name] = func
        logger.info(f"Registered job: {name}")
        return func
    return decorator


# =============================================================================
# Built-in Jobs
# =============================================================================

@register_job('QNTPSYNC')
async def ntp_sync_job():
    """
    Sync time from NTP server and update QNTPTIME system value.
    AS/400-style: Gets accurate time from network time protocol server.
    """
    from src.dk400.web.database import set_system_value, get_system_timezone

    ntp_servers = [
        'pool.ntp.org',
        'time.google.com',
        'time.cloudflare.com',
        'time.apple.com',
    ]

    ntp_client = ntplib.NTPClient()
    ntp_time = None
    server_used = None
    offset = None

    for server in ntp_servers:
        try:
            response = ntp_client.request(server, version=3, timeout=5)
            ntp_time = datetime.fromtimestamp(response.tx_time, tz=ZoneInfo('UTC'))
            offset = response.offset
            server_used = server
            logger.info(f"NTP sync from {server}: {ntp_time.isoformat()}, offset={offset:.3f}s")
            break
        except Exception as e:
            logger.warning(f"NTP server {server} failed: {e}")
            continue

    if ntp_time is None:
        logger.error("All NTP servers failed")
        set_system_value('QNTPSTS', 'FAILED', 'QNTPSYNC')
        return {'status': 'error', 'message': 'All NTP servers unreachable'}

    # Convert to local timezone for display
    tz = get_system_timezone()
    local_time = ntp_time.astimezone(tz)

    # Update system values
    set_system_value('QNTPTIME', local_time.strftime('%Y-%m-%d %H:%M:%S'), 'QNTPSYNC')
    set_system_value('QNTPUTC', ntp_time.strftime('%Y-%m-%d %H:%M:%S'), 'QNTPSYNC')
    set_system_value('QNTPOFFS', f'{offset:+.3f}', 'QNTPSYNC')
    set_system_value('QNTPSRV', server_used, 'QNTPSYNC')
    set_system_value('QNTPSTS', 'OK', 'QNTPSYNC')
    set_system_value('QNTPLAST', datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S'), 'QNTPSYNC')

    logger.info(f"NTP sync complete: {local_time.isoformat()} from {server_used}")

    return {
        'status': 'ok',
        'ntp_time': local_time.isoformat(),
        'utc_time': ntp_time.isoformat(),
        'offset': offset,
        'server': server_used
    }


# =============================================================================
# Scheduler Management
# =============================================================================

def init_scheduler() -> AsyncIOScheduler:
    """Initialize the APScheduler instance."""
    global scheduler

    if scheduler is not None:
        return scheduler

    scheduler = AsyncIOScheduler()
    logger.info("APScheduler initialized")
    return scheduler


def start_scheduler():
    """Start the scheduler and load jobs from database."""
    global scheduler

    if scheduler is None:
        init_scheduler()

    # Add built-in jobs
    _add_builtin_jobs()

    # Load jobs from _jobscde table
    _load_scheduled_jobs()

    if not scheduler.running:
        scheduler.start()
        logger.info("APScheduler started")


def stop_scheduler():
    """Stop the scheduler gracefully."""
    global scheduler

    if scheduler is not None and scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("APScheduler stopped")


def _add_builtin_jobs():
    """Add built-in system jobs."""
    global scheduler

    # NTP sync - every hour at minute 0
    scheduler.add_job(
        ntp_sync_job,
        CronTrigger(minute=0),  # Every hour at :00
        id='QNTPSYNC',
        name='NTP Time Sync',
        replace_existing=True
    )
    logger.info("Added built-in job: QNTPSYNC (hourly)")

    # Also add to database so it shows in WRKJOBSCDE
    _ensure_job_in_database(
        'QNTPSYNC',
        'NTP Time Sync - sync system time from NTP server',
        'QNTPSYNC',
        '*HOURLY'
    )

    # Run NTP sync immediately on startup
    scheduler.add_job(
        ntp_sync_job,
        'date',  # Run once now
        id='QNTPSYNC_INIT',
        name='NTP Time Sync (Initial)',
    )


def _ensure_job_in_database(name: str, text: str, command: str, frequency: str):
    """Ensure a job exists in the _jobscde table for WRKJOBSCDE display."""
    try:
        from src.dk400.web.database import get_cursor

        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO qsys._jobscde (name, text, command, frequency, status, created_by)
                VALUES (%s, %s, %s, %s, '*ACTIVE', 'QSYS')
                ON CONFLICT (name) DO UPDATE
                SET text = EXCLUDED.text,
                    command = EXCLUDED.command,
                    frequency = EXCLUDED.frequency,
                    status = '*ACTIVE'
            """, (name.upper(), text, command, frequency.upper()))
        logger.info(f"Ensured job {name} exists in database")
    except Exception as e:
        logger.warning(f"Could not add job {name} to database: {e}")


def _load_scheduled_jobs():
    """Load scheduled jobs from _jobscde table."""
    global scheduler

    try:
        from src.dk400.web.database import get_cursor

        with get_cursor() as cursor:
            cursor.execute("""
                SELECT name, command, frequency, schedule_time, days_of_week, status
                FROM qsys._jobscde
                WHERE status = '*ACTIVE'
            """)

            for row in cursor.fetchall():
                job_name = row['name']

                if job_name in JOB_REGISTRY:
                    # Job function is registered
                    job_func = JOB_REGISTRY[job_name]
                    trigger = _parse_schedule(row)

                    if trigger:
                        scheduler.add_job(
                            job_func,
                            trigger,
                            id=job_name,
                            name=job_name,
                            replace_existing=True
                        )
                        logger.info(f"Loaded scheduled job: {job_name}")
                else:
                    logger.warning(f"Job {job_name} not in registry, skipping")

    except Exception as e:
        logger.error(f"Failed to load scheduled jobs: {e}")


def _parse_schedule(job: dict):
    """Parse job schedule into APScheduler trigger."""
    frequency = job.get('frequency', '*ONCE')
    schedule_time = job.get('schedule_time')
    days_of_week = job.get('days_of_week', '')

    if frequency == '*HOURLY':
        minute = 0
        if schedule_time:
            minute = schedule_time.minute
        return CronTrigger(minute=minute)

    elif frequency == '*DAILY':
        if schedule_time:
            return CronTrigger(
                hour=schedule_time.hour,
                minute=schedule_time.minute
            )
        return CronTrigger(hour=0, minute=0)

    elif frequency == '*WEEKLY':
        dow = days_of_week if days_of_week else 'mon'
        if schedule_time:
            return CronTrigger(
                day_of_week=dow.lower(),
                hour=schedule_time.hour,
                minute=schedule_time.minute
            )
        return CronTrigger(day_of_week=dow.lower(), hour=0, minute=0)

    elif frequency == '*MONTHLY':
        if schedule_time:
            return CronTrigger(
                day=1,
                hour=schedule_time.hour,
                minute=schedule_time.minute
            )
        return CronTrigger(day=1, hour=0, minute=0)

    # Default to interval-based for unknown frequencies
    return None


def add_job_entry(name: str, command: str, frequency: str = '*HOURLY',
                  schedule_time: str = None, text: str = '') -> tuple[bool, str]:
    """
    Add a job to the _jobscde table.

    Args:
        name: Job name (max 20 chars)
        command: Command to execute or job function name
        frequency: *ONCE, *HOURLY, *DAILY, *WEEKLY, *MONTHLY
        schedule_time: Time to run (HH:MM format)
        text: Description
    """
    try:
        from src.dk400.web.database import get_cursor

        with get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO qsys._jobscde (name, text, command, frequency, schedule_time, status, created_by)
                VALUES (%s, %s, %s, %s, %s, '*ACTIVE', 'QSYS')
                ON CONFLICT (name) DO UPDATE
                SET command = EXCLUDED.command,
                    frequency = EXCLUDED.frequency,
                    schedule_time = EXCLUDED.schedule_time,
                    text = EXCLUDED.text,
                    status = '*ACTIVE'
            """, (name.upper(), text, command, frequency.upper(), schedule_time))

        logger.info(f"Job {name} added/updated in schedule")
        return True, f"Job {name} scheduled"

    except Exception as e:
        logger.error(f"Failed to add job {name}: {e}")
        return False, str(e)


def list_scheduled_jobs() -> list[dict]:
    """List all scheduled jobs and their status."""
    global scheduler

    jobs = []

    # Get jobs from APScheduler
    if scheduler:
        for job in scheduler.get_jobs():
            next_run = job.next_run_time
            jobs.append({
                'name': job.id,
                'next_run': next_run.isoformat() if next_run else 'Not scheduled',
                'trigger': str(job.trigger),
                'source': 'scheduler'
            })

    # Get jobs from database
    try:
        from src.dk400.web.database import get_cursor

        with get_cursor() as cursor:
            cursor.execute("""
                SELECT name, text, command, frequency, schedule_time, status, last_run, next_run
                FROM qsys._jobscde
                ORDER BY name
            """)

            for row in cursor.fetchall():
                # Check if already in list from scheduler
                existing = next((j for j in jobs if j['name'] == row['name']), None)
                if existing:
                    existing['db_status'] = row['status']
                    existing['last_run'] = str(row['last_run']) if row['last_run'] else None
                else:
                    jobs.append({
                        'name': row['name'],
                        'text': row['text'],
                        'command': row['command'],
                        'frequency': row['frequency'],
                        'status': row['status'],
                        'last_run': str(row['last_run']) if row['last_run'] else None,
                        'next_run': str(row['next_run']) if row['next_run'] else None,
                        'source': 'database'
                    })
    except Exception as e:
        logger.error(f"Failed to list jobs from database: {e}")

    return jobs


async def run_job_now(job_name: str) -> dict:
    """Run a job immediately."""
    job_name = job_name.upper()

    if job_name in JOB_REGISTRY:
        try:
            result = await JOB_REGISTRY[job_name]()
            return {'status': 'ok', 'job': job_name, 'result': result}
        except Exception as e:
            logger.error(f"Job {job_name} failed: {e}")
            return {'status': 'error', 'job': job_name, 'error': str(e)}
    else:
        return {'status': 'error', 'job': job_name, 'error': 'Job not found in registry'}
