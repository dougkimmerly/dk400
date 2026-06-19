"""Database-driven Celery Beat Scheduler.

Reads job schedules from qsys._jobscde table instead of hardcoded schedules.py.
Updates next_run and last_run columns as jobs execute.

AS/400 Style:
- Jobs defined in _jobscde table
- WRKJOBSCDE shows accurate next_run times
- Changes take effect without restart
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from celery import current_app
from celery.beat import Scheduler, ScheduleEntry
from celery.schedules import crontab, schedule

logger = logging.getLogger(__name__)

# Default timezone
DEFAULT_TZ = ZoneInfo("America/New_York")


class DatabaseScheduler(Scheduler):
    """
    Celery Beat scheduler that reads from _jobscde database table.

    Features:
    - Loads schedules from qsys._jobscde
    - Updates next_run when calculating schedules
    - Updates last_run after task execution
    - Refreshes schedule periodically to pick up changes
    """

    # How often to refresh schedule from database (seconds)
    UPDATE_INTERVAL = 60

    def __init__(self, *args, **kwargs):
        self._last_refresh = None
        self._schedule = {}
        super().__init__(*args, **kwargs)

    def setup_schedule(self):
        """Called on startup - load schedule from database."""
        logger.info("DatabaseScheduler: Loading schedules from _jobscde")
        self._load_schedule_from_db()

        # Also install task success signal to update last_run
        from celery.signals import task_success
        task_success.connect(self._on_task_success)

    @property
    def schedule(self):
        """Return the current schedule dict."""
        # Periodically refresh from database
        now = datetime.now()
        if (self._last_refresh is None or
            (now - self._last_refresh).total_seconds() > self.UPDATE_INTERVAL):
            self._load_schedule_from_db()
            self._last_refresh = now

        return self._schedule

    def _load_schedule_from_db(self):
        """Load schedule entries from _jobscde table."""
        try:
            import psycopg2
            import psycopg2.extras
            from dk400.config import settings

            conn = psycopg2.connect(settings.database_url)
            conn.autocommit = True

            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT name, text, command, frequency, schedule_date,
                           schedule_time, days_of_week, status
                    FROM qsys._jobscde
                    WHERE status = '*ACTIVE'
                """)

                new_schedule = {}
                for row in cur.fetchall():
                    entry = self._row_to_schedule_entry(row)
                    if entry:
                        new_schedule[row['name']] = entry
                        # Update next_run in database
                        self._update_next_run(cur, row['name'], entry)

                self._schedule = new_schedule
                logger.info(f"DatabaseScheduler: Loaded {len(new_schedule)} active jobs")

            conn.close()

        except Exception as e:
            logger.error(f"DatabaseScheduler: Failed to load schedule: {e}")
            # Keep existing schedule on error

    def _row_to_schedule_entry(self, row: Dict) -> Optional[ScheduleEntry]:
        """Convert a _jobscde row to a Celery ScheduleEntry."""
        try:
            name = row['name']
            command = row['command']
            frequency = row.get('frequency', '*ONCE')
            schedule_time = row.get('schedule_time')
            days_of_week = row.get('days_of_week', '')

            # Parse the schedule
            celery_schedule = self._parse_frequency(frequency, schedule_time, days_of_week)
            if not celery_schedule:
                logger.warning(f"Job {name}: Could not parse schedule (freq={frequency})")
                return None

            # Parse the command to get program name and kwargs
            program_name, kwargs = self._parse_command(command)
            if not program_name:
                logger.warning(f"Job {name}: Could not parse command: {command}")
                return None

            # Create a proper ScheduleEntry object
            return ScheduleEntry(
                name=name,
                task='dk400.robot.tasks.run_program',
                schedule=celery_schedule,
                args=(),
                kwargs={
                    'program_name': program_name,
                    **kwargs,
                },
                options={
                    'expires': 3600,  # Task expires after 1 hour if not picked up
                },
                app=self.app,
            )

        except Exception as e:
            logger.error(f"Job {row.get('name')}: Error creating entry: {e}")
            return None

    def _parse_frequency(self, frequency: str, schedule_time, days_of_week: str):
        """Convert AS/400-style frequency to Celery schedule."""
        # Extract hour/minute from schedule_time
        hour = 0
        minute = 0
        if schedule_time:
            if hasattr(schedule_time, 'hour'):
                hour = schedule_time.hour
                minute = schedule_time.minute
            else:
                # String format
                parts = str(schedule_time).split(':')
                hour = int(parts[0])
                minute = int(parts[1]) if len(parts) > 1 else 0

        frequency = frequency.upper() if frequency else '*ONCE'

        if frequency == '*HOURLY':
            # Run every hour at specified minute
            return crontab(minute=minute)

        elif frequency == '*DAILY':
            # Run daily at specified time
            return crontab(hour=hour, minute=minute)

        elif frequency == '*WEEKLY':
            # Run weekly on specified days at specified time
            dow = self._parse_days_of_week(days_of_week)
            return crontab(hour=hour, minute=minute, day_of_week=dow)

        elif frequency == '*MONTHLY':
            # Run on 1st of month at specified time
            return crontab(hour=hour, minute=minute, day_of_month=1)

        elif frequency.isdigit():
            # Numeric = interval in seconds
            return schedule(timedelta(seconds=int(frequency)))

        elif frequency == '*ONCE':
            # One-time jobs - skip for now (need different handling)
            return None

        else:
            logger.warning(f"Unknown frequency: {frequency}")
            return None

    def _parse_days_of_week(self, days_str: str) -> str:
        """Convert AS/400-style days to crontab format."""
        if not days_str:
            return 'mon'  # Default to Monday

        # Map AS/400 day names to crontab
        day_map = {
            'SUN': '0', 'MON': '1', 'TUE': '2', 'WED': '3',
            'THU': '4', 'FRI': '5', 'SAT': '6',
            'SUNDAY': '0', 'MONDAY': '1', 'TUESDAY': '2', 'WEDNESDAY': '3',
            'THURSDAY': '4', 'FRIDAY': '5', 'SATURDAY': '6',
        }

        days = []
        for part in days_str.upper().replace(',', ' ').split():
            if part in day_map:
                days.append(day_map[part])
            elif part.isdigit():
                days.append(part)

        return ','.join(days) if days else 'mon'

    def _parse_command(self, command: str) -> tuple[Optional[str], Dict]:
        """
        Parse command string to program name and kwargs.

        Formats:
        - "program_name" -> (program_name, {})
        - "program_name|key=value,key2=value2" -> (program_name, {key: value, ...})
        - "tasks.module.function" -> Legacy format, extract function name
        """
        if not command:
            return None, {}

        kwargs = {}

        # Check for kwargs separator
        if '|' in command:
            cmd_part, kwargs_part = command.split('|', 1)
            # Parse kwargs like "key=value,key2=value2"
            for kv in kwargs_part.split(','):
                if '=' in kv:
                    k, v = kv.split('=', 1)
                    kwargs[k.strip()] = v.strip()
            command = cmd_part

        # Handle legacy "tasks.module.function" format
        if command.startswith('tasks.'):
            # Extract the function/program name from "tasks.backup.dk400_usb_tier3"
            parts = command.split('.')
            if len(parts) >= 3:
                # Use last part as program name, but may need mapping
                program_name = parts[-1]
                # Store the full task path in case we need it
                kwargs['_legacy_task'] = command
                return program_name, kwargs

        # Direct program name
        return command, kwargs

    def _update_next_run(self, cursor, job_name: str, entry: ScheduleEntry):
        """Update next_run in database for a job."""
        try:
            celery_schedule = entry.schedule
            if celery_schedule:
                # Calculate next run time
                next_run = self._calculate_next_run(celery_schedule)
                if next_run:
                    cursor.execute("""
                        UPDATE qsys._jobscde
                        SET next_run = %s
                        WHERE name = %s
                    """, (next_run, job_name))
        except Exception as e:
            logger.error(f"Failed to update next_run for {job_name}: {e}")

    def _calculate_next_run(self, celery_schedule) -> Optional[datetime]:
        """Calculate the next run time for a schedule."""
        try:
            now = datetime.now(DEFAULT_TZ)

            if isinstance(celery_schedule, crontab):
                # Use celery's remaining_estimate to find next run
                remaining = celery_schedule.remaining_estimate(now)
                if remaining:
                    return now + remaining

            elif isinstance(celery_schedule, schedule):
                # Interval schedule
                return now + celery_schedule.run_every

            return None
        except Exception as e:
            logger.error(f"Error calculating next_run: {e}")
            return None

    def _on_task_success(self, sender=None, result=None, **kwargs):
        """Signal handler for successful task completion - update last_run."""
        try:
            # Extract job info from task
            task_kwargs = kwargs.get('kwargs', {}) or {}
            program_name = task_kwargs.get('program_name')

            if not program_name:
                return

            # Find the job name for this program
            # (might need to search _jobscde for matching command)
            self._update_last_run(program_name)

        except Exception as e:
            logger.error(f"Error updating last_run: {e}")

    def _update_last_run(self, program_name: str):
        """Update last_run in database after job completes."""
        try:
            import psycopg2
            from dk400.config import settings

            conn = psycopg2.connect(settings.database_url)
            conn.autocommit = True

            with conn.cursor() as cur:
                # Update by program name (might match command ending)
                cur.execute("""
                    UPDATE qsys._jobscde
                    SET last_run = NOW()
                    WHERE command LIKE %s OR command = %s
                """, (f'%{program_name}', program_name))

                if cur.rowcount > 0:
                    logger.debug(f"Updated last_run for {program_name}")

            conn.close()

        except Exception as e:
            logger.error(f"Failed to update last_run for {program_name}: {e}")


# For backwards compatibility, also expose as module-level
def get_scheduler():
    """Get the database scheduler class."""
    return DatabaseScheduler
