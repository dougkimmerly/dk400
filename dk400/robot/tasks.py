"""Robot Tasks - Thin wrappers that call programs.

These are Celery tasks that Robot schedules.
Each task just imports and runs a program.
"""

import asyncio
import importlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict

from celery import current_app

logger = logging.getLogger(__name__)


def _import_program(program_name: str):
    """Import a program module, searching deployment programs first.

    Search order:
    1. programs.{name} — deployment-specific programs
    2. dk400.programs.{name} — built-in platform programs
    """
    for namespace in ["programs", "dk400.programs"]:
        try:
            return importlib.import_module(f"{namespace}.{program_name}")
        except ModuleNotFoundError:
            continue

    raise ModuleNotFoundError(f"Program not found: {program_name}")


def update_last_run(program_name: str):
    """Update last_run in _jobscde for jobs using this program."""
    try:
        import psycopg2
        from dk400.config import settings

        conn = psycopg2.connect(settings.database_url)
        conn.autocommit = True

        with conn.cursor() as cur:
            # Update jobs where command matches program name
            # Handle both direct name and name with kwargs (program|key=val)
            cur.execute("""
                UPDATE qsys._jobscde
                SET last_run = NOW()
                WHERE command = %s
                   OR command LIKE %s
            """, (program_name, f'{program_name}|%'))

        conn.close()
    except Exception as e:
        logger.warning(f"Failed to update last_run for {program_name}: {e}")


@current_app.task(bind=True)
def run_program(self, program_name: str, **kwargs) -> Dict[str, Any]:
    """
    Run a program by name.

    This is the universal task that Robot uses to call any program.

    Args:
        program_name: Name of program (searched in programs.* then dk400.programs.*)
        **kwargs: Arguments to pass to program.run()

    Returns:
        Program result dict
    """
    start = datetime.now(timezone.utc)
    logger.info(f"Running program: {program_name}")

    try:
        # Import the program module
        module = _import_program(program_name)

        # Get the run function
        run_func = getattr(module, "run", None)
        if not run_func:
            raise ValueError(f"Program {program_name} has no run() function")

        # Call it (handle async)
        if asyncio.iscoroutinefunction(run_func):
            result = asyncio.run(run_func(**kwargs))
        else:
            result = run_func(**kwargs)

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"Program {program_name} completed in {duration:.1f}s")

        # Update last_run in database
        update_last_run(program_name)

        return {
            "program": program_name,
            "success": True,
            "result": result,
            "duration": duration,
        }

    except Exception as e:
        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.error(f"Program {program_name} failed: {e}")

        return {
            "program": program_name,
            "success": False,
            "error": str(e),
            "duration": duration,
        }
