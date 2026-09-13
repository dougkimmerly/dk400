"""Tests for dk400.robot.db_scheduler — the Celery Beat database scheduler.

Covers the fix for QNTPSYNC (and any other dk400.web.job_scheduler-owned
job) being dispatched twice: once by the in-process APScheduler registry
(which runs it), and once by Celery Beat (which always failed with
"Program not found", since those jobs have no programs/dk400.programs
module). db_scheduler.py now skips '*ACTIVE' rows with created_by='QSYS' —
the marker job_scheduler.py's own inserts use for jobs it owns.

The real `celery` and `psycopg2` packages aren't dependencies of this test
run; we stub just enough of each so dk400/robot/db_scheduler.py imports and
its DB reads can be observed/controlled through a fake connection/cursor.
"""
import importlib.util
import pathlib
import sys
import types
import unittest
from datetime import timedelta

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
DB_SCHEDULER_PATH = REPO_ROOT / "dk400" / "robot" / "db_scheduler.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _install_fake_celery():
    """Install fake celery* modules, but only if celery isn't already
    present — and hand back their names so the caller can remove them
    again once db_scheduler.py's top-level imports are done. Leaving them
    in sys.modules would make test_robot_tasks.py (which installs its own,
    richer fake with a working current_app.task() decorator) skip its own
    setup when both test modules run together under discovery.
    """
    if "celery" in sys.modules:
        return []

    fake_celery = types.ModuleType("celery")
    fake_celery.current_app = object()
    sys.modules["celery"] = fake_celery

    fake_beat = types.ModuleType("celery.beat")

    class _FakeScheduler:
        def __init__(self, *args, **kwargs):
            self.app = kwargs.get("app")

    class _FakeScheduleEntry:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    fake_beat.Scheduler = _FakeScheduler
    fake_beat.ScheduleEntry = _FakeScheduleEntry
    sys.modules["celery.beat"] = fake_beat

    fake_schedules = types.ModuleType("celery.schedules")

    class _FakeCrontab:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def remaining_estimate(self, now):
            return timedelta(seconds=1)

    class _FakeSchedule:
        def __init__(self, run_every):
            self.run_every = run_every

    fake_schedules.crontab = _FakeCrontab
    fake_schedules.schedule = _FakeSchedule
    sys.modules["celery.schedules"] = fake_schedules

    fake_signals = types.ModuleType("celery.signals")

    class _FakeSignal:
        def connect(self, *_args, **_kwargs):
            pass

    fake_signals.task_success = _FakeSignal()
    sys.modules["celery.signals"] = fake_signals

    return ["celery", "celery.beat", "celery.schedules", "celery.signals"]


def _install_fake_psycopg2():
    if "psycopg2" in sys.modules:
        return

    fake_psycopg2 = types.ModuleType("psycopg2")
    fake_extras = types.ModuleType("psycopg2.extras")
    fake_extras.RealDictCursor = object()
    fake_psycopg2.extras = fake_extras
    sys.modules["psycopg2"] = fake_psycopg2
    sys.modules["psycopg2.extras"] = fake_extras


# dk400.config imports fine as the real module (no env/DB required at
# import time), so it isn't stubbed here.

_installed_celery_modules = _install_fake_celery()
_install_fake_psycopg2()


def _load_db_scheduler_module():
    spec = importlib.util.spec_from_file_location(
        "dk400_robot_db_scheduler_under_test", DB_SCHEDULER_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


db_scheduler = _load_db_scheduler_module()

# db_scheduler.py only needs celery/celery.beat/celery.schedules/celery.signals
# at import time (to bind Scheduler/ScheduleEntry/crontab/schedule into its own
# namespace above) — psycopg2 stays stubbed since _load_schedule_from_db()
# imports it lazily on every call. Removing our fakes now means
# test_robot_tasks.py's own (more complete) celery fake still gets installed
# when the two test modules run together under `unittest discover`.
for _name in _installed_celery_modules:
    sys.modules.pop(_name, None)


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self._fetch = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, query, params=None):
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT") and "FROM qsys._jobscde" in normalized:
            self._fetch = self._rows
        else:
            self._fetch = []

    def fetchall(self):
        return self._fetch


class FakeConnection:
    def __init__(self, rows):
        self._rows = rows
        self.autocommit = False
        self.closed = False

    def cursor(self, cursor_factory=None):
        return FakeCursor(self._rows)

    def close(self):
        self.closed = True


class LoadScheduleFromDbTests(unittest.TestCase):
    def setUp(self):
        import psycopg2
        self._orig_connect = getattr(psycopg2, "connect", None)
        self.addCleanup(self._restore_connect)

    def _restore_connect(self):
        import psycopg2
        if self._orig_connect is None:
            del psycopg2.connect
        else:
            psycopg2.connect = self._orig_connect

    def _set_rows(self, rows):
        import psycopg2
        psycopg2.connect = lambda *a, **kw: FakeConnection(rows)

    def _new_scheduler(self):
        return db_scheduler.DatabaseScheduler()

    def test_qsys_owned_job_excluded_from_celery_schedule(self):
        self._set_rows([{
            'name': 'QNTPSYNC',
            'text': 'NTP Time Sync',
            'command': 'QNTPSYNC',
            'frequency': '*HOURLY',
            'schedule_date': None,
            'schedule_time': None,
            'days_of_week': '',
            'status': '*ACTIVE',
            'created_by': 'QSYS',
        }])

        scheduler = self._new_scheduler()
        scheduler._load_schedule_from_db()

        self.assertNotIn('QNTPSYNC', scheduler._schedule)

    def test_normal_program_job_still_scheduled(self):
        self._set_rows([{
            'name': 'REAL_PROGRAM',
            'text': 'A real Robot program',
            'command': 'real_program',
            'frequency': '*HOURLY',
            'schedule_date': None,
            'schedule_time': None,
            'days_of_week': '',
            'status': '*ACTIVE',
            'created_by': 'SYSTEM',
        }])

        scheduler = self._new_scheduler()
        scheduler._load_schedule_from_db()

        self.assertIn('REAL_PROGRAM', scheduler._schedule)
        entry = scheduler._schedule['REAL_PROGRAM']
        self.assertEqual(entry.task, 'dk400.robot.tasks.run_program')
        self.assertEqual(entry.kwargs['program_name'], 'real_program')

    def test_legacy_row_with_no_created_by_still_scheduled(self):
        # Rows written before this column mattered may have created_by=None
        # (or a value other than 'QSYS') — only the QSYS marker excludes.
        self._set_rows([{
            'name': 'LEGACY_JOB',
            'text': '',
            'command': 'legacy_job',
            'frequency': '*HOURLY',
            'schedule_date': None,
            'schedule_time': None,
            'days_of_week': '',
            'status': '*ACTIVE',
            'created_by': None,
        }])

        scheduler = self._new_scheduler()
        scheduler._load_schedule_from_db()

        self.assertIn('LEGACY_JOB', scheduler._schedule)

    def test_mixed_rows_only_qsys_excluded(self):
        self._set_rows([
            {
                'name': 'QNTPSYNC', 'text': '', 'command': 'QNTPSYNC',
                'frequency': '*HOURLY', 'schedule_date': None,
                'schedule_time': None, 'days_of_week': '',
                'status': '*ACTIVE', 'created_by': 'QSYS',
            },
            {
                'name': 'REAL_PROGRAM', 'text': '', 'command': 'real_program',
                'frequency': '*HOURLY', 'schedule_date': None,
                'schedule_time': None, 'days_of_week': '',
                'status': '*ACTIVE', 'created_by': 'SYSTEM',
            },
        ])

        scheduler = self._new_scheduler()
        scheduler._load_schedule_from_db()

        self.assertNotIn('QNTPSYNC', scheduler._schedule)
        self.assertIn('REAL_PROGRAM', scheduler._schedule)


if __name__ == "__main__":
    unittest.main()
