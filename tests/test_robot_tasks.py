"""Tests for dk400.robot.tasks — the Robot scheduler's job-history write path.

The real `celery` and `psycopg2` packages aren't dependencies of this test run;
we stub just enough of each so dk400/robot/tasks.py imports and its DB calls
can be observed through a fake connection/cursor instead of a real database.
"""
import importlib.util
import pathlib
import sys
import types
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
TASKS_PATH = REPO_ROOT / "dk400" / "robot" / "tasks.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _install_fake_celery():
    """Stub `celery.current_app` with a no-op task decorator.

    tasks.py only needs `current_app.task(bind=True)` to wrap the function;
    the fake decorator preserves the plain function so tests can call it
    directly while still passing a `self` for the bind=True signature.
    """
    if "celery" in sys.modules:
        return

    fake_celery = types.ModuleType("celery")

    class _FakeTask:
        def __init__(self, func):
            self._func = func

        def __call__(self, *args, **kwargs):
            return self._func(self, *args, **kwargs)

    class _FakeCurrentApp:
        def task(self, *_args, **_kwargs):
            def decorator(func):
                return _FakeTask(func)
            return decorator

    fake_celery.current_app = _FakeCurrentApp()
    sys.modules["celery"] = fake_celery


def _install_fake_psycopg2():
    """Stub `psycopg2` with a `connect` swapped out per-test."""
    if "psycopg2" in sys.modules:
        return
    sys.modules["psycopg2"] = types.ModuleType("psycopg2")


_install_fake_celery()
_install_fake_psycopg2()


def _load_tasks_module():
    """Load dk400/robot/tasks.py directly, bypassing dk400/robot/__init__.py
    (which imports the real Celery app and would need a much heavier stub)."""
    spec = importlib.util.spec_from_file_location("dk400_robot_tasks_under_test", TASKS_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


tasks = _load_tasks_module()

JOBHST_COLUMNS = (
    "job_name", "job_type", "status", "submitted_by",
    "started_at", "completed_at", "result", "error",
)


class FakeCursor:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, query, params=None):
        self._conn.executed.append((" ".join(query.split()), params))
        normalized = " ".join(query.split())
        if normalized.startswith("INSERT INTO qsys._jobhst"):
            self._conn.jobhst_rows.append(dict(zip(JOBHST_COLUMNS, params)))
        elif "UPDATE qsys._jobscde" in normalized and "last_run" in normalized:
            self._conn.last_run_calls.append(params)


class FakeConnection:
    def __init__(self):
        self.executed = []
        self.jobhst_rows = []
        self.last_run_calls = []
        self.autocommit = False
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


class RunProgramJobHistoryTests(unittest.TestCase):
    def setUp(self):
        self.conn = FakeConnection()
        import psycopg2
        psycopg2.connect = lambda *a, **kw: self.conn

    def _register_program(self, name, run_func):
        module = types.ModuleType(f"programs.{name}")
        module.run = run_func
        sys.modules[f"programs.{name}"] = module
        self.addCleanup(sys.modules.pop, f"programs.{name}", None)

    def test_successful_run_writes_one_complete_row(self):
        self._register_program("ok_job", lambda **kw: {"copied": 3})

        result = tasks.run_program(program_name="ok_job")

        self.assertTrue(result["success"])
        self.assertEqual(len(self.conn.jobhst_rows), 1)
        row = self.conn.jobhst_rows[0]
        self.assertEqual(row["job_name"], "ok_job")
        self.assertEqual(row["status"], "COMPLETE")
        self.assertIsNotNone(row["started_at"])
        self.assertIsNotNone(row["completed_at"])
        self.assertIsNone(row["error"])

    def test_failed_run_writes_one_error_row_with_exception_text(self):
        def boom(**kw):
            raise RuntimeError("disk full")
        self._register_program("bad_job", boom)

        result = tasks.run_program(program_name="bad_job")

        self.assertFalse(result["success"])
        self.assertEqual(len(self.conn.jobhst_rows), 1)
        row = self.conn.jobhst_rows[0]
        self.assertEqual(row["job_name"], "bad_job")
        self.assertEqual(row["status"], "ERROR")
        self.assertIsNotNone(row["started_at"])
        self.assertIsNotNone(row["completed_at"])
        self.assertIn("disk full", row["error"])

    def test_one_row_per_run_not_per_poll(self):
        # A program that itself polls/loops internally is still one task
        # execution — the writer must not be invoked more than once per run.
        def polls_internally(**kw):
            for _ in range(5):
                pass
            return "done"
        self._register_program("polling_job", polls_internally)

        tasks.run_program(program_name="polling_job")

        self.assertEqual(len(self.conn.jobhst_rows), 1)

    def test_multiple_scheduled_executions_produce_matching_row_count(self):
        self._register_program("repeat_job", lambda **kw: "ok")

        for _ in range(3):
            tasks.run_program(program_name="repeat_job")

        self.assertEqual(len(self.conn.jobhst_rows), 3)

    def test_last_run_updated_on_success(self):
        self._register_program("ok_job2", lambda **kw: "ok")

        tasks.run_program(program_name="ok_job2")

        self.assertEqual(len(self.conn.last_run_calls), 1)

    def test_last_run_updated_on_failure(self):
        def boom(**kw):
            raise ValueError("nope")
        self._register_program("bad_job2", boom)

        tasks.run_program(program_name="bad_job2")

        self.assertEqual(len(self.conn.last_run_calls), 1)

    def test_missing_program_still_writes_error_row(self):
        result = tasks.run_program(program_name="does_not_exist_anywhere")

        self.assertFalse(result["success"])
        self.assertEqual(len(self.conn.jobhst_rows), 1)
        self.assertEqual(self.conn.jobhst_rows[0]["status"], "ERROR")


if __name__ == "__main__":
    unittest.main()
