# Backlog

Non-architectural decisions and known gaps, tracked here until they're worked
or promoted to an ADR.

## 2026-09-10 — `qsys._jobhst` had no producer

**Finding:** the table, its grants, and the `WRKJOBHST` / `DSPJOBLOG` read
screens existed, but nothing ever inserted a row. A deployment consuming this
engine had 23 active scheduled jobs with healthy `last_run` timestamps and
zero rows in `_jobhst`, ever. `last_run` (written separately) masked the gap
because it's what everyone checks.

**Root cause:** `dk400/robot/tasks.py`'s `run_program` task updated
`_jobscde.last_run` on completion but never wrote to `_jobhst`. No other
code path (API, web) wrote to it either — the table was pure schema.

**Fix:** `run_program` now inserts one `_jobhst` row per run via
`_write_job_history()`, in both the success and exception paths, using the
existing column contract (`job_name`, `job_type='SCHEDULED'`, `status`
`COMPLETE`/`ERROR`, `submitted_by='SYSTEM'`, `started_at`, `completed_at`,
`result`, `error`). The existing read paths (`get_job_history`,
`get_job_history_entry` in `dk400/web/database.py`) were not changed. Covered
by `tests/test_robot_tasks.py`.

As a side effect, `update_last_run()` is now also called on the exception
path in `tasks.py` directly, rather than relying solely on the Celery
`task_success` signal in `db_scheduler.py` (which already fired in both
cases, since `run_program` catches its own exceptions and never lets the
Celery task itself fail) — this makes that guarantee self-contained and
testable without a running Celery Beat.

**Out of scope, not fixed here:** job runs submitted interactively (via the
API or a screen, rather than the scheduler) still don't write `_jobhst` rows
either — there is no such write path anywhere in the repo yet. If interactive
submissions should also show up in `WRKJOBHST`, that needs its own producer
at the API/web submission point, with `job_type` distinguishing it from
`SCHEDULED` runs.

**Deployment note:** this fix lives in the engine. It only reaches a
deployment once that deployment bumps its `engine/` submodule.
