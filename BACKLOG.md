# Backlog

Non-architectural decisions and known gaps, tracked here until they're worked
or promoted to an ADR.

## 2026-09-13 — QNTPSYNC (and any in-process job) dispatched twice, one path always failing

**Finding:** two schedulers both read `qsys._jobscde`: `dk400/web/job_scheduler.py`
(APScheduler, in the web process) only runs a row if its name is registered
via `@register_job`, logging "not in registry, skipping" otherwise —
correct. `dk400/robot/db_scheduler.py`'s `_load_schedule_from_db()` (Celery
Beat) turned *every* `*ACTIVE` row into a `ScheduleEntry` for
`dk400.robot.tasks.run_program`, with no equivalent exclusion. QNTPSYNC is
registered in `job_scheduler.JOB_REGISTRY` and also inserted into
`_jobscde` (by `_ensure_job_in_database`, so it shows up in `WRKJOBSCDE`),
so Celery Beat scheduled it too — and `run_program` always failed with
"Program not found: QNTPSYNC", since QNTPSYNC has no
`programs`/`dk400.programs` module; it only exists as an in-process
function. This ran hourly at every deployment, silently, until sites
started keeping `_jobhst` history (see the entry below) and it surfaced as
a recurring false "job failing" alert.

**Fix:** `job_scheduler._ensure_job_in_database` / `add_job_entry` already
wrote these rows with `created_by='QSYS'`, coincidentally distinct from the
`'SYSTEM'`/session-user default every other `_jobscde` insert path uses.
Made that distinction load-bearing: `db_scheduler._load_schedule_from_db()`
now skips `*ACTIVE` rows with `created_by='QSYS'` before turning them into
Celery `ScheduleEntry`s, and both `job_scheduler.py` insert sites got a
comment documenting the contract. This avoids importing
`dk400.web.job_scheduler` (or anything from `dk400/web/`) into the Celery
Beat process — Robot's `_jobscde` scheduling stays decided entirely by a
column already on the row. Covered by `tests/test_db_scheduler.py`.

**Deployment note:** this fix lives in the engine. It only reaches a
deployment once that deployment bumps its `engine/` submodule — until then,
QNTPSYNC (or any other in-process-registry job already recorded in
`_jobscde`) keeps double-firing there.

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
