"""
WRKACTJOB - Work with Active Jobs

Classic AS/400-style active job display connected to Celery.
"""
from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Header, Footer, Static, Input
from textual.binding import Binding
from textual.containers import Vertical, Horizontal, VerticalScroll
from datetime import datetime
from typing import Any
import os

# Celery imports
from celery import Celery


def get_celery_app() -> Celery:
    """Get Celery app connection."""
    broker_url = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
    app = Celery('dk400', broker=broker_url)
    return app


def get_active_jobs() -> list[dict[str, Any]]:
    """Fetch active jobs from Celery."""
    jobs = []

    try:
        app = get_celery_app()
        inspect = app.control.inspect()

        # Get active tasks (currently executing)
        active = inspect.active() or {}
        for worker, tasks in active.items():
            worker_short = worker.split('@')[0] if '@' in worker else worker
            for task in tasks:
                jobs.append({
                    'job': task.get('name', 'UNKNOWN').split('.')[-1][:10].upper(),
                    'user': 'QUSER',
                    'type': 'BCH',
                    'status': 'ACTIVE',
                    'function': f"PGM-{task.get('name', '').split('.')[-1][:8]}",
                    'worker': worker_short.upper()[:8],
                    'id': task.get('id', ''),
                })

        # Get reserved tasks (received but not yet executing)
        reserved = inspect.reserved() or {}
        for worker, tasks in reserved.items():
            worker_short = worker.split('@')[0] if '@' in worker else worker
            for task in tasks:
                jobs.append({
                    'job': task.get('name', 'UNKNOWN').split('.')[-1][:10].upper(),
                    'user': 'QUSER',
                    'type': 'BCH',
                    'status': 'JOBQ',
                    'function': f"PGM-{task.get('name', '').split('.')[-1][:8]}",
                    'worker': worker_short.upper()[:8],
                    'id': task.get('id', ''),
                })

        # Get scheduled tasks (ETA/countdown)
        scheduled = inspect.scheduled() or {}
        for worker, tasks in scheduled.items():
            worker_short = worker.split('@')[0] if '@' in worker else worker
            for task in tasks:
                request = task.get('request', {})
                jobs.append({
                    'job': request.get('name', 'UNKNOWN').split('.')[-1][:10].upper(),
                    'user': 'QUSER',
                    'type': 'BCH',
                    'status': 'SCDW',
                    'function': f"PGM-{request.get('name', '').split('.')[-1][:8]}",
                    'worker': worker_short.upper()[:8],
                    'id': request.get('id', ''),
                })

        # If no jobs found, show the workers as "waiting" jobs
        if not jobs:
            stats = inspect.stats() or {}
            for worker in stats.keys():
                worker_short = worker.split('@')[0] if '@' in worker else worker
                jobs.append({
                    'job': worker_short.upper()[:10],
                    'user': 'QSYS',
                    'type': 'ASJ',
                    'status': 'TIMW',
                    'function': 'PGM-CELERY',
                    'worker': worker_short.upper()[:8],
                    'id': '',
                })

    except Exception as e:
        # On error, show a placeholder
        jobs.append({
            'job': 'ERROR',
            'user': 'QSYSOPR',
            'type': 'SYS',
            'status': 'MSGW',
            'function': str(e)[:20],
            'worker': '',
            'id': '',
        })

    return jobs


class JobRow(Horizontal):
    """A single job row with option input."""

    DEFAULT_CSS = """
    JobRow {
        height: 1;
        width: 100%;
        background: #000000;
    }

    JobRow .opt-input {
        width: 4;
        min-width: 4;
        max-width: 4;
        height: 1;
        border: none;
        background: #000000;
        color: #00ff00;
        padding: 0;
    }

    JobRow .opt-input:focus {
        background: #003300;
    }

    JobRow .job-field {
        color: #00ff00;
        background: #000000;
        height: 1;
        padding: 0;
    }
    """

    def __init__(self, job_data: dict[str, Any], row_index: int) -> None:
        super().__init__()
        self.job_data = job_data
        self.row_index = row_index

    def compose(self) -> ComposeResult:
        yield Input(
            placeholder="",
            max_length=2,
            id=f"opt-{self.row_index}",
            classes="opt-input",
        )
        yield Static(f" {self.job_data['job']:<10}", classes="job-field")
        yield Static(f" {self.job_data['user']:<10}", classes="job-field")
        yield Static(f" {self.job_data['type']:<4}", classes="job-field")
        yield Static(f" {self.job_data['status']:<6}", classes="job-field")
        yield Static(f" {self.job_data['function']:<16}", classes="job-field")
        yield Static(f" {self.job_data['worker']:<8}", classes="job-field")


class WrkActJobScreen(Screen):
    """WRKACTJOB - Work with Active Jobs screen."""

    BINDINGS = [
        Binding("f3", "back", "Exit", show=True),
        Binding("f5", "refresh", "Refresh", show=True),
        Binding("f12", "back", "Cancel", show=True),
        Binding("enter", "process_options", "Enter", show=False),
    ]

    CSS = """
    WrkActJobScreen {
        background: #000000;
    }

    #wrkactjob-header {
        color: #00ff00;
        background: #000000;
        height: auto;
        padding: 0 1;
    }

    #help-text {
        color: #00ff00;
        background: #000000;
        height: auto;
        padding: 0 1;
    }

    #column-header {
        color: #00ff00;
        background: #003300;
        height: 1;
        padding: 0 1;
        text-style: bold;
    }

    #job-list {
        background: #000000;
        height: 1fr;
        padding: 0 1;
    }

    #message-line {
        color: #00ff00;
        background: #000000;
        height: 1;
        padding: 0 1;
    }

    Header {
        background: #003300;
        color: #00ff00;
    }

    Footer {
        background: #003300;
        color: #00ff00;
    }
    """

    def __init__(self) -> None:
        super().__init__()
        self.jobs: list[dict[str, Any]] = []

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(self._get_header_text(), id="wrkactjob-header")
        yield Static(
            " Type options, press Enter.\n"
            "   4=End job   5=Work with   8=Spooled files",
            id="help-text"
        )
        yield Static(
            " Opt  Job         User        Type  Status  Function          Worker",
            id="column-header"
        )
        yield VerticalScroll(id="job-list")
        yield Static("", id="message-line")
        yield Footer()

    def _get_header_text(self) -> str:
        """Generate header text."""
        timestamp = datetime.now().strftime("%m/%d/%y  %H:%M:%S")
        user = getattr(self.app, 'current_user', 'QUSER')
        return (
            f"                          Work with Active Jobs                     {user}\n"
            f"                                                         {timestamp}"
        )

    def on_mount(self) -> None:
        """Initialize the job list."""
        self._refresh_jobs()

    def _refresh_jobs(self) -> None:
        """Refresh the job list from Celery."""
        job_list = self.query_one("#job-list", VerticalScroll)
        job_list.remove_children()

        self.jobs = get_active_jobs()

        if not self.jobs:
            job_list.mount(Static(" No active jobs", classes="job-field"))
        else:
            for i, job in enumerate(self.jobs):
                job_list.mount(JobRow(job, i))

        # Update header with fresh timestamp
        self.query_one("#wrkactjob-header", Static).update(self._get_header_text())

        # Focus first option input if there are jobs
        if self.jobs:
            self.set_timer(0.1, self._focus_first_input)

    def _focus_first_input(self) -> None:
        """Focus the first option input."""
        try:
            first_input = self.query_one("#opt-0", Input)
            first_input.focus()
        except Exception:
            pass

    def action_refresh(self) -> None:
        """Refresh job list."""
        self._refresh_jobs()
        self._show_message("Job list refreshed")

    def action_back(self) -> None:
        """Return to previous screen."""
        self.app.pop_screen()

    def action_process_options(self) -> None:
        """Process all entered options."""
        options_to_process = []

        # Collect all non-empty options
        for i, job in enumerate(self.jobs):
            try:
                opt_input = self.query_one(f"#opt-{i}", Input)
                opt_value = opt_input.value.strip()
                if opt_value:
                    options_to_process.append((opt_value, job, i))
                    opt_input.value = ""  # Clear after reading
            except Exception:
                pass

        if not options_to_process:
            # No options entered, just refresh
            self._show_message("")
            return

        # Process each option
        for opt, job, idx in options_to_process:
            self._process_option(opt, job)

    def _process_option(self, option: str, job: dict[str, Any]) -> None:
        """Process a single option for a job."""
        job_name = job['job']
        job_id = job.get('id', '')

        if option == '4':
            # End job
            if job_id:
                try:
                    app = get_celery_app()
                    app.control.revoke(job_id, terminate=True)
                    self._show_message(f"Job {job_name} end requested")
                except Exception as e:
                    self._show_message(f"Error ending job: {e}")
            else:
                self._show_message(f"Cannot end system job {job_name}")
        elif option == '5':
            # Work with job - show details (placeholder)
            self._show_message(f"Work with {job_name} - Coming soon")
        elif option == '8':
            # Spooled files (placeholder)
            self._show_message(f"Spooled files for {job_name} - Coming soon")
        else:
            self._show_message(f"Option {option} not valid")

        # Refresh after processing
        self.set_timer(0.5, self._refresh_jobs)

    def _show_message(self, message: str) -> None:
        """Show a message on the message line."""
        msg_line = self.query_one("#message-line", Static)
        msg_line.update(f" {message}")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle Enter on an input - process all options."""
        self.action_process_options()
