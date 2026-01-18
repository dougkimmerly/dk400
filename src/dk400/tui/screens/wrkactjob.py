"""
WRKACTJOB - Work with Active Jobs

Classic AS/400-style active job display connected to Celery.
"""
from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Header, Footer, Static, DataTable
from textual.binding import Binding
from textual.containers import Vertical
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
                    'id': task.get('id', '')[:8],
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
                    'id': task.get('id', '')[:8],
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
                    'id': request.get('id', '')[:8],
                })
        
        # Get registered tasks (for reference)
        registered = inspect.registered() or {}
        
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


class WrkActJobScreen(Screen):
    """WRKACTJOB - Work with Active Jobs screen."""

    BINDINGS = [
        Binding("f3", "back", "Exit", show=True),
        Binding("f5", "refresh", "Refresh", show=True),
        Binding("f12", "back", "Cancel", show=True),
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

    #job-table {
        background: #000000;
        color: #00ff00;
        height: 1fr;
        margin: 0 1;
    }

    #job-table > .datatable--header {
        background: #003300;
        color: #00ff00;
        text-style: bold;
    }

    #job-table > .datatable--cursor {
        background: #004400;
        color: #00ff00;
    }

    #job-table > .datatable--row {
        background: #000000;
        color: #00ff00;
    }

    #job-table > .datatable--row-hover {
        background: #002200;
    }

    Header {
        background: #003300;
        color: #00ff00;
    }

    Footer {
        background: #003300;
        color: #00ff00;
    }

    #help-text {
        color: #00ff00;
        background: #000000;
        height: auto;
        padding: 0 1;
    }
    """

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Vertical(
            Static(self._get_header_text(), id="wrkactjob-header"),
            Static(
                "  Type options, press Enter.\n"
                "    4=End job   5=Work with   8=Spooled files\n",
                id="help-text"
            ),
            DataTable(id="job-table"),
        )
        yield Footer()

    def _get_header_text(self) -> str:
        """Generate header text."""
        timestamp = datetime.now().strftime("%m/%d/%y  %H:%M:%S")
        user = getattr(self.app, 'current_user', 'QUSER')
        return (
            f"                           Work with Active Jobs                    {user}\n"
            f"                                                         {timestamp}\n"
        )

    def on_mount(self) -> None:
        """Initialize the job table."""
        table = self.query_one("#job-table", DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = False
        
        # Add columns matching AS/400 WRKACTJOB
        table.add_column("Opt", width=4)
        table.add_column("Job", width=12)
        table.add_column("User", width=10)
        table.add_column("Type", width=5)
        table.add_column("Status", width=8)
        table.add_column("Function", width=18)
        table.add_column("Worker", width=10)
        
        self._refresh_jobs()

    def _refresh_jobs(self) -> None:
        """Refresh the job list from Celery."""
        table = self.query_one("#job-table", DataTable)
        table.clear()
        
        jobs = get_active_jobs()
        
        if not jobs:
            table.add_row("", "No active jobs", "", "", "", "", "")
        else:
            for job in jobs:
                table.add_row(
                    "",  # Opt column for user input
                    job['job'],
                    job['user'],
                    job['type'],
                    job['status'],
                    job['function'],
                    job['worker'],
                )
        
        # Update header with fresh timestamp
        self.query_one("#wrkactjob-header", Static).update(self._get_header_text())

    def action_refresh(self) -> None:
        """Refresh job list."""
        self._refresh_jobs()
        self.notify("Job list refreshed", title="WRKACTJOB")

    def action_back(self) -> None:
        """Return to previous screen."""
        self.app.pop_screen()
