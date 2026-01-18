"""
WRKJOBQ - Work with Job Queues

Classic AS/400-style job queue display connected to Celery/Redis.
"""
from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Header, Footer, Static, Input
from textual.binding import Binding
from textual.containers import Vertical, Horizontal, VerticalScroll
from datetime import datetime
from typing import Any
import os
import socket

from celery import Celery


def get_celery_app() -> Celery:
    """Get Celery app connection."""
    broker_url = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
    app = Celery('dk400', broker=broker_url)
    return app


def get_job_queues() -> list[dict[str, Any]]:
    """Fetch job queue information from Celery."""
    queues = []

    try:
        app = get_celery_app()
        inspect = app.control.inspect()

        # Get active queues from workers
        active_queues = inspect.active_queues() or {}

        # Get stats for job counts
        stats = inspect.stats() or {}
        active = inspect.active() or {}
        reserved = inspect.reserved() or {}
        scheduled = inspect.scheduled() or {}

        # Build queue info
        seen_queues = set()
        for worker, worker_queues in active_queues.items():
            for q in worker_queues:
                queue_name = q.get('name', 'unknown')
                if queue_name in seen_queues:
                    continue
                seen_queues.add(queue_name)

                # Count jobs for this queue
                active_count = 0
                reserved_count = 0

                for w, tasks in active.items():
                    for t in tasks:
                        if t.get('delivery_info', {}).get('routing_key') == queue_name:
                            active_count += 1

                for w, tasks in reserved.items():
                    for t in tasks:
                        if t.get('delivery_info', {}).get('routing_key') == queue_name:
                            reserved_count += 1

                queues.append({
                    'queue': queue_name.upper()[:10],
                    'status': 'RLS',  # Released
                    'jobs': reserved_count,
                    'active': active_count,
                    'subsystem': 'QBATCH',
                    'description': f'Celery queue {queue_name}',
                })

        # If no queues found, show default
        if not queues:
            queues.append({
                'queue': 'CELERY',
                'status': 'RLS',
                'jobs': 0,
                'active': 0,
                'subsystem': 'QBATCH',
                'description': 'Default Celery queue',
            })

    except Exception as e:
        queues.append({
            'queue': 'ERROR',
            'status': 'HLD',
            'jobs': 0,
            'active': 0,
            'subsystem': '',
            'description': str(e)[:30],
        })

    return queues


class JobQueueRow(Horizontal):
    """A single job queue row with option input."""

    DEFAULT_CSS = """
    JobQueueRow {
        height: 1;
        width: 100%;
        background: #000000;
    }

    JobQueueRow .opt-input {
        width: 4;
        min-width: 4;
        max-width: 4;
        height: 1;
        border: none;
        background: #000000;
        color: #00ff00;
        padding: 0;
    }

    JobQueueRow .opt-input:focus {
        background: #003300;
    }

    JobQueueRow .queue-field {
        color: #00ff00;
        background: #000000;
        height: 1;
        padding: 0;
    }
    """

    def __init__(self, queue_data: dict[str, Any], row_index: int) -> None:
        super().__init__()
        self.queue_data = queue_data
        self.row_index = row_index

    def compose(self) -> ComposeResult:
        yield Input(
            placeholder="",
            max_length=2,
            id=f"opt-{self.row_index}",
            classes="opt-input",
        )
        yield Static(f" {self.queue_data['queue']:<10}", classes="queue-field")
        yield Static(f" {self.queue_data['subsystem']:<10}", classes="queue-field")
        yield Static(f" {self.queue_data['status']:<5}", classes="queue-field")
        yield Static(f" {self.queue_data['jobs']:>6}", classes="queue-field")
        yield Static(f" {self.queue_data['active']:>6}", classes="queue-field")
        yield Static(f" {self.queue_data['description']:<30}", classes="queue-field")


class WrkJobQScreen(Screen):
    """WRKJOBQ - Work with Job Queues screen."""

    BINDINGS = [
        Binding("f3", "back", "Exit", show=True),
        Binding("f5", "refresh", "Refresh", show=True),
        Binding("f12", "back", "Cancel", show=True),
        Binding("enter", "process_options", "Enter", show=False),
    ]

    CSS = """
    WrkJobQScreen {
        background: #000000;
    }

    #wrkjobq-header {
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

    #queue-list {
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
        self.queues: list[dict[str, Any]] = []

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(self._get_header_text(), id="wrkjobq-header")
        yield Static(
            " Type options, press Enter.\n"
            "   5=Work with   6=Hold   7=Release   8=Work with job",
            id="help-text"
        )
        yield Static(
            " Opt  Queue       Subsystem   Sts    Jobs  Active  Description",
            id="column-header"
        )
        yield VerticalScroll(id="queue-list")
        yield Static("", id="message-line")
        yield Footer()

    def _get_header_text(self) -> str:
        """Generate header text."""
        timestamp = datetime.now().strftime("%m/%d/%y  %H:%M:%S")
        hostname = socket.gethostname().upper()[:10]
        user = getattr(self.app, 'current_user', 'QUSER')
        return (
            f" {hostname}                   Work with Job Queues                   {user}\n"
            f"                                                          {timestamp}"
        )

    def on_mount(self) -> None:
        """Initialize the queue list."""
        self._refresh_queues()

    def _refresh_queues(self) -> None:
        """Refresh the queue list from Celery."""
        queue_list = self.query_one("#queue-list", VerticalScroll)
        queue_list.remove_children()

        self.queues = get_job_queues()

        if not self.queues:
            queue_list.mount(Static(" No job queues found", classes="queue-field"))
        else:
            for i, queue in enumerate(self.queues):
                queue_list.mount(JobQueueRow(queue, i))

        self.query_one("#wrkjobq-header", Static).update(self._get_header_text())

        if self.queues:
            self.set_timer(0.1, self._focus_first_input)

    def _focus_first_input(self) -> None:
        """Focus the first option input."""
        try:
            first_input = self.query_one("#opt-0", Input)
            first_input.focus()
        except Exception:
            pass

    def action_refresh(self) -> None:
        """Refresh queue list."""
        self._refresh_queues()
        self._show_message("Queue list refreshed")

    def action_back(self) -> None:
        """Return to previous screen."""
        self.app.pop_screen()

    def action_process_options(self) -> None:
        """Process all entered options."""
        options_to_process = []

        for i, queue in enumerate(self.queues):
            try:
                opt_input = self.query_one(f"#opt-{i}", Input)
                opt_value = opt_input.value.strip()
                if opt_value:
                    options_to_process.append((opt_value, queue, i))
                    opt_input.value = ""
            except Exception:
                pass

        if not options_to_process:
            self._show_message("")
            return

        for opt, queue, idx in options_to_process:
            self._process_option(opt, queue)

    def _process_option(self, option: str, queue: dict[str, Any]) -> None:
        """Process a single option for a queue."""
        queue_name = queue['queue']

        if option == '5':
            self._show_message(f"Work with queue {queue_name} - Coming soon")
        elif option == '6':
            self._show_message(f"Queue {queue_name} held")
        elif option == '7':
            self._show_message(f"Queue {queue_name} released")
        elif option == '8':
            self._show_message(f"Work with jobs in {queue_name} - Coming soon")
        else:
            self._show_message(f"Option {option} not valid")

        self.set_timer(0.5, self._refresh_queues)

    def _show_message(self, message: str) -> None:
        """Show a message on the message line."""
        msg_line = self.query_one("#message-line", Static)
        msg_line.update(f" {message}")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle Enter on an input."""
        self.action_process_options()
