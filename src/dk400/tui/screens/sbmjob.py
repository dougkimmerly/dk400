"""
SBMJOB - Submit Job

AS/400-style job submission screen for Celery tasks.
"""
from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Header, Footer, Static, Input
from textual.binding import Binding
from textual.containers import Vertical, Horizontal
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


def get_available_tasks() -> list[str]:
    """Get list of registered tasks."""
    tasks = []
    try:
        app = get_celery_app()
        inspect = app.control.inspect()
        registered = inspect.registered() or {}

        for worker, worker_tasks in registered.items():
            for task in worker_tasks:
                if task not in tasks and not task.startswith('celery.'):
                    tasks.append(task)

    except Exception:
        pass

    # Add known tasks if none found
    if not tasks:
        tasks = ['dk400.ping', 'dk400.echo', 'dk400.delay']

    return sorted(tasks)


class SbmJobScreen(Screen):
    """SBMJOB - Submit Job screen."""

    BINDINGS = [
        Binding("f3", "back", "Exit", show=True),
        Binding("f4", "prompt", "Prompt", show=True),
        Binding("f12", "back", "Cancel", show=True),
        Binding("enter", "submit_job", "Enter", show=False),
    ]

    CSS = """
    SbmJobScreen {
        background: #000000;
    }

    #sbmjob-header {
        color: #00ff00;
        background: #000000;
        height: auto;
        padding: 0 1;
    }

    #form-container {
        background: #000000;
        padding: 1 2;
        height: 1fr;
    }

    .form-row {
        height: 1;
        width: 100%;
        margin-bottom: 1;
    }

    .form-label {
        color: #00ff00;
        background: #000000;
        width: 35;
        height: 1;
    }

    .form-input {
        background: #000000;
        color: #00ff00;
        border: none;
        width: 40;
        height: 1;
        padding: 0;
    }

    .form-input:focus {
        background: #003300;
    }

    #available-tasks {
        color: #00ff00;
        background: #000000;
        height: auto;
        margin-top: 2;
    }

    #message-line {
        color: #ffff00;
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

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(self._get_header_text(), id="sbmjob-header")
        yield Vertical(
            Static(" Type choices, press Enter.", classes="form-label"),
            Static("", classes="form-label"),
            Horizontal(
                Static(" Command to run  . . . . . . . :  ", classes="form-label"),
                Input(placeholder="dk400.ping", id="cmd-input", classes="form-input"),
                classes="form-row"
            ),
            Horizontal(
                Static(" Parameters . . . . . . . . . . :  ", classes="form-label"),
                Input(placeholder="", id="parm-input", classes="form-input"),
                classes="form-row"
            ),
            Horizontal(
                Static(" Job queue . . . . . . . . . . :  ", classes="form-label"),
                Input(placeholder="celery", value="celery", id="jobq-input", classes="form-input"),
                classes="form-row"
            ),
            Horizontal(
                Static(" Job priority . . . . . . . . . :  ", classes="form-label"),
                Input(placeholder="5", value="5", id="priority-input", classes="form-input"),
                classes="form-row"
            ),
            Horizontal(
                Static(" Delay (seconds) . . . . . . . :  ", classes="form-label"),
                Input(placeholder="0", value="0", id="delay-input", classes="form-input"),
                classes="form-row"
            ),
            Static(self._get_available_tasks_text(), id="available-tasks"),
            id="form-container"
        )
        yield Static("", id="message-line")
        yield Footer()

    def _get_header_text(self) -> str:
        """Generate header text."""
        timestamp = datetime.now().strftime("%m/%d/%y  %H:%M:%S")
        hostname = socket.gethostname().upper()[:10]
        user = getattr(self.app, 'current_user', 'QUSER')
        return (
            f" {hostname}                       Submit Job                          {user}\n"
            f"                                                          {timestamp}"
        )

    def _get_available_tasks_text(self) -> str:
        """Generate list of available tasks."""
        tasks = get_available_tasks()
        text = " Available commands:\n"
        for task in tasks[:10]:  # Show first 10
            text += f"   {task}\n"
        return text

    def on_mount(self) -> None:
        """Focus the command input on mount."""
        self.query_one("#cmd-input", Input).focus()

    def action_back(self) -> None:
        """Return to previous screen."""
        self.app.pop_screen()

    def action_prompt(self) -> None:
        """Show available tasks."""
        tasks = get_available_tasks()
        self._show_message(f"Tasks: {', '.join(tasks[:5])}")

    def action_submit_job(self) -> None:
        """Submit the job to Celery."""
        cmd = self.query_one("#cmd-input", Input).value.strip()
        parm = self.query_one("#parm-input", Input).value.strip()
        delay = self.query_one("#delay-input", Input).value.strip()

        if not cmd:
            self._show_message("Command is required")
            return

        try:
            app = get_celery_app()

            # Parse parameters
            args = []
            if parm:
                # Try to parse as JSON-like
                if parm.startswith('['):
                    import json
                    args = json.loads(parm)
                else:
                    # Comma-separated
                    args = [p.strip() for p in parm.split(',')]

            # Get the task
            task = app.signature(cmd, args=args)

            # Apply delay if specified
            delay_secs = int(delay) if delay and delay.isdigit() else 0
            if delay_secs > 0:
                result = task.apply_async(countdown=delay_secs)
            else:
                result = task.apply_async()

            self._show_message(f"Job submitted: {result.id[:8]}")

        except Exception as e:
            self._show_message(f"Error: {str(e)[:50]}")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle Enter - move to next field or submit."""
        current_id = event.input.id
        if current_id == "cmd-input":
            self.query_one("#parm-input", Input).focus()
        elif current_id == "parm-input":
            self.query_one("#jobq-input", Input).focus()
        elif current_id == "jobq-input":
            self.query_one("#priority-input", Input).focus()
        elif current_id == "priority-input":
            self.query_one("#delay-input", Input).focus()
        else:
            self.action_submit_job()

    def _show_message(self, message: str) -> None:
        """Show a message on the message line."""
        msg_line = self.query_one("#message-line", Static)
        msg_line.update(f" {message}")
