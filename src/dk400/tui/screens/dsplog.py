"""
DSPLOG - Display Log

AS/400-style system log display.
"""
from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Header, Footer, Static
from textual.binding import Binding
from textual.containers import VerticalScroll
from datetime import datetime
from typing import Any
import os
import socket
import subprocess


def get_system_logs(lines: int = 50) -> list[dict[str, Any]]:
    """Fetch system logs from various sources."""
    logs = []

    # Try to get Celery worker logs
    try:
        result = subprocess.run(
            ['docker', 'logs', '--tail', str(lines), 'celery-qbatch'],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.stdout or result.stderr:
            output = result.stderr or result.stdout  # Celery logs to stderr
            for line in output.strip().split('\n')[-lines:]:
                if line.strip():
                    # Parse timestamp if present
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    if '[' in line and ']' in line:
                        try:
                            ts_part = line.split(']')[0].split('[')[-1]
                            if ':' in ts_part:
                                timestamp = ts_part.split(' ')[-1][:8]
                        except Exception:
                            pass

                    # Determine severity
                    severity = 'INFO'
                    if 'ERROR' in line.upper():
                        severity = 'ERROR'
                    elif 'WARNING' in line.upper() or 'WARN' in line.upper():
                        severity = 'WARN'
                    elif 'DEBUG' in line.upper():
                        severity = 'DEBUG'

                    logs.append({
                        'timestamp': timestamp,
                        'severity': severity,
                        'source': 'QBATCH',
                        'message': line[-60:] if len(line) > 60 else line,
                    })
    except Exception:
        pass

    # Try to get celery-beat logs
    try:
        result = subprocess.run(
            ['docker', 'logs', '--tail', '20', 'celery-beat'],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.stdout or result.stderr:
            output = result.stderr or result.stdout
            for line in output.strip().split('\n')[-20:]:
                if line.strip():
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    severity = 'INFO'
                    if 'ERROR' in line.upper():
                        severity = 'ERROR'
                    elif 'WARNING' in line.upper():
                        severity = 'WARN'

                    logs.append({
                        'timestamp': timestamp,
                        'severity': severity,
                        'source': 'QSCD',
                        'message': line[-60:] if len(line) > 60 else line,
                    })
    except Exception:
        pass

    # Sort by timestamp (newest first for display, but we'll reverse for AS/400 style)
    # AS/400 shows oldest first typically
    if not logs:
        logs.append({
            'timestamp': datetime.now().strftime("%H:%M:%S"),
            'severity': 'INFO',
            'source': 'QSYSOPR',
            'message': 'No log entries found',
        })

    return logs[-lines:]  # Return last N entries


class DspLogScreen(Screen):
    """DSPLOG - Display Log screen."""

    BINDINGS = [
        Binding("f3", "back", "Exit", show=True),
        Binding("f5", "refresh", "Refresh", show=True),
        Binding("f12", "back", "Cancel", show=True),
        Binding("pageup", "page_up", "Page Up", show=False),
        Binding("pagedown", "page_down", "Page Down", show=False),
    ]

    CSS = """
    DspLogScreen {
        background: #000000;
    }

    #dsplog-header {
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

    #log-list {
        background: #000000;
        height: 1fr;
        padding: 0 1;
    }

    .log-entry {
        color: #00ff00;
        background: #000000;
        height: 1;
    }

    .log-error {
        color: #ff5555;
    }

    .log-warn {
        color: #ffff55;
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

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(self._get_header_text(), id="dsplog-header")
        yield Static(
            " Time      Sev    Source    Message",
            id="column-header"
        )
        yield VerticalScroll(id="log-list")
        yield Static(" F5=Refresh   PageUp/PageDown to scroll", id="message-line")
        yield Footer()

    def _get_header_text(self) -> str:
        """Generate header text."""
        timestamp = datetime.now().strftime("%m/%d/%y  %H:%M:%S")
        hostname = socket.gethostname().upper()[:10]
        user = getattr(self.app, 'current_user', 'QUSER')
        return (
            f" {hostname}                      Display Log                          {user}\n"
            f"                                                          {timestamp}"
        )

    def on_mount(self) -> None:
        """Initialize the log display."""
        self._refresh_logs()

    def _refresh_logs(self) -> None:
        """Refresh the log entries."""
        log_list = self.query_one("#log-list", VerticalScroll)
        log_list.remove_children()

        logs = get_system_logs(100)

        for log in logs:
            sev = log['severity']
            css_class = "log-entry"
            if sev == 'ERROR':
                css_class = "log-entry log-error"
            elif sev == 'WARN':
                css_class = "log-entry log-warn"

            entry_text = f" {log['timestamp']:<9} {log['severity']:<6} {log['source']:<9} {log['message']}"
            log_list.mount(Static(entry_text, classes=css_class))

        self.query_one("#dsplog-header", Static).update(self._get_header_text())

    def action_refresh(self) -> None:
        """Refresh log entries."""
        self._refresh_logs()

    def action_back(self) -> None:
        """Return to previous screen."""
        self.app.pop_screen()

    def action_page_up(self) -> None:
        """Scroll up."""
        log_list = self.query_one("#log-list", VerticalScroll)
        log_list.scroll_page_up()

    def action_page_down(self) -> None:
        """Scroll down."""
        log_list = self.query_one("#log-list", VerticalScroll)
        log_list.scroll_page_down()
