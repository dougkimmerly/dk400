"""
WRKSVC - Work with Services

AS/400-style service display showing Docker containers.
"""
from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Header, Footer, Static, Input
from textual.binding import Binding
from textual.containers import Horizontal, VerticalScroll
from datetime import datetime
from typing import Any
import os
import socket
import subprocess


def get_docker_containers() -> list[dict[str, Any]]:
    """Fetch Docker container information."""
    containers = []

    try:
        # Use docker ps to get container info
        result = subprocess.run(
            ['docker', 'ps', '-a', '--format', '{{.Names}}\t{{.Status}}\t{{.Image}}\t{{.Ports}}'],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode == 0:
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) >= 3:
                    name = parts[0]
                    status = parts[1]
                    image = parts[2]
                    ports = parts[3] if len(parts) > 3 else ''

                    # Determine status code
                    if 'Up' in status:
                        status_code = 'ACTIVE'
                    elif 'Exited' in status:
                        status_code = 'ENDED'
                    elif 'Restarting' in status:
                        status_code = 'RSTRT'
                    else:
                        status_code = 'UNKNOWN'

                    # Parse uptime/downtime
                    elapsed = status.replace('Up ', '').replace('Exited ', '').split(' ')[0]

                    containers.append({
                        'service': name[:12].upper(),
                        'status': status_code,
                        'elapsed': elapsed[:10],
                        'image': image.split(':')[0].split('/')[-1][:15],
                        'ports': ports[:20] if ports else '-',
                    })

    except Exception as e:
        containers.append({
            'service': 'ERROR',
            'status': 'MSGW',
            'elapsed': '',
            'image': str(e)[:15],
            'ports': '',
        })

    return containers


class ServiceRow(Horizontal):
    """A single service row with option input."""

    DEFAULT_CSS = """
    ServiceRow {
        height: 1;
        width: 100%;
        background: #000000;
    }

    ServiceRow .opt-input {
        width: 4;
        min-width: 4;
        max-width: 4;
        height: 1;
        border: none;
        background: #000000;
        color: #00ff00;
        padding: 0;
    }

    ServiceRow .opt-input:focus {
        background: #003300;
    }

    ServiceRow .svc-field {
        color: #00ff00;
        background: #000000;
        height: 1;
        padding: 0;
    }
    """

    def __init__(self, svc_data: dict[str, Any], row_index: int) -> None:
        super().__init__()
        self.svc_data = svc_data
        self.row_index = row_index

    def compose(self) -> ComposeResult:
        yield Input(
            placeholder="",
            max_length=2,
            id=f"opt-{self.row_index}",
            classes="opt-input",
        )
        yield Static(f" {self.svc_data['service']:<12}", classes="svc-field")
        yield Static(f" {self.svc_data['status']:<7}", classes="svc-field")
        yield Static(f" {self.svc_data['elapsed']:<10}", classes="svc-field")
        yield Static(f" {self.svc_data['image']:<15}", classes="svc-field")
        yield Static(f" {self.svc_data['ports']:<20}", classes="svc-field")


class WrkSvcScreen(Screen):
    """WRKSVC - Work with Services screen."""

    BINDINGS = [
        Binding("f3", "back", "Exit", show=True),
        Binding("f5", "refresh", "Refresh", show=True),
        Binding("f12", "back", "Cancel", show=True),
        Binding("enter", "process_options", "Enter", show=False),
    ]

    CSS = """
    WrkSvcScreen {
        background: #000000;
    }

    #wrksvc-header {
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

    #svc-list {
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
        self.services: list[dict[str, Any]] = []

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(self._get_header_text(), id="wrksvc-header")
        yield Static(
            " Type options, press Enter.\n"
            "   2=Start   3=Stop   4=Restart   5=Logs   8=Details",
            id="help-text"
        )
        yield Static(
            " Opt  Service       Status   Elapsed     Image            Ports",
            id="column-header"
        )
        yield VerticalScroll(id="svc-list")
        yield Static("", id="message-line")
        yield Footer()

    def _get_header_text(self) -> str:
        """Generate header text."""
        timestamp = datetime.now().strftime("%m/%d/%y  %H:%M:%S")
        hostname = socket.gethostname().upper()[:10]
        user = getattr(self.app, 'current_user', 'QUSER')
        return (
            f" {hostname}                    Work with Services                    {user}\n"
            f"                                                          {timestamp}"
        )

    def on_mount(self) -> None:
        """Initialize the service list."""
        self._refresh_services()

    def _refresh_services(self) -> None:
        """Refresh the service list."""
        svc_list = self.query_one("#svc-list", VerticalScroll)
        svc_list.remove_children()

        self.services = get_docker_containers()

        if not self.services:
            svc_list.mount(Static(" No services found", classes="svc-field"))
        else:
            for i, svc in enumerate(self.services):
                svc_list.mount(ServiceRow(svc, i))

        self.query_one("#wrksvc-header", Static).update(self._get_header_text())

        if self.services:
            self.set_timer(0.1, self._focus_first_input)

    def _focus_first_input(self) -> None:
        """Focus the first option input."""
        try:
            first_input = self.query_one("#opt-0", Input)
            first_input.focus()
        except Exception:
            pass

    def action_refresh(self) -> None:
        """Refresh service list."""
        self._refresh_services()
        self._show_message("Service list refreshed")

    def action_back(self) -> None:
        """Return to previous screen."""
        self.app.pop_screen()

    def action_process_options(self) -> None:
        """Process all entered options."""
        options_to_process = []

        for i, svc in enumerate(self.services):
            try:
                opt_input = self.query_one(f"#opt-{i}", Input)
                opt_value = opt_input.value.strip()
                if opt_value:
                    options_to_process.append((opt_value, svc, i))
                    opt_input.value = ""
            except Exception:
                pass

        if not options_to_process:
            self._show_message("")
            return

        for opt, svc, idx in options_to_process:
            self._process_option(opt, svc)

    def _process_option(self, option: str, svc: dict[str, Any]) -> None:
        """Process a single option for a service."""
        svc_name = svc['service'].lower()

        if option == '2':
            # Start service
            try:
                subprocess.run(['docker', 'start', svc_name], capture_output=True, timeout=30)
                self._show_message(f"Service {svc_name} start requested")
            except Exception as e:
                self._show_message(f"Error: {e}")
        elif option == '3':
            # Stop service
            try:
                subprocess.run(['docker', 'stop', svc_name], capture_output=True, timeout=30)
                self._show_message(f"Service {svc_name} stop requested")
            except Exception as e:
                self._show_message(f"Error: {e}")
        elif option == '4':
            # Restart service
            try:
                subprocess.run(['docker', 'restart', svc_name], capture_output=True, timeout=30)
                self._show_message(f"Service {svc_name} restart requested")
            except Exception as e:
                self._show_message(f"Error: {e}")
        elif option == '5':
            self._show_message(f"Logs for {svc_name} - Coming soon")
        elif option == '8':
            self._show_message(f"Details for {svc_name} - Coming soon")
        else:
            self._show_message(f"Option {option} not valid")

        self.set_timer(1.0, self._refresh_services)

    def _show_message(self, message: str) -> None:
        """Show a message on the message line."""
        msg_line = self.query_one("#message-line", Static)
        msg_line.update(f" {message}")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle Enter on an input."""
        self.action_process_options()
