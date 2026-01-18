"""
DK/400 Textual Application

Main application class with AS/400-style green screen theme.
"""
from textual.app import App, ComposeResult
from textual.screen import Screen
from textual.widgets import Header, Footer, Static, Input
from textual.containers import Container, Vertical, Horizontal
from textual.binding import Binding
from datetime import datetime
import socket

from src.dk400.tui.screens.wrkactjob import WrkActJobScreen


LOGO = r"""
  ____  _  ______ ___   ___   ___
 |  _ \| |/ / / // _ \ / _ \ / _ \
 | | | | ' / / /| | | | | | | | | |
 | |_| | . \/ /_| |_| | |_| | |_| |
 |____/|_|\_\____\___/ \___/ \___/
"""


def get_system_info() -> tuple[str, str, str]:
    """Get system hostname and current timestamp."""
    hostname = socket.gethostname().upper()[:12]
    date_str = datetime.now().strftime("%m/%d/%y")
    time_str = datetime.now().strftime("%H:%M:%S")
    return hostname, date_str, time_str


class SignOnScreen(Screen):
    """AS/400-style sign-on screen."""

    BINDINGS = [
        Binding("enter", "sign_on", "Sign On", show=False),
        Binding("f3", "exit_session", "Exit", show=True),
    ]

    def action_exit_session(self) -> None:
        """Exit the application completely (ends SSH session)."""
        self.app.exit()

    CSS = """
    SignOnScreen {
        background: #000000;
    }

    #signon-container {
        width: 100%;
        height: 100%;
        align: center middle;
    }

    #signon-box {
        width: 76;
        height: 22;
        border: solid #00ff00;
        background: #000000;
        padding: 1 2;
    }

    .signon-title {
        text-align: center;
        color: #00ff00;
        text-style: bold;
        width: 100%;
    }

    .signon-header {
        color: #00ff00;
        width: 100%;
        height: auto;
    }

    .signon-row {
        height: 1;
        width: 100%;
        margin-top: 1;
    }

    .signon-label {
        color: #00ff00;
        width: 35;
    }

    .signon-input {
        width: 20;
        background: #000000;
        color: #00ff00;
        border: none;
    }

    .signon-input:focus {
        background: #003300;
    }

    .signon-footer {
        color: #00ff00;
        text-align: center;
        width: 100%;
        margin-top: 2;
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
        hostname, date_str, time_str = get_system_info()

        yield Header(show_clock=True)
        yield Container(
            Vertical(
                Static("Sign On", classes="signon-title"),
                Static("", classes="signon-header"),
                Static(
                    f"  System  . . . . . :   {hostname:<12}",
                    classes="signon-header"
                ),
                Static(
                    f"  Subsystem . . . . :   QINTER",
                    classes="signon-header"
                ),
                Static(
                    f"  Display . . . . . :   DSP01",
                    classes="signon-header"
                ),
                Static("", classes="signon-header"),
                Horizontal(
                    Static("  User  . . . . . . . . . . . . :  ", classes="signon-label"),
                    Input(placeholder="", id="user-input", classes="signon-input"),
                    classes="signon-row"
                ),
                Horizontal(
                    Static("  Password  . . . . . . . . . . :  ", classes="signon-label"),
                    Input(placeholder="", password=True, id="password-input", classes="signon-input"),
                    classes="signon-row"
                ),
                Horizontal(
                    Static("  Program/procedure . . . . . . :  ", classes="signon-label"),
                    Input(placeholder="", id="program-input", classes="signon-input"),
                    classes="signon-row"
                ),
                Horizontal(
                    Static("  Menu  . . . . . . . . . . . . :  ", classes="signon-label"),
                    Input(placeholder="", id="menu-input", classes="signon-input"),
                    classes="signon-row"
                ),
                Horizontal(
                    Static("  Current library . . . . . . . :  ", classes="signon-label"),
                    Input(placeholder="", id="library-input", classes="signon-input"),
                    classes="signon-row"
                ),
                Static("", classes="signon-header"),
                Static("(c) COPYRIGHT IBM CORP. 1980, 2024.", classes="signon-footer"),
                id="signon-box"
            ),
            id="signon-container"
        )
        yield Footer()

    def on_mount(self) -> None:
        """Focus the user input on mount."""
        self.query_one("#user-input", Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle enter key on inputs."""
        if event.input.id == "user-input":
            self.query_one("#password-input", Input).focus()
        elif event.input.id == "password-input":
            self.action_sign_on()
        else:
            # For other fields, just move to next or sign on
            self.action_sign_on()

    def action_sign_on(self) -> None:
        """Process sign-on and switch to main menu."""
        user = self.query_one("#user-input", Input).value.strip()
        if not user:
            user = "QUSER"

        # Store user in app for display
        self.app.current_user = user.upper()
        self.app.push_screen("main")


class MainMenuScreen(Screen):
    """Main menu screen."""

    BINDINGS = [
        Binding("f3", "sign_off", "Sign Off", show=True),
        Binding("f5", "refresh", "Refresh", show=True),
        Binding("f12", "previous", "Cancel", show=True),
    ]

    # Command mapping - AS/400 commands to actions
    COMMANDS = {
        # Full commands
        'WRKACTJOB': 'wrkactjob',
        'WRKJOBQ': 'wrkjobq',
        'WRKSVC': 'wrksvc',
        'DSPSYSSTS': 'dspsyssts',
        'DSPLOG': 'dsplog',
        'SBMJOB': 'sbmjob',
        'SIGNOFF': 'signoff',
        # Menu numbers
        '1': 'wrkactjob',
        '2': 'wrkjobq',
        '3': 'wrksvc',
        '4': 'dspsyssts',
        '5': 'dsplog',
        '6': 'sbmjob',
        '90': 'signoff',
    }

    CSS = """
    MainMenuScreen {
        background: #000000;
    }

    #menu-content {
        color: #00ff00;
        background: #000000;
        padding: 1 2;
        width: 100%;
        height: 1fr;
    }

    #cmd-line-container {
        height: auto;
        width: 100%;
        padding: 0 2;
        background: #000000;
    }

    #cmd-label {
        color: #00ff00;
        background: #000000;
        width: auto;
        height: 1;
    }

    #cmd-input {
        background: #000000;
        color: #00ff00;
        border: none;
        width: 1fr;
        height: 1;
        padding: 0;
    }

    #cmd-input:focus {
        background: #002200;
    }

    #message-line {
        color: #ffff00;
        background: #000000;
        height: 1;
        padding: 0 2;
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
        yield Static(self.get_menu_text(), id="menu-content")
        yield Static("", id="message-line")
        yield Horizontal(
            Static("  ===> ", id="cmd-label"),
            Input(placeholder="", id="cmd-input"),
            id="cmd-line-container"
        )
        yield Footer()

    def get_menu_text(self) -> str:
        """Generate the main menu text."""
        hostname, date_str, time_str = get_system_info()
        user = getattr(self.app, 'current_user', 'QUSER')

        return f"""{LOGO}
                                        Main Menu

                                        System: {hostname}
                                        User:   {user}
                                        {date_str}  {time_str}

  Select one of the following:

       1. Work with active jobs         WRKACTJOB
       2. Work with job queues          WRKJOBQ
       3. Work with services            WRKSVC
       4. Display system status         DSPSYSSTS
       5. Display log                   DSPLOG
       6. Submit job                    SBMJOB

      90. Sign off                      SIGNOFF


  Selection or command"""

    def on_mount(self) -> None:
        """Focus command input on mount."""
        self.query_one("#menu-content", Static).update(self.get_menu_text())
        self.query_one("#cmd-input", Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle command input."""
        if event.input.id == "cmd-input":
            cmd = event.input.value.strip().upper()
            event.input.value = ""  # Clear input

            if not cmd:
                self._show_message("")
                return

            self._execute_command(cmd)

    def _execute_command(self, cmd: str) -> None:
        """Execute an AS/400 command."""
        # Check for exact match first
        if cmd in self.COMMANDS:
            action = self.COMMANDS[cmd]
            method = getattr(self, f'action_{action}', None)
            if method:
                self._show_message("")
                method()
                return

        # Check for partial command match (e.g., "WRKA" matches "WRKACTJOB")
        matches = [c for c in self.COMMANDS.keys() if c.startswith(cmd) and not c.isdigit()]
        if len(matches) == 1:
            action = self.COMMANDS[matches[0]]
            method = getattr(self, f'action_{action}', None)
            if method:
                self._show_message("")
                method()
                return
        elif len(matches) > 1:
            self._show_message(f"Ambiguous command: {', '.join(matches)}")
            return

        self._show_message(f"Command {cmd} not found")

    def _show_message(self, message: str) -> None:
        """Show message on the message line."""
        self.query_one("#message-line", Static).update(f"  {message}")

    def action_refresh(self) -> None:
        """Refresh the display."""
        self.query_one("#menu-content", Static).update(self.get_menu_text())
        self._show_message("")

    def action_sign_off(self) -> None:
        """Sign off and return to sign-on screen."""
        self.app.pop_screen()

    def action_signoff(self) -> None:
        """Sign off via menu option."""
        self.app.pop_screen()

    def action_previous(self) -> None:
        """Return to previous (sign-on)."""
        self.app.pop_screen()

    def action_wrkactjob(self) -> None:
        """Work with active jobs."""
        self.app.push_screen("wrkactjob")

    def action_wrkjobq(self) -> None:
        self._show_message("WRKJOBQ - Coming soon")

    def action_wrksvc(self) -> None:
        self._show_message("WRKSVC - Coming soon")

    def action_dspsyssts(self) -> None:
        self._show_message("DSPSYSSTS - Coming soon")

    def action_dsplog(self) -> None:
        self._show_message("DSPLOG - Coming soon")

    def action_sbmjob(self) -> None:
        self._show_message("SBMJOB - Coming soon")


class DK400App(App):
    """DK/400 - AS/400 style terminal application."""

    TITLE = "DK/400"
    SUB_TITLE = "Job Queue System"

    CSS = """
    Screen {
        background: #000000;
    }

    Static {
        color: #00ff00;
        background: #000000;
    }
    """

    SCREENS = {
        "signon": SignOnScreen,
        "main": MainMenuScreen,
        "wrkactjob": WrkActJobScreen,
    }

    def __init__(self):
        super().__init__()
        self.current_user = "QUSER"

    def on_mount(self) -> None:
        """Start with sign-on screen."""
        self.push_screen("signon")


if __name__ == "__main__":
    app = DK400App()
    app.run()
