"""
DK/400 Textual Application

Main application class with AS/400-style green screen theme.
"""
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Static
from textual.binding import Binding
from datetime import datetime
import socket


LOGO = r"""
  ____  _  ______ ___   ___   ___
 |  _ \| |/ / / // _ \ / _ \ / _ \
 | | | | ' / / /| | | | | | | | | |
 | |_| | . \/ /_| |_| | |_| | |_| |
 |____/|_|\_\____\___/ \___/ \___/
"""


def get_main_menu_text() -> str:
    """Generate the main menu text with current timestamp."""
    hostname = socket.gethostname().upper()[:12]
    timestamp = datetime.now().strftime("%m/%d/%y  %H:%M:%S")

    return f"""{LOGO}
                                        Main Menu

                                        System: {hostname}
                                        {timestamp}

  Select one of the following:

       1. Work with active jobs         WRKACTJOB
       2. Work with job queues          WRKJOBQ
       3. Work with services            WRKSVC
       4. Display system status         DSPSYSSTS
       5. Display log                   DSPLOG
       6. Submit job                    SBMJOB


  Selection or command
  ===> _
"""


class MainMenuScreen(Static):
    """Main menu display widget."""

    def compose(self) -> ComposeResult:
        yield Static(get_main_menu_text(), id="menu-content")


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

    Header {
        background: #003300;
        color: #00ff00;
    }

    Footer {
        background: #003300;
        color: #00ff00;
    }

    #menu-content {
        padding: 1 2;
        width: 100%;
        height: 100%;
    }
    """

    BINDINGS = [
        Binding("f3", "quit", "Exit", show=True),
        Binding("f5", "refresh", "Refresh", show=True),
        Binding("f12", "quit", "Cancel", show=True),
        Binding("1", "wrkactjob", "WRKACTJOB", show=False),
        Binding("2", "wrkjobq", "WRKJOBQ", show=False),
        Binding("3", "wrksvc", "WRKSVC", show=False),
        Binding("4", "dspsyssts", "DSPSYSSTS", show=False),
        Binding("5", "dsplog", "DSPLOG", show=False),
        Binding("6", "sbmjob", "SBMJOB", show=False),
    ]

    def compose(self) -> ComposeResult:
        """Create child widgets."""
        yield Header(show_clock=True)
        yield MainMenuScreen()
        yield Footer()

    def action_refresh(self) -> None:
        """Refresh the display."""
        self.query_one("#menu-content", Static).update(get_main_menu_text())

    def action_wrkactjob(self) -> None:
        """Work with active jobs - placeholder."""
        self.notify("WRKACTJOB - Coming soon!", title="DK/400")

    def action_wrkjobq(self) -> None:
        """Work with job queues - placeholder."""
        self.notify("WRKJOBQ - Coming soon!", title="DK/400")

    def action_wrksvc(self) -> None:
        """Work with services - placeholder."""
        self.notify("WRKSVC - Coming soon!", title="DK/400")

    def action_dspsyssts(self) -> None:
        """Display system status - placeholder."""
        self.notify("DSPSYSSTS - Coming soon!", title="DK/400")

    def action_dsplog(self) -> None:
        """Display log - placeholder."""
        self.notify("DSPLOG - Coming soon!", title="DK/400")

    def action_sbmjob(self) -> None:
        """Submit job - placeholder."""
        self.notify("SBMJOB - Coming soon!", title="DK/400")


if __name__ == "__main__":
    app = DK400App()
    app.run()
