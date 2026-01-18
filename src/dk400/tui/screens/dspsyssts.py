"""
DSPSYSSTS - Display System Status

Classic AS/400-style system status display.
"""
from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Header, Footer, Static
from textual.binding import Binding
from textual.containers import Vertical
from datetime import datetime, timedelta
from typing import Any
import os
import subprocess

# Celery imports
from celery import Celery


def get_celery_app() -> Celery:
    """Get Celery app connection."""
    broker_url = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
    app = Celery('dk400', broker=broker_url)
    return app


def get_system_stats() -> dict[str, Any]:
    """Gather system statistics."""
    stats = {
        'cpu_pct': 0.0,
        'memory_total': 0,
        'memory_used': 0,
        'memory_pct': 0.0,
        'disk_total': 0,
        'disk_used': 0,
        'disk_pct': 0.0,
        'uptime': '0d 0h 0m',
        'load_1': 0.0,
        'load_5': 0.0,
        'load_15': 0.0,
        'processes': 0,
        'celery_workers': 0,
        'celery_active': 0,
        'celery_scheduled': 0,
        'celery_reserved': 0,
    }

    try:
        # CPU - read from /proc/stat or use load average as proxy
        with open('/proc/loadavg', 'r') as f:
            loadavg = f.read().split()
            stats['load_1'] = float(loadavg[0])
            stats['load_5'] = float(loadavg[1])
            stats['load_15'] = float(loadavg[2])
            # Estimate CPU% from 1-min load (rough approximation)
            cpu_count = os.cpu_count() or 1
            stats['cpu_pct'] = min(100.0, (stats['load_1'] / cpu_count) * 100)
    except Exception:
        pass

    try:
        # Memory - read from /proc/meminfo
        with open('/proc/meminfo', 'r') as f:
            meminfo = {}
            for line in f:
                parts = line.split(':')
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip().split()[0]
                    meminfo[key] = int(value)

            total = meminfo.get('MemTotal', 0)
            available = meminfo.get('MemAvailable', 0)
            used = total - available

            stats['memory_total'] = total // 1024  # MB
            stats['memory_used'] = used // 1024  # MB
            stats['memory_pct'] = (used / total * 100) if total > 0 else 0
    except Exception:
        pass

    try:
        # Disk - use statvfs on root
        statvfs = os.statvfs('/')
        total = statvfs.f_blocks * statvfs.f_frsize
        free = statvfs.f_bfree * statvfs.f_frsize
        used = total - free

        stats['disk_total'] = total // (1024 ** 3)  # GB
        stats['disk_used'] = used // (1024 ** 3)  # GB
        stats['disk_pct'] = (used / total * 100) if total > 0 else 0
    except Exception:
        pass

    try:
        # Uptime - read from /proc/uptime
        with open('/proc/uptime', 'r') as f:
            uptime_seconds = float(f.read().split()[0])
            uptime_td = timedelta(seconds=int(uptime_seconds))
            days = uptime_td.days
            hours = uptime_td.seconds // 3600
            minutes = (uptime_td.seconds % 3600) // 60
            stats['uptime'] = f"{days}d {hours}h {minutes}m"
    except Exception:
        pass

    try:
        # Process count
        stats['processes'] = len([p for p in os.listdir('/proc') if p.isdigit()])
    except Exception:
        pass

    try:
        # Celery stats
        app = get_celery_app()
        inspect = app.control.inspect()

        # Worker count
        ping_result = inspect.ping() or {}
        stats['celery_workers'] = len(ping_result)

        # Active tasks
        active = inspect.active() or {}
        stats['celery_active'] = sum(len(tasks) for tasks in active.values())

        # Reserved tasks
        reserved = inspect.reserved() or {}
        stats['celery_reserved'] = sum(len(tasks) for tasks in reserved.values())

        # Scheduled tasks
        scheduled = inspect.scheduled() or {}
        stats['celery_scheduled'] = sum(len(tasks) for tasks in scheduled.values())

    except Exception:
        pass

    return stats


def format_bytes_display(mb: int) -> str:
    """Format memory in appropriate units."""
    if mb >= 1024:
        return f"{mb / 1024:.1f} GB"
    return f"{mb} MB"


class DspSysStsScreen(Screen):
    """DSPSYSSTS - Display System Status screen."""

    BINDINGS = [
        Binding("f3", "back", "Exit", show=True),
        Binding("f5", "refresh", "Refresh", show=True),
        Binding("f12", "back", "Cancel", show=True),
    ]

    CSS = """
    DspSysStsScreen {
        background: #000000;
    }

    #dspsyssts-content {
        color: #00ff00;
        background: #000000;
        padding: 0 1;
        width: 100%;
        height: 1fr;
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
        yield Static(self._get_status_text(), id="dspsyssts-content")
        yield Footer()

    def _get_status_text(self) -> str:
        """Generate the system status display."""
        timestamp = datetime.now().strftime("%m/%d/%y  %H:%M:%S")
        user = getattr(self.app, 'current_user', 'QUSER')
        stats = get_system_stats()

        # Create ASCII progress bars
        cpu_bar = self._make_bar(stats['cpu_pct'])
        mem_bar = self._make_bar(stats['memory_pct'])
        disk_bar = self._make_bar(stats['disk_pct'])

        return f"""
                           Display System Status                      {user}
                                                          {timestamp}

  System:   DK400          Uptime:  {stats['uptime']}

  ─────────────────────────────────────────────────────────────────────────

  CPU RESOURCES:
    CPU utilization  . . . . . . . :   {stats['cpu_pct']:5.1f} %   {cpu_bar}
    Load average (1/5/15 min)  . . :   {stats['load_1']:.2f} / {stats['load_5']:.2f} / {stats['load_15']:.2f}

  MEMORY RESOURCES:
    Total memory . . . . . . . . . :   {format_bytes_display(stats['memory_total'])}
    Memory used  . . . . . . . . . :   {format_bytes_display(stats['memory_used'])}
    Memory utilization . . . . . . :   {stats['memory_pct']:5.1f} %   {mem_bar}

  DISK RESOURCES:
    Total disk space . . . . . . . :   {stats['disk_total']} GB
    Disk space used  . . . . . . . :   {stats['disk_used']} GB
    Disk utilization . . . . . . . :   {stats['disk_pct']:5.1f} %   {disk_bar}

  ─────────────────────────────────────────────────────────────────────────

  JOB QUEUE STATUS (QBATCH):
    Active workers . . . . . . . . :   {stats['celery_workers']}
    Jobs running . . . . . . . . . :   {stats['celery_active']}
    Jobs on queue  . . . . . . . . :   {stats['celery_reserved']}
    Jobs scheduled . . . . . . . . :   {stats['celery_scheduled']}

  SYSTEM ACTIVITY:
    Active processes . . . . . . . :   {stats['processes']}


                                                      Press F5 to refresh
"""

    def _make_bar(self, pct: float, width: int = 20) -> str:
        """Create an ASCII progress bar."""
        filled = int((pct / 100) * width)
        empty = width - filled
        return f"[{'█' * filled}{'░' * empty}]"

    def on_mount(self) -> None:
        """Refresh on mount."""
        self.query_one("#dspsyssts-content", Static).update(self._get_status_text())

    def action_refresh(self) -> None:
        """Refresh the display."""
        self.query_one("#dspsyssts-content", Static).update(self._get_status_text())

    def action_back(self) -> None:
        """Return to previous screen."""
        self.app.pop_screen()
