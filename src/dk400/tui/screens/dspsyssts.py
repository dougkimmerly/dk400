"""
DSPSYSSTS - Display System Status

Classic AS/400-style system status display.
"""
from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Header, Footer, Static
from textual.binding import Binding
from datetime import datetime, timedelta
from typing import Any
import os
import socket

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
        'memory_total_mb': 0,
        'memory_used_mb': 0,
        'memory_pct': 0.0,
        'disk_total_mb': 0,
        'disk_used_mb': 0,
        'disk_pct': 0.0,
        'elapsed_time': '00:00:00',
        'uptime_seconds': 0,
        'jobs_in_system': 0,
        'jobs_active': 0,
        'jobs_waiting': 0,
        'celery_workers': 0,
        'celery_active': 0,
        'celery_reserved': 0,
        'db_capability': 0.0,
        'cpu_count': 1,
    }

    try:
        stats['cpu_count'] = os.cpu_count() or 1
    except Exception:
        pass

    try:
        # CPU - read load average
        with open('/proc/loadavg', 'r') as f:
            loadavg = f.read().split()
            load_1 = float(loadavg[0])
            stats['cpu_pct'] = min(100.0, (load_1 / stats['cpu_count']) * 100)
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

            stats['memory_total_mb'] = total // 1024
            stats['memory_used_mb'] = used // 1024
            stats['memory_pct'] = (used / total * 100) if total > 0 else 0
    except Exception:
        pass

    try:
        # Disk - use statvfs on root
        statvfs = os.statvfs('/')
        total = statvfs.f_blocks * statvfs.f_frsize
        free = statvfs.f_bfree * statvfs.f_frsize
        used = total - free

        stats['disk_total_mb'] = total // (1024 * 1024)
        stats['disk_used_mb'] = used // (1024 * 1024)
        stats['disk_pct'] = (used / total * 100) if total > 0 else 0
    except Exception:
        pass

    try:
        # Uptime and elapsed time
        with open('/proc/uptime', 'r') as f:
            uptime_seconds = float(f.read().split()[0])
            stats['uptime_seconds'] = int(uptime_seconds)
            hours = int(uptime_seconds) // 3600
            minutes = (int(uptime_seconds) % 3600) // 60
            seconds = int(uptime_seconds) % 60
            stats['elapsed_time'] = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    except Exception:
        pass

    try:
        # Process/Job counts
        procs = [p for p in os.listdir('/proc') if p.isdigit()]
        stats['jobs_in_system'] = len(procs)

        # Count running vs sleeping
        running = 0
        sleeping = 0
        for pid in procs[:100]:  # Sample first 100 to avoid slowdown
            try:
                with open(f'/proc/{pid}/stat', 'r') as f:
                    state = f.read().split()[2]
                    if state == 'R':
                        running += 1
                    elif state in ('S', 'D'):
                        sleeping += 1
            except Exception:
                pass

        stats['jobs_active'] = running
        stats['jobs_waiting'] = sleeping
    except Exception:
        pass

    try:
        # Celery stats
        app = get_celery_app()
        inspect = app.control.inspect()

        ping_result = inspect.ping() or {}
        stats['celery_workers'] = len(ping_result)

        active = inspect.active() or {}
        stats['celery_active'] = sum(len(tasks) for tasks in active.values())

        reserved = inspect.reserved() or {}
        stats['celery_reserved'] = sum(len(tasks) for tasks in reserved.values())

    except Exception:
        pass

    # DB capability - fake it based on memory
    stats['db_capability'] = min(100.0, stats['memory_pct'] * 0.3)

    return stats


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
        """Generate the system status display in traditional AS/400 format."""
        timestamp = datetime.now().strftime("%m/%d/%y  %H:%M:%S")
        hostname = socket.gethostname().upper()[:10]
        stats = get_system_stats()

        # Calculate pool sizes (fake AS/400-style pools from Linux memory)
        total_mb = stats['memory_total_mb']
        machine_pool = int(total_mb * 0.10)  # 10% for kernel
        base_pool = int(total_mb * 0.50)     # 50% for base
        interact_pool = int(total_mb * 0.30) # 30% for interactive
        spool_pool = int(total_mb * 0.10)    # 10% for spool

        # Activity levels (fake based on CPU)
        cpu = stats['cpu_pct']
        machine_act = min(cpu * 0.3, 100)
        base_act = min(cpu * 0.5, 100)
        interact_act = min(cpu * 0.15, 100)
        spool_act = min(cpu * 0.05, 100)

        return f""" {hostname}                  Display System Status                    {timestamp}

 % CPU utilization . . . . . . . :    {stats['cpu_pct']:5.1f}
 Elapsed time  . . . . . . . . . :  {stats['elapsed_time']}
 Jobs in system  . . . . . . . . :    {stats['jobs_in_system']:5d}
 % perm addresses used . . . . . :     {stats['memory_pct'] * 0.1:4.1f}
 % temp addresses used . . . . . :     {stats['memory_pct'] * 0.05:4.1f}

 % system ASP used . . . . . . . :    {stats['disk_pct']:5.1f}
 Total aux storage (M) . . . . . :  {stats['disk_total_mb']:7d}

 % DB capability . . . . . . . . :    {stats['db_capability']:5.1f}
 % current processing capacity . :    {100.0:5.1f}
 Processing units available  . . :    {stats['cpu_count']:5d}.00

           --------Transition---------
 Pool      Reserved    Max    Allocated   Defined      Pool
           Size        Active               Size    ------------ Subsystem -------------
 *MACHINE       {machine_pool:5d}      ++     {int(machine_act):4d}      {machine_pool:5d}    Machine pool
 *BASE          {base_pool:5d}      ++     {int(base_act):4d}      {base_pool:5d}    QBATCH QINTER QSPL
 *INTERACT      {interact_pool:5d}      ++     {int(interact_act):4d}      {interact_pool:5d}    Interactive jobs
 *SPOOL         {spool_pool:5d}      ++     {int(spool_act):4d}      {spool_pool:5d}    Spooled files

                                  -------Jobs--------
 Subsystem      Subsystem   Active   Wait   Wait      Pool
 Name           Status      Jobs     Msg    Job       Name
 QBATCH         ACTIVE        {stats['celery_active']:4d}      0      {stats['celery_reserved']:3d}    *BASE
 QINTER         ACTIVE        {stats['jobs_active']:4d}      0        0    *INTERACT
 QSPL           ACTIVE           0      0        0    *SPOOL
 QCTL           ACTIVE           1      0        0    *MACHINE


                                                                          Bottom
 F3=Exit   F5=Refresh   F12=Cancel"""

    def on_mount(self) -> None:
        """Refresh on mount."""
        self.query_one("#dspsyssts-content", Static).update(self._get_status_text())

    def action_refresh(self) -> None:
        """Refresh the display."""
        self.query_one("#dspsyssts-content", Static).update(self._get_status_text())

    def action_back(self) -> None:
        """Return to previous screen."""
        self.app.pop_screen()
