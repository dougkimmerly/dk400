"""
DK/400 Screen Definitions

AS/400-style screen layouts with fixed 80 or 132 column grids.
"""
import os
import socket
import subprocess
from datetime import datetime
from typing import Any, Optional
from dataclasses import dataclass, field

from celery import Celery


# Screen dimensions
COLS_80 = 80
COLS_132 = 132
ROWS_24 = 24
ROWS_27 = 27


def get_celery_app() -> Celery:
    """Get Celery app connection."""
    broker_url = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
    app = Celery('dk400', broker=broker_url)
    return app


def get_system_info() -> tuple[str, str, str]:
    """Get system hostname and current timestamp."""
    hostname = socket.gethostname().upper()[:12]
    date_str = datetime.now().strftime("%m/%d/%y")
    time_str = datetime.now().strftime("%H:%M:%S")
    return hostname, date_str, time_str


def pad_line(text: str, width: int = COLS_80) -> str:
    """Pad a line to exact width."""
    if len(text) > width:
        return text[:width]
    return text.ljust(width)


def center_text(text: str, width: int = COLS_80) -> str:
    """Center text within width."""
    return text.center(width)


LOGO = """\
  ____  _  ______ ___   ___   ___
 |  _ \\| |/ / / // _ \\ / _ \\ / _ \\
 | | | | ' / / /| | | | | | | | | |
 | |_| | . \\/ /_| |_| | |_| | |_| |
 |____/|_|\\_\\____|\\___/ \\___/ \\___/ """


@dataclass
class Session:
    """User session state."""
    session_id: str
    user: str = "QUSER"
    current_screen: str = "signon"
    field_values: dict = field(default_factory=dict)
    message: str = ""
    message_level: str = "info"
    page_offsets: dict = field(default_factory=dict)  # {screen_name: offset}

    def get_offset(self, screen: str) -> int:
        """Get current page offset for a screen."""
        return self.page_offsets.get(screen, 0)

    def set_offset(self, screen: str, offset: int):
        """Set page offset for a screen."""
        self.page_offsets[screen] = max(0, offset)

    def reset_offset(self, screen: str):
        """Reset page offset when entering a screen."""
        self.page_offsets[screen] = 0


class ScreenManager:
    """Manages screen rendering and transitions."""

    COMMANDS = {
        'WRKACTJOB': 'wrkactjob',
        'WRKJOBQ': 'wrkjobq',
        'WRKSVC': 'wrksvc',
        'DSPSYSSTS': 'dspsyssts',
        'DSPLOG': 'dsplog',
        'SBMJOB': 'sbmjob',
        'SIGNOFF': 'signon',
        'GO': 'main',
        '1': 'wrkactjob',
        '2': 'wrkjobq',
        '3': 'wrksvc',
        '4': 'dspsyssts',
        '5': 'dsplog',
        '6': 'sbmjob',
        '90': 'signon',
    }

    def get_screen(self, session: Session, screen_name: str) -> dict:
        """Get screen data for rendering."""
        session.current_screen = screen_name
        method = getattr(self, f'_screen_{screen_name}', None)
        if method:
            return method(session)
        return self._screen_main(session)

    def handle_submit(self, session: Session, screen: str, fields: dict) -> dict:
        """Handle screen submission (Enter key)."""
        session.field_values.update(fields)
        method = getattr(self, f'_submit_{screen}', None)
        if method:
            return method(session, fields)
        cmd = fields.get('cmd', '').strip().upper()
        if cmd:
            return self.execute_command(session, cmd)
        return self.get_screen(session, session.current_screen)

    def handle_function_key(self, session: Session, screen: str, key: str, fields: dict) -> dict:
        """Handle function key press."""
        session.field_values.update(fields)

        if key == 'F3':
            if screen == 'signon':
                return {'type': 'exit'}
            elif screen == 'main':
                return self.get_screen(session, 'signon')
            else:
                return self.get_screen(session, 'main')
        elif key == 'F5':
            return self.get_screen(session, screen)
        elif key == 'F12':
            if screen in ('signon', 'main'):
                return self.get_screen(session, screen)
            return self.get_screen(session, 'main')

        return self.get_screen(session, screen)

    # Page sizes for scrollable screens
    PAGE_SIZES = {
        'wrkactjob': 12,
        'wrkjobq': 10,
        'wrksvc': 15,
        'dsplog': 16,
    }

    def handle_roll(self, session: Session, screen: str, direction: str) -> dict:
        """Handle Roll Up/Roll Down (page up/down)."""
        page_size = self.PAGE_SIZES.get(screen, 10)
        current_offset = session.get_offset(screen)

        if direction == 'down':
            session.set_offset(screen, current_offset + page_size)
        elif direction == 'up':
            session.set_offset(screen, current_offset - page_size)

        return self.get_screen(session, screen)

    def execute_command(self, session: Session, command: str) -> dict:
        """Execute an AS/400 command."""
        if command in self.COMMANDS:
            session.message = ""
            return self.get_screen(session, self.COMMANDS[command])

        matches = [c for c in self.COMMANDS.keys() if c.startswith(command) and not c.isdigit()]
        if len(matches) == 1:
            session.message = ""
            return self.get_screen(session, self.COMMANDS[matches[0]])
        elif len(matches) > 1:
            session.message = f"Ambiguous command: {', '.join(matches)}"
            return self.get_screen(session, session.current_screen)

        session.message = f"Command {command} not found"
        session.message_level = "error"
        return self.get_screen(session, session.current_screen)

    # ========== SCREEN DEFINITIONS (80 columns) ==========

    def _screen_signon(self, session: Session) -> dict:
        """Sign-on screen - 80 columns."""
        hostname, date_str, time_str = get_system_info()

        content = [
            pad_line(""),
            pad_line(center_text("Sign On")),
            pad_line(""),
            pad_line(f"                         System  . . . . . :   {hostname}"),
            pad_line(f"                         Subsystem . . . . :   QINTER"),
            pad_line(f"                         Display . . . . . :   DSP01"),
            pad_line(""),
            [
                {"type": "text", "text": "                         User  . . . . . . . . . . . . . :  "},
                {"type": "input", "id": "user", "width": 10, "value": ""},
            ],
            [
                {"type": "text", "text": "                         Password  . . . . . . . . . . . :  "},
                {"type": "input", "id": "password", "width": 10, "password": True},
            ],
            [
                {"type": "text", "text": "                         Program/procedure . . . . . . . :  "},
                {"type": "input", "id": "program", "width": 10},
            ],
            [
                {"type": "text", "text": "                         Menu  . . . . . . . . . . . . . :  "},
                {"type": "input", "id": "menu", "width": 10},
            ],
            [
                {"type": "text", "text": "                         Current library . . . . . . . . :  "},
                {"type": "input", "id": "library", "width": 10},
            ],
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(center_text("(C) COPYRIGHT IBM CORP. 1980, 2024.")),
            pad_line(""),
        ]

        return {
            "type": "screen",
            "screen": "signon",
            "cols": 80,
            "content": content,
            "fields": [
                {"id": "user"},
                {"id": "password"},
                {"id": "program"},
                {"id": "menu"},
                {"id": "library"},
            ],
            "activeField": 0,
        }

    def _submit_signon(self, session: Session, fields: dict) -> dict:
        """Handle sign-on submission."""
        user = fields.get('user', '').strip().upper()
        if not user:
            user = 'QUSER'
        session.user = user
        return self.get_screen(session, 'main')

    def _screen_main(self, session: Session) -> dict:
        """Main menu screen - 80 columns."""
        hostname, date_str, time_str = get_system_info()

        content = []
        for line in LOGO.split('\n'):
            content.append(pad_line(line))

        content.extend([
            pad_line(""),
            pad_line(f"                                        Main Menu"),
            pad_line(""),
            pad_line(f"                                        System:   {hostname}"),
            pad_line(f"                                        User:     {session.user}"),
            pad_line(f"                                        {date_str}  {time_str}"),
            pad_line(""),
            pad_line("  Select one of the following:"),
            pad_line(""),
            pad_line("       1. Work with active jobs                            WRKACTJOB"),
            pad_line("       2. Work with job queues                             WRKJOBQ"),
            pad_line("       3. Work with services                               WRKSVC"),
            pad_line("       4. Display system status                            DSPSYSSTS"),
            pad_line("       5. Display log                                      DSPLOG"),
            pad_line("       6. Submit job                                       SBMJOB"),
            pad_line(""),
            pad_line("      90. Sign off                                         SIGNOFF"),
            pad_line(""),
            pad_line(""),
            pad_line("  Selection or command"),
            [
                {"type": "text", "text": "  ===> "},
                {"type": "input", "id": "cmd", "width": 66, "value": ""},
            ],
        ])

        if session.message:
            content.append([{"type": "text", "text": pad_line(f"  {session.message}"), "class": f"field-{session.message_level}"}])
            session.message = ""
        else:
            content.append(pad_line(""))

        content.append(pad_line(""))

        return {
            "type": "screen",
            "screen": "main",
            "cols": 80,
            "content": content,
            "fields": [{"id": "cmd"}],
            "activeField": 0,
        }

    def _screen_wrkactjob(self, session: Session) -> dict:
        """Work with Active Jobs - 80 columns."""
        hostname, date_str, time_str = get_system_info()
        all_jobs = self._get_celery_jobs()

        # Pagination
        page_size = self.PAGE_SIZES['wrkactjob']
        offset = session.get_offset('wrkactjob')
        total = len(all_jobs)

        # Clamp offset to valid range
        if offset >= total and total > 0:
            offset = max(0, total - page_size)
            session.set_offset('wrkactjob', offset)

        jobs = all_jobs[offset:offset + page_size]

        # Position indicator
        if total > page_size:
            if offset + page_size >= total:
                pos_indicator = "Bottom"
            elif offset == 0:
                pos_indicator = "More..."
            else:
                pos_indicator = "More..."
        else:
            pos_indicator = ""

        content = [
            pad_line(f" {hostname:<20}       Work with Active Jobs                  {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type options, press Enter."),
            pad_line("   2=Change   3=Hold   4=End   5=Work with   8=Spooled files"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  Job         User        Type   Status       CPU%  Function"), "class": "field-reverse"}],
        ]

        fields = []
        for i, job in enumerate(jobs):
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {job['name']:<10}  {job['user']:<10}  {job['type']:<5}  {job['status']:<10}  {job['cpu']:>4}  {job['function']:<15}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}"})

        while len(content) < 19:
            content.append(pad_line(""))

        # Position/More indicator on the right
        content.append(pad_line(f"                                                              {pos_indicator:>12}"))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        fields.append({"id": "cmd"})
        content.append(pad_line(" F3=Exit  F5=Refresh  F12=Cancel  PageDown=Roll Down  PageUp=Roll Up"))

        return {
            "type": "screen",
            "screen": "wrkactjob",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_wrkactjob(self, session: Session, fields: dict) -> dict:
        """Handle WRKACTJOB submission."""
        cmd = fields.get('cmd', '').strip().upper()
        if cmd:
            return self.execute_command(session, cmd)

        jobs = self._get_celery_jobs()
        for i, job in enumerate(jobs[:12]):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt == '4':
                self._revoke_celery_task(job.get('task_id'))
                session.message = f"Job {job['name']} end requested"
                break
            elif opt:
                session.message = f"Option {opt} not implemented"
                break

        return self.get_screen(session, 'wrkactjob')

    def _screen_dspsyssts(self, session: Session) -> dict:
        """Display System Status - 80 columns, compact single-screen view."""
        hostname, date_str, time_str = get_system_info()
        stats = self._get_system_stats()

        # Format memory in MB for pool display
        mem_mb = int(stats['mem_total_mb'])
        mem_used_mb = int(stats['mem_used_mb'])

        content = [
            pad_line(f"                       Display System Status                        {hostname}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(f" % CPU used  . . . . . . :    {stats['cpu_pct']:5.1f}   Auxiliary storage:"),
            pad_line(f" Elapsed time  . . . . . :  {stats['elapsed_time']}     System ASP . . . . . :    {stats['disk_pct']:5.1f} %"),
            pad_line(f" Jobs in system  . . . . :     {stats['jobs_in_system']:4d}       Total  . . . . . . . :  {stats['disk_total_gb']:7.1f} G"),
            pad_line(f" % perm addresses  . . . :    {stats['perm_addr_pct']:5.1f}       Used . . . . . . . . :  {stats['disk_used_gb']:7.1f} G"),
            pad_line(f" % temp addresses  . . . :    {stats['temp_addr_pct']:5.1f}       Available  . . . . . :  {stats['disk_avail_gb']:7.1f} G"),
            pad_line(""),
            pad_line(f" Main storage (MB): {mem_mb:>6}   Used: {mem_used_mb:>6}   % used: {stats['mem_pct']:5.1f}"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Pool  Subsystem   Size(M)  Defined  Max Act  ++Act  ++Wait  ++Fault"), "class": "field-highlight"}],
            pad_line(f"   1   *MACHINE     {stats['machine_pool']:5}    {stats['machine_pool']:5}      +++   {stats['machine_act']:4}       0       0"),
            pad_line(f"   2   *BASE        {stats['base_pool']:5}    {stats['base_pool']:5}      +++   {stats['base_act']:4}       0       0"),
            pad_line(f"   3   *INTERACT    {stats['interact_pool']:5}    {stats['interact_pool']:5}      +++   {stats['interact_act']:4}       0       0"),
            pad_line(f"   4   *SPOOL        {stats['spool_pool']:4}     {stats['spool_pool']:4}      +++     {stats['spool_act']:2}       0       0"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Subsystem    Status     Jobs  Type    Library"), "class": "field-highlight"}],
            pad_line(f" QBATCH       ACTIVE    {stats['celery_active']:4}   SBS     QSYS"),
            pad_line(f" QINTER       ACTIVE    {stats['docker_containers']:4}   SBS     QSYS"),
            pad_line(f" QSPL         ACTIVE       1   SBS     QSYS"),
            pad_line(f" QCTL         ACTIVE       1   SBS     QSYS"),
            pad_line(""),
            pad_line(" F3=Exit   F5=Refresh   F12=Cancel"),
            pad_line(""),
        ]

        return {
            "type": "screen",
            "screen": "dspsyssts",
            "cols": 80,
            "content": content,
            "fields": [],
            "activeField": 0,
        }

    def _screen_wrkjobq(self, session: Session) -> dict:
        """Work with Job Queues - 80 columns."""
        hostname, date_str, time_str = get_system_info()
        all_queues = self._get_celery_queues()

        # Pagination
        page_size = self.PAGE_SIZES['wrkjobq']
        offset = session.get_offset('wrkjobq')
        total = len(all_queues)

        if offset >= total and total > 0:
            offset = max(0, total - page_size)
            session.set_offset('wrkjobq', offset)

        queues = all_queues[offset:offset + page_size]

        # Position indicator
        if total > page_size:
            pos_indicator = "Bottom" if offset + page_size >= total else "More..."
        else:
            pos_indicator = ""

        content = [
            pad_line(f" {hostname:<20}       Work with Job Queues                  {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type options, press Enter."),
            pad_line("   5=Work with   6=Hold queue   7=Release queue   8=Work with jobs"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  Queue         Lib         Status      Jobs   Subsystem"), "class": "field-reverse"}],
        ]

        fields = []
        for i, q in enumerate(queues):
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {q['name']:<12}  {q['lib']:<10}  {q['status']:<10}  {q['jobs']:>4}   {q['subsystem']}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}"})

        while len(content) < 19:
            content.append(pad_line(""))

        content.append(pad_line(f"                                                              {pos_indicator:>12}"))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        fields.append({"id": "cmd"})
        content.append(pad_line(" F3=Exit  F5=Refresh  F12=Cancel  PageDown=Roll Down  PageUp=Roll Up"))

        return {
            "type": "screen",
            "screen": "wrkjobq",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _screen_wrksvc(self, session: Session) -> dict:
        """Work with Services (Docker) - 132 columns for more data."""
        hostname, date_str, time_str = get_system_info()
        all_services = self._get_docker_services()

        # Pagination
        page_size = self.PAGE_SIZES['wrksvc']
        offset = session.get_offset('wrksvc')
        total = len(all_services)

        if offset >= total and total > 0:
            offset = max(0, total - page_size)
            session.set_offset('wrksvc', offset)

        services = all_services[offset:offset + page_size]

        # Position indicator
        if total > page_size:
            pos_indicator = "Bottom" if offset + page_size >= total else "More..."
        else:
            pos_indicator = ""

        content = [
            pad_line(f" {hostname:<20}                       Work with Services                                    {session.user:>10}", 132),
            pad_line(f"                                                                                          {date_str}  {time_str}", 132),
            pad_line("", 132),
            pad_line(" Type options, press Enter.", 132),
            pad_line("   2=Start   3=Stop   4=Restart   5=Display logs   8=Display details", 132),
            pad_line("", 132),
            [{"type": "text", "text": pad_line(" Opt  Service          Status     Elapsed      Image                    Ports", 132), "class": "field-reverse"}],
        ]

        fields = []
        for i, svc in enumerate(services):
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {svc['name']:<15}  {svc['status']:<9}  {svc['elapsed']:<10}   {svc['image']:<23}  {svc['ports']:<30}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}"})

        while len(content) < 21:
            content.append(pad_line("", 132))

        content.append(pad_line(f"                                                                                                    {pos_indicator:>12}", 132))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}", 132))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 120},
        ])
        fields.append({"id": "cmd"})
        content.append(pad_line(" F3=Exit  F5=Refresh  F12=Cancel  PageDown=Roll Down  PageUp=Roll Up", 132))

        return {
            "type": "screen",
            "screen": "wrksvc",
            "cols": 132,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_wrksvc(self, session: Session, fields: dict) -> dict:
        """Handle WRKSVC submission."""
        cmd = fields.get('cmd', '').strip().upper()
        if cmd:
            return self.execute_command(session, cmd)

        services = self._get_docker_services()
        for i, svc in enumerate(services[:15]):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt == '2':
                self._docker_action(svc['name'], 'start')
                session.message = f"Service {svc['name']} start requested"
                break
            elif opt == '3':
                self._docker_action(svc['name'], 'stop')
                session.message = f"Service {svc['name']} stop requested"
                break
            elif opt == '4':
                self._docker_action(svc['name'], 'restart')
                session.message = f"Service {svc['name']} restart requested"
                break
            elif opt:
                session.message = f"Option {opt} not implemented"
                break

        return self.get_screen(session, 'wrksvc')

    def _screen_dsplog(self, session: Session) -> dict:
        """Display Log - 132 columns for full log messages."""
        hostname, date_str, time_str = get_system_info()
        all_logs = self._get_system_logs(limit=100)  # Get more logs for scrolling

        # Pagination
        page_size = self.PAGE_SIZES['dsplog']
        offset = session.get_offset('dsplog')
        total = len(all_logs)

        if offset >= total and total > 0:
            offset = max(0, total - page_size)
            session.set_offset('dsplog', offset)

        logs = all_logs[offset:offset + page_size]

        # Position indicator
        if total > page_size:
            pos_indicator = "Bottom" if offset + page_size >= total else "More..."
        else:
            pos_indicator = ""

        content = [
            pad_line(f" {hostname:<20}                          Display Log                                       {session.user:>10}", 132),
            pad_line(f"                                                                                          {date_str}  {time_str}", 132),
            pad_line("", 132),
            [{"type": "text", "text": pad_line(" Time       Severity  Source       Message", 132), "class": "field-reverse"}],
        ]

        for log in logs:
            css_class = ""
            if log['severity'] == 'ERROR':
                css_class = "field-error"
            elif log['severity'] == 'WARN':
                css_class = "field-warning"

            line = f" {log['time']:<10} {log['severity']:<8}  {log['source']:<10}   {log['message']:<90}"
            content.append([{"type": "text", "text": pad_line(line, 132), "class": css_class}])

        while len(content) < 21:
            content.append(pad_line("", 132))

        content.append(pad_line(f"                                                                                                    {pos_indicator:>12}", 132))
        content.append(pad_line("", 132))
        content.append(pad_line(" F3=Exit  F5=Refresh  F12=Cancel  PageDown=Roll Down  PageUp=Roll Up", 132))

        return {
            "type": "screen",
            "screen": "dsplog",
            "cols": 132,
            "content": content,
            "fields": [],
            "activeField": 0,
        }

    def _screen_sbmjob(self, session: Session) -> dict:
        """Submit Job - 80 columns."""
        hostname, date_str, time_str = get_system_info()
        tasks = self._get_available_tasks()

        content = [
            pad_line(f" {hostname:<20}            Submit Job (SBMJOB)                {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type choices, press Enter."),
            pad_line(""),
            [
                {"type": "text", "text": " Command to run  . . . . . . . . :  "},
                {"type": "input", "id": "task", "width": 35},
            ],
            [
                {"type": "text", "text": " Parameters  . . . . . . . . . . :  "},
                {"type": "input", "id": "params", "width": 35},
            ],
            [
                {"type": "text", "text": " Job queue . . . . . . . . . . . :  "},
                {"type": "input", "id": "queue", "width": 15, "value": "celery"},
            ],
            [
                {"type": "text", "text": " Delay (seconds) . . . . . . . . :  "},
                {"type": "input", "id": "delay", "width": 6, "value": "0"},
            ],
            pad_line(""),
            pad_line(" Available tasks:"),
        ]

        for task in tasks[:6]:
            content.append(pad_line(f"   {task}"))

        while len(content) < 21:
            content.append(pad_line(""))

        msg = session.message if session.message else ""
        css_class = f"field-{session.message_level}" if session.message_level else ""
        content.append([{"type": "text", "text": pad_line(f" {msg}"), "class": css_class}])
        session.message = ""

        content.append(pad_line(""))
        content.append(pad_line(""))

        return {
            "type": "screen",
            "screen": "sbmjob",
            "cols": 80,
            "content": content,
            "fields": [
                {"id": "task"},
                {"id": "params"},
                {"id": "queue"},
                {"id": "delay"},
            ],
            "activeField": 0,
        }

    def _submit_sbmjob(self, session: Session, fields: dict) -> dict:
        """Handle SBMJOB submission."""
        task = fields.get('task', '').strip()
        params = fields.get('params', '').strip()
        delay = fields.get('delay', '0').strip()

        if not task:
            session.message = "Command is required"
            session.message_level = "error"
            return self.get_screen(session, 'sbmjob')

        try:
            result_id = self._submit_celery_task(task, params, int(delay) if delay.isdigit() else 0)
            session.message = f"Job submitted: {result_id[:8]}"
            session.message_level = "info"
        except Exception as e:
            session.message = f"Error: {str(e)[:50]}"
            session.message_level = "error"

        return self.get_screen(session, 'sbmjob')

    # ========== DATA HELPERS ==========

    def _get_celery_jobs(self) -> list[dict]:
        """Get active Celery jobs."""
        jobs = []
        try:
            app = get_celery_app()
            inspect = app.control.inspect()

            active = inspect.active() or {}
            for worker, tasks in active.items():
                for task in tasks:
                    name = task.get('name', 'UNKNOWN')
                    jobs.append({
                        'name': name.split('.')[-1][:10].upper(),
                        'user': 'QBATCH',
                        'type': 'BCH',
                        'status': 'ACTIVE',
                        'cpu': '0.1',
                        'function': name.split('.')[-1][:15],
                        'task_id': task.get('id'),
                    })

            reserved = inspect.reserved() or {}
            for worker, tasks in reserved.items():
                for task in tasks:
                    name = task.get('name', 'UNKNOWN')
                    jobs.append({
                        'name': name.split('.')[-1][:10].upper(),
                        'user': 'QBATCH',
                        'type': 'BCH',
                        'status': 'JOBQ',
                        'cpu': '0.0',
                        'function': name.split('.')[-1][:15],
                        'task_id': task.get('id'),
                    })
        except Exception:
            pass

        if not jobs:
            jobs.append({
                'name': 'QBATCH',
                'user': 'QSYS',
                'type': 'SBS',
                'status': 'ACTIVE',
                'cpu': '0.0',
                'function': 'PGM-QBATCH',
                'task_id': None,
            })

        return jobs

    def _revoke_celery_task(self, task_id: str):
        """Revoke a Celery task."""
        if task_id:
            try:
                app = get_celery_app()
                app.control.revoke(task_id, terminate=True)
            except Exception:
                pass

    def _get_system_stats(self) -> dict:
        """Get system statistics."""
        stats = {
            'cpu_pct': 0.0,
            'cpu_count': 1.0,
            'cpu_used': 0.0,
            'elapsed_time': '00:00:00',
            'jobs_in_system': 1,
            'perm_addr_pct': 0.0,
            'temp_addr_pct': 0.0,
            'asp_util': 0.0,
            'disk_pct': 0.0,
            'disk_total_gb': 0.0,
            'disk_used_gb': 0.0,
            'disk_avail_gb': 0.0,
            'disk_units': 1,
            'mem_total_mb': 0.0,
            'mem_used_mb': 0.0,
            'mem_avail_mb': 0.0,
            'mem_pct': 0.0,
            'machine_pool': 1024,
            'machine_act': 100,
            'base_pool': 2048,
            'base_act': 200,
            'interact_pool': 512,
            'interact_act': 50,
            'spool_pool': 128,
            'spool_act': 10,
            'celery_active': 0,
            'celery_reserved': 0,
            'docker_containers': 0,
        }

        # Get uptime for elapsed time
        try:
            with open('/proc/uptime', 'r') as f:
                uptime_seconds = float(f.read().split()[0])
                hours = int(uptime_seconds // 3600)
                minutes = int((uptime_seconds % 3600) // 60)
                seconds = int(uptime_seconds % 60)
                stats['elapsed_time'] = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        except Exception:
            pass

        # CPU info
        try:
            with open('/proc/loadavg', 'r') as f:
                load = float(f.read().split()[0])
                stats['cpu_pct'] = min(load * 25, 100)
            cpu_count = os.cpu_count() or 1
            stats['cpu_count'] = float(cpu_count)
            stats['cpu_used'] = min(load, float(cpu_count))
        except Exception:
            pass

        # Memory info
        try:
            with open('/proc/meminfo', 'r') as f:
                meminfo = {}
                for line in f:
                    parts = line.split(':')
                    if len(parts) == 2:
                        key = parts[0].strip()
                        val = int(parts[1].strip().split()[0])
                        meminfo[key] = val

                total_kb = meminfo.get('MemTotal', 1)
                avail_kb = meminfo.get('MemAvailable', meminfo.get('MemFree', 0))
                used_kb = total_kb - avail_kb

                stats['mem_total_mb'] = total_kb / 1024
                stats['mem_used_mb'] = used_kb / 1024
                stats['mem_avail_mb'] = avail_kb / 1024
                stats['mem_pct'] = (used_kb / total_kb) * 100 if total_kb > 0 else 0
                stats['perm_addr_pct'] = stats['mem_pct'] * 0.6  # Estimate
                stats['temp_addr_pct'] = stats['mem_pct'] * 0.4  # Estimate

                stats['machine_pool'] = total_kb // 1024
                stats['base_pool'] = (total_kb // 1024) // 2
        except Exception:
            pass

        # Disk info
        try:
            result = subprocess.run(['df', '-B1', '/'], capture_output=True, text=True)
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                if len(lines) > 1:
                    parts = lines[1].split()
                    if len(parts) >= 5:
                        total_bytes = int(parts[1])
                        used_bytes = int(parts[2])
                        avail_bytes = int(parts[3])

                        stats['disk_total_gb'] = total_bytes / (1024 ** 3)
                        stats['disk_used_gb'] = used_bytes / (1024 ** 3)
                        stats['disk_avail_gb'] = avail_bytes / (1024 ** 3)
                        stats['disk_pct'] = (used_bytes / total_bytes) * 100 if total_bytes > 0 else 0
                        stats['asp_util'] = stats['disk_pct']
        except Exception:
            pass

        # Count disk units (block devices)
        try:
            result = subprocess.run(['lsblk', '-d', '-n', '-o', 'NAME'], capture_output=True, text=True)
            if result.returncode == 0 and result.stdout.strip():
                disks = [d for d in result.stdout.strip().split('\n') if d and not d.startswith('loop')]
                stats['disk_units'] = len(disks) if disks else 1
        except Exception:
            pass

        # Celery jobs
        try:
            app = get_celery_app()
            inspect = app.control.inspect()
            active = inspect.active() or {}
            reserved = inspect.reserved() or {}
            stats['celery_active'] = sum(len(tasks) for tasks in active.values())
            stats['celery_reserved'] = sum(len(tasks) for tasks in reserved.values())
            stats['jobs_in_system'] = stats['celery_active'] + stats['celery_reserved'] + 1
        except Exception:
            pass

        # Docker containers
        try:
            result = subprocess.run(['docker', 'ps', '-q'], capture_output=True, text=True, timeout=5)
            if result.returncode == 0 and result.stdout.strip():
                stats['docker_containers'] = len(result.stdout.strip().split('\n'))
        except Exception:
            pass

        return stats

    def _get_celery_queues(self) -> list[dict]:
        """Get Celery queue information."""
        queues = [
            {'name': 'CELERY', 'lib': 'QGPL', 'status': 'ACTIVE', 'jobs': 0, 'subsystem': 'QBATCH'},
            {'name': 'DEFAULT', 'lib': 'QGPL', 'status': 'ACTIVE', 'jobs': 0, 'subsystem': 'QBATCH'},
        ]

        try:
            app = get_celery_app()
            inspect = app.control.inspect()
            reserved = inspect.reserved() or {}
            for worker, tasks in reserved.items():
                queues[0]['jobs'] += len(tasks)
        except Exception:
            pass

        return queues

    def _get_docker_services(self) -> list[dict]:
        """Get Docker container information."""
        services = []

        try:
            result = subprocess.run(
                ['docker', 'ps', '-a', '--format', '{{.Names}}\t{{.Status}}\t{{.Image}}\t{{.Ports}}'],
                capture_output=True, text=True, timeout=10
            )

            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    if not line:
                        continue
                    parts = line.split('\t')
                    if len(parts) >= 3:
                        name = parts[0][:15].upper()
                        status_raw = parts[1]
                        image = parts[2].split(':')[0].split('/')[-1][:23]
                        ports = parts[3][:30] if len(parts) > 3 else ''

                        if 'Up' in status_raw:
                            status = 'ACTIVE'
                            elapsed = status_raw.replace('Up ', '').split(' ')[0][:10]
                        elif 'Exited' in status_raw:
                            status = 'ENDED'
                            elapsed = ''
                        else:
                            status = 'UNKNOWN'
                            elapsed = ''

                        services.append({
                            'name': name,
                            'status': status,
                            'elapsed': elapsed,
                            'image': image,
                            'ports': ports,
                        })
        except Exception:
            pass

        if not services:
            services.append({
                'name': 'DOCKER',
                'status': 'MSGW',
                'elapsed': '',
                'image': 'unavailable',
                'ports': '',
            })

        return services

    def _docker_action(self, container: str, action: str):
        """Perform Docker action."""
        try:
            subprocess.run(['docker', action, container.lower()], capture_output=True, timeout=30)
        except Exception:
            pass

    def _get_system_logs(self, limit: int = 50) -> list[dict]:
        """Get system logs."""
        logs = []

        try:
            result = subprocess.run(
                ['docker', 'logs', '--tail', str(limit), 'celery-qbatch'],
                capture_output=True, text=True, timeout=10
            )
            output = result.stderr or result.stdout
            if output:
                for line in output.strip().split('\n'):
                    if line.strip():
                        severity = 'INFO'
                        if 'ERROR' in line.upper():
                            severity = 'ERROR'
                        elif 'WARN' in line.upper():
                            severity = 'WARN'

                        logs.append({
                            'time': datetime.now().strftime('%H:%M:%S'),
                            'severity': severity,
                            'source': 'QBATCH',
                            'message': line[:90],
                        })
        except Exception:
            pass

        if not logs:
            logs.append({
                'time': datetime.now().strftime('%H:%M:%S'),
                'severity': 'INFO',
                'source': 'QSYSOPR',
                'message': 'No log entries found',
            })

        return logs

    def _get_available_tasks(self) -> list[str]:
        """Get available Celery tasks."""
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

        if not tasks:
            tasks = ['dk400.ping', 'dk400.echo', 'dk400.delay']

        return sorted(tasks)[:10]

    def _submit_celery_task(self, task_name: str, params: str, delay: int) -> str:
        """Submit a Celery task."""
        app = get_celery_app()

        args = []
        if params:
            if params.startswith('['):
                import json
                args = json.loads(params)
            else:
                args = [p.strip() for p in params.split(',') if p.strip()]

        sig = app.signature(task_name, args=args)

        if delay > 0:
            result = sig.apply_async(countdown=delay)
        else:
            result = sig.apply_async()

        return result.id
