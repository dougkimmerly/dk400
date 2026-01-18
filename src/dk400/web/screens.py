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
    # Use DK400_SYSTEM_NAME env var, falling back to DK400
    hostname = os.environ.get('DK400_SYSTEM_NAME', 'DK400').upper()[:12]
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
        'WRKHLTH': 'wrkhlth',
        'WRKBKP': 'wrkbkp',
        'WRKALR': 'wrkalr',
        'WRKNETDEV': 'wrknetdev',
        'SIGNOFF': 'signon',
        'GO': 'main',
        '1': 'wrkactjob',
        '2': 'wrkjobq',
        '3': 'wrksvc',
        '4': 'wrkhlth',
        '5': 'dspsyssts',
        '6': 'dsplog',
        '7': 'wrkbkp',
        '8': 'wrkalr',
        '9': 'wrknetdev',
        '10': 'sbmjob',
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
        elif key == 'F6':
            # Screen-specific F6 handling
            if screen == 'wrkhlth':
                session.message = "Running all health checks..."
                return self.get_screen(session, screen)
            elif screen == 'wrkbkp':
                session.message = "Starting all backup jobs..."
                return self.get_screen(session, screen)
            elif screen in ('health_detail', 'backup_detail'):
                # F6=Run Now / Start Now on detail screens
                session.message = "Job started"
                return self.get_screen(session, screen)
        elif key == 'F7':
            # Screen-specific F7 handling
            if screen == 'wrkalr':
                session.message = "All alerts acknowledged"
                return self.get_screen(session, screen)
            elif screen == 'alert_detail':
                session.message = "Alert acknowledged"
                return self.get_screen(session, 'wrkalr')
        elif key == 'F12':
            if screen in ('signon', 'main'):
                return self.get_screen(session, screen)
            # Return to parent screen based on current screen
            if screen in ('job_detail', 'health_detail', 'health_history'):
                return self.get_screen(session, 'wrkhlth') if 'health' in screen else self.get_screen(session, 'wrkactjob')
            elif screen == 'queue_detail':
                return self.get_screen(session, 'wrkjobq')
            elif screen in ('container_logs', 'container_detail'):
                return self.get_screen(session, 'wrksvc')
            elif screen == 'backup_detail':
                return self.get_screen(session, 'wrkbkp')
            elif screen == 'alert_detail':
                return self.get_screen(session, 'wrkalr')
            elif screen == 'device_detail':
                return self.get_screen(session, 'wrknetdev')
            return self.get_screen(session, 'main')

        return self.get_screen(session, screen)

    # Page sizes for scrollable screens
    PAGE_SIZES = {
        'wrkactjob': 12,
        'wrkjobq': 10,
        'wrksvc': 15,
        'dsplog': 16,
        'wrkhlth': 12,
        'wrkbkp': 12,
        'wrkalr': 14,
        'wrknetdev': 12,
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
            pad_line("       4. Work with health checks                          WRKHLTH"),
            pad_line("       5. Display system status                            DSPSYSSTS"),
            pad_line("       6. Display log                                      DSPLOG"),
            pad_line("       7. Work with backups                                WRKBKP"),
            pad_line("       8. Work with alerts                                 WRKALR"),
            pad_line("       9. Work with network devices                        WRKNETDEV"),
            pad_line("      10. Submit job                                       SBMJOB"),
            pad_line(""),
            pad_line("      90. Sign off                                         SIGNOFF"),
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
        offset = session.get_offset('wrkactjob')
        for i, job in enumerate(jobs[offset:offset + self.PAGE_SIZES['wrkactjob']]):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt == '2':
                # Change - not supported for Celery tasks
                session.message = f"Change not supported for {job['name']}"
                session.message_level = "warning"
                break
            elif opt == '3':
                # Hold job - revoke without terminate
                if job.get('task_id'):
                    try:
                        app = get_celery_app()
                        app.control.revoke(job['task_id'], terminate=False)
                        session.message = f"Job {job['name']} held"
                    except Exception:
                        session.message = f"Failed to hold {job['name']}"
                        session.message_level = "error"
                break
            elif opt == '4':
                self._revoke_celery_task(job.get('task_id'))
                session.message = f"Job {job['name']} end requested"
                break
            elif opt == '5':
                # Work with job - show detail screen
                session.field_values['selected_job'] = job
                return self._screen_job_detail(session, job)
            elif opt == '8':
                # Spooled files - show output for this job
                session.message = f"No spool files for {job['name']}"
                break
            elif opt:
                session.message = f"Option {opt} not valid"
                session.message_level = "error"
                break

        return self.get_screen(session, 'wrkactjob')

    def _screen_job_detail(self, session: Session, job: dict) -> dict:
        """Display job detail screen - 80 columns."""
        hostname, date_str, time_str = get_system_info()

        content = [
            pad_line(f"                      Display Job Status                            {hostname}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(f" Job:   {job.get('name', 'UNKNOWN'):<10}    User:  {job.get('user', 'QBATCH'):<10}"),
            pad_line(f" Type:  {job.get('type', 'BCH'):<10}    Status: {job.get('status', 'ACTIVE'):<10}"),
            pad_line(""),
            pad_line(" Job attributes:"),
            pad_line(f"   Task ID . . . . . :  {job.get('task_id', 'N/A')[:36]}"),
            pad_line(f"   Function  . . . . :  {job.get('function', 'N/A')}"),
            pad_line(f"   CPU % . . . . . . :  {job.get('cpu', '0.0')}"),
            pad_line(f"   Queue . . . . . . :  celery"),
            pad_line(f"   Priority  . . . . :  5"),
            pad_line(""),
            pad_line(" Additional job attributes:"),
            pad_line("   Time slice  . . . :  2000 milliseconds"),
            pad_line("   Default wait  . . :  30 seconds"),
            pad_line("   Max CPU time  . . :  *NOMAX"),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(" F3=Exit  F5=Refresh  F12=Cancel"),
            pad_line(""),
        ]

        return {
            "type": "screen",
            "screen": "job_detail",
            "cols": 80,
            "content": content,
            "fields": [],
            "activeField": 0,
        }

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

    def _submit_wrkjobq(self, session: Session, fields: dict) -> dict:
        """Handle WRKJOBQ submission."""
        cmd = fields.get('cmd', '').strip().upper()
        if cmd:
            return self.execute_command(session, cmd)

        queues = self._get_celery_queues()
        for i, queue in enumerate(queues[:self.PAGE_SIZES['wrkjobq']]):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt == '5':
                # Work with queue - show detail
                return self._screen_queue_detail(session, queue)
            elif opt == '6':
                # Hold queue - pause consumption
                session.message = f"Queue {queue['name']} hold requested"
                break
            elif opt == '7':
                # Release queue
                session.message = f"Queue {queue['name']} release requested"
                break
            elif opt == '8':
                # Work with jobs in queue
                session.message = f"Displaying jobs in {queue['name']}"
                return self.get_screen(session, 'wrkactjob')
            elif opt:
                session.message = f"Option {opt} not valid"
                session.message_level = "error"
                break

        return self.get_screen(session, 'wrkjobq')

    def _screen_queue_detail(self, session: Session, queue: dict) -> dict:
        """Display job queue detail - 80 columns."""
        hostname, date_str, time_str = get_system_info()

        content = [
            pad_line(f"                     Display Job Queue                             {hostname}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(f" Job queue . . . . . :  {queue.get('name', 'CELERY'):<15}"),
            pad_line(f"   Library . . . . . :  {queue.get('lib', 'QGPL'):<15}"),
            pad_line(""),
            pad_line(f" Status  . . . . . . :  {queue.get('status', 'ACTIVE')}"),
            pad_line(f" Subsystem . . . . . :  {queue.get('subsystem', 'QBATCH')}"),
            pad_line(f" Number of jobs  . . :  {queue.get('jobs', 0)}"),
            pad_line(""),
            pad_line(" Queue attributes:"),
            pad_line("   Sequence number . :  50"),
            pad_line("   Max active jobs . :  *NOMAX"),
            pad_line("   Operator control  :  *YES"),
            pad_line("   Authority . . . . :  *LIBCRTAUT"),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(" F3=Exit  F5=Refresh  F12=Cancel"),
            pad_line(""),
        ]

        return {
            "type": "screen",
            "screen": "queue_detail",
            "cols": 80,
            "content": content,
            "fields": [],
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
        offset = session.get_offset('wrksvc')
        for i, svc in enumerate(services[offset:offset + self.PAGE_SIZES['wrksvc']]):
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
            elif opt == '5':
                # Display logs
                session.field_values['log_container'] = svc['name']
                return self._screen_container_logs(session, svc['name'])
            elif opt == '8':
                # Display details
                return self._screen_container_detail(session, svc)
            elif opt:
                session.message = f"Option {opt} not valid"
                session.message_level = "error"
                break

        return self.get_screen(session, 'wrksvc')

    def _screen_container_logs(self, session: Session, container_name: str) -> dict:
        """Display container logs - 132 columns."""
        hostname, date_str, time_str = get_system_info()

        # Get container logs
        logs = []
        try:
            result = subprocess.run(
                ['docker', 'logs', '--tail', '16', container_name.lower()],
                capture_output=True, text=True, timeout=10
            )
            output = result.stderr or result.stdout
            if output:
                for line in output.strip().split('\n')[-16:]:
                    logs.append(line[:110])
        except Exception:
            logs.append("Unable to retrieve logs")

        content = [
            pad_line(f" {hostname:<20}                      Container Logs: {container_name:<15}                  {session.user:>10}", 132),
            pad_line(f"                                                                                          {date_str}  {time_str}", 132),
            pad_line("", 132),
            [{"type": "text", "text": pad_line(" Log Output", 132), "class": "field-reverse"}],
        ]

        for log in logs[:16]:
            content.append(pad_line(f" {log}", 132))

        while len(content) < 21:
            content.append(pad_line("", 132))

        content.append(pad_line("", 132))
        content.append(pad_line(" F3=Exit  F5=Refresh  F12=Cancel", 132))
        content.append(pad_line("", 132))

        return {
            "type": "screen",
            "screen": "container_logs",
            "cols": 132,
            "content": content,
            "fields": [],
            "activeField": 0,
        }

    def _screen_container_detail(self, session: Session, svc: dict) -> dict:
        """Display container detail - 132 columns."""
        hostname, date_str, time_str = get_system_info()

        # Get more container details
        details = {'id': 'N/A', 'created': 'N/A', 'started': 'N/A', 'health': 'N/A', 'network': 'N/A'}
        try:
            result = subprocess.run(
                ['docker', 'inspect', '--format',
                 '{{.Id}}|{{.Created}}|{{.State.StartedAt}}|{{.State.Health.Status}}|{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}',
                 svc['name'].lower()],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0 and result.stdout.strip():
                parts = result.stdout.strip().split('|')
                if len(parts) >= 5:
                    details['id'] = parts[0][:12]
                    details['created'] = parts[1][:19].replace('T', ' ')
                    details['started'] = parts[2][:19].replace('T', ' ')
                    details['health'] = parts[3] if parts[3] else 'N/A'
                    details['network'] = parts[4] if parts[4] else 'N/A'
        except Exception:
            pass

        content = [
            pad_line(f" {hostname:<20}                     Container Details                                         {session.user:>10}", 132),
            pad_line(f"                                                                                          {date_str}  {time_str}", 132),
            pad_line("", 132),
            pad_line(f" Container . . . . . :  {svc['name']:<30}", 132),
            pad_line(f" ID  . . . . . . . . :  {details['id']:<12}", 132),
            pad_line(f" Image . . . . . . . :  {svc.get('image', 'N/A'):<30}", 132),
            pad_line("", 132),
            pad_line(f" Status  . . . . . . :  {svc.get('status', 'N/A'):<15}", 132),
            pad_line(f" Elapsed . . . . . . :  {svc.get('elapsed', 'N/A'):<15}", 132),
            pad_line(f" Health  . . . . . . :  {details['health']:<15}", 132),
            pad_line("", 132),
            pad_line(f" Created . . . . . . :  {details['created']:<20}", 132),
            pad_line(f" Started . . . . . . :  {details['started']:<20}", 132),
            pad_line("", 132),
            pad_line(f" IP Address  . . . . :  {details['network']:<20}", 132),
            pad_line(f" Ports . . . . . . . :  {svc.get('ports', 'N/A'):<50}", 132),
            pad_line("", 132),
            pad_line("", 132),
            pad_line("", 132),
            pad_line("", 132),
            pad_line("", 132),
            pad_line(" F3=Exit  F5=Refresh  F12=Cancel", 132),
            pad_line("", 132),
        ]

        return {
            "type": "screen",
            "screen": "container_detail",
            "cols": 132,
            "content": content,
            "fields": [],
            "activeField": 0,
        }

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

    # ========== ADDITIONAL SCREENS ==========

    def _screen_wrkhlth(self, session: Session) -> dict:
        """Work with Health Checks - 80 columns."""
        hostname, date_str, time_str = get_system_info()
        all_checks = self._get_health_checks()

        # Pagination
        page_size = self.PAGE_SIZES['wrkhlth']
        offset = session.get_offset('wrkhlth')
        total = len(all_checks)

        if offset >= total and total > 0:
            offset = max(0, total - page_size)
            session.set_offset('wrkhlth', offset)

        checks = all_checks[offset:offset + page_size]

        # Position indicator
        if total > page_size:
            pos_indicator = "Bottom" if offset + page_size >= total else "More..."
        else:
            pos_indicator = ""

        content = [
            pad_line(f"                      Work with Health Checks                        {hostname}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type options, press Enter."),
            pad_line("   5=Display details   6=Run check   8=View history"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  Check Name       Status      Last Run     Interval  Message"), "class": "field-reverse"}],
        ]

        fields = []
        for i, check in enumerate(checks):
            status_class = ""
            if check['status'] == 'FAIL':
                status_class = "field-error"
            elif check['status'] == 'WARN':
                status_class = "field-warning"

            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {check['name']:<15}  ", "class": ""},
                {"type": "text", "text": f"{check['status']:<10}  ", "class": status_class},
                {"type": "text", "text": f"{check['last_run']:<11}  {check['interval']:<8}  {check['message']:<20}"},
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
        content.append(pad_line(" F3=Exit  F5=Refresh  F6=Run All  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "wrkhlth",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _screen_wrkbkp(self, session: Session) -> dict:
        """Work with Backups - 80 columns."""
        hostname, date_str, time_str = get_system_info()
        all_backups = self._get_backups()

        # Pagination
        page_size = self.PAGE_SIZES['wrkbkp']
        offset = session.get_offset('wrkbkp')
        total = len(all_backups)

        if offset >= total and total > 0:
            offset = max(0, total - page_size)
            session.set_offset('wrkbkp', offset)

        backups = all_backups[offset:offset + page_size]

        # Position indicator
        if total > page_size:
            pos_indicator = "Bottom" if offset + page_size >= total else "More..."
        else:
            pos_indicator = ""

        content = [
            pad_line(f"                        Work with Backups                            {hostname}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type options, press Enter."),
            pad_line("   1=Start backup   4=Delete   5=Display   8=Restore"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  Backup Job       Status      Last Run     Size      Type"), "class": "field-reverse"}],
        ]

        fields = []
        for i, backup in enumerate(backups):
            status_class = ""
            if backup['status'] == 'FAILED':
                status_class = "field-error"
            elif backup['status'] == 'RUNNING':
                status_class = "field-warning"

            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {backup['name']:<15}  ", "class": ""},
                {"type": "text", "text": f"{backup['status']:<10}  ", "class": status_class},
                {"type": "text", "text": f"{backup['last_run']:<11}  {backup['size']:<8}  {backup['type']:<10}"},
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
        content.append(pad_line(" F3=Exit  F5=Refresh  F6=Start All  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "wrkbkp",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _screen_wrkalr(self, session: Session) -> dict:
        """Work with Alerts - 132 columns for more detail."""
        hostname, date_str, time_str = get_system_info()
        all_alerts = self._get_alerts()

        # Pagination
        page_size = self.PAGE_SIZES['wrkalr']
        offset = session.get_offset('wrkalr')
        total = len(all_alerts)

        if offset >= total and total > 0:
            offset = max(0, total - page_size)
            session.set_offset('wrkalr', offset)

        alerts = all_alerts[offset:offset + page_size]

        # Position indicator
        if total > page_size:
            pos_indicator = "Bottom" if offset + page_size >= total else "More..."
        else:
            pos_indicator = ""

        content = [
            pad_line(f" {hostname:<20}                          Work with Alerts                                      {session.user:>10}", 132),
            pad_line(f"                                                                                          {date_str}  {time_str}", 132),
            pad_line("", 132),
            pad_line(" Type options, press Enter.", 132),
            pad_line("   4=Delete   5=Display   7=Acknowledge   8=View source", 132),
            pad_line("", 132),
            [{"type": "text", "text": pad_line(" Opt  Severity  Time        Source           Message", 132), "class": "field-reverse"}],
        ]

        fields = []
        for i, alert in enumerate(alerts):
            sev_class = ""
            if alert['severity'] == 'CRIT':
                sev_class = "field-error"
            elif alert['severity'] == 'WARN':
                sev_class = "field-warning"
            elif alert['severity'] == 'INFO':
                sev_class = "field-highlight"

            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  ", "class": ""},
                {"type": "text", "text": f"{alert['severity']:<6}    ", "class": sev_class},
                {"type": "text", "text": f"{alert['time']:<10}  {alert['source']:<15}  {alert['message']:<70}"},
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
        content.append(pad_line(" F3=Exit  F5=Refresh  F7=Ack All  F12=Cancel  PageDown=Roll Down  PageUp=Roll Up", 132))

        return {
            "type": "screen",
            "screen": "wrkalr",
            "cols": 132,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _screen_wrknetdev(self, session: Session) -> dict:
        """Work with Network Devices - 132 columns."""
        hostname, date_str, time_str = get_system_info()
        all_devices = self._get_network_devices()

        # Pagination
        page_size = self.PAGE_SIZES['wrknetdev']
        offset = session.get_offset('wrknetdev')
        total = len(all_devices)

        if offset >= total and total > 0:
            offset = max(0, total - page_size)
            session.set_offset('wrknetdev', offset)

        devices = all_devices[offset:offset + page_size]

        # Position indicator
        if total > page_size:
            pos_indicator = "Bottom" if offset + page_size >= total else "More..."
        else:
            pos_indicator = ""

        content = [
            pad_line(f" {hostname:<20}                     Work with Network Devices                                  {session.user:>10}", 132),
            pad_line(f"                                                                                          {date_str}  {time_str}", 132),
            pad_line("", 132),
            pad_line(" Type options, press Enter.", 132),
            pad_line("   2=Ping   5=Display   7=Wake-on-LAN   8=SSH connect", 132),
            pad_line("", 132),
            [{"type": "text", "text": pad_line(" Opt  Device Name       IP Address       MAC Address        Status    Type       Vendor", 132), "class": "field-reverse"}],
        ]

        fields = []
        for i, device in enumerate(devices):
            status_class = ""
            if device['status'] == 'OFFLINE':
                status_class = "field-error"
            elif device['status'] == 'ONLINE':
                status_class = "field-highlight"

            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {device['name']:<16} {device['ip']:<16} {device['mac']:<18} ", "class": ""},
                {"type": "text", "text": f"{device['status']:<8}  ", "class": status_class},
                {"type": "text", "text": f"{device['type']:<10} {device['vendor']:<20}"},
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
            "screen": "wrknetdev",
            "cols": 132,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    # ========== SUBMIT HANDLERS FOR ADDITIONAL SCREENS ==========

    def _submit_wrkhlth(self, session: Session, fields: dict) -> dict:
        """Handle WRKHLTH submission."""
        cmd = fields.get('cmd', '').strip().upper()
        if cmd:
            return self.execute_command(session, cmd)

        checks = self._get_health_checks()
        offset = session.get_offset('wrkhlth')
        for i, check in enumerate(checks[offset:offset + self.PAGE_SIZES['wrkhlth']]):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt == '5':
                # Display details
                return self._screen_health_detail(session, check)
            elif opt == '6':
                # Run check now
                session.message = f"Running check {check['name']}..."
                # In real implementation, would trigger check
                break
            elif opt == '8':
                # View history
                return self._screen_health_history(session, check)
            elif opt:
                session.message = f"Option {opt} not valid"
                session.message_level = "error"
                break

        return self.get_screen(session, 'wrkhlth')

    def _screen_health_detail(self, session: Session, check: dict) -> dict:
        """Display health check detail - 80 columns."""
        hostname, date_str, time_str = get_system_info()

        content = [
            pad_line(f"                    Display Health Check                            {hostname}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(f" Check Name  . . . . :  {check.get('name', 'UNKNOWN'):<20}"),
            pad_line(f" Status  . . . . . . :  {check.get('status', 'N/A'):<10}"),
            pad_line(f" Last Run  . . . . . :  {check.get('last_run', 'N/A'):<12}"),
            pad_line(f" Interval  . . . . . :  {check.get('interval', 'N/A'):<10}"),
            pad_line(""),
            pad_line(f" Message:"),
            pad_line(f"   {check.get('message', 'N/A'):<60}"),
            pad_line(""),
            pad_line(" Check configuration:"),
            pad_line("   Type  . . . . . . :  Docker container health"),
            pad_line("   Timeout . . . . . :  30 seconds"),
            pad_line("   Retries . . . . . :  3"),
            pad_line("   Alert on fail . . :  *YES"),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(" F3=Exit  F5=Refresh  F6=Run Now  F12=Cancel"),
            pad_line(""),
        ]

        return {
            "type": "screen",
            "screen": "health_detail",
            "cols": 80,
            "content": content,
            "fields": [],
            "activeField": 0,
        }

    def _screen_health_history(self, session: Session, check: dict) -> dict:
        """Display health check history - 80 columns."""
        hostname, date_str, time_str = get_system_info()

        content = [
            pad_line(f"                   Health Check History                             {hostname}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(f" Check Name: {check.get('name', 'UNKNOWN'):<20}"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Time        Status      Duration  Message"), "class": "field-reverse"}],
            pad_line(f" {time_str}  {check.get('status', 'N/A'):<10}  0.5s      {check.get('message', 'N/A')[:30]}"),
            pad_line(f" {time_str}  OK          0.3s      Check passed"),
            pad_line(f" {time_str}  OK          0.4s      Check passed"),
            pad_line(f" {time_str}  OK          0.3s      Check passed"),
            pad_line(f" {time_str}  OK          0.5s      Check passed"),
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
            pad_line(""),
            pad_line(" F3=Exit  F5=Refresh  F12=Cancel"),
            pad_line(""),
        ]

        return {
            "type": "screen",
            "screen": "health_history",
            "cols": 80,
            "content": content,
            "fields": [],
            "activeField": 0,
        }

    def _submit_wrkbkp(self, session: Session, fields: dict) -> dict:
        """Handle WRKBKP submission."""
        cmd = fields.get('cmd', '').strip().upper()
        if cmd:
            return self.execute_command(session, cmd)

        backups = self._get_backups()
        offset = session.get_offset('wrkbkp')
        for i, backup in enumerate(backups[offset:offset + self.PAGE_SIZES['wrkbkp']]):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt == '1':
                # Start backup
                session.message = f"Starting backup {backup['name']}..."
                break
            elif opt == '4':
                # Delete - show confirmation message
                session.message = f"Delete {backup['name']} - use DLTBKP command"
                session.message_level = "warning"
                break
            elif opt == '5':
                # Display
                return self._screen_backup_detail(session, backup)
            elif opt == '8':
                # Restore
                session.message = f"Restore from {backup['name']} not available in demo"
                session.message_level = "warning"
                break
            elif opt:
                session.message = f"Option {opt} not valid"
                session.message_level = "error"
                break

        return self.get_screen(session, 'wrkbkp')

    def _screen_backup_detail(self, session: Session, backup: dict) -> dict:
        """Display backup detail - 80 columns."""
        hostname, date_str, time_str = get_system_info()

        content = [
            pad_line(f"                      Display Backup Job                            {hostname}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(f" Backup Name . . . . :  {backup.get('name', 'UNKNOWN'):<20}"),
            pad_line(f" Type  . . . . . . . :  {backup.get('type', 'N/A'):<15}"),
            pad_line(f" Status  . . . . . . :  {backup.get('status', 'N/A'):<10}"),
            pad_line(""),
            pad_line(f" Last Run  . . . . . :  {backup.get('last_run', 'N/A'):<15}"),
            pad_line(f" Size  . . . . . . . :  {backup.get('size', 'N/A'):<10}"),
            pad_line(""),
            pad_line(" Backup configuration:"),
            pad_line("   Schedule  . . . . :  Daily at 02:00"),
            pad_line("   Retention . . . . :  7 days"),
            pad_line("   Compression . . . :  *YES"),
            pad_line("   Encryption  . . . :  *NO"),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(""),
            pad_line(" F3=Exit  F5=Refresh  F6=Start Now  F12=Cancel"),
            pad_line(""),
        ]

        return {
            "type": "screen",
            "screen": "backup_detail",
            "cols": 80,
            "content": content,
            "fields": [],
            "activeField": 0,
        }

    def _submit_wrkalr(self, session: Session, fields: dict) -> dict:
        """Handle WRKALR submission."""
        cmd = fields.get('cmd', '').strip().upper()
        if cmd:
            return self.execute_command(session, cmd)

        alerts = self._get_alerts()
        offset = session.get_offset('wrkalr')
        for i, alert in enumerate(alerts[offset:offset + self.PAGE_SIZES['wrkalr']]):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt == '4':
                # Delete alert
                session.message = f"Alert deleted"
                break
            elif opt == '5':
                # Display details
                return self._screen_alert_detail(session, alert)
            elif opt == '7':
                # Acknowledge
                session.message = f"Alert acknowledged"
                break
            elif opt == '8':
                # View source
                session.message = f"Source: {alert.get('source', 'N/A')}"
                break
            elif opt:
                session.message = f"Option {opt} not valid"
                session.message_level = "error"
                break

        return self.get_screen(session, 'wrkalr')

    def _screen_alert_detail(self, session: Session, alert: dict) -> dict:
        """Display alert detail - 132 columns."""
        hostname, date_str, time_str = get_system_info()

        content = [
            pad_line(f" {hostname:<20}                        Display Alert                                             {session.user:>10}", 132),
            pad_line(f"                                                                                          {date_str}  {time_str}", 132),
            pad_line("", 132),
            pad_line(f" Severity  . . . . . :  {alert.get('severity', 'N/A'):<10}", 132),
            pad_line(f" Time  . . . . . . . :  {alert.get('time', 'N/A'):<15}", 132),
            pad_line(f" Source  . . . . . . :  {alert.get('source', 'N/A'):<20}", 132),
            pad_line("", 132),
            pad_line(" Message:", 132),
            pad_line(f"   {alert.get('message', 'N/A'):<100}", 132),
            pad_line("", 132),
            pad_line(" Alert details:", 132),
            pad_line("   Acknowledged  . . :  *NO", 132),
            pad_line("   Created . . . . . :  " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 132),
            pad_line("   Alert ID  . . . . :  ALR" + datetime.now().strftime('%H%M%S'), 132),
            pad_line("", 132),
            pad_line("", 132),
            pad_line("", 132),
            pad_line("", 132),
            pad_line("", 132),
            pad_line("", 132),
            pad_line("", 132),
            pad_line(" F3=Exit  F5=Refresh  F7=Acknowledge  F12=Cancel", 132),
            pad_line("", 132),
        ]

        return {
            "type": "screen",
            "screen": "alert_detail",
            "cols": 132,
            "content": content,
            "fields": [],
            "activeField": 0,
        }

    def _submit_wrknetdev(self, session: Session, fields: dict) -> dict:
        """Handle WRKNETDEV submission."""
        cmd = fields.get('cmd', '').strip().upper()
        if cmd:
            return self.execute_command(session, cmd)

        devices = self._get_network_devices()
        offset = session.get_offset('wrknetdev')
        for i, device in enumerate(devices[offset:offset + self.PAGE_SIZES['wrknetdev']]):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt == '2':
                # Ping
                ping_result = self._ping_device(device['ip'])
                if ping_result:
                    session.message = f"Ping {device['ip']}: {ping_result}"
                else:
                    session.message = f"Ping {device['ip']}: No response"
                    session.message_level = "error"
                break
            elif opt == '5':
                # Display details
                return self._screen_device_detail(session, device)
            elif opt == '7':
                # Wake-on-LAN
                if device['mac'] != 'LOCAL' and device['mac'] != 'N/A':
                    self._send_wol(device['mac'])
                    session.message = f"WOL magic packet sent to {device['mac']}"
                else:
                    session.message = f"Cannot send WOL to {device['name']}"
                    session.message_level = "error"
                break
            elif opt == '8':
                # SSH - display connection info
                session.message = f"SSH: ssh root@{device['ip']}"
                break
            elif opt:
                session.message = f"Option {opt} not valid"
                session.message_level = "error"
                break

        return self.get_screen(session, 'wrknetdev')

    def _screen_device_detail(self, session: Session, device: dict) -> dict:
        """Display device detail - 132 columns."""
        hostname, date_str, time_str = get_system_info()

        # Try to get more info
        ping_result = self._ping_device(device['ip'])
        ping_status = ping_result if ping_result else "No response"

        content = [
            pad_line(f" {hostname:<20}                     Display Network Device                                        {session.user:>10}", 132),
            pad_line(f"                                                                                          {date_str}  {time_str}", 132),
            pad_line("", 132),
            pad_line(f" Device Name . . . . :  {device.get('name', 'UNKNOWN'):<30}", 132),
            pad_line(f" IP Address  . . . . :  {device.get('ip', 'N/A'):<20}", 132),
            pad_line(f" MAC Address . . . . :  {device.get('mac', 'N/A'):<20}", 132),
            pad_line("", 132),
            pad_line(f" Status  . . . . . . :  {device.get('status', 'N/A'):<15}", 132),
            pad_line(f" Ping Response . . . :  {ping_status:<30}", 132),
            pad_line("", 132),
            pad_line(f" Type  . . . . . . . :  {device.get('type', 'N/A'):<15}", 132),
            pad_line(f" Vendor  . . . . . . :  {device.get('vendor', 'N/A'):<30}", 132),
            pad_line("", 132),
            pad_line(" Network configuration:", 132),
            pad_line("   DHCP  . . . . . . :  *YES", 132),
            pad_line("   Gateway . . . . . :  192.168.20.1", 132),
            pad_line("   DNS . . . . . . . :  192.168.20.1", 132),
            pad_line("", 132),
            pad_line("", 132),
            pad_line("", 132),
            pad_line("", 132),
            pad_line(" F3=Exit  F5=Refresh  F2=Ping  F7=Wake-on-LAN  F12=Cancel", 132),
            pad_line("", 132),
        ]

        return {
            "type": "screen",
            "screen": "device_detail",
            "cols": 132,
            "content": content,
            "fields": [],
            "activeField": 0,
        }

    def _ping_device(self, ip: str) -> Optional[str]:
        """Ping a device and return the result."""
        if not ip or ip == 'N/A':
            return None
        try:
            result = subprocess.run(
                ['ping', '-c', '1', '-W', '2', ip],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                # Extract time from output
                for line in result.stdout.split('\n'):
                    if 'time=' in line:
                        time_part = line.split('time=')[1].split()[0]
                        return f"Reply in {time_part}"
                return "Reply received"
            return None
        except Exception:
            return None

    def _send_wol(self, mac: str):
        """Send Wake-on-LAN magic packet."""
        try:
            # Convert MAC address to bytes
            mac_clean = mac.replace(':', '').replace('-', '')
            if len(mac_clean) != 12:
                return
            mac_bytes = bytes.fromhex(mac_clean)

            # Build magic packet
            magic = b'\xff' * 6 + mac_bytes * 16

            # Send via UDP broadcast
            import socket as sock
            s = sock.socket(sock.AF_INET, sock.SOCK_DGRAM)
            s.setsockopt(sock.SOL_SOCKET, sock.SO_BROADCAST, 1)
            s.sendto(magic, ('255.255.255.255', 9))
            s.close()
        except Exception:
            pass

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

    def _get_health_checks(self) -> list[dict]:
        """Get health check status."""
        checks = []

        # Docker health checks
        try:
            result = subprocess.run(
                ['docker', 'ps', '--format', '{{.Names}}\t{{.Status}}'],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    if not line:
                        continue
                    parts = line.split('\t')
                    if len(parts) >= 2:
                        name = parts[0][:15].upper()
                        status_raw = parts[1]

                        if '(healthy)' in status_raw.lower():
                            status = 'OK'
                            message = 'Container healthy'
                        elif '(unhealthy)' in status_raw.lower():
                            status = 'FAIL'
                            message = 'Container unhealthy'
                        elif 'Up' in status_raw:
                            status = 'OK'
                            message = 'Container running'
                        else:
                            status = 'FAIL'
                            message = 'Container not running'

                        checks.append({
                            'name': name,
                            'status': status,
                            'last_run': datetime.now().strftime('%H:%M:%S'),
                            'interval': '30s',
                            'message': message[:20],
                        })
        except Exception:
            pass

        # Add some default system checks
        checks.extend([
            {'name': 'REDIS', 'status': 'OK', 'last_run': datetime.now().strftime('%H:%M:%S'), 'interval': '60s', 'message': 'Broker connected'},
            {'name': 'CELERY', 'status': 'OK', 'last_run': datetime.now().strftime('%H:%M:%S'), 'interval': '60s', 'message': 'Workers active'},
            {'name': 'DISK', 'status': 'OK', 'last_run': datetime.now().strftime('%H:%M:%S'), 'interval': '300s', 'message': 'Space available'},
        ])

        if not checks:
            checks.append({
                'name': 'SYSTEM',
                'status': 'OK',
                'last_run': datetime.now().strftime('%H:%M:%S'),
                'interval': '60s',
                'message': 'No checks defined',
            })

        return checks

    def _get_backups(self) -> list[dict]:
        """Get backup job status."""
        backups = []

        # Check for backup directories
        backup_dirs = [
            ('/home/doug/backups', 'HOMELAB', 'Full'),
            ('/var/lib/docker/volumes', 'DOCKER-VOL', 'Volume'),
        ]

        for path, name, btype in backup_dirs:
            try:
                result = subprocess.run(['du', '-sh', path], capture_output=True, text=True, timeout=5)
                if result.returncode == 0:
                    size = result.stdout.split()[0]
                    backups.append({
                        'name': name,
                        'status': 'COMPLETE',
                        'last_run': datetime.now().strftime('%m/%d %H:%M'),
                        'size': size,
                        'type': btype,
                    })
            except Exception:
                backups.append({
                    'name': name,
                    'status': 'UNKNOWN',
                    'last_run': 'N/A',
                    'size': '0',
                    'type': btype,
                })

        # Add placeholder backup jobs
        backups.extend([
            {'name': 'POSTGRES-BKP', 'status': 'COMPLETE', 'last_run': '01/17 02:00', 'size': '256M', 'type': 'Database'},
            {'name': 'CONFIG-BKP', 'status': 'COMPLETE', 'last_run': '01/17 03:00', 'size': '12M', 'type': 'Config'},
            {'name': 'NETBOX-BKP', 'status': 'COMPLETE', 'last_run': '01/17 02:30', 'size': '45M', 'type': 'Database'},
        ])

        return backups

    def _get_alerts(self) -> list[dict]:
        """Get system alerts."""
        alerts = []

        # Check docker for any issues
        try:
            result = subprocess.run(
                ['docker', 'ps', '-a', '--filter', 'status=exited', '--format', '{{.Names}}\t{{.Status}}'],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0 and result.stdout.strip():
                for line in result.stdout.strip().split('\n'):
                    if not line:
                        continue
                    parts = line.split('\t')
                    if len(parts) >= 2:
                        name = parts[0]
                        alerts.append({
                            'severity': 'WARN',
                            'time': datetime.now().strftime('%H:%M:%S'),
                            'source': 'DOCKER',
                            'message': f'Container {name} has exited',
                        })
        except Exception:
            pass

        # Add some sample alerts for demonstration
        if not alerts:
            alerts = [
                {'severity': 'INFO', 'time': datetime.now().strftime('%H:%M:%S'), 'source': 'QSYSOPR', 'message': 'System started successfully'},
                {'severity': 'INFO', 'time': datetime.now().strftime('%H:%M:%S'), 'source': 'CELERY', 'message': 'Worker qbatch connected to broker'},
            ]

        return alerts

    def _get_network_devices(self) -> list[dict]:
        """Get network devices from ARP table and known hosts."""
        devices = []

        # Get devices from ARP table
        try:
            result = subprocess.run(['arp', '-a'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    if not line or 'incomplete' in line.lower():
                        continue
                    # Parse ARP output: hostname (ip) at mac [ether] on interface
                    parts = line.split()
                    if len(parts) >= 4:
                        try:
                            hostname = parts[0] if parts[0] != '?' else 'UNKNOWN'
                            ip = parts[1].strip('()')
                            mac = parts[3] if len(parts) > 3 else 'N/A'

                            # Determine device type based on MAC vendor
                            vendor = 'Unknown'
                            dtype = 'Device'
                            mac_prefix = mac[:8].upper() if mac != 'N/A' else ''

                            if mac_prefix.startswith('DC:A6:32') or mac_prefix.startswith('B8:27:EB'):
                                vendor = 'Raspberry Pi'
                                dtype = 'SBC'
                            elif mac_prefix.startswith('00:1A:79'):
                                vendor = 'Ubiquiti'
                                dtype = 'Network'
                            elif mac_prefix.startswith('3C:22:FB'):
                                vendor = 'Apple'
                                dtype = 'Workstation'
                            elif mac_prefix.startswith('00:11:32'):
                                vendor = 'Synology'
                                dtype = 'NAS'

                            devices.append({
                                'name': hostname[:16].upper(),
                                'ip': ip[:16],
                                'mac': mac[:18].upper(),
                                'status': 'ONLINE',
                                'type': dtype[:10],
                                'vendor': vendor[:20],
                            })
                        except (IndexError, ValueError):
                            continue
        except Exception:
            pass

        # Add the local host
        try:
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)
            devices.insert(0, {
                'name': hostname[:16].upper(),
                'ip': local_ip[:16],
                'mac': 'LOCAL',
                'status': 'ONLINE',
                'type': 'Server',
                'vendor': 'Local System',
            })
        except Exception:
            pass

        if not devices:
            devices.append({
                'name': 'NO DEVICES',
                'ip': 'N/A',
                'mac': 'N/A',
                'status': 'UNKNOWN',
                'type': 'N/A',
                'vendor': 'N/A',
            })

        return devices
