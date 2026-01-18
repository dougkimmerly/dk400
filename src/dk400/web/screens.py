"""
DK/400 Screen Definitions

AS/400-style screen layouts and logic for the web terminal.
"""
import os
import socket
import subprocess
from datetime import datetime
from typing import Any, Optional
from dataclasses import dataclass, field

from celery import Celery


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


LOGO = r"""
  ____  _  ______ ___   ___   ___
 |  _ \| |/ / / // _ \ / _ \ / _ \
 | | | | ' / / /| | | | | | | | | |
 | |_| | . \/ /_| |_| | |_| | |_| |
 |____/|_|\_\____\___/ \___/ \___/
"""


@dataclass
class Session:
    """User session state."""
    session_id: str
    user: str = "QUSER"
    current_screen: str = "signon"
    field_values: dict = field(default_factory=dict)
    message: str = ""
    message_level: str = "info"


class ScreenManager:
    """Manages screen rendering and transitions."""

    # Command mapping
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

        # Default: check for command in cmd field
        cmd = fields.get('cmd', '').strip().upper()
        if cmd:
            return self.execute_command(session, cmd)

        return self.get_screen(session, session.current_screen)

    def handle_function_key(self, session: Session, screen: str, key: str, fields: dict) -> dict:
        """Handle function key press."""
        session.field_values.update(fields)

        if key == 'F3':
            # Exit - go back to previous screen or sign off
            if screen == 'signon':
                return {'type': 'message', 'text': 'Press F3 again to disconnect', 'level': 'info'}
            elif screen == 'main':
                return self.get_screen(session, 'signon')
            else:
                return self.get_screen(session, 'main')

        elif key == 'F5':
            # Refresh current screen
            return self.get_screen(session, screen)

        elif key == 'F12':
            # Cancel - go back
            if screen in ('signon', 'main'):
                return self.get_screen(session, screen)
            return self.get_screen(session, 'main')

        elif key == 'F4':
            # Prompt
            session.message = "Prompting not available"
            return self.get_screen(session, screen)

        # Default: refresh
        return self.get_screen(session, screen)

    def execute_command(self, session: Session, command: str) -> dict:
        """Execute an AS/400 command."""
        # Check exact match
        if command in self.COMMANDS:
            screen = self.COMMANDS[command]
            session.message = ""
            return self.get_screen(session, screen)

        # Check partial match
        matches = [c for c in self.COMMANDS.keys() if c.startswith(command) and not c.isdigit()]
        if len(matches) == 1:
            screen = self.COMMANDS[matches[0]]
            session.message = ""
            return self.get_screen(session, screen)
        elif len(matches) > 1:
            session.message = f"Ambiguous command: {', '.join(matches)}"
            return self.get_screen(session, session.current_screen)

        session.message = f"Command {command} not found"
        session.message_level = "error"
        return self.get_screen(session, session.current_screen)

    # ========== SCREEN DEFINITIONS ==========

    def _screen_signon(self, session: Session) -> dict:
        """Sign-on screen."""
        hostname, date_str, time_str = get_system_info()

        content = [
            "",
            [{"type": "text", "text": "                                    Sign On", "class": "field-highlight"}],
            "",
            f"  System  . . . . . :   {hostname:<12}",
            f"  Subsystem . . . . :   QINTER",
            f"  Display . . . . . :   DSP01",
            "",
            [
                {"type": "text", "text": "  User  . . . . . . . . . . . . . :  "},
                {"type": "input", "id": "user", "width": 10, "value": session.field_values.get('user', '')},
            ],
            [
                {"type": "text", "text": "  Password  . . . . . . . . . . . :  "},
                {"type": "input", "id": "password", "width": 10, "password": True},
            ],
            [
                {"type": "text", "text": "  Program/procedure . . . . . . . :  "},
                {"type": "input", "id": "program", "width": 10},
            ],
            [
                {"type": "text", "text": "  Menu  . . . . . . . . . . . . . :  "},
                {"type": "input", "id": "menu", "width": 10},
            ],
            [
                {"type": "text", "text": "  Current library . . . . . . . . :  "},
                {"type": "input", "id": "library", "width": 10},
            ],
            "",
            "",
            "",
            "",
            "",
            "  (C) COPYRIGHT IBM CORP. 1980, 2024.",
        ]

        return {
            "type": "screen",
            "screen": "signon",
            "content": content,
            "fields": [
                {"id": "user", "row": 7, "col": 37},
                {"id": "password", "row": 8, "col": 37},
                {"id": "program", "row": 9, "col": 37},
                {"id": "menu", "row": 10, "col": 37},
                {"id": "library", "row": 11, "col": 37},
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
        """Main menu screen."""
        hostname, date_str, time_str = get_system_info()

        content = []
        for line in LOGO.split('\n'):
            content.append(line)

        content.extend([
            "",
            [{"type": "text", "text": "                                        Main Menu", "class": "field-highlight"}],
            "",
            f"                                        System: {hostname}",
            f"                                        User:   {session.user}",
            f"                                        {date_str}  {time_str}",
            "",
            "  Select one of the following:",
            "",
            "       1. Work with active jobs         WRKACTJOB",
            "       2. Work with job queues          WRKJOBQ",
            "       3. Work with services            WRKSVC",
            "       4. Display system status         DSPSYSSTS",
            "       5. Display log                   DSPLOG",
            "       6. Submit job                    SBMJOB",
            "",
            "      90. Sign off                      SIGNOFF",
            "",
            "",
            "  Selection or command",
            [
                {"type": "text", "text": "  ===> "},
                {"type": "input", "id": "cmd", "width": 50, "value": ""},
            ],
        ])

        # Add message line if present
        if session.message:
            content.append("")
            content.append([{"type": "text", "text": f"  {session.message}", "class": f"field-{session.message_level}"}])
            session.message = ""

        return {
            "type": "screen",
            "screen": "main",
            "content": content,
            "fields": [{"id": "cmd", "row": 20, "col": 8}],
            "activeField": 0,
        }

    def _screen_wrkactjob(self, session: Session) -> dict:
        """Work with Active Jobs screen."""
        hostname, date_str, time_str = get_system_info()
        timestamp = f"{date_str}  {time_str}"

        # Get active Celery tasks
        jobs = self._get_celery_jobs()

        content = [
            f" {hostname}                    Work with Active Jobs                  {session.user}",
            f"                                                          {timestamp}",
            "",
            " Type options, press Enter.",
            "   2=Change   3=Hold   4=End   5=Work with   8=Work with spooled files",
            "",
            [{"type": "text", "text": " Opt  Job         User       Type    Status      CPU%  Function", "class": "field-reverse"}],
        ]

        # Add job rows
        fields = []
        for i, job in enumerate(jobs[:15]):  # Max 15 jobs
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f" {job['name']:<10} {job['user']:<10} {job['type']:<7} {job['status']:<11} {job['cpu']:>4}  {job['function']}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}", "row": 7 + i, "col": 1})

        # Pad to fill screen
        while len(content) < 22:
            content.append("")

        # Add message and command line
        if session.message:
            content.append([{"type": "text", "text": f" {session.message}", "class": f"field-{session.message_level}"}])
            session.message = ""
        else:
            content.append("")

        content.append([
            {"type": "text", "text": " Parameters or command"},
        ])
        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 50},
        ])
        fields.append({"id": "cmd", "row": 24, "col": 7})

        return {
            "type": "screen",
            "screen": "wrkactjob",
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_wrkactjob(self, session: Session, fields: dict) -> dict:
        """Handle WRKACTJOB submission."""
        # Check for command first
        cmd = fields.get('cmd', '').strip().upper()
        if cmd:
            return self.execute_command(session, cmd)

        # Process options
        jobs = self._get_celery_jobs()
        for i, job in enumerate(jobs[:15]):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt == '4':
                # End job - revoke Celery task
                self._revoke_celery_task(job.get('task_id'))
                session.message = f"Job {job['name']} end requested"
                break
            elif opt == '3':
                session.message = f"Hold not implemented for {job['name']}"
                break
            elif opt:
                session.message = f"Option {opt} not valid"
                break

        return self.get_screen(session, 'wrkactjob')

    def _screen_dspsyssts(self, session: Session) -> dict:
        """Display System Status screen."""
        hostname, date_str, time_str = get_system_info()
        timestamp = f"{date_str}  {time_str}"

        stats = self._get_system_stats()

        content = [
            f" {hostname}                  Display System Status                    {timestamp}",
            "",
            f" % CPU utilization . . . . . . . :    {stats['cpu_pct']:5.1f}",
            f" Elapsed time  . . . . . . . . . :  {stats['elapsed_time']}",
            f" Jobs in system  . . . . . . . . :    {stats['jobs_in_system']:5d}",
            f" % permanent addresses . . . . . :     {stats['perm_addr_pct']:4.1f}",
            f" % temporary addresses . . . . . :      {stats['temp_addr_pct']:3.1f}",
            "",
            f" System     ASP . . . . . . . . . :      {stats['asp_util']:4.1f} %",
            f"   % used . . . . . . . . . . . . :      {stats['disk_pct']:4.1f} %",
            "",
            [{"type": "text", "text": "    Pool      Reserved    Max    Allocated   Defined      Pool", "class": "field-highlight"}],
            f"    *MACHINE       {stats['machine_pool']:5d}      ++     {stats['machine_act']:4d}      {stats['machine_pool']:5d}    Machine pool",
            f"    *BASE          {stats['base_pool']:5d}      ++     {stats['base_act']:4d}      {stats['base_pool']:5d}    QBATCH QINTER",
            f"    *INTERACT        {stats['interact_pool']:4d}      ++     {stats['interact_act']:4d}        {stats['interact_pool']:4d}    Interactive",
            f"    *SPOOL           {stats['spool_pool']:4d}      ++     {stats['spool_act']:4d}        {stats['spool_pool']:4d}    Spooling",
            "",
            [{"type": "text", "text": " Subsystem      Subsystem   Active   Wait   Wait      Pool", "class": "field-highlight"}],
            [{"type": "text", "text": " Name           Status      Jobs     Msg    Job       Name", "class": "field-highlight"}],
            f" QBATCH         ACTIVE        {stats['celery_active']:4d}      0      {stats['celery_reserved']:3d}    *BASE",
            f" QINTER         ACTIVE        {stats['docker_containers']:4d}      0        0    *BASE",
            f" QSPL           ACTIVE           1      0        0    *SPOOL",
            f" QCTL           ACTIVE           1      0        0    *MACHINE",
            "",
        ]

        content.append(" Press Enter to continue.")
        content.append("")

        return {
            "type": "screen",
            "screen": "dspsyssts",
            "content": content,
            "fields": [],
            "activeField": 0,
        }

    def _screen_wrkjobq(self, session: Session) -> dict:
        """Work with Job Queues screen."""
        hostname, date_str, time_str = get_system_info()
        timestamp = f"{date_str}  {time_str}"

        queues = self._get_celery_queues()

        content = [
            f" {hostname}                   Work with Job Queues                   {session.user}",
            f"                                                          {timestamp}",
            "",
            " Type options, press Enter.",
            "   5=Work with   6=Hold   7=Release   8=Work with jobs",
            "",
            [{"type": "text", "text": " Opt  Queue       Status    Jobs    Held    Subsystem", "class": "field-reverse"}],
        ]

        fields = []
        for i, q in enumerate(queues[:10]):
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f" {q['name']:<10} {q['status']:<9} {q['jobs']:>4}    {q['held']:>4}    {q['subsystem']}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}", "row": 7 + i, "col": 1})

        while len(content) < 22:
            content.append("")

        if session.message:
            content.append([{"type": "text", "text": f" {session.message}", "class": f"field-{session.message_level}"}])
            session.message = ""
        else:
            content.append("")

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 50},
        ])
        fields.append({"id": "cmd", "row": 24, "col": 7})

        return {
            "type": "screen",
            "screen": "wrkjobq",
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _screen_wrksvc(self, session: Session) -> dict:
        """Work with Services (Docker) screen."""
        hostname, date_str, time_str = get_system_info()
        timestamp = f"{date_str}  {time_str}"

        services = self._get_docker_services()

        content = [
            f" {hostname}                    Work with Services                    {session.user}",
            f"                                                          {timestamp}",
            "",
            " Type options, press Enter.",
            "   2=Start   3=Stop   4=Restart   5=Logs   8=Details",
            "",
            [{"type": "text", "text": " Opt  Service       Status   Elapsed     Image            Ports", "class": "field-reverse"}],
        ]

        fields = []
        for i, svc in enumerate(services[:12]):
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f" {svc['name']:<12} {svc['status']:<8} {svc['elapsed']:<10} {svc['image']:<16} {svc['ports']}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}", "row": 7 + i, "col": 1})

        while len(content) < 22:
            content.append("")

        if session.message:
            content.append([{"type": "text", "text": f" {session.message}", "class": f"field-{session.message_level}"}])
            session.message = ""
        else:
            content.append("")

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 50},
        ])
        fields.append({"id": "cmd", "row": 24, "col": 7})

        return {
            "type": "screen",
            "screen": "wrksvc",
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
        for i, svc in enumerate(services[:12]):
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
        """Display Log screen."""
        hostname, date_str, time_str = get_system_info()
        timestamp = f"{date_str}  {time_str}"

        logs = self._get_system_logs()

        content = [
            f" {hostname}                      Display Log                          {session.user}",
            f"                                                          {timestamp}",
            "",
            [{"type": "text", "text": " Time      Sev    Source    Message", "class": "field-reverse"}],
        ]

        for log in logs[:18]:
            css_class = ""
            if log['severity'] == 'ERROR':
                css_class = "field-error"
            elif log['severity'] == 'WARN':
                css_class = "field-warning"

            row = [{"type": "text", "text": f" {log['time']:<9} {log['severity']:<6} {log['source']:<9} {log['message'][:50]}", "class": css_class}]
            content.append(row)

        while len(content) < 23:
            content.append("")

        content.append(" F5=Refresh   PageUp/PageDown to scroll")

        return {
            "type": "screen",
            "screen": "dsplog",
            "content": content,
            "fields": [],
            "activeField": 0,
        }

    def _screen_sbmjob(self, session: Session) -> dict:
        """Submit Job screen."""
        hostname, date_str, time_str = get_system_info()
        timestamp = f"{date_str}  {time_str}"

        tasks = self._get_available_tasks()

        content = [
            f" {hostname}                       Submit Job                          {session.user}",
            f"                                                          {timestamp}",
            "",
            " Type choices, press Enter.",
            "",
            [
                {"type": "text", "text": " Command to run  . . . . . . . :  "},
                {"type": "input", "id": "task", "width": 30, "value": session.field_values.get('task', '')},
            ],
            [
                {"type": "text", "text": " Parameters  . . . . . . . . . :  "},
                {"type": "input", "id": "params", "width": 30},
            ],
            [
                {"type": "text", "text": " Job queue . . . . . . . . . . :  "},
                {"type": "input", "id": "queue", "width": 15, "value": "celery"},
            ],
            [
                {"type": "text", "text": " Delay (seconds) . . . . . . . :  "},
                {"type": "input", "id": "delay", "width": 5, "value": "0"},
            ],
            "",
            " Available commands:",
        ]

        for task in tasks[:8]:
            content.append(f"   {task}")

        while len(content) < 22:
            content.append("")

        if session.message:
            content.append([{"type": "text", "text": f" {session.message}", "class": f"field-{session.message_level}"}])
            session.message = ""
        else:
            content.append("")

        content.append("")

        return {
            "type": "screen",
            "screen": "sbmjob",
            "content": content,
            "fields": [
                {"id": "task", "row": 5, "col": 35},
                {"id": "params", "row": 6, "col": 35},
                {"id": "queue", "row": 7, "col": 35},
                {"id": "delay", "row": 8, "col": 35},
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
            session.message = f"Error: {str(e)[:40]}"
            session.message_level = "error"

        return self.get_screen(session, 'sbmjob')

    # ========== DATA FETCHING HELPERS ==========

    def _get_celery_jobs(self) -> list[dict]:
        """Get active Celery jobs."""
        jobs = []
        try:
            app = get_celery_app()
            inspect = app.control.inspect()

            active = inspect.active() or {}
            for worker, tasks in active.items():
                for task in tasks:
                    jobs.append({
                        'name': task.get('name', 'UNKNOWN')[-10:].upper(),
                        'user': 'QBATCH',
                        'type': 'BCH',
                        'status': 'ACTIVE',
                        'cpu': '0.1',
                        'function': task.get('name', '')[-20:],
                        'task_id': task.get('id'),
                    })

            reserved = inspect.reserved() or {}
            for worker, tasks in reserved.items():
                for task in tasks:
                    jobs.append({
                        'name': task.get('name', 'UNKNOWN')[-10:].upper(),
                        'user': 'QBATCH',
                        'type': 'BCH',
                        'status': 'JOBQ',
                        'cpu': '0.0',
                        'function': task.get('name', '')[-20:],
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
            'elapsed_time': '00:00:00',
            'jobs_in_system': 1,
            'perm_addr_pct': 0.0,
            'temp_addr_pct': 0.0,
            'asp_util': 0.0,
            'disk_pct': 0.0,
            'machine_pool': 1024,
            'machine_act': 100,
            'base_pool': 2048,
            'base_act': 200,
            'interact_pool': 512,
            'interact_act': 50,
            'spool_pool': 256,
            'spool_act': 10,
            'celery_active': 0,
            'celery_reserved': 0,
            'docker_containers': 0,
        }

        try:
            # CPU from /proc/loadavg
            with open('/proc/loadavg', 'r') as f:
                load = float(f.read().split()[0])
                stats['cpu_pct'] = min(load * 10, 100)
        except Exception:
            pass

        try:
            # Memory from /proc/meminfo
            with open('/proc/meminfo', 'r') as f:
                meminfo = {}
                for line in f:
                    parts = line.split(':')
                    if len(parts) == 2:
                        key = parts[0].strip()
                        val = int(parts[1].strip().split()[0])
                        meminfo[key] = val

                total = meminfo.get('MemTotal', 1)
                free = meminfo.get('MemAvailable', meminfo.get('MemFree', 0))
                used_pct = ((total - free) / total) * 100

                stats['machine_pool'] = total // 1024
                stats['base_pool'] = (total // 1024) // 2
                stats['perm_addr_pct'] = used_pct
        except Exception:
            pass

        try:
            # Disk usage
            result = subprocess.run(['df', '/'], capture_output=True, text=True)
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                if len(lines) > 1:
                    parts = lines[1].split()
                    if len(parts) >= 5:
                        stats['disk_pct'] = float(parts[4].replace('%', ''))
                        stats['asp_util'] = stats['disk_pct']
        except Exception:
            pass

        try:
            # Celery stats
            app = get_celery_app()
            inspect = app.control.inspect()
            active = inspect.active() or {}
            reserved = inspect.reserved() or {}

            stats['celery_active'] = sum(len(tasks) for tasks in active.values())
            stats['celery_reserved'] = sum(len(tasks) for tasks in reserved.values())
            stats['jobs_in_system'] = stats['celery_active'] + stats['celery_reserved'] + 1
        except Exception:
            pass

        try:
            # Docker containers
            result = subprocess.run(['docker', 'ps', '-q'], capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                stats['docker_containers'] = len(result.stdout.strip().split('\n')) if result.stdout.strip() else 0
        except Exception:
            pass

        return stats

    def _get_celery_queues(self) -> list[dict]:
        """Get Celery queue information."""
        queues = [
            {'name': 'celery', 'status': 'ACTIVE', 'jobs': 0, 'held': 0, 'subsystem': 'QBATCH'},
            {'name': 'default', 'status': 'ACTIVE', 'jobs': 0, 'held': 0, 'subsystem': 'QBATCH'},
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
                        name = parts[0][:12].upper()
                        status_raw = parts[1]
                        image = parts[2].split(':')[0].split('/')[-1][:16]
                        ports = parts[3][:20] if len(parts) > 3 else ''

                        if 'Up' in status_raw:
                            status = 'ACTIVE'
                            elapsed = status_raw.replace('Up ', '').split(' ')[0]
                        elif 'Exited' in status_raw:
                            status = 'ENDED'
                            elapsed = ''
                        else:
                            status = 'UNKNOWN'
                            elapsed = ''

                        services.append({
                            'name': name,
                            'status': status,
                            'elapsed': elapsed[:10],
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

    def _get_system_logs(self) -> list[dict]:
        """Get system logs."""
        logs = []

        try:
            result = subprocess.run(
                ['docker', 'logs', '--tail', '50', 'celery-qbatch'],
                capture_output=True, text=True, timeout=10
            )
            output = result.stderr or result.stdout
            if output:
                for line in output.strip().split('\n')[-20:]:
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
                            'message': line[-60:],
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

        return sorted(tasks)

    def _submit_celery_task(self, task_name: str, params: str, delay: int) -> str:
        """Submit a Celery task."""
        app = get_celery_app()

        args = []
        if params:
            if params.startswith('['):
                import json
                args = json.loads(params)
            else:
                args = [p.strip() for p in params.split(',')]

        sig = app.signature(task_name, args=args)

        if delay > 0:
            result = sig.apply_async(countdown=delay)
        else:
            result = sig.apply_async()

        return result.id
