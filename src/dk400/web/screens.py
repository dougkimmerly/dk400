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

from src.dk400.web.users import user_manager, UserProfile
from src.dk400.web.database import (
    create_schema, drop_schema, list_schemas, list_schema_tables,
    grant_object_authority, revoke_object_authority, get_object_authorities,
    get_effective_authorities, get_user_group, AUTHORITY_GRANTS,
    get_system_value, set_system_value, list_system_values,
    # Message Queues
    create_message_queue, delete_message_queue, list_message_queues,
    send_message, get_messages, mark_message_old, reply_to_message,
    delete_message, clear_message_queue,
    # Data Areas
    create_data_area, delete_data_area, get_data_area, change_data_area,
    lock_data_area, unlock_data_area, list_data_areas,
    # Job Descriptions
    create_job_description, delete_job_description, get_job_description,
    list_job_descriptions, change_job_description,
    # Output Queues and Spooled Files
    create_output_queue, delete_output_queue, list_output_queues,
    hold_output_queue, release_output_queue,
    create_spooled_file, get_spooled_file, list_spooled_files,
    delete_spooled_file, hold_spooled_file, release_spooled_file,
    # Job Schedule Entries
    add_job_schedule_entry, remove_job_schedule_entry, get_job_schedule_entry,
    list_job_schedule_entries, hold_job_schedule_entry, release_job_schedule_entry,
    change_job_schedule_entry,
    # Authorization Lists
    create_authorization_list, delete_authorization_list, list_authorization_lists,
    add_authorization_list_entry, remove_authorization_list_entry,
    get_authorization_list_entries, add_object_to_authorization_list,
    remove_object_from_authorization_list, get_authorization_list_objects,
    # Subsystem Descriptions
    create_subsystem_description, delete_subsystem_description,
    get_subsystem_description, list_subsystem_descriptions,
    start_subsystem, end_subsystem, add_job_queue_entry,
    remove_job_queue_entry, get_subsystem_job_queues,
    # Commands
    list_commands, get_command, get_command_parameters, get_parameter_valid_values,
)


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
    """Get system name and current timestamp."""
    # Use QSYSNAME system value, with env var as fallback
    try:
        hostname = get_system_value('QSYSNAME', 'DK400').upper()[:12]
    except Exception:
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


LOGO_FULL = """\
  ____  _  ______ ___   ___   ___
 |  _ \\| |/ / / // _ \\ / _ \\ / _ \\
 | | | | ' / / /| | | | | | | | | |
 | |_| | . \\/ /_| |_| | |_| | |_| |
 |____/|_|\\_\\____|\\___/ \\___/ \\___/ """

LOGO_SMALL = "  DK/400"


def get_logo() -> str:
    """Get the logo based on QLOGOSIZE system value."""
    logo_size = get_system_value('QLOGOSIZE', '*SMALL')
    if logo_size == '*FULL':
        return LOGO_FULL
    elif logo_size == '*NONE':
        return ""
    else:  # *SMALL or default
        return LOGO_SMALL


@dataclass
class Session:
    """User session state."""
    session_id: str
    user: str = "QUSER"
    user_class: str = "*USER"  # *SECOFR, *SECADM, *PGMR, *SYSOPR, *USER
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
        'WRKUSRPRF': 'wrkusrprf',
        'CRTUSRPRF': 'user_create',
        'DSPUSRAUT': 'user_authorities',
        # Schema and object authority commands
        'WRKSCHEMA': 'wrkschema',
        'WRKLIB': 'wrkschema',  # AS/400 alias
        'CRTSCHEMA': 'schema_create',
        'CRTLIB': 'schema_create',  # AS/400 alias
        'GRTOBJAUT': 'grtobjaut',
        'RVKOBJAUT': 'rvkobjaut',
        'DSPOBJAUT': 'dspobjaut',
        # System values
        'WRKSYSVAL': 'wrksysval',
        'DSPSYSVAL': 'wrksysval',  # Alias
        'CHGSYSVAL': 'chgsysval',
        # Message Queues
        'WRKMSGQ': 'wrkmsgq',
        'DSPMSG': 'dspmsg',
        'SNDMSG': 'sndmsg',
        # Data Areas
        'WRKDTAARA': 'wrkdtaara',
        'DSPDTAARA': 'dspdtaara',
        'CRTDTAARA': 'crtdtaara',
        'CHGDTAARA': 'chgdtaara',
        # Job Descriptions
        'WRKJOBD': 'wrkjobd',
        'DSPJOBD': 'dspjobd',
        'CRTJOBD': 'crtjobd',
        # Output Queues and Spooled Files
        'WRKOUTQ': 'wrkoutq',
        'WRKSPLF': 'wrksplf',
        'DSPSPLF': 'dspsplf',
        # Job Schedule Entries
        'WRKJOBSCDE': 'wrkjobscde',
        'ADDJOBSCDE': 'addjobscde',
        # Authorization Lists
        'WRKAUTL': 'wrkautl',
        'DSPAUTL': 'dspautl',
        'CRTAUTL': 'crtautl',
        # Subsystem Descriptions
        'WRKSBSD': 'wrksbsd',
        'STRSBS': 'strsbs',
        'ENDSBS': 'endsbs',
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

    # Command descriptions for F4 prompt
    COMMAND_DESCRIPTIONS = {
        'WRKACTJOB': 'Work with Active Jobs',
        'WRKJOBQ': 'Work with Job Queues',
        'WRKSVC': 'Work with Services (Docker)',
        'DSPSYSSTS': 'Display System Status',
        'DSPLOG': 'Display System Log',
        'SBMJOB': 'Submit Job',
        'WRKHLTH': 'Work with Health Checks',
        'WRKBKP': 'Work with Backups',
        'WRKALR': 'Work with Alerts',
        'WRKNETDEV': 'Work with Network Devices',
        'WRKUSRPRF': 'Work with User Profiles',
        'CRTUSRPRF': 'Create User Profile',
        'DSPUSRAUT': 'Display User Authorities',
        'WRKSCHEMA': 'Work with Schemas (Libraries)',
        'WRKLIB': 'Work with Libraries',
        'CRTSCHEMA': 'Create Schema (Library)',
        'CRTLIB': 'Create Library',
        'GRTOBJAUT': 'Grant Object Authority',
        'RVKOBJAUT': 'Revoke Object Authority',
        'DSPOBJAUT': 'Display Object Authority',
        'WRKSYSVAL': 'Work with System Values',
        'DSPSYSVAL': 'Display System Values',
        'CHGSYSVAL': 'Change System Value',
        'WRKMSGQ': 'Work with Message Queues',
        'DSPMSG': 'Display Messages',
        'SNDMSG': 'Send Message',
        'CRTMSGQ': 'Create Message Queue',
        'WRKDTAARA': 'Work with Data Areas',
        'DSPDTAARA': 'Display Data Area',
        'CRTDTAARA': 'Create Data Area',
        'CHGDTAARA': 'Change Data Area',
        'WRKJOBD': 'Work with Job Descriptions',
        'DSPJOBD': 'Display Job Description',
        'CRTJOBD': 'Create Job Description',
        'WRKOUTQ': 'Work with Output Queues',
        'CRTOUTQ': 'Create Output Queue',
        'WRKSPLF': 'Work with Spooled Files',
        'DSPSPLF': 'Display Spooled File',
        'WRKJOBSCDE': 'Work with Job Schedule Entries',
        'ADDJOBSCDE': 'Add Job Schedule Entry',
        'WRKAUTL': 'Work with Authorization Lists',
        'DSPAUTL': 'Display Authorization List',
        'CRTAUTL': 'Create Authorization List',
        'WRKSBSD': 'Work with Subsystem Descriptions',
        'STRSBS': 'Start Subsystem',
        'ENDSBS': 'End Subsystem',
        'SIGNOFF': 'Sign Off',
        'GO': 'Go to Main Menu',
    }

    # Map screen names back to command names for F4 parameter prompts
    # Used to look up parameters when F4 is pressed on a field
    SCREEN_COMMAND_MAP = {
        'user_create': 'CRTUSRPRF',
        'sbmjob': 'SBMJOB',
        'sndmsg': 'SNDMSG',
        'crtdtaara': 'CRTDTAARA',
        'chgdtaara': 'CHGDTAARA',
        'chgsysval': 'CHGSYSVAL',
        'grtobjaut': 'GRTOBJAUT',
        'rvkobjaut': 'RVKOBJAUT',
        'addjobscde': 'ADDJOBSCDE',
        'crtjobd': 'CRTJOBD',
        'crtautl': 'CRTAUTL',
        'schema_create': 'CRTSCHEMA',
    }

    # Map field IDs to parameter names for F4 prompts
    # If field ID matches parameter name (uppercase), no mapping needed
    FIELD_PARM_MAP = {
        # user_create screen
        'usrcls': 'USRCLS',
        'grpprf': 'GRPPRF',
        'password': 'PASSWORD',
        # sbmjob screen
        'jobq': 'JOBQ',
        'jobd': 'JOBD',
        # crtdtaara screen
        'type': 'TYPE',
        # grtobjaut screen
        'objtype': 'OBJTYPE',
        'aut': 'AUT',
        # addjobscde screen
        'frq': 'FRQ',
        'scdday': 'SCDDAY',
        'scddate': 'SCDDATE',
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
        elif key == 'F4':
            # F4 = Prompt - show command list or parameter help
            session.field_values['f4_return_screen'] = screen

            # Check if we have a focused field that has parameter prompts
            active_field = fields.get('_active_field', '').lower()

            # If on command line or no active field, show command list
            if not active_field or active_field == 'cmd':
                cmd_input = fields.get('cmd', '').strip().upper()
                session.field_values['f4_filter'] = cmd_input
                return self.get_screen(session, 'cmdlist')

            # Check if this screen has a command mapping
            command_name = self.SCREEN_COMMAND_MAP.get(screen)
            if command_name:
                # Map field ID to parameter name
                parm_name = self.FIELD_PARM_MAP.get(active_field, active_field.upper())

                # Check if valid values exist for this parameter
                valid_values = get_parameter_valid_values(command_name, parm_name)
                if valid_values:
                    # Show parameter value prompt
                    session.field_values['f4_command'] = command_name
                    session.field_values['f4_parm'] = parm_name
                    session.field_values['f4_field_id'] = active_field
                    return self.get_screen(session, 'parmlist')

            # No parameter prompts available, show command list
            cmd_input = fields.get('cmd', '').strip().upper()
            session.field_values['f4_filter'] = cmd_input
            return self.get_screen(session, 'cmdlist')
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
            elif screen == 'wrkusrprf':
                # F6=Create on Work with User Profiles
                return self.get_screen(session, 'user_create')
            elif screen == 'wrkschema':
                # F6=Create on Work with Schemas
                return self.get_screen(session, 'schema_create')
            elif screen == 'wrkmsgq':
                # F6=Create on Work with Message Queues
                return self.get_screen(session, 'crtmsgq')
            elif screen == 'dspmsg':
                # F6=Send message
                return self.get_screen(session, 'sndmsg')
            elif screen == 'wrkdtaara':
                # F6=Create on Work with Data Areas
                return self.get_screen(session, 'crtdtaara')
            elif screen == 'wrkjobd':
                # F6=Create on Work with Job Descriptions
                return self.get_screen(session, 'crtjobd')
            elif screen == 'wrkoutq':
                # F6=Create on Work with Output Queues
                return self.get_screen(session, 'crtoutq')
            elif screen == 'wrkjobscde':
                # F6=Add on Work with Job Schedule Entries
                return self.get_screen(session, 'addjobscde')
            elif screen == 'wrkautl':
                # F6=Create on Work with Authorization Lists
                return self.get_screen(session, 'crtautl')
            elif screen == 'wrkautlent':
                # F6=Add entry
                return self.get_screen(session, 'addautlent')
            elif screen == 'wrksbsd':
                # F6=Create on Work with Subsystems
                return self.get_screen(session, 'crtsbsd')
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
            elif screen in ('user_display', 'user_chgpwd', 'user_create', 'user_change'):
                return self.get_screen(session, 'wrkusrprf')
            elif screen in ('schema_create', 'schema_detail', 'schema_tables'):
                return self.get_screen(session, 'wrkschema')
            elif screen in ('grtobjaut', 'rvkobjaut', 'dspobjaut'):
                return self.get_screen(session, 'wrkschema')
            # Message Queue screens
            elif screen in ('dspmsg', 'sndmsg', 'crtmsgq', 'msgq_detail'):
                return self.get_screen(session, 'wrkmsgq')
            # Data Area screens
            elif screen in ('dspdtaara', 'crtdtaara', 'chgdtaara'):
                return self.get_screen(session, 'wrkdtaara')
            # Job Description screens
            elif screen in ('dspjobd', 'crtjobd', 'chgjobd'):
                return self.get_screen(session, 'wrkjobd')
            # Output Queue screens
            elif screen in ('wrksplf', 'crtoutq'):
                return self.get_screen(session, 'wrkoutq')
            elif screen == 'dspsplf':
                return self.get_screen(session, 'wrksplf')
            # Job Schedule Entry screens
            elif screen in ('dspjobscde', 'addjobscde'):
                return self.get_screen(session, 'wrkjobscde')
            # Authorization List screens
            elif screen in ('dspautl', 'crtautl'):
                return self.get_screen(session, 'wrkautl')
            elif screen == 'wrkautlent':
                return self.get_screen(session, 'wrkautl')
            elif screen == 'addautlent':
                return self.get_screen(session, 'wrkautlent')
            # Subsystem screens
            elif screen in ('dspsbsd', 'crtsbsd', 'strsbs', 'endsbs'):
                return self.get_screen(session, 'wrksbsd')
            # Command list (F4 prompt)
            elif screen == 'cmdlist':
                return_screen = session.field_values.get('f4_return_screen', 'main')
                session.field_values.pop('f4_filter', None)
                session.field_values.pop('f4_return_screen', None)
                return self.get_screen(session, return_screen)
            # Parameter list (F4 prompt for field values)
            elif screen == 'parmlist':
                return_screen = session.field_values.get('f4_return_screen', 'main')
                session.field_values.pop('f4_command', None)
                session.field_values.pop('f4_parm', None)
                session.field_values.pop('f4_field_id', None)
                session.field_values.pop('f4_return_screen', None)
                session.set_offset('parmlist', 0)
                return self.get_screen(session, return_screen)
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
        'wrkusrprf': 10,
        'wrkschema': 10,
        'dspobjaut': 10,
        'cmdlist': 12,
        'parmlist': 10,
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
        command = command.upper().strip()

        # Check numeric shortcuts first (from COMMANDS dict)
        if command in self.COMMANDS:
            session.message = ""
            return self.get_screen(session, self.COMMANDS[command])

        # Look up command in database
        cmd_def = get_command(command)
        if cmd_def and cmd_def.get('screen_name'):
            session.message = ""
            return self.get_screen(session, cmd_def['screen_name'])

        # Try partial match from database
        matches = list_commands(command)
        if len(matches) == 1:
            session.message = ""
            return self.get_screen(session, matches[0]['screen_name'])
        elif len(matches) > 1:
            cmd_names = [m['command_name'] for m in matches[:5]]
            session.message = f"Ambiguous command: {', '.join(cmd_names)}"
            return self.get_screen(session, session.current_screen)

        session.message = f"Command {command} not found"
        session.message_level = "error"
        return self.get_screen(session, session.current_screen)

    def _message_line(self, session: Session, width: int = COLS_80) -> list:
        """Format a message line with appropriate styling."""
        if not session.message:
            return pad_line("", width)

        css_class = "field-error" if session.message_level == "error" else "field-warning"
        # Center the message
        msg_text = session.message.center(width)
        return [{"type": "text", "text": msg_text, "class": css_class}]

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
            # Display error message if present
            self._message_line(session) if session.message else pad_line(""),
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
        password = fields.get('password', '')

        if not user:
            user = 'QUSER'
            password = 'QUSER'

        # Authenticate user
        success, message = user_manager.authenticate(user, password)

        if not success:
            session.message = message
            session.message_level = "error"
            return self.get_screen(session, 'signon')

        # Get user profile for class info
        user_profile = user_manager.get_user(user)
        session.user = user
        session.user_class = user_profile.user_class if user_profile else "*USER"
        session.message = ""  # Clear any previous error message

        return self.get_screen(session, 'main')

    def _screen_main(self, session: Session) -> dict:
        """Main menu screen - 80 columns."""
        hostname, date_str, time_str = get_system_info()

        content = []
        logo = get_logo()
        if logo:
            for line in logo.split('\n'):
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

    def _screen_cmdlist(self, session: Session) -> dict:
        """Command List (F4 Prompt) - show all available commands from COMMAND_INFO."""
        hostname, date_str, time_str = get_system_info()

        # Get filter from F4 context
        cmd_filter = session.field_values.get('f4_filter', '').upper()

        # Query commands from database (already filtered and sorted)
        db_commands = list_commands(cmd_filter)

        # Convert to tuple format for display
        commands = [
            (c['command_name'], c.get('text_description', ''))
            for c in db_commands
        ]

        # Pagination
        page_size = 12
        offset = session.get_offset('cmdlist')
        total = len(commands)

        if offset >= total and total > 0:
            offset = max(0, total - page_size)
            session.set_offset('cmdlist', offset)

        page_commands = commands[offset:offset + page_size]

        # Position indicator
        if total > page_size:
            if offset + page_size >= total:
                pos_indicator = "Bottom"
            elif offset == 0:
                pos_indicator = "More..."
            else:
                pos_indicator = "More..."
        else:
            pos_indicator = "Bottom"

        content = [
            pad_line(f" {hostname:<20}       Select Command                           {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type option, press Enter to select command."),
            pad_line("   1=Select"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  Command       Description"), "class": "field-reverse"}],
        ]

        fields = []
        for i, (cmd, desc) in enumerate(page_commands):
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {cmd:<12}  {desc[:50]}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}"})

        # Pad to consistent height
        while len(content) < 19:
            content.append(pad_line(""))

        content.append(pad_line(f"                                                           {pos_indicator:>10}"))

        # Message area
        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        # Filter input
        content.append([
            {"type": "text", "text": " Position to . . . :  "},
            {"type": "input", "id": "filter", "width": 10, "value": cmd_filter, "class": "field-input"},
        ])
        fields.append({"id": "filter"})

        content.append(pad_line(" F3=Exit  F5=Refresh  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "cmdlist",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_cmdlist(self, session: Session, fields: dict) -> dict:
        """Handle command list submission."""
        # Check for filter change
        new_filter = fields.get('filter', '').strip().upper()
        old_filter = session.field_values.get('f4_filter', '')

        if new_filter != old_filter:
            session.field_values['f4_filter'] = new_filter
            session.set_offset('cmdlist', 0)
            return self.get_screen(session, 'cmdlist')

        # Get current page of commands from database
        cmd_filter = session.field_values.get('f4_filter', '').upper()
        db_commands = list_commands(cmd_filter)

        page_size = 12
        offset = session.get_offset('cmdlist')
        page_commands = db_commands[offset:offset + page_size]

        # Check for option selection
        for i, cmd_row in enumerate(page_commands):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt == '1':
                # Execute the selected command
                return self.execute_command(session, cmd_row['command_name'])

        return self.get_screen(session, 'cmdlist')

    def _screen_parmlist(self, session: Session) -> dict:
        """Parameter Valid Values (F4 Prompt) - show valid values from PARM_VALID_VALUES."""
        hostname, date_str, time_str = get_system_info()

        # Get context from F4 press
        command_name = session.field_values.get('f4_command', '')
        parm_name = session.field_values.get('f4_parm', '')
        field_id = session.field_values.get('f4_field_id', '')

        # Query valid values from database
        valid_values = get_parameter_valid_values(command_name, parm_name)

        # Pagination
        page_size = 10
        offset = session.get_offset('parmlist')
        total = len(valid_values)

        if offset >= total and total > 0:
            offset = max(0, total - page_size)
            session.set_offset('parmlist', offset)

        page_values = valid_values[offset:offset + page_size]

        # Position indicator
        if total > page_size:
            if offset + page_size >= total:
                pos_indicator = "Bottom"
            elif offset == 0:
                pos_indicator = "More..."
            else:
                pos_indicator = "More..."
        else:
            pos_indicator = "Bottom"

        content = [
            pad_line(f" {hostname:<20}     Select Parameter Value                    {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(f" Command . . : {command_name:<10}  Parameter . . : {parm_name}"),
            pad_line(""),
            pad_line(" Type option, press Enter to select value."),
            pad_line("   1=Select"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  Value           Description"), "class": "field-reverse"}],
        ]

        fields = []
        for i, val in enumerate(page_values):
            value = val.get('valid_value', '')
            desc = val.get('text_description', '')
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {value:<14}  {desc[:45]}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}"})

        # Pad to consistent height
        while len(content) < 19:
            content.append(pad_line(""))

        content.append(pad_line(f"                                                           {pos_indicator:>10}"))

        # Message area
        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "parmlist",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_parmlist(self, session: Session, fields: dict) -> dict:
        """Handle parameter list submission."""
        command_name = session.field_values.get('f4_command', '')
        parm_name = session.field_values.get('f4_parm', '')
        field_id = session.field_values.get('f4_field_id', '')
        return_screen = session.field_values.get('f4_return_screen', 'main')

        # Query valid values from database
        valid_values = get_parameter_valid_values(command_name, parm_name)

        page_size = 10
        offset = session.get_offset('parmlist')
        page_values = valid_values[offset:offset + page_size]

        # Check for option selection
        for i, val in enumerate(page_values):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt == '1':
                # Store selected value to be used by the return screen
                selected_value = val.get('valid_value', '')
                session.field_values[f'f4_selected_{field_id}'] = selected_value
                # Clear F4 context
                session.field_values.pop('f4_command', None)
                session.field_values.pop('f4_parm', None)
                session.field_values.pop('f4_field_id', None)
                session.set_offset('parmlist', 0)
                return self.get_screen(session, return_screen)

        return self.get_screen(session, 'parmlist')

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

    # ========== USER MANAGEMENT SCREENS ==========

    def _screen_wrkusrprf(self, session: Session) -> dict:
        """Work with User Profiles screen."""
        # Only security officers can access user management
        if session.user_class not in ('*SECOFR', '*SECADM'):
            session.message = "Not authorized to user management"
            session.message_level = "error"
            return self.get_screen(session, 'main')

        hostname, date_str, time_str = get_system_info()

        users = user_manager.list_users()
        offset = session.get_offset('wrkusrprf')
        page_size = 10

        content = []

        # Header
        logo = get_logo()
        if logo:
            for line in logo.split('\n'):
                content.append(pad_line(f"  {line}"))

        content.append(pad_line(""))
        content.append(pad_line(f"  Work with User Profiles                              {hostname}"))
        content.append(pad_line(f"                                                       {date_str}  {time_str}"))
        content.append(pad_line(""))
        content.append(pad_line("  Type options, press Enter."))
        content.append(pad_line("    2=Change   3=Copy   4=Delete   5=Display   7=Rename   8=CHGPWD   12=Auth"))
        content.append(pad_line(""))

        # Column headers
        content.append(pad_line("  Opt  User       Status     Class      Description"))

        # User list
        fields = []
        page_users = users[offset:offset + page_size]

        for i, user in enumerate(page_users):
            field_id = f"opt_{i}"
            fields.append({"id": field_id})

            status = user.status.replace('*', '')[:10]
            user_class = user.user_class.replace('*', '')[:10]
            desc = user.description[:30] if user.description else ""

            content.append([
                {"type": "text", "text": "  "},
                {"type": "input", "id": field_id, "width": 3, "value": ""},
                {"type": "text", "text": f"  {user.username:<10} {status:<10} {user_class:<10} {desc}"},
            ])

        # Pad empty rows
        for _ in range(page_size - len(page_users)):
            content.append(pad_line(""))

        # Footer
        more = "More..." if len(users) > offset + page_size else "Bottom"
        content.append(pad_line(f"                                                              {more}"))
        content.append(pad_line(""))

        # Command line
        fields.append({"id": "cmd"})
        content.append([
            {"type": "text", "text": " Command"},
            {"type": "text", "text": pad_line("", 70)},
        ])
        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 60, "value": ""},
        ])

        # Function keys
        content.append(pad_line(""))
        content.append(pad_line(" F3=Exit  F5=Refresh  F6=Create  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "wrkusrprf",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_wrkusrprf(self, session: Session, fields: dict) -> dict:
        """Handle Work with User Profiles submission."""
        users = user_manager.list_users()
        offset = session.get_offset('wrkusrprf')
        page_size = 10
        page_users = users[offset:offset + page_size]

        for i, user in enumerate(page_users):
            opt = fields.get(f"opt_{i}", "").strip()
            if opt:
                if opt == '2':  # Change
                    session.field_values['selected_user'] = user.username
                    return self.get_screen(session, 'user_change')
                elif opt == '4':  # Delete
                    if user.username in ('QSECOFR', 'QSYSOPR', 'QUSER'):
                        session.message = f"Cannot delete system user {user.username}"
                        session.message_level = "error"
                    else:
                        success, msg = user_manager.delete_user(user.username)
                        session.message = msg
                        session.message_level = "info" if success else "error"
                    return self.get_screen(session, 'wrkusrprf')
                elif opt == '5':  # Display
                    session.field_values['selected_user'] = user.username
                    return self.get_screen(session, 'user_display')
                elif opt == '8':  # Change Password
                    session.field_values['selected_user'] = user.username
                    return self.get_screen(session, 'user_chgpwd')
                elif opt == '12':  # Authorities
                    session.field_values['selected_user'] = user.username
                    return self.get_screen(session, 'user_authorities')
                else:
                    session.message = f"Option {opt} not valid"
                    session.message_level = "error"
                    return self.get_screen(session, 'wrkusrprf')

        cmd = fields.get('cmd', '').strip().upper()
        if cmd:
            return self.execute_command(session, cmd)

        return self.get_screen(session, 'wrkusrprf')

    def _screen_user_display(self, session: Session) -> dict:
        """Display User Profile details."""
        hostname, date_str, time_str = get_system_info()
        username = session.field_values.get('selected_user', 'QUSER')
        user = user_manager.get_user(username)

        if not user:
            session.message = f"User {username} not found"
            session.message_level = "error"
            return self.get_screen(session, 'wrkusrprf')

        content = []

        logo = get_logo()
        if logo:
            for line in logo.split('\n'):
                content.append(pad_line(f"  {line}"))

        content.append(pad_line(""))
        content.append(pad_line(f"  Display User Profile                                 {hostname}"))
        content.append(pad_line(f"                                                       {date_str}  {time_str}"))
        content.append(pad_line(""))
        content.append(pad_line(f"    User profile  . . . . . . . . . :   {user.username}"))
        content.append(pad_line(f"    Status  . . . . . . . . . . . . :   {user.status}"))
        content.append(pad_line(f"    User class  . . . . . . . . . . :   {user.user_class}"))
        content.append(pad_line(f"    Group profile . . . . . . . . . :   {user.group_profile}"))
        content.append(pad_line(f"    Description . . . . . . . . . . :   {user.description}"))
        content.append(pad_line(""))
        content.append(pad_line(f"    Created . . . . . . . . . . . . :   {user.created}"))
        content.append(pad_line(f"    Last sign-on  . . . . . . . . . :   {user.last_signon or 'Never'}"))
        content.append(pad_line(f"    Sign-on attempts  . . . . . . . :   {user.signon_attempts}"))
        content.append(pad_line(f"    Password expires  . . . . . . . :   {user.password_expires}"))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "user_display",
            "cols": 80,
            "content": content,
            "fields": [],
            "activeField": 0,
        }

    def _screen_user_authorities(self, session: Session) -> dict:
        """Display User Authorities screen - shows all object authorities for a user."""
        hostname, date_str, time_str = get_system_info()
        username = session.field_values.get('selected_user', session.user)
        user = user_manager.get_user(username)

        if not user:
            session.message = f"User {username} not found"
            session.message_level = "error"
            return self.get_screen(session, 'wrkusrprf')

        # Get effective authorities (direct + inherited from group)
        authorities = get_effective_authorities(username)
        group_profile = user.group_profile

        content = []

        logo = get_logo()
        if logo:
            for line in logo.split('\n'):
                content.append(pad_line(f"  {line}"))

        content.append(pad_line(""))
        content.append(pad_line(f"  Display User Authorities                             {hostname}"))
        content.append(pad_line(f"                                                       {date_str}  {time_str}"))
        content.append(pad_line(""))
        content.append(pad_line(f"    User profile  . . . . . . . . . :   {username}"))
        content.append(pad_line(f"    Group profile . . . . . . . . . :   {group_profile}"))
        content.append(pad_line(""))

        if group_profile and group_profile != '*NONE':
            content.append(pad_line(f"    (Authorities inherited from {group_profile} shown with *)"))
            content.append(pad_line(""))

        # Column headers
        content.append(pad_line("  Object Type    Object Name                    Authority"))
        content.append(pad_line("  -----------    ------------------------------ ---------"))

        # List authorities (paginated)
        offset = session.get_offset('user_authorities')
        page_size = 8
        page_auths = authorities[offset:offset + page_size]

        for auth in page_auths:
            obj_type = auth['object_type'][:12].ljust(12)
            obj_name = auth['object_name'][:30].ljust(30)
            authority = auth['authority']
            inherited = auth.get('inherited_from', '')
            marker = '*' if inherited else ' '
            content.append(pad_line(f"  {obj_type}   {obj_name} {authority}{marker}"))

        # Pad empty rows
        for _ in range(page_size - len(page_auths)):
            content.append(pad_line(""))

        # Footer
        more = "More..." if len(authorities) > offset + page_size else "Bottom"
        content.append(pad_line(f"                                                              {more}"))
        content.append(pad_line(""))
        content.append(pad_line(" F3=Exit  F12=Cancel  PgUp/PgDn=Scroll"))

        return {
            "type": "screen",
            "screen": "user_authorities",
            "cols": 80,
            "content": content,
            "fields": [],
            "activeField": 0,
        }

    def _screen_user_chgpwd(self, session: Session) -> dict:
        """Change User Password screen."""
        hostname, date_str, time_str = get_system_info()
        username = session.field_values.get('selected_user', session.user)

        content = []

        logo = get_logo()
        if logo:
            for line in logo.split('\n'):
                content.append(pad_line(f"  {line}"))

        content.append(pad_line(""))
        content.append(pad_line(f"  Change Password                                      {hostname}"))
        content.append(pad_line(f"                                                       {date_str}  {time_str}"))
        content.append(pad_line(""))
        content.append(pad_line(f"    User profile  . . . . . . . . . :   {username}"))
        content.append(pad_line(""))

        # Message display
        if session.message:
            content.append(self._message_line(session))
        else:
            content.append(pad_line(""))

        content.append(pad_line(""))
        content.append([
            {"type": "text", "text": "    Current password  . . . . . . . :   "},
            {"type": "input", "id": "current_pwd", "width": 10, "password": True},
        ])
        content.append([
            {"type": "text", "text": "    New password  . . . . . . . . . :   "},
            {"type": "input", "id": "new_pwd", "width": 10, "password": True},
        ])
        content.append([
            {"type": "text", "text": "    Confirm password  . . . . . . . :   "},
            {"type": "input", "id": "confirm_pwd", "width": 10, "password": True},
        ])
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "user_chgpwd",
            "cols": 80,
            "content": content,
            "fields": [
                {"id": "current_pwd"},
                {"id": "new_pwd"},
                {"id": "confirm_pwd"},
            ],
            "activeField": 0,
        }

    def _submit_user_chgpwd(self, session: Session, fields: dict) -> dict:
        """Handle Change Password submission."""
        username = session.field_values.get('selected_user', session.user)
        current_pwd = fields.get('current_pwd', '')
        new_pwd = fields.get('new_pwd', '')
        confirm_pwd = fields.get('confirm_pwd', '')

        # Security officers can change any password without current
        is_secofr = session.user_class in ('*SECOFR', '*SECADM')
        changing_own = username == session.user

        # Verify current password if changing own or not security officer
        if changing_own or not is_secofr:
            success, _ = user_manager.authenticate(username, current_pwd)
            if not success:
                session.message = "Current password not valid"
                session.message_level = "error"
                return self.get_screen(session, 'user_chgpwd')

        # Validate new password
        if not new_pwd:
            session.message = "New password required"
            session.message_level = "error"
            return self.get_screen(session, 'user_chgpwd')

        if new_pwd != confirm_pwd:
            session.message = "Passwords do not match"
            session.message_level = "error"
            return self.get_screen(session, 'user_chgpwd')

        # Change the password
        success, msg = user_manager.change_password(username, new_pwd)
        session.message = msg
        session.message_level = "info" if success else "error"

        if success:
            return self.get_screen(session, 'wrkusrprf')
        return self.get_screen(session, 'user_chgpwd')

    def _screen_user_create(self, session: Session) -> dict:
        """Create User Profile screen."""
        hostname, date_str, time_str = get_system_info()

        content = []

        logo = get_logo()
        if logo:
            for line in logo.split('\n'):
                content.append(pad_line(f"  {line}"))

        content.append(pad_line(""))
        content.append(pad_line(f"  Create User Profile (CRTUSRPRF)                      {hostname}"))
        content.append(pad_line(f"                                                       {date_str}  {time_str}"))
        content.append(pad_line(""))

        # Message display
        if session.message:
            content.append(self._message_line(session))
        else:
            content.append(pad_line(""))

        content.append(pad_line(""))
        content.append([
            {"type": "text", "text": "    User profile  . . . . . . . . . :   "},
            {"type": "input", "id": "new_user", "width": 10, "value": ""},
        ])
        content.append([
            {"type": "text", "text": "    Password  . . . . . . . . . . . :   "},
            {"type": "input", "id": "new_pwd", "width": 10, "password": True},
        ])
        content.append([
            {"type": "text", "text": "    Confirm password  . . . . . . . :   "},
            {"type": "input", "id": "confirm_pwd", "width": 10, "password": True},
        ])
        content.append([
            {"type": "text", "text": "    User class  . . . . . . . . . . :   "},
            {"type": "input", "id": "user_class", "width": 10, "value": "*USER"},
        ])
        content.append([
            {"type": "text", "text": "    Group profile . . . . . . . . . :   "},
            {"type": "input", "id": "group_profile", "width": 10, "value": "*NONE"},
        ])
        content.append([
            {"type": "text", "text": "    Description . . . . . . . . . . :   "},
            {"type": "input", "id": "description", "width": 30, "value": ""},
        ])
        content.append([
            {"type": "text", "text": "    Copy authorities from . . . . . :   "},
            {"type": "input", "id": "copy_from", "width": 10, "value": ""},
        ])
        content.append(pad_line(""))
        content.append(pad_line("    Valid classes: *SECOFR, *SECADM, *PGMR, *SYSOPR, *USER"))
        content.append(pad_line("    Group profile: User to inherit authorities from (*NONE for none)"))
        content.append(pad_line("    Copy authorities from: Copy object authorities from this user"))
        content.append(pad_line(""))
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "user_create",
            "cols": 80,
            "content": content,
            "fields": [
                {"id": "new_user"},
                {"id": "new_pwd"},
                {"id": "confirm_pwd"},
                {"id": "user_class"},
                {"id": "group_profile"},
                {"id": "description"},
                {"id": "copy_from"},
            ],
            "activeField": 0,
        }

    def _submit_user_create(self, session: Session, fields: dict) -> dict:
        """Handle Create User Profile submission."""
        new_user = fields.get('new_user', '').strip().upper()
        new_pwd = fields.get('new_pwd', '')
        confirm_pwd = fields.get('confirm_pwd', '')
        user_class = fields.get('user_class', '*USER').strip().upper()
        group_profile = fields.get('group_profile', '*NONE').strip().upper()
        description = fields.get('description', '').strip()
        copy_from = fields.get('copy_from', '').strip().upper()

        # Validate inputs
        if not new_user:
            session.message = "User profile name required"
            session.message_level = "error"
            return self.get_screen(session, 'user_create')

        if not new_pwd:
            session.message = "Password required"
            session.message_level = "error"
            return self.get_screen(session, 'user_create')

        if new_pwd != confirm_pwd:
            session.message = "Passwords do not match"
            session.message_level = "error"
            return self.get_screen(session, 'user_create')

        valid_classes = ('*SECOFR', '*SECADM', '*PGMR', '*SYSOPR', '*USER')
        if user_class not in valid_classes:
            session.message = f"User class must be one of: {', '.join(valid_classes)}"
            session.message_level = "error"
            return self.get_screen(session, 'user_create')

        # Create the user
        success, msg = user_manager.create_user(
            username=new_user,
            password=new_pwd,
            user_class=user_class,
            description=description,
            group_profile=group_profile,
            copy_from_user=copy_from
        )

        session.message = msg
        session.message_level = "info" if success else "error"

        if success:
            return self.get_screen(session, 'wrkusrprf')
        return self.get_screen(session, 'user_create')

    # ========== SCHEMA AND OBJECT AUTHORITY SCREENS ==========

    def _screen_wrkschema(self, session: Session) -> dict:
        """Work with Schemas (Libraries) screen."""
        # Only security officers and admins can manage schemas
        if session.user_class not in ('*SECOFR', '*SECADM', '*PGMR'):
            session.message = "Not authorized to schema management"
            session.message_level = "error"
            return self.get_screen(session, 'main')

        hostname, date_str, time_str = get_system_info()

        schemas = list_schemas()
        offset = session.get_offset('wrkschema')
        page_size = 10

        content = []

        # Header
        logo = get_logo()
        if logo:
            for line in logo.split('\n'):
                content.append(pad_line(f"  {line}"))

        content.append(pad_line(""))
        content.append(pad_line(f"  Work with Schemas (Libraries)                        {hostname}"))
        content.append(pad_line(f"                                                       {date_str}  {time_str}"))
        content.append(pad_line(""))
        content.append(pad_line("  Type options, press Enter."))
        content.append(pad_line("    4=Delete   5=Display Tables   8=Grant Authority   9=Display Auth"))
        content.append(pad_line(""))

        # Column headers
        content.append(pad_line("  Opt  Schema         Owner        Tables  Authority"))

        # Schema list
        fields = []
        page_schemas = schemas[offset:offset + page_size]

        for i, schema in enumerate(page_schemas):
            field_id = f"opt_{i}"
            fields.append({"id": field_id})

            content.append([
                {"type": "text", "text": "  "},
                {"type": "input", "id": field_id, "width": 3, "value": ""},
                {"type": "text", "text": f"  {schema['name']:<14} {schema['owner']:<12} {schema['table_count']:<7} {schema['authority']}"},
            ])

        # Pad empty rows
        for _ in range(page_size - len(page_schemas)):
            content.append(pad_line(""))

        # Footer
        more = "More..." if len(schemas) > offset + page_size else "Bottom"
        content.append(pad_line(f"                                                              {more}"))
        content.append(pad_line(""))

        # Command line
        fields.append({"id": "cmd"})
        content.append([
            {"type": "text", "text": " Command"},
            {"type": "text", "text": pad_line("", 70)},
        ])
        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 60, "value": ""},
        ])

        # Function keys
        content.append(pad_line(""))
        content.append(pad_line(" F3=Exit  F5=Refresh  F6=Create  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "wrkschema",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_wrkschema(self, session: Session, fields: dict) -> dict:
        """Handle Work with Schemas submission."""
        schemas = list_schemas()
        offset = session.get_offset('wrkschema')
        page_size = 10
        page_schemas = schemas[offset:offset + page_size]

        for i, schema in enumerate(page_schemas):
            opt = fields.get(f"opt_{i}", "").strip()
            if opt:
                if opt == '4':  # Delete
                    if schema['name'] == 'PUBLIC':
                        session.message = "Cannot delete PUBLIC schema"
                        session.message_level = "error"
                    else:
                        success, msg = drop_schema(schema['name'])
                        session.message = msg
                        session.message_level = "info" if success else "error"
                    return self.get_screen(session, 'wrkschema')
                elif opt == '5':  # Display Tables
                    session.field_values['selected_schema'] = schema['name']
                    return self.get_screen(session, 'schema_tables')
                elif opt == '8':  # Grant Authority
                    session.field_values['selected_schema'] = schema['name']
                    return self.get_screen(session, 'grtobjaut')
                elif opt == '9':  # Display Authority
                    session.field_values['selected_schema'] = schema['name']
                    return self.get_screen(session, 'dspobjaut')
                else:
                    session.message = f"Option {opt} not valid"
                    session.message_level = "error"
                    return self.get_screen(session, 'wrkschema')

        cmd = fields.get('cmd', '').strip().upper()
        if cmd:
            return self.execute_command(session, cmd)

        return self.get_screen(session, 'wrkschema')

    def _screen_schema_create(self, session: Session) -> dict:
        """Create Schema screen."""
        hostname, date_str, time_str = get_system_info()

        content = []

        logo = get_logo()
        if logo:
            for line in logo.split('\n'):
                content.append(pad_line(f"  {line}"))

        content.append(pad_line(""))
        content.append(pad_line(f"  Create Schema (CRTSCHEMA)                            {hostname}"))
        content.append(pad_line(f"                                                       {date_str}  {time_str}"))
        content.append(pad_line(""))
        content.append(pad_line(""))

        # Input fields
        fields = [
            {"id": "schema_name"},
            {"id": "owner"},
            {"id": "description"},
        ]

        content.append([
            {"type": "text", "text": "    Schema name  . . . . . . . . . :  "},
            {"type": "input", "id": "schema_name", "width": 20, "value": ""},
        ])
        content.append(pad_line(""))
        content.append([
            {"type": "text", "text": "    Owner (user profile) . . . . . :  "},
            {"type": "input", "id": "owner", "width": 10, "value": ""},
        ])
        content.append(pad_line(""))
        content.append([
            {"type": "text", "text": "    Description  . . . . . . . . . :  "},
            {"type": "input", "id": "description", "width": 40, "value": ""},
        ])
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line("    Owner will have *OWNER authority on schema"))
        content.append(pad_line(""))

        # Pad to fill screen
        for _ in range(6):
            content.append(pad_line(""))

        # Function keys
        content.append(pad_line(""))
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "schema_create",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_schema_create(self, session: Session, fields: dict) -> dict:
        """Handle Create Schema submission."""
        schema_name = fields.get('schema_name', '').strip().upper()
        owner = fields.get('owner', '').strip().upper()
        description = fields.get('description', '').strip()

        if not schema_name:
            session.message = "Schema name is required"
            session.message_level = "error"
            return self.get_screen(session, 'schema_create')

        # Create the schema
        success, msg = create_schema(schema_name, owner if owner else None, description)

        session.message = msg
        session.message_level = "info" if success else "error"

        if success:
            return self.get_screen(session, 'wrkschema')
        return self.get_screen(session, 'schema_create')

    def _screen_schema_tables(self, session: Session) -> dict:
        """Display tables in a schema."""
        hostname, date_str, time_str = get_system_info()
        schema_name = session.field_values.get('selected_schema', 'PUBLIC')

        tables = list_schema_tables(schema_name)

        content = []

        logo = get_logo()
        if logo:
            for line in logo.split('\n'):
                content.append(pad_line(f"  {line}"))

        content.append(pad_line(""))
        content.append(pad_line(f"  Display Schema Tables                                {hostname}"))
        content.append(pad_line(f"                                                       {date_str}  {time_str}"))
        content.append(pad_line(""))
        content.append(pad_line(f"    Schema  . . . . . . . . :  {schema_name}"))
        content.append(pad_line(""))

        # Column headers
        content.append(pad_line("    Table Name                      Type           Columns"))

        if tables:
            for table in tables[:12]:
                content.append(pad_line(f"      {table['name']:<32} {table['type']:<14} {table['columns']}"))
        else:
            content.append(pad_line("      (no tables in this schema)"))

        # Pad to fill screen
        for _ in range(12 - len(tables)):
            content.append(pad_line(""))

        # Function keys
        content.append(pad_line(""))
        content.append(pad_line(" F3=Exit  F5=Refresh  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "schema_tables",
            "cols": 80,
            "content": content,
            "fields": [],
            "activeField": -1,
        }

    def _screen_grtobjaut(self, session: Session) -> dict:
        """Grant Object Authority screen."""
        hostname, date_str, time_str = get_system_info()
        selected_schema = session.field_values.get('selected_schema', '')

        content = []

        logo = get_logo()
        if logo:
            for line in logo.split('\n'):
                content.append(pad_line(f"  {line}"))

        content.append(pad_line(""))
        content.append(pad_line(f"  Grant Object Authority (GRTOBJAUT)                   {hostname}"))
        content.append(pad_line(f"                                                       {date_str}  {time_str}"))
        content.append(pad_line(""))
        content.append(pad_line(""))

        # Input fields
        fields = [
            {"id": "obj_name"},
            {"id": "obj_type"},
            {"id": "username"},
            {"id": "authority"},
        ]

        content.append([
            {"type": "text", "text": "    Object . . . . . . . . . . . . :  "},
            {"type": "input", "id": "obj_name", "width": 30, "value": selected_schema},
        ])
        content.append(pad_line(""))
        content.append([
            {"type": "text", "text": "    Object type  . . . . . . . . . :  "},
            {"type": "input", "id": "obj_type", "width": 10, "value": "*SCHEMA" if selected_schema else ""},
        ])
        content.append(pad_line(""))
        content.append([
            {"type": "text", "text": "    User . . . . . . . . . . . . . :  "},
            {"type": "input", "id": "username", "width": 10, "value": ""},
        ])
        content.append(pad_line(""))
        content.append([
            {"type": "text", "text": "    Authority  . . . . . . . . . . :  "},
            {"type": "input", "id": "authority", "width": 10, "value": "*USE"},
        ])
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line("    Object types: *SCHEMA, *TABLE"))
        content.append(pad_line("    Authorities:  *USE, *CHANGE, *ALL, *OBJMGT, *OWNER, *EXCLUDE"))
        content.append(pad_line(""))

        # Pad to fill screen
        for _ in range(4):
            content.append(pad_line(""))

        # Function keys
        content.append(pad_line(""))
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "grtobjaut",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0 if not selected_schema else 2,  # Focus on user if schema pre-filled
        }

    def _submit_grtobjaut(self, session: Session, fields: dict) -> dict:
        """Handle Grant Object Authority submission."""
        obj_name = fields.get('obj_name', '').strip().upper()
        obj_type = fields.get('obj_type', '').strip().upper()
        username = fields.get('username', '').strip().upper()
        authority = fields.get('authority', '').strip().upper()

        # Normalize object type
        if obj_type.startswith('*'):
            obj_type = obj_type[1:]  # Remove leading *

        if not obj_name:
            session.message = "Object name is required"
            session.message_level = "error"
            return self.get_screen(session, 'grtobjaut')

        if not username:
            session.message = "User is required"
            session.message_level = "error"
            return self.get_screen(session, 'grtobjaut')

        if not authority:
            session.message = "Authority is required"
            session.message_level = "error"
            return self.get_screen(session, 'grtobjaut')

        # Grant the authority
        success, msg = grant_object_authority(
            object_type=obj_type,
            object_name=obj_name,
            username=username,
            authority=authority,
            granted_by=session.user
        )

        session.message = msg
        session.message_level = "info" if success else "error"

        if success:
            return self.get_screen(session, 'wrkschema')
        return self.get_screen(session, 'grtobjaut')

    def _screen_rvkobjaut(self, session: Session) -> dict:
        """Revoke Object Authority screen."""
        hostname, date_str, time_str = get_system_info()
        selected_schema = session.field_values.get('selected_schema', '')

        content = []

        logo = get_logo()
        if logo:
            for line in logo.split('\n'):
                content.append(pad_line(f"  {line}"))

        content.append(pad_line(""))
        content.append(pad_line(f"  Revoke Object Authority (RVKOBJAUT)                  {hostname}"))
        content.append(pad_line(f"                                                       {date_str}  {time_str}"))
        content.append(pad_line(""))
        content.append(pad_line(""))

        # Input fields
        fields = [
            {"id": "obj_name"},
            {"id": "obj_type"},
            {"id": "username"},
        ]

        content.append([
            {"type": "text", "text": "    Object . . . . . . . . . . . . :  "},
            {"type": "input", "id": "obj_name", "width": 30, "value": selected_schema},
        ])
        content.append(pad_line(""))
        content.append([
            {"type": "text", "text": "    Object type  . . . . . . . . . :  "},
            {"type": "input", "id": "obj_type", "width": 10, "value": "*SCHEMA" if selected_schema else ""},
        ])
        content.append(pad_line(""))
        content.append([
            {"type": "text", "text": "    User . . . . . . . . . . . . . :  "},
            {"type": "input", "id": "username", "width": 10, "value": ""},
        ])
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line("    This will revoke ALL authority from the user on this object."))
        content.append(pad_line(""))

        # Pad to fill screen
        for _ in range(6):
            content.append(pad_line(""))

        # Function keys
        content.append(pad_line(""))
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "rvkobjaut",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0 if not selected_schema else 2,
        }

    def _submit_rvkobjaut(self, session: Session, fields: dict) -> dict:
        """Handle Revoke Object Authority submission."""
        obj_name = fields.get('obj_name', '').strip().upper()
        obj_type = fields.get('obj_type', '').strip().upper()
        username = fields.get('username', '').strip().upper()

        # Normalize object type
        if obj_type.startswith('*'):
            obj_type = obj_type[1:]

        if not obj_name:
            session.message = "Object name is required"
            session.message_level = "error"
            return self.get_screen(session, 'rvkobjaut')

        if not username:
            session.message = "User is required"
            session.message_level = "error"
            return self.get_screen(session, 'rvkobjaut')

        # Revoke the authority
        success, msg = revoke_object_authority(
            object_type=obj_type,
            object_name=obj_name,
            username=username
        )

        session.message = msg
        session.message_level = "info" if success else "error"

        if success:
            return self.get_screen(session, 'wrkschema')
        return self.get_screen(session, 'rvkobjaut')

    def _screen_dspobjaut(self, session: Session) -> dict:
        """Display Object Authorities screen."""
        hostname, date_str, time_str = get_system_info()
        selected_schema = session.field_values.get('selected_schema', '')

        # Get authorities, optionally filtered by schema
        if selected_schema:
            authorities = get_object_authorities(object_name=selected_schema)
        else:
            authorities = get_object_authorities()

        offset = session.get_offset('dspobjaut')
        page_size = 10

        content = []

        logo = get_logo()
        if logo:
            for line in logo.split('\n'):
                content.append(pad_line(f"  {line}"))

        content.append(pad_line(""))
        content.append(pad_line(f"  Display Object Authorities                           {hostname}"))
        content.append(pad_line(f"                                                       {date_str}  {time_str}"))
        content.append(pad_line(""))

        if selected_schema:
            content.append(pad_line(f"    Object  . . . . . . . . :  {selected_schema}"))
        else:
            content.append(pad_line("    Showing all object authorities"))
        content.append(pad_line(""))

        # Column headers
        content.append(pad_line("    Type     Object              User       Authority  Granted By"))

        # Authority list
        page_auths = authorities[offset:offset + page_size]

        for auth in page_auths:
            content.append(pad_line(
                f"    {auth['object_type']:<8} {auth['object_name']:<18} {auth['username']:<10} "
                f"{auth['authority']:<10} {auth['granted_by']}"
            ))

        if not authorities:
            content.append(pad_line("    (no authorities found)"))

        # Pad empty rows
        for _ in range(page_size - len(page_auths)):
            content.append(pad_line(""))

        # Footer
        more = "More..." if len(authorities) > offset + page_size else "Bottom"
        content.append(pad_line(f"                                                              {more}"))
        content.append(pad_line(""))

        # Function keys
        content.append(pad_line(" F3=Exit  F5=Refresh  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "dspobjaut",
            "cols": 80,
            "content": content,
            "fields": [],
            "activeField": -1,
        }

    # ========== SYSTEM VALUES SCREENS ==========

    def _screen_wrksysval(self, session: Session) -> dict:
        """Work with System Values screen."""
        # Only security officers can manage system values
        if session.user_class not in ('*SECOFR', '*SECADM'):
            session.message = "Not authorized to system values"
            session.message_level = "error"
            return self.get_screen(session, 'main')

        hostname, date_str, time_str = get_system_info()

        sysvals = list_system_values()
        offset = session.get_offset('wrksysval')
        page_size = 10

        content = []

        # Header with configurable logo
        logo = get_logo()
        if logo:
            for line in logo.split('\n'):
                content.append(pad_line(f"  {line}"))

        content.append(pad_line(""))
        content.append(pad_line(f"  Work with System Values                              {hostname}"))
        content.append(pad_line(f"                                                       {date_str}  {time_str}"))
        content.append(pad_line(""))

        # Message display
        if session.message:
            content.append(self._message_line(session))
        else:
            content.append(pad_line(""))

        content.append(pad_line("  Type option, press Enter."))
        content.append(pad_line("    2=Change"))
        content.append(pad_line(""))

        # Column headers
        content.append(pad_line("  Opt  System Value    Value                Category"))

        # System values list
        fields = []
        page_vals = sysvals[offset:offset + page_size]

        for i, sv in enumerate(page_vals):
            field_id = f"opt_{i}"
            fields.append({"id": field_id})

            content.append([
                {"type": "text", "text": "  "},
                {"type": "input", "id": field_id, "width": 3, "value": ""},
                {"type": "text", "text": f"  {sv['name']:<14} {sv['value']:<20} {sv['category']}"},
            ])

        # Pad empty rows
        for _ in range(page_size - len(page_vals)):
            content.append(pad_line(""))

        # Footer
        more = "More..." if len(sysvals) > offset + page_size else "Bottom"
        content.append(pad_line(f"                                                              {more}"))
        content.append(pad_line(""))
        content.append(pad_line(" F3=Exit  F5=Refresh  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "wrksysval",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_wrksysval(self, session: Session, fields: dict) -> dict:
        """Handle Work with System Values submission."""
        sysvals = list_system_values()
        offset = session.get_offset('wrksysval')
        page_size = 10
        page_vals = sysvals[offset:offset + page_size]

        for i, sv in enumerate(page_vals):
            opt = fields.get(f"opt_{i}", "").strip()
            if opt == '2':  # Change
                session.field_values['selected_sysval'] = sv['name']
                session.field_values['sysval_value'] = sv['value']
                session.field_values['sysval_desc'] = sv['description']
                return self.get_screen(session, 'chgsysval')

        return self.get_screen(session, 'wrksysval')

    def _screen_chgsysval(self, session: Session) -> dict:
        """Change System Value screen."""
        hostname, date_str, time_str = get_system_info()
        sysval_name = session.field_values.get('selected_sysval', '')
        sysval_value = session.field_values.get('sysval_value', '')
        sysval_desc = session.field_values.get('sysval_desc', '')

        content = []

        # Header with configurable logo
        logo = get_logo()
        if logo:
            for line in logo.split('\n'):
                content.append(pad_line(f"  {line}"))

        content.append(pad_line(""))
        content.append(pad_line(f"  Change System Value                                  {hostname}"))
        content.append(pad_line(f"                                                       {date_str}  {time_str}"))
        content.append(pad_line(""))

        # Message display
        if session.message:
            content.append(self._message_line(session))
        else:
            content.append(pad_line(""))

        content.append(pad_line(""))
        content.append(pad_line(f"    System value  . . . . . . . :   {sysval_name}"))
        content.append(pad_line(f"    Description . . . . . . . . :   {sysval_desc}"))
        content.append(pad_line(""))
        content.append([
            {"type": "text", "text": "    Value . . . . . . . . . . . :   "},
            {"type": "input", "id": "new_value", "width": 30, "value": sysval_value},
        ])
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(""))
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "chgsysval",
            "cols": 80,
            "content": content,
            "fields": [{"id": "new_value"}],
            "activeField": 0,
        }

    def _submit_chgsysval(self, session: Session, fields: dict) -> dict:
        """Handle Change System Value submission."""
        sysval_name = session.field_values.get('selected_sysval', '')
        new_value = fields.get('new_value', '').strip()

        if not new_value:
            session.message = "Value is required"
            session.message_level = "error"
            return self.get_screen(session, 'chgsysval')

        success, msg = set_system_value(sysval_name, new_value, session.user)

        session.message = msg
        session.message_level = "info" if success else "error"

        if success:
            return self.get_screen(session, 'wrksysval')
        return self.get_screen(session, 'chgsysval')

    # ========================================
    # MESSAGE QUEUE SCREENS
    # ========================================

    def _screen_wrkmsgq(self, session: Session) -> dict:
        """Work with Message Queues screen."""
        hostname, date_str, time_str = get_system_info()
        queues = list_message_queues()

        content = [
            pad_line(f" {hostname:<20}       Work with Message Queues              {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type options, press Enter."),
            pad_line("   2=Display   4=Delete   5=Display messages   6=Send message"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  Queue       Type      Msgs  Description"), "class": "field-reverse"}],
        ]

        fields = []
        for i, q in enumerate(queues):
            msg_count = q.get('total_count', 0)
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {q['name']:<10}  {q['queue_type']:<8}  {msg_count:>4}  {q['description'][:30]}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}"})

        while len(content) < 19:
            content.append(pad_line(""))

        content.append(pad_line("                                                                  Bottom"))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        fields.append({"id": "cmd"})
        content.append(pad_line(" F3=Exit  F5=Refresh  F6=Create  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "wrkmsgq",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_wrkmsgq(self, session: Session, fields: dict) -> dict:
        """Handle Work with Message Queues submission."""
        cmd = fields.get('cmd', '').strip().upper()
        if cmd:
            return self.execute_command(session, cmd)

        queues = list_message_queues()

        for i, q in enumerate(queues):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt:
                queue_name = q['name']
                if opt == '2':
                    session.field_values['selected_msgq'] = queue_name
                    return self.get_screen(session, 'dspmsg')
                elif opt == '4':
                    success, msg = delete_message_queue(queue_name)
                    session.message = msg
                    session.message_level = "info" if success else "error"
                    return self.get_screen(session, 'wrkmsgq')
                elif opt == '5':
                    session.field_values['selected_msgq'] = queue_name
                    return self.get_screen(session, 'dspmsg')
                elif opt == '6':
                    session.field_values['selected_msgq'] = queue_name
                    return self.get_screen(session, 'sndmsg')

        return self.get_screen(session, 'wrkmsgq')

    def _screen_dspmsg(self, session: Session) -> dict:
        """Display Messages screen."""
        hostname, date_str, time_str = get_system_info()
        queue_name = session.field_values.get('selected_msgq', 'QSYSOPR')
        messages = get_messages(queue_name, limit=12)

        content = [
            pad_line(f" {hostname:<20}       Display Messages - {queue_name:<10}         {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type options, press Enter."),
            pad_line("   4=Remove"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  Type      From        Time      Message"), "class": "field-reverse"}],
        ]

        fields = []
        for i, msg in enumerate(messages):
            sent_at = msg.get('sent_at', '')
            if sent_at:
                sent_at = str(sent_at)[11:19]
            msg_text = msg.get('msg_text', '')[:32]
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {msg['msg_type']:<8}  {msg.get('sent_by', ''):<10}  {sent_at}  {msg_text}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}"})

        while len(content) < 19:
            content.append(pad_line(""))

        content.append(pad_line("                                                                  Bottom"))

        msg_display = session.message if session.message else ""
        content.append(pad_line(f" {msg_display}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        fields.append({"id": "cmd"})
        content.append(pad_line(" F3=Exit  F5=Refresh  F6=Send message  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "dspmsg",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_dspmsg(self, session: Session, fields: dict) -> dict:
        """Handle Display Messages submission."""
        cmd = fields.get('cmd', '').strip().upper()
        if cmd:
            return self.execute_command(session, cmd)

        queue_name = session.field_values.get('selected_msgq', 'QSYSOPR')
        messages = get_messages(queue_name, limit=12)

        for i, msg in enumerate(messages):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt == '4':
                msg_id = msg['id']
                success, result = delete_message(queue_name, msg_id)
                session.message = result
                session.message_level = "info" if success else "error"

        return self.get_screen(session, 'dspmsg')

    def _screen_sndmsg(self, session: Session) -> dict:
        """Send Message screen."""
        hostname, date_str, time_str = get_system_info()
        queue_name = session.field_values.get('selected_msgq', 'QSYSOPR')

        content = [
            pad_line(f" {hostname:<20}           Send Message                       {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(f" Message queue  . . . . :  {queue_name}"),
            pad_line(""),
            pad_line(" Type message text, press Enter."),
            pad_line(""),
            [
                {"type": "text", "text": " Message type  . . . . :  "},
                {"type": "input", "id": "msg_type", "width": 10, "value": "*INFO"},
                {"type": "text", "text": "  (*INFO, *INQ, *NOTIFY)"},
            ],
            pad_line(""),
            pad_line(" Message text:"),
            [
                {"type": "text", "text": " "},
                {"type": "input", "id": "msg_text", "width": 70},
            ],
        ]

        while len(content) < 20:
            content.append(pad_line(""))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "sndmsg",
            "cols": 80,
            "content": content,
            "fields": [{"id": "msg_type"}, {"id": "msg_text"}],
            "activeField": 1,
        }

    def _submit_sndmsg(self, session: Session, fields: dict) -> dict:
        """Handle Send Message submission."""
        queue_name = session.field_values.get('selected_msgq', 'QSYSOPR')
        msg_type = fields.get('msg_type', '*INFO').strip().upper() or '*INFO'
        msg_text = fields.get('msg_text', '').strip()

        if not msg_text:
            session.message = "Message text is required"
            session.message_level = "error"
            return self.get_screen(session, 'sndmsg')

        success, msg = send_message(queue_name, msg_text, msg_type=msg_type, sent_by=session.user)

        session.message = msg
        session.message_level = "info" if success else "error"

        if success:
            return self.get_screen(session, 'dspmsg')
        return self.get_screen(session, 'sndmsg')

    # ========================================
    # DATA AREA SCREENS
    # ========================================

    def _screen_wrkdtaara(self, session: Session) -> dict:
        """Work with Data Areas screen."""
        hostname, date_str, time_str = get_system_info()
        dtaaras = list_data_areas()

        content = [
            pad_line(f" {hostname:<20}       Work with Data Areas                   {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type options, press Enter."),
            pad_line("   2=Display   4=Delete   5=Change"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  Data Area   Library    Type    Len  Description"), "class": "field-reverse"}],
        ]

        fields = []
        for i, d in enumerate(dtaaras[:12]):
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {d['name']:<10}  {d['library']:<10} {d['type']:<6} {d['length']:>5}  {d['description'][:20]}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}"})

        while len(content) < 19:
            content.append(pad_line(""))

        content.append(pad_line("                                                                  Bottom"))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        fields.append({"id": "cmd"})
        content.append(pad_line(" F3=Exit  F5=Refresh  F6=Create  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "wrkdtaara",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_wrkdtaara(self, session: Session, fields: dict) -> dict:
        """Handle Work with Data Areas submission."""
        cmd = fields.get('cmd', '').strip().upper()
        if cmd:
            return self.execute_command(session, cmd)

        dtaaras = list_data_areas()

        for i, d in enumerate(dtaaras[:12]):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt:
                if opt == '2':
                    session.field_values['selected_dtaara'] = d['name']
                    session.field_values['selected_dtaara_lib'] = d['library']
                    return self.get_screen(session, 'dspdtaara')
                elif opt == '4':
                    success, msg = delete_data_area(d['name'], d['library'])
                    session.message = msg
                    session.message_level = "info" if success else "error"
                    return self.get_screen(session, 'wrkdtaara')
                elif opt == '5':
                    session.field_values['selected_dtaara'] = d['name']
                    session.field_values['selected_dtaara_lib'] = d['library']
                    return self.get_screen(session, 'chgdtaara')

        return self.get_screen(session, 'wrkdtaara')

    def _screen_dspdtaara(self, session: Session) -> dict:
        """Display Data Area screen."""
        hostname, date_str, time_str = get_system_info()
        name = session.field_values.get('selected_dtaara', '')
        library = session.field_values.get('selected_dtaara_lib', '*LIBL')
        dtaara = get_data_area(name, library)

        if not dtaara:
            session.message = f"Data area {name} not found"
            session.message_level = "error"
            return self.get_screen(session, 'wrkdtaara')

        content = [
            pad_line(f" {hostname:<20}       Display Data Area                      {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(f" Data area  . . . . :  {dtaara['name']}"),
            pad_line(f" Library  . . . . . :  {dtaara['library']}"),
            pad_line(f" Type . . . . . . . :  {dtaara['type']}"),
            pad_line(f" Length . . . . . . :  {dtaara['length']}"),
            pad_line(f" Description  . . . :  {dtaara['description']}"),
            pad_line(""),
            pad_line(" Value:"),
            pad_line(f"   {str(dtaara.get('value', ''))[:65]}"),
            pad_line(""),
            pad_line(f" Locked by  . . . . :  {dtaara.get('locked_by') or '*NONE'}"),
            pad_line(f" Created by . . . . :  {dtaara.get('created_by', '')}"),
        ]

        while len(content) < 21:
            content.append(pad_line(""))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append(pad_line(" F3=Exit  F5=Refresh  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "dspdtaara",
            "cols": 80,
            "content": content,
            "fields": [],
            "activeField": None,
        }

    def _screen_crtdtaara(self, session: Session) -> dict:
        """Create Data Area screen."""
        hostname, date_str, time_str = get_system_info()

        content = [
            pad_line(f" {hostname:<20}    Create Data Area (CRTDTAARA)              {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type choices, press Enter."),
            pad_line(""),
            [
                {"type": "text", "text": " Data area  . . . . . . :  "},
                {"type": "input", "id": "name", "width": 10},
            ],
            [
                {"type": "text", "text": " Library  . . . . . . . :  "},
                {"type": "input", "id": "library", "width": 10, "value": "*LIBL"},
            ],
            [
                {"type": "text", "text": " Type . . . . . . . . . :  "},
                {"type": "input", "id": "type", "width": 10, "value": "*CHAR"},
                {"type": "text", "text": "  *CHAR, *DEC, *LGL"},
            ],
            [
                {"type": "text", "text": " Length . . . . . . . . :  "},
                {"type": "input", "id": "length", "width": 10, "value": "2000"},
            ],
            [
                {"type": "text", "text": " Initial value  . . . . :  "},
                {"type": "input", "id": "value", "width": 40},
            ],
            [
                {"type": "text", "text": " Description  . . . . . :  "},
                {"type": "input", "id": "description", "width": 40},
            ],
        ]

        while len(content) < 21:
            content.append(pad_line(""))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "crtdtaara",
            "cols": 80,
            "content": content,
            "fields": [{"id": "name"}, {"id": "library"}, {"id": "type"}, {"id": "length"}, {"id": "value"}, {"id": "description"}],
            "activeField": 0,
        }

    def _submit_crtdtaara(self, session: Session, fields: dict) -> dict:
        """Handle Create Data Area submission."""
        name = fields.get('name', '').strip().upper()
        library = fields.get('library', '*LIBL').strip().upper() or '*LIBL'
        dtaara_type = fields.get('type', '*CHAR').strip().upper() or '*CHAR'
        try:
            length = int(fields.get('length', '2000') or '2000')
        except ValueError:
            length = 2000
        value = fields.get('value', '').strip()
        description = fields.get('description', '').strip()

        if not name:
            session.message = "Data area name is required"
            session.message_level = "error"
            return self.get_screen(session, 'crtdtaara')

        success, msg = create_data_area(
            name=name,
            library=library,
            dtaara_type=dtaara_type,
            length=length,
            value=value,
            description=description,
            created_by=session.user
        )

        session.message = msg
        session.message_level = "info" if success else "error"

        if success:
            return self.get_screen(session, 'wrkdtaara')
        return self.get_screen(session, 'crtdtaara')

    def _screen_chgdtaara(self, session: Session) -> dict:
        """Change Data Area screen."""
        hostname, date_str, time_str = get_system_info()
        name = session.field_values.get('selected_dtaara', '')
        library = session.field_values.get('selected_dtaara_lib', '*LIBL')
        dtaara = get_data_area(name, library)

        if not dtaara:
            session.message = f"Data area {name} not found"
            session.message_level = "error"
            return self.get_screen(session, 'wrkdtaara')

        current_value = str(dtaara.get('value', ''))

        content = [
            pad_line(f" {hostname:<20}   Change Data Area (CHGDTAARA)               {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(f" Data area  . . . . :  {name}"),
            pad_line(f" Library  . . . . . :  {library}"),
            pad_line(f" Type . . . . . . . :  {dtaara['type']}"),
            pad_line(f" Length . . . . . . :  {dtaara['length']}"),
            pad_line(""),
            pad_line(" Type new value, press Enter."),
            pad_line(""),
            [
                {"type": "text", "text": " New value  . . . . :  "},
                {"type": "input", "id": "new_value", "width": 50, "value": current_value[:50]},
            ],
        ]

        while len(content) < 21:
            content.append(pad_line(""))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "chgdtaara",
            "cols": 80,
            "content": content,
            "fields": [{"id": "new_value"}],
            "activeField": 0,
        }

    def _submit_chgdtaara(self, session: Session, fields: dict) -> dict:
        """Handle Change Data Area submission."""
        name = session.field_values.get('selected_dtaara', '')
        library = session.field_values.get('selected_dtaara_lib', '*LIBL')
        new_value = fields.get('new_value', '').strip()

        success, msg = change_data_area(name, library, value=new_value, updated_by=session.user)

        session.message = msg
        session.message_level = "info" if success else "error"

        if success:
            return self.get_screen(session, 'wrkdtaara')
        return self.get_screen(session, 'chgdtaara')

    # ========================================
    # JOB DESCRIPTION SCREENS
    # ========================================

    def _screen_wrkjobd(self, session: Session) -> dict:
        """Work with Job Descriptions screen."""
        hostname, date_str, time_str = get_system_info()
        library = session.field_values.get('jobd_library', '*ALL')
        jobds = list_job_descriptions(library if library != '*ALL' else None)

        content = [
            pad_line(f" {hostname:<20}       Work with Job Descriptions             {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(f" Library  . . . . . :  {library}"),
            pad_line(""),
            pad_line(" Type options, press Enter."),
            pad_line("   2=Display   4=Delete   5=Change"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  Job Desc    Library    Job Queue   Priority  Description"), "class": "field-reverse"}],
        ]

        fields = []
        for i, j in enumerate(jobds):
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {j['name']:<10}  {j['library']:<10} {j['job_queue']:<10} {j['job_priority']:>3}       {j['description'][:20]}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}"})

        while len(content) < 19:
            content.append(pad_line(""))

        content.append(pad_line("                                                                  Bottom"))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        fields.append({"id": "cmd"})
        content.append(pad_line(" F3=Exit  F5=Refresh  F6=Create  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "wrkjobd",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_wrkjobd(self, session: Session, fields: dict) -> dict:
        """Handle Work with Job Descriptions submission."""
        library = session.field_values.get('jobd_library', '*ALL')
        jobds = list_job_descriptions(library if library != '*ALL' else None)

        for i, j in enumerate(jobds):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt:
                if opt == '2':
                    session.field_values['selected_jobd'] = j['name']
                    session.field_values['selected_jobd_lib'] = j['library']
                    return self.get_screen(session, 'dspjobd')
                elif opt == '4':
                    success, msg = delete_job_description(j['name'], j['library'])
                    session.message = msg
                    session.message_level = "info" if success else "error"
                    return self.get_screen(session, 'wrkjobd')
                elif opt == '5':
                    session.field_values['selected_jobd'] = j['name']
                    session.field_values['selected_jobd_lib'] = j['library']
                    return self.get_screen(session, 'chgjobd')

        return self.get_screen(session, 'wrkjobd')

    def _screen_dspjobd(self, session: Session) -> dict:
        """Display Job Description screen."""
        hostname, date_str, time_str = get_system_info()
        name = session.field_values.get('selected_jobd', '')
        library = session.field_values.get('selected_jobd_lib', '*LIBL')
        jobd = get_job_description(name, library)

        if not jobd:
            session.message = f"Job description {name} not found"
            session.message_level = "error"
            return self.get_screen(session, 'wrkjobd')

        content = [
            pad_line(f" {hostname:<20}       Display Job Description                 {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(f" Job description  . :  {jobd['name']}"),
            pad_line(f" Library  . . . . . :  {jobd['library']}"),
            pad_line(f" Description  . . . :  {jobd['description']}"),
            pad_line(""),
            pad_line(" Job attributes:"),
            pad_line(f"   Job queue  . . . :  {jobd['job_queue']}"),
            pad_line(f"   Job priority . . :  {jobd['job_priority']}"),
            pad_line(f"   Output queue . . :  {jobd['output_queue']}"),
            pad_line(f"   Output priority  :  {jobd.get('output_priority', 5)}"),
            pad_line(f"   Print device . . :  {jobd.get('print_device', '*USRPRF')}"),
            pad_line(""),
            pad_line(f"   Log level  . . . :  {jobd.get('log_level', 4)}"),
            pad_line(f"   Log severity . . :  {jobd.get('log_severity', 0)}"),
            pad_line(f"   Log text . . . . :  {jobd.get('log_text', '*MSG')}"),
            pad_line(""),
        ]

        while len(content) < 21:
            content.append(pad_line(""))

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "dspjobd",
            "cols": 80,
            "content": content,
            "fields": [{"id": "cmd"}],
            "activeField": 0,
        }

    def _screen_crtjobd(self, session: Session) -> dict:
        """Create Job Description screen."""
        hostname, date_str, time_str = get_system_info()

        content = [
            pad_line(f" {hostname:<20}       Create Job Description (CRTJOBD)        {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type choices, press Enter."),
            pad_line(""),
            [
                {"type": "text", "text": " Job description  . . . :  "},
                {"type": "input", "id": "name", "width": 10, "class": "field-input"},
            ],
            [
                {"type": "text", "text": " Library  . . . . . . . :  "},
                {"type": "input", "id": "library", "width": 10, "class": "field-input", "value": "*LIBL"},
                {"type": "text", "text": "  *LIBL, name"},
            ],
            [
                {"type": "text", "text": " Description  . . . . . :  "},
                {"type": "input", "id": "description", "width": 40, "class": "field-input"},
            ],
            pad_line(""),
            pad_line(" Job attributes:"),
            [
                {"type": "text", "text": "   Job queue  . . . . . :  "},
                {"type": "input", "id": "job_queue", "width": 10, "class": "field-input", "value": "QBATCH"},
            ],
            [
                {"type": "text", "text": "   Job priority . . . . :  "},
                {"type": "input", "id": "job_priority", "width": 2, "class": "field-input", "value": "5"},
                {"type": "text", "text": "        1-9"},
            ],
            [
                {"type": "text", "text": "   Output queue . . . . :  "},
                {"type": "input", "id": "output_queue", "width": 10, "class": "field-input", "value": "*USRPRF"},
            ],
            pad_line(""),
        ]

        while len(content) < 21:
            content.append(pad_line(""))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "crtjobd",
            "cols": 80,
            "content": content,
            "fields": [
                {"id": "name"},
                {"id": "library"},
                {"id": "description"},
                {"id": "job_queue"},
                {"id": "job_priority"},
                {"id": "output_queue"},
                {"id": "cmd"},
            ],
            "activeField": 0,
        }

    def _submit_crtjobd(self, session: Session, fields: dict) -> dict:
        """Handle Create Job Description submission."""
        name = fields.get('name', '').strip().upper()
        library = fields.get('library', '*LIBL').strip().upper() or '*LIBL'
        description = fields.get('description', '').strip()
        job_queue = fields.get('job_queue', 'QBATCH').strip().upper() or 'QBATCH'
        job_priority = int(fields.get('job_priority', '5') or '5')
        output_queue = fields.get('output_queue', '*USRPRF').strip().upper() or '*USRPRF'

        if not name:
            session.message = "Job description name is required"
            session.message_level = "error"
            return self.get_screen(session, 'crtjobd')

        success, msg = create_job_description(
            name=name,
            library=library,
            description=description,
            job_queue=job_queue,
            job_priority=job_priority,
            output_queue=output_queue,
            created_by=session.user
        )

        session.message = msg
        session.message_level = "info" if success else "error"

        if success:
            return self.get_screen(session, 'wrkjobd')
        return self.get_screen(session, 'crtjobd')

    # ========================================
    # OUTPUT QUEUE AND SPOOLED FILE SCREENS
    # ========================================

    def _screen_wrkoutq(self, session: Session) -> dict:
        """Work with Output Queues screen."""
        hostname, date_str, time_str = get_system_info()
        outqs = list_output_queues()

        content = [
            pad_line(f" {hostname:<20}       Work with Output Queues                 {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type options, press Enter."),
            pad_line("   2=Display   4=Delete   5=Work with spooled files   8=Hold   9=Release"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  Output Queue  Library    Status   Files  Description"), "class": "field-reverse"}],
        ]

        fields = []
        for i, q in enumerate(outqs):
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {q['name']:<12}  {q['library']:<10} {q['status']:<7} {q.get('file_count', 0):>5}  {q['description'][:20]}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}"})

        while len(content) < 19:
            content.append(pad_line(""))

        content.append(pad_line("                                                                  Bottom"))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        fields.append({"id": "cmd"})
        content.append(pad_line(" F3=Exit  F5=Refresh  F6=Create  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "wrkoutq",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_wrkoutq(self, session: Session, fields: dict) -> dict:
        """Handle Work with Output Queues submission."""
        outqs = list_output_queues()

        for i, q in enumerate(outqs):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt:
                if opt == '4':
                    success, msg = delete_output_queue(q['name'], q['library'])
                    session.message = msg
                    session.message_level = "info" if success else "error"
                elif opt == '5':
                    session.field_values['selected_outq'] = q['name']
                    return self.get_screen(session, 'wrksplf')
                elif opt == '8':
                    success, msg = hold_output_queue(q['name'], q['library'])
                    session.message = msg
                    session.message_level = "info" if success else "error"
                elif opt == '9':
                    success, msg = release_output_queue(q['name'], q['library'])
                    session.message = msg
                    session.message_level = "info" if success else "error"

        return self.get_screen(session, 'wrkoutq')

    def _screen_wrksplf(self, session: Session) -> dict:
        """Work with Spooled Files screen."""
        hostname, date_str, time_str = get_system_info()
        user = session.field_values.get('splf_user', '*CURRENT')
        if user == '*CURRENT':
            user = session.user

        outq = session.field_values.get('selected_outq')
        splfs = list_spooled_files(user=user, output_queue=outq)

        content = [
            pad_line(f" {hostname:<20}       Work with Spooled Files                 {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(f" User . . . . . . . :  {user}"),
            pad_line(""),
            pad_line(" Type options, press Enter."),
            pad_line("   2=Display   4=Delete   8=Hold   9=Release"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  File       Nbr  Job                Status   Pages  Queue"), "class": "field-reverse"}],
        ]

        fields = []
        for i, s in enumerate(splfs):
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {s['name']:<10} {s['file_number']:>3}  {s['job_name'][:18]:<18} {s['status']:<7} {s.get('pages', 0):>5}  {s['output_queue']}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}"})

        while len(content) < 19:
            content.append(pad_line(""))

        content.append(pad_line("                                                                  Bottom"))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        fields.append({"id": "cmd"})
        content.append(pad_line(" F3=Exit  F5=Refresh  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "wrksplf",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_wrksplf(self, session: Session, fields: dict) -> dict:
        """Handle Work with Spooled Files submission."""
        user = session.field_values.get('splf_user', '*CURRENT')
        if user == '*CURRENT':
            user = session.user

        outq = session.field_values.get('selected_outq')
        splfs = list_spooled_files(user=user, output_queue=outq)

        for i, s in enumerate(splfs):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt:
                splf_id = s['id']
                if opt == '2':
                    session.field_values['selected_splf'] = splf_id
                    return self.get_screen(session, 'dspsplf')
                elif opt == '4':
                    success, msg = delete_spooled_file(splf_id)
                    session.message = msg
                    session.message_level = "info" if success else "error"
                elif opt == '8':
                    success, msg = hold_spooled_file(splf_id)
                    session.message = msg
                    session.message_level = "info" if success else "error"
                elif opt == '9':
                    success, msg = release_spooled_file(splf_id)
                    session.message = msg
                    session.message_level = "info" if success else "error"

        return self.get_screen(session, 'wrksplf')

    def _screen_dspsplf(self, session: Session) -> dict:
        """Display Spooled File screen."""
        hostname, date_str, time_str = get_system_info()
        splf_id = session.field_values.get('selected_splf')
        splf = get_spooled_file(splf_id) if splf_id else None

        if not splf:
            session.message = "Spooled file not found"
            session.message_level = "error"
            return self.get_screen(session, 'wrksplf')

        content_lines = (splf.get('content') or '').split('\n')[:12]

        content = [
            pad_line(f" {hostname:<20}       Display Spooled File                     {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(f" File  . . . . . . :  {splf['name']}"),
            pad_line(f" Job  . . . . . . . :  {splf['job_name']}"),
            pad_line(f" File number  . . . :  {splf['file_number']}"),
            pad_line(f" Status . . . . . . :  {splf['status']}"),
            pad_line(""),
            pad_line(" " + "-" * 72),
        ]

        for line in content_lines:
            content.append(pad_line(f" {line[:72]}"))

        content.append(pad_line(" " + "-" * 72))

        while len(content) < 21:
            content.append(pad_line(""))

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "dspsplf",
            "cols": 80,
            "content": content,
            "fields": [{"id": "cmd"}],
            "activeField": 0,
        }

    # ========================================
    # JOB SCHEDULE ENTRY SCREENS
    # ========================================

    def _screen_wrkjobscde(self, session: Session) -> dict:
        """Work with Job Schedule Entries screen."""
        hostname, date_str, time_str = get_system_info()
        entries = list_job_schedule_entries()

        content = [
            pad_line(f" {hostname:<20}       Work with Job Schedule Entries           {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type options, press Enter."),
            pad_line("   2=Display   4=Remove   8=Hold   9=Release"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  Entry Name         Frequency  Time     Status     Next Run"), "class": "field-reverse"}],
        ]

        fields = []
        for i, e in enumerate(entries):
            sched_time = str(e.get('schedule_time', ''))[:5] if e.get('schedule_time') else ''
            next_run = str(e.get('next_run_time', ''))[:16] if e.get('next_run_time') else ''
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {e['name']:<18} {e['frequency']:<10} {sched_time:<8} {e['status']:<10} {next_run}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}"})

        while len(content) < 19:
            content.append(pad_line(""))

        content.append(pad_line("                                                                  Bottom"))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        fields.append({"id": "cmd"})
        content.append(pad_line(" F3=Exit  F5=Refresh  F6=Add  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "wrkjobscde",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_wrkjobscde(self, session: Session, fields: dict) -> dict:
        """Handle Work with Job Schedule Entries submission."""
        entries = list_job_schedule_entries()

        for i, e in enumerate(entries):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt:
                entry_name = e['name']
                if opt == '2':
                    session.field_values['selected_jobscde'] = entry_name
                    return self.get_screen(session, 'dspjobscde')
                elif opt == '4':
                    success, msg = remove_job_schedule_entry(entry_name)
                    session.message = msg
                    session.message_level = "info" if success else "error"
                elif opt == '8':
                    success, msg = hold_job_schedule_entry(entry_name)
                    session.message = msg
                    session.message_level = "info" if success else "error"
                elif opt == '9':
                    success, msg = release_job_schedule_entry(entry_name)
                    session.message = msg
                    session.message_level = "info" if success else "error"

        return self.get_screen(session, 'wrkjobscde')

    def _screen_dspjobscde(self, session: Session) -> dict:
        """Display Job Schedule Entry screen."""
        hostname, date_str, time_str = get_system_info()
        name = session.field_values.get('selected_jobscde', '')
        entry = get_job_schedule_entry(name)

        if not entry:
            session.message = f"Job schedule entry {name} not found"
            session.message_level = "error"
            return self.get_screen(session, 'wrkjobscde')

        content = [
            pad_line(f" {hostname:<20}       Display Job Schedule Entry               {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(f" Entry name . . . . :  {entry['name']}"),
            pad_line(f" Description  . . . :  {entry['description']}"),
            pad_line(f" Status . . . . . . :  {entry['status']}"),
            pad_line(""),
            pad_line(" Schedule:"),
            pad_line(f"   Frequency  . . . :  {entry['frequency']}"),
            pad_line(f"   Time . . . . . . :  {entry.get('schedule_time', '')}"),
            pad_line(f"   Days . . . . . . :  {entry.get('schedule_days', '*ALL')}"),
            pad_line(""),
            pad_line(" Command:"),
            pad_line(f"   {entry['command'][:60]}"),
            pad_line(""),
            pad_line(f" Last run . . . . . :  {entry.get('last_run_time', '*NONE')}"),
            pad_line(f" Next run . . . . . :  {entry.get('next_run_time', '')}"),
            pad_line(""),
        ]

        while len(content) < 21:
            content.append(pad_line(""))

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "dspjobscde",
            "cols": 80,
            "content": content,
            "fields": [{"id": "cmd"}],
            "activeField": 0,
        }

    def _screen_addjobscde(self, session: Session) -> dict:
        """Add Job Schedule Entry screen."""
        hostname, date_str, time_str = get_system_info()

        content = [
            pad_line(f" {hostname:<20}       Add Job Schedule Entry (ADDJOBSCDE)      {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type choices, press Enter."),
            pad_line(""),
            [
                {"type": "text", "text": " Entry name . . . . . . :  "},
                {"type": "input", "id": "name", "width": 18, "class": "field-input"},
            ],
            [
                {"type": "text", "text": " Description  . . . . . :  "},
                {"type": "input", "id": "description", "width": 40, "class": "field-input"},
            ],
            pad_line(""),
            [
                {"type": "text", "text": " Command  . . . . . . . :  "},
                {"type": "input", "id": "command", "width": 40, "class": "field-input"},
            ],
            pad_line(""),
            pad_line(" Schedule:"),
            [
                {"type": "text", "text": "   Frequency  . . . . . :  "},
                {"type": "input", "id": "frequency", "width": 10, "class": "field-input", "value": "*DAILY"},
                {"type": "text", "text": "  *ONCE, *DAILY, *WEEKLY, *MONTHLY"},
            ],
            [
                {"type": "text", "text": "   Time (HH:MM) . . . . :  "},
                {"type": "input", "id": "time", "width": 5, "class": "field-input"},
            ],
            [
                {"type": "text", "text": "   Days . . . . . . . . :  "},
                {"type": "input", "id": "days", "width": 10, "class": "field-input", "value": "*ALL"},
                {"type": "text", "text": "  *ALL, *MON, *TUE, etc."},
            ],
            pad_line(""),
        ]

        while len(content) < 21:
            content.append(pad_line(""))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "addjobscde",
            "cols": 80,
            "content": content,
            "fields": [
                {"id": "name"},
                {"id": "description"},
                {"id": "command"},
                {"id": "frequency"},
                {"id": "time"},
                {"id": "days"},
                {"id": "cmd"},
            ],
            "activeField": 0,
        }

    def _submit_addjobscde(self, session: Session, fields: dict) -> dict:
        """Handle Add Job Schedule Entry submission."""
        name = fields.get('name', '').strip().upper()
        description = fields.get('description', '').strip()
        command = fields.get('command', '').strip()
        frequency = fields.get('frequency', '*DAILY').strip().upper() or '*DAILY'
        time_str = fields.get('time', '').strip()
        days = fields.get('days', '*ALL').strip().upper() or '*ALL'

        if not name:
            session.message = "Entry name is required"
            session.message_level = "error"
            return self.get_screen(session, 'addjobscde')

        if not command:
            session.message = "Command is required"
            session.message_level = "error"
            return self.get_screen(session, 'addjobscde')

        success, msg = add_job_schedule_entry(
            name=name,
            command=command,
            frequency=frequency,
            schedule_time=time_str if time_str else None,
            schedule_days=days,
            description=description,
            created_by=session.user
        )

        session.message = msg
        session.message_level = "info" if success else "error"

        if success:
            return self.get_screen(session, 'wrkjobscde')
        return self.get_screen(session, 'addjobscde')

    # ========================================
    # AUTHORIZATION LIST SCREENS
    # ========================================

    def _screen_wrkautl(self, session: Session) -> dict:
        """Work with Authorization Lists screen."""
        hostname, date_str, time_str = get_system_info()
        autls = list_authorization_lists()

        content = [
            pad_line(f" {hostname:<20}       Work with Authorization Lists            {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type options, press Enter."),
            pad_line("   2=Display   4=Delete   5=Work with entries"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  Auth List   Entries  Description"), "class": "field-reverse"}],
        ]

        fields = []
        for i, a in enumerate(autls):
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {a['name']:<10}  {a.get('entry_count', 0):>5}    {a['description'][:35]}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}"})

        while len(content) < 19:
            content.append(pad_line(""))

        content.append(pad_line("                                                                  Bottom"))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        fields.append({"id": "cmd"})
        content.append(pad_line(" F3=Exit  F5=Refresh  F6=Create  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "wrkautl",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_wrkautl(self, session: Session, fields: dict) -> dict:
        """Handle Work with Authorization Lists submission."""
        autls = list_authorization_lists()

        for i, a in enumerate(autls):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt:
                autl_name = a['name']
                if opt == '2':
                    session.field_values['selected_autl'] = autl_name
                    return self.get_screen(session, 'dspautl')
                elif opt == '4':
                    success, msg = delete_authorization_list(autl_name)
                    session.message = msg
                    session.message_level = "info" if success else "error"
                elif opt == '5':
                    session.field_values['selected_autl'] = autl_name
                    return self.get_screen(session, 'wrkautlent')

        return self.get_screen(session, 'wrkautl')

    def _screen_dspautl(self, session: Session) -> dict:
        """Display Authorization List screen."""
        hostname, date_str, time_str = get_system_info()
        name = session.field_values.get('selected_autl', '')
        entries = get_authorization_list_entries(name)
        objects = get_authorization_list_objects(name)

        content = [
            pad_line(f" {hostname:<20}       Display Authorization List               {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(f" Authorization list . :  {name}"),
            pad_line(""),
            pad_line(" User Entries:"),
            pad_line("   User        Authority"),
        ]

        for e in entries[:5]:
            content.append(pad_line(f"   {e['username']:<10}  {e['authority']}"))

        content.append(pad_line(""))
        content.append(pad_line(" Secured Objects:"))
        content.append(pad_line("   Object       Type       Library"))

        for o in objects[:5]:
            content.append(pad_line(f"   {o['object_name']:<10}  {o['object_type']:<10} {o['object_library']}"))

        while len(content) < 21:
            content.append(pad_line(""))

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "dspautl",
            "cols": 80,
            "content": content,
            "fields": [{"id": "cmd"}],
            "activeField": 0,
        }

    def _screen_crtautl(self, session: Session) -> dict:
        """Create Authorization List screen."""
        hostname, date_str, time_str = get_system_info()

        content = [
            pad_line(f" {hostname:<20}       Create Authorization List (CRTAUTL)     {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type choices, press Enter."),
            pad_line(""),
            [
                {"type": "text", "text": " Authorization list . . :  "},
                {"type": "input", "id": "name", "width": 10, "class": "field-input"},
            ],
            [
                {"type": "text", "text": " Description  . . . . . :  "},
                {"type": "input", "id": "description", "width": 40, "class": "field-input"},
            ],
            pad_line(""),
        ]

        while len(content) < 21:
            content.append(pad_line(""))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "crtautl",
            "cols": 80,
            "content": content,
            "fields": [
                {"id": "name"},
                {"id": "description"},
                {"id": "cmd"},
            ],
            "activeField": 0,
        }

    def _submit_crtautl(self, session: Session, fields: dict) -> dict:
        """Handle Create Authorization List submission."""
        name = fields.get('name', '').strip().upper()
        description = fields.get('description', '').strip()

        if not name:
            session.message = "Authorization list name is required"
            session.message_level = "error"
            return self.get_screen(session, 'crtautl')

        success, msg = create_authorization_list(name, description, created_by=session.user)

        session.message = msg
        session.message_level = "info" if success else "error"

        if success:
            return self.get_screen(session, 'wrkautl')
        return self.get_screen(session, 'crtautl')

    def _screen_wrkautlent(self, session: Session) -> dict:
        """Work with Authorization List Entries screen."""
        hostname, date_str, time_str = get_system_info()
        autl_name = session.field_values.get('selected_autl', '')
        entries = get_authorization_list_entries(autl_name)

        content = [
            pad_line(f" {hostname:<20}       Work with Auth List Entries              {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(f" Authorization list . :  {autl_name}"),
            pad_line(""),
            pad_line(" Type options, press Enter."),
            pad_line("   4=Remove   5=Change authority"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  User        Authority"), "class": "field-reverse"}],
        ]

        fields = []
        for i, e in enumerate(entries):
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {e['username']:<10}  {e['authority']}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}"})

        while len(content) < 19:
            content.append(pad_line(""))

        content.append(pad_line("                                                                  Bottom"))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        fields.append({"id": "cmd"})
        content.append(pad_line(" F3=Exit  F5=Refresh  F6=Add entry  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "wrkautlent",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_wrkautlent(self, session: Session, fields: dict) -> dict:
        """Handle Work with Authorization List Entries submission."""
        autl_name = session.field_values.get('selected_autl', '')
        entries = get_authorization_list_entries(autl_name)

        for i, e in enumerate(entries):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt:
                if opt == '4':
                    success, msg = remove_authorization_list_entry(autl_name, e['username'])
                    session.message = msg
                    session.message_level = "info" if success else "error"

        return self.get_screen(session, 'wrkautlent')

    # ========================================
    # SUBSYSTEM DESCRIPTION SCREENS
    # ========================================

    def _screen_wrksbsd(self, session: Session) -> dict:
        """Work with Subsystem Descriptions screen."""
        hostname, date_str, time_str = get_system_info()
        sbsds = list_subsystem_descriptions()

        content = [
            pad_line(f" {hostname:<20}       Work with Subsystem Descriptions         {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type options, press Enter."),
            pad_line("   2=Display   4=Delete   8=Start   9=End"),
            pad_line(""),
            [{"type": "text", "text": pad_line(" Opt  Subsystem   Status      Workers  Queue           Description"), "class": "field-reverse"}],
        ]

        fields = []
        for i, s in enumerate(sbsds):
            row = [
                {"type": "input", "id": f"opt_{i}", "width": 3, "class": "field-input"},
                {"type": "text", "text": f"  {s['name']:<10} {s['status']:<10} {s.get('worker_concurrency', 0):>5}    {(s.get('celery_queue') or ''):<15} {(s.get('description') or '')[:15]}"},
            ]
            content.append(row)
            fields.append({"id": f"opt_{i}"})

        while len(content) < 19:
            content.append(pad_line(""))

        content.append(pad_line("                                                                  Bottom"))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        fields.append({"id": "cmd"})
        content.append(pad_line(" F3=Exit  F5=Refresh  F6=Create  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "wrksbsd",
            "cols": 80,
            "content": content,
            "fields": fields,
            "activeField": 0,
        }

    def _submit_wrksbsd(self, session: Session, fields: dict) -> dict:
        """Handle Work with Subsystem Descriptions submission."""
        sbsds = list_subsystem_descriptions()

        for i, s in enumerate(sbsds):
            opt = fields.get(f'opt_{i}', '').strip()
            if opt:
                sbs_name = s['name']
                if opt == '2':
                    session.field_values['selected_sbsd'] = sbs_name
                    return self.get_screen(session, 'dspsbsd')
                elif opt == '4':
                    success, msg = delete_subsystem_description(sbs_name)
                    session.message = msg
                    session.message_level = "info" if success else "error"
                elif opt == '8':
                    success, msg = start_subsystem(sbs_name)
                    session.message = msg
                    session.message_level = "info" if success else "error"
                elif opt == '9':
                    success, msg = end_subsystem(sbs_name)
                    session.message = msg
                    session.message_level = "info" if success else "error"

        return self.get_screen(session, 'wrksbsd')

    def _screen_dspsbsd(self, session: Session) -> dict:
        """Display Subsystem Description screen."""
        hostname, date_str, time_str = get_system_info()
        name = session.field_values.get('selected_sbsd', '')
        sbsd = get_subsystem_description(name)

        if not sbsd:
            session.message = f"Subsystem {name} not found"
            session.message_level = "error"
            return self.get_screen(session, 'wrksbsd')

        job_queues = get_subsystem_job_queues(name)

        content = [
            pad_line(f" {hostname:<20}       Display Subsystem Description             {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(f" Subsystem  . . . . :  {sbsd['name']}"),
            pad_line(f" Description  . . . :  {sbsd['description']}"),
            pad_line(f" Status . . . . . . :  {sbsd['status']}"),
            pad_line(""),
            pad_line(" Celery settings:"),
            pad_line(f"   Queue  . . . . . :  {sbsd.get('celery_queue', '*DEFAULT')}"),
            pad_line(f"   Concurrency  . . :  {sbsd.get('worker_concurrency', 4)}"),
            pad_line(""),
            pad_line(" Job Queues:"),
        ]

        for jq in job_queues[:5]:
            content.append(pad_line(f"   {jq['job_queue']:<10}  Priority: {jq['sequence']}"))

        while len(content) < 21:
            content.append(pad_line(""))

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "dspsbsd",
            "cols": 80,
            "content": content,
            "fields": [{"id": "cmd"}],
            "activeField": 0,
        }

    def _screen_strsbs(self, session: Session) -> dict:
        """Start Subsystem screen."""
        hostname, date_str, time_str = get_system_info()

        content = [
            pad_line(f" {hostname:<20}       Start Subsystem (STRSBS)                  {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type choices, press Enter."),
            pad_line(""),
            [
                {"type": "text", "text": " Subsystem description  :  "},
                {"type": "input", "id": "name", "width": 10, "class": "field-input"},
            ],
            pad_line(""),
        ]

        while len(content) < 21:
            content.append(pad_line(""))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "strsbs",
            "cols": 80,
            "content": content,
            "fields": [{"id": "name"}, {"id": "cmd"}],
            "activeField": 0,
        }

    def _submit_strsbs(self, session: Session, fields: dict) -> dict:
        """Handle Start Subsystem submission."""
        name = fields.get('name', '').strip().upper()

        if not name:
            session.message = "Subsystem name is required"
            session.message_level = "error"
            return self.get_screen(session, 'strsbs')

        success, msg = start_subsystem(name)

        session.message = msg
        session.message_level = "info" if success else "error"

        return self.get_screen(session, 'wrksbsd')

    def _screen_endsbs(self, session: Session) -> dict:
        """End Subsystem screen."""
        hostname, date_str, time_str = get_system_info()

        content = [
            pad_line(f" {hostname:<20}       End Subsystem (ENDSBS)                    {session.user:>10}"),
            pad_line(f"                                                          {date_str}  {time_str}"),
            pad_line(""),
            pad_line(" Type choices, press Enter."),
            pad_line(""),
            [
                {"type": "text", "text": " Subsystem description  :  "},
                {"type": "input", "id": "name", "width": 10, "class": "field-input"},
            ],
            [
                {"type": "text", "text": " How to end . . . . . . :  "},
                {"type": "input", "id": "option", "width": 10, "class": "field-input", "value": "*CNTRLD"},
                {"type": "text", "text": "  *CNTRLD, *IMMED"},
            ],
            pad_line(""),
        ]

        while len(content) < 21:
            content.append(pad_line(""))

        msg = session.message if session.message else ""
        content.append(pad_line(f" {msg}"))
        session.message = ""

        content.append([
            {"type": "text", "text": " ===> "},
            {"type": "input", "id": "cmd", "width": 66},
        ])
        content.append(pad_line(" F3=Exit  F12=Cancel"))

        return {
            "type": "screen",
            "screen": "endsbs",
            "cols": 80,
            "content": content,
            "fields": [
                {"id": "name"},
                {"id": "option"},
                {"id": "cmd"},
            ],
            "activeField": 0,
        }

    def _submit_endsbs(self, session: Session, fields: dict) -> dict:
        """Handle End Subsystem submission."""
        name = fields.get('name', '').strip().upper()
        option = fields.get('option', '*CNTRLD').strip().upper() or '*CNTRLD'

        if not name:
            session.message = "Subsystem name is required"
            session.message_level = "error"
            return self.get_screen(session, 'endsbs')

        success, msg = end_subsystem(name, option)

        session.message = msg
        session.message_level = "info" if success else "error"

        return self.get_screen(session, 'wrksbsd')
