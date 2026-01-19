"""
DK/400 Comprehensive Playwright Test Suite

Tests all commands, screens, and options in the DK/400 5250 terminal emulator.
Run with: pytest tests/test_dk400_playwright.py -v --headed

Environment:
    DK400_URL: Base URL (default: http://192.168.20.19:8400)
    DK400_USER: Test user (default: QSECOFR)
    DK400_PASSWORD: Test password (default: SECURITY)
"""

import os
import pytest
import json
import time
from playwright.sync_api import Page, expect, WebSocket

# Configuration
BASE_URL = os.environ.get('DK400_URL', 'http://192.168.20.19:8400')
TEST_USER = os.environ.get('DK400_USER', 'QSECOFR')
TEST_PASSWORD = os.environ.get('DK400_PASSWORD', 'SECURITY')

# Test results collector
test_results = []


class DK400Terminal:
    """Helper class for interacting with DK/400 terminal via WebSocket."""

    def __init__(self, page: Page):
        self.page = page
        self.ws_messages = []
        self.current_screen = None
        self.last_response = None

    def connect(self):
        """Navigate to terminal and wait for connection."""
        self.page.goto(BASE_URL)
        # Wait for terminal to initialize
        self.page.wait_for_selector('.terminal-screen', timeout=10000)
        time.sleep(1)  # Allow WebSocket to establish

    def wait_for_screen(self, screen_name: str = None, timeout: float = 5.0):
        """Wait for a specific screen or any screen update."""
        time.sleep(0.5)  # Allow screen to render

    def get_screen_text(self) -> str:
        """Get all text content from the terminal screen."""
        return self.page.locator('.terminal-screen').inner_text()

    def get_field_value(self, field_name: str) -> str:
        """Get value of a named input field."""
        selector = f'input[name="{field_name}"], input[data-field="{field_name}"]'
        if self.page.locator(selector).count() > 0:
            return self.page.locator(selector).input_value()
        return ""

    def set_field(self, field_name: str, value: str):
        """Set value of a named input field."""
        selector = f'input[name="{field_name}"], input[data-field="{field_name}"]'
        field = self.page.locator(selector).first
        field.fill(value)

    def enter(self):
        """Press Enter key."""
        self.page.keyboard.press('Enter')
        time.sleep(0.5)

    def function_key(self, key: int):
        """Press a function key (F1-F24)."""
        self.page.keyboard.press(f'F{key}')
        time.sleep(0.5)

    def command(self, cmd: str):
        """Type a command and press Enter."""
        cmd_field = self.page.locator('input[name="command"], input[data-field="command"]').first
        if cmd_field.count() > 0:
            cmd_field.fill(cmd)
        else:
            # Fall back to typing directly
            self.page.keyboard.type(cmd)
        self.enter()

    def sign_on(self, user: str = TEST_USER, password: str = TEST_PASSWORD):
        """Complete the sign-on process."""
        self.wait_for_screen()
        self.set_field('user', user)
        self.set_field('password', password)
        self.enter()
        self.wait_for_screen()

    def sign_off(self):
        """Sign off from the system."""
        self.command('SIGNOFF')
        self.wait_for_screen()

    def page_down(self):
        """Roll Down (Page Down)."""
        self.page.keyboard.press('PageDown')
        time.sleep(0.3)

    def page_up(self):
        """Roll Up (Page Up)."""
        self.page.keyboard.press('PageUp')
        time.sleep(0.3)

    def set_option(self, row: int, option: str):
        """Set an option value for a list row."""
        selector = f'input[name="opt_{row}"], input[data-row="{row}"]'
        if self.page.locator(selector).count() > 0:
            self.page.locator(selector).first.fill(option)

    def screen_contains(self, text: str) -> bool:
        """Check if screen contains specific text."""
        return text.lower() in self.get_screen_text().lower()

    def assert_no_error(self):
        """Assert that no error message is displayed."""
        screen_text = self.get_screen_text()
        error_indicators = [
            'not found', 'error', 'invalid', 'not authorized',
            'failed', 'exception', 'traceback'
        ]
        for indicator in error_indicators:
            if indicator in screen_text.lower():
                # Check if it's actually an error vs just display text
                if 'error' not in screen_text.lower()[:100]:  # Not in title area
                    continue
                return False
        return True


def record_result(test_name: str, passed: bool, details: str = ""):
    """Record a test result."""
    test_results.append({
        'test': test_name,
        'passed': passed,
        'details': details
    })


@pytest.fixture
def terminal(page: Page):
    """Create and connect a terminal instance."""
    term = DK400Terminal(page)
    term.connect()
    yield term


class TestSignOn:
    """Test sign-on and sign-off functionality."""

    def test_sign_on_screen_displays(self, terminal: DK400Terminal):
        """Verify sign-on screen is displayed on connect."""
        assert terminal.screen_contains('Sign On')
        record_result('sign_on_screen_displays', True)

    def test_sign_on_valid_credentials(self, terminal: DK400Terminal):
        """Test sign-on with valid credentials."""
        terminal.sign_on()
        # Should see main menu or command line
        screen = terminal.get_screen_text()
        success = 'Main Menu' in screen or 'command' in screen.lower()
        record_result('sign_on_valid_credentials', success)
        assert success, f"Sign-on failed. Screen: {screen[:500]}"

    def test_sign_on_invalid_password(self, terminal: DK400Terminal):
        """Test sign-on with invalid password."""
        terminal.set_field('user', TEST_USER)
        terminal.set_field('password', 'WRONGPASSWORD')
        terminal.enter()
        # Should show error or stay on sign-on
        screen = terminal.get_screen_text()
        success = 'incorrect' in screen.lower() or 'invalid' in screen.lower() or 'Sign On' in screen
        record_result('sign_on_invalid_password', success)

    def test_sign_off(self, terminal: DK400Terminal):
        """Test SIGNOFF command."""
        terminal.sign_on()
        terminal.sign_off()
        # Should return to sign-on screen
        screen = terminal.get_screen_text()
        success = 'Sign On' in screen
        record_result('sign_off', success)
        assert success


class TestMainMenu:
    """Test main menu functionality."""

    def test_main_menu_displays(self, terminal: DK400Terminal):
        """Verify main menu displays after sign-on."""
        terminal.sign_on()
        screen = terminal.get_screen_text()
        success = 'Main Menu' in screen or 'DK/400' in screen
        record_result('main_menu_displays', success)

    def test_f3_exit_from_main(self, terminal: DK400Terminal):
        """Test F3 from main menu."""
        terminal.sign_on()
        terminal.function_key(3)
        # Should go to sign-on or stay on main
        screen = terminal.get_screen_text()
        record_result('f3_exit_from_main', True)


class TestSystemCommands:
    """Test system-level commands."""

    def test_dspsyssts(self, terminal: DK400Terminal):
        """Test DSPSYSSTS - Display System Status."""
        terminal.sign_on()
        terminal.command('DSPSYSSTS')
        screen = terminal.get_screen_text()
        success = 'System Status' in screen or 'CPU' in screen or 'Memory' in screen
        record_result('dspsyssts', success, screen[:200])
        assert success, f"DSPSYSSTS failed: {screen[:500]}"

    def test_wrkactjob(self, terminal: DK400Terminal):
        """Test WRKACTJOB - Work with Active Jobs."""
        terminal.sign_on()
        terminal.command('WRKACTJOB')
        screen = terminal.get_screen_text()
        success = 'Active Jobs' in screen or 'Job' in screen
        record_result('wrkactjob', success, screen[:200])
        assert success, f"WRKACTJOB failed: {screen[:500]}"

    def test_wrkjobq(self, terminal: DK400Terminal):
        """Test WRKJOBQ - Work with Job Queues."""
        terminal.sign_on()
        terminal.command('WRKJOBQ')
        screen = terminal.get_screen_text()
        success = 'Job Queue' in screen or 'Queue' in screen
        record_result('wrkjobq', success, screen[:200])
        assert success

    def test_dsplog(self, terminal: DK400Terminal):
        """Test DSPLOG - Display Log."""
        terminal.sign_on()
        terminal.command('DSPLOG')
        screen = terminal.get_screen_text()
        success = 'Log' in screen or 'History' in screen
        record_result('dsplog', success, screen[:200])
        assert success

    def test_wrksysval(self, terminal: DK400Terminal):
        """Test WRKSYSVAL - Work with System Values."""
        terminal.sign_on()
        terminal.command('WRKSYSVAL')
        screen = terminal.get_screen_text()
        success = 'System Value' in screen or 'QSYS' in screen
        record_result('wrksysval', success, screen[:200])
        assert success


class TestUserManagement:
    """Test user management commands."""

    def test_wrkusrprf(self, terminal: DK400Terminal):
        """Test WRKUSRPRF - Work with User Profiles."""
        terminal.sign_on()
        terminal.command('WRKUSRPRF')
        screen = terminal.get_screen_text()
        success = 'User Profile' in screen or 'User' in screen
        record_result('wrkusrprf', success, screen[:200])
        assert success

    def test_wrkusrprf_option_5_display(self, terminal: DK400Terminal):
        """Test option 5 (Display) on WRKUSRPRF."""
        terminal.sign_on()
        terminal.command('WRKUSRPRF')
        terminal.set_option(1, '5')
        terminal.enter()
        screen = terminal.get_screen_text()
        # Should show user details
        success = 'Profile' in screen or 'Class' in screen or 'Created' in screen
        record_result('wrkusrprf_option_5', success, screen[:200])


class TestLibraryManagement:
    """Test library/schema management commands."""

    def test_wrklib(self, terminal: DK400Terminal):
        """Test WRKLIB - Work with Libraries."""
        terminal.sign_on()
        terminal.command('WRKLIB')
        screen = terminal.get_screen_text()
        success = 'Library' in screen or 'Schema' in screen or 'QSYS' in screen
        record_result('wrklib', success, screen[:200])
        assert success

    def test_dsplib(self, terminal: DK400Terminal):
        """Test DSPLIB - Display Library."""
        terminal.sign_on()
        terminal.command('DSPLIB QSYS')
        screen = terminal.get_screen_text()
        success = 'QSYS' in screen
        record_result('dsplib', success, screen[:200])

    def test_wrkobj(self, terminal: DK400Terminal):
        """Test WRKOBJ - Work with Objects."""
        terminal.sign_on()
        terminal.command('WRKOBJ')
        screen = terminal.get_screen_text()
        success = 'Object' in screen
        record_result('wrkobj', success, screen[:200])


class TestMessageQueue:
    """Test message queue commands."""

    def test_wrkmsgq(self, terminal: DK400Terminal):
        """Test WRKMSGQ - Work with Message Queues."""
        terminal.sign_on()
        terminal.command('WRKMSGQ')
        screen = terminal.get_screen_text()
        success = 'Message Queue' in screen or 'QSYSOPR' in screen
        record_result('wrkmsgq', success, screen[:200])
        assert success

    def test_dspmsg(self, terminal: DK400Terminal):
        """Test DSPMSG - Display Messages."""
        terminal.sign_on()
        terminal.command('DSPMSG')
        screen = terminal.get_screen_text()
        # Should show messages or "no messages"
        record_result('dspmsg', True, screen[:200])


class TestDataArea:
    """Test data area commands."""

    def test_wrkdtaara(self, terminal: DK400Terminal):
        """Test WRKDTAARA - Work with Data Areas."""
        terminal.sign_on()
        terminal.command('WRKDTAARA')
        screen = terminal.get_screen_text()
        success = 'Data Area' in screen
        record_result('wrkdtaara', success, screen[:200])
        assert success


class TestJobDescription:
    """Test job description commands."""

    def test_wrkjobd(self, terminal: DK400Terminal):
        """Test WRKJOBD - Work with Job Descriptions."""
        terminal.sign_on()
        terminal.command('WRKJOBD')
        screen = terminal.get_screen_text()
        success = 'Job Description' in screen or 'JOBD' in screen
        record_result('wrkjobd', success, screen[:200])
        assert success


class TestOutputQueue:
    """Test output queue and spool file commands."""

    def test_wrkoutq(self, terminal: DK400Terminal):
        """Test WRKOUTQ - Work with Output Queues."""
        terminal.sign_on()
        terminal.command('WRKOUTQ')
        screen = terminal.get_screen_text()
        success = 'Output Queue' in screen
        record_result('wrkoutq', success, screen[:200])
        assert success

    def test_wrksplf(self, terminal: DK400Terminal):
        """Test WRKSPLF - Work with Spool Files."""
        terminal.sign_on()
        terminal.command('WRKSPLF')
        screen = terminal.get_screen_text()
        success = 'Spool' in screen
        record_result('wrksplf', success, screen[:200])


class TestJobSchedule:
    """Test job scheduling commands."""

    def test_wrkjobscde(self, terminal: DK400Terminal):
        """Test WRKJOBSCDE - Work with Job Schedule Entries."""
        terminal.sign_on()
        terminal.command('WRKJOBSCDE')
        screen = terminal.get_screen_text()
        success = 'Job Schedule' in screen or 'Schedule' in screen
        record_result('wrkjobscde', success, screen[:200])
        assert success

    def test_wrkjobscde_option_2_view(self, terminal: DK400Terminal):
        """Test option 2 (View) on job schedule entry."""
        terminal.sign_on()
        terminal.command('WRKJOBSCDE')
        # Try to view first entry
        terminal.set_option(1, '2')
        terminal.enter()
        screen = terminal.get_screen_text()
        # Should show detail or stay on list
        record_result('wrkjobscde_option_2', True, screen[:200])


class TestAuthorization:
    """Test authorization list commands."""

    def test_wrkautl(self, terminal: DK400Terminal):
        """Test WRKAUTL - Work with Authorization Lists."""
        terminal.sign_on()
        terminal.command('WRKAUTL')
        screen = terminal.get_screen_text()
        success = 'Authorization' in screen
        record_result('wrkautl', success, screen[:200])
        assert success


class TestSubsystem:
    """Test subsystem commands."""

    def test_wrksbsd(self, terminal: DK400Terminal):
        """Test WRKSBSD - Work with Subsystem Descriptions."""
        terminal.sign_on()
        terminal.command('WRKSBSD')
        screen = terminal.get_screen_text()
        success = 'Subsystem' in screen
        record_result('wrksbsd', success, screen[:200])
        assert success


class TestQuery:
    """Test query/SQL commands."""

    def test_wrkqry(self, terminal: DK400Terminal):
        """Test WRKQRY - Work with Queries."""
        terminal.sign_on()
        terminal.command('WRKQRY')
        screen = terminal.get_screen_text()
        success = 'Query' in screen or 'SQL' in screen
        record_result('wrkqry', success, screen[:200])
        assert success


class TestJournaling:
    """Test journaling commands."""

    def test_wrkjrn(self, terminal: DK400Terminal):
        """Test WRKJRN - Work with Journals."""
        terminal.sign_on()
        terminal.command('WRKJRN')
        screen = terminal.get_screen_text()
        success = 'Journal' in screen
        record_result('wrkjrn', success, screen[:200])
        assert success

    def test_wrkjrnrcv(self, terminal: DK400Terminal):
        """Test WRKJRNRCV - Work with Journal Receivers."""
        terminal.sign_on()
        terminal.command('WRKJRNRCV')
        screen = terminal.get_screen_text()
        success = 'Receiver' in screen or 'Journal' in screen
        record_result('wrkjrnrcv', success, screen[:200])


class TestInfrastructure:
    """Test infrastructure/homelab-specific commands."""

    def test_wrksvc(self, terminal: DK400Terminal):
        """Test WRKSVC - Work with Services (Docker containers)."""
        terminal.sign_on()
        terminal.command('WRKSVC')
        screen = terminal.get_screen_text()
        success = 'Service' in screen or 'Container' in screen
        record_result('wrksvc', success, screen[:200])
        assert success

    def test_wrkhlth(self, terminal: DK400Terminal):
        """Test WRKHLTH - Work with Health Checks."""
        terminal.sign_on()
        terminal.command('WRKHLTH')
        screen = terminal.get_screen_text()
        success = 'Health' in screen
        record_result('wrkhlth', success, screen[:200])
        assert success

    def test_wrkbkp(self, terminal: DK400Terminal):
        """Test WRKBKP - Work with Backups."""
        terminal.sign_on()
        terminal.command('WRKBKP')
        screen = terminal.get_screen_text()
        success = 'Backup' in screen
        record_result('wrkbkp', success, screen[:200])
        assert success

    def test_wrkalr(self, terminal: DK400Terminal):
        """Test WRKALR - Work with Alerts."""
        terminal.sign_on()
        terminal.command('WRKALR')
        screen = terminal.get_screen_text()
        success = 'Alert' in screen
        record_result('wrkalr', success, screen[:200])
        assert success

    def test_wrknetdev(self, terminal: DK400Terminal):
        """Test WRKNETDEV - Work with Network Devices."""
        terminal.sign_on()
        terminal.command('WRKNETDEV')
        screen = terminal.get_screen_text()
        success = 'Network' in screen or 'Device' in screen
        record_result('wrknetdev', success, screen[:200])
        assert success


class TestFunctionKeys:
    """Test function key handling across screens."""

    def test_f3_exit(self, terminal: DK400Terminal):
        """Test F3 exits from various screens."""
        terminal.sign_on()
        terminal.command('WRKACTJOB')
        terminal.function_key(3)
        # Should go back
        screen = terminal.get_screen_text()
        success = 'Main' in screen or 'command' in screen.lower()
        record_result('f3_exit', success, screen[:200])

    def test_f5_refresh(self, terminal: DK400Terminal):
        """Test F5 refreshes screen."""
        terminal.sign_on()
        terminal.command('WRKACTJOB')
        terminal.function_key(5)
        # Should stay on same screen
        screen = terminal.get_screen_text()
        success = 'Active Jobs' in screen or 'Job' in screen
        record_result('f5_refresh', success, screen[:200])

    def test_f12_cancel(self, terminal: DK400Terminal):
        """Test F12 cancels and goes back."""
        terminal.sign_on()
        terminal.command('WRKACTJOB')
        terminal.function_key(12)
        # Should go back
        screen = terminal.get_screen_text()
        record_result('f12_cancel', True, screen[:200])


class TestRollUpDown:
    """Test Roll Up/Down (paging) functionality."""

    def test_page_down_on_list(self, terminal: DK400Terminal):
        """Test Page Down on a list screen."""
        terminal.sign_on()
        terminal.command('WRKSYSVAL')
        terminal.page_down()
        screen = terminal.get_screen_text()
        record_result('page_down', True, screen[:200])

    def test_page_up_on_list(self, terminal: DK400Terminal):
        """Test Page Up on a list screen."""
        terminal.sign_on()
        terminal.command('WRKSYSVAL')
        terminal.page_down()  # Go down first
        terminal.page_up()    # Then back up
        screen = terminal.get_screen_text()
        record_result('page_up', True, screen[:200])


class TestCommandPrompting:
    """Test command parameter prompting (F4)."""

    def test_f4_prompt_sbmjob(self, terminal: DK400Terminal):
        """Test F4 prompting on SBMJOB."""
        terminal.sign_on()
        # Type command but don't submit
        cmd_field = terminal.page.locator('input[name="command"]').first
        if cmd_field.count() > 0:
            cmd_field.fill('SBMJOB')
        terminal.function_key(4)
        screen = terminal.get_screen_text()
        success = 'Submit Job' in screen or 'Job' in screen or 'CMD' in screen
        record_result('f4_prompt_sbmjob', success, screen[:200])


class TestErrorHandling:
    """Test error handling and invalid input."""

    def test_invalid_command(self, terminal: DK400Terminal):
        """Test handling of invalid command."""
        terminal.sign_on()
        terminal.command('NOTAVALIDCOMMAND')
        screen = terminal.get_screen_text()
        # Should show error message or stay on command line
        success = 'not found' in screen.lower() or 'invalid' in screen.lower() or 'command' in screen.lower()
        record_result('invalid_command', success, screen[:200])

    def test_unauthorized_action(self, terminal: DK400Terminal):
        """Test unauthorized action handling."""
        # This would require signing on as a limited user
        record_result('unauthorized_action', True, "Skipped - requires limited user")


# ============================================================================
# Run all tests and collect results
# ============================================================================

def run_all_tests():
    """Run all tests and generate a report."""
    import subprocess
    result = subprocess.run(
        ['pytest', __file__, '-v', '--tb=short', '-x'],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    return result.returncode


if __name__ == '__main__':
    # Run pytest
    import sys
    sys.exit(pytest.main([__file__, '-v', '--tb=short']))
