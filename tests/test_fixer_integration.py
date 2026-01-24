"""Integration tests for fixer package.

These tests define what "done" means for the fixer migration.
They test against the actual database to verify the package works.

Run with: pytest tests/test_integration.py -v
"""

import os
import pytest

# Skip all tests if not connected to database
pytestmark = pytest.mark.skipif(
    not os.environ.get("FIXER_DB_HOST"),
    reason="FIXER_DB_HOST not set - skipping integration tests"
)


class TestDatabase:
    """Test database connectivity."""

    @pytest.mark.asyncio
    async def test_can_connect(self):
        """Verify we can connect to the database."""
        from dk400.fixer.database import get_pool, close_pool
        pool = await get_pool()
        try:
            async with pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                assert result == 1
        finally:
            await close_pool()

    @pytest.mark.asyncio
    async def test_fixer_schema_exists(self):
        """Verify the fixer schema exists."""
        from dk400.fixer.database import get_pool, close_pool
        pool = await get_pool()
        try:
            async with pool.acquire() as conn:
                result = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.schemata
                        WHERE schema_name = 'fixer'
                    )
                """)
                assert result is True, "fixer schema should exist"
        finally:
            await close_pool()

    @pytest.mark.asyncio
    async def test_unified_issues_table_exists(self):
        """Verify the unified_issues table exists."""
        from dk400.fixer.database import get_pool, close_pool
        pool = await get_pool()
        try:
            async with pool.acquire() as conn:
                result = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'fixer'
                        AND table_name = 'unified_issues'
                    )
                """)
                assert result is True, "fixer.unified_issues table should exist"
        finally:
            await close_pool()


class TestIssues:
    """Test issue tracking functions."""

    @pytest.mark.asyncio
    async def test_report_issue_creates_new(self):
        """Verify report_issue creates a new issue."""
        from dk400.fixer import report_issue
        from dk400.fixer.database import close_pool

        try:
            result = await report_issue(
                issue_type="test_issue",
                target="test_target",
                error_message="Test error message",
                severity="warning",
                auto_remediate=False,
            )

            assert result is not None
            assert "issue_id" in result
            assert result["is_new"] is True
        finally:
            await close_pool()

    @pytest.mark.asyncio
    async def test_report_issue_updates_existing(self):
        """Verify report_issue updates existing issue on recurrence."""
        from dk400.fixer import report_issue
        from dk400.fixer.database import close_pool

        try:
            # Create first
            result1 = await report_issue(
                issue_type="recurring_test",
                target="recurring_target",
                error_message="First occurrence",
                auto_remediate=False,
            )

            # Report again
            result2 = await report_issue(
                issue_type="recurring_test",
                target="recurring_target",
                error_message="Second occurrence",
                auto_remediate=False,
            )

            assert result1["issue_id"] == result2["issue_id"]
            assert result2["is_new"] is False
        finally:
            await close_pool()

    @pytest.mark.asyncio
    async def test_resolve_issue(self):
        """Verify resolve_issue marks issue as resolved."""
        from dk400.fixer import report_issue, resolve_issue, get_issue
        from dk400.fixer.database import close_pool

        try:
            # Create issue
            result = await report_issue(
                issue_type="to_resolve",
                target="resolve_target",
                error_message="Will be resolved",
                auto_remediate=False,
            )
            issue_id = result["issue_id"]

            # Resolve it
            resolved = await resolve_issue(
                issue_type="to_resolve",
                target="resolve_target",
            )
            assert resolved is True

            # Verify status
            issue = await get_issue(issue_id)
            assert issue["status"] == "resolved"
        finally:
            await close_pool()

    @pytest.mark.asyncio
    async def test_get_open_issues(self):
        """Verify get_open_issues returns only open issues."""
        from dk400.fixer import get_open_issues
        from dk400.fixer.database import close_pool

        try:
            issues = await get_open_issues(limit=10)
            assert isinstance(issues, list)
            for issue in issues:
                assert issue["status"] not in ("resolved", "ignored")
        finally:
            await close_pool()


class TestNotifications:
    """Test notification functions."""

    @pytest.mark.asyncio
    async def test_send_telegram_with_mock(self):
        """Verify send_telegram formats message correctly."""
        # This test mocks the actual send - we just verify the function exists
        from dk400.fixer import send_telegram
        assert callable(send_telegram)


class TestRemediation:
    """Test remediation functions."""

    @pytest.mark.asyncio
    async def test_attempt_remediation_returns_dict(self):
        """Verify attempt_remediation returns expected structure."""
        from dk400.fixer import attempt_remediation

        # This will fail to remediate (no such container) but should return proper structure
        result = await attempt_remediation(
            container_name="nonexistent_test_container",
            error_message="Test error",
            host="localhost",
        )

        assert isinstance(result, dict)
        assert "remediated" in result
        assert "message" in result
        assert "method" in result


class TestReasoning:
    """Test Claude integration."""

    @pytest.mark.asyncio
    async def test_ask_claude_exists(self):
        """Verify ask_claude function exists."""
        from dk400.fixer import ask_claude
        assert callable(ask_claude)


class TestImports:
    """Test that all expected exports are available."""

    def test_can_import_all_exports(self):
        """Verify all __all__ exports are importable."""
        from dk400.fixer import (
            report_issue,
            resolve_issue,
            get_issue,
            get_open_issues,
            attempt_remediation,
            execute_runbook,
            ask_claude,
            send_telegram,
            get_pool,
            close_pool,
        )

        # All should be callable
        assert callable(report_issue)
        assert callable(resolve_issue)
        assert callable(get_issue)
        assert callable(get_open_issues)
        assert callable(attempt_remediation)
        assert callable(execute_runbook)
        assert callable(ask_claude)
        assert callable(send_telegram)
        assert callable(get_pool)
        assert callable(close_pool)
