"""Issue tracking functions.

These are the main entry points for issue management in fixer.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..database import get_pool
from .models import ActionType, Issue, IssueAction, IssueStatus

logger = logging.getLogger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def _generate_hash(
    source_type: str,
    source_name: str,
    host: Optional[str],
    title: str,
) -> str:
    """Generate unique hash for issue deduplication."""
    key = f"{source_type}:{source_name}:{host or ''}:{title}"
    return hashlib.sha256(key.encode()).hexdigest()[:32]


async def report_issue(
    issue_type: str,
    target: str,
    error_message: Optional[str] = None,
    severity: str = "warning",
    host: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    auto_remediate: bool = True,
) -> Dict[str, Any]:
    """Report an issue and optionally attempt remediation.

    Args:
        issue_type: Type of issue (e.g., container_down, service_unhealthy)
        target: Name of affected target (container, service, device)
        error_message: Error message or description
        severity: Issue severity (critical, error, warning, info)
        host: Host where issue occurred
        context: Additional context data
        auto_remediate: Whether to attempt auto-remediation

    Returns:
        Dict with issue_id, is_new, and remediation result if attempted
    """
    title = f"{issue_type} on {target}"
    issue_hash = _generate_hash(issue_type, target, host, title)
    now = datetime.now(timezone.utc)

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Check if issue exists
        existing = await conn.fetchrow(
            "SELECT id, occurrence_count, status FROM fixer.unified_issues WHERE issue_hash = $1",
            issue_hash
        )

        if existing:
            # Update existing issue
            await conn.execute(
                """
                UPDATE fixer.unified_issues SET
                    last_seen = $1,
                    occurrence_count = occurrence_count + 1,
                    message = COALESCE($2, message),
                    status = CASE
                        WHEN status IN ('resolved', 'ignored') THEN 'new'
                        ELSE status
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE issue_hash = $3
                """,
                now, error_message, issue_hash
            )

            issue_id = existing["id"]
            is_new = False
            logger.debug(f"Updated issue #{issue_id}: {title}")
        else:
            # Create new issue
            row = await conn.fetchrow(
                """
                INSERT INTO fixer.unified_issues (
                    issue_hash, source_type, source_name, host, severity,
                    title, message, first_seen, last_seen,
                    occurrence_count, status, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                RETURNING id
                """,
                issue_hash, issue_type, target, host, severity,
                title, error_message, now, now,
                1, IssueStatus.NEW.value, json.dumps(context, cls=DateTimeEncoder) if context else None
            )

            issue_id = row["id"]
            is_new = True
            logger.info(f"Created issue #{issue_id}: [{severity}] {title}")

        result = {
            "issue_id": issue_id,
            "is_new": is_new,
            "issue_type": issue_type,
            "target": target,
        }

        # Attempt remediation if requested
        if auto_remediate:
            from ..actions import attempt_remediation
            remediation_result = await attempt_remediation(
                container_name=target,
                error_message=error_message,
                host=host,
                issue_type=issue_type,
            )
            result.update(remediation_result)

        return result


async def resolve_issue(
    issue_type: str,
    target: str,
    host: Optional[str] = None,
) -> bool:
    """Resolve an issue by type and target.

    Args:
        issue_type: Type of issue
        target: Name of affected target
        host: Host where issue occurred (optional filter)

    Returns:
        True if an issue was found and resolved
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Find open issue matching criteria
        query = """
            SELECT id, status FROM fixer.unified_issues
            WHERE source_type = $1
              AND source_name = $2
              AND status NOT IN ('resolved', 'ignored')
        """
        params = [issue_type, target]

        if host:
            query += " AND host = $3"
            params.append(host)

        query += " ORDER BY last_seen DESC LIMIT 1"

        row = await conn.fetchrow(query, *params)

        if not row:
            return False

        issue_id = row["id"]
        old_status = row["status"]

        # Update to resolved
        await conn.execute(
            """
            UPDATE fixer.unified_issues SET
                status = 'resolved',
                resolved_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $1
            """,
            issue_id
        )

        # Log the resolution
        await conn.execute(
            """
            INSERT INTO fixer.issue_actions (
                issue_id, action_type, actor, summary
            ) VALUES ($1, $2, $3, $4)
            """,
            issue_id, ActionType.RESOLVED.value, "fixer",
            f"Issue auto-resolved - target recovered (was: {old_status})"
        )

        logger.info(f"Resolved issue #{issue_id}: {target} recovered")
        return True


async def get_issue(issue_id: int) -> Optional[Dict[str, Any]]:
    """Get an issue by ID.

    Args:
        issue_id: Issue ID

    Returns:
        Issue dict or None if not found
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM fixer.unified_issues WHERE id = $1",
            issue_id
        )

        if not row:
            return None

        return {
            "id": row["id"],
            "issue_hash": row["issue_hash"],
            "source_type": row["source_type"],
            "source_name": row["source_name"],
            "host": row["host"],
            "severity": row["severity"],
            "category": row.get("category"),
            "title": row["title"],
            "message": row["message"],
            "first_seen": row["first_seen"].isoformat() if row["first_seen"] else None,
            "last_seen": row["last_seen"].isoformat() if row["last_seen"] else None,
            "occurrence_count": row["occurrence_count"],
            "status": row["status"],
            "assigned_to": row.get("assigned_to"),
            "resolution": row.get("resolution"),
            "metadata": row.get("metadata"),
        }


async def get_open_issues(
    severity: Optional[str] = None,
    source_type: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Get open issues with optional filters.

    Args:
        severity: Filter by severity
        source_type: Filter by source type
        limit: Maximum results

    Returns:
        List of issue dicts
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        query = """
            SELECT * FROM fixer.unified_issues
            WHERE status NOT IN ('resolved', 'ignored')
        """
        params = []
        param_idx = 1

        if severity:
            query += f" AND severity = ${param_idx}"
            params.append(severity)
            param_idx += 1

        if source_type:
            query += f" AND source_type = ${param_idx}"
            params.append(source_type)
            param_idx += 1

        query += f" ORDER BY severity DESC, last_seen DESC LIMIT ${param_idx}"
        params.append(limit)

        rows = await conn.fetch(query, *params)

        return [
            {
                "id": row["id"],
                "source_type": row["source_type"],
                "source_name": row["source_name"],
                "host": row["host"],
                "severity": row["severity"],
                "title": row["title"],
                "status": row["status"],
                "occurrence_count": row["occurrence_count"],
                "last_seen": row["last_seen"].isoformat() if row["last_seen"] else None,
            }
            for row in rows
        ]


async def log_action(
    issue_id: int,
    action_type: str,
    actor: str,
    summary: Optional[str] = None,
    request_data: Optional[str] = None,
    response_data: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> int:
    """Log an action on an issue.

    Returns the action ID.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO fixer.issue_actions (
                issue_id, action_type, actor, summary,
                request_data, response_data, metadata
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            """,
            issue_id, action_type, actor, summary,
            request_data, response_data,
            json.dumps(metadata, cls=DateTimeEncoder) if metadata else None
        )
        return row["id"]
