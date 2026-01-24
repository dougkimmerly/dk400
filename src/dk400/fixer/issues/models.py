"""Data models for issue tracking."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


class IssueSeverity(str, Enum):
    """Issue severity levels."""
    CRITICAL = "critical"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class IssueStatus(str, Enum):
    """Issue lifecycle states."""
    NEW = "new"
    ACKNOWLEDGED = "acknowledged"
    INVESTIGATING = "investigating"
    IN_PROGRESS = "in_progress"
    FIX_APPLIED = "fix_applied"
    RESOLVED = "resolved"
    IGNORED = "ignored"


class ActionType(str, Enum):
    """Types of actions on issues."""
    CREATED = "created"
    UPDATED = "updated"
    STATUS_CHANGE = "status_change"
    CLAUDE_REQUEST = "claude_request"
    CLAUDE_RESPONSE = "claude_response"
    REMEDIATION_ATTEMPT = "remediation_attempt"
    REMEDIATION_SUCCESS = "remediation_success"
    REMEDIATION_FAILED = "remediation_failed"
    TELEGRAM_SENT = "telegram_sent"
    NOTE_ADDED = "note_added"
    ASSIGNED = "assigned"
    ESCALATED = "escalated"
    RESOLVED = "resolved"
    REOPENED = "reopened"


@dataclass
class Issue:
    """A tracked issue."""
    id: Optional[int] = None
    issue_hash: str = ""
    source_type: str = ""
    source_name: str = ""
    host: Optional[str] = None
    severity: str = "warning"
    category: Optional[str] = None
    title: str = ""
    message: Optional[str] = None
    first_seen: Optional[datetime] = None
    last_seen: Optional[datetime] = None
    occurrence_count: int = 1
    status: str = "new"
    assigned_to: Optional[str] = None
    resolution: Optional[str] = None
    fix_applied_at: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class IssueAction:
    """An action taken on an issue."""
    id: Optional[int] = None
    issue_id: int = 0
    action_type: str = ""
    actor: str = ""
    summary: Optional[str] = None
    request_data: Optional[str] = None
    response_data: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
