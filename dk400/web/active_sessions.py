"""
DK/400 Active Sessions Registry

Tracks interactive terminal sessions for WRKACTJOB display.
"""
from datetime import datetime
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Global registry of active interactive sessions
# Key: session_id, Value: session info dict
_active_sessions: dict[str, dict] = {}


def register_session(session_id: str, user: str, client_ip: str = 'unknown',
                     job_name: str = None) -> None:
    """Register an interactive session when user signs on."""
    if not job_name:
        # Generate AS/400-style job name: QPADEV####
        job_num = str(len(_active_sessions) + 1).zfill(4)
        job_name = f"QPADEV{job_num}"

    _active_sessions[session_id] = {
        'session_id': session_id,
        'job_name': job_name,
        'user': user.upper(),
        'client_ip': client_ip,
        'type': 'INT',  # Interactive
        'status': 'ACTIVE',
        'function': 'CMD-ENTRY',
        'signed_on': datetime.now(),
        'last_activity': datetime.now(),
    }
    logger.info(f"Registered interactive session {job_name} for user {user}")


def unregister_session(session_id: str) -> None:
    """Unregister a session when user signs off or disconnects."""
    if session_id in _active_sessions:
        session = _active_sessions.pop(session_id)
        logger.info(f"Unregistered session {session['job_name']} for user {session['user']}")


def update_session_activity(session_id: str, function: str = None) -> None:
    """Update last activity time and optionally the current function."""
    if session_id in _active_sessions:
        _active_sessions[session_id]['last_activity'] = datetime.now()
        if function:
            _active_sessions[session_id]['function'] = function


def update_session_user(session_id: str, user: str) -> None:
    """Update the user for a session (after sign-on)."""
    if session_id in _active_sessions:
        _active_sessions[session_id]['user'] = user.upper()
        _active_sessions[session_id]['signed_on'] = datetime.now()


def get_active_sessions() -> list[dict]:
    """Get all active interactive sessions for WRKACTJOB."""
    sessions = []
    for sid, info in _active_sessions.items():
        # Calculate elapsed time
        elapsed = datetime.now() - info['signed_on']
        hours, remainder = divmod(int(elapsed.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        elapsed_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        sessions.append({
            'name': info['job_name'],
            'user': info['user'],
            'type': info['type'],
            'status': info['status'],
            'cpu': '0.0',  # Interactive jobs don't really have CPU stats
            'function': info['function'],
            'elapsed': elapsed_str,
            'session_id': sid,
        })
    return sessions


def get_session_count() -> int:
    """Get count of active sessions."""
    return len(_active_sessions)


def get_session_info(session_id: str) -> Optional[dict]:
    """Get info for a specific session."""
    return _active_sessions.get(session_id)
