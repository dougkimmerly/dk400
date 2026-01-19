"""
DK/400 Web Terminal Server

FastAPI server with WebSocket support for the 5250 terminal emulator.
"""
import os
import json
import asyncio
import secrets
import logging
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path
from collections import defaultdict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

from src.dk400.web.screens import ScreenManager, Session
from src.dk400.web.job_scheduler import start_scheduler, stop_scheduler
from src.dk400.web.active_sessions import register_session, unregister_session, update_session_activity

logger = logging.getLogger(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Security configuration
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX_ATTEMPTS = 10  # max auth attempts per IP per window
SESSION_TIMEOUT_MINUTES = 30  # session expires after inactivity

# Get the static files directory
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="DK/400", description="AS/400 Job Queue System")

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.on_event("startup")
async def startup_event():
    """Start the job scheduler on application startup."""
    logger.info("Starting DK/400 job scheduler...")
    start_scheduler()
    logger.info("DK/400 job scheduler started")


@app.on_event("shutdown")
async def shutdown_event():
    """Stop the job scheduler on application shutdown."""
    logger.info("Stopping DK/400 job scheduler...")
    stop_scheduler()
    logger.info("DK/400 job scheduler stopped")


class RateLimiter:
    """Simple in-memory rate limiter for authentication attempts."""

    def __init__(self, window_seconds: int = 60, max_attempts: int = 10):
        self.window = window_seconds
        self.max_attempts = max_attempts
        self.attempts: dict[str, list[datetime]] = defaultdict(list)

    def is_allowed(self, client_ip: str) -> bool:
        """Check if client is allowed to attempt authentication."""
        now = datetime.now()
        cutoff = now - timedelta(seconds=self.window)

        # Clean old attempts
        self.attempts[client_ip] = [
            t for t in self.attempts[client_ip] if t > cutoff
        ]

        return len(self.attempts[client_ip]) < self.max_attempts

    def record_attempt(self, client_ip: str):
        """Record an authentication attempt."""
        self.attempts[client_ip].append(datetime.now())

    def get_remaining(self, client_ip: str) -> int:
        """Get remaining attempts for client."""
        now = datetime.now()
        cutoff = now - timedelta(seconds=self.window)
        recent = [t for t in self.attempts[client_ip] if t > cutoff]
        return max(0, self.max_attempts - len(recent))

    def cleanup(self):
        """Remove stale entries (call periodically)."""
        now = datetime.now()
        cutoff = now - timedelta(seconds=self.window * 2)
        stale_ips = [
            ip for ip, times in self.attempts.items()
            if all(t < cutoff for t in times)
        ]
        for ip in stale_ips:
            del self.attempts[ip]


rate_limiter = RateLimiter(RATE_LIMIT_WINDOW, RATE_LIMIT_MAX_ATTEMPTS)


@app.get("/")
async def index():
    """Serve the main terminal page."""
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok", "service": "dk400-web", "timestamp": datetime.now().isoformat()}


@app.get("/api/jobs")
async def list_jobs():
    """List all scheduled jobs."""
    from src.dk400.web.job_scheduler import list_scheduled_jobs
    return {"jobs": list_scheduled_jobs()}


@app.get("/api/ntp")
async def ntp_status():
    """Get NTP sync status."""
    from src.dk400.web.database import get_system_value
    return {
        "status": get_system_value('QNTPSTS', 'UNKNOWN'),
        "local_time": get_system_value('QNTPTIME', ''),
        "utc_time": get_system_value('QNTPUTC', ''),
        "offset": get_system_value('QNTPOFFS', ''),
        "server": get_system_value('QNTPSRV', ''),
        "last_sync": get_system_value('QNTPLAST', '')
    }


@app.post("/api/ntp/sync")
async def trigger_ntp_sync():
    """Manually trigger NTP sync."""
    from src.dk400.web.job_scheduler import run_job_now
    result = await run_job_now('QNTPSYNC')
    return result


class ConnectionManager:
    """Manages WebSocket connections with secure session handling."""

    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}
        self.sessions: dict[str, Session] = {}
        self.session_last_activity: dict[str, datetime] = {}
        self.session_client_ips: dict[str, str] = {}  # Track IP per session

    def generate_session_id(self) -> str:
        """Generate a cryptographically secure session ID."""
        return secrets.token_urlsafe(32)

    async def connect(self, websocket: WebSocket) -> tuple[str, Session]:
        """Accept connection and create secure session."""
        await websocket.accept()

        # Generate secure session ID
        session_id = self.generate_session_id()
        client_ip = websocket.client.host if websocket.client else "unknown"

        self.active_connections[session_id] = websocket
        self.sessions[session_id] = Session(session_id)
        self.session_last_activity[session_id] = datetime.now()
        self.session_client_ips[session_id] = client_ip

        # Register as interactive session (user will be updated after sign-on)
        register_session(session_id, 'QSYSOPR', client_ip)

        return session_id, self.sessions[session_id]

    def touch_session(self, session_id: str):
        """Update last activity time for session."""
        if session_id in self.sessions:
            self.session_last_activity[session_id] = datetime.now()

    def is_session_expired(self, session_id: str) -> bool:
        """Check if session has expired due to inactivity."""
        if session_id not in self.session_last_activity:
            return True
        last_activity = self.session_last_activity[session_id]
        return datetime.now() - last_activity > timedelta(minutes=SESSION_TIMEOUT_MINUTES)

    def get_client_ip(self, session_id: str) -> str:
        """Get the client IP for a session."""
        return self.session_client_ips.get(session_id, "unknown")

    def disconnect(self, session_id: str):
        """Clean up session on disconnect."""
        # Unregister from active sessions
        unregister_session(session_id)

        if session_id in self.active_connections:
            del self.active_connections[session_id]
        if session_id in self.sessions:
            del self.sessions[session_id]
        if session_id in self.session_last_activity:
            del self.session_last_activity[session_id]
        if session_id in self.session_client_ips:
            del self.session_client_ips[session_id]

    async def send_message(self, session_id: str, message: dict):
        if session_id in self.active_connections:
            await self.active_connections[session_id].send_json(message)

    def cleanup_expired_sessions(self):
        """Remove expired sessions (call periodically)."""
        expired = [
            sid for sid in self.sessions
            if self.is_session_expired(sid)
        ]
        for sid in expired:
            self.disconnect(sid)
        if expired:
            logger.info(f"Cleaned up {len(expired)} expired sessions")


manager = ConnectionManager()
screen_manager = ScreenManager()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for terminal communication."""
    client_ip = websocket.client.host if websocket.client else "unknown"

    # Create secure session
    session_id, session = await manager.connect(websocket)

    try:
        while True:
            # Check session expiry
            if manager.is_session_expired(session_id):
                await websocket.send_json({
                    "screen": "session_expired",
                    "message": "Session expired due to inactivity",
                    "rows": [{"type": "text", "text": "Session expired. Please refresh to sign on again."}]
                })
                break

            data = await websocket.receive_json()
            action = data.get("action")

            # Update session activity
            manager.touch_session(session_id)

            if action == "init":
                # Send the sign-on screen
                screen_data = screen_manager.get_screen(session, "signon")
                await websocket.send_json(screen_data)

            elif action == "submit":
                # Handle screen submission
                screen = data.get("screen")
                fields = data.get("fields", {})

                # Rate limit authentication attempts
                if screen == "signon":
                    if not rate_limiter.is_allowed(client_ip):
                        await websocket.send_json({
                            "screen": "signon",
                            "message": "Too many sign-on attempts. Please wait 60 seconds.",
                            "message_level": "error",
                            "rows": screen_manager.get_screen(session, "signon")["rows"]
                        })
                        continue
                    rate_limiter.record_attempt(client_ip)

                result = screen_manager.handle_submit(session, screen, fields)
                await websocket.send_json(result)

            elif action == "function_key":
                # Handle function key press
                key = data.get("key")
                screen = data.get("screen")
                fields = data.get("fields", {})
                result = screen_manager.handle_function_key(session, screen, key, fields)
                await websocket.send_json(result)

            elif action == "roll":
                # Handle Roll Up/Roll Down (page up/down)
                direction = data.get("direction")
                screen = data.get("screen")
                result = screen_manager.handle_roll(session, screen, direction)
                await websocket.send_json(result)

            elif action == "field_update":
                # Real-time field update (optional)
                field = data.get("field")
                value = data.get("value")
                session.field_values[field] = value

            elif action == "command":
                # Direct command execution
                command = data.get("command", "").strip().upper()
                result = screen_manager.execute_command(session, command)
                await websocket.send_json(result)

    except WebSocketDisconnect:
        manager.disconnect(session_id)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(session_id)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8400))
    uvicorn.run(app, host="0.0.0.0", port=port)
