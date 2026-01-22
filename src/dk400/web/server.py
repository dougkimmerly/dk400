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

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException, Depends, Cookie
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import redis

from src.dk400.web.screens import ScreenManager, Session
from src.dk400.web.job_scheduler import start_scheduler, stop_scheduler
from src.dk400.web.active_sessions import register_session, unregister_session, update_session_activity
from src.dk400.web.database import get_latest_health_results, get_health_summary, get_last_health_run

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

# Redis client for reading health state (same DB as container_monitor task)
REDIS_HOST = os.environ.get('REDIS_HOST', 'dk400-redis')
health_redis = redis.Redis(host=REDIS_HOST, port=6379, db=1, decode_responses=True)

# Monitored containers (mirrors tasks/health.py CONTAINER_HOSTS)
MONITORED_SERVICES = {
    'docker-server': [
        'nginx-proxy-manager',
        'portainer',
        'homelab-dashboard',
        'homelab-brain',
        'dk400-web',
        'dk400-postgres',
        'dk400-qbatch',
        'dk400-beat',
    ],
    'synology': [
        'gluetun',
        'radarr',
        'sonarr',
        'prowlarr',
    ],
}

# Session cookie name for API authentication
SESSION_COOKIE_NAME = "dk400_session"


def get_authenticated_session(dk400_session: Optional[str] = Cookie(None)) -> str:
    """
    Dependency that validates the session cookie.
    Returns the username if authenticated, raises 401 if not.
    """
    if not dk400_session:
        raise HTTPException(status_code=401, detail="Not authenticated - please sign in via terminal")

    # Check if session exists and has an authenticated user
    if dk400_session not in manager.sessions:
        raise HTTPException(status_code=401, detail="Session expired or invalid")

    session = manager.sessions[dk400_session]
    if not session.user or session.user == "":
        raise HTTPException(status_code=401, detail="Not signed in")

    return session.user


@app.post("/api/auth/validate")
async def validate_session(request: Request):
    """
    Validate a WebSocket session and set a cookie for API access.
    Called by the terminal JavaScript after successful sign-on.
    """
    try:
        data = await request.json()
        session_id = data.get("session_id")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid request")

    if not session_id or session_id not in manager.sessions:
        raise HTTPException(status_code=401, detail="Invalid session")

    session = manager.sessions[session_id]
    if not session.user or session.user == "":
        raise HTTPException(status_code=401, detail="Not signed in")

    # Create response with session cookie
    response = JSONResponse({"status": "ok", "user": session.user})
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_id,
        httponly=True,
        secure=True,  # HTTPS only
        samesite="strict",
        max_age=SESSION_TIMEOUT_MINUTES * 60
    )
    return response


@app.post("/api/auth/logout")
async def logout_session():
    """Clear the session cookie."""
    response = JSONResponse({"status": "ok"})
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response


@app.get("/")
async def index():
    """Serve the main terminal page."""
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok", "service": "dk400-web", "timestamp": datetime.now().isoformat()}


@app.get("/api/jobs")
async def list_jobs(user: str = Depends(get_authenticated_session)):
    """List all scheduled jobs. Requires authentication."""
    from src.dk400.web.job_scheduler import list_scheduled_jobs
    return {"jobs": list_scheduled_jobs(), "user": user}


@app.get("/api/ntp")
async def ntp_status(user: str = Depends(get_authenticated_session)):
    """Get NTP sync status. Requires authentication."""
    from src.dk400.web.database import get_system_value
    return {
        "status": get_system_value('QNTPSTS', 'UNKNOWN'),
        "local_time": get_system_value('QNTPTIME', ''),
        "utc_time": get_system_value('QNTPUTC', ''),
        "offset": get_system_value('QNTPOFFS', ''),
        "server": get_system_value('QNTPSRV', ''),
        "last_sync": get_system_value('QNTPLAST', ''),
        "user": user
    }


@app.post("/api/ntp/sync")
async def trigger_ntp_sync(user: str = Depends(get_authenticated_session)):
    """Manually trigger NTP sync. Requires authentication."""
    from src.dk400.web.job_scheduler import run_job_now
    logger.info(f"NTP sync triggered by user {user}")
    result = await run_job_now('QNTPSYNC')
    return result


@app.get("/api/health/services")
async def health_services():
    """
    Get status of all monitored services.

    This endpoint is used by the dashboard to display service health.
    Returns status from the container_monitor task (runs every 2 minutes).

    Status values:
    - "ok": Container is running
    - "down": Container is down (being tracked)
    - "unknown": Unable to determine status
    """
    services = []

    try:
        for host, containers in MONITORED_SERVICES.items():
            # Check SSH connectivity for this host
            ssh_down = health_redis.get(f"container_down:{host}:ssh")
            ssh_timeout = health_redis.get(f"container_down:{host}:ssh_timeout")

            host_reachable = not (ssh_down or ssh_timeout)

            for container in containers:
                container_key = f"container_down:{host}:{container}"
                down_state = health_redis.get(container_key)

                if not host_reachable:
                    status = "unknown"
                    message = "Host unreachable"
                elif down_state:
                    status = "down"
                    try:
                        state_data = json.loads(down_state)
                        first_seen = state_data.get('first_seen', '')
                        message = f"Down since {first_seen}"
                    except json.JSONDecodeError:
                        message = "Down"
                else:
                    status = "ok"
                    message = None

                services.append({
                    "host": host,
                    "name": container,
                    "status": status,
                    "message": message,
                })
    except redis.ConnectionError as e:
        logger.error(f"Redis connection error: {e}")
        # Return unknown status for all services if Redis is down
        for host, containers in MONITORED_SERVICES.items():
            for container in containers:
                services.append({
                    "host": host,
                    "name": container,
                    "status": "unknown",
                    "message": "Health check unavailable",
                })

    # Summary counts
    ok_count = sum(1 for s in services if s["status"] == "ok")
    down_count = sum(1 for s in services if s["status"] == "down")
    unknown_count = sum(1 for s in services if s["status"] == "unknown")

    return {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total": len(services),
            "ok": ok_count,
            "down": down_count,
            "unknown": unknown_count,
        },
        "services": services,
    }


@app.get("/api/healthchecks/results")
async def healthcheck_results():
    """
    Get comprehensive health check results for all monitored services.

    This endpoint returns results from the full health check system that checks
    all 69 services configured in Dashboard (HTTP, Docker, ping checks).

    Returns:
    - checks: List of all service check results with status, response time, errors
    - summary: Count of services by status (up/down/unknown)
    - last_run: Timestamp of most recent health check run
    """
    checks = get_latest_health_results()
    summary = get_health_summary()
    last_run = get_last_health_run()

    return {
        "checks": checks,
        "summary": summary,
        "last_run": last_run.isoformat() if last_run else None,
    }


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
                # Send the sign-on screen with session_id for auth
                screen_data = screen_manager.get_screen(session, "signon")
                screen_data["session_id"] = session_id
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
                # Include session_id for auth validation after sign-on
                if session.user and session.user != "":
                    result["session_id"] = session_id
                    result["authenticated"] = True
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
