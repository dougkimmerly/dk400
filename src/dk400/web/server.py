"""
DK/400 Web Terminal Server

FastAPI server with WebSocket support for the 5250 terminal emulator.
"""
import os
import json
import asyncio
from datetime import datetime
from typing import Optional
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from src.dk400.web.screens import ScreenManager, Session

# Get the static files directory
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="DK/400", description="AS/400 Job Queue System")

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index():
    """Serve the main terminal page."""
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok", "service": "dk400-web", "timestamp": datetime.now().isoformat()}


class ConnectionManager:
    """Manages WebSocket connections."""

    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}
        self.sessions: dict[str, Session] = {}

    async def connect(self, websocket: WebSocket, session_id: str):
        await websocket.accept()
        self.active_connections[session_id] = websocket

        # Create or retrieve session
        if session_id not in self.sessions:
            self.sessions[session_id] = Session(session_id)

        return self.sessions[session_id]

    def disconnect(self, session_id: str):
        if session_id in self.active_connections:
            del self.active_connections[session_id]
        # Keep session for reconnection

    async def send_message(self, session_id: str, message: dict):
        if session_id in self.active_connections:
            await self.active_connections[session_id].send_json(message)


manager = ConnectionManager()
screen_manager = ScreenManager()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for terminal communication."""
    # Generate session ID from client info
    client_host = websocket.client.host if websocket.client else "unknown"
    session_id = f"{client_host}_{id(websocket)}"

    session = await manager.connect(websocket, session_id)

    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")

            if action == "init":
                # Send the sign-on screen
                screen_data = screen_manager.get_screen(session, "signon")
                await websocket.send_json(screen_data)

            elif action == "submit":
                # Handle screen submission
                screen = data.get("screen")
                fields = data.get("fields", {})
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
        print(f"WebSocket error: {e}")
        manager.disconnect(session_id)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8400)
