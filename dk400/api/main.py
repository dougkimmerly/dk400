"""API Main - FastAPI application.

This is a thin layer that allows external systems to call programs.
The API has no logic - it just calls programs.
"""

import asyncio
import importlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from dk400.config import settings

logger = logging.getLogger(__name__)

app = FastAPI(
    title="dk400",
    description="Call programs externally",
    version="1.0.0",
)


class ProgramRequest(BaseModel):
    """Request body for program calls."""
    kwargs: Optional[Dict[str, Any]] = None


def _import_program(program_name: str):
    """Import a program module, searching deployment programs first.

    Search order:
    1. programs.{name} — deployment-specific programs
    2. dk400.programs.{name} — built-in platform programs
    """
    for namespace in ["programs", "dk400.programs"]:
        try:
            return importlib.import_module(f"{namespace}.{program_name}")
        except ModuleNotFoundError:
            continue

    raise ModuleNotFoundError(f"Program not found: {program_name}")


# Health check (for container health)
@app.get("/health")
async def health():
    """Container health check."""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


# Generic program caller
@app.post("/pgm/{program_name}")
async def call_program(program_name: str, request: ProgramRequest = None):
    """
    Call a program by name.

    This is how external systems call dk400 programs.
    """
    kwargs = request.kwargs if request else {}

    logger.info(f"API call: {program_name}({kwargs})")

    try:
        # Import the program
        module = _import_program(program_name)
        run_func = getattr(module, "run", None)

        if not run_func:
            raise HTTPException(404, f"Program {program_name} has no run() function")

        # Call it
        if asyncio.iscoroutinefunction(run_func):
            result = await run_func(**kwargs)
        else:
            result = run_func(**kwargs)

        return {
            "program": program_name,
            "result": result,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except ModuleNotFoundError:
        raise HTTPException(404, f"Program not found: {program_name}")
    except Exception as e:
        logger.error(f"Program {program_name} failed: {e}")
        raise HTTPException(500, str(e))


# Startup logging
@app.on_event("startup")
async def startup():
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("dk400 API starting")


@app.on_event("shutdown")
async def shutdown():
    logger.info("dk400 API stopping")
