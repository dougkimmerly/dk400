"""Auto-remediation for infrastructure issues.

Hierarchy:
1. Try runbook (known fixes) first
2. Ask Claude for unknown issues
3. Escalate to human as last resort
"""

import logging
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import yaml

from ..config import config

logger = logging.getLogger(__name__)

# Runbook directories
RUNBOOK_DIR = Path(config.runbook_dir) / "vms"
DEVICE_RUNBOOK_DIR = Path(config.runbook_dir) / "devices"


@dataclass
class RemediationResult:
    """Result of a remediation attempt."""
    success: bool
    action_taken: str
    method: str  # 'runbook', 'claude', 'escalated', 'investigation'
    details: str
    timestamp: str = None

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()


class Runbook:
    """Load and execute runbook remediation steps."""

    def __init__(self, runbook_dir: Path = RUNBOOK_DIR):
        self.runbook_dir = runbook_dir
        self.runbooks: Dict[str, Dict] = {}
        self._load_runbooks()

    def _load_runbooks(self):
        """Load all runbook YAML files."""
        if not self.runbook_dir.exists():
            logger.warning(f"Runbook directory not found: {self.runbook_dir}")
            return

        for yaml_file in self.runbook_dir.glob("*.yaml"):
            try:
                with open(yaml_file) as f:
                    runbook = yaml.safe_load(f)
                    if runbook and "name" in runbook:
                        self.runbooks[runbook["name"]] = runbook
                        logger.debug(f"Loaded runbook: {runbook['name']}")
            except Exception as e:
                logger.error(f"Failed to load runbook {yaml_file}: {e}")

    def get_runbook(self, container_name: str, host: str, use_default: bool = True) -> Optional[Dict]:
        """Get runbook for a container."""
        # Try exact match
        key = f"{host}:{container_name}"
        if key in self.runbooks:
            return self.runbooks[key]

        # Try container name only
        if container_name in self.runbooks:
            return self.runbooks[container_name]

        # Try pattern matching
        for name, runbook in self.runbooks.items():
            patterns = runbook.get("patterns", [])
            if container_name in patterns or key in patterns:
                return runbook
            patterns_lower = [p.lower() for p in patterns]
            if container_name.lower() in patterns_lower:
                return runbook

        # Return default if no match
        if use_default and "_default" in self.runbooks:
            return self.runbooks["_default"]

        return None


async def attempt_remediation(
    container_name: str,
    error_message: Optional[str] = None,
    host: Optional[str] = None,
    issue_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Attempt to remediate a container issue.

    Args:
        container_name: Name of the container
        error_message: Error message or description
        host: Host where container runs (defaults to localhost)
        issue_type: Type of issue

    Returns:
        Dict with remediated (bool), message, method, action_taken
    """
    if not host:
        host = "localhost"

    logger.info(f"Attempting remediation for {container_name} on {host}")

    runbook = Runbook()
    rb = runbook.get_runbook(container_name, host)

    if rb:
        result = await _execute_runbook(rb, container_name, host)
        if result.success:
            return {
                "remediated": True,
                "message": result.details,
                "method": result.method,
                "action_taken": result.action_taken,
            }

    # Try Claude if runbook failed or doesn't exist
    if config.anthropic_api_key:
        result = await _ask_claude_for_fix(container_name, host, error_message, rb)
        if result.success:
            return {
                "remediated": True,
                "message": result.details,
                "method": result.method,
                "action_taken": result.action_taken,
            }

    # Escalate to human
    result = await _escalate_to_human(container_name, host, error_message)

    return {
        "remediated": False,
        "message": result.details,
        "method": result.method,
        "action_taken": result.action_taken,
    }


async def execute_runbook(
    runbook_name: str,
    target: str,
    host: str,
    user: str = "doug",
) -> Dict[str, Any]:
    """Execute a specific runbook by name.

    Args:
        runbook_name: Name of the runbook
        target: Target container/service
        host: Host where to execute
        user: SSH user

    Returns:
        Dict with success, message, and details
    """
    runbook = Runbook()
    rb = runbook.runbooks.get(runbook_name)

    if not rb:
        return {
            "success": False,
            "message": f"Runbook '{runbook_name}' not found",
            "details": None,
        }

    result = await _execute_runbook(rb, target, host, user)

    return {
        "success": result.success,
        "message": result.action_taken,
        "details": result.details,
    }


async def _execute_runbook(
    runbook: Dict,
    container_name: str,
    host: str,
    user: str = "doug",
) -> RemediationResult:
    """Execute runbook steps."""
    steps = runbook.get("steps", [])
    is_investigation = runbook.get("auto_fix") is False

    for i, step in enumerate(steps):
        step_name = step.get("name", f"Step {i+1}")
        command = step.get("command", "")

        # Variable substitution
        command = command.replace("${HOST}", host)
        command = command.replace("${USER}", user)
        command = command.replace("${CONTAINER}", container_name)

        logger.info(f"  Executing: {step_name}")

        try:
            if step.get("local", False):
                result = subprocess.run(
                    command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=step.get("timeout", 60),
                )
            else:
                ssh_cmd = f"ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no {user}@{host} '{command}'"
                result = subprocess.run(
                    ssh_cmd,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=step.get("timeout", 60),
                )

            if not is_investigation and result.returncode != 0 and not step.get("ignore_errors", False):
                return RemediationResult(
                    success=False,
                    action_taken=f"Runbook step failed: {step_name}",
                    method="runbook",
                    details=f"Error: {result.stderr[:200]}",
                )

        except subprocess.TimeoutExpired:
            if not is_investigation:
                return RemediationResult(
                    success=False,
                    action_taken=f"Runbook step timed out: {step_name}",
                    method="runbook",
                    details="Command timed out",
                )
        except Exception as e:
            if not is_investigation:
                return RemediationResult(
                    success=False,
                    action_taken=f"Runbook step error: {step_name}",
                    method="runbook",
                    details=str(e),
                )

    # Verify container is running (for fix runbooks)
    if not is_investigation:
        if await _verify_container_running(container_name, host, user):
            return RemediationResult(
                success=True,
                action_taken=f"Runbook '{runbook['name']}' executed successfully",
                method="runbook",
                details=f"Container {container_name} is now running",
            )
        else:
            return RemediationResult(
                success=False,
                action_taken="Runbook completed but container still not running",
                method="runbook",
                details="Verification failed",
            )

    return RemediationResult(
        success=True,
        action_taken=f"Investigation runbook '{runbook['name']}' completed",
        method="investigation",
        details="Diagnostic info collected",
    )


async def _ask_claude_for_fix(
    container_name: str,
    host: str,
    error: Optional[str],
    failed_runbook: Optional[Dict],
) -> RemediationResult:
    """Ask Claude to analyze and suggest a fix."""
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=config.anthropic_api_key)

        context = await _gather_context(container_name, host)

        prompt = f"""You are a homelab infrastructure expert. A container is not running.

Container: {container_name}
Host: {host}
Error: {error or 'Container not found'}

Context:
{context}

{"Previous runbook failed: " + str(failed_runbook) if failed_runbook else ""}

Analyze and provide a fix. Format:

ANALYSIS: <brief analysis>
CAN_FIX: <yes or no>
COMMANDS: <shell commands if yes>
REASON: <if no, why>
"""

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )

        response_text = response.content[0].text
        can_fix = "can_fix: yes" in response_text.lower()

        if can_fix:
            commands = _extract_commands(response_text)
            if commands:
                await _execute_commands(commands, host)
                if await _verify_container_running(container_name, host):
                    return RemediationResult(
                        success=True,
                        action_taken="Claude-suggested fix applied",
                        method="claude",
                        details=f"Commands executed: {'; '.join(commands[:3])}...",
                    )

        return RemediationResult(
            success=False,
            action_taken="Claude analyzed but couldn't fix",
            method="claude",
            details=response_text[:200],
        )

    except Exception as e:
        logger.error(f"Claude analysis failed: {e}")
        return RemediationResult(
            success=False,
            action_taken="Claude analysis failed",
            method="claude",
            details=str(e),
        )


async def _escalate_to_human(
    container_name: str,
    host: str,
    error: Optional[str],
) -> RemediationResult:
    """Send alert to human via Telegram."""
    from ..notifications import send_telegram

    message = f"""MANUAL INTERVENTION REQUIRED

Container: {container_name}
Host: {host}
Error: {error or 'Not running'}

Auto-remediation failed. Please investigate."""

    try:
        await send_telegram(
            message,
            message_type="escalation",
            target=container_name,
            host=host,
        )
        logger.info(f"Escalation sent for {container_name}")
    except Exception as e:
        logger.error(f"Failed to send escalation: {e}")

    return RemediationResult(
        success=False,
        action_taken="Escalated to human",
        method="escalated",
        details="Sent Telegram notification",
    )


async def _gather_context(container_name: str, host: str, user: str = "doug") -> str:
    """Gather diagnostic context from host."""
    context_parts = []
    commands = [
        (f"docker ps -a | grep -i {container_name[:10]}", "Container status"),
        (f"docker logs {container_name} --tail 10 2>&1", "Recent logs"),
        ("df -h | head -5", "Disk space"),
    ]

    for cmd, label in commands:
        try:
            ssh_cmd = f"ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no {user}@{host} '{cmd}'"
            result = subprocess.run(ssh_cmd, shell=True, capture_output=True, text=True, timeout=15)
            if result.stdout.strip():
                context_parts.append(f"=== {label} ===\n{result.stdout.strip()}")
        except Exception:
            pass

    return "\n\n".join(context_parts) if context_parts else "No context gathered"


def _extract_commands(response: str) -> List[str]:
    """Extract commands from Claude response."""
    commands = []
    in_commands = False

    for line in response.split("\n"):
        if "COMMANDS:" in line.upper():
            in_commands = True
            after = line.split(":", 1)[-1].strip()
            if after and not after.startswith("<"):
                commands.append(after)
            continue
        if in_commands:
            if line.strip().startswith(("REASON:", "ANALYSIS:", "CAN_FIX:")):
                break
            if line.strip() and not line.strip().startswith("#"):
                cmd = line.strip().lstrip("- ").lstrip("$ ")
                if cmd:
                    commands.append(cmd)

    return commands


async def _execute_commands(commands: List[str], host: str, user: str = "doug") -> bool:
    """Execute commands on host."""
    for cmd in commands:
        try:
            ssh_cmd = f"ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no {user}@{host} '{cmd}'"
            subprocess.run(ssh_cmd, shell=True, capture_output=True, text=True, timeout=120)
        except Exception as e:
            logger.error(f"Command failed: {e}")
            return False
    return True


async def _verify_container_running(container_name: str, host: str, user: str = "doug") -> bool:
    """Verify container is running."""
    try:
        cmd = f"docker ps --format '{{{{.Names}}}}' | grep -q '^{container_name}$'"
        ssh_cmd = f"ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no {user}@{host} \"{cmd}\""
        result = subprocess.run(ssh_cmd, shell=True, capture_output=True, timeout=10)
        return result.returncode == 0
    except Exception:
        return False
