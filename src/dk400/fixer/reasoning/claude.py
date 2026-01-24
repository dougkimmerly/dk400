"""Claude API integration for incident analysis and remediation suggestions."""

import json
import logging
from typing import Any, Dict, List, Optional

from ..config import config

logger = logging.getLogger(__name__)


def get_client():
    """Get Anthropic client if API key is configured."""
    if not config.anthropic_api_key:
        return None
    try:
        from anthropic import Anthropic
        return Anthropic(api_key=config.anthropic_api_key)
    except ImportError:
        logger.warning("anthropic package not installed")
        return None


async def analyze_incident(
    incident_type: str,
    source: str,
    details: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Analyze an incident using Claude and return recommendations.

    Args:
        incident_type: Type of incident (e.g., container_down, service_unhealthy)
        source: Source of the incident (e.g., container name, service name)
        details: Additional details about the incident

    Returns:
        Analysis result with root_cause, severity, actions, etc.
    """
    client = get_client()
    if not client:
        logger.warning("Claude API key not configured, skipping analysis")
        return None

    user_message = f"""Incident detected:

Type: {incident_type}
Source: {source}
Details:
```json
{json.dumps(details, indent=2)}
```

Analyze this incident and provide:
1. Likely root cause
2. Severity assessment (info/warning/critical)
3. Recommended actions (be specific)
4. Any dependencies to check

Respond in JSON format:
{{
  "root_cause": "string",
  "severity": "info|warning|critical",
  "summary": "one line summary",
  "actions": ["action1", "action2"],
  "dependencies_to_check": ["service1", "service2"]
}}
"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            temperature=0.3,
            system="You are a homelab infrastructure expert. Analyze incidents and provide actionable remediation steps.",
            messages=[
                {"role": "user", "content": user_message}
            ],
        )

        content = response.content[0].text

        # Try to extract JSON from response
        try:
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]

            result = json.loads(content.strip())
            return result

        except json.JSONDecodeError:
            return {
                "summary": content[:200],
                "raw_response": content,
                "severity": "warning",
            }

    except Exception as e:
        logger.error(f"Claude API error: {e}")
        return None


async def ask_claude(
    question: str,
    context: Optional[str] = None,
    system_prompt: Optional[str] = None,
) -> Optional[str]:
    """Ask Claude a question and get a text response.

    Args:
        question: The question to ask
        context: Optional context to provide
        system_prompt: Optional custom system prompt

    Returns:
        Claude's response as text, or None if failed
    """
    client = get_client()
    if not client:
        logger.warning("Claude API key not configured")
        return None

    user_message = question
    if context:
        user_message = f"""Context:
{context}

Question:
{question}
"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2048,
            temperature=0.3,
            system=system_prompt or "You are a homelab infrastructure expert. Provide clear, actionable advice.",
            messages=[
                {"role": "user", "content": user_message}
            ],
        )

        return response.content[0].text

    except Exception as e:
        logger.error(f"Claude API error: {e}")
        return None


async def suggest_fix(
    container_name: str,
    error: str,
    logs: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Ask Claude to suggest a fix for a container issue.

    Args:
        container_name: Name of the container
        error: Error message or description
        logs: Recent container logs
        context: Additional context (host, previous attempts, etc.)

    Returns:
        Dict with can_fix, commands, and reason
    """
    client = get_client()
    if not client:
        return None

    prompt = f"""You are a homelab infrastructure expert. A container is not running.

Container: {container_name}
Error: {error}
"""

    if logs:
        prompt += f"""
Recent logs:
{logs[:2000]}
"""

    if context:
        prompt += f"""
Context:
{json.dumps(context, indent=2)}
"""

    prompt += """
Analyze and provide a fix. Format:

ANALYSIS: <brief analysis>
CAN_FIX: <yes or no>
COMMANDS: <shell commands if yes>
REASON: <if no, why>
"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )

        response_text = response.content[0].text
        can_fix = "can_fix: yes" in response_text.lower()

        commands = []
        if can_fix:
            commands = _extract_commands(response_text)

        return {
            "can_fix": can_fix,
            "commands": commands,
            "analysis": response_text,
        }

    except Exception as e:
        logger.error(f"Claude API error: {e}")
        return None


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
