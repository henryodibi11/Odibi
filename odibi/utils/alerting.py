"""Alerting utilities for notifications."""

import json
import urllib.request
from typing import Any, Dict
from odibi.config import AlertConfig, AlertType
from odibi.utils.logging import logger


def send_alert(config: AlertConfig, message: str, context: Dict[str, Any]) -> None:
    """Send alert to configured channel.

    Args:
        config: Alert configuration
        message: Alert message
        context: Context dictionary (pipeline name, status, etc.)
    """
    payload = _build_payload(config, message, context)

    try:
        headers = {"Content-Type": "application/json"}
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(config.url, data=data, headers=headers)

        with urllib.request.urlopen(req) as response:
            if response.status >= 400:
                logger.error(f"Alert failed: HTTP {response.status}")
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")


def _build_payload(config: AlertConfig, message: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """Build payload based on alert type."""
    pipeline = context.get("pipeline", "Unknown Pipeline")
    status = context.get("status", "UNKNOWN")
    duration = context.get("duration", 0.0)
    project_config = context.get("project_config")

    # Extract rich metadata if available
    project_name = "Odibi Project"
    owner = None
    # environment = "Production"  # Default, or from env var/config if available in future

    if project_config:
        project_name = getattr(project_config, "project", project_name)
        owner = getattr(project_config, "owner", None)
        # environment = getattr(project_config, "environment", environment) # Phase 3

    if config.type == AlertType.SLACK:
        # Slack Block Kit
        # color = "#36a64f" if status == "SUCCESS" else "#ff0000"
        icon = "✅" if status == "SUCCESS" else "❌"

        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": f"{icon} ODIBI: {pipeline} - {status}"},
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Project:*\n{project_name}"},
                        {"type": "mrkdwn", "text": f"*Status:*\n{status}"},
                        {"type": "mrkdwn", "text": f"*Duration:*\n{duration:.2f}s"},
                        {"type": "mrkdwn", "text": f"*Message:*\n{message}"},
                    ],
                },
            ]
        }

        if owner:
            payload["blocks"][1]["fields"].append({"type": "mrkdwn", "text": f"*Owner:*\n{owner}"})

        # Merge user metadata (e.g., channel override)
        payload.update(config.metadata)
        return payload

    elif config.type == AlertType.TEAMS:
        # Modern Adaptive Card (v1.4)

        # Determine style and icon based on status
        if status == "SUCCESS":
            style = "Good"  # Green
            status_icon = "✅"
        elif status == "STARTED":
            style = "Accent"  # Blue
            status_icon = "🚀"
        else:
            style = "Attention"  # Red
            status_icon = "❌"

        # Build Fact Set
        facts = [
            {"title": "⏱ Duration", "value": f"{duration:.2f}s"},
            {"title": "📅 Time", "value": context.get("timestamp", "")},
            {"title": "📝 Message", "value": message},
        ]

        if owner:
            facts.insert(0, {"title": "👤 Owner", "value": owner})

        # Add manual metadata to facts if present
        if config.metadata:
            for k, v in config.metadata.items():
                facts.append({"title": f"📎 {k}", "value": str(v)})

        adaptive_card = {
            "type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.4",
            "body": [
                {
                    "type": "Container",
                    "style": style,
                    "items": [
                        {
                            "type": "TextBlock",
                            "text": f"{status_icon} Pipeline: {pipeline}",
                            "weight": "Bolder",
                            "size": "Medium",
                            "color": "Light",
                        },
                        {
                            "type": "TextBlock",
                            "text": f"Project: {project_name} | Status: {status}",
                            "isSubtle": True,
                            "spacing": "None",
                            "color": "Light",
                            "size": "Small",
                        },
                    ],
                },
                {"type": "Container", "items": [{"type": "FactSet", "facts": facts}]},
            ],
        }

        # Wrapper required for Incoming Webhooks / Workflows
        payload = {
            "type": "message",
            "attachments": [
                {"contentType": "application/vnd.microsoft.card.adaptive", "content": adaptive_card}
            ],
        }

        return payload

    else:
        # Generic Webhook
        return {
            "pipeline": pipeline,
            "status": status,
            "duration": duration,
            "message": message,
            "timestamp": context.get("timestamp"),
            "metadata": config.metadata,
        }
