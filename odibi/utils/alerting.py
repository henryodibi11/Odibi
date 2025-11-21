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

    if config.type == AlertType.SLACK:
        # Slack Block Kit
        # color = "#36a64f" if status == "SUCCESS" else "#ff0000"
        icon = "✅" if status == "SUCCESS" else "❌"

        return {
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": f"{icon} ODIBI: {pipeline} - {status}"},
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Status:*\n{status}"},
                        {"type": "mrkdwn", "text": f"*Duration:*\n{duration:.2f}s"},
                        {"type": "mrkdwn", "text": f"*Message:*\n{message}"},
                    ],
                },
            ]
        }

    elif config.type == AlertType.TEAMS:
        # Microsoft Teams Card
        theme_color = "00FF00" if status == "SUCCESS" else "FF0000"

        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": theme_color,
            "summary": f"Pipeline {pipeline} {status}",
            "sections": [
                {
                    "activityTitle": f"ODIBI Pipeline: {pipeline}",
                    "activitySubtitle": f"Status: {status}",
                    "facts": [
                        {"name": "Duration", "value": f"{duration:.2f}s"},
                        {"name": "Message", "value": message},
                    ],
                    "markdown": True,
                }
            ],
        }

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
