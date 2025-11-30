"""Alerting utilities for notifications."""

import json
import logging
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from odibi.config import AlertConfig, AlertEvent, AlertType

logger = logging.getLogger(__name__)


class AlertThrottler:
    """Prevent alert spam by throttling repeated alerts."""

    def __init__(self):
        self._last_alerts: Dict[str, datetime] = {}
        self._alert_counts: Dict[str, int] = {}

    def should_send(
        self,
        alert_key: str,
        throttle_minutes: int = 15,
        max_per_hour: int = 10,
    ) -> bool:
        """Check if alert should be sent based on throttling rules.

        Args:
            alert_key: Unique key for this alert type
            throttle_minutes: Minimum minutes between same alerts
            max_per_hour: Maximum alerts of same type per hour

        Returns:
            True if alert should be sent, False if throttled
        """
        now = datetime.now(timezone.utc)
        last = self._last_alerts.get(alert_key)

        if last and (now - last).total_seconds() < throttle_minutes * 60:
            logger.debug(f"Alert throttled: {alert_key} (within {throttle_minutes}m)")
            return False

        hour_key = f"{alert_key}:{now.strftime('%Y%m%d%H')}"
        count = self._alert_counts.get(hour_key, 0)
        if count >= max_per_hour:
            logger.debug(f"Alert rate-limited: {alert_key} ({count}/{max_per_hour} per hour)")
            return False

        self._last_alerts[alert_key] = now
        self._alert_counts[hour_key] = count + 1

        return True

    def reset(self) -> None:
        """Reset throttler state (useful for testing)."""
        self._last_alerts.clear()
        self._alert_counts.clear()


_throttler = AlertThrottler()


def get_throttler() -> AlertThrottler:
    """Get the global throttler instance."""
    return _throttler


def send_alert(
    config: AlertConfig,
    message: str,
    context: Dict[str, Any],
    throttle: bool = True,
) -> bool:
    """Send alert to configured channel with throttling support.

    Args:
        config: Alert configuration
        message: Alert message
        context: Context dictionary (pipeline name, status, event_type, etc.)
        throttle: Whether to apply throttling (default: True)

    Returns:
        True if alert was sent, False if throttled or failed
    """
    if throttle:
        pipeline = context.get("pipeline", "unknown")
        event = context.get("event_type", "unknown")
        throttle_key = f"{pipeline}:{event}"

        throttle_minutes = config.metadata.get("throttle_minutes", 15)
        max_per_hour = config.metadata.get("max_per_hour", 10)

        if not _throttler.should_send(throttle_key, throttle_minutes, max_per_hour):
            return False

    payload = _build_payload(config, message, context)

    try:
        headers = {"Content-Type": "application/json"}
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(config.url, data=data, headers=headers)

        with urllib.request.urlopen(req) as response:
            if response.status >= 400:
                logger.error(f"Alert failed: HTTP {response.status}")
                return False
        return True
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")
        return False


def _get_event_color(event_type: str, status: str) -> Dict[str, str]:
    """Get color scheme based on event type or status.

    Returns:
        Dict with 'hex' (for Slack), 'style' (for Teams Adaptive Card)
    """
    if event_type == AlertEvent.ON_QUARANTINE.value:
        return {"hex": "#FFA500", "style": "Warning"}
    elif event_type == AlertEvent.ON_GATE_BLOCK.value:
        return {"hex": "#FF0000", "style": "Attention"}
    elif event_type == AlertEvent.ON_THRESHOLD_BREACH.value:
        return {"hex": "#FF6600", "style": "Warning"}
    elif status == "SUCCESS":
        return {"hex": "#36a64f", "style": "Good"}
    elif status == "STARTED":
        return {"hex": "#0078D4", "style": "Accent"}
    else:
        return {"hex": "#FF0000", "style": "Attention"}


def _get_event_icon(event_type: str, status: str) -> str:
    """Get icon based on event type or status."""
    icons = {
        AlertEvent.ON_QUARANTINE.value: "🔶",
        AlertEvent.ON_GATE_BLOCK.value: "🚫",
        AlertEvent.ON_THRESHOLD_BREACH.value: "⚠️",
        "SUCCESS": "✅",
        "STARTED": "🚀",
    }
    return icons.get(event_type, icons.get(status, "❌"))


def _build_payload(
    config: AlertConfig,
    message: str,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """Build payload based on alert type and event."""
    pipeline = context.get("pipeline", "Unknown Pipeline")
    status = context.get("status", "UNKNOWN")
    duration = context.get("duration", 0.0)
    project_config = context.get("project_config")
    event_type = context.get("event_type", "")
    timestamp = context.get("timestamp", datetime.now(timezone.utc).isoformat())

    # Row count summary from story
    total_rows = context.get("total_rows_processed", 0)
    rows_dropped = context.get("rows_dropped", 0)
    final_rows = context.get("final_output_rows")

    project_name = "Odibi Project"
    owner = None

    if project_config:
        project_name = getattr(project_config, "project", project_name)
        owner = getattr(project_config, "owner", None)

    color = _get_event_color(event_type, status)
    icon = _get_event_icon(event_type, status)

    if config.type == AlertType.SLACK:
        return _build_slack_payload(
            pipeline=pipeline,
            project_name=project_name,
            status=status,
            duration=duration,
            message=message,
            owner=owner,
            event_type=event_type,
            timestamp=timestamp,
            context=context,
            config=config,
            color=color["hex"],
            icon=icon,
            total_rows=total_rows,
            rows_dropped=rows_dropped,
            final_rows=final_rows,
        )

    elif config.type == AlertType.TEAMS:
        return _build_teams_payload(
            pipeline=pipeline,
            project_name=project_name,
            status=status,
            duration=duration,
            message=message,
            owner=owner,
            event_type=event_type,
            timestamp=timestamp,
            context=context,
            config=config,
            style=color["style"],
            icon=icon,
            total_rows=total_rows,
            rows_dropped=rows_dropped,
            final_rows=final_rows,
        )

    else:
        return _build_generic_payload(
            pipeline=pipeline,
            status=status,
            duration=duration,
            message=message,
            timestamp=timestamp,
            context=context,
            config=config,
        )


def _build_slack_payload(
    pipeline: str,
    project_name: str,
    status: str,
    duration: float,
    message: str,
    owner: Optional[str],
    event_type: str,
    timestamp: str,
    context: Dict[str, Any],
    config: AlertConfig,
    color: str,
    icon: str,
    total_rows: int = 0,
    rows_dropped: int = 0,
    final_rows: Optional[int] = None,
) -> Dict[str, Any]:
    """Build Slack Block Kit payload with event-specific content."""
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{icon} ODIBI: {pipeline} - {status}"},
        }
    ]

    fields = [
        {"type": "mrkdwn", "text": f"*Project:*\n{project_name}"},
        {"type": "mrkdwn", "text": f"*Status:*\n{status}"},
        {"type": "mrkdwn", "text": f"*Duration:*\n{duration:.2f}s"},
    ]

    # Add row summary for success/failure events (not start)
    if total_rows > 0 or final_rows is not None:
        row_text = f"{final_rows:,}" if final_rows else f"{total_rows:,}"
        fields.append({"type": "mrkdwn", "text": f"*Rows Processed:*\n{row_text}"})
        if rows_dropped > 0:
            fields.append({"type": "mrkdwn", "text": f"*Rows Filtered:*\n{rows_dropped:,}"})

    if timestamp:
        fields.append({"type": "mrkdwn", "text": f"*Timestamp:*\n{timestamp}"})

    if owner:
        fields.append({"type": "mrkdwn", "text": f"*Owner:*\n{owner}"})

    if event_type == AlertEvent.ON_QUARANTINE.value:
        qd = context.get("quarantine_details", {})
        fields.extend(
            [
                {
                    "type": "mrkdwn",
                    "text": f"*Rows Quarantined:*\n{qd.get('rows_quarantined', 0):,}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Quarantine Table:*\n{qd.get('quarantine_path', 'N/A')}",
                },
            ]
        )
        failed_tests = qd.get("failed_tests", [])
        if failed_tests:
            fields.append(
                {
                    "type": "mrkdwn",
                    "text": f"*Failed Tests:*\n{', '.join(failed_tests[:5])}",
                }
            )

    elif event_type == AlertEvent.ON_GATE_BLOCK.value:
        gd = context.get("gate_details", {})
        fields.extend(
            [
                {"type": "mrkdwn", "text": f"*Pass Rate:*\n{gd.get('pass_rate', 0):.1%}"},
                {"type": "mrkdwn", "text": f"*Required:*\n{gd.get('required_rate', 0.95):.1%}"},
                {"type": "mrkdwn", "text": f"*Rows Failed:*\n{gd.get('failed_rows', 0):,}"},
            ]
        )
        failure_reasons = gd.get("failure_reasons", [])
        if failure_reasons:
            fields.append(
                {
                    "type": "mrkdwn",
                    "text": f"*Reasons:*\n{'; '.join(failure_reasons[:3])}",
                }
            )

    elif event_type == AlertEvent.ON_THRESHOLD_BREACH.value:
        td = context.get("threshold_details", {})
        fields.extend(
            [
                {"type": "mrkdwn", "text": f"*Threshold:*\n{td.get('threshold', 'N/A')}"},
                {"type": "mrkdwn", "text": f"*Actual Value:*\n{td.get('actual_value', 'N/A')}"},
                {"type": "mrkdwn", "text": f"*Metric:*\n{td.get('metric', 'N/A')}"},
            ]
        )

    blocks.append({"type": "section", "fields": fields})

    story_path = context.get("story_path")
    if story_path:
        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"📂 Story: `{story_path}`"}],
            }
        )

    payload = {"blocks": blocks}

    if color:
        payload["attachments"] = [{"color": color, "blocks": []}]

    payload.update(config.metadata)
    return payload


def _build_teams_payload(
    pipeline: str,
    project_name: str,
    status: str,
    duration: float,
    message: str,
    owner: Optional[str],
    event_type: str,
    timestamp: str,
    context: Dict[str, Any],
    config: AlertConfig,
    style: str,
    icon: str,
    total_rows: int = 0,
    rows_dropped: int = 0,
    final_rows: Optional[int] = None,
) -> Dict[str, Any]:
    """Build Teams Adaptive Card payload with event-specific content."""
    facts = [
        {"title": "⏱ Duration", "value": f"{duration:.2f}s"},
        {"title": "📅 Time", "value": timestamp},
    ]

    # Add row summary for success/failure events (not start)
    if total_rows > 0 or final_rows is not None:
        row_text = f"{final_rows:,}" if final_rows else f"{total_rows:,}"
        facts.append({"title": "📊 Rows Processed", "value": row_text})
        if rows_dropped > 0:
            facts.append({"title": "🔻 Rows Filtered", "value": f"{rows_dropped:,}"})

    if owner:
        facts.insert(0, {"title": "👤 Owner", "value": owner})

    if event_type == AlertEvent.ON_QUARANTINE.value:
        qd = context.get("quarantine_details", {})
        facts.extend(
            [
                {"title": "🔶 Rows Quarantined", "value": f"{qd.get('rows_quarantined', 0):,}"},
                {"title": "📍 Quarantine Table", "value": qd.get("quarantine_path", "N/A")},
            ]
        )
        failed_tests = qd.get("failed_tests", [])
        if failed_tests:
            facts.append(
                {
                    "title": "❌ Failed Tests",
                    "value": ", ".join(failed_tests[:5]),
                }
            )

    elif event_type == AlertEvent.ON_GATE_BLOCK.value:
        gd = context.get("gate_details", {})
        facts.extend(
            [
                {"title": "📊 Pass Rate", "value": f"{gd.get('pass_rate', 0):.1%}"},
                {"title": "🎯 Required", "value": f"{gd.get('required_rate', 0.95):.1%}"},
                {"title": "❌ Rows Failed", "value": f"{gd.get('failed_rows', 0):,}"},
            ]
        )

    elif event_type == AlertEvent.ON_THRESHOLD_BREACH.value:
        td = context.get("threshold_details", {})
        facts.extend(
            [
                {"title": "📏 Threshold", "value": str(td.get("threshold", "N/A"))},
                {"title": "📈 Actual Value", "value": str(td.get("actual_value", "N/A"))},
                {"title": "📊 Metric", "value": td.get("metric", "N/A")},
            ]
        )

    if config.metadata:
        for k, v in config.metadata.items():
            if k not in ["throttle_minutes", "max_per_hour"]:
                facts.append({"title": f"📎 {k}", "value": str(v)})

    body_items = [
        {
            "type": "Container",
            "style": style,
            "items": [
                {
                    "type": "TextBlock",
                    "text": f"{icon} Pipeline: {pipeline}",
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
    ]

    story_path = context.get("story_path")
    if story_path:
        body_items.append(
            {
                "type": "TextBlock",
                "text": f"📂 Story: {story_path}",
                "size": "Small",
                "isSubtle": True,
                "wrap": True,
            }
        )

    adaptive_card = {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.4",
        "body": body_items,
    }

    payload = {
        "type": "message",
        "attachments": [
            {"contentType": "application/vnd.microsoft.card.adaptive", "content": adaptive_card}
        ],
    }

    return payload


def _build_generic_payload(
    pipeline: str,
    status: str,
    duration: float,
    message: str,
    timestamp: str,
    context: Dict[str, Any],
    config: AlertConfig,
) -> Dict[str, Any]:
    """Build generic webhook payload."""
    payload = {
        "pipeline": pipeline,
        "status": status,
        "duration": duration,
        "message": message,
        "timestamp": timestamp,
        "event_type": context.get("event_type"),
        "metadata": config.metadata,
    }

    if context.get("event_type") == AlertEvent.ON_QUARANTINE.value:
        payload["quarantine_details"] = context.get("quarantine_details", {})
    elif context.get("event_type") == AlertEvent.ON_GATE_BLOCK.value:
        payload["gate_details"] = context.get("gate_details", {})
    elif context.get("event_type") == AlertEvent.ON_THRESHOLD_BREACH.value:
        payload["threshold_details"] = context.get("threshold_details", {})

    return payload


def send_quarantine_alert(
    config: AlertConfig,
    pipeline: str,
    node_name: str,
    rows_quarantined: int,
    quarantine_path: str,
    failed_tests: list,
    context: Optional[Dict[str, Any]] = None,
) -> bool:
    """Convenience function to send a quarantine alert.

    Args:
        config: Alert configuration
        pipeline: Pipeline name
        node_name: Node that quarantined rows
        rows_quarantined: Number of rows quarantined
        quarantine_path: Path/table where quarantined rows are stored
        failed_tests: List of test names that failed
        context: Optional additional context

    Returns:
        True if alert sent, False otherwise
    """
    ctx = context.copy() if context else {}
    ctx.update(
        {
            "pipeline": pipeline,
            "status": "QUARANTINE",
            "event_type": AlertEvent.ON_QUARANTINE.value,
            "quarantine_details": {
                "rows_quarantined": rows_quarantined,
                "quarantine_path": quarantine_path,
                "failed_tests": failed_tests,
                "node_name": node_name,
            },
        }
    )

    message = f"{rows_quarantined} rows quarantined in {node_name}"
    return send_alert(config, message, ctx)


def send_gate_block_alert(
    config: AlertConfig,
    pipeline: str,
    node_name: str,
    pass_rate: float,
    required_rate: float,
    failed_rows: int,
    total_rows: int,
    failure_reasons: list,
    context: Optional[Dict[str, Any]] = None,
) -> bool:
    """Convenience function to send a gate block alert.

    Args:
        config: Alert configuration
        pipeline: Pipeline name
        node_name: Node where gate failed
        pass_rate: Actual pass rate
        required_rate: Required pass rate
        failed_rows: Number of failed rows
        total_rows: Total rows processed
        failure_reasons: List of failure reasons
        context: Optional additional context

    Returns:
        True if alert sent, False otherwise
    """
    ctx = context.copy() if context else {}
    ctx.update(
        {
            "pipeline": pipeline,
            "status": "GATE_BLOCKED",
            "event_type": AlertEvent.ON_GATE_BLOCK.value,
            "gate_details": {
                "pass_rate": pass_rate,
                "required_rate": required_rate,
                "failed_rows": failed_rows,
                "total_rows": total_rows,
                "failure_reasons": failure_reasons,
                "node_name": node_name,
            },
        }
    )

    message = f"Quality gate failed in {node_name}: {pass_rate:.1%} < {required_rate:.1%}"
    return send_alert(config, message, ctx)


def send_threshold_breach_alert(
    config: AlertConfig,
    pipeline: str,
    node_name: str,
    metric: str,
    threshold: Any,
    actual_value: Any,
    context: Optional[Dict[str, Any]] = None,
) -> bool:
    """Convenience function to send a threshold breach alert.

    Args:
        config: Alert configuration
        pipeline: Pipeline name
        node_name: Node where threshold was breached
        metric: Name of the metric that breached
        threshold: Expected threshold value
        actual_value: Actual value that breached
        context: Optional additional context

    Returns:
        True if alert sent, False otherwise
    """
    ctx = context.copy() if context else {}
    ctx.update(
        {
            "pipeline": pipeline,
            "status": "THRESHOLD_BREACH",
            "event_type": AlertEvent.ON_THRESHOLD_BREACH.value,
            "threshold_details": {
                "metric": metric,
                "threshold": threshold,
                "actual_value": actual_value,
                "node_name": node_name,
            },
        }
    )

    message = f"Threshold breach in {node_name}: {metric} = {actual_value} (threshold: {threshold})"
    return send_alert(config, message, ctx)
