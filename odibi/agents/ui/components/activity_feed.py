"""Activity feed component for real-time agent progress.

Shows live updates of what the agent is doing, similar to Amp's visible reasoning.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

import gradio as gr

from ..constants import get_tool_emoji


@dataclass
class ActivityItem:
    """A single activity in the feed."""

    id: str
    message: str
    timestamp: datetime = field(default_factory=datetime.now)
    status: str = "active"  # active, completed, error
    tool_name: Optional[str] = None
    details: Optional[str] = None

    @property
    def emoji(self) -> str:
        if self.tool_name:
            return get_tool_emoji(self.tool_name)
        if self.status == "completed":
            return "✅"
        if self.status == "error":
            return "❌"
        return "⏳"

    @property
    def time_str(self) -> str:
        return self.timestamp.strftime("%H:%M:%S")


class ActivityFeed:
    """Manages the activity feed state."""

    def __init__(self, max_items: int = 50):
        self.items: list[ActivityItem] = []
        self.max_items = max_items
        self._counter = 0

    def add(
        self,
        message: str,
        tool_name: Optional[str] = None,
        details: Optional[str] = None,
        status: str = "active",
    ) -> str:
        """Add an activity item.

        Returns:
            The ID of the new item.
        """
        self._counter += 1
        item_id = f"activity_{self._counter}"

        item = ActivityItem(
            id=item_id,
            message=message,
            tool_name=tool_name,
            details=details,
            status=status,
        )

        self.items.append(item)

        if len(self.items) > self.max_items:
            self.items = self.items[-self.max_items :]

        return item_id

    def update(self, item_id: str, status: str = "completed", message: Optional[str] = None):
        """Update an activity item's status."""
        for item in self.items:
            if item.id == item_id:
                item.status = status
                if message:
                    item.message = message
                break

    def clear(self):
        """Clear all activity items."""
        self.items = []

    def format_html(self) -> str:
        """Format the feed as HTML."""
        if not self.items:
            return '<div class="activity-feed"><em>No activity yet</em></div>'

        lines = ['<div class="activity-feed">']

        for item in self.items[-20:]:
            status_class = item.status
            lines.append(
                f'<div class="activity-item {status_class}">'
                f'<span class="timestamp">{item.time_str}</span> '
                f"{item.emoji} {item.message}"
                f"</div>"
            )

        lines.append("</div>")
        return "\n".join(lines)

    def format_markdown(self) -> str:
        """Format the feed as Markdown."""
        if not self.items:
            return "_No activity yet_"

        lines = []
        for item in self.items[-15:]:
            status_icon = {
                "active": "🔄",
                "completed": "✅",
                "error": "❌",
            }.get(item.status, "⏳")

            lines.append(f"`{item.time_str}` {status_icon} {item.emoji} {item.message}")

        return "  \n".join(lines)


_activity_feed = ActivityFeed()


def get_activity_feed() -> ActivityFeed:
    """Get the global activity feed instance."""
    return _activity_feed


def add_activity(
    message: str,
    tool_name: Optional[str] = None,
    details: Optional[str] = None,
) -> str:
    """Add an activity to the global feed."""
    return _activity_feed.add(message, tool_name, details)


def complete_activity(item_id: str, message: Optional[str] = None):
    """Mark an activity as completed."""
    _activity_feed.update(item_id, "completed", message)


def error_activity(item_id: str, message: Optional[str] = None):
    """Mark an activity as error."""
    _activity_feed.update(item_id, "error", message)


def clear_activity():
    """Clear all activities."""
    _activity_feed.clear()


def create_activity_panel() -> tuple[gr.Column, dict[str, Any]]:
    """Create the activity feed panel.

    Returns:
        Tuple of (Gradio Column, dict of component references).
    """
    components: dict[str, Any] = {}

    with gr.Column() as activity_column:
        with gr.Accordion("📊 Activity Feed", open=True):
            components["activity_display"] = gr.Markdown(
                value="_No activity yet_",
                elem_classes=["activity-feed"],
            )

            with gr.Row():
                components["clear_activity_btn"] = gr.Button(
                    "🗑️ Clear",
                    size="sm",
                    scale=1,
                )

    return activity_column, components


def setup_activity_handlers(components: dict[str, Any]):
    """Set up event handlers for the activity panel."""

    def on_clear():
        clear_activity()
        return "_No activity yet_"

    components["clear_activity_btn"].click(
        fn=on_clear,
        outputs=[components["activity_display"]],
    )


def refresh_activity_display() -> str:
    """Get the current activity feed display."""
    return _activity_feed.format_markdown()
