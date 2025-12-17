"""Event system for real-time streaming feedback from the improvement brain.

Provides an event-driven architecture for transparent LLM reasoning,
tool execution, and file changes - similar to how Amp shows its work.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable


class BrainEventType(str, Enum):
    """Types of events emitted by the improvement brain."""

    # LLM events
    THINKING_START = "thinking_start"
    THINKING_CHUNK = "thinking_chunk"
    THINKING_END = "thinking_end"
    LLM_RESPONSE = "llm_response"

    # Tool events
    TOOL_CALL_START = "tool_call_start"
    TOOL_CALL_END = "tool_call_end"

    # File events
    FILE_READ = "file_read"
    FILE_WRITE = "file_write"
    FILE_LIST = "file_list"

    # Iteration events
    ITERATION_START = "iteration_start"
    ITERATION_END = "iteration_end"

    # Improvement loop events
    IMPROVEMENT_START = "improvement_start"
    IMPROVEMENT_END = "improvement_end"
    ATTEMPT_START = "attempt_start"
    ATTEMPT_END = "attempt_end"

    # Gate events
    GATES_START = "gates_start"
    GATES_END = "gates_end"


@dataclass
class BrainEvent:
    """An event emitted by the improvement brain."""

    type: BrainEventType
    timestamp: datetime = field(default_factory=datetime.now)
    data: dict[str, Any] = field(default_factory=dict)

    @property
    def time_str(self) -> str:
        """Format timestamp for display."""
        return self.timestamp.strftime("%H:%M:%S.%f")[:-3]

    def format_activity(self) -> str:
        """Format as activity log entry."""
        emoji = self._get_emoji()
        message = self._get_message()
        return f"`{self.time_str}` {emoji} {message}"

    def _get_emoji(self) -> str:
        """Get emoji for event type."""
        emoji_map = {
            BrainEventType.THINKING_START: "🧠",
            BrainEventType.THINKING_CHUNK: "💭",
            BrainEventType.THINKING_END: "🧠",
            BrainEventType.LLM_RESPONSE: "💬",
            BrainEventType.TOOL_CALL_START: "🔧",
            BrainEventType.TOOL_CALL_END: "✅",
            BrainEventType.FILE_READ: "📖",
            BrainEventType.FILE_WRITE: "✏️",
            BrainEventType.FILE_LIST: "📂",
            BrainEventType.ITERATION_START: "🔄",
            BrainEventType.ITERATION_END: "✔️",
            BrainEventType.IMPROVEMENT_START: "🚀",
            BrainEventType.IMPROVEMENT_END: "🏁",
            BrainEventType.ATTEMPT_START: "🎯",
            BrainEventType.ATTEMPT_END: "📊",
            BrainEventType.GATES_START: "🧪",
            BrainEventType.GATES_END: "🏆",
        }
        return emoji_map.get(self.type, "📌")

    def _get_message(self) -> str:
        """Get message for event type."""
        data = self.data

        if self.type == BrainEventType.THINKING_START:
            return f"Thinking... (iteration {data.get('iteration', '?')})"

        if self.type == BrainEventType.THINKING_CHUNK:
            chunk = data.get("chunk", "")[:50]
            return f"...{chunk}..." if chunk else "..."

        if self.type == BrainEventType.THINKING_END:
            return "Finished thinking"

        if self.type == BrainEventType.LLM_RESPONSE:
            content = data.get("content", "")[:100]
            return f"Response: {content}..." if content else "No content"

        if self.type == BrainEventType.TOOL_CALL_START:
            name = data.get("tool_name", "unknown")
            args = data.get("args", {})
            path = args.get("path", "")
            return f"Calling `{name}`" + (f" on `{path}`" if path else "")

        if self.type == BrainEventType.TOOL_CALL_END:
            name = data.get("tool_name", "unknown")
            success = data.get("success", True)
            return f"`{name}` {'completed' if success else 'failed'}"

        if self.type == BrainEventType.FILE_READ:
            return f"Reading `{data.get('path', '?')}`"

        if self.type == BrainEventType.FILE_WRITE:
            path = data.get("path", "?")
            action = "Created" if data.get("is_new") else "Updated"
            return f"{action} `{path}`"

        if self.type == BrainEventType.FILE_LIST:
            return f"Listing `{data.get('path', '.')}`"

        if self.type == BrainEventType.ITERATION_START:
            return f"━━━ Iteration {data.get('iteration', '?')}/{data.get('max', '?')} ━━━"

        if self.type == BrainEventType.ITERATION_END:
            return f"Iteration {data.get('iteration', '?')} complete"

        if self.type == BrainEventType.IMPROVEMENT_START:
            return "Starting LLM improvement loop"

        if self.type == BrainEventType.IMPROVEMENT_END:
            attempts = data.get("attempts", 0)
            success = data.get("success", False)
            status = "✅ succeeded" if success else "❌ failed"
            return f"Improvement loop {status} after {attempts} attempt(s)"

        if self.type == BrainEventType.ATTEMPT_START:
            return f"Attempt {data.get('attempt', '?')}/{data.get('max', '?')}"

        if self.type == BrainEventType.ATTEMPT_END:
            modified = data.get("files_modified", False)
            return f"Attempt complete ({'files changed' if modified else 'no changes'})"

        if self.type == BrainEventType.GATES_START:
            return "Running gate checks..."

        if self.type == BrainEventType.GATES_END:
            passed = data.get("passed", False)
            return f"Gates {'passed ✅' if passed else 'failed ❌'}"

        return str(self.type.value)


# Type alias for event callback
EventCallback = Callable[[BrainEvent], None]


class EventEmitter:
    """Simple event emitter for broadcasting brain events."""

    def __init__(self):
        self._listeners: list[EventCallback] = []
        self._event_log: list[BrainEvent] = []
        self._max_log_size: int = 500

    def subscribe(self, callback: EventCallback) -> Callable[[], None]:
        """Subscribe to events.

        Args:
            callback: Function to call with each event.

        Returns:
            Unsubscribe function.
        """
        self._listeners.append(callback)

        def unsubscribe():
            if callback in self._listeners:
                self._listeners.remove(callback)

        return unsubscribe

    def emit(self, event: BrainEvent) -> None:
        """Emit an event to all listeners."""
        self._event_log.append(event)
        if len(self._event_log) > self._max_log_size:
            self._event_log = self._event_log[-self._max_log_size :]

        for listener in self._listeners:
            try:
                listener(event)
            except Exception:
                pass  # Don't let listener errors break the chain

    def emit_type(self, event_type: BrainEventType, **data: Any) -> None:
        """Convenience method to emit an event by type."""
        self.emit(BrainEvent(type=event_type, data=data))

    def get_recent_events(self, count: int = 50) -> list[BrainEvent]:
        """Get recent events from the log."""
        return self._event_log[-count:]

    def clear_log(self) -> None:
        """Clear the event log."""
        self._event_log = []

    def format_activity_log(self, count: int = 30) -> str:
        """Format recent events as activity log markdown."""
        events = self.get_recent_events(count)
        if not events:
            return "_Waiting for brain activity..._"

        lines = [e.format_activity() for e in events]
        return "\n".join(lines)


# Global event emitter for the brain
_brain_emitter = EventEmitter()


def get_brain_emitter() -> EventEmitter:
    """Get the global brain event emitter."""
    return _brain_emitter


def emit_brain_event(event_type: BrainEventType, **data: Any) -> None:
    """Emit a brain event globally."""
    _brain_emitter.emit_type(event_type, **data)


def subscribe_to_brain(callback: EventCallback) -> Callable[[], None]:
    """Subscribe to global brain events."""
    return _brain_emitter.subscribe(callback)
