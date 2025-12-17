"""Real-time streaming infrastructure for Odibi UI.

Provides low-latency event emission and consumption for cycle execution,
tool calls, and LLM responses. Designed for sub-second UI updates.

Key improvements over previous architecture:
- Non-blocking event emission
- Thread-safe queue with configurable timeout
- Rich event types for tool visibility
- Collapsible output support
"""

import queue
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class StreamEventType(str, Enum):
    """Types of streaming events."""

    # Lifecycle events
    CYCLE_START = "cycle_start"
    CYCLE_END = "cycle_end"
    STEP_START = "step_start"
    STEP_END = "step_end"
    STEP_SKIP = "step_skip"

    # Agent events
    AGENT_THINKING = "agent_thinking"
    AGENT_RESPONSE = "agent_response"
    AGENT_PROGRESS = "agent_progress"
    AGENT_TOKEN = "agent_token"  # Streaming LLM token

    # Tool/execution events
    TOOL_START = "tool_start"
    TOOL_PROGRESS = "tool_progress"
    TOOL_OUTPUT = "tool_output"
    TOOL_END = "tool_end"

    # Subprocess events
    SUBPROCESS_START = "subprocess_start"
    SUBPROCESS_STDOUT = "subprocess_stdout"
    SUBPROCESS_STDERR = "subprocess_stderr"
    SUBPROCESS_END = "subprocess_end"

    # UI-specific events
    ACTIVITY_UPDATE = "activity_update"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class StreamEvent:
    """A single streaming event with rich metadata."""

    event_type: StreamEventType
    message: str
    timestamp: datetime = field(default_factory=datetime.now)
    detail: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    is_error: bool = False
    collapsible: bool = False  # If True, UI should render as collapsible
    tool_name: Optional[str] = None
    agent_role: Optional[str] = None
    step_name: Optional[str] = None
    cycle_id: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "event_type": self.event_type.value,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "detail": self.detail,
            "metadata": self.metadata,
            "is_error": self.is_error,
            "collapsible": self.collapsible,
            "tool_name": self.tool_name,
            "agent_role": self.agent_role,
            "step_name": self.step_name,
            "cycle_id": self.cycle_id,
        }


StreamEventCallback = Callable[[StreamEvent], None]


class StreamingEventEmitter:
    """Thread-safe event emitter for real-time UI updates.

    Supports multiple subscribers and non-blocking emission.
    Queue-based design allows UI polling with configurable timeout.
    """

    def __init__(self, max_queue_size: int = 1000):
        self._subscribers: list[StreamEventCallback] = []
        self._queues: list[queue.Queue] = []
        self._lock = threading.Lock()
        self._max_queue_size = max_queue_size
        self._event_history: list[StreamEvent] = []
        self._max_history = 200

    def subscribe(self, callback: StreamEventCallback) -> None:
        """Subscribe to events with a callback."""
        with self._lock:
            self._subscribers.append(callback)

    def unsubscribe(self, callback: StreamEventCallback) -> None:
        """Unsubscribe from events."""
        with self._lock:
            if callback in self._subscribers:
                self._subscribers.remove(callback)

    def create_queue(self) -> queue.Queue:
        """Create a new event queue for polling.

        Returns:
            A queue that will receive all emitted events.
        """
        q = queue.Queue(maxsize=self._max_queue_size)
        with self._lock:
            self._queues.append(q)
        return q

    def remove_queue(self, q: queue.Queue) -> None:
        """Remove a queue from the subscriber list."""
        with self._lock:
            if q in self._queues:
                self._queues.remove(q)

    def emit(self, event: StreamEvent) -> None:
        """Emit an event to all subscribers (non-blocking).

        Events are pushed to all queues and callbacks are invoked.
        """
        with self._lock:
            # Store in history
            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history = self._event_history[-self._max_history :]

            # Push to queues (non-blocking)
            for q in self._queues:
                try:
                    q.put_nowait(event)
                except queue.Full:
                    # Drop oldest event if queue is full
                    try:
                        q.get_nowait()
                        q.put_nowait(event)
                    except queue.Empty:
                        pass

            # Invoke callbacks
            subscribers = list(self._subscribers)

        for callback in subscribers:
            try:
                callback(event)
            except Exception:
                pass  # Don't let bad callbacks break emission

    def emit_simple(
        self,
        event_type: StreamEventType,
        message: str,
        detail: str = "",
        **kwargs,
    ) -> None:
        """Convenience method to emit an event with minimal args."""
        self.emit(
            StreamEvent(
                event_type=event_type,
                message=message,
                detail=detail,
                **kwargs,
            )
        )

    def emit_tool_start(
        self,
        tool_name: str,
        args_summary: str = "",
        cycle_id: Optional[str] = None,
    ) -> None:
        """Emit a tool start event."""
        self.emit(
            StreamEvent(
                event_type=StreamEventType.TOOL_START,
                message=f"Running {tool_name}...",
                detail=args_summary,
                tool_name=tool_name,
                cycle_id=cycle_id,
            )
        )

    def emit_tool_progress(
        self,
        tool_name: str,
        message: str,
        detail: str = "",
    ) -> None:
        """Emit a tool progress event."""
        self.emit(
            StreamEvent(
                event_type=StreamEventType.TOOL_PROGRESS,
                message=message,
                detail=detail,
                tool_name=tool_name,
            )
        )

    def emit_tool_output(
        self,
        tool_name: str,
        output: str,
        is_stderr: bool = False,
        collapsible: bool = True,
    ) -> None:
        """Emit a tool output event (stdout/stderr)."""
        self.emit(
            StreamEvent(
                event_type=StreamEventType.TOOL_OUTPUT,
                message=f"{tool_name} output" + (" (stderr)" if is_stderr else ""),
                detail=output,
                tool_name=tool_name,
                collapsible=collapsible,
                is_error=is_stderr,
            )
        )

    def emit_tool_end(
        self,
        tool_name: str,
        success: bool,
        summary: str = "",
        exit_code: Optional[int] = None,
    ) -> None:
        """Emit a tool end event."""
        self.emit(
            StreamEvent(
                event_type=StreamEventType.TOOL_END,
                message=f"{tool_name} {'completed' if success else 'failed'}",
                detail=summary,
                tool_name=tool_name,
                is_error=not success,
                metadata={"exit_code": exit_code} if exit_code is not None else {},
            )
        )

    def emit_subprocess_start(
        self,
        command: str,
        tool_name: Optional[str] = None,
    ) -> None:
        """Emit subprocess start event."""
        self.emit(
            StreamEvent(
                event_type=StreamEventType.SUBPROCESS_START,
                message=f"Executing: {command[:100]}{'...' if len(command) > 100 else ''}",
                detail=command,
                tool_name=tool_name,
            )
        )

    def emit_subprocess_output(
        self,
        line: str,
        is_stderr: bool = False,
        tool_name: Optional[str] = None,
    ) -> None:
        """Emit subprocess output line."""
        event_type = (
            StreamEventType.SUBPROCESS_STDERR if is_stderr else StreamEventType.SUBPROCESS_STDOUT
        )
        self.emit(
            StreamEvent(
                event_type=event_type,
                message=line.strip()[:200],
                detail=line,
                tool_name=tool_name,
                is_error=is_stderr,
            )
        )

    def emit_subprocess_end(
        self,
        exit_code: int,
        duration_seconds: float,
        tool_name: Optional[str] = None,
    ) -> None:
        """Emit subprocess end event."""
        success = exit_code == 0
        status = "Completed" if success else "Failed"
        msg = f"{status} (exit {exit_code}) in {duration_seconds:.1f}s"
        self.emit(
            StreamEvent(
                event_type=StreamEventType.SUBPROCESS_END,
                message=msg,
                is_error=not success,
                tool_name=tool_name,
                metadata={"exit_code": exit_code, "duration_seconds": duration_seconds},
            )
        )

    def emit_agent_thinking(
        self,
        agent_role: str,
        step_name: Optional[str] = None,
    ) -> None:
        """Emit agent thinking event."""
        self.emit(
            StreamEvent(
                event_type=StreamEventType.AGENT_THINKING,
                message=f"{agent_role} is thinking...",
                agent_role=agent_role,
                step_name=step_name,
            )
        )

    def emit_agent_token(
        self,
        token: str,
        agent_role: Optional[str] = None,
    ) -> None:
        """Emit streaming LLM token."""
        self.emit(
            StreamEvent(
                event_type=StreamEventType.AGENT_TOKEN,
                message=token,
                agent_role=agent_role,
            )
        )

    def emit_agent_response(
        self,
        agent_role: str,
        response: str,
        success: bool = True,
        step_name: Optional[str] = None,
    ) -> None:
        """Emit agent response event."""
        self.emit(
            StreamEvent(
                event_type=StreamEventType.AGENT_RESPONSE,
                message=f"{agent_role} {'completed' if success else 'failed'}",
                detail=response,
                agent_role=agent_role,
                step_name=step_name,
                is_error=not success,
                collapsible=len(response) > 500,
            )
        )

    def get_recent_events(self, limit: int = 50) -> list[StreamEvent]:
        """Get recent events from history."""
        with self._lock:
            return list(self._event_history[-limit:])

    def clear_history(self) -> None:
        """Clear event history."""
        with self._lock:
            self._event_history.clear()


# Global emitter instance
_global_emitter: Optional[StreamingEventEmitter] = None
_emitter_lock = threading.Lock()


def get_streaming_emitter() -> StreamingEventEmitter:
    """Get the global streaming emitter instance."""
    global _global_emitter
    with _emitter_lock:
        if _global_emitter is None:
            _global_emitter = StreamingEventEmitter()
        return _global_emitter


def reset_streaming_emitter() -> None:
    """Reset the global emitter (for testing)."""
    global _global_emitter
    with _emitter_lock:
        _global_emitter = None


class StreamingQueue:
    """Context manager for consuming events from a queue.

    Handles queue lifecycle and provides iteration with timeout.

    Example:
        with StreamingQueue() as sq:
            for event in sq.iter_events(timeout=0.1):
                process(event)
    """

    def __init__(self, emitter: Optional[StreamingEventEmitter] = None):
        self._emitter = emitter or get_streaming_emitter()
        self._queue: Optional[queue.Queue] = None

    def __enter__(self) -> "StreamingQueue":
        self._queue = self._emitter.create_queue()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._queue:
            self._emitter.remove_queue(self._queue)
            self._queue = None
        return False

    def get(self, timeout: float = 0.1) -> Optional[StreamEvent]:
        """Get the next event or None if timeout."""
        if self._queue is None:
            return None
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def get_all(self, timeout: float = 0.05) -> list[StreamEvent]:
        """Get all available events up to timeout."""
        events = []
        if self._queue is None:
            return events

        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                event = self._queue.get_nowait()
                events.append(event)
            except queue.Empty:
                break

        return events

    def iter_events(self, timeout: float = 0.1):
        """Iterate over events with timeout between each."""
        while True:
            event = self.get(timeout=timeout)
            if event is None:
                yield None  # Yield None on timeout to allow UI updates
            else:
                yield event


def create_cycle_event_adapter(emitter: StreamingEventEmitter) -> Callable:
    """Create a CycleEvent callback that forwards to StreamingEventEmitter.

    This bridges the existing CycleEvent system to the new streaming infrastructure.
    """
    from odibi.agents.core.cycle import CycleEvent

    def adapter(event: CycleEvent) -> None:
        if event.event_type == "step_start":
            emitter.emit_simple(
                StreamEventType.STEP_START,
                event.message,
                event.detail,
                step_name=event.step,
                agent_role=event.agent_role,
            )
        elif event.event_type == "step_end":
            emitter.emit_simple(
                StreamEventType.STEP_END,
                event.message,
                event.detail,
                step_name=event.step,
                agent_role=event.agent_role,
                is_error=event.is_error,
            )
        elif event.event_type == "step_skip":
            emitter.emit_simple(
                StreamEventType.STEP_SKIP,
                event.message,
                event.detail,
                step_name=event.step,
            )
        elif event.event_type == "agent_thinking":
            emitter.emit_agent_thinking(event.agent_role, event.step)
        elif event.event_type == "agent_response":
            emitter.emit_agent_response(
                event.agent_role,
                event.detail,
                success=not event.is_error,
                step_name=event.step,
            )
        elif event.event_type == "agent_progress":
            emitter.emit_simple(
                StreamEventType.AGENT_PROGRESS,
                event.message,
                event.detail,
                agent_role=event.agent_role,
            )
        elif event.event_type == "cycle_end":
            emitter.emit_simple(
                StreamEventType.CYCLE_END,
                event.message,
                event.detail,
                is_error=event.is_error,
            )
        elif event.event_type == "warning":
            emitter.emit_simple(
                StreamEventType.WARNING,
                event.message,
                event.detail,
                is_error=True,
            )

    return adapter
