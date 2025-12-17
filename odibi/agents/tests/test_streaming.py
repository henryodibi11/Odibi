"""Tests for the streaming infrastructure."""

import queue
import threading
import time


from odibi.agents.ui.streaming import (
    StreamEvent,
    StreamEventType,
    StreamingEventEmitter,
    StreamingQueue,
    create_cycle_event_adapter,
    get_streaming_emitter,
    reset_streaming_emitter,
)


class TestStreamEvent:
    """Tests for StreamEvent dataclass."""

    def test_create_event(self):
        """Test basic event creation."""
        event = StreamEvent(
            event_type=StreamEventType.TOOL_START,
            message="Running test tool",
            tool_name="pytest",
        )
        assert event.event_type == StreamEventType.TOOL_START
        assert event.message == "Running test tool"
        assert event.tool_name == "pytest"
        assert event.is_error is False
        assert event.collapsible is False

    def test_event_to_dict(self):
        """Test event serialization."""
        event = StreamEvent(
            event_type=StreamEventType.AGENT_RESPONSE,
            message="Agent completed",
            detail="Full response content",
            agent_role="observer",
            is_error=False,
            collapsible=True,
        )
        data = event.to_dict()
        assert data["event_type"] == "agent_response"
        assert data["message"] == "Agent completed"
        assert data["detail"] == "Full response content"
        assert data["agent_role"] == "observer"
        assert data["collapsible"] is True
        assert "timestamp" in data


class TestStreamingEventEmitter:
    """Tests for StreamingEventEmitter."""

    def setup_method(self):
        """Reset global emitter before each test."""
        reset_streaming_emitter()

    def test_emit_simple(self):
        """Test simple event emission."""
        emitter = StreamingEventEmitter()
        received = []

        def callback(event):
            received.append(event)

        emitter.subscribe(callback)
        emitter.emit_simple(StreamEventType.STEP_START, "Starting step")

        assert len(received) == 1
        assert received[0].event_type == StreamEventType.STEP_START
        assert received[0].message == "Starting step"

    def test_queue_based_consumption(self):
        """Test consuming events via queue."""
        emitter = StreamingEventEmitter()
        q = emitter.create_queue()

        emitter.emit_simple(StreamEventType.TOOL_START, "Tool 1")
        emitter.emit_simple(StreamEventType.TOOL_END, "Tool 1 done")

        events = []
        while True:
            try:
                events.append(q.get_nowait())
            except queue.Empty:
                break

        assert len(events) == 2
        assert events[0].event_type == StreamEventType.TOOL_START
        assert events[1].event_type == StreamEventType.TOOL_END

        emitter.remove_queue(q)

    def test_tool_emission_helpers(self):
        """Test convenience methods for tool events."""
        emitter = StreamingEventEmitter()
        q = emitter.create_queue()

        emitter.emit_tool_start("pytest", "--verbose")
        emitter.emit_tool_progress("pytest", "Running 10 tests")
        emitter.emit_tool_output("pytest", "test_foo.py PASSED", is_stderr=False)
        emitter.emit_tool_end("pytest", success=True, exit_code=0)

        events = []
        while True:
            try:
                events.append(q.get_nowait())
            except queue.Empty:
                break

        assert len(events) == 4
        assert events[0].event_type == StreamEventType.TOOL_START
        assert events[0].tool_name == "pytest"
        assert events[1].event_type == StreamEventType.TOOL_PROGRESS
        assert events[2].event_type == StreamEventType.TOOL_OUTPUT
        assert events[2].collapsible is True
        assert events[3].event_type == StreamEventType.TOOL_END
        assert events[3].metadata.get("exit_code") == 0

    def test_subprocess_emission_helpers(self):
        """Test convenience methods for subprocess events."""
        emitter = StreamingEventEmitter()
        q = emitter.create_queue()

        emitter.emit_subprocess_start("wsl bash -c 'pytest'")
        emitter.emit_subprocess_output("line 1", is_stderr=False)
        emitter.emit_subprocess_output("error!", is_stderr=True)
        emitter.emit_subprocess_end(exit_code=0, duration_seconds=1.5)

        events = []
        while True:
            try:
                events.append(q.get_nowait())
            except queue.Empty:
                break

        assert len(events) == 4
        assert events[0].event_type == StreamEventType.SUBPROCESS_START
        assert events[1].event_type == StreamEventType.SUBPROCESS_STDOUT
        assert events[2].event_type == StreamEventType.SUBPROCESS_STDERR
        assert events[2].is_error is True
        assert events[3].event_type == StreamEventType.SUBPROCESS_END

    def test_agent_emission_helpers(self):
        """Test convenience methods for agent events."""
        emitter = StreamingEventEmitter()
        q = emitter.create_queue()

        emitter.emit_agent_thinking("observer", step_name="observation")
        emitter.emit_agent_token("Hello")
        emitter.emit_agent_token(" world")
        emitter.emit_agent_response("observer", "Full response text", success=True)

        events = []
        while True:
            try:
                events.append(q.get_nowait())
            except queue.Empty:
                break

        assert len(events) == 4
        assert events[0].event_type == StreamEventType.AGENT_THINKING
        assert events[0].agent_role == "observer"
        assert events[1].event_type == StreamEventType.AGENT_TOKEN
        assert events[1].message == "Hello"
        assert events[3].event_type == StreamEventType.AGENT_RESPONSE
        assert events[3].collapsible is False  # Short response (< 500 chars)

    def test_multiple_subscribers(self):
        """Test multiple callbacks receive events."""
        emitter = StreamingEventEmitter()
        received1 = []
        received2 = []

        emitter.subscribe(lambda e: received1.append(e))
        emitter.subscribe(lambda e: received2.append(e))

        emitter.emit_simple(StreamEventType.STEP_START, "Test")

        assert len(received1) == 1
        assert len(received2) == 1

    def test_unsubscribe(self):
        """Test unsubscribing from events."""
        emitter = StreamingEventEmitter()
        received = []

        def callback(event):
            received.append(event)

        emitter.subscribe(callback)
        emitter.emit_simple(StreamEventType.STEP_START, "Event 1")

        emitter.unsubscribe(callback)
        emitter.emit_simple(StreamEventType.STEP_START, "Event 2")

        assert len(received) == 1

    def test_event_history(self):
        """Test event history retrieval."""
        emitter = StreamingEventEmitter()

        for i in range(5):
            emitter.emit_simple(StreamEventType.STEP_START, f"Step {i}")

        history = emitter.get_recent_events(limit=3)
        assert len(history) == 3
        assert history[0].message == "Step 2"
        assert history[2].message == "Step 4"

    def test_clear_history(self):
        """Test clearing event history."""
        emitter = StreamingEventEmitter()
        emitter.emit_simple(StreamEventType.STEP_START, "Test")
        emitter.clear_history()

        assert len(emitter.get_recent_events()) == 0

    def test_queue_overflow_handling(self):
        """Test queue handles overflow gracefully."""
        emitter = StreamingEventEmitter(max_queue_size=2)
        q = emitter.create_queue()

        # Emit more events than queue can hold
        for i in range(5):
            emitter.emit_simple(StreamEventType.STEP_START, f"Event {i}")

        # Should have last 2 events (oldest dropped)
        events = []
        while True:
            try:
                events.append(q.get_nowait())
            except queue.Empty:
                break

        assert len(events) == 2
        assert events[0].message == "Event 3"
        assert events[1].message == "Event 4"

    def test_bad_callback_doesnt_break_emission(self):
        """Test that a bad callback doesn't prevent other callbacks."""
        emitter = StreamingEventEmitter()
        received = []

        def bad_callback(event):
            raise RuntimeError("Intentional error")

        def good_callback(event):
            received.append(event)

        emitter.subscribe(bad_callback)
        emitter.subscribe(good_callback)

        emitter.emit_simple(StreamEventType.STEP_START, "Test")
        assert len(received) == 1


class TestStreamingQueue:
    """Tests for StreamingQueue context manager."""

    def setup_method(self):
        reset_streaming_emitter()

    def test_context_manager(self):
        """Test queue lifecycle with context manager."""
        emitter = StreamingEventEmitter()

        with StreamingQueue(emitter) as sq:
            emitter.emit_simple(StreamEventType.STEP_START, "Test")
            event = sq.get(timeout=0.1)
            assert event is not None
            assert event.message == "Test"

    def test_get_timeout(self):
        """Test get returns None on timeout."""
        emitter = StreamingEventEmitter()

        with StreamingQueue(emitter) as sq:
            event = sq.get(timeout=0.05)
            assert event is None

    def test_get_all(self):
        """Test getting all available events."""
        emitter = StreamingEventEmitter()

        with StreamingQueue(emitter) as sq:
            emitter.emit_simple(StreamEventType.STEP_START, "Event 1")
            emitter.emit_simple(StreamEventType.STEP_END, "Event 2")
            time.sleep(0.01)  # Let events propagate

            events = sq.get_all(timeout=0.1)
            assert len(events) == 2

    def test_iter_events(self):
        """Test iterating over events."""
        emitter = StreamingEventEmitter()

        with StreamingQueue(emitter) as sq:
            emitter.emit_simple(StreamEventType.STEP_START, "Test")

            count = 0
            for event in sq.iter_events(timeout=0.05):
                if event is None:
                    break
                count += 1

            assert count == 1


class TestGlobalEmitter:
    """Tests for global emitter singleton."""

    def setup_method(self):
        reset_streaming_emitter()

    def test_get_creates_singleton(self):
        """Test get_streaming_emitter creates singleton."""
        emitter1 = get_streaming_emitter()
        emitter2 = get_streaming_emitter()
        assert emitter1 is emitter2

    def test_reset_clears_singleton(self):
        """Test reset creates new instance."""
        emitter1 = get_streaming_emitter()
        reset_streaming_emitter()
        emitter2 = get_streaming_emitter()
        assert emitter1 is not emitter2


class TestCycleEventAdapter:
    """Tests for CycleEvent to StreamEvent adapter."""

    def setup_method(self):
        reset_streaming_emitter()

    def test_adapter_converts_step_start(self):
        """Test adapter converts step_start events."""
        from odibi.agents.core.cycle import CycleEvent

        emitter = StreamingEventEmitter()
        adapter = create_cycle_event_adapter(emitter)
        q = emitter.create_queue()

        cycle_event = CycleEvent(
            event_type="step_start",
            step="observation",
            agent_role="observer",
            message="Starting observation",
        )
        adapter(cycle_event)

        event = q.get_nowait()
        assert event.event_type == StreamEventType.STEP_START
        assert event.step_name == "observation"
        assert event.agent_role == "observer"

    def test_adapter_converts_agent_thinking(self):
        """Test adapter converts agent_thinking events."""
        from odibi.agents.core.cycle import CycleEvent

        emitter = StreamingEventEmitter()
        adapter = create_cycle_event_adapter(emitter)
        q = emitter.create_queue()

        cycle_event = CycleEvent(
            event_type="agent_thinking",
            step="observation",
            agent_role="observer",
            message="Observer is thinking",
        )
        adapter(cycle_event)

        event = q.get_nowait()
        assert event.event_type == StreamEventType.AGENT_THINKING
        assert event.agent_role == "observer"

    def test_adapter_converts_agent_response(self):
        """Test adapter converts agent_response events."""
        from odibi.agents.core.cycle import CycleEvent

        emitter = StreamingEventEmitter()
        adapter = create_cycle_event_adapter(emitter)
        q = emitter.create_queue()

        cycle_event = CycleEvent(
            event_type="agent_response",
            step="observation",
            agent_role="observer",
            message="Observer completed",
            detail="Full observation result",
            is_error=False,
        )
        adapter(cycle_event)

        event = q.get_nowait()
        assert event.event_type == StreamEventType.AGENT_RESPONSE
        assert event.detail == "Full observation result"
        assert event.is_error is False


class TestThreadSafety:
    """Tests for thread safety."""

    def setup_method(self):
        reset_streaming_emitter()

    def test_concurrent_emission(self):
        """Test concurrent emission from multiple threads."""
        emitter = StreamingEventEmitter()
        received = []
        lock = threading.Lock()

        def callback(event):
            with lock:
                received.append(event)

        emitter.subscribe(callback)

        def emit_events(thread_id):
            for i in range(100):
                emitter.emit_simple(StreamEventType.STEP_START, f"Thread {thread_id} Event {i}")

        threads = [threading.Thread(target=emit_events, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(received) == 500

    def test_concurrent_queue_access(self):
        """Test concurrent queue creation and removal."""
        emitter = StreamingEventEmitter()
        queues = []
        errors = []

        def create_and_use_queue():
            try:
                q = emitter.create_queue()
                queues.append(q)
                time.sleep(0.01)
                emitter.remove_queue(q)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=create_and_use_queue) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
