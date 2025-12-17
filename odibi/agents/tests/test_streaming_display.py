"""Tests for the streaming display components."""

from datetime import datetime


from odibi.agents.ui.components.streaming_display import (
    CollapsibleSection,
    ProgressIndicator,
    StreamingMessageBuffer,
    format_collapsible_output,
    format_execution_output,
    format_progress_bar,
    format_step_progress,
    format_tool_progress,
)


class TestCollapsibleSection:
    """Tests for CollapsibleSection."""

    def test_basic_section(self):
        """Test basic collapsible section formatting."""
        section = CollapsibleSection(
            title="Output",
            content="Hello world",
        )
        md = section.format_markdown()
        assert "<details>" in md
        assert "<summary>" in md
        assert "Output" in md
        assert "Hello world" in md

    def test_open_section(self):
        """Test section that starts open."""
        section = CollapsibleSection(
            title="Errors",
            content="Error message",
            is_open=True,
        )
        md = section.format_markdown()
        assert "<details open>" in md

    def test_stdout_section(self):
        """Test stdout formatting with code block."""
        section = CollapsibleSection(
            title="Output",
            content="line 1\nline 2",
            section_type="stdout",
        )
        md = section.format_markdown()
        assert "```\nline 1\nline 2\n```" in md

    def test_code_section(self):
        """Test code formatting with python syntax."""
        section = CollapsibleSection(
            title="Code",
            content="def foo():\n    pass",
            section_type="code",
        )
        md = section.format_markdown()
        assert "```python\ndef foo():\n    pass\n```" in md

    def test_diff_section(self):
        """Test diff formatting."""
        section = CollapsibleSection(
            title="Changes",
            content="+added\n-removed",
            section_type="diff",
        )
        md = section.format_markdown()
        assert "```diff\n+added\n-removed\n```" in md

    def test_timestamp_display(self):
        """Test timestamp is displayed."""
        timestamp = datetime(2024, 1, 15, 14, 30, 45)
        section = CollapsibleSection(
            title="Output",
            content="test",
            timestamp=timestamp,
        )
        md = section.format_markdown()
        assert "14:30:45" in md

    def test_exit_code_display(self):
        """Test exit code is displayed."""
        section = CollapsibleSection(
            title="Command",
            content="output",
            exit_code=0,
        )
        md = section.format_markdown()
        assert "exit 0" in md
        assert "✅" in md

    def test_error_exit_code(self):
        """Test error exit code shows different icon."""
        section = CollapsibleSection(
            title="Command",
            content="error",
            exit_code=1,
        )
        md = section.format_markdown()
        assert "❌" in md

    def test_stderr_icon(self):
        """Test stderr section shows warning icon."""
        section = CollapsibleSection(
            title="Errors",
            content="error",
            section_type="stderr",
        )
        md = section.format_markdown()
        assert "⚠️" in md


class TestFormatFunctions:
    """Tests for formatting functions."""

    def test_format_collapsible_output(self):
        """Test format_collapsible_output convenience function."""
        md = format_collapsible_output(
            title="Test",
            content="content",
            section_type="stdout",
            is_open=True,
        )
        assert "<details open>" in md
        assert "Test" in md

    def test_format_execution_output_success(self):
        """Test formatting successful execution."""
        md = format_execution_output(
            stdout="test passed",
            stderr="",
            exit_code=0,
            command="pytest test.py",
            duration_seconds=1.5,
            tool_name="pytest",
        )
        assert "✅" in md
        assert "pytest" in md
        assert "1.5s" in md
        assert "test passed" in md

    def test_format_execution_output_failure(self):
        """Test formatting failed execution."""
        md = format_execution_output(
            stdout="",
            stderr="assertion error",
            exit_code=1,
            tool_name="pytest",
        )
        assert "❌" in md
        assert "exit 1" in md
        assert "assertion error" in md

    def test_format_execution_output_long_stdout(self):
        """Test long stdout is collapsible."""
        long_output = "\n".join([f"line {i}" for i in range(20)])
        md = format_execution_output(
            stdout=long_output,
            stderr="",
            exit_code=0,
            tool_name="command",
        )
        assert "<details" in md
        assert "20 lines" in md

    def test_format_progress_bar(self):
        """Test progress bar formatting."""
        bar = format_progress_bar(5, 10, width=10)
        assert "█████░░░░░" in bar
        assert "50%" in bar

    def test_format_progress_bar_complete(self):
        """Test complete progress bar."""
        bar = format_progress_bar(10, 10, width=10)
        assert "██████████" in bar
        assert "100%" in bar

    def test_format_progress_bar_zero(self):
        """Test empty progress bar."""
        bar = format_progress_bar(0, 10, width=10)
        assert "░░░░░░░░░░" in bar
        assert "0%" in bar

    def test_format_progress_bar_zero_total(self):
        """Test progress bar with zero total."""
        bar = format_progress_bar(0, 0)
        assert bar == ""

    def test_format_step_progress(self):
        """Test step progress formatting."""
        md = format_step_progress(
            step_name="observation",
            step_index=2,
            total_steps=10,
            agent_role="observer",
            status="running",
        )
        assert "Step 3/10" in md
        assert "observation" in md
        assert "observer" in md
        assert "🔄" in md

    def test_format_step_progress_completed(self):
        """Test completed step progress."""
        md = format_step_progress(
            step_name="test",
            step_index=0,
            total_steps=5,
            status="completed",
        )
        assert "✅" in md

    def test_format_tool_progress(self):
        """Test tool progress formatting."""
        md = format_tool_progress(
            tool_name="pytest",
            status="Running tests",
            elapsed_seconds=5.2,
            substeps=["test_foo.py", "test_bar.py"],
        )
        assert "pytest" in md
        assert "Running tests" in md
        assert "5.2s" in md
        assert "test_foo.py" in md


class TestProgressIndicator:
    """Tests for ProgressIndicator."""

    def test_basic_indicator(self):
        """Test basic progress indicator."""
        indicator = ProgressIndicator()
        indicator.set_message("Loading...")

        md = indicator.format()
        assert "Loading..." in md

    def test_frame_animation(self):
        """Test frame advances on each format call."""
        indicator = ProgressIndicator(style="dots")

        frames = []
        for _ in range(10):
            md = indicator.format()
            # Extract the spinner character
            frames.append(md[2])  # After "**"

        # Should cycle through frames
        unique_frames = set(frames)
        assert len(unique_frames) > 1

    def test_substeps(self):
        """Test substep display."""
        indicator = ProgressIndicator()
        indicator.set_message("Running")
        indicator.add_substep("Step 1")
        indicator.add_substep("Step 2")

        md = indicator.format()
        assert "Step 1" in md
        assert "Step 2" in md
        assert "↳" in md

    def test_substep_limit(self):
        """Test substep list is limited."""
        indicator = ProgressIndicator()
        for i in range(10):
            indicator.add_substep(f"Step {i}")

        # Should only keep last 5
        assert len(indicator._substeps) == 5
        assert indicator._substeps[0] == "Step 5"

    def test_elapsed_time(self):
        """Test elapsed time is shown."""
        import time

        indicator = ProgressIndicator()
        indicator.set_message("Working")
        time.sleep(0.1)

        md = indicator.format()
        # Should show some elapsed time
        assert "s)" in md


class TestStreamingMessageBuffer:
    """Tests for StreamingMessageBuffer."""

    def test_append_messages(self):
        """Test appending user and assistant messages."""
        buffer = StreamingMessageBuffer()
        buffer.append_user("Hello")
        buffer.append_assistant("Hi there")

        messages = buffer.get_messages()
        assert len(messages) == 2
        assert messages[0]["role"] == "user"
        assert messages[1]["role"] == "assistant"

    def test_streaming_tokens(self):
        """Test streaming tokens to assistant message."""
        buffer = StreamingMessageBuffer()
        buffer.append_user("Hello")

        buffer.stream_assistant("Hi ")
        buffer.stream_assistant("there!")
        buffer.finalize_stream()

        messages = buffer.get_messages()
        assert len(messages) == 2
        assert messages[1]["content"] == "Hi there!"

    def test_streaming_updates_last_message(self):
        """Test streaming updates the last message in place."""
        buffer = StreamingMessageBuffer()

        buffer.stream_assistant("Hello")
        assert len(buffer.get_messages()) == 1

        buffer.stream_assistant(" world")
        assert len(buffer.get_messages()) == 1
        assert buffer.get_messages()[0]["content"] == "Hello world"

    def test_finalize_allows_new_stream(self):
        """Test finalize allows starting new stream."""
        buffer = StreamingMessageBuffer()

        buffer.stream_assistant("Message 1")
        buffer.finalize_stream()
        buffer.stream_assistant("Message 2")

        messages = buffer.get_messages()
        assert len(messages) == 2
        assert messages[0]["content"] == "Message 1"
        assert messages[1]["content"] == "Message 2"

    def test_clear(self):
        """Test clearing buffer."""
        buffer = StreamingMessageBuffer()
        buffer.append_user("Test")
        buffer.clear()

        assert len(buffer.get_messages()) == 0

    def test_max_messages_limit(self):
        """Test buffer respects max message limit."""
        buffer = StreamingMessageBuffer(max_messages=5)

        for i in range(10):
            buffer.append_user(f"Message {i}")

        messages = buffer.get_messages()
        assert len(messages) == 5
        assert messages[0]["content"] == "Message 5"
