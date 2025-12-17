"""Streaming display components for real-time UI updates.

Provides:
- Collapsible sections for long outputs
- Progress spinners/indicators
- Formatted execution outputs with syntax highlighting
- Real-time streaming message formatting
- Amp-style tool blocks with visual hierarchy
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import html


@dataclass
class CollapsibleSection:
    """A collapsible section of content."""

    title: str
    content: str
    is_open: bool = False
    section_type: str = "default"  # default, stdout, stderr, code, diff
    timestamp: Optional[datetime] = None
    exit_code: Optional[int] = None

    def format_markdown(self) -> str:
        """Format as Markdown with details/summary tags.

        Note: Gradio Markdown supports HTML details/summary.
        """
        status_icon = self._get_status_icon()
        timestamp_str = f" `{self.timestamp.strftime('%H:%M:%S')}`" if self.timestamp else ""

        if self.section_type in ("stdout", "stderr"):
            content_formatted = f"```\n{self.content}\n```"
        elif self.section_type == "code":
            content_formatted = f"```python\n{self.content}\n```"
        elif self.section_type == "diff":
            content_formatted = f"```diff\n{self.content}\n```"
        else:
            content_formatted = self.content

        if self.exit_code is not None:
            exit_badge = f" `exit {self.exit_code}`"
        else:
            exit_badge = ""

        open_attr = " open" if self.is_open else ""
        return f"""<details{open_attr}>
<summary>{status_icon} {self.title}{timestamp_str}{exit_badge}</summary>

{content_formatted}

</details>"""

    def _get_status_icon(self) -> str:
        """Get icon based on section type and exit code."""
        if self.section_type == "stderr":
            return "⚠️"
        if self.exit_code is not None:
            return "✅" if self.exit_code == 0 else "❌"
        return "📄"


def format_collapsible_output(
    title: str,
    content: str,
    section_type: str = "default",
    is_open: bool = False,
    exit_code: Optional[int] = None,
    timestamp: Optional[datetime] = None,
) -> str:
    """Format content as a collapsible section.

    Args:
        title: Section title shown in summary
        content: Content to display when expanded
        section_type: Type for syntax highlighting (stdout, stderr, code, diff)
        is_open: Whether to start expanded
        exit_code: Optional exit code to display
        timestamp: Optional timestamp to display

    Returns:
        Markdown string with collapsible section
    """
    section = CollapsibleSection(
        title=title,
        content=content,
        is_open=is_open,
        section_type=section_type,
        timestamp=timestamp,
        exit_code=exit_code,
    )
    return section.format_markdown()


def format_execution_output(
    stdout: str,
    stderr: str,
    exit_code: int,
    command: str = "",
    duration_seconds: Optional[float] = None,
    tool_name: str = "Command",
) -> str:
    """Format execution results with collapsible stdout/stderr.

    Args:
        stdout: Standard output
        stderr: Standard error
        exit_code: Process exit code
        command: Command that was run
        duration_seconds: Execution duration
        tool_name: Name of tool/command for display

    Returns:
        Formatted Markdown with collapsible sections
    """
    parts = []
    success = exit_code == 0
    status_icon = "✅" if success else "❌"

    header = f"**{status_icon} {tool_name}**"
    if duration_seconds is not None:
        header += f" ({duration_seconds:.1f}s)"
    if exit_code != 0:
        header += f" — exit {exit_code}"
    parts.append(header)

    if command:
        parts.append(f"`{command[:100]}{'...' if len(command) > 100 else ''}`")

    if stdout.strip():
        stdout_lines = stdout.strip().split("\n")
        if len(stdout_lines) > 10:
            parts.append(
                format_collapsible_output(
                    f"Output ({len(stdout_lines)} lines)",
                    stdout.strip(),
                    section_type="stdout",
                    is_open=False,
                )
            )
        else:
            parts.append(f"```\n{stdout.strip()}\n```")

    if stderr.strip():
        stderr_lines = stderr.strip().split("\n")
        parts.append(
            format_collapsible_output(
                f"Errors ({len(stderr_lines)} lines)",
                stderr.strip(),
                section_type="stderr",
                is_open=len(stderr_lines) <= 5,
            )
        )

    return "\n\n".join(parts)


PROGRESS_SPINNERS = {
    "dots": ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"],
    "line": ["—", "\\", "|", "/"],
    "circle": ["◐", "◓", "◑", "◒"],
    "box": ["▖", "▘", "▝", "▗"],
    "pulse": ["⠁", "⠂", "⠄", "⡀", "⢀", "⠠", "⠐", "⠈"],
}


class ProgressIndicator:
    """Animated progress indicator for long operations.

    Maintains frame state for animation.
    """

    def __init__(self, style: str = "dots"):
        self.style = style
        self.frames = PROGRESS_SPINNERS.get(style, PROGRESS_SPINNERS["dots"])
        self.current_frame = 0
        self.message = ""
        self.started_at = datetime.now()
        self._substeps: list[str] = []

    def next_frame(self) -> str:
        """Get the next animation frame."""
        frame = self.frames[self.current_frame]
        self.current_frame = (self.current_frame + 1) % len(self.frames)
        return frame

    def set_message(self, message: str) -> None:
        """Update the progress message."""
        self.message = message

    def add_substep(self, step: str) -> None:
        """Add a substep to show what's happening."""
        self._substeps.append(step)
        if len(self._substeps) > 5:
            self._substeps = self._substeps[-5:]

    def format(self) -> str:
        """Format the progress indicator as Markdown."""
        frame = self.next_frame()
        elapsed = (datetime.now() - self.started_at).total_seconds()

        parts = [f"**{frame} {self.message}** ({elapsed:.0f}s)"]

        if self._substeps:
            for step in self._substeps[-3:]:
                parts.append(f"  ↳ {step}")

        return "\n".join(parts)


class StreamingMessageBuffer:
    """Buffer for accumulating streaming messages with deduplication.

    Helps format chat history without duplicate entries.
    """

    def __init__(self, max_messages: int = 100):
        self.messages: list[dict] = []
        self.max_messages = max_messages
        self._current_assistant_msg: Optional[str] = None

    def append_user(self, content: str) -> None:
        """Append a user message."""
        self.messages.append({"role": "user", "content": content})
        self._trim()

    def append_assistant(self, content: str) -> None:
        """Append an assistant message."""
        self.messages.append({"role": "assistant", "content": content})
        self._current_assistant_msg = None
        self._trim()

    def stream_assistant(self, token: str) -> None:
        """Stream a token to the current assistant message.

        If no current message, starts a new one.
        """
        if self._current_assistant_msg is None:
            self._current_assistant_msg = token
            self.messages.append({"role": "assistant", "content": token})
        else:
            self._current_assistant_msg += token
            if self.messages and self.messages[-1]["role"] == "assistant":
                self.messages[-1]["content"] = self._current_assistant_msg

    def finalize_stream(self) -> None:
        """Finalize the current streaming message."""
        self._current_assistant_msg = None

    def get_messages(self) -> list[dict]:
        """Get all messages."""
        return list(self.messages)

    def clear(self) -> None:
        """Clear all messages."""
        self.messages.clear()
        self._current_assistant_msg = None

    def _trim(self) -> None:
        """Trim old messages if over limit."""
        if len(self.messages) > self.max_messages:
            self.messages = self.messages[-self.max_messages :]


def format_tool_progress(
    tool_name: str,
    status: str,
    elapsed_seconds: float = 0,
    substeps: Optional[list[str]] = None,
) -> str:
    """Format tool progress as Markdown.

    Args:
        tool_name: Name of the tool being executed
        status: Current status message
        elapsed_seconds: Time elapsed
        substeps: Optional list of substeps completed

    Returns:
        Formatted Markdown string
    """
    from ..constants import get_tool_emoji

    emoji = get_tool_emoji(tool_name)
    parts = [f"**{emoji} {tool_name}** — {status} ({elapsed_seconds:.1f}s)"]

    if substeps:
        for step in substeps[-5:]:
            parts.append(f"  ↳ {step}")

    return "\n".join(parts)


def format_step_progress(
    step_name: str,
    step_index: int,
    total_steps: int,
    agent_role: Optional[str] = None,
    status: str = "running",
) -> str:
    """Format cycle step progress.

    Args:
        step_name: Name of the current step
        step_index: Current step index (0-based)
        total_steps: Total number of steps
        agent_role: Agent handling this step
        status: Step status (running, completed, skipped, error)

    Returns:
        Formatted Markdown string
    """
    progress_bar = format_progress_bar(step_index + 1, total_steps)

    status_icons = {
        "running": "🔄",
        "completed": "✅",
        "skipped": "⏭️",
        "error": "❌",
    }
    icon = status_icons.get(status, "⏳")

    parts = [f"### {icon} Step {step_index + 1}/{total_steps}: {step_name}"]
    parts.append(progress_bar)

    if agent_role:
        parts.append(f"Agent: **{agent_role}**")

    return "\n".join(parts)


def format_progress_bar(current: int, total: int, width: int = 20) -> str:
    """Format a text-based progress bar.

    Args:
        current: Current progress value
        total: Total value
        width: Bar width in characters

    Returns:
        Progress bar string like [████████░░░░] 67%
    """
    if total <= 0:
        return ""

    ratio = min(current / total, 1.0)
    filled = int(width * ratio)
    empty = width - filled

    bar = "█" * filled + "░" * empty
    percentage = int(ratio * 100)

    return f"[{bar}] {percentage}%"


def _escape_html(text: str) -> str:
    """Escape HTML special characters."""
    return html.escape(text)


@dataclass
class ToolBlockState:
    """State for a live-updating tool block.

    Amp-style tool execution block with:
    - Status badges (running/success/error)
    - Collapsible output sections
    - Truncated output with "Show more"
    - Elapsed time display
    - Exit code badges
    """

    tool_name: str
    emoji: str
    status: str = "running"  # running, success, error
    started_at: datetime = field(default_factory=datetime.now)
    elapsed_seconds: float = 0
    exit_code: Optional[int] = None
    stdout_lines: list[str] = field(default_factory=list)
    stderr_lines: list[str] = field(default_factory=list)
    subprocess_lines: list[tuple[str, bool]] = field(default_factory=list)  # (line, is_stderr)
    command: str = ""
    max_visible_lines: int = 15  # Lines to show before truncating

    def add_stdout(self, line: str) -> None:
        """Add a stdout line."""
        self.stdout_lines.append(line)

    def add_stderr(self, line: str) -> None:
        """Add a stderr line."""
        self.stderr_lines.append(line)

    def add_subprocess_line(self, line: str, is_stderr: bool = False) -> None:
        """Add a subprocess output line."""
        self.subprocess_lines.append((line, is_stderr))

    def complete(self, exit_code: int = 0) -> None:
        """Mark the tool block as complete."""
        self.exit_code = exit_code
        self.status = "success" if exit_code == 0 else "error"
        self.elapsed_seconds = (datetime.now() - self.started_at).total_seconds()

    def format_html(self) -> str:
        """Format as Amp-style HTML tool block.

        Works in Gradio Markdown component with HTML support.
        Uses <details> for collapsibility when complete.
        """
        status_class = self.status
        elapsed = self.elapsed_seconds or (datetime.now() - self.started_at).total_seconds()

        header_right_parts = []

        if self.status == "running":
            header_right_parts.append(
                '<span class="status-badge running"><span class="tool-status-spinner"></span> Running</span>'
            )
        elif self.status == "success":
            header_right_parts.append('<span class="status-badge success">✓ SUCCESS</span>')
        elif self.status == "error":
            header_right_parts.append('<span class="status-badge error">✗ FAILED</span>')

        header_right_parts.append(f'<span class="tool-elapsed">{elapsed:.1f}s</span>')

        if self.exit_code is not None and self.status != "running":
            badge_class = "success" if self.exit_code == 0 else "error"
            header_right_parts.append(
                f'<span class="exit-badge {badge_class}">exit {self.exit_code}</span>'
            )

        header_right = " ".join(header_right_parts)

        # Build inner content
        inner_parts = []

        if self.command:
            cmd_display = self.command[:100] + ("..." if len(self.command) > 100 else "")
            inner_parts.append(
                f'<div style="padding: 10px 16px; background: #010409; border-top: 1px solid #21262d;"><code style="color: #7d8590; font-size: 12px; font-family: \'SF Mono\', Consolas, monospace;">{_escape_html(cmd_display)}</code></div>'
            )

        if self.subprocess_lines:
            subprocess_html = self._format_subprocess_section()
            inner_parts.append(subprocess_html)

        if self.stdout_lines:
            stdout_html = self._format_output_section("stdout", self.stdout_lines, "Output")
            inner_parts.append(stdout_html)

        if self.stderr_lines:
            stderr_html = self._format_output_section("stderr", self.stderr_lines, "Errors")
            inner_parts.append(stderr_html)

        inner_content = "\n".join(inner_parts)

        # Use <details> for collapsibility when complete, open by default when running
        if self.status == "running":
            # While running, show as open details so user can see progress
            return f"""<details open class="tool-block-details {status_class}">
<summary class="tool-block-summary">
<div class="tool-header">
<div class="tool-header-left">{self.emoji} <span class="tool-name">{_escape_html(self.tool_name)}</span></div>
<div class="tool-header-right">{header_right}</div>
</div>
</summary>
<div class="tool-block-content">{inner_content}</div>
</details>"""
        else:
            # When complete, collapsed by default (user can expand)
            return f"""<details class="tool-block-details {status_class}">
<summary class="tool-block-summary">
<div class="tool-header">
<div class="tool-header-left">{self.emoji} <span class="tool-name">{_escape_html(self.tool_name)}</span></div>
<div class="tool-header-right">{header_right}</div>
</div>
</summary>
<div class="tool-block-content">{inner_content}</div>
</details>"""

    def _format_output_section(self, section_type: str, lines: list[str], title: str) -> str:
        """Format stdout/stderr as collapsible section with truncation."""
        line_count = len(lines)
        is_truncated = line_count > self.max_visible_lines
        display_lines = lines[-100:] if line_count > 100 else lines  # Cap at 100 lines

        if is_truncated and len(display_lines) > self.max_visible_lines:
            visible_content = "\n".join(
                _escape_html(line) for line in display_lines[: self.max_visible_lines]
            )
            hidden_content = "\n".join(
                _escape_html(line) for line in display_lines[self.max_visible_lines :]
            )
            hidden_count = len(display_lines) - self.max_visible_lines

            return f"""<details>
<summary>{title} ({line_count} lines)</summary>
<div class="output-content output-{section_type}">{visible_content}</div>
<details style="margin: 0; border: none; border-radius: 0;">
<summary style="padding: 8px 16px; background: #161b22; color: #58a6ff; font-size: 12px;">Show {hidden_count} more lines</summary>
<div class="output-content output-{section_type}">{hidden_content}</div>
</details>
</details>"""
        else:
            content = "\n".join(_escape_html(line) for line in display_lines)
            is_open = line_count <= 10
            open_attr = " open" if is_open else ""

            return f"""<details{open_attr}>
<summary>{title} ({line_count} lines)</summary>
<div class="output-content output-{section_type}">{content}</div>
</details>"""

    def _format_subprocess_section(self) -> str:
        """Format subprocess output with visual nesting and truncation.

        Shows first 5 lines, a collapsible middle section, and last 15 lines
        when there are many lines.
        """
        total_lines = len(self.subprocess_lines)
        running_class = "running" if self.status == "running" else ""

        if total_lines <= 25:
            # Show all lines if not too many
            lines_html = []
            for line, is_stderr in self.subprocess_lines:
                css_class = "stderr" if is_stderr else "stdout"
                lines_html.append(
                    f'<div class="subprocess-line {css_class}">{_escape_html(line)}</div>'
                )
            return f'<div class="subprocess-output {running_class}">{"".join(lines_html)}</div>'

        # Show first 5, collapsible middle, last 15
        first_lines = self.subprocess_lines[:5]
        last_lines = self.subprocess_lines[-15:]
        middle_lines = self.subprocess_lines[5:-15]
        middle_count = len(middle_lines)

        first_html = []
        for line, is_stderr in first_lines:
            css_class = "stderr" if is_stderr else "stdout"
            first_html.append(
                f'<div class="subprocess-line {css_class}">{_escape_html(line)}</div>'
            )

        last_html = []
        for line, is_stderr in last_lines:
            css_class = "stderr" if is_stderr else "stdout"
            last_html.append(f'<div class="subprocess-line {css_class}">{_escape_html(line)}</div>')

        middle_html = []
        for line, is_stderr in middle_lines:
            css_class = "stderr" if is_stderr else "stdout"
            middle_html.append(
                f'<div class="subprocess-line {css_class}">{_escape_html(line)}</div>'
            )

        middle_section = f"""<details style="margin: 4px 0;">
<summary style="color: #7d8590; font-size: 11px; cursor: pointer;">... {middle_count} more lines</summary>
<div>{"".join(middle_html)}</div>
</details>"""

        return f'<div class="subprocess-output {running_class}">{"".join(first_html)}{middle_section}{"".join(last_html)}</div>'


def format_tool_block(
    tool_name: str,
    emoji: str,
    status: str = "running",
    elapsed_seconds: float = 0,
    exit_code: Optional[int] = None,
    command: str = "",
    stdout: str = "",
    stderr: str = "",
) -> str:
    """Format a tool execution as an Amp-style block.

    Args:
        tool_name: Name of the tool
        emoji: Emoji icon for the tool
        status: running, success, or error
        elapsed_seconds: Time elapsed
        exit_code: Exit code if completed
        command: Command that was run
        stdout: Standard output
        stderr: Standard error

    Returns:
        HTML string for Gradio Markdown
    """
    block = ToolBlockState(
        tool_name=tool_name,
        emoji=emoji,
        status=status,
        elapsed_seconds=elapsed_seconds,
        exit_code=exit_code,
        command=command,
    )

    if stdout:
        block.stdout_lines = stdout.strip().split("\n") if stdout.strip() else []
    if stderr:
        block.stderr_lines = stderr.strip().split("\n") if stderr.strip() else []

    return block.format_html()


def format_thinking_block(agent_role: str, message: str = "", is_active: bool = True) -> str:
    """Format agent thinking as a styled block.

    Args:
        agent_role: Name of the agent
        message: Optional thinking message
        is_active: Whether currently thinking (if False, block is hidden/collapsed)

    Returns:
        HTML string for Gradio Markdown
    """
    if not is_active:
        # When thinking is complete, return empty string to hide the block
        # The agent_response block will show the result instead
        return ""

    spinner = '<span class="thinking-spinner"></span>'
    msg_html = f'<div class="thinking-message">{_escape_html(message)}</div>' if message else ""

    return f"""<div class="thinking-block">
<div class="thinking-header">{spinner} 🧠 <strong>{_escape_html(agent_role)}</strong> is thinking...</div>
{msg_html}
</div>"""


def format_step_block(
    step_name: str,
    step_index: int,
    total_steps: int,
    status: str = "running",
    agent_role: Optional[str] = None,
) -> str:
    """Format a cycle step as a progress block.

    Args:
        step_name: Name of the step
        step_index: Current step (0-based)
        total_steps: Total steps
        status: running, completed, skipped, error
        agent_role: Agent handling the step

    Returns:
        HTML string for Gradio Markdown
    """
    progress_pct = int(((step_index + 1) / total_steps) * 100) if total_steps > 0 else 0

    status_icons = {
        "running": "🔄",
        "completed": "✅",
        "skipped": "⏭️",
        "error": "❌",
    }
    icon = status_icons.get(status, "⏳")
    status_class = status if status in ("running", "completed") else ""

    agent_html = ""
    if agent_role:
        agent_html = f'<span style="color: #7d8590; font-size: 12px; margin-left: 8px;">• {_escape_html(agent_role)}</span>'

    return f"""<div class="step-block {status_class}">
<div class="step-header">
<span class="step-title">{icon} {_escape_html(step_name)}{agent_html}</span>
<span class="step-counter">{step_index + 1}/{total_steps}</span>
</div>
<div class="step-progress"><div class="step-progress-bar" style="width: {progress_pct}%;"></div></div>
</div>"""


def format_warning_block(message: str, detail: str = "") -> str:
    """Format a warning message."""
    detail_html = ""
    if detail:
        detail_html = f'<div style="margin-top: 8px; font-size: 12px; color: #d29922; opacity: 0.85;">{_escape_html(detail)}</div>'
    return f"""<div class="warning-block">
<span style="font-size: 16px;">⚠️</span>
<div>
<strong>Warning:</strong> {_escape_html(message)}
{detail_html}
</div>
</div>"""


def format_error_block(message: str, detail: str = "") -> str:
    """Format an error message."""
    detail_html = ""
    if detail:
        detail_html = f'<div style="margin-top: 8px; font-size: 12px;"><pre style="margin: 0; white-space: pre-wrap; color: #f85149; opacity: 0.85;">{_escape_html(detail)}</pre></div>'
    return f"""<div class="error-block">
<span style="font-size: 16px;">❌</span>
<div>
<strong>Error:</strong> {_escape_html(message)}
{detail_html}
</div>
</div>"""


def format_live_indicator(message: str = "Processing") -> str:
    """Format a live activity indicator."""
    return f"""<div class="live-indicator">
<span class="live-dot"></span>
<span>{_escape_html(message)}</span>
</div>"""


def format_running_banner(
    message: str = "Running...",
    elapsed_seconds: float = 0,
    step_info: str = "",
) -> str:
    """Format a prominent running banner that's always visible during operations.

    Args:
        message: Main message to display
        elapsed_seconds: Time elapsed
        step_info: Optional step/stage information

    Returns:
        HTML string for Gradio Markdown
    """
    elapsed_str = f"{elapsed_seconds:.0f}s" if elapsed_seconds else ""
    step_html = ""
    if step_info:
        step_html = f' <span class="running-banner-step">• {_escape_html(step_info)}</span>'

    return f"""<div class="running-banner">
<div class="running-banner-left">
<div class="running-banner-spinner"></div>
<span class="running-banner-text">{_escape_html(message)}{step_html}</span>
</div>
<span class="running-banner-elapsed">{elapsed_str}</span>
</div>"""


def format_running_inline(message: str = "Running") -> str:
    """Format a smaller inline running indicator."""
    return f"""<span class="running-inline">
<span class="running-inline-dot"></span>
<span>{_escape_html(message)}</span>
</span>"""


class LiveMessageBuffer:
    """Buffer for in-place message updates during streaming.

    Instead of appending new messages, this allows updating a single
    "live" message slot in the chat history.
    """

    def __init__(self):
        self._content_parts: list[str] = []
        self._is_live: bool = False
        self._tool_blocks: dict[str, ToolBlockState] = {}

    def start_live(self) -> None:
        """Start a live message session."""
        self._content_parts = []
        self._is_live = True
        self._tool_blocks = {}

    def end_live(self) -> None:
        """End the live message session."""
        self._is_live = False

    def append(self, content: str) -> None:
        """Append content to the live message."""
        self._content_parts.append(content)

    def set_content(self, content: str) -> None:
        """Replace all content."""
        self._content_parts = [content]

    def start_tool(self, tool_id: str, tool_name: str, emoji: str, command: str = "") -> None:
        """Start a new tool block."""
        self._tool_blocks[tool_id] = ToolBlockState(
            tool_name=tool_name,
            emoji=emoji,
            command=command,
        )

    def add_tool_output(self, tool_id: str, line: str, is_stderr: bool = False) -> None:
        """Add output to a tool block."""
        if tool_id in self._tool_blocks:
            if is_stderr:
                self._tool_blocks[tool_id].add_stderr(line)
            else:
                self._tool_blocks[tool_id].add_stdout(line)

    def add_subprocess_line(self, tool_id: str, line: str, is_stderr: bool = False) -> None:
        """Add subprocess line to a tool block."""
        if tool_id in self._tool_blocks:
            self._tool_blocks[tool_id].add_subprocess_line(line, is_stderr)

    def complete_tool(self, tool_id: str, exit_code: int = 0) -> None:
        """Mark a tool block as complete."""
        if tool_id in self._tool_blocks:
            self._tool_blocks[tool_id].complete(exit_code)

    def get_content(self) -> str:
        """Get the current combined content."""
        parts = list(self._content_parts)

        for block in self._tool_blocks.values():
            parts.append(block.format_html())

        if self._is_live and not self._tool_blocks:
            parts.append(format_live_indicator("Processing..."))

        return "\n\n".join(parts)

    @property
    def is_live(self) -> bool:
        return self._is_live


# Legacy CSS (kept for backward compatibility, now in constants.py)
STREAMING_CSS = """"""
