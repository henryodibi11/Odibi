"""Thinking panel component for showing AI reasoning.

Displays the model's chain-of-thought and reasoning process in real-time,
similar to how Amp shows its thinking before taking actions.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import gradio as gr


@dataclass
class ThinkingState:
    """State of the thinking panel."""

    content: str = ""
    is_active: bool = False
    started_at: Optional[datetime] = None
    step_count: int = 0
    current_phase: str = "idle"  # idle, thinking, executing, complete

    def start(self, phase: str = "thinking"):
        """Start thinking state."""
        self.is_active = True
        self.started_at = datetime.now()
        self.current_phase = phase
        self.step_count += 1

    def append(self, text: str):
        """Append text to thinking content."""
        self.content += text

    def set(self, text: str):
        """Set thinking content."""
        self.content = text

    def complete(self):
        """Mark thinking as complete."""
        self.is_active = False
        self.current_phase = "complete"

    def reset(self):
        """Reset thinking state."""
        self.content = ""
        self.is_active = False
        self.started_at = None
        self.step_count = 0
        self.current_phase = "idle"

    @property
    def elapsed_seconds(self) -> float:
        """Get elapsed time in seconds."""
        if not self.started_at:
            return 0
        return (datetime.now() - self.started_at).total_seconds()

    def format_display(self) -> str:
        """Format for display."""
        if not self.content and not self.is_active:
            return ""

        phase_emoji = {
            "idle": "💤",
            "thinking": "🧠",
            "executing": "⚡",
            "complete": "✅",
        }.get(self.current_phase, "🔄")

        header = f"{phase_emoji} **Step {self.step_count}**"
        if self.is_active and self.started_at:
            elapsed = self.elapsed_seconds
            header += f" ({elapsed:.1f}s)"

        if not self.content:
            return f"{header}\n\n_Processing..._"

        truncated = self.content
        if len(truncated) > 2000:
            truncated = "..." + truncated[-2000:]

        return f"{header}\n\n```\n{truncated}\n```"


_thinking_state = ThinkingState()


def get_thinking_state() -> ThinkingState:
    """Get the global thinking state."""
    return _thinking_state


def start_thinking(phase: str = "thinking") -> str:
    """Start the thinking display."""
    _thinking_state.start(phase)
    return _thinking_state.format_display()


def update_thinking(text: str, append: bool = True) -> str:
    """Update the thinking content."""
    if append:
        _thinking_state.append(text)
    else:
        _thinking_state.set(text)
    return _thinking_state.format_display()


def complete_thinking() -> str:
    """Complete the thinking phase."""
    _thinking_state.complete()
    return _thinking_state.format_display()


def reset_thinking() -> str:
    """Reset the thinking panel."""
    _thinking_state.reset()
    return ""


def create_thinking_panel() -> tuple[gr.Column, dict[str, Any]]:
    """Create the thinking panel component.

    Returns:
        Tuple of (Gradio Column, dict of component references).
    """
    components: dict[str, Any] = {}

    with gr.Column() as thinking_column:
        with gr.Accordion("🧠 Thinking", open=False) as thinking_accordion:
            components["thinking_accordion"] = thinking_accordion

            components["thinking_display"] = gr.Markdown(
                value="",
                elem_classes=["thinking-panel"],
            )

            components["thinking_status"] = gr.Markdown(
                value="",
                elem_classes=["thinking-status"],
            )

    return thinking_column, components


def format_thinking_status(phase: str, step: int, elapsed: float = 0) -> str:
    """Format the thinking status line."""
    indicators = {
        "idle": "💤 Idle",
        "thinking": f"🧠 Thinking (step {step}, {elapsed:.1f}s)",
        "executing": f"⚡ Executing tool (step {step})",
        "complete": f"✅ Complete (step {step})",
    }
    return indicators.get(phase, f"🔄 {phase}")


def create_typing_indicator() -> str:
    """Create a typing indicator animation in HTML."""
    return """
<div class="typing-indicator">
    <div class="dot"></div>
    <div class="dot"></div>
    <div class="dot"></div>
</div>
"""


def create_spinner(text: str = "Processing...") -> str:
    """Create a spinner with text."""
    return f'<div class="spinner"></div> {text}'
