"""Campaign monitoring and control panel for the Odibi Assistant.

Provides UI components for:
- Viewing campaign status
- Starting/stopping campaigns with live streaming feedback
- Viewing cycle history
- Viewing campaign reports
- Live activity log with chatbot-style streaming
- Real-time brain event streaming (thinking, tool calls, file changes)
"""

import json
import queue
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Generator, Optional

import gradio as gr

from odibi.agents.improve.config import CampaignConfig
from odibi.agents.improve.events import BrainEvent, BrainEventType, EventEmitter
from odibi.agents.improve.status import StatusTracker
from .activity_feed import add_activity, get_activity_feed

DEFAULT_ENVIRONMENT_ROOT = Path("D:/improve_odibi")


@dataclass
class CampaignState:
    """State for campaign runner UI."""

    stop_requested: bool = False
    is_running: bool = False
    activity_log: list[str] = field(default_factory=list)
    current_cycle: int = 0
    files_changed: list[dict] = field(default_factory=list)
    thinking_content: str = ""
    brain_events: list[BrainEvent] = field(default_factory=list)


# Global state for campaign control
_campaign_state = CampaignState()


def _get_status_tracker(root: Path) -> Optional[StatusTracker]:
    """Get a StatusTracker for the given environment root."""
    status_file = root / "status.json"
    reports_dir = root / "reports"
    if not root.exists():
        return None
    return StatusTracker(status_file, reports_dir)


def _format_elapsed_time(hours: float) -> str:
    """Format elapsed hours as readable string."""
    if hours < 1:
        return f"{int(hours * 60)}m"
    return f"{hours:.1f}h"


def _load_status(root_path: str) -> dict:
    """Load status from status.json."""
    try:
        root = Path(root_path)
        status_file = root / "status.json"
        if status_file.exists():
            with open(status_file, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _format_status_card(status: dict) -> str:
    """Format status as a markdown card."""
    if not status:
        return "**No campaign data available.**\n\nInitialize an environment first."

    campaign_id = status.get("campaign_id", "N/A")
    state = status.get("status", "UNKNOWN")
    cycles = status.get("cycles_completed", 0)
    promoted = status.get("improvements_promoted", 0)
    rejected = status.get("improvements_rejected", 0)
    elapsed = status.get("elapsed_hours", 0)
    stop_reason = status.get("stop_reason", "")

    state_emoji = {
        "RUNNING": "🟢",
        "COMPLETED": "✅",
        "STOPPED": "⏹️",
        "ERROR": "❌",
        "IDLE": "⚪",
    }.get(state, "❓")

    lines = [
        f"## {state_emoji} Campaign Status",
        "",
        f"**Campaign:** `{campaign_id}`",
        f"**Status:** {state}",
        f"**Elapsed:** {_format_elapsed_time(elapsed)}",
        "",
        "### Progress",
        f"- Cycles completed: **{cycles}**",
        f"- Improvements promoted: **{promoted}**",
        f"- Improvements rejected: **{rejected}**",
    ]

    if stop_reason:
        lines.append(f"- Stop reason: {stop_reason}")

    convergence = status.get("convergence_counter", 0)
    if convergence > 0:
        lines.append(f"- Convergence counter: {convergence}")

    return "\n".join(lines)


def _format_cycles_table(status: dict) -> str:
    """Format recent cycles as markdown table."""
    cycles = status.get("recent_cycles", [])
    if not cycles:
        return "_No cycles recorded yet._"

    lines = [
        "| Cycle | Status | Learning | Lesson |",
        "|-------|--------|----------|--------|",
    ]

    for c in cycles[-10:]:  # Last 10 cycles
        cycle_id = c.get("cycle_id", "?")
        promoted = "✅ Promoted" if c.get("promoted") else "❌ Rejected"
        learning = "📚 Yes" if c.get("learning") else "—"
        lesson = c.get("lesson", "—")[:50]
        lines.append(f"| {cycle_id} | {promoted} | {learning} | {lesson} |")

    return "\n".join(lines)


def _load_report(root_path: str) -> str:
    """Load the latest campaign report."""
    try:
        root = Path(root_path)
        tracker = _get_status_tracker(root)
        if tracker:
            return tracker.generate_report()
    except Exception as e:
        return f"Error loading report: {e}"
    return "_No report available._"


def _format_activity_log(entries: list[str], max_entries: int = 50) -> str:
    """Format activity log as markdown chatbot-style display."""
    if not entries:
        return "_Waiting for campaign to start..._"

    # Keep only recent entries
    recent = entries[-max_entries:]

    lines = []
    for entry in recent:
        lines.append(f"- {entry}")

    return "\n".join(lines)


def _format_diff_display(files_changed: list[dict]) -> str:
    """Format changed files with diffs."""
    if not files_changed:
        return "_No file changes yet._"

    lines = ["## 📝 Files Changed This Cycle\n"]

    for change in files_changed[-10:]:  # Last 10 changes
        path = change.get("path", "unknown")
        action = change.get("action", "modified")
        diff = change.get("diff", "")

        emoji = {"created": "🆕", "modified": "✏️", "deleted": "🗑️"}.get(action, "📄")
        lines.append(f"### {emoji} `{path}` ({action})\n")

        if diff:
            lines.append(f"```diff\n{diff[:500]}\n```\n")

    return "\n".join(lines)


def _add_activity(message: str, emoji: str = "📌") -> str:
    """Add an entry to the shared activity feed."""
    # Add to shared activity feed (shows in main UI Activity panel)
    activity_id = add_activity(f"{emoji} {message}", tool_name="campaign")
    # Also keep local copy for campaign-specific display
    timestamp = datetime.now().strftime("%H:%M:%S")
    _campaign_state.activity_log.append(f"`{timestamp}` {emoji} {message}")
    return activity_id


def _clear_activity() -> None:
    """Clear the activity log."""
    _campaign_state.activity_log = []
    _campaign_state.files_changed = []
    _campaign_state.thinking_content = ""
    _campaign_state.brain_events = []


def _format_thinking_display() -> str:
    """Format the current thinking content for display."""
    content = _campaign_state.thinking_content
    if not content:
        return "_No thinking yet..._"

    if len(content) > 2000:
        content = "..." + content[-2000:]

    return f"```\n{content}\n```"


def _format_brain_activity() -> str:
    """Format brain events as activity log."""
    events = _campaign_state.brain_events[-30:]
    if not events:
        return ""

    lines = [e.format_activity() for e in events]
    return "\n".join(lines)


class StreamingCampaignRunner:
    """Campaign runner with streaming callbacks for UI updates."""

    def __init__(
        self,
        root_path: Path,
        config: CampaignConfig,
        on_activity: Optional[Callable[[str, str], None]] = None,
        on_file_change: Optional[Callable[[dict], None]] = None,
    ):
        self.root_path = root_path
        self.config = config
        self.on_activity = on_activity or (lambda msg, emoji: None)
        self.on_file_change = on_file_change or (lambda change: None)
        self._stop_requested = False
        self._event_emitter = EventEmitter()
        self._event_queue: queue.Queue[BrainEvent] = queue.Queue()
        self._last_cycle_result = None  # Stores result from generator

        self._event_emitter.subscribe(self._handle_brain_event)

    def _handle_brain_event(self, event: BrainEvent) -> None:
        """Handle brain events and queue for UI update."""
        _campaign_state.brain_events.append(event)

        if event.type == BrainEventType.THINKING_CHUNK:
            chunk = event.data.get("chunk", "")
            _campaign_state.thinking_content += chunk

        elif event.type == BrainEventType.THINKING_START:
            _campaign_state.thinking_content = ""

        elif event.type == BrainEventType.FILE_WRITE:
            path = event.data.get("path", "unknown")
            is_new = event.data.get("is_new", False)
            action = "created" if is_new else "modified"
            change = {"path": path, "action": action, "diff": ""}
            _campaign_state.files_changed.append(change)
            self.on_file_change(change)

        self._event_queue.put(event)

    def request_stop(self) -> None:
        """Request the campaign to stop."""
        self._stop_requested = True
        _campaign_state.stop_requested = True

    @property
    def should_stop(self) -> bool:
        """Check if stop was requested."""
        return self._stop_requested or _campaign_state.stop_requested

    def run_streaming(self) -> Generator[dict, None, None]:
        """Run campaign with streaming yields for UI updates.

        Yields status updates as dicts with:
        - activity_log: str - formatted activity log
        - status_card: str - formatted status card
        - cycles_table: str - formatted cycles table
        - diff_display: str - formatted diff display
        - thinking_display: str - current LLM thinking
        - brain_activity: str - brain event log
        - is_complete: bool - whether campaign finished
        - result_message: str - final result message
        """
        from odibi.agents.improve.campaign import create_campaign_runner
        from odibi.agents.improve.environment import load_environment

        _clear_activity()
        _campaign_state.stop_requested = False
        _campaign_state.is_running = True

        self.on_activity("🚀 Starting campaign...", "🚀")
        yield self._build_update(
            is_complete=False,
            chat_message="🚀 **Starting improvement campaign...**",
        )

        try:
            self.on_activity(f"Loading environment: {self.root_path}", "📂")
            yield self._build_update(
                is_complete=False,
                chat_message=f"📂 Loading environment from `{self.root_path}`",
            )

            load_environment(self.root_path)
            self.on_activity("Environment loaded successfully", "✅")
            yield self._build_update(is_complete=False)

            runner = create_campaign_runner(
                self.root_path,
                self.config,
                event_emitter=self._event_emitter,
            )
            self.on_activity(
                f"Campaign configured: max_cycles={self.config.max_cycles}, "
                f"model={self.config.llm_model}",
                "⚙️",
            )
            yield self._build_update(
                is_complete=False,
                chat_message=(
                    f"⚙️ Campaign configured:\n"
                    f"- **Max cycles:** {self.config.max_cycles}\n"
                    f"- **Model:** {self.config.llm_model}\n"
                    f"- **Goal:** {self.config.goal}"
                ),
            )

            # Run cycles with streaming
            for cycle_update in self._run_cycles_streaming(runner):
                if self.should_stop:
                    self.on_activity("⏹️ Stop requested by user", "⏹️")
                    yield self._build_update(
                        is_complete=True,
                        result_message="Campaign stopped by user",
                        chat_message="⏹️ Campaign stopped by user.",
                    )
                    return
                yield cycle_update

            # Generate final result
            result = self._finalize_campaign(runner)
            yield self._build_update(
                is_complete=True,
                result_message=result,
                chat_message=result,
            )

        except Exception as e:
            self.on_activity(f"❌ Error: {e}", "❌")
            yield self._build_update(
                is_complete=True,
                result_message=f"❌ Campaign failed: {e}",
            )

        finally:
            _campaign_state.is_running = False

    def _run_cycles_streaming(self, runner) -> Generator[dict, None, None]:
        """Run campaign cycles with streaming updates."""
        campaign_id = runner._generate_campaign_id()
        runner._campaign_id = campaign_id
        runner._started_at = datetime.now()
        runner._cycles = []

        self.on_activity(f"Campaign ID: {campaign_id}", "🏷️")
        runner._status.start_campaign(campaign_id)
        yield self._build_update(is_complete=False)

        for cycle_num in range(self.config.max_cycles):
            if self.should_stop:
                return

            # Check time budget
            elapsed = runner.elapsed_hours()
            if elapsed > self.config.max_hours:
                self.on_activity(f"⏰ Time budget exhausted ({self.config.max_hours}h)", "⏰")
                return

            # Check convergence
            if runner.is_converged():
                self.on_activity("🔄 Converged - no more learning", "🔄")
                return

            # Start cycle with unique ID including campaign
            campaign_short = campaign_id[-12:] if campaign_id else "unknown"
            cycle_id = f"{campaign_short}_cycle_{cycle_num:03d}"
            _campaign_state.current_cycle = cycle_num
            self.on_activity(f"━━━ Starting {cycle_id} ━━━", "🔄")
            yield self._build_update(
                is_complete=False,
                chat_message=f"🔄 **Starting cycle {cycle_num + 1}/{self.config.max_cycles}**",
            )

            # Run cycle with detailed logging - consume generator and yield updates
            self._last_cycle_result = None
            for update in self._run_single_cycle_streaming(runner, cycle_num):
                yield update
            result = self._last_cycle_result
            if result is None:
                self.on_activity("Cycle produced no result", "⚠️")
                continue
            runner._cycles.append(result)

            # Record to memory and status
            runner._memory.record(result)
            runner._status.update(result)

            # Log result
            if result.promoted:
                self.on_activity(f"✅ {cycle_id}: Changes promoted to Master!", "✅")
                yield self._build_update(
                    is_complete=False,
                    chat_message=f"✅ **Cycle {cycle_num + 1} succeeded!** Changes promoted to Master.",
                )
            else:
                reason = result.rejection_reason or "Unknown"
                self.on_activity(f"❌ {cycle_id}: Rejected - {reason[:60]}", "❌")
                yield self._build_update(
                    is_complete=False,
                    chat_message=f"❌ Cycle {cycle_num + 1} rejected: {reason[:100]}",
                )

            # Small delay for UI responsiveness
            time.sleep(0.1)

    def _run_single_cycle_streaming(self, runner, cycle_num: int) -> Generator[dict, None, None]:
        """Run a single cycle with detailed activity logging.

        This is a generator that yields UI updates during LLM processing.
        The final CycleResult is stored in self._last_cycle_result after iteration.
        """
        from odibi.agents.improve.results import CycleResult

        cycle_id = f"cycle_{cycle_num:03d}"
        result = CycleResult(
            cycle_id=cycle_id,
            sandbox_path=Path("."),
            started_at=datetime.now(),
        )

        try:
            avoid_issues = runner._memory.get_failed_approaches()
            if avoid_issues:
                self.on_activity(f"Avoiding {len(avoid_issues)} known bad approaches", "🚫")
        except Exception as e:
            self.on_activity(f"Warning: Could not load memory: {e}", "⚠️")
            avoid_issues = []

        # Create sandbox
        self.on_activity("Creating sandbox from Master...", "📦")
        sandbox = runner._env.create_sandbox(cycle_id)
        result.sandbox_path = sandbox.sandbox_path

        try:
            # Run gate checks
            self.on_activity("Running gate checks (tests, lint, golden)...", "🧪")
            gate_result = runner.check_gates(sandbox)

            # Report gate results safely
            try:
                if gate_result.test_result:
                    tr = gate_result.test_result
                    status = "passed" if tr.success else f"{tr.failed} failed"
                    emoji = "✅" if tr.success else "❌"
                    self.on_activity(f"Tests: {emoji} {status}", "🧪")

                if gate_result.lint_result:
                    lr = gate_result.lint_result
                    status = "clean" if lr.success else f"{lr.error_count} errors"
                    emoji = "✅" if lr.success else "❌"
                    self.on_activity(f"Lint: {emoji} {status}", "📝")

                if gate_result.golden_results:
                    passed = sum(1 for g in gate_result.golden_results if g.passed)
                    total = len(gate_result.golden_results)
                    self.on_activity(f"Golden: {passed}/{total} passed", "🏆")
            except Exception as e:
                self.on_activity(f"Warning parsing gate results: {e}", "⚠️")

            # LLM improvement if needed - run in background thread for real-time streaming
            improvement_attempts = 0
            if not gate_result.all_passed and runner._brain is not None:
                self.on_activity("🧠 LLM Brain analyzing failures...", "🧠")

                # Track file changes during improvement
                original_files = self._snapshot_files(sandbox.sandbox_path)

                # Drain any old events from the queue
                while not self._event_queue.empty():
                    try:
                        self._event_queue.get_nowait()
                    except queue.Empty:
                        break

                # Run brain in background thread for real-time streaming
                brain_result = {
                    "gate_result": gate_result,
                    "attempts": 0,
                    "files_modified": [],
                    "done": False,
                }

                def run_brain():
                    try:
                        gr, attempts, files_changed = runner._brain.improve_sandbox(
                            sandbox=sandbox,
                            gate_result=gate_result,
                            avoid_issues=avoid_issues,
                            campaign_goal=self.config.goal,
                            run_gates=lambda s: runner.check_gates(s),
                        )
                        brain_result["gate_result"] = gr
                        brain_result["attempts"] = attempts
                        brain_result["files_modified"] = files_changed
                    except Exception as e:
                        self.on_activity(f"Brain error: {e}", "❌")
                    finally:
                        brain_result["done"] = True
                        self._event_queue.put(None)  # Sentinel

                brain_thread = threading.Thread(target=run_brain, daemon=True)
                brain_thread.start()

                # Poll event queue and yield updates while brain runs
                last_yield = time.time()
                yield_interval = 0.15  # 150ms for responsive UI

                while not brain_result["done"] or not self._event_queue.empty():
                    try:
                        event = self._event_queue.get(timeout=0.05)
                        if event is None:
                            continue  # Sentinel, just continue draining
                    except queue.Empty:
                        pass  # No event, just check time

                    # Yield update periodically for smooth UI
                    now = time.time()
                    if now - last_yield >= yield_interval:
                        last_yield = now
                        yield self._build_update(is_complete=False)

                    if self.should_stop:
                        break

                # Wait for thread to finish
                brain_thread.join(timeout=5.0)

                gate_result = brain_result["gate_result"]
                improvement_attempts = brain_result["attempts"]
                result.files_modified = brain_result["files_modified"]

                # Detect file changes for UI display
                new_files = self._snapshot_files(sandbox.sandbox_path)
                changes = self._diff_files(original_files, new_files)
                for change in changes:
                    self.on_file_change(change)
                    _campaign_state.files_changed.append(change)

                if improvement_attempts > 0:
                    files_str = ", ".join(result.files_modified[:3]) or "none"
                    if len(result.files_modified) > 3:
                        files_str += f" (+{len(result.files_modified) - 3} more)"
                    self.on_activity(
                        f"LLM made {improvement_attempts} attempt(s), modified: {files_str}", "🧠"
                    )
                    status = "passed ✅" if gate_result.all_passed else "still failing ❌"
                    self.on_activity(f"Gates after LLM: {status}", "🔍")

            result.gate_result = gate_result

            # Extract results
            if gate_result.test_result:
                result.test_result = gate_result.test_result
                result.tests_passed = gate_result.test_result.passed
                result.tests_failed = gate_result.test_result.failed

            if gate_result.lint_result:
                result.lint_result = gate_result.lint_result
                result.lint_errors = gate_result.lint_result.error_count

            result.golden_results = gate_result.golden_results
            result.golden_passed = sum(1 for g in gate_result.golden_results if g.passed)
            result.golden_failed = sum(1 for g in gate_result.golden_results if not g.passed)

            if gate_result.all_passed:
                self.on_activity("All gates passed! Promoting to Master...", "🎉")
                runner._env.snapshot_master(cycle_id)
                runner._env.promote_to_master(sandbox)

                lesson = (
                    f"Gates passed after {improvement_attempts} LLM attempts"
                    if improvement_attempts > 0
                    else "Gates passed, changes promoted"
                )
                result.mark_complete(promoted=True, learning=True, lesson=lesson)
            else:
                reasons = gate_result.failure_reasons
                reason = "; ".join(reasons) if reasons else "Gate check failed"
                if improvement_attempts > 0:
                    reason = f"LLM tried {improvement_attempts}x but: {reason}"

                result.mark_complete(
                    promoted=False,
                    rejection_reason=reason,
                    learning=len(reasons) > 0,
                    lesson=reason,
                )

        except Exception as e:
            self.on_activity(f"Cycle error: {e}", "❌")
            result.mark_complete(
                promoted=False,
                rejection_reason=str(e),
                learning=False,
            )

        finally:
            try:
                runner._env.destroy_sandbox(sandbox)
                self.on_activity("Sandbox cleaned up", "🧹")
            except Exception:
                pass

        # Store result for caller to retrieve after generator exhaustion
        self._last_cycle_result = result

    def _snapshot_files(self, sandbox_path: Path) -> dict[str, str]:
        """Snapshot file contents in sandbox."""
        files = {}
        try:
            for py_file in sandbox_path.rglob("*.py"):
                if ".git" in str(py_file) or "__pycache__" in str(py_file):
                    continue
                rel_path = py_file.relative_to(sandbox_path)
                try:
                    files[str(rel_path)] = py_file.read_text(encoding="utf-8")
                except Exception:
                    pass
        except Exception:
            pass
        return files

    def _diff_files(self, before: dict[str, str], after: dict[str, str]) -> list[dict]:
        """Compute file diffs between snapshots."""
        import difflib

        changes = []

        # Find modified and new files
        for path, content in after.items():
            if path not in before:
                changes.append({"path": path, "action": "created", "diff": f"+++ {path}\n..."})
            elif before[path] != content:
                diff = difflib.unified_diff(
                    before[path].splitlines(keepends=True),
                    content.splitlines(keepends=True),
                    fromfile=f"a/{path}",
                    tofile=f"b/{path}",
                    lineterm="",
                )
                diff_text = "".join(list(diff)[:30])  # Limit diff size
                changes.append({"path": path, "action": "modified", "diff": diff_text})

        # Find deleted files
        for path in before:
            if path not in after:
                changes.append({"path": path, "action": "deleted", "diff": ""})

        return changes

    def _finalize_campaign(self, runner) -> str:
        """Generate final result message."""
        cycles = len(runner._cycles)
        promoted = sum(1 for c in runner._cycles if c.promoted)
        rejected = cycles - promoted
        elapsed = runner.elapsed_hours()

        runner._status.finish_campaign("COMPLETED")
        try:
            runner._status.save_report()
        except Exception:
            pass

        self.on_activity("━━━ Campaign Complete ━━━", "🏁")

        return (
            f"✅ **Campaign completed!**\n\n"
            f"- **Cycles:** {cycles}\n"
            f"- **Promoted:** {promoted}\n"
            f"- **Rejected:** {rejected}\n"
            f"- **Duration:** {_format_elapsed_time(elapsed)}"
        )

    def _build_update(
        self, is_complete: bool, result_message: str = "", chat_message: str = ""
    ) -> dict:
        """Build a status update dict for the UI."""
        status = _load_status(str(self.root_path))

        return {
            "activity_log": get_activity_feed().format_markdown(),  # Shared feed
            "status_card": _format_status_card(status),
            "cycles_table": _format_cycles_table(status),
            "diff_display": "",
            "thinking_display": "",
            "is_complete": is_complete,
            "result_message": result_message,
            "chat_message": chat_message,
        }


class CampaignsPanel:
    """Campaign monitoring and control panel for Gradio UI."""

    def __init__(self, environment_root: Optional[Path] = None):
        """Initialize the campaigns panel.

        Args:
            environment_root: Path to improvement environment root.
        """
        self._root = environment_root or DEFAULT_ENVIRONMENT_ROOT
        self._streaming_runner: Optional[StreamingCampaignRunner] = None

    def render(self) -> tuple[gr.Column, dict[str, Any]]:
        """Render the campaigns panel.

        Returns:
            Tuple of (Gradio Column, component references dict).
        """
        components: dict[str, Any] = {}

        with gr.Column() as column:
            gr.Markdown("## 🚀 Campaign Runner")

            with gr.Row():
                components["root_path"] = gr.Textbox(
                    label="Environment Root",
                    value=str(self._root),
                    scale=3,
                )
                components["refresh_btn"] = gr.Button("🔄 Refresh", scale=1)

            components["status_card"] = gr.Markdown(
                value="_Click Refresh to load status_",
                elem_classes=["status-card"],
            )

            with gr.Row():
                components["cycles_table"] = gr.Markdown(
                    value="_No cycles yet_",
                    elem_classes=["cycles-table"],
                )

            with gr.Accordion("📋 Campaign Settings", open=False):
                with gr.Row():
                    components["max_cycles"] = gr.Slider(
                        label="Max Cycles",
                        minimum=1,
                        maximum=50,
                        value=10,
                        step=1,
                    )
                    components["max_hours"] = gr.Slider(
                        label="Max Hours",
                        minimum=0.5,
                        maximum=8.0,
                        value=4.0,
                        step=0.5,
                    )

                with gr.Row():
                    components["llm_model"] = gr.Dropdown(
                        label="LLM Model",
                        choices=[
                            "gpt-4o-mini",
                            "gpt-4o",
                            "gpt-4-turbo",
                            "o4-mini",
                        ],
                        value="gpt-4o-mini",
                    )
                    components["max_attempts"] = gr.Slider(
                        label="Max LLM Attempts",
                        minimum=1,
                        maximum=10,
                        value=3,
                        step=1,
                    )

                components["goal"] = gr.Textbox(
                    label="Campaign Goal",
                    value="Find and fix bugs to stabilize odibi",
                    lines=2,
                )

                components["enable_llm"] = gr.Checkbox(
                    label="Enable LLM Improvements",
                    value=True,
                )

            with gr.Accordion("🐧 WSL / Spark Settings", open=False):
                components["use_wsl"] = gr.Checkbox(
                    label="Run through WSL (required for Spark on Windows)",
                    value=True,
                )
                with gr.Row():
                    components["wsl_distro"] = gr.Textbox(
                        label="WSL Distribution",
                        value="Ubuntu-20.04",
                        scale=1,
                    )
                    components["wsl_python"] = gr.Textbox(
                        label="WSL Python Command",
                        value="python3.9",
                        scale=1,
                    )

                gr.Markdown("**Spark Environment Variables:**")
                with gr.Row():
                    components["spark_home"] = gr.Textbox(
                        label="SPARK_HOME",
                        value="/opt/spark",
                        scale=1,
                    )
                    components["java_home"] = gr.Textbox(
                        label="JAVA_HOME",
                        value="/usr/lib/jvm/java-11-openjdk-amd64",
                        scale=1,
                    )

                components["wsl_shell_init"] = gr.Textbox(
                    label="Shell Init (optional)",
                    value="",
                    placeholder="e.g., source ~/.bashrc or source ~/spark-env.sh",
                    info="Commands to run before each WSL command",
                )

                components["test_wsl_btn"] = gr.Button(
                    "🔍 Test WSL Environment",
                    variant="secondary",
                )
                components["wsl_test_result"] = gr.Markdown(
                    value="_Click 'Test WSL Environment' to validate setup_",
                    elem_classes=["wsl-test-result"],
                )

            with gr.Row():
                components["start_btn"] = gr.Button(
                    "▶️ Start Campaign",
                    variant="primary",
                    scale=2,
                )
                components["stop_btn"] = gr.Button(
                    "⏹️ Stop",
                    variant="stop",
                    scale=1,
                )

            # Status display only - activity goes to shared Activity panel
            components["run_status"] = gr.Markdown(
                value="_Ready to start campaign_",
                elem_classes=["run-status"],
            )

            # Hidden components to maintain compatibility with handlers
            components["activity_log"] = gr.Markdown(value="", visible=False)
            components["thinking_display"] = gr.Markdown(value="", visible=False)
            components["diff_display"] = gr.Markdown(value="", visible=False)

            with gr.Accordion("📄 Campaign Report", open=False):
                components["report_view"] = gr.Markdown(
                    value="_Load a report to view_",
                )
                components["load_report_btn"] = gr.Button("Load Report")

        return column, components


def create_campaigns_panel(
    environment_root: Optional[Path] = None,
) -> tuple[gr.Column, dict[str, Any]]:
    """Create the campaigns panel component.

    Args:
        environment_root: Path to improvement environment.

    Returns:
        Tuple of (Gradio Column, component references dict).
    """
    panel = CampaignsPanel(environment_root)
    return panel.render()


def setup_campaigns_handlers(
    components: dict[str, Any],
    chat_components: Optional[dict[str, Any]] = None,
) -> None:
    """Set up event handlers for campaigns panel.

    Args:
        components: Dict of Gradio components from create_campaigns_panel.
        chat_components: Optional chat components dict for shared activity display.
    """

    def on_refresh(root_path: str):
        """Refresh status display."""
        status = _load_status(root_path)
        status_card = _format_status_card(status)
        cycles_table = _format_cycles_table(status)
        return status_card, cycles_table

    components["refresh_btn"].click(
        fn=on_refresh,
        inputs=[components["root_path"]],
        outputs=[components["status_card"], components["cycles_table"]],
    )

    def on_load_report(root_path: str):
        """Load campaign report."""
        return _load_report(root_path)

    components["load_report_btn"].click(
        fn=on_load_report,
        inputs=[components["root_path"]],
        outputs=[components["report_view"]],
    )

    has_shared_activity = chat_components and "activity_display" in chat_components
    has_chatbot = chat_components and "chatbot" in chat_components

    # Accumulate chat messages during campaign
    campaign_chat_history: list[dict] = []

    def make_result(activity, status_card, cycles, diff, thinking, run_status, chat_msg=""):
        """Build result tuple, optionally including shared activity and chatbot."""
        base = (activity, status_card, cycles, diff, thinking, run_status)
        if has_shared_activity:
            base = base + (activity,)
        if has_chatbot:
            if chat_msg:
                campaign_chat_history.append({"role": "assistant", "content": chat_msg})
            base = base + (list(campaign_chat_history),)
        return base

    def on_start_campaign(
        root_path: str,
        max_cycles: int,
        max_hours: float,
        llm_model: str,
        max_attempts: int,
        goal: str,
        enable_llm: bool,
        use_wsl: bool,
        wsl_distro: str,
        wsl_python: str,
        spark_home: str,
        java_home: str,
        wsl_shell_init: str,
    ):
        """Start a campaign with streaming updates."""
        global _campaign_state

        # Clear chat history for new campaign
        campaign_chat_history.clear()

        if _campaign_state.is_running:
            yield make_result(
                _format_activity_log(_campaign_state.activity_log),
                _format_status_card(_load_status(root_path)),
                _format_cycles_table(_load_status(root_path)),
                _format_diff_display(_campaign_state.files_changed),
                _format_thinking_display(),
                "⚠️ Campaign already running",
            )
            return

        root = Path(root_path)
        if not root.exists():
            yield make_result(
                "_Environment not found_",
                f"❌ Environment not found: {root_path}",
                "_No cycles_",
                "_No changes_",
                "_No thinking yet..._",
                f"❌ Environment not found: {root_path}",
            )
            return

        if use_wsl:
            try:
                from odibi.agents.improve.runner import check_wsl_available

                wsl_ok, wsl_msg = check_wsl_available(wsl_distro)
                if not wsl_ok:
                    yield make_result(
                        f"❌ WSL pre-flight check failed:\n{wsl_msg}",
                        f"❌ {wsl_msg}",
                        "_WSL check failed_",
                        "_No changes_",
                        "_No thinking yet..._",
                        f"❌ WSL Error: {wsl_msg}",
                    )
                    return

                _add_activity(f"WSL pre-flight: {wsl_msg}", "✅")
            except Exception as e:
                _add_activity(f"WSL check error (continuing anyway): {e}", "⚠️")

        # Build WSL environment variables
        wsl_env = {}
        if spark_home.strip():
            wsl_env["SPARK_HOME"] = spark_home.strip()
        if java_home.strip():
            wsl_env["JAVA_HOME"] = java_home.strip()
        wsl_env["PYSPARK_PYTHON"] = wsl_python

        config = CampaignConfig(
            name="UI Campaign",
            goal=goal,
            max_cycles=int(max_cycles),
            max_hours=max_hours,
            convergence_threshold=3,
            enable_llm_improvements=enable_llm,
            llm_model=llm_model,
            max_improvement_attempts_per_cycle=int(max_attempts),
            use_wsl=use_wsl,
            wsl_distro=wsl_distro,
            wsl_python=wsl_python,
            wsl_env=wsl_env,
            wsl_shell_init=wsl_shell_init,
        )

        runner = StreamingCampaignRunner(
            root_path=root,
            config=config,
            on_activity=_add_activity,
            on_file_change=lambda c: _campaign_state.files_changed.append(c),
        )

        for update in runner.run_streaming():
            yield make_result(
                update["activity_log"],
                update["status_card"],
                update["cycles_table"],
                update["diff_display"],
                update.get("thinking_display", "_No thinking yet..._"),
                update["result_message"] if update["is_complete"] else "",
                update.get("chat_message", ""),
            )

    start_outputs = [
        components["activity_log"],
        components["status_card"],
        components["cycles_table"],
        components["diff_display"],
        components["thinking_display"],
        components["run_status"],
    ]
    if has_shared_activity:
        start_outputs.append(chat_components["activity_display"])
    if has_chatbot:
        start_outputs.append(chat_components["chatbot"])

    components["start_btn"].click(
        fn=on_start_campaign,
        inputs=[
            components["root_path"],
            components["max_cycles"],
            components["max_hours"],
            components["llm_model"],
            components["max_attempts"],
            components["goal"],
            components["enable_llm"],
            components["use_wsl"],
            components["wsl_distro"],
            components["wsl_python"],
            components["spark_home"],
            components["java_home"],
            components["wsl_shell_init"],
        ],
        outputs=start_outputs,
    )

    def on_stop_campaign():
        """Stop the running campaign."""
        global _campaign_state
        _campaign_state.stop_requested = True
        _add_activity("Stop requested by user", "⏹️")
        activity = _format_activity_log(_campaign_state.activity_log)
        base = (activity, "⏹️ Stop requested...")
        if has_shared_activity:
            return base + (activity,)
        return base

    stop_outputs = [components["activity_log"], components["run_status"]]
    if has_shared_activity:
        stop_outputs.append(chat_components["activity_display"])

    components["stop_btn"].click(
        fn=on_stop_campaign,
        outputs=stop_outputs,
    )

    def on_test_wsl(
        wsl_distro: str,
        wsl_python: str,
        spark_home: str,
        java_home: str,
    ):
        """Test WSL environment and report results."""
        try:
            from odibi.agents.improve.runner import check_wsl_environment

            is_ok, messages = check_wsl_environment(
                distro=wsl_distro,
                python_cmd=wsl_python,
                check_spark=True,
                spark_home=spark_home,
                java_home=java_home,
            )

            status = "✅ **Environment Ready**" if is_ok else "⚠️ **Issues Found**"
            result = f"### {status}\n\n" + "\n".join(f"- {m}" for m in messages)
            return result

        except Exception as e:
            return f"### ❌ **Test Failed**\n\nError: {e}"

    components["test_wsl_btn"].click(
        fn=on_test_wsl,
        inputs=[
            components["wsl_distro"],
            components["wsl_python"],
            components["spark_home"],
            components["java_home"],
        ],
        outputs=[components["wsl_test_result"]],
    )
