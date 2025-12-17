"""Cycle panel component for the Odibi Assistant.

Provides UI for:
- Assistant mode selection (Interactive, Guided Execution, Scheduled Assistant)
- Scheduled assistant configuration
- Cycle status display
- Morning summary view

Phase 9.A Enhancement: Real-time streaming updates during cycle execution.
Phase 9.B Enhancement: UI for AutonomousLearningScheduler.run_session().
Phase 2 Enhancement: Amp-style streaming output formatting.
"""

import queue
import threading
import time
from typing import Any, Callable, Optional

import gradio as gr

from .activity_feed import add_activity, complete_activity, error_activity, get_activity_feed
from .streaming_display import (
    format_tool_block,
    format_thinking_block,
    format_step_block,
    format_warning_block,
    format_error_block,
    format_running_banner,
    ToolBlockState,
)
from ..constants import get_tool_emoji
from ..utils import get_odibi_root


_learning_scheduler_ref = [None]

# Metadata markers to strip from LLM agent detail output
_METADATA_PREFIXES = ("Command:", "Exit Code:", "Evidence Hash:")


def _clean_agent_detail(detail: str) -> str:
    """Clean LLM agent detail output by removing verbose metadata.

    Strips lines containing command/exit code/hash metadata that clutter the UI.
    """
    if not detail:
        return ""
    lines = detail.split("\n")
    cleaned = [line for line in lines if not line.strip().startswith(_METADATA_PREFIXES)]
    return "\n".join(cleaned).strip()


def create_cycle_panel(
    on_mode_change: Optional[Callable[[str], None]] = None,
) -> tuple[gr.Column, dict[str, Any]]:
    """Create the cycle management panel.

    Args:
        on_mode_change: Callback when mode changes.

    Returns:
        Tuple of (Gradio Column, dict of component references).
    """
    components: dict[str, Any] = {}

    with gr.Column() as cycle_column:
        gr.Markdown("## 🔄 Assistant Mode")

        components["mode_selector"] = gr.Radio(
            label="Operating Mode",
            choices=[
                ("Interactive", "interactive"),
                ("Guided Execution", "guided_execution"),
                ("Scheduled Assistant", "scheduled"),
                ("🧪 Explorer", "explorer"),
            ],
            value="interactive",
            info="Interactive: Answer questions. Guided: Run cycles. Scheduled: Background work. Explorer: Sandboxed experiments.",
        )

        with gr.Accordion(
            "⏰ Scheduled Assistant Config", open=False, visible=False
        ) as scheduled_accordion:
            components["scheduled_accordion"] = scheduled_accordion

            gr.Markdown(
                """
                Configure background work cycles. The assistant can work overnight
                on bounded, finite, auditable tasks.
                """
            )

            components["scheduled_enabled"] = gr.Checkbox(
                label="Enable Scheduled Cycles",
                value=False,
            )

            components["max_cycles"] = gr.Slider(
                label="Max Cycles",
                minimum=1,
                maximum=10,
                value=1,
                step=1,
                info="Maximum number of cycles to run",
            )

            components["max_improvements"] = gr.Slider(
                label="Max Improvements per Cycle",
                minimum=1,
                maximum=10,
                value=3,
                step=1,
                info="Maximum improvements to propose per cycle",
            )

            components["max_runtime"] = gr.Slider(
                label="Max Runtime (hours)",
                minimum=1,
                maximum=24,
                value=8,
                step=1,
                info="Maximum runtime for scheduled work",
            )

            components["stop_convergence"] = gr.Checkbox(
                label="Stop on Convergence",
                value=True,
                info="Stop early if learning plateau detected",
            )

            components["project_root"] = gr.Textbox(
                label="Workspace Root",
                placeholder="d:/odibi/examples",
                info="Directory scope for scheduled cycles",
            )

            components["golden_projects"] = gr.Textbox(
                label="Golden Projects",
                value="smoke_test:smoke_test/odibi.yaml, test_odibi_local:test_odibi_local/odibi.yaml",
                placeholder="name:path, name:path",
                info="Immutable verification targets for regression checks (required for improvement mode)",
            )

            with gr.Row():
                components["start_scheduled"] = gr.Button(
                    "▶️ Start Scheduled Work",
                    variant="primary",
                )
                components["stop_scheduled"] = gr.Button(
                    "⏹️ Stop",
                    variant="stop",
                    visible=False,
                )

            components["scheduled_status"] = gr.Markdown(
                value="**Status:** Not started",
            )

        with gr.Accordion("📊 Cycle Status", open=True) as status_accordion:
            components["status_accordion"] = status_accordion

            components["cycle_status"] = gr.JSON(
                label="Current Cycle",
                value=None,
            )

            with gr.Row():
                components["refresh_status"] = gr.Button(
                    "🔄 Refresh",
                    size="sm",
                )
                components["interrupt_cycle"] = gr.Button(
                    "⏹️ Interrupt",
                    size="sm",
                    variant="stop",
                )

        with gr.Accordion("🌅 Morning Summary", open=False) as summary_accordion:
            components["summary_accordion"] = summary_accordion

            components["morning_summary"] = gr.Markdown(
                value="_No recent cycles to summarize_",
            )

            components["refresh_summary"] = gr.Button(
                "🔄 Refresh Summary",
                size="sm",
            )

    return cycle_column, components


def create_guided_execution_panel() -> tuple[gr.Column, dict[str, Any]]:
    """Create the guided execution controls panel.

    Returns:
        Tuple of (Gradio Column, dict of component references).
    """
    components: dict[str, Any] = {}

    with gr.Column(visible=False) as guided_column:
        gr.Markdown("## 🎯 Guided Execution")

        components["guided_column"] = guided_column

        with gr.Row():
            components["guided_project"] = gr.Textbox(
                label="Workspace Root",
                placeholder="d:/odibi/examples/basic",
                info="Directory scope for this cycle (agents will discover projects within)",
                scale=2,
            )

        components["guided_task"] = gr.Textbox(
            label="Task Description",
            placeholder="Validate and test the example pipeline",
            info="Describe what the cycle should accomplish",
        )

        components["guided_golden_projects"] = gr.Textbox(
            label="Golden Projects (Regression Checks)",
            value="smoke_test:smoke_test/odibi.yaml, test_odibi_local:test_odibi_local/odibi.yaml",
            placeholder="name:path, name:path",
            info="Immutable verification targets. Run after improvements to detect regressions.",
        )

        with gr.Row():
            components["guided_max_improvements"] = gr.Slider(
                label="Max Improvements",
                minimum=0,
                maximum=5,
                value=3,
                step=1,
                scale=2,
                info="0 = Learning Mode (observe only). 1+ = Improvement Mode (autonomous suggestions enabled).",
            )
            components["auto_execute"] = gr.Checkbox(
                label="Auto Execute",
                value=True,
                info="Run all steps automatically",
                scale=1,
            )

        components["mode_indicator"] = gr.Markdown(
            value="**Mode:** 🔧 Improvement (Autonomous Suggestions Enabled)",
        )

        with gr.Row():
            components["start_cycle"] = gr.Button(
                "▶️ Start Cycle",
                variant="primary",
            )
            components["continue_cycle"] = gr.Button(
                "⏭️ Next Step",
                visible=False,
            )
            components["stop_cycle"] = gr.Button(
                "⏹️ Stop",
                variant="stop",
                visible=False,
            )

        components["step_progress"] = gr.Markdown(
            value="Not started",
        )

        with gr.Accordion("📚 Learning Session (Long-Running)", open=False) as learning_accordion:
            components["learning_accordion"] = learning_accordion

            gr.Markdown(
                """
                **Observation-Only Mode** — Run extended learning sessions without code changes.

                ⚠️ **This is a long-running operation** (default: up to 168 hours / 100 cycles).
                The session will observe, analyze, and index patterns from your codebase.

                **Safety Guarantees:**
                - ❌ No code modifications will be made
                - ❌ ImprovementAgent is NEVER invoked
                - ❌ ReviewAgent is NEVER invoked
                - ✅ Disk guards enforce storage budgets
                - ✅ Heartbeat file updated for external monitoring
                - ✅ Session survives UI refresh (read heartbeat to reconnect)
                """
            )

            components["learning_project_root"] = gr.Textbox(
                label="Project Root",
                placeholder="d:/odibi/examples",
                info="Directory to analyze (required)",
            )

            with gr.Row():
                components["learning_profile_dropdown"] = gr.Dropdown(
                    label="Cycle Profile",
                    choices=[],
                    value=None,
                    info="Select a profile from .odibi/cycle_profiles/",
                    interactive=True,
                    scale=2,
                )
                components["refresh_profiles"] = gr.Button(
                    "🔄",
                    size="sm",
                    scale=0,
                )

            components["learning_profile_info"] = gr.Markdown(
                value="_Select a profile to see its details_",
            )

            with gr.Row():
                components["learning_max_cycles"] = gr.Slider(
                    label="Max Cycles",
                    minimum=1,
                    maximum=500,
                    value=100,
                    step=1,
                    info="Maximum learning cycles to run",
                )
                components["learning_max_hours"] = gr.Slider(
                    label="Max Wall-Clock Hours",
                    minimum=1,
                    maximum=336,
                    value=168,
                    step=1,
                    info="Maximum runtime (168h = 1 week, 336h = 2 weeks)",
                )

            with gr.Row():
                components["start_learning_session"] = gr.Button(
                    "▶️ Start Learning Session",
                    variant="primary",
                )
                components["stop_learning_session"] = gr.Button(
                    "⏹️ Stop Session",
                    variant="stop",
                    visible=False,
                )
                components["refresh_learning_status"] = gr.Button(
                    "🔄 Refresh Status",
                    size="sm",
                )

            with gr.Group():
                gr.Markdown("### 📊 Session Monitor (Read-Only)")

                components["learning_session_state"] = gr.Markdown(
                    value="**State:** `NOT_STARTED`",
                )

                with gr.Row():
                    components["learning_session_id"] = gr.Textbox(
                        label="Session ID",
                        value="—",
                        interactive=False,
                        scale=1,
                    )
                    components["learning_cycles_completed"] = gr.Textbox(
                        label="Cycles Completed",
                        value="0",
                        interactive=False,
                        scale=1,
                    )
                    components["learning_cycles_failed"] = gr.Textbox(
                        label="Cycles Failed",
                        value="0",
                        interactive=False,
                        scale=1,
                    )

                with gr.Row():
                    components["learning_last_heartbeat"] = gr.Textbox(
                        label="Last Heartbeat",
                        value="—",
                        interactive=False,
                        scale=1,
                    )
                    components["learning_last_cycle_id"] = gr.Textbox(
                        label="Last Cycle ID",
                        value="—",
                        interactive=False,
                        scale=1,
                    )
                    components["learning_last_status"] = gr.Textbox(
                        label="Last Status",
                        value="—",
                        interactive=False,
                        scale=1,
                    )

                components["learning_disk_usage"] = gr.Markdown(
                    value="**Disk Usage:** _Not available_",
                )

        with gr.Accordion(
            "🔧 Controlled Improvement (Single-Shot)", open=False
        ) as improvement_accordion:
            components["improvement_accordion"] = improvement_accordion

            gr.Markdown(
                """
                **Controlled Improvement Mode** — Apply a single, scoped fix to a specific file.

                ⚠️ **Phase 9.G** — Improvements are earned, not assumed.

                **Safety Guarantees:**
                - ✅ Only ONE improvement per cycle
                - ✅ Only the scoped file can be modified
                - ✅ Automatic rollback on failure
                - ✅ Before/after snapshots captured
                - ✅ Golden projects tested for regressions
                - ❌ No multi-file refactors
                - ❌ No heuristic optimization
                """
            )

            components["improvement_project_root"] = gr.Textbox(
                label="Project Root",
                placeholder="d:/odibi/.odibi/learning_harness",
                info="Directory containing the pipeline configs",
            )

            components["improvement_target_file"] = gr.Textbox(
                label="Target File (Improvement Scope)",
                placeholder="scale_join.odibi.yaml",
                info="Only this file can be modified by the improvement",
            )

            components["improvement_issue_description"] = gr.Textbox(
                label="Issue Description (What to Fix)",
                placeholder="e.g., The deduplicate transformer is missing order_by, causing non-deterministic results",
                info="Describe the specific issue to fix. This guides the agent.",
                lines=2,
            )

            components["improvement_golden_projects"] = gr.Textbox(
                label="Golden Projects (Regression Checks)",
                value="skew_test:skew_test.odibi.yaml, schema_drift:schema_drift.odibi.yaml",
                placeholder="name:path, name:path",
                info="These must remain passing after the improvement. Paths auto-resolve to .odibi/learning_harness/ if not found elsewhere.",
            )

            with gr.Row():
                components["improvement_rollback_on_failure"] = gr.Checkbox(
                    label="Auto Rollback on Failure",
                    value=True,
                    info="Automatically restore original file if improvement fails",
                )
                components["improvement_require_regression"] = gr.Checkbox(
                    label="Require Regression Check",
                    value=True,
                    info="Golden projects must pass after improvement",
                )

            with gr.Row():
                components["start_controlled_improvement"] = gr.Button(
                    "🔧 Start Controlled Improvement",
                    variant="primary",
                )
                components["stop_controlled_improvement"] = gr.Button(
                    "⏹️ Stop",
                    variant="stop",
                    visible=False,
                )

            components["improvement_status"] = gr.Markdown(
                value="**Status:** Not started",
            )

            with gr.Accordion("📋 Improvement Result", open=False) as improvement_result_accordion:
                components["improvement_result_accordion"] = improvement_result_accordion
                components["improvement_result"] = gr.JSON(
                    label="Improvement Details",
                    value=None,
                )

    return guided_column, components


_stop_requested = threading.Event()


def setup_cycle_handlers(
    cycle_components: dict[str, Any],
    guided_components: dict[str, Any],
    get_runner: Callable,
    chat_components: Optional[dict[str, Any]] = None,
    discovery_components: Optional[dict[str, Any]] = None,
    explorer_components: Optional[dict[str, Any]] = None,
) -> None:
    """Set up event handlers for cycle panels.

    Args:
        cycle_components: Components from create_cycle_panel.
        guided_components: Components from create_guided_execution_panel.
        get_runner: Function to get AgentRunner instance.
        chat_components: Optional chat components for activity feed updates.
        discovery_components: Optional issue discovery panel components.
        explorer_components: Optional explorer panel components.
    """
    global _stop_requested

    def on_mode_change(mode: str):
        is_scheduled = mode == "scheduled"
        is_guided = mode == "guided_execution"
        is_explorer = mode == "explorer"
        results = [
            gr.update(visible=is_scheduled),
            gr.update(visible=is_guided),
        ]
        if discovery_components:
            results.append(gr.update(visible=is_guided))
        if explorer_components:
            results.append(gr.update(visible=is_explorer))
        return tuple(results)

    mode_change_outputs = [
        cycle_components["scheduled_accordion"],
        guided_components["guided_column"],
    ]
    if discovery_components and "discovery_column" in discovery_components:
        mode_change_outputs.append(discovery_components["discovery_column"])
    if explorer_components and "explorer_column" in explorer_components:
        mode_change_outputs.append(explorer_components["explorer_column"])

    cycle_components["mode_selector"].change(
        fn=on_mode_change,
        inputs=[cycle_components["mode_selector"]],
        outputs=mode_change_outputs,
    )

    def on_max_improvements_change(value: int) -> str:
        if value == 0:
            return "**Mode:** 🔍 Learning (Observation Only)"
        return "**Mode:** 🔧 Improvement (Autonomous Suggestions Enabled)"

    guided_components["guided_max_improvements"].change(
        fn=on_max_improvements_change,
        inputs=[guided_components["guided_max_improvements"]],
        outputs=[guided_components["mode_indicator"]],
    )

    def load_available_profiles():
        """Load list of available profiles from .odibi/cycle_profiles/."""
        try:
            from odibi.agents.core.cycle_profile import CycleProfileLoader

            runner = get_runner()
            config = runner.config if runner and hasattr(runner, "config") else None
            odibi_root = get_odibi_root(config)
            loader = CycleProfileLoader(odibi_root)
            profiles = loader.list_profiles()
            if profiles:
                return gr.update(choices=profiles, value=profiles[0])
            return gr.update(choices=[], value=None)
        except Exception:
            return gr.update(choices=[], value=None)

    def get_profile_info(profile_name: str):
        """Get profile summary for display."""
        if not profile_name:
            return "_Select a profile to see its details_"

        try:
            from odibi.agents.core.cycle_profile import CycleProfileLoader

            runner = get_runner()
            config = runner.config if runner and hasattr(runner, "config") else None
            odibi_root = get_odibi_root(config)
            loader = CycleProfileLoader(odibi_root)
            summary = loader.get_profile_summary(profile_name)

            lines = [
                f"### 📋 {summary.get('profile_name', profile_name)}",
                "",
                f"**ID:** `{summary.get('profile_id', 'N/A')}`",
                f"**Hash:** `{summary.get('content_hash', 'N/A')}`",
                "",
                summary.get("description", "_No description_"),
            ]
            return "\n".join(lines)
        except Exception as e:
            return f"⚠️ Error loading profile: {e}"

    if "learning_profile_dropdown" in guided_components:
        guided_components["learning_profile_dropdown"].change(
            fn=get_profile_info,
            inputs=[guided_components["learning_profile_dropdown"]],
            outputs=[guided_components["learning_profile_info"]],
        )

    if "refresh_profiles" in guided_components:
        guided_components["refresh_profiles"].click(
            fn=load_available_profiles,
            outputs=[guided_components["learning_profile_dropdown"]],
        )

    def refresh_status():
        try:
            runner = get_runner()
            if runner:
                status = runner.get_cycle_status()
                return status
        except Exception as e:
            return {"error": str(e)}
        return None

    cycle_components["refresh_status"].click(
        fn=refresh_status,
        outputs=[cycle_components["cycle_status"]],
    )

    def interrupt_cycle():
        try:
            runner = get_runner()
            if runner:
                state = runner.interrupt_current_cycle("User interrupted via UI")
                if state:
                    return {"interrupted": True, "cycle_id": state.cycle_id}
        except Exception as e:
            return {"error": str(e)}
        return {"message": "No active cycle to interrupt"}

    cycle_components["interrupt_cycle"].click(
        fn=interrupt_cycle,
        outputs=[cycle_components["cycle_status"]],
    )

    def refresh_summary():
        try:
            runner = get_runner()
            if runner:
                summary = runner.get_morning_summary()
                return format_morning_summary(summary)
        except Exception as e:
            return f"Error loading summary: {e}"
        return "_No recent cycles_"

    cycle_components["refresh_summary"].click(
        fn=refresh_summary,
        outputs=[cycle_components["morning_summary"]],
    )

    _scheduled_stop_requested = threading.Event()

    def start_scheduled_work(
        enabled: bool,
        max_cycles: int,
        max_improvements: int,
        max_runtime: float,
        stop_convergence: bool,
        project_root: str,
        golden_projects_str: str,
        history: list,
    ):
        """Streaming generator for scheduled work execution."""
        _scheduled_stop_requested.clear()

        history = list(history) if history else []
        history.append({"role": "user", "content": "⏰ Start Scheduled Work"})

        def make_status(text: str):
            return f"**Status:** {text}"

        if not enabled:
            history.append({"role": "assistant", "content": "❌ Scheduled cycles not enabled"})
            yield (
                make_status("Not enabled"),
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=False),
            )
            return

        if not project_root or not project_root.strip():
            history.append({"role": "assistant", "content": "❌ Please specify a workspace root"})
            yield (
                make_status("Error: Workspace root required"),
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=False),
            )
            return

        try:
            runner = get_runner()
            if not runner:
                history.append({"role": "assistant", "content": "❌ Runner not initialized"})
                yield (
                    make_status("Error: Runner not initialized"),
                    get_activity_feed().format_markdown(),
                    history,
                    gr.update(visible=False),
                )
                return

            event_queue = queue.Queue()
            history.append(
                {
                    "role": "assistant",
                    "content": f"🚀 Starting scheduled work for `{project_root}`...",
                }
            )
            history.append(
                {
                    "role": "assistant",
                    "content": f"ℹ️ Max cycles: {int(max_cycles)}, Max improvements: {int(max_improvements)}, Max hours: {max_runtime}",
                }
            )
            if int(max_improvements) == 0:
                history.append(
                    {"role": "assistant", "content": "ℹ️ **Learning Mode** — observation only"}
                )
            else:
                history.append(
                    {
                        "role": "assistant",
                        "content": f"ℹ️ **Improvement Mode** — up to {int(max_improvements)} suggestions per cycle",
                    }
                )
            yield (
                make_status("Initializing..."),
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=True),
            )

            runner.cycle_runner.set_event_callback(_create_streaming_callback(event_queue, history))

            work_complete = [False]
            work_error = [None]
            cycles_completed = [0]

            def run_scheduled_thread():
                """Run scheduled cycles in background thread."""
                try:
                    for cycle_num in range(int(max_cycles)):
                        if _scheduled_stop_requested.is_set():
                            break

                        state = runner.start_guided_cycle(
                            project_root=project_root.strip(),
                            task_description=f"Scheduled cycle {cycle_num + 1}/{max_cycles}",
                            max_improvements=int(max_improvements),
                            golden_projects=golden_projects_str,
                        )

                        while not state.completed and not _scheduled_stop_requested.is_set():
                            status = runner.get_cycle_status()
                            if not status or status.get("completed"):
                                break
                            state = runner.continue_cycle(status["cycle_id"])

                        cycles_completed[0] += 1

                        if stop_convergence and hasattr(state, "exit_status"):
                            if state.exit_status == "CONVERGED":
                                break

                    work_complete[0] = True
                    event_queue.put(("done", None))
                except Exception as e:
                    work_error[0] = e
                    event_queue.put(("error", e))

            thread = threading.Thread(target=run_scheduled_thread, daemon=True)
            thread.start()

            last_status = "Running..."

            while not work_complete[0] and work_error[0] is None:
                if _scheduled_stop_requested.is_set():
                    history.append({"role": "assistant", "content": "⏹️ Stop requested..."})

                try:
                    event_type, event = event_queue.get(timeout=0.1)

                    if event_type == "done":
                        break
                    elif event_type == "error":
                        raise event
                    elif event_type == "thinking":
                        last_status = f"🧠 {event.agent_role} thinking..."
                    elif event_type == "response":
                        last_status = f"✓ {event.agent_role}"
                    elif event_type == "complete":
                        last_status = f"Cycle {cycles_completed[0]} complete"
                    elif event_type == "update":
                        last_status = event.message if hasattr(event, "message") else "Working..."
                    elif event_type == "progress":
                        last_status = f"  📍 {event.message if hasattr(event, 'message') else 'Processing...'}"

                    yield (
                        make_status(last_status),
                        get_activity_feed().format_markdown(),
                        history,
                        gr.update(visible=True),
                    )

                except queue.Empty:
                    yield (
                        make_status(f"{last_status} ⏳"),
                        get_activity_feed().format_markdown(),
                        history,
                        gr.update(visible=True),
                    )

            thread.join(timeout=10.0)

            if work_error[0]:
                raise work_error[0]

            final_msg = f"✅ Scheduled work complete — {cycles_completed[0]} cycles"
            history.append({"role": "assistant", "content": final_msg})
            yield (
                make_status(final_msg),
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=False),
            )

        except Exception as e:
            history.append(
                {"role": "assistant", "content": f"❌ **Error:** {type(e).__name__}: {e}"}
            )
            yield (
                make_status(f"Error: {e}"),
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=False),
            )

    scheduled_outputs = [
        cycle_components["scheduled_status"],
    ]
    if chat_components and "activity_display" in chat_components:
        scheduled_outputs.append(chat_components["activity_display"])
    if chat_components and "chatbot" in chat_components:
        scheduled_outputs.append(chat_components["chatbot"])
    scheduled_outputs.append(cycle_components["stop_scheduled"])

    cycle_components["start_scheduled"].click(
        fn=start_scheduled_work,
        inputs=[
            cycle_components["scheduled_enabled"],
            cycle_components["max_cycles"],
            cycle_components["max_improvements"],
            cycle_components["max_runtime"],
            cycle_components["stop_convergence"],
            cycle_components["project_root"],
            cycle_components["golden_projects"],
            chat_components["chatbot"] if chat_components else gr.State([]),
        ],
        outputs=scheduled_outputs,
    )

    def stop_scheduled_work():
        _scheduled_stop_requested.set()
        return "**Status:** ⏹️ Stop requested..."

    cycle_components["stop_scheduled"].click(
        fn=stop_scheduled_work,
        outputs=[cycle_components["scheduled_status"]],
    )

    def _create_streaming_callback(event_queue: queue.Queue, chat_history_ref: list):
        """Create a callback that pushes events to a queue for streaming updates.

        This allows the UI to update in real-time as the cycle executes,
        providing an Amp-style visual experience with:
        - Collapsible tool outputs
        - In-place message updates
        - Visual tool blocks with status indicators
        - Nested subprocess output
        """
        _activity_ids = {}
        _step_index = [0]
        _total_steps = [5]  # Approximate number of steps in a cycle
        _live_message_idx = [None]  # Track the "live" message index for in-place updates
        _current_tool_state = [None]  # Current ToolBlockState for subprocess lines
        _step_start_times = {}
        _session_start = [time.time()]  # Track overall session start
        _running_banner_idx = [None]  # Track the running banner message index
        _current_step_name = [""]  # Current step being executed

        def _update_live_message(content: str) -> None:
            """Update the live message in-place or append if none exists."""
            if _live_message_idx[0] is not None and _live_message_idx[0] < len(chat_history_ref):
                chat_history_ref[_live_message_idx[0]]["content"] = content
            else:
                chat_history_ref.append({"role": "assistant", "content": content})
                _live_message_idx[0] = len(chat_history_ref) - 1

        def _finalize_live_message() -> None:
            """End the live message and reset tracking."""
            _live_message_idx[0] = None
            _current_tool_state[0] = None

        def _update_running_banner(step_info: str = "") -> None:
            """Update or create the running banner."""
            elapsed = time.time() - _session_start[0]
            banner = format_running_banner(
                message="Session running",
                elapsed_seconds=elapsed,
                step_info=step_info or _current_step_name[0],
            )
            if _running_banner_idx[0] is not None and _running_banner_idx[0] < len(
                chat_history_ref
            ):
                chat_history_ref[_running_banner_idx[0]]["content"] = banner
            else:
                chat_history_ref.append({"role": "assistant", "content": banner})
                _running_banner_idx[0] = len(chat_history_ref) - 1

        def _remove_running_banner() -> None:
            """Remove the running banner when session ends."""
            if _running_banner_idx[0] is not None and _running_banner_idx[0] < len(
                chat_history_ref
            ):
                # Use invisible placeholder instead of empty string to avoid visible gap
                chat_history_ref[_running_banner_idx[0]][
                    "content"
                ] = '<div style="display:none"></div>'
            _running_banner_idx[0] = None

        def callback(event):
            if event.event_type == "step_start":
                _finalize_live_message()
                aid = add_activity(event.message, tool_name="cycle")
                _activity_ids[event.step] = aid
                _step_start_times[event.step] = time.time()
                _current_step_name[0] = event.message

                _update_running_banner(event.message)

                step_block = format_step_block(
                    step_name=event.message,
                    step_index=_step_index[0],
                    total_steps=_total_steps[0],
                    status="running",
                    agent_role=getattr(event, "agent_role", None),
                )
                chat_history_ref.append({"role": "assistant", "content": step_block})

                if event.step == "project_selection":
                    chat_history_ref.append(
                        {
                            "role": "assistant",
                            "content": '<div style="padding: 8px 12px; color: #8b949e; font-size: 13px;">ℹ️ ProjectAgent analyzing coverage — may suggest focus areas within workspace</div>',
                        }
                    )

                _step_index[0] += 1
                event_queue.put(("update", event))

            elif event.event_type == "step_skip":
                _finalize_live_message()
                add_activity(event.message, tool_name="cycle")

                step_block = format_step_block(
                    step_name=event.message,
                    step_index=_step_index[0],
                    total_steps=_total_steps[0],
                    status="skipped",
                )
                chat_history_ref.append({"role": "assistant", "content": step_block})

                if event.step == "project_selection":
                    chat_history_ref.append(
                        {
                            "role": "assistant",
                            "content": '<div style="padding: 8px 12px; color: #8b949e; font-size: 13px;">ℹ️ ProjectAgent skipped — using user-provided workspace directly (Learning Mode)</div>',
                        }
                    )

                _step_index[0] += 1
                event_queue.put(("update", event))

            elif event.event_type == "agent_thinking":
                _finalize_live_message()
                add_activity(event.message, tool_name=event.agent_role)

                _update_running_banner(f"{event.agent_role} thinking...")

                thinking_block = format_thinking_block(
                    agent_role=event.agent_role,
                    message="",
                    is_active=True,
                )
                chat_history_ref.append({"role": "assistant", "content": thinking_block})
                _live_message_idx[0] = len(chat_history_ref) - 1

                event_queue.put(("thinking", event))

            elif event.event_type == "agent_response":
                aid = _activity_ids.get(event.step)
                elapsed = time.time() - _step_start_times.get(event.step, time.time())

                if aid:
                    if event.is_error:
                        error_activity(aid, event.message)
                    else:
                        complete_activity(aid, event.message)

                # Clear the thinking block (it's at _live_message_idx if set)
                if _live_message_idx[0] is not None and _live_message_idx[0] < len(
                    chat_history_ref
                ):
                    chat_history_ref[_live_message_idx[0]][
                        "content"
                    ] = '<div style="display:none"></div>'

                if event.is_error:
                    content = format_error_block(
                        message=f"{event.agent_role} failed",
                        detail=event.detail[:2000] if event.detail else "",
                    )
                else:
                    emoji = (
                        get_tool_emoji(event.agent_role.lower().replace(" ", "_"))
                        if event.agent_role
                        else "📤"
                    )
                    detail_text = event.detail[:2000] if event.detail else ""
                    has_more = len(event.detail or "") > 2000

                    content = format_tool_block(
                        tool_name=event.agent_role,
                        emoji=emoji,
                        status="success",
                        elapsed_seconds=elapsed,
                        stdout=detail_text + ("..." if has_more else "") if detail_text else "",
                    )

                _finalize_live_message()
                chat_history_ref.append({"role": "assistant", "content": content})
                event_queue.put(("response", event))

            elif event.event_type == "warning":
                _finalize_live_message()
                add_activity(f"⚠️ {event.message}", tool_name="system")
                content = format_warning_block(
                    event.message, event.detail if hasattr(event, "detail") else ""
                )
                chat_history_ref.append({"role": "assistant", "content": content})
                event_queue.put(("warning", event))

            elif event.event_type == "cycle_end":
                _finalize_live_message()
                _remove_running_banner()
                add_activity(event.message, tool_name="cycle")

                is_error = getattr(event, "is_error", False)
                status_class = "completed" if not is_error else ""
                status_icon = "✅" if not is_error else "❌"
                content = f"""<div class="step-block {status_class}">
<div class="step-header">
<span class="step-title">{status_icon} {event.message}</span>
<span class="step-counter">Complete</span>
</div>
<div class="step-progress"><div class="step-progress-bar" style="width: 100%;"></div></div>
</div>"""
                chat_history_ref.append({"role": "assistant", "content": content})
                _step_index[0] = 0
                _current_step_name[0] = ""
                event_queue.put(("complete", event))

            elif event.event_type in ("disk_cleanup", "report_written", "report_error"):
                _finalize_live_message()
                emoji = "📝" if event.event_type != "report_error" else "❌"
                content = f'<div style="padding: 10px 16px; background: #0d1117; border: 1px solid #21262d; border-radius: 8px; margin: 8px 0; color: #7d8590; font-size: 13px;">{emoji} {event.message}</div>'
                chat_history_ref.append({"role": "assistant", "content": content})
                event_queue.put(("update", event))

            elif event.event_type == "agent_progress":
                detail = event.detail if hasattr(event, "detail") and event.detail else ""

                _update_running_banner(event.message[:50])

                if _live_message_idx[0] is not None and _live_message_idx[0] < len(
                    chat_history_ref
                ):
                    current = chat_history_ref[_live_message_idx[0]]["content"]
                    progress_line = f'<div class="subprocess-line stdout">  ↳ {event.message}</div>'
                    if detail:
                        progress_line += f'<div class="subprocess-line" style="padding-left: 24px; color: #6e7681;">{detail[:200]}</div>'

                    if '<div class="subprocess-output' in current:
                        insert_pos = current.rfind("</div></div>")
                        if insert_pos > 0:
                            updated = current[:insert_pos] + progress_line + current[insert_pos:]
                            _update_live_message(updated)
                    else:
                        subprocess_container = (
                            f'<div class="subprocess-output running">{progress_line}</div>'
                        )
                        _update_live_message(current + subprocess_container)
                else:
                    content = f'<div class="subprocess-output"><div class="subprocess-line stdout">  ↳ {event.message}</div></div>'
                    chat_history_ref.append({"role": "assistant", "content": content})

                event_queue.put(("progress", event))

        return callback

    def start_guided_cycle(
        project: str,
        task: str,
        golden_projects_str: str,
        max_improvements: int,
        auto_execute: bool,
        history: list,
    ):
        """Streaming generator for guided cycle execution.

        Yields updates after each event so the UI updates in real-time,
        providing an experience similar to watching an AI assistant work.
        """
        global _stop_requested
        _stop_requested.clear()

        history = list(history) if history else []
        history.append({"role": "user", "content": f"🎯 Start cycle: {task or 'Guided execution'}"})

        def make_output(status_dict, progress_text):
            return (
                status_dict,
                progress_text,
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=False),
                gr.update(visible=True),
            )

        if not project:
            history.append({"role": "assistant", "content": "❌ Please specify a project root"})
            yield (
                None,
                "Please specify a project root",
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=False),
                gr.update(visible=False),
            )
            return

        try:
            runner = get_runner()
            if not runner:
                history.append({"role": "assistant", "content": "❌ Runner not initialized"})
                yield make_output(
                    {"error": "Runner not initialized"}, "Error: Runner not initialized"
                )
                return

            event_queue = queue.Queue()
            history.append(
                {"role": "assistant", "content": f"🚀 Starting cycle for `{project}`..."}
            )
            yield make_output({"status": "starting"}, "Initializing...")

            history.append(
                {"role": "assistant", "content": f"ℹ️ Workspace scope set to: `{project}`"}
            )
            if int(max_improvements) == 0:
                history.append(
                    {
                        "role": "assistant",
                        "content": "ℹ️ **Learning Mode** — observation only, no code changes",
                    }
                )
            else:
                history.append(
                    {
                        "role": "assistant",
                        "content": f"ℹ️ **Improvement Mode** — up to {int(max_improvements)} autonomous suggestions enabled",
                    }
                )
            yield make_output({"status": "configuring"}, "Configuring...")

            if golden_projects_str and golden_projects_str.strip():
                history.append(
                    {
                        "role": "assistant",
                        "content": f"ℹ️ **Golden Projects:** `{golden_projects_str}` (regression checks enabled)",
                    }
                )
                yield make_output({"status": "configuring"}, "Golden projects configured...")

            runner.cycle_runner.set_event_callback(_create_streaming_callback(event_queue, history))

            cycle_complete = [False]
            cycle_state = [None]
            cycle_error = [None]

            def run_cycle_thread():
                """Run cycle in background thread."""
                try:
                    state = runner.start_guided_cycle(
                        project_root=project,
                        task_description=task or "Guided execution cycle",
                        max_improvements=int(max_improvements),
                        golden_projects=golden_projects_str,
                    )
                    cycle_state[0] = state

                    if auto_execute:
                        while not state.completed and not _stop_requested.is_set():
                            status = runner.get_cycle_status()
                            if not status or status.get("completed"):
                                break
                            state = runner.continue_cycle(status["cycle_id"])
                            cycle_state[0] = state

                    cycle_complete[0] = True
                    event_queue.put(("done", None))

                except Exception as e:
                    cycle_error[0] = e
                    event_queue.put(("error", e))

            thread = threading.Thread(target=run_cycle_thread, daemon=True)
            thread.start()

            last_progress = "Starting..."
            step_count = 0

            while not cycle_complete[0] and cycle_error[0] is None:
                try:
                    event_type, event = event_queue.get(timeout=0.1)

                    if event_type == "done":
                        break
                    elif event_type == "error":
                        raise event
                    elif event_type == "thinking":
                        last_progress = f"🧠 {event.agent_role} thinking..."
                    elif event_type == "response":
                        step_count += 1
                        last_progress = f"Step {step_count}: {event.agent_role} ✓"
                    elif event_type == "update":
                        last_progress = f"Step {step_count + 1}: {event.message}"
                    elif event_type == "progress":
                        last_progress = f"  📍 {event.message}"
                    elif event_type == "complete":
                        last_progress = "✅ Cycle complete"

                    state = cycle_state[0]
                    status = {"status": "running", "step": step_count}
                    if state:
                        status["cycle_id"] = state.cycle_id
                        status["completed"] = state.completed

                    yield (
                        status,
                        last_progress,
                        get_activity_feed().format_markdown(),
                        history,
                        gr.update(visible=False),
                        gr.update(visible=not cycle_complete[0]),
                    )

                except queue.Empty:
                    yield (
                        {"status": "running", "step": step_count},
                        f"{last_progress} ⏳",
                        get_activity_feed().format_markdown(),
                        history,
                        gr.update(visible=False),
                        gr.update(visible=True),
                    )

            thread.join(timeout=5.0)

            if cycle_error[0]:
                raise cycle_error[0]

            state = cycle_state[0]
            if _stop_requested[0]:
                history.append({"role": "assistant", "content": "⏹️ Cycle stopped by user."})

            if state:
                final_status = (
                    "✅ Cycle complete"
                    if state.completed
                    else f"Step {state.current_step_index}/{len(state.steps)}"
                )
                yield (
                    {"cycle_id": state.cycle_id, "completed": state.completed},
                    final_status,
                    get_activity_feed().format_markdown(),
                    history,
                    gr.update(visible=not auto_execute and not state.completed),
                    gr.update(visible=False),
                )
            else:
                yield make_output({"status": "unknown"}, "Cycle state unknown")

        except Exception as e:
            history.append(
                {"role": "assistant", "content": f"❌ **Error:** {type(e).__name__}: {e}"}
            )
            yield (
                {"error": str(e)},
                f"Error: {e}",
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=False),
                gr.update(visible=False),
            )

    start_outputs = [
        cycle_components["cycle_status"],
        guided_components["step_progress"],
    ]
    if chat_components and "activity_display" in chat_components:
        start_outputs.append(chat_components["activity_display"])
    if chat_components and "chatbot" in chat_components:
        start_outputs.append(chat_components["chatbot"])
    start_outputs.append(guided_components["continue_cycle"])
    start_outputs.append(guided_components["stop_cycle"])

    guided_components["start_cycle"].click(
        fn=start_guided_cycle,
        inputs=[
            guided_components["guided_project"],
            guided_components["guided_task"],
            guided_components["guided_golden_projects"],
            guided_components["guided_max_improvements"],
            guided_components["auto_execute"],
            chat_components["chatbot"] if chat_components else gr.State([]),
        ],
        outputs=start_outputs,
    )

    def stop_cycle():
        global _stop_requested
        _stop_requested.set()
        return "⏹️ Stop requested..."

    guided_components["stop_cycle"].click(
        fn=stop_cycle,
        outputs=[guided_components["step_progress"]],
    )

    def continue_guided_cycle(history: list):
        history = list(history) if history else []
        try:
            runner = get_runner()
            if runner:
                status = runner.get_cycle_status()
                if status and not status.get("completed"):
                    state = runner.continue_cycle(status["cycle_id"])
                    step = state.current_step()
                    step_name = step.value if step else "complete"
                    return (
                        runner.get_cycle_status(),
                        f"Step {state.current_step_index}/{len(state.steps)}: {step_name}",
                        get_activity_feed().format_markdown(),
                        history,
                        gr.update(visible=not state.completed),
                        gr.update(visible=not state.completed),
                    )
        except Exception as e:
            history.append({"role": "assistant", "content": f"❌ Error: {e}"})
            return (
                {"error": str(e)},
                f"Error: {e}",
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=False),
                gr.update(visible=False),
            )
        return (
            None,
            "No active cycle",
            get_activity_feed().format_markdown(),
            history,
            gr.update(visible=False),
            gr.update(visible=False),
        )

    continue_outputs = [
        cycle_components["cycle_status"],
        guided_components["step_progress"],
    ]
    if chat_components and "activity_display" in chat_components:
        continue_outputs.append(chat_components["activity_display"])
    if chat_components and "chatbot" in chat_components:
        continue_outputs.append(chat_components["chatbot"])
    continue_outputs.append(guided_components["continue_cycle"])
    continue_outputs.append(guided_components["stop_cycle"])

    guided_components["continue_cycle"].click(
        fn=continue_guided_cycle,
        inputs=[chat_components["chatbot"] if chat_components else gr.State([])],
        outputs=continue_outputs,
    )

    _learning_session_stop_requested = threading.Event()

    def _create_learning_session_callback(event_queue: queue.Queue, chat_history_ref: list):
        """Create a callback for learning session events.

        Similar to the cycle callback but tuned for long-running sessions.
        Uses Amp-style HTML blocks for visual polish.
        """
        _activity_ids = {}
        _step_index = [0]
        _step_start_times = {}
        _session_start = [time.time()]
        _current_tool_state = [None]  # ToolBlockState for accumulating subprocess lines
        _tool_block_idx = [None]  # Index of the tool block message in chat history
        _cycle_banner_idx = [None]  # Index of the cycle running banner to update on end
        _last_heartbeat = [time.time()]

        def callback(event):
            if event.event_type == "learning_session_start":
                aid = add_activity(event.message, tool_name="learning")
                _activity_ids["session"] = aid
                _session_start[0] = time.time()

                content = f"""<div class="step-block running">
<div class="step-header">
<span class="step-title">📚 {event.message}</span>
<span class="step-counter">Session</span>
</div>
</div>"""
                chat_history_ref.append({"role": "assistant", "content": content})

                if event.detail:
                    info_content = f'<div style="padding: 10px 16px; background: #0d1117; border: 1px solid #21262d; border-radius: 8px; margin: 8px 0; color: #7d8590; font-size: 13px;">ℹ️ {event.detail}</div>'
                    chat_history_ref.append({"role": "assistant", "content": info_content})
                event_queue.put(("session_start", event))

            elif event.event_type == "learning_cycle_start":
                aid = add_activity(event.message, tool_name="learning")
                _activity_ids["cycle"] = aid
                _step_index[0] = 0

                banner = format_running_banner(
                    message="Learning cycle",
                    elapsed_seconds=time.time() - _session_start[0],
                    step_info="Starting",
                )
                chat_history_ref.append({"role": "assistant", "content": banner})
                _cycle_banner_idx[0] = len(chat_history_ref) - 1
                event_queue.put(("cycle_start", event))

            elif event.event_type == "learning_cycle_end":
                aid = _activity_ids.get("cycle")
                if aid:
                    if event.is_error:
                        error_activity(aid, event.message)
                    else:
                        complete_activity(aid, event.message)

                is_error = getattr(event, "is_error", False)
                status_class = "completed" if not is_error else "error"
                status_icon = "✅" if not is_error else "❌"
                elapsed = time.time() - _session_start[0]
                detail_html = f" — {event.detail}" if event.detail else ""

                content = f"""<div class="step-block {status_class}">
<div class="step-header">
<span class="step-title">{status_icon} {event.message}{detail_html}</span>
<span class="step-counter">{elapsed:.0f}s</span>
</div>
<div class="step-progress"><div class="step-progress-bar" style="width: 100%;"></div></div>
</div>"""
                # Update the running banner in-place instead of appending new message
                if _cycle_banner_idx[0] is not None and _cycle_banner_idx[0] < len(
                    chat_history_ref
                ):
                    chat_history_ref[_cycle_banner_idx[0]]["content"] = content
                else:
                    chat_history_ref.append({"role": "assistant", "content": content})
                _cycle_banner_idx[0] = None
                event_queue.put(("cycle_end", event))

            elif event.event_type in ("step_start", "step_skip"):
                _step_start_times[event.step] = time.time()
                is_skip = event.event_type == "step_skip"
                status = "skipped" if is_skip else "running"
                _step_index[0] += 1

                aid = add_activity(event.message, tool_name="cycle")
                _activity_ids[event.step] = aid

                status_class = "running" if status == "running" else ""

                step_block = f"""<div class="step-block {status_class}">
<div class="step-header">
<span class="step-title">{event.message}</span>
<span class="step-counter">Step {_step_index[0]}</span>
</div>
<div class="step-progress"><div class="step-progress-bar running-bar"></div></div>
</div>"""
                chat_history_ref.append({"role": "assistant", "content": step_block})
                event_queue.put(("step", event))

            elif event.event_type == "agent_thinking":
                add_activity(event.message, tool_name=event.agent_role)
                emoji = (
                    get_tool_emoji(event.agent_role.lower().replace(" ", "_"))
                    if event.agent_role
                    else "🔧"
                )
                _current_tool_state[0] = ToolBlockState(
                    tool_name=event.agent_role or "Agent",
                    emoji=emoji,
                    status="running",
                )
                chat_history_ref.append(
                    {"role": "assistant", "content": _current_tool_state[0].format_html()}
                )
                _tool_block_idx[0] = len(chat_history_ref) - 1
                event_queue.put(("thinking", event))

            elif event.event_type == "agent_response":
                elapsed = time.time() - _step_start_times.get(
                    getattr(event, "step", ""), time.time()
                )

                if _current_tool_state[0] is not None:
                    exit_code = 1 if event.is_error else 0
                    _current_tool_state[0].complete(exit_code=exit_code)
                    _current_tool_state[0].elapsed_seconds = elapsed
                    # Only add stdout if no subprocess lines (LLM agents don't stream)
                    if event.detail and not _current_tool_state[0].subprocess_lines:
                        cleaned = _clean_agent_detail(event.detail)
                        if cleaned:
                            _current_tool_state[0].add_stdout(cleaned[:2000])
                    if _tool_block_idx[0] is not None and _tool_block_idx[0] < len(
                        chat_history_ref
                    ):
                        chat_history_ref[_tool_block_idx[0]]["content"] = _current_tool_state[
                            0
                        ].format_html()
                    _current_tool_state[0] = None
                    _tool_block_idx[0] = None
                else:
                    cleaned_detail = _clean_agent_detail(event.detail) if event.detail else ""
                    if event.is_error:
                        content = format_error_block(
                            message=f"{event.agent_role} failed",
                            detail=cleaned_detail[:2000] if cleaned_detail else "",
                        )
                    else:
                        emoji = (
                            get_tool_emoji(event.agent_role.lower().replace(" ", "_"))
                            if event.agent_role
                            else "📤"
                        )
                        detail_text = cleaned_detail[:2000] if cleaned_detail else ""
                        has_more = len(cleaned_detail) > 2000

                        content = format_tool_block(
                            tool_name=event.agent_role,
                            emoji=emoji,
                            status="success",
                            elapsed_seconds=elapsed,
                            stdout=detail_text + ("..." if has_more else "") if detail_text else "",
                        )
                    chat_history_ref.append({"role": "assistant", "content": content})
                event_queue.put(("response", event))

            elif event.event_type == "agent_progress":
                if _current_tool_state[0] is not None:
                    # Only add detail if it's meaningfully different (avoid duplicates)
                    msg = event.message.strip()
                    det = (event.detail or "").strip()
                    # Skip detail if: same as message, contained in message, or message contains it
                    should_show_detail = det and det != msg and det not in msg and msg not in det
                    if should_show_detail:
                        line = f"↳ {msg} — {det[:200]}"
                    else:
                        line = f"↳ {msg}"
                    _current_tool_state[0].add_subprocess_line(line, is_stderr=False)
                    if _tool_block_idx[0] is not None and _tool_block_idx[0] < len(
                        chat_history_ref
                    ):
                        chat_history_ref[_tool_block_idx[0]]["content"] = _current_tool_state[
                            0
                        ].format_html()
                else:
                    detail_html = ""
                    msg = event.message.strip()
                    det = (event.detail or "").strip()
                    should_show_detail = det and det != msg and det not in msg and msg not in det
                    if should_show_detail:
                        detail_html = f'<div class="subprocess-line" style="padding-left: 16px; color: #7d8590;">{det[:200]}</div>'

                    content = f"""<div class="subprocess-output">
<div class="subprocess-line stdout">↳ {event.message}</div>
{detail_html}
</div>"""
                    chat_history_ref.append({"role": "assistant", "content": content})
                event_queue.put(("progress", event))

            elif event.event_type == "disk_cleanup":
                content = f'<div style="padding: 10px 16px; background: #0d1117; border: 1px solid #21262d; border-radius: 8px; margin: 8px 0; color: #7d8590; font-size: 13px;">🧹 {event.message}</div>'
                chat_history_ref.append({"role": "assistant", "content": content})
                event_queue.put(("disk", event))

            elif event.event_type == "learning_session_end":
                aid = _activity_ids.get("session")
                if aid:
                    complete_activity(aid, event.message)

                elapsed = time.time() - _session_start[0]
                content = f"""<div class="step-block completed">
<div class="step-header">
<span class="step-title">🏁 {event.message}</span>
<span class="step-counter">{elapsed:.0f}s</span>
</div>
<div class="step-progress"><div class="step-progress-bar" style="width: 100%;"></div></div>
</div>"""
                chat_history_ref.append({"role": "assistant", "content": content})
                event_queue.put(("session_end", event))

            elif event.event_type == "warning":
                content = format_warning_block(event.message)
                chat_history_ref.append({"role": "assistant", "content": content})
                event_queue.put(("warning", event))

            elif event.event_type == "heartbeat":
                # Refresh running tool block with updated elapsed time
                if _current_tool_state[0] is not None and _tool_block_idx[0] is not None:
                    if _tool_block_idx[0] < len(chat_history_ref):
                        chat_history_ref[_tool_block_idx[0]]["content"] = _current_tool_state[
                            0
                        ].format_html()
                event_queue.put(("heartbeat", event))

        # Return callback plus state references for heartbeat updates
        return callback, _current_tool_state, _tool_block_idx

    def start_learning_session(
        project_root: str,
        profile_name: str,
        max_cycles: int,
        max_hours: float,
        history: list,
    ):
        """Streaming generator for learning session execution.

        Calls AutonomousLearningScheduler.run_session() with streaming updates.
        If a profile is selected, loads and uses that profile's configuration.
        """
        global _learning_scheduler_ref
        _learning_session_stop_requested.clear()

        history = list(history) if history else []
        history.append({"role": "user", "content": "📚 Start Learning Session (Observation Only)"})

        def make_state(state_code: str, emoji: str = ""):
            return f"**State:** `{state_code}` {emoji}"

        if not project_root or not project_root.strip():
            history.append({"role": "assistant", "content": "❌ Please specify a project root"})
            yield (
                make_state("ERROR", "❌"),
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=False),
            )
            return

        try:
            from odibi.agents.core.autonomous_learning import (
                AutonomousLearningScheduler,
                LearningCycleConfig,
            )
            from odibi.agents.core.cycle_profile import CycleProfileError, CycleProfileLoader

            runner = get_runner()
            if not runner:
                history.append({"role": "assistant", "content": "❌ Runner not initialized"})
                yield (
                    make_state("ERROR", "❌"),
                    get_activity_feed().format_markdown(),
                    history,
                    gr.update(visible=False),
                )
                return

            config = runner.config if hasattr(runner, "config") else None
            odibi_root = get_odibi_root(config)

            if _learning_scheduler_ref[0] is None:
                _learning_scheduler_ref[0] = AutonomousLearningScheduler(
                    odibi_root=odibi_root,
                    azure_config=runner.azure_config if hasattr(runner, "azure_config") else None,
                )

            scheduler = _learning_scheduler_ref[0]

            if profile_name and profile_name.strip():
                try:
                    loader = CycleProfileLoader(odibi_root)
                    frozen_profile = loader.load_profile(profile_name.strip())
                    config = LearningCycleConfig.from_frozen_profile(
                        profile=frozen_profile,
                        project_root=project_root.strip(),
                        task_description="Autonomous learning session",
                    )
                    history.append(
                        {
                            "role": "assistant",
                            "content": f"📋 Loaded profile: **{frozen_profile.profile_name}** (`{frozen_profile.content_hash[:8]}`)",
                        }
                    )
                except CycleProfileError as e:
                    history.append({"role": "assistant", "content": f"❌ Profile error: {e}"})
                    yield (
                        make_state("ERROR", "❌"),
                        get_activity_feed().format_markdown(),
                        history,
                        gr.update(visible=False),
                    )
                    return
            else:
                config = LearningCycleConfig(
                    project_root=project_root.strip(),
                    task_description="Autonomous learning session",
                    max_improvements=0,
                )

            validation_errors = config.validate()
            if validation_errors:
                history.append(
                    {
                        "role": "assistant",
                        "content": f"❌ Config validation failed: {validation_errors}",
                    }
                )
                yield (
                    make_state("ERROR", "❌"),
                    get_activity_feed().format_markdown(),
                    history,
                    gr.update(visible=False),
                )
                return

            event_queue = queue.Queue()
            history.append(
                {
                    "role": "assistant",
                    "content": f"🚀 Starting learning session for `{project_root}`...",
                }
            )
            profile_info = f", Profile: {config.profile_name}" if config.profile_id else ""
            history.append(
                {
                    "role": "assistant",
                    "content": f"ℹ️ Max cycles: {int(max_cycles)}, Max hours: {int(max_hours)}{profile_info}",
                }
            )
            history.append(
                {
                    "role": "assistant",
                    "content": "ℹ️ **Observation-only mode** — no code changes will be made",
                }
            )
            yield (
                make_state("STARTING", "🟡"),
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=True),
            )

            callback, _tool_state_ref, _tool_idx_ref = _create_learning_session_callback(
                event_queue, history
            )
            scheduler.set_event_callback(callback)

            session_complete = [False]
            session_result = [None]
            session_error = [None]

            def run_session_thread():
                """Run learning session in background thread."""
                try:
                    result = scheduler.run_session(
                        config=config,
                        max_cycles=int(max_cycles),
                        max_wall_clock_hours=float(max_hours),
                    )
                    session_result[0] = result
                    session_complete[0] = True
                    event_queue.put(("done", None))
                except Exception as e:
                    session_error[0] = e
                    event_queue.put(("error", e))

            thread = threading.Thread(target=run_session_thread, daemon=True)
            thread.start()

            last_state = ("RUNNING", "🟢")
            cycles_done = 0

            while not session_complete[0] and session_error[0] is None:
                if _learning_session_stop_requested.is_set():
                    scheduler.stop()
                    history.append(
                        {
                            "role": "assistant",
                            "content": "⏹️ Stop requested, finishing current cycle...",
                        }
                    )

                try:
                    event_type, event = event_queue.get(timeout=0.1)

                    if event_type == "done":
                        break
                    elif event_type == "error":
                        raise event
                    elif event_type == "session_start":
                        last_state = ("RUNNING", "🟢")
                    elif event_type == "cycle_start":
                        last_state = ("RUNNING", "🟢")
                    elif event_type == "cycle_end":
                        cycles_done += 1
                        last_state = ("RUNNING", "🟢")
                    elif event_type == "session_end":
                        last_state = ("COMPLETED", "✅")
                    elif event_type == "thinking":
                        last_state = ("RUNNING", "🟢")
                    elif event_type == "progress":
                        last_state = ("RUNNING", "🟢")

                    yield (
                        make_state(*last_state),
                        get_activity_feed().format_markdown(),
                        history,
                        gr.update(visible=True),
                    )

                except queue.Empty:
                    # Heartbeat: refresh running tool block with updated elapsed time
                    if _tool_state_ref[0] is not None and _tool_idx_ref[0] is not None:
                        if _tool_idx_ref[0] < len(history):
                            history[_tool_idx_ref[0]]["content"] = _tool_state_ref[0].format_html()
                    yield (
                        make_state(*last_state),
                        get_activity_feed().format_markdown(),
                        history,
                        gr.update(visible=True),
                    )

            thread.join(timeout=2.0)

            if session_error[0]:
                raise session_error[0]

            result = session_result[0]
            if result:
                final_msg = (
                    f"✅ Session complete — {result.cycles_completed} cycles, "
                    f"{result.cycles_failed} failed"
                )
                history.append({"role": "assistant", "content": final_msg})
                yield (
                    make_state("COMPLETED", "✅"),
                    get_activity_feed().format_markdown(),
                    history,
                    gr.update(visible=False),
                )
            else:
                yield (
                    make_state("COMPLETED", "✅"),
                    get_activity_feed().format_markdown(),
                    history,
                    gr.update(visible=False),
                )

        except Exception as e:
            history.append(
                {"role": "assistant", "content": f"❌ **Error:** {type(e).__name__}: {e}"}
            )
            yield (
                make_state("FAILED", "❌"),
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=False),
            )

    learning_session_outputs = [
        guided_components["learning_session_state"],
    ]
    if chat_components and "activity_display" in chat_components:
        learning_session_outputs.append(chat_components["activity_display"])
    if chat_components and "chatbot" in chat_components:
        learning_session_outputs.append(chat_components["chatbot"])
    learning_session_outputs.append(guided_components["stop_learning_session"])

    learning_session_inputs = [
        guided_components["learning_project_root"],
        guided_components.get("learning_profile_dropdown", gr.State(None)),
        guided_components["learning_max_cycles"],
        guided_components["learning_max_hours"],
        chat_components["chatbot"] if chat_components else gr.State([]),
    ]

    guided_components["start_learning_session"].click(
        fn=start_learning_session,
        inputs=learning_session_inputs,
        outputs=learning_session_outputs,
    )

    def stop_learning_session():
        _learning_session_stop_requested.set()
        return "**State:** `STOPPING`"

    guided_components["stop_learning_session"].click(
        fn=stop_learning_session,
        outputs=[guided_components["learning_session_state"]],
    )

    def refresh_learning_status():
        """Read heartbeat.json and update monitor panel (read-only).

        This allows the UI to reconnect to a running session after refresh.
        """
        try:
            from odibi.agents.core.disk_guard import HeartbeatWriter

            runner = get_runner()
            if not runner:
                return (
                    "**State:** `ERROR` — Runner not initialized",
                    "—",
                    "0",
                    "0",
                    "—",
                    "—",
                    "—",
                    "**Disk Usage:** _Not available_",
                )

            config = runner.config if hasattr(runner, "config") else None
            odibi_root = get_odibi_root(config)
            heartbeat_writer = HeartbeatWriter(odibi_root)
            heartbeat = heartbeat_writer.read()

            if heartbeat is None:
                return (
                    "**State:** `NOT_STARTED`",
                    "—",
                    "0",
                    "0",
                    "—",
                    "—",
                    "—",
                    "**Disk Usage:** _No heartbeat file found_",
                )

            status = heartbeat.last_status.upper()
            if "RUNNING" in status or "SESSION_RUNNING" in status:
                state = "`RUNNING` 🟢"
            elif "STOPPING" in status:
                state = "`STOPPING` 🟡"
            elif "COMPLETED" in status or "SESSION_COMPLETED" in status:
                state = "`COMPLETED` ✅"
            elif "FAILED" in status or "SESSION_FAILED" in status:
                state = f"`FAILED` ❌ — {heartbeat.last_status}"
            else:
                state = f"`{status}`"

            disk_usage = heartbeat.disk_usage or {}
            disk_lines = []
            for key, value in disk_usage.items():
                mb = value / (1024 * 1024)
                disk_lines.append(f"- **{key}:** {mb:.1f} MB")
            disk_md = (
                "**Disk Usage:**\n" + "\n".join(disk_lines)
                if disk_lines
                else "**Disk Usage:** _Empty_"
            )

            session_id = "—"
            if _learning_scheduler_ref[0] is not None:
                session_id = getattr(_learning_scheduler_ref[0], "_current_session_id", "—") or "—"

            profile_info = ""
            if hasattr(heartbeat, "profile_id") and heartbeat.profile_id:
                profile_info = f"\n\n📋 **Profile:** {heartbeat.profile_name} (`{heartbeat.profile_hash[:8] if heartbeat.profile_hash else 'N/A'}`)"

            return (
                f"**State:** {state}{profile_info}",
                session_id,
                str(heartbeat.cycles_completed),
                str(heartbeat.cycles_failed),
                heartbeat.timestamp or "—",
                heartbeat.last_cycle_id or "—",
                heartbeat.last_status or "—",
                disk_md,
            )

        except Exception as e:
            return (
                f"**State:** `ERROR` — {type(e).__name__}: {e}",
                "—",
                "0",
                "0",
                "—",
                "—",
                "—",
                "**Disk Usage:** _Error reading heartbeat_",
            )

    guided_components["refresh_learning_status"].click(
        fn=refresh_learning_status,
        outputs=[
            guided_components["learning_session_state"],
            guided_components["learning_session_id"],
            guided_components["learning_cycles_completed"],
            guided_components["learning_cycles_failed"],
            guided_components["learning_last_heartbeat"],
            guided_components["learning_last_cycle_id"],
            guided_components["learning_last_status"],
            guided_components["learning_disk_usage"],
        ],
    )

    # Phase 9.G: Controlled Improvement handlers
    _improvement_stop_requested = threading.Event()

    def start_controlled_improvement(
        project_root: str,
        target_file: str,
        issue_description: str,
        golden_projects_str: str,
        rollback_on_failure: bool,
        require_regression: bool,
        history: list,
    ):
        """Start a controlled improvement cycle (single-shot, scoped)."""
        _improvement_stop_requested.clear()

        history = list(history) if history else []
        history.append({"role": "user", "content": "🔧 Start Controlled Improvement"})

        def make_status(text: str):
            return f"**Status:** {text}"

        if not project_root or not project_root.strip():
            history.append({"role": "assistant", "content": "❌ Please specify a project root"})
            yield (
                make_status("Error: Project root required"),
                None,
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=False),
            )
            return

        if not target_file or not target_file.strip():
            history.append(
                {
                    "role": "assistant",
                    "content": "❌ Please specify a target file for improvement scope",
                }
            )
            yield (
                make_status("Error: Target file required"),
                None,
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=False),
            )
            return

        try:
            from pathlib import Path
            from odibi.agents.core.controlled_improvement import (
                ControlledImprovementConfig,
                ControlledImprovementRunner,
                ImprovementScope,
                is_learning_harness_path,
            )
            from odibi.agents.core.cycle import parse_golden_projects_input

            runner = get_runner()
            if not runner:
                history.append({"role": "assistant", "content": "❌ Runner not initialized"})
                yield (
                    make_status("Error: Runner not initialized"),
                    None,
                    get_activity_feed().format_markdown(),
                    history,
                    gr.update(visible=False),
                )
                return

            config = runner.config if hasattr(runner, "config") else None
            odibi_root = get_odibi_root(config)

            # Resolve target file path
            target_path = Path(project_root.strip()) / target_file.strip()
            if not target_path.exists():
                history.append(
                    {"role": "assistant", "content": f"❌ Target file not found: {target_path}"}
                )
                yield (
                    make_status("Error: Target file not found"),
                    None,
                    get_activity_feed().format_markdown(),
                    history,
                    gr.update(visible=False),
                )
                return

            # Block improvements to learning harness files (early validation)
            if is_learning_harness_path(str(target_path)):
                history.append(
                    {
                        "role": "assistant",
                        "content": "❌ Cannot improve files in `.odibi/learning_harness/` — these are system validation fixtures",
                    }
                )
                yield (
                    make_status("Error: Protected path"),
                    None,
                    get_activity_feed().format_markdown(),
                    history,
                    gr.update(visible=False),
                )
                return

            # Create improvement scope
            scope = ImprovementScope.for_single_file(
                str(target_path),
                description=f"Fix issues in {target_file}",
            )

            # Parse golden projects
            golden_projects = parse_golden_projects_input(golden_projects_str, project_root.strip())

            # Build task description from issue or default
            task_desc = (
                issue_description.strip() if issue_description else f"Fix issues in {target_file}"
            )

            # Create config
            config = ControlledImprovementConfig(
                project_root=project_root.strip(),
                task_description=task_desc,
                improvement_scope=scope,
                golden_projects=golden_projects,
                require_regression_check=require_regression and bool(golden_projects),
                rollback_on_failure=rollback_on_failure,
            )

            validation_errors = config.validate()
            if validation_errors:
                history.append(
                    {
                        "role": "assistant",
                        "content": f"❌ Config validation failed: {validation_errors}",
                    }
                )
                yield (
                    make_status("Error: Invalid configuration"),
                    None,
                    get_activity_feed().format_markdown(),
                    history,
                    gr.update(visible=False),
                )
                return

            history.append(
                {
                    "role": "assistant",
                    "content": f"🚀 Starting controlled improvement for `{target_file}`...",
                }
            )
            history.append(
                {
                    "role": "assistant",
                    "content": f"ℹ️ **Scope:** Only `{target_file}` can be modified",
                }
            )
            if golden_projects:
                names = [gp.name for gp in golden_projects]
                history.append(
                    {"role": "assistant", "content": f"ℹ️ **Golden Projects:** {', '.join(names)}"}
                )
            yield (
                make_status("Initializing..."),
                None,
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=True),
            )

            # Create runner and start cycle
            improvement_runner = ControlledImprovementRunner(
                odibi_root=odibi_root,
                azure_config=runner.azure_config if hasattr(runner, "azure_config") else None,
            )

            # Set up event callback for streaming UI updates
            event_queue = queue.Queue()
            callback, _tool_state_ref, _tool_idx_ref = _create_learning_session_callback(
                event_queue, history
            )
            improvement_runner.set_event_callback(callback)

            history.append(
                {"role": "assistant", "content": "📸 Capturing snapshot of target file..."}
            )
            yield (
                make_status("Capturing snapshot..."),
                None,
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=True),
            )

            state = improvement_runner.start_controlled_improvement_cycle(config)

            history.append(
                {"role": "assistant", "content": f"✅ Cycle started: `{state.cycle_id}`"}
            )
            history.append(
                {"role": "assistant", "content": "🔍 Running observation and improvement steps..."}
            )
            yield (
                make_status("Running cycle..."),
                None,
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=True),
            )

            # Run the cycle in a background thread for streaming updates
            cycle_complete = [False]
            cycle_error = [None]

            def run_improvement_thread():
                """Run improvement cycle in background thread."""
                nonlocal state
                try:
                    while not state.is_finished() and not _improvement_stop_requested.is_set():
                        state = improvement_runner.run_next_step(state)
                    cycle_complete[0] = True
                    event_queue.put(("done", None))
                except Exception as e:
                    cycle_error[0] = e
                    event_queue.put(("error", e))

            thread = threading.Thread(target=run_improvement_thread, daemon=True)
            thread.start()

            last_step_name = "Running"

            # Stream updates from event queue
            while not cycle_complete[0] and cycle_error[0] is None:
                if _improvement_stop_requested.is_set():
                    history.append(
                        {
                            "role": "assistant",
                            "content": "⏹️ Stop requested, finishing current step...",
                        }
                    )

                try:
                    event_type, event = event_queue.get(timeout=0.1)

                    if event_type == "done":
                        break
                    elif event_type == "error":
                        raise event
                    elif event_type == "step":
                        last_step_name = event.message if event else last_step_name

                    yield (
                        make_status(f"Step: {last_step_name}"),
                        None,
                        get_activity_feed().format_markdown(),
                        history,
                        gr.update(visible=True),
                    )

                except queue.Empty:
                    # Heartbeat: refresh running tool block with updated elapsed time
                    if _tool_state_ref[0] is not None and _tool_idx_ref[0] is not None:
                        if _tool_idx_ref[0] < len(history):
                            history[_tool_idx_ref[0]]["content"] = _tool_state_ref[0].format_html()
                    yield (
                        make_status(f"Step: {last_step_name}"),
                        None,
                        get_activity_feed().format_markdown(),
                        history,
                        gr.update(visible=True),
                    )

            thread.join(timeout=2.0)

            if cycle_error[0]:
                raise cycle_error[0]

            # Get improvement result
            result = improvement_runner.get_improvement_result()

            if result:
                result_dict = result.to_dict()
                status_emoji = {
                    "APPLIED": "✅",
                    "REJECTED": "❌",
                    "ROLLED_BACK": "⚠️",
                    "NO_PROPOSAL": "ℹ️",
                }.get(result.status, "❓")

                history.append(
                    {
                        "role": "assistant",
                        "content": f"{status_emoji} **Improvement Status:** {result.status}",
                    }
                )

                if result.status == "APPLIED":
                    history.append(
                        {
                            "role": "assistant",
                            "content": f"✅ Files modified: {', '.join(result.files_modified)}",
                        }
                    )
                elif result.status == "REJECTED" and result.rejection_reason:
                    history.append(
                        {
                            "role": "assistant",
                            "content": f"❌ Rejected: {result.rejection_reason.value} - {result.rejection_details}",
                        }
                    )
                elif result.status == "ROLLED_BACK":
                    history.append(
                        {
                            "role": "assistant",
                            "content": f"⚠️ Changes rolled back. Restored: {', '.join(result.files_restored)}",
                        }
                    )

                yield (
                    make_status(f"{status_emoji} {result.status}"),
                    result_dict,
                    get_activity_feed().format_markdown(),
                    history,
                    gr.update(visible=False),
                )
            else:
                history.append(
                    {"role": "assistant", "content": "ℹ️ No improvement result available"}
                )
                yield (
                    make_status("Complete (no result)"),
                    None,
                    get_activity_feed().format_markdown(),
                    history,
                    gr.update(visible=False),
                )

        except Exception as e:
            history.append(
                {"role": "assistant", "content": f"❌ **Error:** {type(e).__name__}: {e}"}
            )
            yield (
                make_status(f"Error: {e}"),
                None,
                get_activity_feed().format_markdown(),
                history,
                gr.update(visible=False),
            )

    def stop_controlled_improvement():
        _improvement_stop_requested.set()
        return "**Status:** ⏹️ Stop requested..."

    # Wire up controlled improvement handlers
    if "start_controlled_improvement" in guided_components:
        improvement_outputs = [
            guided_components["improvement_status"],
            guided_components["improvement_result"],
        ]
        if chat_components and "activity_display" in chat_components:
            improvement_outputs.append(chat_components["activity_display"])
        if chat_components and "chatbot" in chat_components:
            improvement_outputs.append(chat_components["chatbot"])
        improvement_outputs.append(guided_components["stop_controlled_improvement"])

        guided_components["start_controlled_improvement"].click(
            fn=start_controlled_improvement,
            inputs=[
                guided_components["improvement_project_root"],
                guided_components["improvement_target_file"],
                guided_components["improvement_issue_description"],
                guided_components["improvement_golden_projects"],
                guided_components["improvement_rollback_on_failure"],
                guided_components["improvement_require_regression"],
                chat_components["chatbot"] if chat_components else gr.State([]),
            ],
            outputs=improvement_outputs,
        )

        guided_components["stop_controlled_improvement"].click(
            fn=stop_controlled_improvement,
            outputs=[guided_components["improvement_status"]],
        )


def format_morning_summary(summary: dict) -> str:
    """Format morning summary as markdown.

    Args:
        summary: Summary dictionary from CycleRunner.

    Returns:
        Formatted markdown string.
    """
    if not summary:
        return "_No recent cycles_"

    lines = [
        f"### {summary.get('period', 'Recent Cycles')}",
        "",
        f"**Cycles Completed:** {summary.get('cycles_completed', 0)}",
        f"**Cycles Interrupted:** {summary.get('cycles_interrupted', 0)}",
        "",
    ]

    if summary.get("improvements_approved", 0) or summary.get("improvements_rejected", 0):
        lines.extend(
            [
                "**Improvements:**",
                f"- ✅ Approved: {summary.get('improvements_approved', 0)}",
                f"- ❌ Rejected: {summary.get('improvements_rejected', 0)}",
                "",
            ]
        )

    if summary.get("regressions_detected", 0):
        lines.append(f"⚠️ **Regressions Detected:** {summary.get('regressions_detected', 0)}")
        lines.append("")

    if summary.get("convergence_reached"):
        lines.append("✨ **Convergence Reached** - System has stabilized")
        lines.append("")

    if summary.get("latest_summary"):
        lines.extend(
            [
                "---",
                "### Latest Cycle Summary",
                summary.get("latest_summary", ""),
            ]
        )

    return "\n".join(lines)
