"""Explorer Panel for sandboxed experimentation.

Provides UI for:
- Creating isolated sandbox environments
- Running experiments with captured diffs
- Viewing experiment results and memory entries
- Submitting candidates for promotion (human review required)

TRUST BOUNDARY INVARIANTS:
- All experiments run in sandbox clones (never trusted codebase)
- Explorer can ONLY submit PENDING candidates
- Status changes require human review OUTSIDE Explorer
- No auto-promotion or auto-approval
"""

import os
from pathlib import Path
from typing import Any, Optional

import gradio as gr

from .activity_feed import add_activity, complete_activity, error_activity


def _create_initial_explorer_state() -> dict:
    """Create initial explorer state for a new session."""
    return {
        "clone_manager": None,
        "experiment_runner": None,
        "memory": None,
        "bucket": None,
        "current_sandbox": None,
    }


def _get_default_paths() -> dict:
    """Get default paths for explorer from env or defaults."""
    odibi_root = os.environ.get("ODIBI_ROOT", "")
    if not odibi_root:
        current = Path(__file__).resolve()
        for parent in current.parents:
            if (parent / ".odibi").is_dir() or (
                (parent / "agents").is_dir() and (parent / ".git").exists()
            ):
                odibi_root = str(parent)
                break
        else:
            odibi_root = "D:/odibi"

    sandbox_root = os.environ.get(
        "ODIBI_SANDBOX_ROOT", str(Path(odibi_root).parent / "explorer_sandboxes")
    )
    return {
        "sandbox_root": sandbox_root,
        "trusted_repo": odibi_root,
    }


def _init_explorer(sandbox_root: str, trusted_repo: str, state: dict) -> tuple[bool, str, dict]:
    """Initialize explorer components.

    Args:
        sandbox_root: Path to sandbox directory.
        trusted_repo: Path to trusted repository.
        state: Current explorer state dict (session-isolated).

    Returns:
        Tuple of (success, message, updated_state).
    """
    try:
        from odibi.agents.explorer.clone_manager import RepoCloneManager
        from odibi.agents.explorer.experiment_runner import ExperimentRunner
        from odibi.agents.explorer.memory import ExplorerMemory
        from odibi.agents.explorer.promotion_bucket import PromotionBucket

        sandbox_path = Path(sandbox_root)
        trusted_path = Path(trusted_repo)

        sandbox_path.mkdir(parents=True, exist_ok=True)

        state["clone_manager"] = RepoCloneManager(
            sandbox_root=sandbox_path,
            trusted_repo_root=trusted_path,
        )
        state["experiment_runner"] = ExperimentRunner(state["clone_manager"])
        state["memory"] = ExplorerMemory(sandbox_path / "explorer_memory.jsonl")
        state["bucket"] = PromotionBucket(sandbox_path / "promotion_bucket.jsonl")

        return True, f"✅ Explorer initialized (sandbox: {sandbox_root})", state
    except Exception as e:
        return False, f"❌ Failed to initialize: {e}", state


def create_explorer_panel() -> tuple[gr.Column, dict[str, Any]]:
    """Create the Explorer panel for sandboxed experimentation.

    Returns:
        Tuple of (Gradio Column, dict of component references).
    """
    components: dict[str, Any] = {}
    defaults = _get_default_paths()

    with gr.Column(visible=False) as explorer_column:
        components["explorer_column"] = explorer_column

        components["explorer_state"] = gr.State(value=_create_initial_explorer_state)

        gr.Markdown("## 🧪 Explorer (Sandboxed Experimentation)")

        gr.Markdown(
            """
            **Safe Experimentation** — Run experiments in isolated sandbox environments.

            **Trust Boundaries:**
            - ✅ All changes happen in sandbox clones
            - ✅ Diffs are captured for review
            - ✅ Promotions require human approval
            - ❌ No auto-apply to trusted codebase
            """
        )

        with gr.Accordion("⚙️ Explorer Setup", open=True) as setup_accordion:
            components["setup_accordion"] = setup_accordion

            with gr.Row():
                components["sandbox_root"] = gr.Textbox(
                    label="Sandbox Root",
                    value=defaults["sandbox_root"],
                    placeholder="D:/explorer_sandboxes",
                    info="Where sandbox clones are created (must be outside trusted repo)",
                    scale=2,
                )
                components["trusted_repo"] = gr.Textbox(
                    label="Trusted Repo Root",
                    value=defaults["trusted_repo"],
                    placeholder="D:/odibi",
                    info="Your main codebase (protected from changes)",
                    scale=2,
                )

            with gr.Row():
                components["init_explorer_btn"] = gr.Button(
                    "🔧 Initialize Explorer",
                    variant="primary",
                )

            components["explorer_status"] = gr.Markdown(value="**Status:** Not initialized")

        with gr.Accordion("📂 Sandbox Management", open=True) as sandbox_accordion:
            components["sandbox_accordion"] = sandbox_accordion

            with gr.Row():
                components["source_project"] = gr.Textbox(
                    label="Source Project",
                    placeholder="D:/some_project",
                    info="Project to clone for experimentation",
                    scale=2,
                )
                components["create_sandbox_btn"] = gr.Button(
                    "📋 Create Sandbox",
                    variant="secondary",
                )

            components["sandbox_status"] = gr.Markdown(value="_No active sandbox_")

            with gr.Row():
                components["cleanup_sandbox_btn"] = gr.Button(
                    "🗑️ Cleanup Sandbox",
                    variant="stop",
                    visible=False,
                )
                components["cleanup_all_btn"] = gr.Button(
                    "🧹 Cleanup All",
                    variant="stop",
                )

        with gr.Accordion("🔬 Run Experiment", open=True) as experiment_accordion:
            components["experiment_accordion"] = experiment_accordion

            components["exp_name"] = gr.Textbox(
                label="Experiment Name",
                placeholder="add_helper_function",
                info="Short descriptive name",
            )

            components["exp_description"] = gr.Textbox(
                label="Description",
                placeholder="Test adding a helper function to improve code organization",
                info="What this experiment attempts",
                lines=2,
            )

            components["exp_commands"] = gr.Textbox(
                label="Commands (one per line)",
                placeholder='echo "def helper(): pass" >> utils.py\necho "# Added helper" >> README.md',
                info="Shell commands to execute in sandbox",
                lines=4,
            )

            components["exp_hypothesis"] = gr.Textbox(
                label="Hypothesis",
                placeholder="Adding a helper function will improve code organization",
                info="What you're testing",
            )

            with gr.Row():
                components["exp_timeout"] = gr.Slider(
                    label="Timeout (seconds)",
                    minimum=10,
                    maximum=600,
                    value=60,
                    step=10,
                )
                components["run_experiment_btn"] = gr.Button(
                    "▶️ Run Experiment",
                    variant="primary",
                )

        with gr.Accordion("📊 Experiment Results", open=True) as results_accordion:
            components["results_accordion"] = results_accordion

            components["experiment_result"] = gr.Markdown(value="_No experiment run yet_")

            with gr.Accordion("📝 Diff Preview", open=False) as diff_accordion:
                components["diff_accordion"] = diff_accordion
                components["diff_preview"] = gr.Textbox(
                    label="Captured Diff",
                    value="",
                    interactive=False,
                    lines=15,
                    max_lines=30,
                )

        with gr.Accordion("💾 Memory & Promotions", open=True) as memory_accordion:
            components["memory_accordion"] = memory_accordion

            with gr.Row():
                components["save_to_memory_btn"] = gr.Button(
                    "💾 Save to Memory",
                    variant="secondary",
                    interactive=False,
                )
                components["submit_promotion_btn"] = gr.Button(
                    "📤 Submit for Review",
                    variant="primary",
                    interactive=False,
                )

            components["promotion_status"] = gr.Markdown(value="_No promotion submitted_")

            gr.Markdown("### 📋 Pending Promotions")
            components["pending_promotions"] = gr.Dataframe(
                headers=["ID", "Diff Hash", "Summary", "Submitted At", "Status"],
                datatype=["str", "str", "str", "str", "str"],
                interactive=False,
                wrap=True,
                value=[],
            )

            components["refresh_promotions_btn"] = gr.Button(
                "🔄 Refresh",
                size="sm",
            )

        with gr.Accordion("📚 Explorer Memory", open=False) as explorer_memory_accordion:
            components["explorer_memory_accordion"] = explorer_memory_accordion

            components["memory_entries"] = gr.Dataframe(
                headers=["ID", "Experiment", "Outcome", "Diff Hash", "Timestamp"],
                datatype=["str", "str", "str", "str", "str"],
                interactive=False,
                wrap=True,
                value=[],
            )

            components["refresh_memory_btn"] = gr.Button(
                "🔄 Refresh Memory",
                size="sm",
            )

    return explorer_column, components


def setup_explorer_handlers(
    components: dict[str, Any],
    chat_components: Optional[dict[str, Any]] = None,
) -> None:
    """Set up event handlers for the Explorer panel.

    Args:
        components: Components from create_explorer_panel.
        chat_components: Optional chat components for activity feed updates.

    Note:
        All handlers use gr.State() for session-isolated state management.
        The state dict contains: clone_manager, experiment_runner, memory,
        bucket, current_sandbox, last_experiment_result, last_memory_entry.
    """

    def on_init_explorer(sandbox_root: str, trusted_repo: str, state: dict):
        """Initialize the explorer."""
        activity_id = add_activity("Initializing Explorer...", tool_name="explorer")

        success, message, state = _init_explorer(sandbox_root, trusted_repo, state)

        if success:
            complete_activity(activity_id, "Explorer initialized")
        else:
            error_activity(activity_id, "Explorer init failed")

        return f"**Status:** {message}", state

    def on_create_sandbox(source_project: str, state: dict):
        """Create a sandbox clone."""
        if not state["clone_manager"]:
            return "_Explorer not initialized_", gr.update(visible=False), state

        if not source_project or not source_project.strip():
            return "❌ Please specify a source project", gr.update(visible=False), state

        activity_id = add_activity(
            f"Creating sandbox from {source_project}...", tool_name="explorer"
        )

        try:
            source_path = Path(source_project)
            if not source_path.exists():
                error_activity(activity_id, "Source not found")
                return (
                    f"❌ Source project not found: {source_project}",
                    gr.update(visible=False),
                    state,
                )

            sandbox_info = state["clone_manager"].create_sandbox(source_path)
            state["current_sandbox"] = sandbox_info

            state["experiment_runner"].create_baseline(sandbox_info.sandbox_id)

            complete_activity(activity_id, f"Sandbox created: {sandbox_info.sandbox_id[:8]}")

            status = f"""✅ **Sandbox Created**
- **ID:** `{sandbox_info.sandbox_id}`
- **Path:** `{sandbox_info.sandbox_path}`
- **Source:** `{sandbox_info.source_repo}`
- **Created:** {sandbox_info.created_at}
"""
            return status, gr.update(visible=True), state

        except Exception as e:
            error_activity(activity_id, f"Failed: {e}")
            return f"❌ Failed to create sandbox: {e}", gr.update(visible=False), state

    def on_cleanup_sandbox(state: dict):
        """Cleanup current sandbox."""
        if not state["clone_manager"] or not state["current_sandbox"]:
            return "_No active sandbox to cleanup_", gr.update(visible=False), state

        sandbox_id = state["current_sandbox"].sandbox_id
        activity_id = add_activity(f"Cleaning up sandbox {sandbox_id[:8]}...", tool_name="explorer")

        try:
            state["clone_manager"].destroy_sandbox(sandbox_id)
            state["current_sandbox"] = None
            complete_activity(activity_id, "Sandbox cleaned up")
            return "_No active sandbox_", gr.update(visible=False), state
        except Exception as e:
            error_activity(activity_id, f"Cleanup failed: {e}")
            return f"❌ Cleanup failed: {e}", gr.update(visible=True), state

    def on_cleanup_all(state: dict):
        """Cleanup all sandboxes."""
        if not state["clone_manager"]:
            return "_Explorer not initialized_", state

        activity_id = add_activity("Cleaning up all sandboxes...", tool_name="explorer")

        try:
            count = state["clone_manager"].cleanup_all()
            state["current_sandbox"] = None
            complete_activity(activity_id, f"Cleaned up {count} sandboxes")
            return f"✅ Cleaned up {count} sandbox(es)", state
        except Exception as e:
            error_activity(activity_id, f"Cleanup failed: {e}")
            return f"❌ Cleanup failed: {e}", state

    def on_run_experiment(
        name: str, description: str, commands: str, hypothesis: str, timeout: int, state: dict
    ):
        """Run an experiment in the sandbox."""
        if not state["experiment_runner"]:
            return (
                "_Explorer not initialized_",
                "",
                gr.update(interactive=False),
                gr.update(interactive=False),
                state,
            )

        if not state["current_sandbox"]:
            return (
                "❌ No active sandbox. Create one first.",
                "",
                gr.update(interactive=False),
                gr.update(interactive=False),
                state,
            )

        if not name or not commands:
            return (
                "❌ Name and commands are required",
                "",
                gr.update(interactive=False),
                gr.update(interactive=False),
                state,
            )

        activity_id = add_activity(f"Running experiment: {name}...", tool_name="explorer")

        try:
            from odibi.agents.explorer.experiment_runner import ExperimentSpec

            cmd_list = [c.strip() for c in commands.strip().split("\n") if c.strip()]

            spec = ExperimentSpec(
                name=name,
                description=description or name,
                commands=cmd_list,
                timeout_seconds=int(timeout),
                capture_diff=True,
            )

            result = state["experiment_runner"].run_experiment(
                spec, state["current_sandbox"].sandbox_id
            )
            state["last_experiment_result"] = (result, hypothesis)

            status_emoji = {
                "completed": "✅",
                "failed": "❌",
                "timeout": "⏱️",
            }.get(result.status.value, "❓")

            diff_display = result.diff_content if result.diff_content else "_No diff captured_"

            complete_activity(activity_id, f"Experiment {result.status.value}")

            result_md = f"""### {status_emoji} Experiment: {name}

- **Status:** {result.status.value}
- **Exit Code:** {result.exit_code}
- **Diff Empty:** {result.diff_is_empty}
- **Diff Hash:** `{result.diff_hash[:16] if result.diff_hash else "N/A"}`
- **Started:** {result.started_at}
- **Ended:** {result.ended_at}

**Stdout:**
```
{result.stdout[:500] if result.stdout else "(none)"}
```

**Stderr:**
```
{result.stderr[:500] if result.stderr else "(none)"}
```
"""
            can_save = not result.diff_is_empty
            return (
                result_md,
                diff_display,
                gr.update(interactive=can_save),
                gr.update(interactive=False),
                state,
            )

        except Exception as e:
            error_activity(activity_id, f"Failed: {e}")
            state["last_experiment_result"] = None
            return (
                f"❌ Experiment failed: {e}",
                "",
                gr.update(interactive=False),
                gr.update(interactive=False),
                state,
            )

    def on_save_to_memory(state: dict):
        """Save experiment result to memory."""
        if not state["memory"] or not state.get("last_experiment_result"):
            return "_No experiment to save_", gr.update(interactive=False), state

        activity_id = add_activity("Saving to memory...", tool_name="explorer")

        try:
            from odibi.agents.explorer.memory import create_memory_entry_from_experiment

            result, hypothesis = state["last_experiment_result"]

            entry = create_memory_entry_from_experiment(
                experiment_result=result,
                hypothesis=hypothesis or "Experimental change",
                observation=f"Experiment {result.status.value}",
            )

            state["memory"].append(entry)
            state["last_memory_entry"] = entry

            complete_activity(activity_id, f"Saved entry: {entry.entry_id[:12]}")

            return f"✅ Saved to memory: `{entry.entry_id}`", gr.update(interactive=True), state

        except Exception as e:
            error_activity(activity_id, f"Save failed: {e}")
            return f"❌ Failed to save: {e}", gr.update(interactive=False), state

    def on_submit_promotion(state: dict):
        """Submit for promotion review."""
        if not state["bucket"] or not state.get("last_memory_entry"):
            return "_No memory entry to promote_", state

        activity_id = add_activity("Submitting for review...", tool_name="explorer")

        try:
            from odibi.agents.explorer.promotion_bucket import create_promotion_entry_from_memory

            entry = state["last_memory_entry"]

            promotion = create_promotion_entry_from_memory(
                memory_entry=entry,
                diff_summary=f"Experiment: {entry.experiment_id}",
            )

            state["bucket"].submit(promotion)

            complete_activity(activity_id, f"Submitted: {promotion.candidate_id[:12]}")

            return (
                f"""✅ **Submitted for Review**
- **Candidate ID:** `{promotion.candidate_id}`
- **Status:** PENDING (requires human approval)
- **Diff Hash:** `{promotion.diff_hash[:16]}`
""",
                state,
            )

        except Exception as e:
            error_activity(activity_id, f"Submit failed: {e}")
            return f"❌ Failed to submit: {e}", state

    def on_refresh_promotions(state: dict):
        """Refresh pending promotions list."""
        if not state["bucket"]:
            return []

        try:
            entries = state["bucket"].list_all()
            rows = []
            for e in entries:
                rows.append(
                    [
                        e.candidate_id[:16] + "...",
                        e.diff_hash[:12] + "..." if e.diff_hash else "N/A",
                        e.diff_summary[:50] + "..." if len(e.diff_summary) > 50 else e.diff_summary,
                        e.submitted_at[:19],
                        e.status.value.upper(),
                    ]
                )
            return rows
        except Exception:
            return []

    def on_refresh_memory(state: dict):
        """Refresh memory entries list."""
        if not state["memory"]:
            return []

        try:
            entries = state["memory"].list_all()
            rows = []
            for e in entries[-20:]:  # Last 20
                rows.append(
                    [
                        e.entry_id[:16] + "...",
                        e.experiment_id[:16] + "...",
                        e.outcome.value,
                        e.diff_hash[:12] + "..." if e.diff_hash else "N/A",
                        e.timestamp[:19],
                    ]
                )
            return rows
        except Exception:
            return []

    # Wire up handlers with session-isolated state
    explorer_state = components["explorer_state"]

    components["init_explorer_btn"].click(
        fn=on_init_explorer,
        inputs=[components["sandbox_root"], components["trusted_repo"], explorer_state],
        outputs=[components["explorer_status"], explorer_state],
    )

    components["create_sandbox_btn"].click(
        fn=on_create_sandbox,
        inputs=[components["source_project"], explorer_state],
        outputs=[components["sandbox_status"], components["cleanup_sandbox_btn"], explorer_state],
    )

    components["cleanup_sandbox_btn"].click(
        fn=on_cleanup_sandbox,
        inputs=[explorer_state],
        outputs=[components["sandbox_status"], components["cleanup_sandbox_btn"], explorer_state],
    )

    components["cleanup_all_btn"].click(
        fn=on_cleanup_all,
        inputs=[explorer_state],
        outputs=[components["sandbox_status"], explorer_state],
    )

    components["run_experiment_btn"].click(
        fn=on_run_experiment,
        inputs=[
            components["exp_name"],
            components["exp_description"],
            components["exp_commands"],
            components["exp_hypothesis"],
            components["exp_timeout"],
            explorer_state,
        ],
        outputs=[
            components["experiment_result"],
            components["diff_preview"],
            components["save_to_memory_btn"],
            components["submit_promotion_btn"],
            explorer_state,
        ],
    )

    components["save_to_memory_btn"].click(
        fn=on_save_to_memory,
        inputs=[explorer_state],
        outputs=[
            components["promotion_status"],
            components["submit_promotion_btn"],
            explorer_state,
        ],
    )

    components["submit_promotion_btn"].click(
        fn=on_submit_promotion,
        inputs=[explorer_state],
        outputs=[components["promotion_status"], explorer_state],
    )

    components["refresh_promotions_btn"].click(
        fn=on_refresh_promotions,
        inputs=[explorer_state],
        outputs=[components["pending_promotions"]],
    )

    components["refresh_memory_btn"].click(
        fn=on_refresh_memory,
        inputs=[explorer_state],
        outputs=[components["memory_entries"]],
    )
