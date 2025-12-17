"""Phase 12: Escalation Panel for Human-Gated Improvement.

This panel provides the UI for escalating from Issue Discovery to
Controlled Improvement under strict human control.

PHASE 12 UI INVARIANTS:

1. Discovery Panel remains READ-ONLY
   - The existing issue_discovery_panel.py is NOT modified
   - It continues to only display issues without any action capabilities

2. Escalation requires EXPLICIT human actions:
   - Select an issue (from discovery)
   - Confirm target file
   - Provide intent description
   - Explicitly start the improvement

3. NO automatic wiring:
   - No "Fix All" button
   - No automatic selection
   - No background escalation

4. Clear separation:
   - Discovery = read-only inspection
   - Escalation = human-gated action
"""

from pathlib import Path
from typing import Any, Callable, Optional

import gradio as gr

from ..utils import get_odibi_root


def create_escalation_panel() -> tuple[gr.Column, dict[str, Any]]:
    """Create the escalation panel for human-gated improvement.

    This panel is SEPARATE from the discovery panel.
    Users must explicitly choose to escalate.

    Returns:
        Tuple of (Gradio Column, dict of component references).
    """
    components: dict[str, Any] = {}

    with gr.Column(visible=False) as escalation_column:
        components["escalation_column"] = escalation_column

        gr.Markdown("## 🔧 Controlled Improvement (Human-Gated)")

        gr.Markdown(
            """
            **Phase 12: Human-Gated Escalation**

            Use this panel to escalate a discovered issue to controlled improvement.

            **Requirements:**
            1. ✅ Select exactly ONE issue from discovery
            2. ✅ Confirm the target file
            3. ✅ Describe your intent (what should be fixed)
            4. ✅ Review and explicitly confirm

            **Safety Guarantees:**
            - 🔒 Learning harness files are BLOCKED
            - 🔄 All changes can be rolled back
            - ✅ Regression checks before completion
            - 📋 Full audit trail maintained
            """
        )

        with gr.Row():
            with gr.Column(scale=2):
                gr.Markdown("### Step 1: Select Issue")

                components["selected_issue_id"] = gr.Textbox(
                    label="Issue ID",
                    placeholder="Enter issue ID from discovery (e.g., abc123)",
                    info="Copy the ID from the discovered issues table",
                )

                components["selected_issue_type"] = gr.Textbox(
                    label="Issue Type",
                    interactive=False,
                    info="Auto-filled from selection",
                )

                components["selected_issue_description"] = gr.Textbox(
                    label="Issue Description",
                    lines=3,
                    interactive=False,
                    info="Auto-filled from selection",
                )

            with gr.Column(scale=2):
                gr.Markdown("### Step 2: Confirm Target")

                components["target_file"] = gr.Textbox(
                    label="Target File",
                    placeholder="Path to file to improve",
                    info="Must NOT be in learning harness",
                )

                components["file_validation_status"] = gr.Markdown(
                    value="⏳ Enter a file path to validate",
                )

        gr.Markdown("### Step 3: Describe Intent")

        components["user_intent"] = gr.Textbox(
            label="Your Intent",
            placeholder="Describe what you want to fix and why...",
            lines=3,
            info="REQUIRED: Explain what the improvement should do",
        )

        gr.Markdown("### Step 4: Review and Confirm")

        with gr.Accordion("📋 Escalation Preview", open=True) as preview_accordion:
            components["preview_accordion"] = preview_accordion

            components["escalation_preview"] = gr.Markdown(
                value="_Fill in the fields above to see the escalation preview_"
            )

        with gr.Row():
            components["validate_escalation"] = gr.Button(
                "🔍 Validate Escalation",
                variant="secondary",
            )
            components["confirm_escalation"] = gr.Button(
                "✅ Confirm & Start Improvement",
                variant="primary",
                interactive=False,
            )
            components["cancel_escalation"] = gr.Button(
                "❌ Cancel",
                variant="stop",
            )

        components["escalation_status"] = gr.Markdown(value="**Status:** Not started")

        with gr.Accordion(
            "🔄 Improvement Progress", open=False, visible=False
        ) as progress_accordion:
            components["progress_accordion"] = progress_accordion

            components["improvement_progress"] = gr.Markdown(value="_Improvement not started_")

            components["improvement_result"] = gr.JSON(
                label="Improvement Result",
                visible=False,
            )

        with gr.Accordion("📜 Audit Log", open=False) as audit_accordion:
            components["audit_accordion"] = audit_accordion

            components["audit_log"] = gr.Dataframe(
                headers=["Timestamp", "Event", "Details"],
                datatype=["str", "str", "str"],
                interactive=False,
                value=[],
            )

        gr.Markdown(
            """
            ---
            ⚠️ **Important:** This action will modify your codebase.
            All changes are reversible via rollback if regression checks fail.

            🔒 Learning harness files (`**.odibi/learning_harness/**`) are protected
            and cannot be selected as targets.
            """
        )

    return escalation_column, components


def setup_escalation_handlers(
    escalation_components: dict[str, Any],
    discovery_components: dict[str, Any],
    get_runner: Callable,
    chat_components: Optional[dict[str, Any]] = None,
) -> None:
    """Set up event handlers for the escalation panel.

    Args:
        escalation_components: Components from create_escalation_panel.
        discovery_components: Components from create_issue_discovery_panel.
        get_runner: Function to get AgentRunner instance.
        chat_components: Optional chat components for updates.
    """
    _current_discovery_result = [None]
    _current_selection = [None]
    _current_request = [None]
    _validation_passed = [False]

    def on_issue_id_change(issue_id: str):
        """Handle issue ID input - look up issue details."""
        if not issue_id or not issue_id.strip():
            return (
                "",  # issue_type
                "",  # issue_description
                "",  # target_file
                "⏳ Enter an issue ID",
                False,  # can_confirm
            )

        try:
            from odibi.agents.core.issue_discovery import IssueDiscoveryManager

            runner = get_runner()
            config = runner.config if runner and hasattr(runner, "config") else None
            odibi_root = get_odibi_root(config)

            manager = IssueDiscoveryManager(odibi_root)
            result = manager.load_latest_result()

            if not result:
                return (
                    "",
                    "⚠️ No discovery result found. Run issue discovery first.",
                    "",
                    "❌ No discovery result",
                    False,
                )

            _current_discovery_result[0] = result

            # Find the issue
            issue = next(
                (i for i in result.issues if i.issue_id.startswith(issue_id.strip())),
                None,
            )

            if not issue:
                return (
                    "",
                    f"⚠️ Issue '{issue_id}' not found in latest discovery",
                    "",
                    "❌ Issue not found",
                    False,
                )

            # Check if it's an improvement candidate
            if not issue.is_improvement_candidate():
                reason = (
                    "in learning harness"
                    if issue.location.is_in_learning_harness()
                    else "low confidence"
                )
                return (
                    issue.issue_type.value,
                    f"⚠️ {issue.description}\n\n**Cannot improve:** Issue is {reason}",
                    issue.location.file_path,
                    f"❌ Not an improvement candidate ({reason})",
                    False,
                )

            return (
                issue.issue_type.value,
                issue.description,
                issue.location.file_path,
                "✅ Issue found and is improvable",
                False,  # Still need to validate target file
            )

        except Exception as e:
            return (
                "",
                f"❌ Error looking up issue: {e}",
                "",
                f"❌ Error: {e}",
                False,
            )

    escalation_components["selected_issue_id"].change(
        fn=on_issue_id_change,
        inputs=[escalation_components["selected_issue_id"]],
        outputs=[
            escalation_components["selected_issue_type"],
            escalation_components["selected_issue_description"],
            escalation_components["target_file"],
            escalation_components["file_validation_status"],
            escalation_components["confirm_escalation"],
        ],
    )

    def on_target_file_change(target_file: str):
        """Validate target file."""
        if not target_file or not target_file.strip():
            return "⏳ Enter a target file path", False

        from odibi.agents.core.controlled_improvement import is_learning_harness_path
        from pathlib import Path

        target = target_file.strip()

        # Check harness protection
        if is_learning_harness_path(target):
            return (
                "🔒 **BLOCKED:** This file is in the learning harness and CANNOT be modified.",
                False,
            )

        # Check if file exists
        if not Path(target).exists():
            return (
                f"⚠️ File does not exist: {target}",
                False,
            )

        return "✅ Target file is valid", False  # Still need full validation

    escalation_components["target_file"].change(
        fn=on_target_file_change,
        inputs=[escalation_components["target_file"]],
        outputs=[
            escalation_components["file_validation_status"],
            escalation_components["confirm_escalation"],
        ],
    )

    def validate_escalation(issue_id: str, target_file: str, user_intent: str):
        """Full validation of escalation request."""
        _validation_passed[0] = False

        if not issue_id or not issue_id.strip():
            return (
                "❌ **Validation Failed:** No issue selected",
                "_Please select an issue first_",
                False,
                [],
            )

        if not target_file or not target_file.strip():
            return (
                "❌ **Validation Failed:** No target file specified",
                "_Please specify a target file_",
                False,
                [],
            )

        if not user_intent or not user_intent.strip():
            return (
                "❌ **Validation Failed:** No intent description provided",
                "_Please describe what you want to fix_",
                False,
                [],
            )

        try:
            from odibi.agents.core.issue_discovery import IssueDiscoveryManager
            from odibi.agents.core.escalation import (
                create_issue_selection,
                validate_escalation_safety,
                HarnessEscalationBlockedError,
                AmbiguousSelectionError,
            )
            from datetime import datetime

            runner = get_runner()
            config = runner.config if runner and hasattr(runner, "config") else None
            odibi_root = get_odibi_root(config)

            manager = IssueDiscoveryManager(odibi_root)
            result = manager.load_latest_result()

            if not result:
                return (
                    "❌ **Validation Failed:** No discovery result available",
                    "_Run issue discovery first_",
                    False,
                    [],
                )

            # Find the issue
            issue = next(
                (i for i in result.issues if i.issue_id.startswith(issue_id.strip())),
                None,
            )

            if not issue:
                return (
                    f"❌ **Validation Failed:** Issue '{issue_id}' not found",
                    "_Issue ID not found in latest discovery_",
                    False,
                    [],
                )

            # Try to create selection (validates harness, etc.)
            try:
                selection = create_issue_selection(
                    issue=issue,
                    user_intent=user_intent.strip(),
                    target_file=target_file.strip(),
                )
                _current_selection[0] = selection
            except HarnessEscalationBlockedError as e:
                return (
                    f"🔒 **BLOCKED:** {e}",
                    "_Learning harness files cannot be improved_",
                    False,
                    [["", "HARNESS_BLOCKED", str(e)]],
                )
            except AmbiguousSelectionError as e:
                return (
                    f"❌ **Validation Failed:** {e}",
                    "_Please provide complete information_",
                    False,
                    [["", "AMBIGUOUS", str(e)]],
                )

            # Full safety validation
            is_safe, violations = validate_escalation_safety(selection, result)

            if not is_safe:
                return (
                    "❌ **Validation Failed:**\n- " + "\n- ".join(violations),
                    "_Cannot proceed with escalation_",
                    False,
                    [[datetime.now().isoformat(), "VALIDATION_FAILED", v] for v in violations],
                )

            # Validation passed!
            _validation_passed[0] = True

            preview = f"""
### ✅ Escalation Ready

**Issue:** {issue.issue_type.value} (`{issue.issue_id[:8]}`)

**Target File:** `{target_file.strip()}`

**Your Intent:**
> {user_intent.strip()}

**Evidence:**
"""
            for i, ev in enumerate(issue.evidence[:3], 1):
                preview += f"\n{i}. [{ev.evidence_type}] {ev.excerpt[:80]}..."

            preview += "\n\n**Click 'Confirm & Start Improvement' to proceed.**"

            return (
                "✅ **Validation Passed:** Ready to escalate",
                preview,
                True,  # Enable confirm button
                [
                    [
                        datetime.now().isoformat(),
                        "VALIDATION_PASSED",
                        "Escalation validated successfully",
                    ]
                ],
            )

        except Exception as e:
            from datetime import datetime as dt

            return (
                f"❌ **Error:** {type(e).__name__}: {e}",
                f"_Validation error: {e}_",
                False,
                [[dt.now().isoformat(), "ERROR", str(e)]],
            )

    escalation_components["validate_escalation"].click(
        fn=validate_escalation,
        inputs=[
            escalation_components["selected_issue_id"],
            escalation_components["target_file"],
            escalation_components["user_intent"],
        ],
        outputs=[
            escalation_components["escalation_status"],
            escalation_components["escalation_preview"],
            escalation_components["confirm_escalation"],
            escalation_components["audit_log"],
        ],
    )

    def confirm_and_start_improvement(issue_id: str, target_file: str, user_intent: str):
        """Confirm and start the controlled improvement."""
        from datetime import datetime

        if not _validation_passed[0]:
            return (
                "❌ **Error:** Please validate the escalation first",
                "_Click 'Validate Escalation' before confirming_",
                gr.update(visible=False),
                None,
                [[datetime.now().isoformat(), "CONFIRM_FAILED", "Validation not passed"]],
            )

        if not _current_selection[0]:
            return (
                "❌ **Error:** No selection available",
                "_Please validate the escalation first_",
                gr.update(visible=False),
                None,
                [[datetime.now().isoformat(), "CONFIRM_FAILED", "No selection"]],
            )

        try:
            from odibi.agents.core.escalation import HumanGatedEscalator

            runner = get_runner()
            config = runner.config if runner and hasattr(runner, "config") else None
            odibi_root = get_odibi_root(config)
            project_root = str(Path(target_file.strip()).parent) if target_file.strip() else "."

            # Get azure config from runner if available
            azure_config = (
                runner.azure_config if runner and hasattr(runner, "azure_config") else None
            )

            escalator = HumanGatedEscalator(
                odibi_root=odibi_root,
                project_root=project_root,
                azure_config=azure_config,
            )

            # Create escalation request
            request = escalator.create_escalation_request(_current_selection[0])
            _current_request[0] = request

            # Confirm it (this is the explicit human action)
            escalator.confirm_escalation(request)

            # Execute the improvement
            result = escalator.execute_escalation(request)

            # Build result display
            status_emoji = {
                "APPLIED": "✅",
                "NO_PROPOSAL": "ℹ️",
                "REJECTED": "❌",
                "ROLLED_BACK": "🔄",
            }.get(result.status, "⚪")

            progress = f"""
### {status_emoji} Improvement {result.status}

**Improvement ID:** `{result.improvement_id}`

**Files Modified:** {len(result.files_modified)}
"""
            if result.files_modified:
                for f in result.files_modified:
                    progress += f"\n- `{f}`"

            if result.status == "ROLLED_BACK":
                progress += "\n\n**⚠️ Changes were rolled back due to regression check failure.**"
                progress += f"\n**Files Restored:** {len(result.files_restored)}"

            if result.rejection_details:
                progress += f"\n\n**Details:** {result.rejection_details}"

            # Build audit log
            audit_entries = [
                [datetime.now().isoformat(), "CONFIRMED", "User confirmed escalation"],
                [datetime.now().isoformat(), "STARTED", "Controlled improvement started"],
                [
                    datetime.now().isoformat(),
                    result.status,
                    f"Improvement completed: {result.status}",
                ],
            ]

            return (
                f"**Status:** {status_emoji} {result.status}",
                progress,
                gr.update(visible=True),
                result.to_dict() if result else None,
                audit_entries,
            )

        except Exception as e:
            import traceback

            traceback.print_exc()
            return (
                f"❌ **Error:** {type(e).__name__}: {e}",
                f"_Improvement failed: {e}_",
                gr.update(visible=True),
                None,
                [[datetime.now().isoformat(), "ERROR", str(e)]],
            )

    escalation_components["confirm_escalation"].click(
        fn=confirm_and_start_improvement,
        inputs=[
            escalation_components["selected_issue_id"],
            escalation_components["target_file"],
            escalation_components["user_intent"],
        ],
        outputs=[
            escalation_components["escalation_status"],
            escalation_components["improvement_progress"],
            escalation_components["progress_accordion"],
            escalation_components["improvement_result"],
            escalation_components["audit_log"],
        ],
    )

    def cancel_escalation():
        """Cancel and reset the escalation."""
        _current_selection[0] = None
        _current_request[0] = None
        _validation_passed[0] = False

        return (
            "",  # issue_id
            "",  # issue_type
            "",  # issue_description
            "",  # target_file
            "",  # user_intent
            "⏳ Enter an issue ID",  # file_validation_status
            "_Fill in the fields above to see the escalation preview_",  # preview
            "**Status:** Cancelled",  # status
            False,  # confirm button
            gr.update(visible=False),  # progress accordion
        )

    escalation_components["cancel_escalation"].click(
        fn=cancel_escalation,
        outputs=[
            escalation_components["selected_issue_id"],
            escalation_components["selected_issue_type"],
            escalation_components["selected_issue_description"],
            escalation_components["target_file"],
            escalation_components["user_intent"],
            escalation_components["file_validation_status"],
            escalation_components["escalation_preview"],
            escalation_components["escalation_status"],
            escalation_components["confirm_escalation"],
            escalation_components["progress_accordion"],
        ],
    )


def link_discovery_to_escalation(
    discovery_components: dict[str, Any],
    escalation_components: dict[str, Any],
) -> None:
    """Add a button to discovery panel to initiate escalation.

    This adds a non-automatic way for users to move from discovery to escalation.
    The user must explicitly click to use an issue for improvement.
    """

    def on_use_for_improvement(evt: gr.SelectData):
        """Handle 'use for improvement' action from discovery table.

        Returns the issue ID to populate in escalation panel.
        """
        # This is triggered by selecting a row in the discovery table
        # We just return the issue ID - user must still complete escalation manually
        try:
            # The evt contains the selected data
            if evt.value:
                # First column is the issue ID
                return evt.value
        except Exception:
            pass
        return ""

    # Note: The actual button/link to "Use this issue" should be added
    # to the discovery panel, but we don't modify it here to maintain
    # its read-only nature. Instead, users copy the issue ID manually.
