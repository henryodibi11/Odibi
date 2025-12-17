"""Issue Discovery Panel for Phase 11.

Provides UI for:
- Running passive issue discovery scans
- Viewing discovered issues with evidence
- Inspecting issue details (READ-ONLY)

PHASE 11 INVARIANTS (READ-ONLY):
- Detection only — no automatic modifications
- No improvement execution from this panel
- No file mutations
- No calls to Controlled Improvement logic
- This phase is inspection-only

The panel surfaces issues for visibility and trust, not automation.
"""

from typing import Any, Callable, Optional

import gradio as gr

from ..utils import get_odibi_root


def create_issue_discovery_panel() -> tuple[gr.Column, dict[str, Any]]:
    """Create the issue discovery panel (read-only inspection).

    Returns:
        Tuple of (Gradio Column, dict of component references).
    """
    components: dict[str, Any] = {}

    with gr.Column(visible=False) as discovery_column:
        components["discovery_column"] = discovery_column

        gr.Markdown("## 🔍 Issue Discovery (Read-Only)")

        gr.Markdown(
            """
            **Passive Issue Detection** — Scan your codebase to identify potential issues.

            **This panel is read-only:**
            - ✅ Scan and detect issues
            - ✅ View issue details and evidence
            - ✅ Inspect severity and confidence
            - ❌ No automatic fixes
            - ❌ No file modifications
            """
        )

        with gr.Row():
            components["discovery_project_root"] = gr.Textbox(
                label="Project Root",
                placeholder="d:/odibi/examples",
                info="Directory to scan for issues",
                scale=2,
            )
            components["run_discovery"] = gr.Button(
                "🔍 Scan for Issues",
                variant="primary",
                scale=1,
            )

        components["discovery_status"] = gr.Markdown(
            value="**Status:** Not scanned",
        )

        with gr.Accordion("📋 Discovered Issues", open=True) as issues_accordion:
            components["issues_accordion"] = issues_accordion

            components["issues_summary"] = gr.Markdown(
                value="_No issues discovered yet. Run a scan to detect potential issues._",
            )

            components["issues_list"] = gr.Dataframe(
                headers=[
                    "ID",
                    "Type",
                    "Severity",
                    "Location",
                    "Description",
                    "Confidence",
                ],
                datatype=["str", "str", "str", "str", "str", "str"],
                interactive=False,
                wrap=True,
                value=[],
                visible=False,
            )

        with gr.Accordion(
            "📊 Issue Details (Select row above)", open=False, visible=False
        ) as details_accordion:
            components["details_accordion"] = details_accordion

            components["issue_detail_view"] = gr.Markdown(
                value="_Click on an issue in the table above to view details_",
            )

            with gr.Accordion("📎 Evidence", open=False) as evidence_accordion:
                components["evidence_accordion"] = evidence_accordion
                components["issue_evidence"] = gr.JSON(
                    label="Evidence Data",
                    value=None,
                )

        gr.Markdown(
            """
            ---
            ℹ️ *Issues shown here are detected passively from config analysis and execution output.
            No changes will be made. To address issues, use the Controlled Improvement panel separately.*
            """
        )

    return discovery_column, components


def setup_issue_discovery_handlers(
    discovery_components: dict[str, Any],
    guided_components: dict[str, Any],
    get_runner: Callable,
    chat_components: Optional[dict[str, Any]] = None,
) -> None:
    """Set up event handlers for issue discovery panel (read-only).

    Args:
        discovery_components: Components from create_issue_discovery_panel.
        guided_components: Components from create_guided_execution_panel.
        get_runner: Function to get AgentRunner instance.
        chat_components: Optional chat components for activity feed updates.
    """
    _current_discovery_result = [None]
    _current_issues_map = [{}]

    def run_issue_discovery(project_root: str):
        """Run passive issue discovery scan (read-only, no mutations)."""
        if not project_root or not project_root.strip():
            return (
                "**Status:** ❌ Please specify a project root",
                "_No scan performed_",
                [],
                gr.update(visible=False),
                gr.update(visible=False),
            )

        try:
            from odibi.agents.core.issue_discovery import IssueDiscoveryManager

            runner = get_runner()
            config = runner.config if runner and hasattr(runner, "config") else None
            odibi_root = get_odibi_root(config)

            manager = IssueDiscoveryManager(odibi_root)
            result = manager.run_discovery(project_root.strip())

            _current_discovery_result[0] = result
            _current_issues_map[0] = {i.issue_id: i for i in result.issues}

            if not result.issues:
                return (
                    f"**Status:** ✅ Scan complete — No issues found in {len(result.files_scanned)} files",
                    "_No issues discovered._",
                    [],
                    gr.update(visible=False),
                    gr.update(visible=False),
                )

            severity_counts = {
                "HIGH": sum(1 for i in result.issues if i.severity.value == "HIGH"),
                "MEDIUM": sum(1 for i in result.issues if i.severity.value == "MEDIUM"),
                "LOW": sum(1 for i in result.issues if i.severity.value == "LOW"),
            }

            harness_note = ""
            if result.harness_issues_excluded > 0:
                harness_note = f"\n\n*Note: {result.harness_issues_excluded} issue(s) in learning harness files are shown but cannot be improved.*"

            summary = f"""
**Scan Complete** — {result.total_issues} issue(s) found in {len(result.files_scanned)} files

| Severity | Count |
|----------|-------|
| 🔴 HIGH | {severity_counts["HIGH"]} |
| 🟡 MEDIUM | {severity_counts["MEDIUM"]} |
| 🟢 LOW | {severity_counts["LOW"]} |

**Actionable Issues:** {result.actionable_issues}
**High Confidence:** {len(result.get_high_confidence_issues())}{harness_note}
            """

            table_data = []
            for issue in result.issues:
                severity_emoji = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(
                    issue.severity.value, "⚪"
                )
                confidence_badge = {"HIGH": "✅", "MEDIUM": "⚠️", "LOW": "❓"}.get(
                    issue.confidence.value, ""
                )

                location_str = issue.location.file_path
                if issue.location.node_name:
                    location_str += f" ({issue.location.node_name})"

                harness_marker = ""
                if issue.location.is_in_learning_harness():
                    harness_marker = " 🔒"

                table_data.append(
                    [
                        issue.issue_id[:8],
                        issue.issue_type.value,
                        f"{severity_emoji} {issue.severity.value}",
                        location_str[:50] + harness_marker,
                        (
                            issue.description[:80] + "..."
                            if len(issue.description) > 80
                            else issue.description
                        ),
                        f"{confidence_badge} {issue.confidence.value}",
                    ]
                )

            return (
                "**Status:** ✅ Scan complete",
                summary,
                table_data,
                gr.update(visible=True),
                gr.update(visible=True),
            )

        except Exception as e:
            return (
                f"**Status:** ❌ Error: {type(e).__name__}: {e}",
                f"_Scan failed: {e}_",
                [],
                gr.update(visible=False),
                gr.update(visible=False),
            )

    discovery_components["run_discovery"].click(
        fn=run_issue_discovery,
        inputs=[discovery_components["discovery_project_root"]],
        outputs=[
            discovery_components["discovery_status"],
            discovery_components["issues_summary"],
            discovery_components["issues_list"],
            discovery_components["issues_list"],
            discovery_components["details_accordion"],
        ],
    )

    def show_issue_details(evt: gr.SelectData):
        """Show details for selected issue from table (read-only display)."""
        if not _current_discovery_result[0]:
            return "_No issues to display_", None

        try:
            row_index = evt.index[0] if isinstance(evt.index, list) else evt.index
            issues = _current_discovery_result[0].issues

            if row_index >= len(issues):
                return "_Issue not found_", None

            issue = issues[row_index]

            harness_warning = ""
            if issue.location.is_in_learning_harness():
                harness_warning = "\n\n🔒 **Protected:** This file is in the learning harness and cannot be modified."

            details = f"""
### {issue.issue_type.value}

**Issue ID:** `{issue.issue_id}`

**Location:**
- **File:** `{issue.location.file_path}`
- **Node:** {issue.location.node_name or "N/A"}
- **Field:** {issue.location.field_path or "N/A"}
- **Lines:** {issue.location.line_range or "N/A"}

**Severity:** {issue.severity.value}
**Confidence:** {issue.confidence.value}

**Description:**
{issue.description}

**Discovered:** {issue.discovered_at}{harness_warning}
            """

            evidence_data = [e.to_dict() for e in issue.evidence]

            return details, evidence_data or None

        except Exception as e:
            return f"_Error displaying issue: {e}_", None

    discovery_components["issues_list"].select(
        fn=show_issue_details,
        outputs=[
            discovery_components["issue_detail_view"],
            discovery_components["issue_evidence"],
        ],
    )


def format_issues_for_chat(issues: list) -> str:
    """Format discovered issues for chat display.

    Args:
        issues: List of DiscoveredIssue objects.

    Returns:
        Markdown-formatted string for chat.
    """
    if not issues:
        return "✅ No issues discovered."

    lines = [f"🔍 **Discovered {len(issues)} issue(s):**\n"]

    for i, issue in enumerate(issues[:5], 1):
        severity_emoji = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(issue.severity.value, "⚪")
        lines.append(f"{i}. {severity_emoji} **{issue.issue_type.value}**")
        lines.append(f"   📍 {issue.location.format_location()}")
        lines.append(f"   📝 {issue.description[:100]}...")
        lines.append("")

    if len(issues) > 5:
        lines.append(f"_...and {len(issues) - 5} more issues_")

    lines.append("\n⚠️ **This is read-only inspection.** No automatic fixes will be applied.")

    return "\n".join(lines)
