from __future__ import annotations

"""Cycle Report Generator for human-readable Markdown reports.

Phase 5.B: Human-Readable Cycle Reports
Phase 5.E: Memory-Informed Proposal Scorecard integration
Phase 5.F: Files Affected & Memory Signals sections

This module generates passive, read-only Markdown reports from completed
cycle state. Reports are for human review only - agents do not read them.

CONSTRAINTS:
- Logging only - no agent logic changes
- Derived ONLY from existing structured state
- NO new autonomy or decision making
- Scorecard is ADVISORY ONLY (Phase 5.E)
- Memory signals are ADVISORY ONLY (Phase 5.F)
"""

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from odibi.agents.core.cycle import CycleState
    from odibi.agents.core.evidence import ExecutionEvidence
    from odibi.agents.core.proposal_scorecard import ProposalScorecard
    from odibi.agents.core.review_context import AdvisoryContext


class CycleReportGenerator:
    """Generates Markdown reports from completed cycle state.

    Reports are written to .odibi/reports/cycle_<cycle_id>.md
    """

    def __init__(self, odibi_root: str):
        """Initialize report generator.

        Args:
            odibi_root: Path to .odibi directory.
        """
        self.odibi_root = odibi_root
        self.reports_dir = os.path.join(odibi_root, "reports")
        self._scorecard: "ProposalScorecard | None" = None
        self._advisory_context: "AdvisoryContext | None" = None
        self._execution_evidence: dict[str, "ExecutionEvidence"] = {}

    def set_scorecard(self, scorecard: "ProposalScorecard") -> None:
        """Attach a proposal scorecard to include in the report.

        The scorecard is ADVISORY ONLY - it does not influence
        any decisions or execution flow.

        Args:
            scorecard: The ProposalScorecard to attach.
        """
        self._scorecard = scorecard

    def set_advisory_context(self, advisory: "AdvisoryContext") -> None:
        """Attach advisory context for Phase 5.F report sections.

        The advisory context is READ-ONLY and does not influence
        any decisions or execution flow.

        Args:
            advisory: The AdvisoryContext to attach.
        """
        self._advisory_context = advisory

    def set_execution_evidence(self, step_name: str, evidence: "ExecutionEvidence") -> None:
        """Attach execution evidence for a step (Phase 6).

        The evidence is used to generate the Evidence section in reports.
        This is informational only and does not affect decisions.

        Args:
            step_name: The step name (e.g., "env_validation").
            evidence: The ExecutionEvidence from that step.
        """
        self._execution_evidence[step_name] = evidence

    def _ensure_reports_dir(self) -> None:
        """Create reports directory if it doesn't exist."""
        os.makedirs(self.reports_dir, exist_ok=True)

    def generate_report(self, state: "CycleState") -> str:
        """Generate Markdown report from cycle state.

        Args:
            state: Completed CycleState.

        Returns:
            Markdown report content.
        """
        sections = [
            self._generate_header(state),
            self._generate_learning_mode_disclaimer(state),
            self._generate_metadata(state),
            self._generate_source_resolution(state),
            self._generate_source_selection_policy(state),
            self._generate_execution_evidence(state),
            self._generate_projects_exercised(state),
            self._generate_observations(state),
            self._generate_improvements(state),
            self._generate_files_affected(state),
            self._generate_proposal_scorecard(),
            self._generate_memory_signals(),
            self._generate_regression_results(state),
            self._generate_final_status(state),
            self._generate_conclusions(state),
        ]
        return "\n\n".join(s for s in sections if s)

    def write_report(self, state: "CycleState") -> str:
        """Write report to disk.

        Args:
            state: Completed CycleState.

        Returns:
            Path to written report file.
        """
        self._ensure_reports_dir()
        report_content = self.generate_report(state)
        report_path = os.path.join(self.reports_dir, f"cycle_{state.cycle_id}.md")

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_content)

        return report_path

    def _generate_header(self, state: "CycleState") -> str:
        """Generate report header."""
        return f"# Cycle Report: {state.cycle_id}"

    def _generate_learning_mode_disclaimer(self, state: "CycleState") -> str:
        """Generate Learning Mode disclaimer (Phase 9.A).

        For learning-mode cycles (max_improvements=0), displays a prominent
        disclaimer that no code changes were applied.
        """
        if not state.config.is_learning_mode:
            return ""

        lines = [
            "> **🔍 Learning Mode — No Changes Applied**",
            ">",
            "> This cycle ran in **observation-only mode**. The system:",
            "> - ✅ Executed pipelines against frozen datasets",
            "> - ✅ Collected execution evidence",
            "> - ✅ Generated observations and patterns",
            "> - ❌ Did NOT propose any code changes",
            "> - ❌ Did NOT modify any files",
            "> - ❌ Did NOT execute ImprovementAgent or ReviewerAgent",
            ">",
            "> All learnings are indexed for future reference but no autonomous",
            "> improvements were made.",
        ]
        return "\n".join(lines)

    def _generate_metadata(self, state: "CycleState") -> str:
        """Generate Cycle Metadata section."""
        config = state.config
        lines = [
            "## Cycle Metadata",
            "",
            f"- **Cycle ID:** `{state.cycle_id}`",
            f"- **Mode:** {state.mode}",
            f"- **Started:** {state.started_at}",
            f"- **Completed:** {state.completed_at or 'N/A'}",
            f"- **Project Root:** `{config.project_root}`",
            f"- **Task:** {config.task_description}",
            f"- **Max Improvements:** {config.max_improvements}",
            f"- **Learning Mode:** {'Yes' if config.is_learning_mode else 'No'}",
        ]

        if hasattr(state, "profile_id") and state.profile_id:
            lines.extend(
                [
                    "",
                    "### Cycle Profile",
                    "",
                    f"- **Profile ID:** `{state.profile_id}`",
                    f"- **Profile Name:** {state.profile_name}",
                    f"- **Profile Hash:** `{state.profile_hash}`",
                ]
            )

        return "\n".join(lines)

    def _generate_source_resolution(self, state: "CycleState") -> str:
        """Generate Source Resolution section (Phase 8.B).

        Displays source pool selection for the cycle:
        - Selected pools and tiers
        - Selection policy and strategy
        - Determinism hashes for verification
        """
        source_resolution = getattr(state, "source_resolution", None)
        if not source_resolution:
            return ""

        lines = [
            "## Source Resolution",
            "",
            "_Source pools resolved at cycle start (Phase 8.B - read-only, no execution)._",
            "",
        ]

        # Pool selection summary
        pool_names = source_resolution.get("selected_pool_names", [])
        pool_count = len(pool_names)
        if pool_count > 0:
            lines.append(f"### Selected Pools ({pool_count})")
            lines.append("")
            for name in pool_names:
                lines.append(f"- {name}")
            lines.append("")
        else:
            lines.append("_No pools selected._")
            lines.append("")

        # Resolution details table
        lines.append("### Resolution Details")
        lines.append("")
        lines.append("| Property | Value |")
        lines.append("|----------|-------|")
        lines.append(f"| Policy | `{source_resolution.get('policy_id', 'N/A')}` |")
        lines.append(f"| Strategy | `{source_resolution.get('selection_strategy', 'N/A')}` |")
        lines.append(
            f"| Tiers Used | {', '.join(source_resolution.get('tiers_used', [])) or 'N/A'} |"
        )
        lines.append(f"| Max Sources | {source_resolution.get('max_sources', 'N/A')} |")
        lines.append(
            f"| Require Frozen | {'Yes' if source_resolution.get('require_frozen') else 'No'} |"
        )
        lines.append(f"| Allow Messy | {'Yes' if source_resolution.get('allow_messy') else 'No'} |")
        lines.append("")

        # Statistics
        pools_considered = source_resolution.get("pools_considered", 0)
        pools_eligible = source_resolution.get("pools_eligible", 0)
        pools_excluded = source_resolution.get("pools_excluded", 0)
        lines.append(
            f"**Pool Statistics:** {pools_considered} considered → {pools_eligible} eligible → {pool_count} selected ({pools_excluded} excluded)"
        )
        lines.append("")

        # Determinism hashes
        selection_hash = source_resolution.get("selection_hash", "")
        input_hash = source_resolution.get("input_hash", "")
        if selection_hash or input_hash:
            lines.append("### Determinism Verification")
            lines.append("")
            lines.append("_Same inputs → Same selection (verified via hashes)._")
            lines.append("")
            if selection_hash:
                lines.append(f"- **Selection Hash:** `{selection_hash}`")
            if input_hash:
                lines.append(f"- **Input Hash:** `{input_hash}`")
            lines.append(f"- **Context ID:** `{source_resolution.get('context_id', 'N/A')}`")
            lines.append(f"- **Resolved At:** {source_resolution.get('resolved_at', 'N/A')}")

        return "\n".join(lines)

    def _generate_source_selection_policy(self, state: "CycleState") -> str:
        """Generate Source Selection Policy section (Phase 8.C).

        Displays policy metadata for the cycle:
        - Policy name and version
        - Policy hash for determinism verification
        - Validation status and warnings
        - Policy constraints
        """
        source_resolution = getattr(state, "source_resolution", None)
        if not source_resolution:
            return ""

        policy_hash = source_resolution.get("policy_hash", "")
        policy_validation_status = source_resolution.get("policy_validation_status", "")

        if not policy_hash and not policy_validation_status:
            return ""

        lines = [
            "## Source Selection Policy",
            "",
            "_Policy configuration validated at cycle start (Phase 8.C)._",
            "",
        ]

        policy_id = source_resolution.get("policy_id", "N/A")
        policy_version = source_resolution.get("policy_version", "N/A")

        lines.append("### Policy Details")
        lines.append("")
        lines.append("| Property | Value |")
        lines.append("|----------|-------|")
        lines.append(f"| Policy ID | `{policy_id}` |")
        lines.append(f"| Version | `{policy_version}` |")
        lines.append(f"| Hash | `{policy_hash or 'N/A'}` |")
        lines.append(f"| Validation | {policy_validation_status or 'N/A'} |")
        lines.append("")

        # Policy constraints
        lines.append("### Constraints")
        lines.append("")
        lines.append("| Constraint | Value |")
        lines.append("|------------|-------|")
        lines.append(f"| Max Sources | {source_resolution.get('max_sources', 'N/A')} |")
        lines.append(
            f"| Require Frozen | {'Yes' if source_resolution.get('require_frozen') else 'No'} |"
        )
        lines.append(f"| Allow Messy | {'Yes' if source_resolution.get('allow_messy') else 'No'} |")
        lines.append(
            f"| Allow Tier Mixing | {'Yes' if source_resolution.get('allow_tier_mixing') else 'No'} |"
        )
        lines.append(
            f"| Allowed Tiers | {', '.join(source_resolution.get('allowed_tiers', [])) or 'N/A'} |"
        )
        lines.append("")

        # Warnings
        policy_warnings = source_resolution.get("policy_warnings", [])
        if policy_warnings:
            lines.append("### ⚠️ Warnings")
            lines.append("")
            for warning in policy_warnings:
                lines.append(f"- {warning}")
            lines.append("")

        # Determinism note
        if policy_hash:
            lines.append("### Determinism Guarantee")
            lines.append("")
            lines.append(
                "_Same policy hash + same pool index → same selection. "
                "Re-run with identical inputs to verify._"
            )

        return "\n".join(lines)

    def _generate_execution_evidence(self, state: "CycleState") -> str:
        """Generate Execution Evidence section (Phase 6).

        Lists all execution evidence with commands, exit codes,
        and links to artifact files. Does NOT paste full output.
        """
        if not self._execution_evidence:
            return ""

        lines = [
            "## Execution Evidence",
            "",
            "_Evidence from real ExecutionGateway calls. Full outputs in linked artifacts._",
            "",
            "| Step | Command | Exit Code | Status | Hash | Artifacts |",
            "|------|---------|-----------|--------|------|-----------|",
        ]

        for step_name, evidence in self._execution_evidence.items():
            command = (
                evidence.raw_command[:40] + "..."
                if len(evidence.raw_command) > 40
                else evidence.raw_command
            )
            command = command.replace("|", "\\|")

            artifact_links = []
            for artifact in evidence.artifacts:
                artifact_links.append(f"[{artifact.artifact_type}]({artifact.path})")
            artifacts_str = ", ".join(artifact_links) if artifact_links else "_none_"

            lines.append(
                f"| {step_name} | `{command}` | {evidence.exit_code} | "
                f"{evidence.status.value} | `{evidence.evidence_hash}` | {artifacts_str} |"
            )

        lines.append("")
        lines.append(
            f"_Total duration: {sum(e.duration_seconds for e in self._execution_evidence.values()):.2f}s_"
        )

        return "\n".join(lines)

    def _generate_projects_exercised(self, state: "CycleState") -> str:
        """Generate Projects Exercised section."""
        lines = ["## Projects Exercised", ""]

        golden_projects = state.config.golden_projects
        if golden_projects:
            for gp in golden_projects:
                desc = f" - {gp.description}" if gp.description else ""
                lines.append(f"- **{gp.name}:** `{gp.path}`{desc}")
        else:
            lines.append("_No golden projects configured._")

        return "\n".join(lines)

    def _generate_observations(self, state: "CycleState") -> str:
        """Generate Observations section from ObserverOutput."""
        lines = ["## Observations", ""]

        observer_log = self._get_step_log(state, "observation")
        if not observer_log:
            lines.append("_No observations recorded._")
            return "\n".join(lines)

        observer_output = observer_log.metadata.get("observer_output")
        if observer_output and isinstance(observer_output, dict):
            issues = observer_output.get("issues", [])
            if issues:
                for issue in issues:
                    severity = issue.get("severity", "UNKNOWN")
                    issue_type = issue.get("type", "UNKNOWN")
                    location = issue.get("location", "unknown")
                    description = issue.get("description", "")
                    lines.append(f"- **[{severity}] {issue_type}** at `{location}`")
                    if description:
                        lines.append(f"  - {description}")
            else:
                lines.append("_No issues observed._")

            summary = observer_output.get("observation_summary", "")
            if summary:
                lines.append("")
                lines.append(f"**Summary:** {summary}")
        else:
            output_summary = observer_log.output_summary
            if output_summary:
                lines.append(output_summary[:500])
            else:
                lines.append("_No observation data available._")

        return "\n".join(lines)

    def _generate_improvements(self, state: "CycleState") -> str:
        """Generate Improvements section."""
        lines = ["## Improvements", ""]

        approved = state.improvements_approved
        rejected = state.improvements_rejected
        total = approved + rejected

        if total == 0:
            lines.append("_No improvements were proposed during this cycle._")
            return "\n".join(lines)

        lines.append(f"- **Approved:** {approved}")
        lines.append(f"- **Rejected:** {rejected}")
        lines.append("")

        improvement_log = self._get_step_log(state, "improvement")
        if improvement_log and improvement_log.metadata:
            proposal = improvement_log.metadata.get("proposal")
            if proposal and isinstance(proposal, dict):
                title = proposal.get("title", "Untitled")
                rationale = proposal.get("rationale", "")
                status = "✅ Approved" if state.review_approved else "❌ Rejected"

                lines.append(f"### {title}")
                lines.append(f"**Status:** {status}")
                if rationale:
                    lines.append(f"**Rationale:** {rationale}")

        return "\n".join(lines)

    def _generate_files_affected(self, state: "CycleState") -> str:
        """Generate Files Affected (Summary) section (Phase 5.F).

        Lists file changes without code content.
        Advisory disclaimer is included.
        """
        improvement_log = self._get_step_log(state, "improvement")
        if not improvement_log or not improvement_log.metadata:
            return ""

        proposal = improvement_log.metadata.get("proposal")
        if not proposal or not isinstance(proposal, dict):
            return ""

        changes = proposal.get("changes", [])
        if not changes:
            return ""

        lines = [
            "## Files Affected (Summary)",
            "",
            "_Advisory only — no impact on approval_",
            "",
            "| File | Change | Description |",
            "|------|--------|-------------|",
        ]

        for change in changes:
            file_path = change.get("file", "unknown")
            before = change.get("before", "")
            after = change.get("after", "")

            if not before and after:
                change_type = "ADD"
                description = "New content added"
            elif before and not after:
                change_type = "DELETE"
                description = "Content removed"
            else:
                change_type = "MODIFY"
                description = "Content modified"

            lines.append(f"| `{file_path}` | {change_type} | {description} |")

        return "\n".join(lines)

    def _generate_proposal_scorecard(self) -> str:
        """Generate Proposal Scorecard section (Phase 5.E).

        The scorecard is ADVISORY ONLY. It provides context
        but does NOT influence decisions or execution.
        """
        if self._scorecard is None:
            return ""

        return self._scorecard.to_human_readable()

    def _generate_memory_signals(self) -> str:
        """Generate Memory Signals section (Phase 5.F).

        Displays similarity, risk, and novelty from advisory context.
        Explicit disclaimer that this is advisory only.
        """
        if self._advisory_context is None and self._scorecard is None:
            return ""

        lines = [
            "## Memory Signals",
            "",
            "_⚠️ Advisory only — no impact on approval. This information is contextual "
            "and does not approve, reject, or prioritize changes._",
            "",
        ]

        scorecard = self._scorecard
        if self._advisory_context and self._advisory_context.scorecard:
            scorecard = self._advisory_context.scorecard

        if scorecard:
            lines.append("| Signal | Level | Evidence |")
            lines.append("|--------|-------|----------|")
            lines.append(
                f"| Similarity | {scorecard.similarity.level.value} | "
                f"{scorecard.similarity.evidence} |"
            )
            lines.append(
                f"| Historical Risk | {scorecard.risk.level.value} | {scorecard.risk.evidence} |"
            )
            lines.append(
                f"| Novelty | {scorecard.novelty.level.value} | {scorecard.novelty.evidence} |"
            )

            if scorecard.golden_coverage.projects:
                lines.append("")
                lines.append(
                    f"**Golden Coverage:** {', '.join(scorecard.golden_coverage.projects)}"
                )
        else:
            lines.append("_No memory signals available._")

        return "\n".join(lines)

    def _generate_regression_results(self, state: "CycleState") -> str:
        """Generate Regression Results section."""
        lines = ["## Regression Results", ""]

        if not state.config.golden_projects:
            lines.append("_No golden projects configured for regression testing._")
            return "\n".join(lines)

        results = state.golden_project_results
        if not results:
            lines.append("_Regression checks were not executed._")
            return "\n".join(lines)

        passed = sum(1 for r in results if r.get("status") == "PASSED")
        failed = sum(1 for r in results if r.get("status") in ("FAILED", "ERROR"))

        lines.append(f"**Summary:** {passed} passed, {failed} failed")
        lines.append("")

        if state.regressions_detected > 0:
            lines.append("### ⚠️ REGRESSIONS DETECTED")
            lines.append("")

        for result in results:
            name = result.get("name", "unknown")
            status = result.get("status", "UNKNOWN")
            icon = "✅" if status == "PASSED" else "❌"
            lines.append(f"- {icon} **{name}:** {status}")

            if status in ("FAILED", "ERROR"):
                error_msg = result.get("error_message", "")
                if error_msg:
                    lines.append(f"  - Error: {error_msg[:200]}")

        return "\n".join(lines)

    def _generate_final_status(self, state: "CycleState") -> str:
        """Generate Final Status section."""
        lines = ["## Final Status", ""]

        final_status = state.get_final_status()
        icon = "✅" if final_status == "SUCCESS" else ("⚠️" if "PARTIAL" in final_status else "❌")

        lines.append(f"**Status:** {icon} {final_status}")
        lines.append("")
        lines.append(f"- **Completed:** {'Yes' if state.completed else 'No'}")
        lines.append(f"- **Interrupted:** {'Yes' if state.interrupted else 'No'}")
        if state.interrupted:
            lines.append(f"- **Interrupt Reason:** {state.interrupt_reason}")
        lines.append(f"- **Exit Status:** {state.exit_status or 'N/A'}")
        lines.append(f"- **Steps Completed:** {state.current_step_index}")
        lines.append(f"- **Convergence Reached:** {'Yes' if state.convergence_reached else 'No'}")

        return "\n".join(lines)

    def _generate_conclusions(self, state: "CycleState") -> str:
        """Generate Conclusions section from convergence output."""
        lines = ["## Conclusions", ""]

        if state.summary:
            lines.append(state.summary)
        else:
            lines.append("_No conclusions available._")

        return "\n".join(lines)

    def _get_step_log(self, state: "CycleState", step_name: str):
        """Get log entry for a specific step.

        Args:
            state: Cycle state.
            step_name: Step name to find.

        Returns:
            CycleStepLog or None.
        """
        for log in state.logs:
            if log.step == step_name:
                return log
        return None
