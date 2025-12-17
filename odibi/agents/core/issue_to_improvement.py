"""Issue-to-Improvement Integration for Phase 10.D.

This module bridges passive issue discovery (Phase 10.A-C) with the
existing controlled improvement flow (Phase 9.G).

PHASE 10 INVARIANTS:
1. User selection is REQUIRED — no automatic improvement triggers
2. Controlled improvement flow remains UNCHANGED — same rollback, gates, approval
3. Learning harness files remain protected
4. Evidence flows from discovery to improvement task description

Integration Flow:
1. IssueDiscoveryManager.run_discovery() → IssueDiscoveryResult
2. User selects issues → UserIssueSelection
3. UserIssueSelection.to_task_description() → task string
4. Create ControlledImprovementConfig with task_description
5. Run controlled improvement cycle (existing Phase 9.G flow)
"""

import logging
from dataclasses import dataclass
from typing import Any, Optional

from .controlled_improvement import (
    ControlledImprovementConfig,
    ControlledImprovementRunner,
    ImprovementResult,
    ImprovementScope,
    is_learning_harness_path,
)
from .cycle import GoldenProjectConfig
from .issue_discovery import (
    DiscoveredIssue,
    IssueDiscoveryManager,
    IssueDiscoveryResult,
    UserIssueSelection,
)

logger = logging.getLogger(__name__)


class IssueImprovementIntegrationError(Exception):
    """Raised when issue-to-improvement integration fails."""

    pass


@dataclass
class IssueImprovementRequest:
    """Request to improve based on discovered issues.

    This is the bridge between discovery and improvement.
    User intent is captured here.
    """

    selection: UserIssueSelection
    issues: list[DiscoveredIssue]
    project_root: str
    golden_projects: list[GoldenProjectConfig]
    rollback_on_failure: bool = True
    require_regression_check: bool = True

    def validate(self) -> list[str]:
        """Validate the improvement request.

        Returns:
            List of validation errors (empty if valid).
        """
        errors = []

        if not self.selection.selected_issues:
            errors.append("No issues selected for improvement")

        if not self.selection.target_file:
            errors.append("No target file specified")

        if is_learning_harness_path(self.selection.target_file):
            errors.append(
                f"Target file '{self.selection.target_file}' is in protected learning harness"
            )

        selected_ids = set(self.selection.selected_issues)
        available_ids = {i.issue_id for i in self.issues}
        missing = selected_ids - available_ids
        if missing:
            errors.append(f"Selected issue IDs not found: {missing}")

        for issue_id in self.selection.selected_issues:
            issue = next((i for i in self.issues if i.issue_id == issue_id), None)
            if issue and not issue.is_improvement_candidate():
                errors.append(
                    f"Issue {issue_id[:8]} is not an improvement candidate "
                    f"(low confidence or in harness)"
                )

        if self.require_regression_check and not self.golden_projects:
            errors.append("Regression check required but no golden projects specified")

        return errors

    def get_task_description(self) -> str:
        """Generate task description from selection.

        Returns:
            Task description string for controlled improvement.
        """
        return self.selection.to_task_description(self.issues)

    def to_controlled_improvement_config(self) -> ControlledImprovementConfig:
        """Convert to ControlledImprovementConfig.

        Returns:
            Config ready for controlled improvement cycle.

        Raises:
            IssueImprovementIntegrationError: If validation fails.
        """
        errors = self.validate()
        if errors:
            raise IssueImprovementIntegrationError(
                f"Cannot create improvement config: {'; '.join(errors)}"
            )

        scope = ImprovementScope.for_single_file(
            self.selection.target_file,
            description=f"Fix {len(self.selection.selected_issues)} discovered issue(s)",
        )

        return ControlledImprovementConfig(
            project_root=self.project_root,
            task_description=self.get_task_description(),
            improvement_scope=scope,
            golden_projects=self.golden_projects,
            require_regression_check=self.require_regression_check,
            rollback_on_failure=self.rollback_on_failure,
        )


class IssueImprovementIntegrator:
    """Integrates issue discovery with controlled improvement.

    Coordinates the full flow from discovered issues to applied improvements,
    while preserving all Phase 9.G safety guarantees.

    USAGE:
        integrator = IssueImprovementIntegrator(odibi_root="d:/odibi")

        # 1. Discover issues
        discovery_result = integrator.discover_issues(project_root)

        # 2. User selects issues (UI interaction)
        selection = integrator.create_selection(issue_ids, target_file)

        # 3. Run improvement with user approval
        result = integrator.run_improvement(
            selection=selection,
            discovery_result=discovery_result,
            project_root=project_root,
            golden_projects=[...],
        )
    """

    def __init__(
        self,
        odibi_root: str,
        azure_config: Optional[Any] = None,
    ):
        """Initialize the integrator.

        Args:
            odibi_root: Path to odibi repository root.
            azure_config: Optional Azure configuration for agents.
        """
        self.odibi_root = odibi_root
        self.azure_config = azure_config

        self._discovery_manager = IssueDiscoveryManager(odibi_root)
        self._improvement_runner: Optional[ControlledImprovementRunner] = None

        self._last_discovery: Optional[IssueDiscoveryResult] = None
        self._last_selection: Optional[UserIssueSelection] = None

    def discover_issues(self, project_root: str, cycle_id: str = "") -> IssueDiscoveryResult:
        """Run passive issue discovery.

        Args:
            project_root: Path to project to scan.
            cycle_id: Optional cycle ID to associate.

        Returns:
            IssueDiscoveryResult with discovered issues.
        """
        result = self._discovery_manager.run_discovery(project_root, cycle_id)
        self._last_discovery = result

        logger.info(
            f"Discovered {result.total_issues} issues "
            f"({result.actionable_issues} actionable) in {project_root}"
        )

        return result

    def create_selection(
        self,
        issue_ids: list[str],
        target_file: str,
        user_notes: str = "",
        constraints: Optional[dict[str, Any]] = None,
    ) -> UserIssueSelection:
        """Create a user selection from discovered issues.

        Args:
            issue_ids: IDs of issues to address.
            target_file: File to improve.
            user_notes: Optional user notes.
            constraints: Optional constraints.

        Returns:
            UserIssueSelection ready for improvement.

        Raises:
            ValueError: If target_file is in learning harness.
        """
        selection = self._discovery_manager.create_selection(
            issue_ids=issue_ids,
            target_file=target_file,
            user_notes=user_notes,
            constraints=constraints,
        )
        self._last_selection = selection
        return selection

    def run_improvement(
        self,
        selection: UserIssueSelection,
        discovery_result: IssueDiscoveryResult,
        project_root: str,
        golden_projects: Optional[list[GoldenProjectConfig]] = None,
        rollback_on_failure: bool = True,
        require_regression_check: bool = True,
    ) -> ImprovementResult:
        """Run controlled improvement based on user selection.

        This is the main integration point. It:
        1. Validates the selection
        2. Builds improvement config from discovered issues
        3. Runs the standard controlled improvement cycle
        4. Returns the improvement result

        Args:
            selection: User's issue selection.
            discovery_result: Discovery result containing issues.
            project_root: Project root path.
            golden_projects: Optional golden projects for regression.
            rollback_on_failure: Whether to rollback on failure.
            require_regression_check: Whether to require regression checks.

        Returns:
            ImprovementResult from the controlled improvement cycle.

        Raises:
            IssueImprovementIntegrationError: If validation or improvement fails.
        """
        request = IssueImprovementRequest(
            selection=selection,
            issues=discovery_result.issues,
            project_root=project_root,
            golden_projects=golden_projects or [],
            rollback_on_failure=rollback_on_failure,
            require_regression_check=require_regression_check,
        )

        errors = request.validate()
        if errors:
            raise IssueImprovementIntegrationError(
                f"Improvement request validation failed: {'; '.join(errors)}"
            )

        config = request.to_controlled_improvement_config()

        logger.info(
            f"Starting improvement for {len(selection.selected_issues)} issue(s) "
            f"on {selection.target_file}"
        )

        self._improvement_runner = ControlledImprovementRunner(
            odibi_root=self.odibi_root,
            azure_config=self.azure_config,
        )

        state = self._improvement_runner.start_controlled_improvement_cycle(config)

        while not state.is_finished():
            state = self._improvement_runner.run_next_step(state)

        result = self._improvement_runner.get_improvement_result()

        if result is None:
            raise IssueImprovementIntegrationError(
                "Improvement cycle completed but no result available"
            )

        logger.info(f"Improvement completed: {result.status}")

        return result

    def get_improvement_candidates(self) -> list[DiscoveredIssue]:
        """Get issues that can be improvement targets.

        Returns:
            List of improvement-candidate issues from last discovery.
        """
        return self._discovery_manager.get_improvement_candidates(self._last_discovery)

    def preview_task_description(
        self,
        selection: UserIssueSelection,
        discovery_result: Optional[IssueDiscoveryResult] = None,
    ) -> str:
        """Preview the task description that would be generated.

        Args:
            selection: User's issue selection.
            discovery_result: Optional discovery result; uses last if None.

        Returns:
            Task description string.
        """
        result = discovery_result or self._last_discovery
        if result is None:
            return "No discovery result available"

        return selection.to_task_description(result.issues)


def create_improvement_from_discovery(
    odibi_root: str,
    project_root: str,
    issue_ids: list[str],
    target_file: str,
    golden_projects: Optional[list[GoldenProjectConfig]] = None,
    azure_config: Optional[Any] = None,
    user_notes: str = "",
) -> ImprovementResult:
    """Convenience function to run discovery-to-improvement flow.

    This is a simplified interface for common use cases.

    Args:
        odibi_root: Path to odibi repository root.
        project_root: Path to project to scan and improve.
        issue_ids: IDs of issues to address (from prior discovery).
        target_file: File to improve.
        golden_projects: Optional golden projects for regression.
        azure_config: Optional Azure configuration.
        user_notes: Optional user notes.

    Returns:
        ImprovementResult from the improvement cycle.
    """
    integrator = IssueImprovementIntegrator(
        odibi_root=odibi_root,
        azure_config=azure_config,
    )

    discovery_result = integrator.discover_issues(project_root)

    selection = integrator.create_selection(
        issue_ids=issue_ids,
        target_file=target_file,
        user_notes=user_notes,
    )

    return integrator.run_improvement(
        selection=selection,
        discovery_result=discovery_result,
        project_root=project_root,
        golden_projects=golden_projects,
    )
