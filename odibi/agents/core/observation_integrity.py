"""Observation Integrity for Phase 9.F.

This module provides invariant checks to ensure observer and convergence
agree on failure counts. This prevents false "No issues observed" when
pipelines actually failed.

Key Invariant:
    If convergence reports failed pipelines,
    observer must have emitted ≥1 issue.

This invariant is MECHANICALLY enforced - violations cause fail-fast.
"""

import logging
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)


class ObservationIntegrityError(Exception):
    """Raised when observer-convergence invariant is violated.

    This is a FAIL-FAST error. Learning correctness > convenience.
    """

    def __init__(
        self,
        message: str,
        observer_issue_count: int,
        pipeline_failure_count: int,
        details: Optional[dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.observer_issue_count = observer_issue_count
        self.pipeline_failure_count = pipeline_failure_count
        self.details = details or {}


@dataclass
class ObservationIntegrityCheck:
    """Result of an observation integrity check."""

    is_valid: bool
    """True if observer and convergence agree."""

    observer_issue_count: int
    """Number of issues emitted by observer."""

    pipeline_failure_count: int
    """Number of failed pipelines detected."""

    error_message: Optional[str] = None
    """Error message if validation failed."""

    def __bool__(self) -> bool:
        return self.is_valid


def check_observer_convergence_consistency(
    observer_output: dict[str, Any],
    pipeline_results: list[dict[str, Any]],
    fail_fast: bool = True,
) -> ObservationIntegrityCheck:
    """Check that observer and convergence agree on failure counts.

    Phase 9.F Invariant:
        If pipeline_results contains failures, observer_output must contain ≥1 issue.

    Args:
        observer_output: Dictionary from ObserverOutput.to_dict().
        pipeline_results: List of pipeline execution results.
        fail_fast: If True, raise ObservationIntegrityError on violation.

    Returns:
        ObservationIntegrityCheck with validation result.

    Raises:
        ObservationIntegrityError: If fail_fast=True and invariant is violated.
    """
    # Count failed pipelines
    failed_pipelines = [r for r in pipeline_results if r.get("status", "SUCCESS") != "SUCCESS"]
    pipeline_failure_count = len(failed_pipelines)

    # Count observer issues
    observer_issues = observer_output.get("issues", [])
    observer_issue_count = len(observer_issues)

    # Check invariant: if failures exist, observer must have reported issues
    if pipeline_failure_count > 0 and observer_issue_count == 0:
        error_msg = (
            f"OBSERVER-CONVERGENCE INCONSISTENCY: "
            f"{pipeline_failure_count} pipeline(s) failed, but observer reported 0 issues. "
            f"Observer summary: '{observer_output.get('observation_summary', '')}'"
        )
        logger.error(error_msg)

        if fail_fast:
            raise ObservationIntegrityError(
                message=error_msg,
                observer_issue_count=observer_issue_count,
                pipeline_failure_count=pipeline_failure_count,
                details={
                    "failed_pipelines": [
                        {"name": p.get("pipeline_name"), "status": p.get("status")}
                        for p in failed_pipelines
                    ],
                    "observer_summary": observer_output.get("observation_summary", ""),
                },
            )

        return ObservationIntegrityCheck(
            is_valid=False,
            observer_issue_count=observer_issue_count,
            pipeline_failure_count=pipeline_failure_count,
            error_message=error_msg,
        )

    # Invariant satisfied
    return ObservationIntegrityCheck(
        is_valid=True,
        observer_issue_count=observer_issue_count,
        pipeline_failure_count=pipeline_failure_count,
    )


def validate_observer_output_integrity(
    observer_metadata: dict[str, Any],
    execution_evidence: dict[str, Any],
    fail_fast: bool = True,
) -> ObservationIntegrityCheck:
    """Validate observer output against execution evidence.

    This is the main entry point for integrity validation, called by CycleRunner
    after the observation step completes.

    Args:
        observer_metadata: Metadata from observer AgentResponse.
        execution_evidence: Dictionary from ExecutionEvidence.to_dict().
        fail_fast: If True, raise ObservationIntegrityError on violation.

    Returns:
        ObservationIntegrityCheck with validation result.
    """
    observer_output = observer_metadata.get("observer_output", {})
    pipeline_results = execution_evidence.get("pipeline_results", [])

    return check_observer_convergence_consistency(
        observer_output=observer_output,
        pipeline_results=pipeline_results,
        fail_fast=fail_fast,
    )


def count_pipeline_failures_from_evidence(execution_evidence: dict[str, Any]) -> int:
    """Count the number of failed pipelines in execution evidence.

    Args:
        execution_evidence: Dictionary from ExecutionEvidence.to_dict().

    Returns:
        Number of pipelines with non-SUCCESS status.
    """
    pipeline_results = execution_evidence.get("pipeline_results", [])
    return sum(1 for r in pipeline_results if r.get("status", "SUCCESS") != "SUCCESS")


def validate_observation_summary(
    observation_summary: str,
    pipeline_results: list[dict[str, Any]],
) -> tuple[bool, str]:
    """Validate that observation summary is consistent with pipeline results.

    Checks that "No issues observed" is only returned when all pipelines succeeded.

    Args:
        observation_summary: The observation_summary from ObserverOutput.
        pipeline_results: List of pipeline execution results.

    Returns:
        Tuple of (is_valid, error_message).
    """
    failed_pipelines = [r for r in pipeline_results if r.get("status", "SUCCESS") != "SUCCESS"]

    # Check for false "No issues observed"
    summary_lower = observation_summary.lower()
    if "no issues" in summary_lower and failed_pipelines:
        failed_names = [p.get("pipeline_name", "unknown") for p in failed_pipelines]
        return (
            False,
            f"Observer reported 'No issues observed' but {len(failed_pipelines)} "
            f"pipeline(s) failed: {failed_names}",
        )

    return (True, "")
