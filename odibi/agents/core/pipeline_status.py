"""Pipeline Execution Status for Phase 9.F: Observation Integrity.

This module provides canonical pipeline execution outcome status values,
ensuring the observer can reliably detect pipeline failures regardless
of shell exit codes.

Key Principle:
- Do NOT infer success from shell exit code alone
- Parse pipeline output for explicit failure markers
- Capture validation errors (Pydantic), runtime errors, and abort conditions
"""

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class PipelineExecutionStatus(str, Enum):
    """Canonical status for pipeline execution outcome.

    These values are mutually exclusive. A pipeline has exactly ONE status.
    """

    SUCCESS = "SUCCESS"
    """Pipeline completed all stages without errors."""

    VALIDATION_FAILED = "VALIDATION_FAILED"
    """Pipeline failed during config/schema validation (e.g., Pydantic errors)."""

    RUNTIME_FAILED = "RUNTIME_FAILED"
    """Pipeline failed during execution (e.g., Spark errors, data errors)."""

    SKIPPED = "SKIPPED"
    """Pipeline was not executed (e.g., precondition not met)."""

    ABORTED = "ABORTED"
    """Pipeline execution was aborted before completion (timeout, interrupt)."""


# Patterns that indicate validation failures
VALIDATION_FAILURE_PATTERNS = [
    r"validation error for \w+",  # Pydantic validation error
    r"\d+ validation error",  # "1 validation error for ProjectConfig"
    r"ValidationError",
    r"Invalid config",
    r"invalid configuration",
    r"schema validation failed",
    r"missing required field",
    r"Input should be a valid",  # Pydantic v2 pattern
    r"field required",  # Pydantic v1 pattern
]

# Patterns that indicate runtime failures
# NOTE: Use word boundaries or specific context to avoid false positives
# like "resume_from_failure=False" matching "FAILURE"
RUNTIME_FAILURE_PATTERNS = [
    r"Pipeline failed",
    r"ERROR:?\s+Pipeline",
    r"RuntimeError",
    r"SparkException",
    r"AnalysisException",
    r"Py4JJavaError",
    r"Job aborted",
    r"Stage \d+ failed",
    r"Task \d+ failed",
    r"OutOfMemoryError",
    r"FileNotFoundException",
    r"PermissionError",
    r"ConnectionError",
    r"Execution failed",
    r"Status:\s*FAILURE",  # Explicit status marker
    r"Pipeline status:\s*FAILURE",
    r"\bFAILURE\b",  # Word boundary to avoid matching 'resume_from_failure'
]

# Patterns that indicate explicit SUCCESS (checked before failure patterns)
SUCCESS_PATTERNS = [
    r"Pipeline SUCCESS:",
    r"Status:\s*SUCCESS",
    r"status=SUCCESS",
    r"Pipeline status:\s*SUCCESS",
]

# Patterns that indicate abort/incomplete execution
ABORT_PATTERNS = [
    r"Timeout",
    r"timed out",
    r"Interrupted",
    r"Killed",
    r"SIGTERM",
    r"SIGKILL",
    r"Aborted",
]


@dataclass
class PipelineValidationError:
    """A single validation error from pipeline config parsing."""

    field_path: str
    """Field path that failed validation (e.g., 'pipelines.0.nodes.2.params.1')"""

    message: str
    """Error message describing what was wrong."""

    expected_type: Optional[str] = None
    """Expected type if this is a type error."""

    actual_value: Optional[str] = None
    """Actual value that failed validation (truncated)."""

    def to_dict(self) -> dict[str, Any]:
        return {
            "field_path": self.field_path,
            "message": self.message,
            "expected_type": self.expected_type,
            "actual_value": self.actual_value,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PipelineValidationError":
        return cls(
            field_path=data.get("field_path", ""),
            message=data.get("message", ""),
            expected_type=data.get("expected_type"),
            actual_value=data.get("actual_value"),
        )


@dataclass
class PipelineExecutionResult:
    """Structured result from a single pipeline execution.

    This captures the canonical execution outcome, not just the exit code.
    The observer uses this to emit issues regardless of shell exit code.
    """

    pipeline_name: str
    """Name or path of the pipeline config."""

    status: PipelineExecutionStatus
    """Canonical execution status."""

    exit_code: int
    """Shell exit code (may be 0 even on logical failure)."""

    validation_errors: list[PipelineValidationError] = field(default_factory=list)
    """List of validation errors if status is VALIDATION_FAILED."""

    runtime_error: Optional[str] = None
    """Runtime error message if status is RUNTIME_FAILED."""

    error_stage: Optional[str] = None
    """Stage where error occurred: 'config_validation', 'execution', 'output'."""

    output_excerpt: str = ""
    """Relevant excerpt from stdout/stderr (truncated)."""

    raw_stdout: str = ""
    """Full stdout (may be truncated)."""

    raw_stderr: str = ""
    """Full stderr (may be truncated)."""

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "status": self.status.value,
            "exit_code": self.exit_code,
            "validation_errors": [e.to_dict() for e in self.validation_errors],
            "runtime_error": self.runtime_error,
            "error_stage": self.error_stage,
            "output_excerpt": self.output_excerpt,
            "raw_stdout": self.raw_stdout[:5000] if self.raw_stdout else "",
            "raw_stderr": self.raw_stderr[:5000] if self.raw_stderr else "",
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PipelineExecutionResult":
        status_str = data.get("status", "SUCCESS")
        try:
            status = PipelineExecutionStatus(status_str)
        except ValueError:
            status = PipelineExecutionStatus.RUNTIME_FAILED

        validation_errors = [
            PipelineValidationError.from_dict(e) for e in data.get("validation_errors", [])
        ]

        return cls(
            pipeline_name=data.get("pipeline_name", ""),
            status=status,
            exit_code=data.get("exit_code", -1),
            validation_errors=validation_errors,
            runtime_error=data.get("runtime_error"),
            error_stage=data.get("error_stage"),
            output_excerpt=data.get("output_excerpt", ""),
            raw_stdout=data.get("raw_stdout", ""),
            raw_stderr=data.get("raw_stderr", ""),
        )

    def is_success(self) -> bool:
        """Check if this pipeline succeeded."""
        return self.status == PipelineExecutionStatus.SUCCESS

    def is_failure(self) -> bool:
        """Check if this pipeline failed (any failure type)."""
        return self.status in (
            PipelineExecutionStatus.VALIDATION_FAILED,
            PipelineExecutionStatus.RUNTIME_FAILED,
            PipelineExecutionStatus.ABORTED,
        )

    def get_failure_summary(self) -> str:
        """Get a human-readable failure summary."""
        if self.status == PipelineExecutionStatus.SUCCESS:
            return "No failure"

        if self.status == PipelineExecutionStatus.VALIDATION_FAILED:
            if self.validation_errors:
                first = self.validation_errors[0]
                return f"Validation failed: {first.field_path} - {first.message}"
            return "Validation failed (unknown reason)"

        if self.status == PipelineExecutionStatus.RUNTIME_FAILED:
            return self.runtime_error or "Runtime failure (unknown reason)"

        if self.status == PipelineExecutionStatus.ABORTED:
            return self.runtime_error or "Execution aborted"

        if self.status == PipelineExecutionStatus.SKIPPED:
            return "Pipeline skipped"

        return "Unknown status"


def parse_validation_errors(output: str) -> list[PipelineValidationError]:
    """Parse Pydantic validation errors from pipeline output.

    Handles Pydantic v1 and v2 error formats.

    Args:
        output: Combined stdout/stderr from pipeline execution.

    Returns:
        List of parsed validation errors.
    """
    errors: list[PipelineValidationError] = []

    # Pattern for Pydantic v2 style: "field_path\n  Input should be..."
    # Example: "pipelines.0.nodes.2.params.1\n  Input should be a valid string"
    pydantic_v2_pattern = r"([a-zA-Z_][a-zA-Z0-9_\.]*(?:\.\d+)*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\s*\n\s*(Input should be [^\n]+)"
    for match in re.finditer(pydantic_v2_pattern, output):
        field_path = match.group(1)
        message = match.group(2).strip()
        errors.append(
            PipelineValidationError(
                field_path=field_path,
                message=message,
            )
        )

    # Pattern for "N validation error(s) for ModelName"
    count_pattern = r"(\d+) validation errors? for (\w+)"
    count_match = re.search(count_pattern, output)

    # If we found validation error indicators but no specific errors parsed,
    # create a generic validation error
    if count_match and not errors:
        count = int(count_match.group(1))
        model = count_match.group(2)
        errors.append(
            PipelineValidationError(
                field_path="<unknown>",
                message=f"{count} validation error(s) for {model}",
            )
        )

    return errors


def detect_pipeline_status(
    exit_code: int,
    stdout: str,
    stderr: str,
) -> tuple[PipelineExecutionStatus, str, list[PipelineValidationError]]:
    """Detect the canonical pipeline status from execution output.

    This is the core detection logic. It does NOT rely solely on exit code.

    Priority order:
    1. Validation failures (highest priority - Pydantic errors)
    2. Explicit SUCCESS patterns (if found, pipeline succeeded)
    3. Abort patterns (timeout, interrupt)
    4. Runtime failure patterns
    5. Non-zero exit code fallback
    6. Default to SUCCESS if exit code 0 and no failure patterns

    Args:
        exit_code: Shell exit code from subprocess.
        stdout: Standard output from pipeline.
        stderr: Standard error from pipeline.

    Returns:
        Tuple of (status, error_message, validation_errors).
    """
    combined = f"{stdout}\n{stderr}"

    # Check for validation failures first (they may have exit code 0 or 1)
    # Validation errors are always failures, even if SUCCESS is also printed
    for pattern in VALIDATION_FAILURE_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            validation_errors = parse_validation_errors(combined)
            error_msg = _extract_error_message(combined, pattern)
            return (
                PipelineExecutionStatus.VALIDATION_FAILED,
                error_msg,
                validation_errors,
            )

    # Check for explicit SUCCESS patterns before failure patterns
    # If pipeline output contains "Pipeline SUCCESS:" or "status=SUCCESS",
    # it succeeded regardless of other patterns like "resume_from_failure"
    has_explicit_success = any(
        re.search(pattern, combined, re.IGNORECASE) for pattern in SUCCESS_PATTERNS
    )

    if has_explicit_success:
        # Explicit success found - pipeline succeeded
        return (PipelineExecutionStatus.SUCCESS, "", [])

    # Check for abort patterns (timeout, interrupt)
    for pattern in ABORT_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            error_msg = _extract_error_message(combined, pattern)
            return (
                PipelineExecutionStatus.ABORTED,
                error_msg,
                [],
            )

    # Check for runtime failure patterns
    for pattern in RUNTIME_FAILURE_PATTERNS:
        if re.search(pattern, combined, re.IGNORECASE):
            error_msg = _extract_error_message(combined, pattern)
            return (
                PipelineExecutionStatus.RUNTIME_FAILED,
                error_msg,
                [],
            )

    # Check exit code as fallback
    if exit_code != 0:
        # Non-zero exit but no recognized pattern - still a failure
        error_msg = f"Exit code {exit_code}"
        if stderr.strip():
            error_msg = stderr.strip()[:500]
        return (
            PipelineExecutionStatus.RUNTIME_FAILED,
            error_msg,
            [],
        )

    # If exit code is 0 and no failure patterns found, it's a success
    return (PipelineExecutionStatus.SUCCESS, "", [])


def _extract_error_message(output: str, trigger_pattern: str) -> str:
    """Extract a meaningful error message near the trigger pattern.

    Args:
        output: Combined stdout/stderr.
        trigger_pattern: The regex pattern that triggered detection.

    Returns:
        Extracted error message (truncated to 500 chars).
    """
    match = re.search(trigger_pattern, output, re.IGNORECASE)
    if not match:
        return "Error detected"

    # Get context around the match (up to 200 chars before, 300 after)
    start = max(0, match.start() - 50)
    end = min(len(output), match.end() + 300)
    excerpt = output[start:end].strip()

    # Find the full line containing the match
    lines = excerpt.split("\n")
    for line in lines:
        if re.search(trigger_pattern, line, re.IGNORECASE):
            return line.strip()[:500]

    return excerpt[:500]


def create_pipeline_result_from_execution(
    pipeline_name: str,
    exit_code: int,
    stdout: str,
    stderr: str,
) -> PipelineExecutionResult:
    """Create a PipelineExecutionResult from raw execution output.

    This is the main entry point for converting ExecutionResult to
    a structured PipelineExecutionResult.

    Args:
        pipeline_name: Name or path of the pipeline.
        exit_code: Shell exit code.
        stdout: Standard output.
        stderr: Standard error.

    Returns:
        Fully populated PipelineExecutionResult.
    """
    status, error_msg, validation_errors = detect_pipeline_status(exit_code, stdout, stderr)

    # Determine error stage
    error_stage = None
    if status == PipelineExecutionStatus.VALIDATION_FAILED:
        error_stage = "config_validation"
    elif status == PipelineExecutionStatus.RUNTIME_FAILED:
        error_stage = "execution"
    elif status == PipelineExecutionStatus.ABORTED:
        error_stage = "execution"

    # Extract relevant excerpt for evidence
    if status != PipelineExecutionStatus.SUCCESS:
        combined = f"{stdout}\n{stderr}"
        # Find the most relevant 500-char excerpt
        output_excerpt = _find_relevant_excerpt(combined, error_msg)
    else:
        output_excerpt = ""

    return PipelineExecutionResult(
        pipeline_name=pipeline_name,
        status=status,
        exit_code=exit_code,
        validation_errors=validation_errors,
        runtime_error=error_msg if error_msg else None,
        error_stage=error_stage,
        output_excerpt=output_excerpt,
        raw_stdout=stdout,
        raw_stderr=stderr,
    )


def _find_relevant_excerpt(output: str, error_msg: str) -> str:
    """Find the most relevant excerpt from output around an error.

    Args:
        output: Combined output.
        error_msg: The error message to search for.

    Returns:
        Relevant excerpt (max 500 chars).
    """
    if not output:
        return ""

    # Try to find the error message in the output
    if error_msg and error_msg in output:
        idx = output.find(error_msg)
        start = max(0, idx - 100)
        end = min(len(output), idx + len(error_msg) + 200)
        return output[start:end].strip()[:500]

    # Look for ERROR lines
    for line in output.split("\n"):
        if "ERROR" in line.upper() or "FAILED" in line.upper():
            # Get this line and 2 lines before/after
            idx = output.find(line)
            start = max(0, idx - 200)
            end = min(len(output), idx + len(line) + 200)
            return output[start:end].strip()[:500]

    # Fallback: last 500 chars
    return output[-500:].strip()
