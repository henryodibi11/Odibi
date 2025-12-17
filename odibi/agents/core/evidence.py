"""Evidence-Based Execution for the Odibi AI Agent Suite.

Phase 6: Evidence-Based Cycles
Phase 9.F: Observation Integrity

This module provides structured evidence from real execution via ExecutionGateway.
Agents MUST NOT fabricate pipeline runs, configs, errors, or durations.

Key Components:
- ExecutionEvidence: Structured schema for execution results
- ArtifactWriter: Persists evidence artifacts to disk
- Deterministic hashing and truncation for reproducibility

Phase 9.F Additions:
- Pipeline execution results are captured with canonical status
- Validation and runtime errors are parsed and structured
- Observer uses pipeline_results to detect failures (not exit code alone)

CONSTRAINTS:
- Evidence MUST come from actual ExecutionGateway calls
- NO_EVIDENCE status used when execution cannot be performed
- Truncation is deterministic (same input -> same output)
- Artifacts written before report generation
- Pipeline failures detected by output parsing, NOT exit code alone
"""

import hashlib
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from .execution import ExecutionResult


DEFAULT_TRUNCATION_LIMIT = 10000
TRUNCATION_MARKER = "\n\n[TRUNCATED - output exceeded limit]"


class EvidenceStatus(str, Enum):
    """Status of execution evidence."""

    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    NO_EVIDENCE = "NO_EVIDENCE"


@dataclass
class ArtifactRef:
    """Reference to a persisted artifact file."""

    path: str
    artifact_type: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ArtifactRef":
        return cls(
            path=data.get("path", ""),
            artifact_type=data.get("artifact_type", "unknown"),
        )


@dataclass
class SourceUsageSummary:
    """Summary of source pool usage during execution (Phase 7.D)."""

    context_id: str = ""
    pools_bound: list[str] = field(default_factory=list)
    pools_used: list[str] = field(default_factory=list)
    files_accessed: int = 0
    integrity_verified: bool = False
    violations_detected: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "context_id": self.context_id,
            "pools_bound": self.pools_bound,
            "pools_used": self.pools_used,
            "files_accessed": self.files_accessed,
            "integrity_verified": self.integrity_verified,
            "violations_detected": self.violations_detected,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SourceUsageSummary":
        return cls(
            context_id=data.get("context_id", ""),
            pools_bound=data.get("pools_bound", []),
            pools_used=data.get("pools_used", []),
            files_accessed=data.get("files_accessed", 0),
            integrity_verified=data.get("integrity_verified", False),
            violations_detected=data.get("violations_detected", 0),
        )


@dataclass
class ExecutionEvidence:
    """Structured evidence from real execution.

    This schema captures the complete execution context including:
    - The exact command that was run
    - Exit code and output (truncated if needed)
    - Timing information
    - References to persisted artifacts
    - Deterministic hash for verification
    - Source pool usage (Phase 7.D)
    - Pipeline execution results with canonical status (Phase 9.F)

    Agents MUST populate this from actual ExecutionGateway calls,
    NEVER from fabricated or imagined outputs.

    Phase 9.F: The Observer MUST use pipeline_results to detect failures,
    NOT rely on exit_code or EvidenceStatus alone. A pipeline may fail
    validation and return exit_code=0, but pipeline_results will show
    VALIDATION_FAILED status.
    """

    raw_command: str
    exit_code: int
    stdout: str
    stderr: str
    started_at: str
    finished_at: str
    duration_seconds: float
    artifacts: list[ArtifactRef] = field(default_factory=list)
    status: EvidenceStatus = EvidenceStatus.SUCCESS
    evidence_hash: str = ""

    # Phase 7.D: Source usage tracking
    source_usage: Optional[SourceUsageSummary] = None

    # Phase 9.F: Structured pipeline execution results
    # This is the authoritative source for pipeline success/failure detection
    pipeline_results: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self):
        if not self.evidence_hash:
            self.evidence_hash = self.compute_hash()

    def compute_hash(self) -> str:
        """Compute deterministic SHA256 hash over execution evidence.

        Hash is computed over: command + stdout + stderr + exit_code
        This provides a unique fingerprint for verification.

        Returns:
            Hexadecimal hash string (first 16 characters).
        """
        content = f"{self.raw_command}|{self.stdout}|{self.stderr}|{self.exit_code}"
        return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            "raw_command": self.raw_command,
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "artifacts": [a.to_dict() for a in self.artifacts],
            "status": self.status.value,
            "evidence_hash": self.evidence_hash,
            # Phase 9.F: Include pipeline execution results
            "pipeline_results": self.pipeline_results,
        }
        # Phase 7.D: Include source usage if present
        if self.source_usage:
            result["source_usage"] = self.source_usage.to_dict()
        return result

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExecutionEvidence":
        """Create from dictionary."""
        artifacts = [ArtifactRef.from_dict(a) for a in data.get("artifacts", [])]
        status_str = data.get("status", "SUCCESS")
        try:
            status = EvidenceStatus(status_str)
        except ValueError:
            status = EvidenceStatus.SUCCESS

        # Phase 7.D: Parse source usage if present
        source_usage_data = data.get("source_usage")
        source_usage = (
            SourceUsageSummary.from_dict(source_usage_data) if source_usage_data else None
        )

        evidence = cls(
            raw_command=data.get("raw_command", ""),
            exit_code=data.get("exit_code", -1),
            stdout=data.get("stdout", ""),
            stderr=data.get("stderr", ""),
            started_at=data.get("started_at", ""),
            finished_at=data.get("finished_at", ""),
            duration_seconds=data.get("duration_seconds", 0.0),
            artifacts=artifacts,
            status=status,
            evidence_hash=data.get("evidence_hash", ""),
            source_usage=source_usage,
            # Phase 9.F: Parse pipeline results
            pipeline_results=data.get("pipeline_results", []),
        )
        if not evidence.evidence_hash:
            evidence.evidence_hash = evidence.compute_hash()
        return evidence

    @classmethod
    def from_json(cls, json_str: str) -> "ExecutionEvidence":
        """Create from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)

    @classmethod
    def from_execution_result(
        cls,
        result: "ExecutionResult",
        started_at: str,
        finished_at: str,
        truncation_limit: int = DEFAULT_TRUNCATION_LIMIT,
    ) -> "ExecutionEvidence":
        """Create ExecutionEvidence from an ExecutionResult.

        Args:
            result: The ExecutionResult from ExecutionGateway.
            started_at: ISO timestamp when execution started.
            finished_at: ISO timestamp when execution finished.
            truncation_limit: Maximum characters for stdout/stderr.

        Returns:
            ExecutionEvidence with truncated outputs and computed hash.
        """
        stdout = truncate_output(result.stdout, truncation_limit)
        stderr = truncate_output(result.stderr, truncation_limit)

        try:
            start_dt = datetime.fromisoformat(started_at)
            end_dt = datetime.fromisoformat(finished_at)
            duration = (end_dt - start_dt).total_seconds()
        except (ValueError, TypeError):
            duration = 0.0

        status = EvidenceStatus.SUCCESS if result.success else EvidenceStatus.FAILURE

        return cls(
            raw_command=result.command,
            exit_code=result.exit_code,
            stdout=stdout,
            stderr=stderr,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration,
            status=status,
        )

    @classmethod
    def no_evidence(cls, reason: str) -> "ExecutionEvidence":
        """Create a NO_EVIDENCE instance when execution cannot be performed.

        Use this when:
        - ExecutionGateway is unavailable
        - Permissions do not allow execution
        - System is in a mode that cannot run tasks

        Args:
            reason: Explanation of why evidence is not available.

        Returns:
            ExecutionEvidence with NO_EVIDENCE status.
        """
        now = datetime.now().isoformat()
        return cls(
            raw_command="",
            exit_code=-1,
            stdout="",
            stderr=reason,
            started_at=now,
            finished_at=now,
            duration_seconds=0.0,
            status=EvidenceStatus.NO_EVIDENCE,
        )

    def is_no_evidence(self) -> bool:
        """Check if this is a NO_EVIDENCE instance."""
        return self.status == EvidenceStatus.NO_EVIDENCE

    def get_summary(self) -> str:
        """Get a brief summary of the evidence.

        Returns:
            Human-readable summary string.
        """
        if self.is_no_evidence():
            return f"NO_EVIDENCE: {self.stderr}"
        summary = (
            f"Command: {self.raw_command[:50]}... | "
            f"Exit: {self.exit_code} | "
            f"Status: {self.status.value} | "
            f"Duration: {self.duration_seconds:.2f}s"
        )
        # Phase 7.D: Include source usage summary
        if self.source_usage:
            summary += f" | Pools: {len(self.source_usage.pools_used)}/{len(self.source_usage.pools_bound)}"
        return summary

    def set_source_usage(
        self,
        context_id: str,
        pools_bound: list[str],
        pools_used: list[str],
        files_accessed: int,
        integrity_verified: bool = False,
        violations_detected: int = 0,
    ) -> None:
        """Set source usage summary for this evidence.

        Args:
            context_id: The execution source context ID
            pools_bound: List of pool IDs that were bound
            pools_used: List of pool IDs that were actually accessed
            files_accessed: Number of files accessed
            integrity_verified: Whether integrity was verified
            violations_detected: Number of source violations
        """
        self.source_usage = SourceUsageSummary(
            context_id=context_id,
            pools_bound=pools_bound,
            pools_used=pools_used,
            files_accessed=files_accessed,
            integrity_verified=integrity_verified,
            violations_detected=violations_detected,
        )

    # ================================================================
    # Phase 9.F: Pipeline Execution Result Methods
    # ================================================================

    def add_pipeline_result(self, result_dict: dict[str, Any]) -> None:
        """Add a pipeline execution result to the evidence.

        Args:
            result_dict: Dictionary from PipelineExecutionResult.to_dict()
        """
        self.pipeline_results.append(result_dict)

    def has_pipeline_failures(self) -> bool:
        """Check if any pipeline in this evidence failed.

        Phase 9.F: This is the authoritative check for pipeline failure.
        Do NOT rely on exit_code or EvidenceStatus alone.

        Returns:
            True if any pipeline has status other than SUCCESS.
        """
        if not self.pipeline_results:
            return False

        for result in self.pipeline_results:
            status = result.get("status", "SUCCESS")
            if status != "SUCCESS":
                return True

        return False

    def get_failed_pipelines(self) -> list[dict[str, Any]]:
        """Get all failed pipeline results.

        Returns:
            List of pipeline results with non-SUCCESS status.
        """
        return [r for r in self.pipeline_results if r.get("status", "SUCCESS") != "SUCCESS"]

    def get_pipeline_failure_count(self) -> int:
        """Get count of failed pipelines.

        Returns:
            Number of pipelines with non-SUCCESS status.
        """
        return len(self.get_failed_pipelines())

    def get_pipeline_failure_summary(self) -> str:
        """Get a summary of all pipeline failures.

        Returns:
            Human-readable summary of failures.
        """
        failures = self.get_failed_pipelines()
        if not failures:
            return "No pipeline failures"

        lines = [f"{len(failures)} pipeline(s) failed:"]
        for f in failures:
            name = f.get("pipeline_name", "unknown")
            status = f.get("status", "UNKNOWN")
            error = f.get("runtime_error", "") or f.get("output_excerpt", "")[:100]
            lines.append(f"  - {name}: {status} - {error}")

        return "\n".join(lines)


def truncate_output(text: str, limit: int = DEFAULT_TRUNCATION_LIMIT) -> str:
    """Truncate text to a maximum length deterministically.

    If text exceeds limit, it is cut and a truncation marker is appended.
    The truncation is deterministic: same input always produces same output.

    Args:
        text: The text to truncate.
        limit: Maximum characters before truncation.

    Returns:
        Original text if under limit, otherwise truncated with marker.
    """
    if not text or len(text) <= limit:
        return text

    cut_point = limit - len(TRUNCATION_MARKER)
    if cut_point < 0:
        cut_point = 0

    return text[:cut_point] + TRUNCATION_MARKER


class ArtifactWriter:
    """Writes execution artifacts to disk.

    Artifacts are stored in a structured directory:
        .odibi/artifacts/<cycle_id>/execution/<step_name>/
            - command.txt: The raw command
            - stdout.log: Standard output
            - stderr.log: Standard error
            - evidence.json: Full evidence JSON

    Artifacts MUST be written before report generation to ensure
    reports can link to valid paths.
    """

    def __init__(self, odibi_root: str):
        """Initialize artifact writer.

        Args:
            odibi_root: Path to .odibi directory.
        """
        self.odibi_root = odibi_root
        self.artifacts_dir = os.path.join(odibi_root, "artifacts")

    def _ensure_dir(self, path: str) -> None:
        """Create directory if it doesn't exist."""
        os.makedirs(path, exist_ok=True)

    def get_artifact_dir(self, cycle_id: str, step_name: str) -> str:
        """Get the artifact directory path for a step.

        Args:
            cycle_id: The cycle ID.
            step_name: The step name (e.g., "env_validation").

        Returns:
            Absolute path to the artifact directory.
        """
        return os.path.join(self.artifacts_dir, cycle_id, "execution", step_name)

    def write_execution_artifacts(
        self,
        cycle_id: str,
        step_name: str,
        evidence: ExecutionEvidence,
    ) -> list[ArtifactRef]:
        """Write execution artifacts to disk.

        Creates:
            - command.txt: The raw command
            - stdout.log: Standard output (full, not truncated)
            - stderr.log: Standard error (full, not truncated)
            - evidence.json: Full evidence JSON

        Args:
            cycle_id: The cycle ID.
            step_name: The step name.
            evidence: The ExecutionEvidence to persist.

        Returns:
            List of ArtifactRef pointing to written files.
        """
        artifact_dir = self.get_artifact_dir(cycle_id, step_name)
        self._ensure_dir(artifact_dir)

        artifacts = []

        command_path = os.path.join(artifact_dir, "command.txt")
        with open(command_path, "w", encoding="utf-8") as f:
            f.write(evidence.raw_command)
        artifacts.append(
            ArtifactRef(
                path=self._relative_path(command_path),
                artifact_type="command",
            )
        )

        stdout_path = os.path.join(artifact_dir, "stdout.log")
        with open(stdout_path, "w", encoding="utf-8") as f:
            f.write(evidence.stdout)
        artifacts.append(
            ArtifactRef(
                path=self._relative_path(stdout_path),
                artifact_type="stdout",
            )
        )

        stderr_path = os.path.join(artifact_dir, "stderr.log")
        with open(stderr_path, "w", encoding="utf-8") as f:
            f.write(evidence.stderr)
        artifacts.append(
            ArtifactRef(
                path=self._relative_path(stderr_path),
                artifact_type="stderr",
            )
        )

        evidence_path = os.path.join(artifact_dir, "evidence.json")
        with open(evidence_path, "w", encoding="utf-8") as f:
            f.write(evidence.to_json())
        artifacts.append(
            ArtifactRef(
                path=self._relative_path(evidence_path),
                artifact_type="evidence",
            )
        )

        return artifacts

    def _relative_path(self, absolute_path: str) -> str:
        """Convert absolute path to relative path from odibi_root.

        Args:
            absolute_path: Absolute file path.

        Returns:
            Path relative to odibi_root.
        """
        base = os.path.dirname(self.odibi_root)
        try:
            return os.path.relpath(absolute_path, base)
        except ValueError:
            return absolute_path

    def read_artifact(self, artifact_ref: ArtifactRef) -> Optional[str]:
        """Read an artifact's contents.

        Args:
            artifact_ref: Reference to the artifact.

        Returns:
            File contents as string, or None if not found.
        """
        base = os.path.dirname(self.odibi_root)
        full_path = os.path.join(base, artifact_ref.path)

        if os.path.exists(full_path):
            with open(full_path, "r", encoding="utf-8") as f:
                return f.read()
        return None
