"""Passive Issue Discovery for Phase 10.

This module provides agent-discovered candidate issues while preserving
all existing safety and control guarantees from Phases 9.x.

PHASE 10 INVARIANTS (NON-NEGOTIABLE):
1. No automatic fixes — Observer may identify issues, ImprovementAgent must NOT
   propose or apply changes without explicit user approval
2. Controlled Improvement remains unchanged — Same rollback logic, same regression
   gates, same approval flow
3. Learning harness remains read-only — Files under .odibi/learning_harness/
   may be executed, observed, and used for regression, but NEVER improved
4. Evidence-based reporting only — No speculative suggestions; every issue must
   be tied to execution output, schema inspection, config structure, or engine constraints

The system surfaces potential issues, not fixes.

Issue Types Detected:
- Non-deterministic transformers (e.g., deduplicate without order_by)
- Engine incompatibilities (e.g., merge with pyarrow engine)
- Invalid or suspicious join patterns (e.g., cross join + join keys)
- Schema drift risks
- Performance smells (wide joins, skew indicators)
- Configuration validation edge cases
"""

import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from .controlled_improvement import is_learning_harness_path
from .evidence import ExecutionEvidence
from .schemas import IssueSeverity

logger = logging.getLogger(__name__)


class DiscoveredIssueType(str, Enum):
    """Types of issues that can be passively discovered.

    These map to known patterns that the Observer can detect from:
    - Execution output
    - Schema inspection
    - Config structure analysis
    - Engine constraint validation
    """

    NON_DETERMINISTIC_TRANSFORMER = "NON_DETERMINISTIC_TRANSFORMER"
    ENGINE_INCOMPATIBILITY = "ENGINE_INCOMPATIBILITY"
    INVALID_JOIN_PATTERN = "INVALID_JOIN_PATTERN"
    SCHEMA_DRIFT_RISK = "SCHEMA_DRIFT_RISK"
    PERFORMANCE_SMELL = "PERFORMANCE_SMELL"
    CONFIG_VALIDATION_EDGE_CASE = "CONFIG_VALIDATION_EDGE_CASE"
    MISSING_REQUIRED_PARAM = "MISSING_REQUIRED_PARAM"
    DEPRECATED_PATTERN = "DEPRECATED_PATTERN"


class ConfidenceLevel(str, Enum):
    """Confidence in the discovered issue.

    HIGH = Direct evidence from execution output or config parsing
    MEDIUM = Inferred from patterns or partial evidence
    LOW = Heuristic detection, requires user verification
    """

    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


@dataclass
class IssueEvidence:
    """Structured evidence supporting a discovered issue.

    Every discovered issue MUST have evidence. If evidence is weak,
    the issue is downgraded or omitted.
    """

    evidence_type: str
    source: str
    excerpt: str
    line_number: Optional[int] = None
    context: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "evidence_type": self.evidence_type,
            "source": self.source,
            "excerpt": self.excerpt,
            "line_number": self.line_number,
            "context": self.context,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "IssueEvidence":
        return cls(
            evidence_type=data.get("evidence_type", "unknown"),
            source=data.get("source", ""),
            excerpt=data.get("excerpt", ""),
            line_number=data.get("line_number"),
            context=data.get("context", ""),
        )

    @classmethod
    def from_config_inspection(
        cls, file_path: str, excerpt: str, line_number: Optional[int] = None
    ) -> "IssueEvidence":
        return cls(
            evidence_type="config_inspection",
            source=file_path,
            excerpt=excerpt,
            line_number=line_number,
            context="Detected from YAML/config file analysis",
        )

    @classmethod
    def from_execution_output(cls, command: str, output_excerpt: str) -> "IssueEvidence":
        return cls(
            evidence_type="execution_output",
            source=command,
            excerpt=output_excerpt,
            context="Detected from pipeline execution output",
        )

    @classmethod
    def from_schema_inspection(cls, schema_source: str, finding: str) -> "IssueEvidence":
        return cls(
            evidence_type="schema_inspection",
            source=schema_source,
            excerpt=finding,
            context="Detected from schema/metadata analysis",
        )


@dataclass
class IssueLocation:
    """Location of a discovered issue within a file/node."""

    file_path: str
    node_name: Optional[str] = None
    field_path: Optional[str] = None
    line_range: Optional[tuple[int, int]] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "file_path": self.file_path,
            "node_name": self.node_name,
            "field_path": self.field_path,
            "line_range": list(self.line_range) if self.line_range else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "IssueLocation":
        line_range = data.get("line_range")
        return cls(
            file_path=data.get("file_path", ""),
            node_name=data.get("node_name"),
            field_path=data.get("field_path"),
            line_range=tuple(line_range) if line_range else None,
        )

    def is_in_learning_harness(self) -> bool:
        """Check if this location is within the protected learning harness."""
        return is_learning_harness_path(self.file_path)

    def format_location(self) -> str:
        """Format location as a human-readable string."""
        parts = [self.file_path]
        if self.node_name:
            parts.append(f"node: {self.node_name}")
        if self.field_path:
            parts.append(f"field: {self.field_path}")
        if self.line_range:
            parts.append(f"lines {self.line_range[0]}-{self.line_range[1]}")
        return " | ".join(parts)


@dataclass
class DiscoveredIssue:
    """A candidate issue discovered by the Observer.

    This is a CANDIDATE for user review, not an automatic fix target.
    Users must explicitly select which issues to act on.
    """

    issue_id: str
    issue_type: DiscoveredIssueType
    location: IssueLocation
    description: str
    severity: IssueSeverity
    evidence: list[IssueEvidence]
    confidence: ConfidenceLevel

    discovered_at: str = ""
    discovery_cycle_id: str = ""

    user_selected: bool = False
    user_notes: str = ""

    def __post_init__(self):
        if not self.discovered_at:
            self.discovered_at = datetime.now().isoformat()
        if not self.issue_id:
            self.issue_id = self._generate_issue_id()

    def _generate_issue_id(self) -> str:
        """Generate a deterministic issue ID from content."""
        content = f"{self.issue_type.value}:{self.location.file_path}:{self.description}"
        return hashlib.sha256(content.encode()).hexdigest()[:12]

    def to_dict(self) -> dict[str, Any]:
        return {
            "issue_id": self.issue_id,
            "issue_type": self.issue_type.value,
            "location": self.location.to_dict(),
            "description": self.description,
            "severity": self.severity.value,
            "evidence": [e.to_dict() for e in self.evidence],
            "confidence": self.confidence.value,
            "discovered_at": self.discovered_at,
            "discovery_cycle_id": self.discovery_cycle_id,
            "user_selected": self.user_selected,
            "user_notes": self.user_notes,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DiscoveredIssue":
        return cls(
            issue_id=data.get("issue_id", ""),
            issue_type=DiscoveredIssueType(data.get("issue_type", "NON_DETERMINISTIC_TRANSFORMER")),
            location=IssueLocation.from_dict(data.get("location", {})),
            description=data.get("description", ""),
            severity=IssueSeverity(data.get("severity", "MEDIUM")),
            evidence=[IssueEvidence.from_dict(e) for e in data.get("evidence", [])],
            confidence=ConfidenceLevel(data.get("confidence", "MEDIUM")),
            discovered_at=data.get("discovered_at", ""),
            discovery_cycle_id=data.get("discovery_cycle_id", ""),
            user_selected=data.get("user_selected", False),
            user_notes=data.get("user_notes", ""),
        )

    def is_actionable(self) -> bool:
        """Check if this issue has sufficient evidence to be actionable.

        Issues with LOW confidence or no evidence should not be presented
        for action without additional verification.
        """
        if not self.evidence:
            return False
        if self.confidence == ConfidenceLevel.LOW and len(self.evidence) < 2:
            return False
        return True

    def is_improvement_candidate(self) -> bool:
        """Check if this issue can be an improvement target.

        Learning harness files are NEVER improvement targets.
        """
        if self.location.is_in_learning_harness():
            return False
        return self.is_actionable()

    def format_for_display(self) -> str:
        """Format issue for user display."""
        severity_emoji = {
            IssueSeverity.HIGH: "🔴",
            IssueSeverity.MEDIUM: "🟡",
            IssueSeverity.LOW: "🟢",
        }.get(self.severity, "⚪")

        confidence_badge = {
            ConfidenceLevel.HIGH: "✅",
            ConfidenceLevel.MEDIUM: "⚠️",
            ConfidenceLevel.LOW: "❓",
        }.get(self.confidence, "")

        lines = [
            f"{severity_emoji} **{self.issue_type.value}** {confidence_badge}",
            f"📍 {self.location.format_location()}",
            f"📝 {self.description}",
        ]

        if self.evidence:
            lines.append("📋 Evidence:")
            for e in self.evidence[:3]:
                excerpt = e.excerpt[:100] + "..." if len(e.excerpt) > 100 else e.excerpt
                lines.append(f"  - [{e.evidence_type}] {excerpt}")

        return "\n".join(lines)


@dataclass
class IssueDiscoveryResult:
    """Result of a passive issue discovery scan."""

    scan_id: str
    cycle_id: str
    scanned_at: str
    files_scanned: list[str]
    issues: list[DiscoveredIssue]

    total_issues: int = 0
    actionable_issues: int = 0
    harness_issues_excluded: int = 0
    low_confidence_excluded: int = 0

    def __post_init__(self):
        if not self.scan_id:
            self.scan_id = hashlib.sha256(
                f"{self.cycle_id}:{self.scanned_at}".encode()
            ).hexdigest()[:12]
        self.total_issues = len(self.issues)
        self.actionable_issues = sum(1 for i in self.issues if i.is_actionable())
        self.harness_issues_excluded = sum(
            1 for i in self.issues if i.location.is_in_learning_harness()
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "scan_id": self.scan_id,
            "cycle_id": self.cycle_id,
            "scanned_at": self.scanned_at,
            "files_scanned": self.files_scanned,
            "issues": [i.to_dict() for i in self.issues],
            "total_issues": self.total_issues,
            "actionable_issues": self.actionable_issues,
            "harness_issues_excluded": self.harness_issues_excluded,
            "low_confidence_excluded": self.low_confidence_excluded,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "IssueDiscoveryResult":
        return cls(
            scan_id=data.get("scan_id", ""),
            cycle_id=data.get("cycle_id", ""),
            scanned_at=data.get("scanned_at", ""),
            files_scanned=data.get("files_scanned", []),
            issues=[DiscoveredIssue.from_dict(i) for i in data.get("issues", [])],
        )

    def get_improvement_candidates(self) -> list[DiscoveredIssue]:
        """Get issues that can be improvement targets (excludes harness files)."""
        return [i for i in self.issues if i.is_improvement_candidate()]

    def get_by_severity(self, severity: IssueSeverity) -> list[DiscoveredIssue]:
        """Get issues filtered by severity."""
        return [i for i in self.issues if i.severity == severity]

    def get_high_confidence_issues(self) -> list[DiscoveredIssue]:
        """Get only HIGH confidence issues."""
        return [i for i in self.issues if i.confidence == ConfidenceLevel.HIGH]


class PassiveIssueDetector:
    """Detects potential issues from config files and execution evidence.

    This detector implements PASSIVE issue discovery:
    - Scans config files for known problematic patterns
    - Analyzes execution output for warning signs
    - Does NOT propose fixes
    - Does NOT modify anything

    All detections are evidence-based.
    """

    def __init__(self, project_root: str):
        self.project_root = project_root
        self._detected_issues: list[DiscoveredIssue] = []

    def detect_issues_in_file(self, file_path: str, content: str) -> list[DiscoveredIssue]:
        """Detect potential issues in a single config file.

        Args:
            file_path: Path to the config file.
            content: File content.

        Returns:
            List of discovered issues (may be empty).
        """
        issues = []

        issues.extend(self._detect_non_deterministic_deduplicate(file_path, content))
        issues.extend(self._detect_engine_incompatibilities(file_path, content))
        issues.extend(self._detect_suspicious_join_patterns(file_path, content))
        issues.extend(self._detect_missing_required_params(file_path, content))

        return issues

    def _detect_non_deterministic_deduplicate(
        self, file_path: str, content: str
    ) -> list[DiscoveredIssue]:
        """Detect deduplicate transformers without order_by (non-deterministic)."""
        issues = []
        lines = content.split("\n")

        in_dedupe_block = False
        dedupe_node_name = None
        dedupe_start_line = 0
        has_order_by = False
        block_content = []

        for i, line in enumerate(lines, 1):
            if "transformer: deduplicate" in line or "transformer: dedupe" in line:
                in_dedupe_block = True
                dedupe_start_line = i
                has_order_by = False
                block_content = [line]

                for j in range(max(0, i - 5), i):
                    if "- name:" in lines[j]:
                        match = re.search(r"- name:\s*(\S+)", lines[j])
                        if match:
                            dedupe_node_name = match.group(1)
                        break
                continue

            if in_dedupe_block:
                block_content.append(line)

                if "order_by:" in line:
                    has_order_by = True

                if line.strip() and not line.startswith(" ") and not line.startswith("\t"):
                    if not has_order_by:
                        issues.append(
                            DiscoveredIssue(
                                issue_id="",
                                issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
                                location=IssueLocation(
                                    file_path=file_path,
                                    node_name=dedupe_node_name,
                                    field_path="params",
                                    line_range=(dedupe_start_line, i),
                                ),
                                description=(
                                    f"The `deduplicate` transformer in node `{dedupe_node_name}` "
                                    "is missing the `order_by` parameter. Without `order_by`, "
                                    "duplicate resolution is NON-DETERMINISTIC — different runs "
                                    "may keep different rows."
                                ),
                                severity=IssueSeverity.HIGH,
                                evidence=[
                                    IssueEvidence.from_config_inspection(
                                        file_path,
                                        "\n".join(block_content),
                                        dedupe_start_line,
                                    )
                                ],
                                confidence=ConfidenceLevel.HIGH,
                            )
                        )
                    in_dedupe_block = False
                    dedupe_node_name = None

                if "write:" in line or "- name:" in line:
                    if not has_order_by and dedupe_node_name:
                        issues.append(
                            DiscoveredIssue(
                                issue_id="",
                                issue_type=DiscoveredIssueType.NON_DETERMINISTIC_TRANSFORMER,
                                location=IssueLocation(
                                    file_path=file_path,
                                    node_name=dedupe_node_name,
                                    field_path="params",
                                    line_range=(dedupe_start_line, i),
                                ),
                                description=(
                                    f"The `deduplicate` transformer in node `{dedupe_node_name}` "
                                    "is missing the `order_by` parameter. Without `order_by`, "
                                    "duplicate resolution is NON-DETERMINISTIC."
                                ),
                                severity=IssueSeverity.HIGH,
                                evidence=[
                                    IssueEvidence.from_config_inspection(
                                        file_path,
                                        "\n".join(block_content),
                                        dedupe_start_line,
                                    )
                                ],
                                confidence=ConfidenceLevel.HIGH,
                            )
                        )
                    in_dedupe_block = False
                    dedupe_node_name = None

        return issues

    def _detect_engine_incompatibilities(
        self, file_path: str, content: str
    ) -> list[DiscoveredIssue]:
        """Detect patterns that are incompatible with certain engines."""
        issues = []

        if "engine: pyarrow" in content.lower() or "engine: arrow" in content.lower():
            if "transformer: merge" in content:
                match = re.search(r"transformer:\s*merge", content)
                line_num = content[: match.start()].count("\n") + 1 if match else None

                issues.append(
                    DiscoveredIssue(
                        issue_id="",
                        issue_type=DiscoveredIssueType.ENGINE_INCOMPATIBILITY,
                        location=IssueLocation(
                            file_path=file_path,
                            field_path="transformer: merge",
                            line_range=(line_num, line_num) if line_num else None,
                        ),
                        description=(
                            "The `merge` transformer may have limited support in PyArrow engine. "
                            "Consider using DuckDB or Polars engine for complex merge operations."
                        ),
                        severity=IssueSeverity.MEDIUM,
                        evidence=[
                            IssueEvidence.from_config_inspection(
                                file_path,
                                "engine: pyarrow with transformer: merge",
                                line_num,
                            )
                        ],
                        confidence=ConfidenceLevel.MEDIUM,
                    )
                )

        return issues

    def _detect_suspicious_join_patterns(
        self, file_path: str, content: str
    ) -> list[DiscoveredIssue]:
        """Detect suspicious join patterns like cross join with keys."""
        issues = []
        lines = content.split("\n")

        in_join_block = False
        join_how = None
        has_on_keys = False
        join_start_line = 0
        join_node_name = None

        for i, line in enumerate(lines, 1):
            if "transformer: join" in line:
                in_join_block = True
                join_start_line = i
                join_how = None
                has_on_keys = False

                for j in range(max(0, i - 5), i):
                    if "- name:" in lines[j]:
                        match = re.search(r"- name:\s*(\S+)", lines[j])
                        if match:
                            join_node_name = match.group(1)
                        break
                continue

            if in_join_block:
                if "how:" in line:
                    if "cross" in line.lower():
                        join_how = "cross"

                if '"on":' in line or "'on':" in line or "on:" in line:
                    has_on_keys = True

                if line.strip() and not line.startswith(" ") and not line.startswith("\t"):
                    if join_how == "cross" and has_on_keys:
                        issues.append(
                            DiscoveredIssue(
                                issue_id="",
                                issue_type=DiscoveredIssueType.INVALID_JOIN_PATTERN,
                                location=IssueLocation(
                                    file_path=file_path,
                                    node_name=join_node_name,
                                    field_path="params",
                                    line_range=(join_start_line, i),
                                ),
                                description=(
                                    f"Join node `{join_node_name}` uses `how: cross` with join keys. "
                                    "Cross joins should not have join keys as they produce a "
                                    "Cartesian product. This may indicate a configuration error."
                                ),
                                severity=IssueSeverity.HIGH,
                                evidence=[
                                    IssueEvidence.from_config_inspection(
                                        file_path,
                                        f"how: cross with on: keys at line {join_start_line}",
                                        join_start_line,
                                    )
                                ],
                                confidence=ConfidenceLevel.HIGH,
                            )
                        )
                    in_join_block = False

                if "write:" in line or "- name:" in line:
                    in_join_block = False

        return issues

    def _detect_missing_required_params(
        self, file_path: str, content: str
    ) -> list[DiscoveredIssue]:
        """Detect transformers missing required parameters."""
        issues = []

        transformer_requirements = {
            "join": ["right_dataset", '"on"'],
            "union": ["datasets"],
            "fill_nulls": ["values"],
        }

        for transformer, required_params in transformer_requirements.items():
            pattern = rf"transformer:\s*{transformer}"
            matches = list(re.finditer(pattern, content))

            for match in matches:
                start_pos = match.start()
                end_pos = content.find("\n- name:", start_pos + 1)
                if end_pos == -1:
                    end_pos = len(content)

                block = content[start_pos:end_pos]
                line_num = content[:start_pos].count("\n") + 1

                missing = []
                for param in required_params:
                    if param not in block and param.strip('"') + ":" not in block:
                        missing.append(param)

                if missing:
                    issues.append(
                        DiscoveredIssue(
                            issue_id="",
                            issue_type=DiscoveredIssueType.MISSING_REQUIRED_PARAM,
                            location=IssueLocation(
                                file_path=file_path,
                                field_path=f"transformer: {transformer}",
                                line_range=(line_num, line_num + block.count("\n")),
                            ),
                            description=(
                                f"The `{transformer}` transformer is missing required parameter(s): "
                                f"{', '.join(missing)}. This will likely cause runtime errors."
                            ),
                            severity=IssueSeverity.HIGH,
                            evidence=[
                                IssueEvidence.from_config_inspection(
                                    file_path,
                                    block[:200],
                                    line_num,
                                )
                            ],
                            confidence=ConfidenceLevel.HIGH,
                        )
                    )

        return issues

    def detect_from_execution_evidence(self, evidence: ExecutionEvidence) -> list[DiscoveredIssue]:
        """Detect issues from execution output.

        Args:
            evidence: Execution evidence from a pipeline run.

        Returns:
            List of discovered issues.
        """
        issues = []

        combined_output = f"{evidence.stdout}\n{evidence.stderr}"

        performance_patterns = [
            (r"skew detected.*ratio[:\s]+(\d+)", "Data skew"),
            (r"memory exceeded|out of memory|OOM", "Memory issue"),
            (r"timeout.*(\d+)\s*seconds", "Timeout"),
            (r"slow query.*(\d+)\s*ms", "Slow query"),
        ]

        for pattern, issue_name in performance_patterns:
            match = re.search(pattern, combined_output, re.IGNORECASE)
            if match:
                issues.append(
                    DiscoveredIssue(
                        issue_id="",
                        issue_type=DiscoveredIssueType.PERFORMANCE_SMELL,
                        location=IssueLocation(
                            file_path="[execution output]",
                        ),
                        description=f"Performance issue detected: {issue_name}",
                        severity=IssueSeverity.MEDIUM,
                        evidence=[
                            IssueEvidence.from_execution_output(
                                evidence.raw_command,
                                match.group(0)[:200],
                            )
                        ],
                        confidence=ConfidenceLevel.MEDIUM,
                    )
                )

        schema_patterns = [
            r"column.*not found|missing column",
            r"schema mismatch|type mismatch",
            r"unexpected column|extra column",
        ]

        for pattern in schema_patterns:
            match = re.search(pattern, combined_output, re.IGNORECASE)
            if match:
                issues.append(
                    DiscoveredIssue(
                        issue_id="",
                        issue_type=DiscoveredIssueType.SCHEMA_DRIFT_RISK,
                        location=IssueLocation(
                            file_path="[execution output]",
                        ),
                        description="Potential schema drift detected in execution output",
                        severity=IssueSeverity.MEDIUM,
                        evidence=[
                            IssueEvidence.from_execution_output(
                                evidence.raw_command,
                                match.group(0)[:200],
                            )
                        ],
                        confidence=ConfidenceLevel.MEDIUM,
                    )
                )

        return issues

    def scan_project(self, cycle_id: str = "") -> IssueDiscoveryResult:
        """Scan the entire project for potential issues.

        Args:
            cycle_id: Optional cycle ID to associate with this scan.

        Returns:
            IssueDiscoveryResult with all discovered issues.
        """
        all_issues: list[DiscoveredIssue] = []
        files_scanned: list[str] = []

        yaml_files = list(Path(self.project_root).rglob("*.yaml")) + list(
            Path(self.project_root).rglob("*.yml")
        )

        for yaml_file in yaml_files:
            if ".odibi" in str(yaml_file) and "learning_harness" not in str(yaml_file):
                continue
            if "__pycache__" in str(yaml_file) or ".git" in str(yaml_file):
                continue

            try:
                content = yaml_file.read_text(encoding="utf-8")
                files_scanned.append(str(yaml_file))

                if "pipelines:" in content or "transformer:" in content:
                    issues = self.detect_issues_in_file(str(yaml_file), content)
                    for issue in issues:
                        issue.discovery_cycle_id = cycle_id
                    all_issues.extend(issues)

            except Exception as e:
                logger.warning(f"Failed to scan {yaml_file}: {e}")

        return IssueDiscoveryResult(
            scan_id="",
            cycle_id=cycle_id,
            scanned_at=datetime.now().isoformat(),
            files_scanned=files_scanned,
            issues=all_issues,
        )


@dataclass
class UserIssueSelection:
    """User's selection of issues to act on.

    Phase 10.C: Users explicitly select which issues to address.
    This creates the intent that feeds into controlled improvement.
    """

    selection_id: str
    selected_issues: list[str]
    target_file: str
    constraints: dict[str, Any] = field(default_factory=dict)
    user_notes: str = ""
    selected_at: str = ""

    def __post_init__(self):
        if not self.selection_id:
            self.selection_id = hashlib.sha256(
                f"{self.target_file}:{','.join(self.selected_issues)}:{datetime.now().isoformat()}".encode()
            ).hexdigest()[:12]
        if not self.selected_at:
            self.selected_at = datetime.now().isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "selection_id": self.selection_id,
            "selected_issues": self.selected_issues,
            "target_file": self.target_file,
            "constraints": self.constraints,
            "user_notes": self.user_notes,
            "selected_at": self.selected_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "UserIssueSelection":
        return cls(
            selection_id=data.get("selection_id", ""),
            selected_issues=data.get("selected_issues", []),
            target_file=data.get("target_file", ""),
            constraints=data.get("constraints", {}),
            user_notes=data.get("user_notes", ""),
            selected_at=data.get("selected_at", ""),
        )

    def to_task_description(self, issues: list[DiscoveredIssue]) -> str:
        """Convert selection to a task description for controlled improvement.

        Args:
            issues: The full list of discovered issues (to lookup by ID).

        Returns:
            Task description string for ControlledImprovementConfig.
        """
        selected = [i for i in issues if i.issue_id in self.selected_issues]

        if not selected:
            return f"Review and improve {self.target_file}"

        descriptions = []
        for issue in selected:
            descriptions.append(f"- {issue.description}")

        task = f"Fix the following issues in {self.target_file}:\n" + "\n".join(descriptions)

        if self.user_notes:
            task += f"\n\nUser notes: {self.user_notes}"

        if self.constraints:
            constraint_str = json.dumps(self.constraints, indent=2)
            task += f"\n\nConstraints: {constraint_str}"

        return task


class IssueDiscoveryManager:
    """Manages the full issue discovery lifecycle.

    Coordinates:
    - Passive scanning
    - Issue persistence
    - User selection
    - Integration with controlled improvement

    INVARIANT: This manager NEVER triggers improvements automatically.
    User selection is REQUIRED.
    """

    def __init__(self, odibi_root: str):
        self.odibi_root = odibi_root
        self._issues_dir = os.path.join(odibi_root, ".odibi", "discovered_issues")
        os.makedirs(self._issues_dir, exist_ok=True)

    def run_discovery(self, project_root: str, cycle_id: str = "") -> IssueDiscoveryResult:
        """Run passive issue discovery on a project.

        Args:
            project_root: Path to the project to scan.
            cycle_id: Optional cycle ID to associate.

        Returns:
            IssueDiscoveryResult with discovered issues.
        """
        detector = PassiveIssueDetector(project_root)
        result = detector.scan_project(cycle_id)

        self._persist_result(result)

        logger.info(
            f"Issue discovery complete: {result.total_issues} issues found, "
            f"{result.actionable_issues} actionable"
        )

        return result

    def _persist_result(self, result: IssueDiscoveryResult) -> None:
        """Persist discovery result to disk."""
        result_path = os.path.join(self._issues_dir, f"{result.scan_id}.json")
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(result.to_dict(), f, indent=2)

    def load_latest_result(self) -> Optional[IssueDiscoveryResult]:
        """Load the most recent discovery result."""
        if not os.path.exists(self._issues_dir):
            return None

        files = sorted(
            Path(self._issues_dir).glob("*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

        if not files:
            return None

        with open(files[0], "r", encoding="utf-8") as f:
            data = json.load(f)
        return IssueDiscoveryResult.from_dict(data)

    def create_selection(
        self,
        issue_ids: list[str],
        target_file: str,
        constraints: Optional[dict[str, Any]] = None,
        user_notes: str = "",
    ) -> UserIssueSelection:
        """Create a user selection from discovered issues.

        Args:
            issue_ids: IDs of issues to address.
            target_file: File to improve.
            constraints: Optional constraints for improvement.
            user_notes: Optional user notes.

        Returns:
            UserIssueSelection ready for controlled improvement.

        Raises:
            ValueError: If target_file is in learning harness.
        """
        if is_learning_harness_path(target_file):
            raise ValueError(
                f"Cannot select '{target_file}' as improvement target: "
                "Learning harness files are protected."
            )

        return UserIssueSelection(
            selection_id="",
            selected_issues=issue_ids,
            target_file=target_file,
            constraints=constraints or {},
            user_notes=user_notes,
        )

    def get_improvement_candidates(
        self, result: Optional[IssueDiscoveryResult] = None
    ) -> list[DiscoveredIssue]:
        """Get issues that can be improvement targets.

        Excludes:
        - Learning harness files
        - Low confidence issues without sufficient evidence

        Args:
            result: Optional specific result; defaults to latest.

        Returns:
            List of improvement-candidate issues.
        """
        if result is None:
            result = self.load_latest_result()

        if result is None:
            return []

        return result.get_improvement_candidates()


def _cli_demo():
    """Command-line demo of issue discovery."""
    import sys

    print("=" * 60)
    print("PHASE 10 ISSUE DISCOVERY - CLI DEMO")
    print("=" * 60)

    project_root = sys.argv[1] if len(sys.argv) > 1 else "d:/odibi/examples"
    odibi_root = "d:/odibi"

    manager = IssueDiscoveryManager(odibi_root)
    result = manager.run_discovery(project_root)

    print(f"\nScanned: {len(result.files_scanned)} files")
    print(f"Found: {result.total_issues} issue(s)")

    for issue in result.issues:
        print("\n" + "-" * 40)
        print(issue.format_for_display())

    candidates = result.get_improvement_candidates()
    print(f"\n\nImprovement candidates: {len(candidates)}")
    for c in candidates:
        print(f"  [{c.issue_id[:8]}] {c.location.file_path}")

    if candidates:
        print("\n" + "=" * 60)
        print("DEMO: Creating user selection")
        print("=" * 60)
        selection = manager.create_selection(
            issue_ids=[candidates[0].issue_id],
            target_file=candidates[0].location.file_path,
        )
        print(f"Selection ID: {selection.selection_id}")
        print("Task description preview:")
        task = selection.to_task_description(result.issues)
        print(task[:300] + "..." if len(task) > 300 else task)


if __name__ == "__main__":
    _cli_demo()
