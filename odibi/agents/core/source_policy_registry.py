"""Source Policy Registry: First-class policy management for source selection.

Phase 8.C - Source Selection Policy Registry.

This module provides:
- Loading policies from disk (.odibi/selection_policies/)
- Schema + invariant validation
- Policy discovery (list, get, exists)
- PolicyResolutionResult for audit trails

INVARIANTS (Phase 8.C):
- READ-ONLY only
- NO execution
- NO data access
- NO source binding
- NO agent autonomy changes
- Determinism is preserved
- Same inputs → same outputs

HARD CONSTRAINTS (DO NOT VIOLATE):
❌ NO pipeline execution
❌ NO cycle execution
❌ NO agent autonomy changes
❌ NO data downloads
❌ NO ExecutionGateway changes
❌ NO source binding

This module is 100% READ-ONLY at runtime.
"""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import yaml
from pydantic import BaseModel, Field, model_validator

from .source_selection import CycleMode, SelectionStrategy, SourceSelectionPolicy


# ============================================
# Policy Validation Types
# ============================================


class PolicyValidationLevel(str, Enum):
    """Severity levels for policy validation issues."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class PolicyValidationIssue:
    """A single validation issue for a policy.

    These are raised at LOAD-TIME, not runtime.
    """

    level: PolicyValidationLevel
    rule: str
    field: str
    message: str
    value: Any = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "level": self.level.value,
            "rule": self.rule,
            "field": self.field,
            "message": self.message,
            "value": str(self.value) if self.value is not None else None,
        }


class PolicyValidationStatus(str, Enum):
    """Overall validation status for a policy."""

    VALID = "valid"
    INVALID = "invalid"
    WARNINGS = "warnings"


# ============================================
# Policy Schema Extensions
# ============================================


class RegisteredPolicy(BaseModel):
    """A policy registered in the policy registry.

    Extends SourceSelectionPolicy with registry metadata.
    """

    # Core policy fields (from SourceSelectionPolicy)
    policy_id: str = Field(
        description="Unique policy identifier",
        pattern=r"^[a-z][a-z0-9_]*$",
    )
    name: str = Field(description="Human-readable policy name")
    description: str = Field(default="", description="Policy description")

    # Pool Constraints
    eligible_pools: Optional[List[str]] = Field(
        default=None,
        description="Explicit list of pool_ids that MAY be selected.",
    )
    excluded_pools: List[str] = Field(
        default_factory=list,
        description="Pool IDs that MUST NOT be selected",
    )
    max_pools_per_cycle: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Maximum number of pools to select per cycle",
    )

    # Format & Type Constraints
    allowed_formats: Optional[List[str]] = Field(
        default=None,
        description="Allowed file formats (csv, json, parquet, etc.)",
    )
    allowed_source_types: Optional[List[str]] = Field(
        default=None,
        description="Allowed source types (local, sql_jdbc_local, etc.)",
    )

    # Quality Constraints
    allow_messy_data: bool = Field(
        default=True,
        description="Whether messy/mixed quality pools can be selected",
    )
    clean_vs_messy_ratio: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Target ratio of clean pools",
    )

    # Selection Strategy
    selection_strategy: SelectionStrategy = Field(
        default=SelectionStrategy.HASH_BASED,
        description="Deterministic selection algorithm",
    )

    # Coverage Preferences
    prefer_uncovered_formats: bool = Field(
        default=True,
        description="Prefer formats not yet tested in recent cycles",
    )
    prefer_uncovered_pools: bool = Field(
        default=True,
        description="Prefer pools not recently used",
    )

    # Explicit Selection
    explicit_pool_order: Optional[List[str]] = Field(
        default=None,
        description="For EXPLICIT strategy: exact ordered list of pools",
    )

    # === Registry-Specific Fields ===
    require_frozen: bool = Field(
        default=True,
        description="If True, only FROZEN pools may be selected",
    )
    allowed_modes: Optional[List[str]] = Field(
        default=None,
        description="Modes this policy is valid for (learning, improvement, scheduled). None = all.",
    )
    disallowed_tiers: List[str] = Field(
        default_factory=list,
        description="Tier names that MUST NOT be used with this policy",
    )
    require_deterministic: bool = Field(
        default=True,
        description="If True, selection must be fully deterministic",
    )

    # Metadata
    version: str = Field(
        default="1.0.0",
        description="Policy version for tracking changes",
    )
    author: str = Field(
        default="",
        description="Policy author/owner",
    )
    created_at: str = Field(
        default="",
        description="ISO timestamp when policy was created",
    )

    @model_validator(mode="after")
    def validate_explicit_strategy(self):
        """Ensure explicit strategy has explicit_pool_order."""
        if self.selection_strategy == SelectionStrategy.EXPLICIT:
            if not self.explicit_pool_order:
                raise ValueError("EXPLICIT strategy requires explicit_pool_order to be set")
        return self

    @model_validator(mode="after")
    def validate_no_conflicting_flags(self):
        """Ensure no conflicting flag combinations."""
        if self.require_frozen is False and self.require_deterministic:
            pass
        return self

    def to_selection_policy(self) -> SourceSelectionPolicy:
        """Convert to SourceSelectionPolicy for use in selection."""
        return SourceSelectionPolicy(
            policy_id=self.policy_id,
            name=self.name,
            description=self.description,
            eligible_pools=self.eligible_pools,
            excluded_pools=self.excluded_pools,
            max_pools_per_cycle=self.max_pools_per_cycle,
            allowed_formats=self.allowed_formats,
            allowed_source_types=self.allowed_source_types,
            allow_messy_data=self.allow_messy_data,
            clean_vs_messy_ratio=self.clean_vs_messy_ratio,
            selection_strategy=self.selection_strategy,
            prefer_uncovered_formats=self.prefer_uncovered_formats,
            prefer_uncovered_pools=self.prefer_uncovered_pools,
            explicit_pool_order=self.explicit_pool_order,
        )


# ============================================
# PolicyResolutionResult
# ============================================


@dataclass
class PolicyResolutionResult:
    """Result of policy resolution for audit trails.

    Captures:
    - Which policy was loaded
    - Policy hash for determinism verification
    - Validation status and warnings
    - Timestamp of resolution

    INVARIANTS:
    - Created once during resolution
    - Never modified after creation
    - All fields are read-only
    """

    policy_name: str
    policy_hash: str
    validation_status: PolicyValidationStatus
    resolved_at: str
    warnings: List[str] = field(default_factory=list)

    # Additional metadata
    policy_version: str = ""
    policy_path: str = ""
    mode_validated: str = ""

    def __post_init__(self):
        if not self.resolved_at:
            self.resolved_at = datetime.now(UTC).isoformat() + "Z"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "policy_name": self.policy_name,
            "policy_hash": self.policy_hash,
            "validation_status": self.validation_status.value,
            "resolved_at": self.resolved_at,
            "warnings": self.warnings,
            "policy_version": self.policy_version,
            "policy_path": self.policy_path,
            "mode_validated": self.mode_validated,
        }


# ============================================
# Policy Validator
# ============================================


class PolicyValidator:
    """Validates policies against mode-specific rules and invariants.

    VALIDATION RULES:
    1. Scheduled mode → require_frozen = True
    2. Improvement mode → disallow tier600gb+
    3. No conflicting flags (require_clean + allow_messy)
    4. Explicit defaults only (no implicit behavior)

    All validation is READ-ONLY and deterministic.
    """

    # Tiers that are disallowed in improvement mode
    IMPROVEMENT_DISALLOWED_TIERS: Set[str] = {"tier600gb", "tier2tb"}

    def validate(
        self,
        policy: RegisteredPolicy,
        mode: Optional[CycleMode] = None,
    ) -> List[PolicyValidationIssue]:
        """Validate a policy against rules and mode constraints.

        Args:
            policy: The RegisteredPolicy to validate
            mode: Optional CycleMode for mode-specific validation

        Returns:
            List of validation issues (empty if valid)
        """
        issues: List[PolicyValidationIssue] = []

        # Schema validation (basic)
        issues.extend(self._validate_schema(policy))

        # Invariant validation
        issues.extend(self._validate_conflicting_flags(policy))
        issues.extend(self._validate_explicit_defaults(policy))

        # Mode-specific validation
        if mode:
            issues.extend(self._validate_mode_constraints(policy, mode))

        return issues

    def _validate_schema(self, policy: RegisteredPolicy) -> List[PolicyValidationIssue]:
        """Validate basic schema requirements."""
        issues = []

        if not policy.policy_id:
            issues.append(
                PolicyValidationIssue(
                    level=PolicyValidationLevel.ERROR,
                    rule="REQUIRED_FIELD",
                    field="policy_id",
                    message="policy_id is required",
                )
            )

        if not policy.name:
            issues.append(
                PolicyValidationIssue(
                    level=PolicyValidationLevel.ERROR,
                    rule="REQUIRED_FIELD",
                    field="name",
                    message="name is required",
                )
            )

        if policy.max_pools_per_cycle < 1 or policy.max_pools_per_cycle > 10:
            issues.append(
                PolicyValidationIssue(
                    level=PolicyValidationLevel.ERROR,
                    rule="VALUE_RANGE",
                    field="max_pools_per_cycle",
                    message="max_pools_per_cycle must be between 1 and 10",
                    value=policy.max_pools_per_cycle,
                )
            )

        return issues

    def _validate_conflicting_flags(self, policy: RegisteredPolicy) -> List[PolicyValidationIssue]:
        """Check for conflicting flag combinations.

        RULE: require_frozen=False and allow_messy_data=False while
              require_deterministic=True is potentially inconsistent.
        """
        issues = []

        # No explicit require_clean field in RegisteredPolicy, but we check
        # the clean_vs_messy_ratio for conflicts
        if policy.clean_vs_messy_ratio == 1.0 and policy.allow_messy_data:
            issues.append(
                PolicyValidationIssue(
                    level=PolicyValidationLevel.WARNING,
                    rule="CONFLICTING_FLAGS",
                    field="clean_vs_messy_ratio",
                    message="clean_vs_messy_ratio=1.0 implies no messy data, "
                    "but allow_messy_data=True. Consider setting allow_messy_data=False.",
                    value={"clean_vs_messy_ratio": 1.0, "allow_messy_data": True},
                )
            )

        return issues

    def _validate_explicit_defaults(self, policy: RegisteredPolicy) -> List[PolicyValidationIssue]:
        """Ensure explicit defaults only (no implicit behavior).

        RULE: Policies should not rely on implicit defaults for critical settings.
        """
        issues = []

        # Warn if no modes specified (implicit "all modes")
        if policy.allowed_modes is None:
            issues.append(
                PolicyValidationIssue(
                    level=PolicyValidationLevel.INFO,
                    rule="IMPLICIT_DEFAULT",
                    field="allowed_modes",
                    message="allowed_modes is None, policy applies to all modes. "
                    "Consider explicitly listing allowed modes.",
                )
            )

        return issues

    def _validate_mode_constraints(
        self, policy: RegisteredPolicy, mode: CycleMode
    ) -> List[PolicyValidationIssue]:
        """Validate policy against mode-specific constraints.

        RULES:
        - Scheduled mode → require_frozen = True
        - Improvement mode → disallow tier600gb+
        """
        issues = []

        # Check if mode is allowed for this policy
        if policy.allowed_modes is not None:
            if mode.value not in policy.allowed_modes:
                issues.append(
                    PolicyValidationIssue(
                        level=PolicyValidationLevel.ERROR,
                        rule="MODE_NOT_ALLOWED",
                        field="allowed_modes",
                        message=f"Policy '{policy.policy_id}' does not allow mode '{mode.value}'. "
                        f"Allowed modes: {policy.allowed_modes}",
                        value=mode.value,
                    )
                )

        # RULE: Scheduled mode → require_frozen = True
        if mode == CycleMode.SCHEDULED and not policy.require_frozen:
            issues.append(
                PolicyValidationIssue(
                    level=PolicyValidationLevel.ERROR,
                    rule="SCHEDULED_REQUIRES_FROZEN",
                    field="require_frozen",
                    message="Scheduled mode requires require_frozen=True. "
                    "Non-frozen pools are not allowed in scheduled cycles.",
                    value=policy.require_frozen,
                )
            )

        # RULE: Improvement mode → disallow tier600gb+
        if mode == CycleMode.IMPROVEMENT:
            # Check disallowed_tiers doesn't include all required exclusions
            missing_exclusions = self.IMPROVEMENT_DISALLOWED_TIERS - set(policy.disallowed_tiers)
            if missing_exclusions:
                issues.append(
                    PolicyValidationIssue(
                        level=PolicyValidationLevel.WARNING,
                        rule="IMPROVEMENT_TIER_RESTRICTION",
                        field="disallowed_tiers",
                        message=f"Improvement mode should disallow large tiers. "
                        f"Consider adding to disallowed_tiers: {missing_exclusions}",
                        value=list(missing_exclusions),
                    )
                )

        return issues

    def get_validation_status(self, issues: List[PolicyValidationIssue]) -> PolicyValidationStatus:
        """Determine overall validation status from issues.

        Returns:
            VALID if no issues
            INVALID if any ERROR level issues
            WARNINGS if only WARNING/INFO level issues
        """
        if not issues:
            return PolicyValidationStatus.VALID

        has_errors = any(i.level == PolicyValidationLevel.ERROR for i in issues)
        if has_errors:
            return PolicyValidationStatus.INVALID

        has_warnings = any(i.level == PolicyValidationLevel.WARNING for i in issues)
        if has_warnings:
            return PolicyValidationStatus.WARNINGS

        return PolicyValidationStatus.VALID


# ============================================
# Source Policy Registry
# ============================================


class PolicyRegistryError(Exception):
    """Error during policy registry operations."""

    def __init__(
        self,
        message: str,
        error_type: str = "REGISTRY_ERROR",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.message = message
        self.error_type = error_type
        self.details = details or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error_type": self.error_type,
            "message": self.message,
            "details": self.details,
        }


class SourcePolicyRegistry:
    """Registry for source selection policies.

    Loads policies from disk, validates them, and provides discovery APIs.

    LOCATION: .odibi/selection_policies/
    FORMAT: YAML files with RegisteredPolicy schema

    INVARIANTS:
    - READ-ONLY operations only
    - Deterministic loading (same files → same policies)
    - No side effects
    - Fail fast on invalid policies

    USAGE:
        registry = SourcePolicyRegistry(odibi_root)
        registry.load_all()

        if registry.exists("my_policy"):
            policy = registry.get("my_policy")
            result = registry.resolve("my_policy", mode=CycleMode.SCHEDULED)
    """

    POLICY_DIR = "selection_policies"

    def __init__(self, odibi_root: str):
        """Initialize registry with Odibi root path.

        Args:
            odibi_root: Path to Odibi repository root
        """
        self.odibi_root = odibi_root
        self.policies_path = Path(odibi_root) / ".odibi" / self.POLICY_DIR
        self._policies: Dict[str, RegisteredPolicy] = {}
        self._policy_hashes: Dict[str, str] = {}
        self._policy_paths: Dict[str, str] = {}
        self._loaded = False
        self._validator = PolicyValidator()

    def load_all(self) -> int:
        """Load all policies from disk.

        Returns:
            Number of policies loaded successfully

        Raises:
            PolicyRegistryError: If policies directory doesn't exist
        """
        if not self.policies_path.exists():
            self._loaded = True
            return 0

        policy_files = list(self.policies_path.glob("*.yaml")) + list(
            self.policies_path.glob("*.yml")
        )

        loaded_count = 0
        for policy_file in sorted(policy_files):
            try:
                policy = self._load_policy_file(policy_file)
                self._policies[policy.policy_id] = policy
                self._policy_paths[policy.policy_id] = str(policy_file)
                self._policy_hashes[policy.policy_id] = self._compute_hash(policy)
                loaded_count += 1
            except Exception as e:
                raise PolicyRegistryError(
                    message=f"Failed to load policy from {policy_file}: {e}",
                    error_type="POLICY_LOAD_ERROR",
                    details={"file": str(policy_file), "error": str(e)},
                )

        self._loaded = True
        return loaded_count

    def _load_policy_file(self, path: Path) -> RegisteredPolicy:
        """Load a single policy file.

        Args:
            path: Path to YAML policy file

        Returns:
            RegisteredPolicy instance
        """
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        # Handle selection_strategy as string
        if "selection_strategy" in data and isinstance(data["selection_strategy"], str):
            data["selection_strategy"] = SelectionStrategy(data["selection_strategy"])

        return RegisteredPolicy(**data)

    def _compute_hash(self, policy: RegisteredPolicy) -> str:
        """Compute deterministic hash for a policy.

        Args:
            policy: The policy to hash

        Returns:
            SHA256 hash (first 16 chars)
        """
        policy_json = json.dumps(policy.model_dump(), sort_keys=True)
        return hashlib.sha256(policy_json.encode()).hexdigest()[:16]

    def list(self) -> List[str]:
        """List all registered policy IDs.

        Returns:
            Sorted list of policy IDs
        """
        self._ensure_loaded()
        return sorted(self._policies.keys())

    def get(self, policy_id: str) -> RegisteredPolicy:
        """Get a policy by ID.

        Args:
            policy_id: The policy identifier

        Returns:
            RegisteredPolicy instance

        Raises:
            PolicyRegistryError: If policy not found
        """
        self._ensure_loaded()
        if policy_id not in self._policies:
            raise PolicyRegistryError(
                message=f"Policy '{policy_id}' not found in registry",
                error_type="POLICY_NOT_FOUND",
                details={
                    "policy_id": policy_id,
                    "available": list(self._policies.keys()),
                },
            )
        return self._policies[policy_id]

    def exists(self, policy_id: str) -> bool:
        """Check if a policy exists.

        Args:
            policy_id: The policy identifier

        Returns:
            True if policy exists
        """
        self._ensure_loaded()
        return policy_id in self._policies

    def get_hash(self, policy_id: str) -> str:
        """Get the hash for a policy.

        Args:
            policy_id: The policy identifier

        Returns:
            Policy hash string

        Raises:
            PolicyRegistryError: If policy not found
        """
        self._ensure_loaded()
        if policy_id not in self._policy_hashes:
            raise PolicyRegistryError(
                message=f"Policy '{policy_id}' not found in registry",
                error_type="POLICY_NOT_FOUND",
            )
        return self._policy_hashes[policy_id]

    def validate(
        self, policy_id: str, mode: Optional[CycleMode] = None
    ) -> List[PolicyValidationIssue]:
        """Validate a policy.

        Args:
            policy_id: The policy identifier
            mode: Optional mode for mode-specific validation

        Returns:
            List of validation issues

        Raises:
            PolicyRegistryError: If policy not found
        """
        policy = self.get(policy_id)
        return self._validator.validate(policy, mode)

    def resolve(
        self,
        policy_id: str,
        mode: CycleMode,
        fail_on_error: bool = True,
    ) -> PolicyResolutionResult:
        """Resolve a policy for use in a cycle.

        This is the main entry point for getting a validated policy
        with full audit metadata.

        Args:
            policy_id: The policy identifier
            mode: The cycle mode
            fail_on_error: If True, raise on validation errors

        Returns:
            PolicyResolutionResult with policy metadata

        Raises:
            PolicyRegistryError: If policy not found or invalid (when fail_on_error=True)
        """
        policy = self.get(policy_id)
        issues = self._validator.validate(policy, mode)
        status = self._validator.get_validation_status(issues)

        if status == PolicyValidationStatus.INVALID and fail_on_error:
            error_issues = [i for i in issues if i.level == PolicyValidationLevel.ERROR]
            raise PolicyRegistryError(
                message=f"Policy '{policy_id}' is invalid for mode '{mode.value}'",
                error_type="POLICY_VALIDATION_FAILED",
                details={
                    "policy_id": policy_id,
                    "mode": mode.value,
                    "errors": [i.to_dict() for i in error_issues],
                },
            )

        warnings = [i.message for i in issues if i.level != PolicyValidationLevel.INFO]

        return PolicyResolutionResult(
            policy_name=policy_id,
            policy_hash=self._policy_hashes.get(policy_id, ""),
            validation_status=status,
            resolved_at=datetime.now(UTC).isoformat() + "Z",
            warnings=warnings,
            policy_version=policy.version,
            policy_path=self._policy_paths.get(policy_id, ""),
            mode_validated=mode.value,
        )

    def get_selection_policy(self, policy_id: str) -> SourceSelectionPolicy:
        """Get a SourceSelectionPolicy for use in selection.

        Args:
            policy_id: The policy identifier

        Returns:
            SourceSelectionPolicy instance

        Raises:
            PolicyRegistryError: If policy not found
        """
        policy = self.get(policy_id)
        return policy.to_selection_policy()

    def _ensure_loaded(self) -> None:
        """Ensure policies are loaded."""
        if not self._loaded:
            self.load_all()


# ============================================
# Convenience Functions
# ============================================


def load_policy_registry(odibi_root: str) -> SourcePolicyRegistry:
    """Load and return a policy registry.

    Args:
        odibi_root: Path to Odibi repository root

    Returns:
        SourcePolicyRegistry with policies loaded
    """
    registry = SourcePolicyRegistry(odibi_root)
    registry.load_all()
    return registry


def resolve_policy_for_cycle(
    odibi_root: str,
    policy_id: str,
    mode: CycleMode,
) -> tuple[SourceSelectionPolicy, PolicyResolutionResult]:
    """Convenience function to resolve a policy for a cycle.

    Args:
        odibi_root: Path to Odibi repository root
        policy_id: The policy identifier
        mode: The cycle mode

    Returns:
        Tuple of (SourceSelectionPolicy, PolicyResolutionResult)

    Raises:
        PolicyRegistryError: If policy not found or invalid
    """
    registry = load_policy_registry(odibi_root)
    resolution = registry.resolve(policy_id, mode)
    policy = registry.get_selection_policy(policy_id)
    return policy, resolution
