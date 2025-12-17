"""Cycle Profile Schema and Loader - Phase 9.C.

This module provides:
1. CycleProfile Pydantic schema for validating profile YAML files
2. Profile loading from .odibi/cycle_profiles/
3. Profile hash computation for auditability
4. Deterministic resolution to LearningCycleConfig

PHASE 9.C INVARIANTS:
- Profiles are READ-ONLY at runtime
- Profile config is FROZEN for entire session
- Same profile + same inputs → same execution
- No runtime reloading
- No agent writes to profiles

SAFETY:
- Fail fast on invalid profiles
- All existing learning-mode safety guards maintained
"""

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)


class CycleProfileError(Exception):
    """Raised when profile loading or validation fails."""

    def __init__(self, message: str, profile_path: str = "", errors: list[str] | None = None):
        self.message = message
        self.profile_path = profile_path
        self.errors = errors or []
        super().__init__(f"{message}: {profile_path}" if profile_path else message)


class CycleSourceConfigSchema(BaseModel):
    """Schema for cycle_source_config section of a profile."""

    selection_policy: str = Field(
        default="learning_default",
        description="Reference to selection policy in .odibi/selection_policies/",
    )
    allowed_tiers: list[str] = Field(
        default_factory=lambda: ["tier100gb", "tier600gb"],
        description="Allowed data tiers for this profile",
    )
    max_sources: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Maximum number of source pools per cycle",
    )
    require_frozen: bool = Field(
        default=True,
        description="Require frozen source pools (must be True for learning mode)",
    )
    require_clean: bool = Field(
        default=False,
        description="Require clean data (false allows observing quality issues)",
    )
    allow_messy: bool = Field(
        default=True,
        description="Allow messy data for learning from real-world patterns",
    )
    deterministic: bool = Field(
        default=True,
        description="Require deterministic source selection (must be True for reproducibility)",
    )
    allow_tier_mixing: bool = Field(
        default=False,
        description="Allow mixing data from different tiers in one cycle",
    )


class GuardrailsSchema(BaseModel):
    """Schema for guardrails section of a profile."""

    allow_execution: bool = Field(
        default=False,
        description="Allow execution of external commands (should be False for safety)",
    )
    allow_downloads: bool = Field(
        default=False,
        description="Allow data downloads during cycle (should be False for safety)",
    )
    allow_source_mutation: bool = Field(
        default=False,
        description="Allow modifications to source pools (must be False)",
    )
    max_duration_hours: float = Field(
        default=24.0,
        ge=0.1,
        le=168.0,
        description="Maximum cycle duration in hours",
    )


class ProfileMetadataSchema(BaseModel):
    """Schema for metadata section of a profile."""

    author: str = Field(default="", description="Profile author")
    created_at: str = Field(default="", description="Profile creation timestamp")
    intended_use: str = Field(default="", description="Intended use case description")
    review_required: bool = Field(default=True, description="Whether human review is required")
    non_goals: list[str] = Field(
        default_factory=list,
        description="Explicit list of things this profile should NOT do",
    )


class CycleProfile(BaseModel):
    """Pydantic schema for cycle profile YAML files.

    Validates profile structure and enforces Phase 9.A invariants
    for learning mode profiles.
    """

    profile_id: str = Field(..., description="Unique profile identifier")
    profile_name: str = Field(..., description="Human-readable profile name")
    profile_version: str = Field(default="1.0.0", description="Profile version")
    description: str = Field(default="", description="Profile description")

    mode: str = Field(
        default="learning",
        description="Cycle mode (must be 'learning' for Phase 9.A)",
    )
    max_improvements: int = Field(
        default=0,
        ge=0,
        description="Maximum improvements (must be 0 for learning mode)",
    )

    cycle_source_config: CycleSourceConfigSchema = Field(
        default_factory=CycleSourceConfigSchema,
        description="Source configuration for the cycle",
    )
    guardrails: GuardrailsSchema = Field(
        default_factory=GuardrailsSchema,
        description="Safety guardrails for the cycle",
    )
    metadata: ProfileMetadataSchema = Field(
        default_factory=ProfileMetadataSchema,
        description="Profile metadata",
    )

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, v: str) -> str:
        if v != "learning":
            raise ValueError(f"mode MUST be 'learning' for Phase 9.A, got '{v}'")
        return v

    @field_validator("max_improvements")
    @classmethod
    def validate_max_improvements(cls, v: int) -> int:
        if v != 0:
            raise ValueError(f"max_improvements MUST be 0 for learning mode, got {v}")
        return v

    @model_validator(mode="after")
    def validate_learning_invariants(self) -> "CycleProfile":
        """Validate Phase 9.A learning mode invariants."""
        if not self.cycle_source_config.require_frozen:
            raise ValueError("cycle_source_config.require_frozen MUST be True for learning mode")
        if not self.cycle_source_config.deterministic:
            raise ValueError("cycle_source_config.deterministic MUST be True for reproducibility")
        if self.guardrails.allow_source_mutation:
            raise ValueError("guardrails.allow_source_mutation MUST be False")
        return self


@dataclass(frozen=True)
class FrozenCycleProfile:
    """Immutable, frozen representation of a loaded profile.

    This is the runtime representation that CANNOT be modified
    after loading. Used throughout a session to ensure
    deterministic behavior.
    """

    profile_id: str
    profile_name: str
    profile_version: str
    description: str
    mode: str
    max_improvements: int

    selection_policy: str
    allowed_tiers: tuple[str, ...]
    max_sources: int
    require_frozen: bool
    require_clean: bool
    allow_messy: bool
    deterministic: bool
    allow_tier_mixing: bool

    allow_execution: bool
    allow_downloads: bool
    allow_source_mutation: bool
    max_duration_hours: float

    author: str
    intended_use: str
    review_required: bool

    content_hash: str
    source_path: str
    loaded_at: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "profile_id": self.profile_id,
            "profile_name": self.profile_name,
            "profile_version": self.profile_version,
            "description": self.description,
            "mode": self.mode,
            "max_improvements": self.max_improvements,
            "selection_policy": self.selection_policy,
            "allowed_tiers": list(self.allowed_tiers),
            "max_sources": self.max_sources,
            "require_frozen": self.require_frozen,
            "require_clean": self.require_clean,
            "allow_messy": self.allow_messy,
            "deterministic": self.deterministic,
            "allow_tier_mixing": self.allow_tier_mixing,
            "allow_execution": self.allow_execution,
            "allow_downloads": self.allow_downloads,
            "allow_source_mutation": self.allow_source_mutation,
            "max_duration_hours": self.max_duration_hours,
            "author": self.author,
            "intended_use": self.intended_use,
            "review_required": self.review_required,
            "content_hash": self.content_hash,
            "source_path": self.source_path,
            "loaded_at": self.loaded_at,
        }

    def get_audit_summary(self) -> dict[str, Any]:
        """Get summary for audit trails (heartbeat, reports, metadata)."""
        return {
            "profile_id": self.profile_id,
            "profile_name": self.profile_name,
            "profile_version": self.profile_version,
            "content_hash": self.content_hash,
            "source_path": self.source_path,
            "loaded_at": self.loaded_at,
        }


def compute_profile_hash(content: str) -> str:
    """Compute SHA-256 hash of profile content.

    Uses canonical form (normalized whitespace) for consistent hashing.

    Args:
        content: Raw YAML content.

    Returns:
        SHA-256 hex digest (first 16 chars for brevity).
    """
    normalized = content.strip().encode("utf-8")
    full_hash = hashlib.sha256(normalized).hexdigest()
    return full_hash[:16]


def load_profile_yaml(profile_path: str) -> tuple[dict[str, Any], str]:
    """Load and parse a profile YAML file.

    Args:
        profile_path: Absolute path to the profile YAML.

    Returns:
        Tuple of (parsed dict, raw content for hashing).

    Raises:
        CycleProfileError: If file cannot be read or parsed.
    """
    if not os.path.exists(profile_path):
        raise CycleProfileError(
            "Profile file not found",
            profile_path=profile_path,
        )

    try:
        with open(profile_path, "r", encoding="utf-8") as f:
            raw_content = f.read()
    except Exception as e:
        raise CycleProfileError(
            f"Failed to read profile file: {e}",
            profile_path=profile_path,
        )

    try:
        parsed = yaml.safe_load(raw_content)
    except yaml.YAMLError as e:
        raise CycleProfileError(
            f"Invalid YAML syntax: {e}",
            profile_path=profile_path,
        )

    if not isinstance(parsed, dict):
        raise CycleProfileError(
            "Profile must be a YAML mapping (dict)",
            profile_path=profile_path,
        )

    return parsed, raw_content


def validate_profile(profile_dict: dict[str, Any], profile_path: str = "") -> CycleProfile:
    """Validate a profile dictionary against the schema.

    Args:
        profile_dict: Parsed profile dictionary.
        profile_path: Optional path for error messages.

    Returns:
        Validated CycleProfile.

    Raises:
        CycleProfileError: If validation fails.
    """
    try:
        return CycleProfile(**profile_dict)
    except Exception as e:
        errors = []
        if hasattr(e, "errors"):
            for err in e.errors():
                loc = ".".join(str(x) for x in err.get("loc", []))
                msg = err.get("msg", str(err))
                errors.append(f"{loc}: {msg}")
        else:
            errors.append(str(e))

        raise CycleProfileError(
            "Profile validation failed",
            profile_path=profile_path,
            errors=errors,
        )


def freeze_profile(
    profile: CycleProfile,
    content_hash: str,
    source_path: str,
) -> FrozenCycleProfile:
    """Create an immutable frozen profile from a validated CycleProfile.

    Args:
        profile: Validated CycleProfile.
        content_hash: SHA-256 hash of the source file.
        source_path: Path to the source YAML file.

    Returns:
        FrozenCycleProfile that cannot be modified.
    """
    return FrozenCycleProfile(
        profile_id=profile.profile_id,
        profile_name=profile.profile_name,
        profile_version=profile.profile_version,
        description=profile.description,
        mode=profile.mode,
        max_improvements=profile.max_improvements,
        selection_policy=profile.cycle_source_config.selection_policy,
        allowed_tiers=tuple(profile.cycle_source_config.allowed_tiers),
        max_sources=profile.cycle_source_config.max_sources,
        require_frozen=profile.cycle_source_config.require_frozen,
        require_clean=profile.cycle_source_config.require_clean,
        allow_messy=profile.cycle_source_config.allow_messy,
        deterministic=profile.cycle_source_config.deterministic,
        allow_tier_mixing=profile.cycle_source_config.allow_tier_mixing,
        allow_execution=profile.guardrails.allow_execution,
        allow_downloads=profile.guardrails.allow_downloads,
        allow_source_mutation=profile.guardrails.allow_source_mutation,
        max_duration_hours=profile.guardrails.max_duration_hours,
        author=profile.metadata.author,
        intended_use=profile.metadata.intended_use,
        review_required=profile.metadata.review_required,
        content_hash=content_hash,
        source_path=source_path,
        loaded_at=datetime.now().isoformat(),
    )


class CycleProfileLoader:
    """Loader for cycle profiles from .odibi/cycle_profiles/.

    Provides:
    - Profile discovery
    - Profile loading with validation
    - Profile freezing for runtime use

    Thread-safe for read operations.
    """

    def __init__(self, odibi_root: str):
        """Initialize profile loader.

        Args:
            odibi_root: Path to Odibi repository root.
        """
        self.odibi_root = os.path.abspath(odibi_root)
        self.profiles_dir = os.path.join(self.odibi_root, ".odibi", "cycle_profiles")

    def list_profiles(self) -> list[str]:
        """List available profile names.

        Returns:
            List of profile names (without .yaml extension).
        """
        if not os.path.isdir(self.profiles_dir):
            return []

        profiles = []
        for filename in os.listdir(self.profiles_dir):
            if filename.endswith(".yaml") or filename.endswith(".yml"):
                name = filename.rsplit(".", 1)[0]
                profiles.append(name)

        return sorted(profiles)

    def get_profile_path(self, profile_name: str) -> str:
        """Get the full path to a profile file.

        Args:
            profile_name: Profile name (without extension).

        Returns:
            Absolute path to the profile YAML.

        Raises:
            CycleProfileError: If profile not found.
        """
        yaml_path = os.path.join(self.profiles_dir, f"{profile_name}.yaml")
        if os.path.exists(yaml_path):
            return yaml_path

        yml_path = os.path.join(self.profiles_dir, f"{profile_name}.yml")
        if os.path.exists(yml_path):
            return yml_path

        raise CycleProfileError(
            f"Profile not found: {profile_name}",
            profile_path=yaml_path,
        )

    def load_profile(self, profile_name: str) -> FrozenCycleProfile:
        """Load and freeze a profile by name.

        This is the main entry point for loading profiles.
        Returns a frozen, immutable profile that cannot be
        modified at runtime.

        Args:
            profile_name: Profile name (without extension).

        Returns:
            FrozenCycleProfile ready for use.

        Raises:
            CycleProfileError: If loading or validation fails.
        """
        profile_path = self.get_profile_path(profile_name)
        return self.load_profile_from_path(profile_path)

    def load_profile_from_path(self, profile_path: str) -> FrozenCycleProfile:
        """Load and freeze a profile from a specific path.

        Args:
            profile_path: Absolute path to profile YAML.

        Returns:
            FrozenCycleProfile ready for use.

        Raises:
            CycleProfileError: If loading or validation fails.
        """
        profile_dict, raw_content = load_profile_yaml(profile_path)
        validated = validate_profile(profile_dict, profile_path)
        content_hash = compute_profile_hash(raw_content)
        return freeze_profile(validated, content_hash, profile_path)

    def get_profile_summary(self, profile_name: str) -> dict[str, Any]:
        """Get a summary of a profile without fully loading it.

        Useful for UI display before selection.

        Args:
            profile_name: Profile name (without extension).

        Returns:
            Dict with profile_id, profile_name, description, hash.

        Raises:
            CycleProfileError: If profile cannot be read.
        """
        profile_path = self.get_profile_path(profile_name)
        profile_dict, raw_content = load_profile_yaml(profile_path)
        content_hash = compute_profile_hash(raw_content)

        return {
            "profile_id": profile_dict.get("profile_id", profile_name),
            "profile_name": profile_dict.get("profile_name", profile_name),
            "description": profile_dict.get("description", ""),
            "mode": profile_dict.get("mode", ""),
            "content_hash": content_hash,
            "source_path": profile_path,
        }
