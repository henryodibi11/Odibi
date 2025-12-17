"""Tests for Source Policy Registry (Phase 8.C).

Tests cover:
1. Deterministic loading from disk
2. Invalid policy rejection
3. Hash stability
4. Scheduled vs Improvement mode enforcement
5. PolicyResolutionResult creation
6. Registry API (list, get, exists)
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from odibi.agents.core.source_policy_registry import (
    PolicyRegistryError,
    PolicyResolutionResult,
    PolicyValidationIssue,
    PolicyValidationLevel,
    PolicyValidationStatus,
    PolicyValidator,
    RegisteredPolicy,
    SourcePolicyRegistry,
    load_policy_registry,
    resolve_policy_for_cycle,
)
from odibi.agents.core.source_selection import CycleMode, SelectionStrategy


# ============================================
# Fixtures
# ============================================


@pytest.fixture
def temp_odibi_root():
    """Create a temporary Odibi root with policies directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        policies_dir = Path(tmpdir) / ".odibi" / "selection_policies"
        policies_dir.mkdir(parents=True)
        yield tmpdir


@pytest.fixture
def valid_policy_data():
    """Return valid policy data for testing."""
    return {
        "policy_id": "test_policy",
        "name": "Test Policy",
        "description": "A test policy for unit tests",
        "max_pools_per_cycle": 3,
        "allow_messy_data": True,
        "selection_strategy": "hash_based",
        "require_frozen": True,
        "allowed_modes": ["learning", "scheduled"],
        "version": "1.0.0",
        "author": "test",
    }


@pytest.fixture
def scheduled_policy_data():
    """Return a valid scheduled-mode policy."""
    return {
        "policy_id": "scheduled_test",
        "name": "Scheduled Test Policy",
        "description": "Policy for scheduled cycles",
        "max_pools_per_cycle": 2,
        "allow_messy_data": False,
        "selection_strategy": "hash_based",
        "require_frozen": True,
        "allowed_modes": ["scheduled"],
        "disallowed_tiers": ["tier600gb", "tier2tb"],
        "version": "1.0.0",
    }


@pytest.fixture
def improvement_policy_data():
    """Return a valid improvement-mode policy."""
    return {
        "policy_id": "improvement_test",
        "name": "Improvement Test Policy",
        "description": "Policy for improvement cycles",
        "max_pools_per_cycle": 2,
        "allow_messy_data": False,
        "selection_strategy": "hash_based",
        "require_frozen": True,
        "allowed_modes": ["improvement"],
        "disallowed_tiers": ["tier600gb", "tier2tb"],
        "version": "1.0.0",
    }


def write_policy_file(policies_dir: Path, filename: str, data: dict) -> Path:
    """Write a policy YAML file."""
    policy_path = policies_dir / filename
    with open(policy_path, "w", encoding="utf-8") as f:
        yaml.dump(data, f)
    return policy_path


# ============================================
# RegisteredPolicy Tests
# ============================================


class TestRegisteredPolicy:
    """Tests for RegisteredPolicy schema."""

    def test_create_valid_policy(self, valid_policy_data):
        """Valid policy data should create a RegisteredPolicy."""
        policy = RegisteredPolicy(**valid_policy_data)
        assert policy.policy_id == "test_policy"
        assert policy.name == "Test Policy"
        assert policy.require_frozen is True

    def test_explicit_strategy_requires_pool_order(self):
        """EXPLICIT strategy must have explicit_pool_order."""
        with pytest.raises(ValueError, match="explicit_pool_order"):
            RegisteredPolicy(
                policy_id="explicit_test",
                name="Explicit Test",
                selection_strategy=SelectionStrategy.EXPLICIT,
            )

    def test_explicit_strategy_with_pool_order(self):
        """EXPLICIT strategy with pool_order should succeed."""
        policy = RegisteredPolicy(
            policy_id="explicit_test",
            name="Explicit Test",
            selection_strategy=SelectionStrategy.EXPLICIT,
            explicit_pool_order=["pool_a", "pool_b"],
        )
        assert policy.explicit_pool_order == ["pool_a", "pool_b"]

    def test_to_selection_policy(self, valid_policy_data):
        """RegisteredPolicy should convert to SourceSelectionPolicy."""
        policy = RegisteredPolicy(**valid_policy_data)
        selection_policy = policy.to_selection_policy()

        assert selection_policy.policy_id == "test_policy"
        assert selection_policy.name == "Test Policy"
        assert selection_policy.max_pools_per_cycle == 3


# ============================================
# PolicyValidator Tests
# ============================================


class TestPolicyValidator:
    """Tests for PolicyValidator."""

    def test_validate_valid_policy(self, valid_policy_data):
        """Valid policy should have no ERROR issues."""
        policy = RegisteredPolicy(**valid_policy_data)
        validator = PolicyValidator()
        issues = validator.validate(policy)

        errors = [i for i in issues if i.level == PolicyValidationLevel.ERROR]
        assert len(errors) == 0

    def test_scheduled_requires_frozen(self, valid_policy_data):
        """Scheduled mode should require require_frozen=True."""
        valid_policy_data["require_frozen"] = False
        policy = RegisteredPolicy(**valid_policy_data)
        validator = PolicyValidator()

        issues = validator.validate(policy, mode=CycleMode.SCHEDULED)

        errors = [i for i in issues if i.level == PolicyValidationLevel.ERROR]
        assert len(errors) == 1
        assert errors[0].rule == "SCHEDULED_REQUIRES_FROZEN"

    def test_scheduled_with_frozen_passes(self, scheduled_policy_data):
        """Scheduled mode with require_frozen=True should pass."""
        policy = RegisteredPolicy(**scheduled_policy_data)
        validator = PolicyValidator()

        issues = validator.validate(policy, mode=CycleMode.SCHEDULED)

        errors = [i for i in issues if i.level == PolicyValidationLevel.ERROR]
        assert len(errors) == 0

    def test_improvement_tier_warning(self, valid_policy_data):
        """Improvement mode should warn about missing tier exclusions."""
        valid_policy_data["disallowed_tiers"] = []
        valid_policy_data["allowed_modes"] = ["improvement"]
        policy = RegisteredPolicy(**valid_policy_data)
        validator = PolicyValidator()

        issues = validator.validate(policy, mode=CycleMode.IMPROVEMENT)

        warnings = [i for i in issues if i.level == PolicyValidationLevel.WARNING]
        tier_warnings = [i for i in warnings if i.rule == "IMPROVEMENT_TIER_RESTRICTION"]
        assert len(tier_warnings) == 1

    def test_improvement_with_tier_exclusions(self, improvement_policy_data):
        """Improvement mode with proper tier exclusions should pass."""
        policy = RegisteredPolicy(**improvement_policy_data)
        validator = PolicyValidator()

        issues = validator.validate(policy, mode=CycleMode.IMPROVEMENT)

        tier_warnings = [
            i
            for i in issues
            if i.rule == "IMPROVEMENT_TIER_RESTRICTION" and i.level == PolicyValidationLevel.WARNING
        ]
        assert len(tier_warnings) == 0

    def test_mode_not_allowed(self, scheduled_policy_data):
        """Policy should fail if mode not in allowed_modes."""
        policy = RegisteredPolicy(**scheduled_policy_data)
        validator = PolicyValidator()

        issues = validator.validate(policy, mode=CycleMode.LEARNING)

        errors = [i for i in issues if i.level == PolicyValidationLevel.ERROR]
        mode_errors = [i for i in errors if i.rule == "MODE_NOT_ALLOWED"]
        assert len(mode_errors) == 1

    def test_conflicting_flags_warning(self, valid_policy_data):
        """Conflicting clean_vs_messy_ratio and allow_messy should warn."""
        valid_policy_data["clean_vs_messy_ratio"] = 1.0
        valid_policy_data["allow_messy_data"] = True
        policy = RegisteredPolicy(**valid_policy_data)
        validator = PolicyValidator()

        issues = validator.validate(policy)

        warnings = [i for i in issues if i.level == PolicyValidationLevel.WARNING]
        conflict_warnings = [i for i in warnings if i.rule == "CONFLICTING_FLAGS"]
        assert len(conflict_warnings) == 1

    def test_validation_status_valid(self):
        """No issues should return VALID status."""
        validator = PolicyValidator()
        status = validator.get_validation_status([])
        assert status == PolicyValidationStatus.VALID

    def test_validation_status_invalid(self):
        """ERROR issues should return INVALID status."""
        validator = PolicyValidator()
        issues = [
            PolicyValidationIssue(
                level=PolicyValidationLevel.ERROR,
                rule="TEST",
                field="test",
                message="Test error",
            )
        ]
        status = validator.get_validation_status(issues)
        assert status == PolicyValidationStatus.INVALID

    def test_validation_status_warnings(self):
        """Only WARNING issues should return WARNINGS status."""
        validator = PolicyValidator()
        issues = [
            PolicyValidationIssue(
                level=PolicyValidationLevel.WARNING,
                rule="TEST",
                field="test",
                message="Test warning",
            )
        ]
        status = validator.get_validation_status(issues)
        assert status == PolicyValidationStatus.WARNINGS


# ============================================
# PolicyResolutionResult Tests
# ============================================


class TestPolicyResolutionResult:
    """Tests for PolicyResolutionResult dataclass."""

    def test_create_result(self):
        """Should create result with all fields."""
        result = PolicyResolutionResult(
            policy_name="test_policy",
            policy_hash="abc123",
            validation_status=PolicyValidationStatus.VALID,
            resolved_at="2024-01-01T00:00:00Z",
        )
        assert result.policy_name == "test_policy"
        assert result.policy_hash == "abc123"
        assert result.validation_status == PolicyValidationStatus.VALID

    def test_result_with_warnings(self):
        """Should store warnings."""
        result = PolicyResolutionResult(
            policy_name="test_policy",
            policy_hash="abc123",
            validation_status=PolicyValidationStatus.WARNINGS,
            resolved_at="2024-01-01T00:00:00Z",
            warnings=["Warning 1", "Warning 2"],
        )
        assert len(result.warnings) == 2

    def test_to_dict(self):
        """Should serialize to dict."""
        result = PolicyResolutionResult(
            policy_name="test_policy",
            policy_hash="abc123",
            validation_status=PolicyValidationStatus.VALID,
            resolved_at="2024-01-01T00:00:00Z",
            policy_version="1.0.0",
        )
        d = result.to_dict()
        assert d["policy_name"] == "test_policy"
        assert d["validation_status"] == "valid"
        assert d["policy_version"] == "1.0.0"


# ============================================
# SourcePolicyRegistry Tests
# ============================================


class TestSourcePolicyRegistry:
    """Tests for SourcePolicyRegistry."""

    def test_load_empty_registry(self, temp_odibi_root):
        """Empty policies directory should load zero policies."""
        registry = SourcePolicyRegistry(temp_odibi_root)
        count = registry.load_all()
        assert count == 0
        assert registry.list() == []

    def test_load_single_policy(self, temp_odibi_root, valid_policy_data):
        """Should load a single policy from YAML file."""
        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"
        write_policy_file(policies_dir, "test_policy.yaml", valid_policy_data)

        registry = SourcePolicyRegistry(temp_odibi_root)
        count = registry.load_all()

        assert count == 1
        assert registry.exists("test_policy")
        assert "test_policy" in registry.list()

    def test_load_multiple_policies(self, temp_odibi_root, valid_policy_data):
        """Should load multiple policies."""
        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"

        write_policy_file(policies_dir, "policy_a.yaml", valid_policy_data)

        policy_b = valid_policy_data.copy()
        policy_b["policy_id"] = "policy_b"
        policy_b["name"] = "Policy B"
        write_policy_file(policies_dir, "policy_b.yaml", policy_b)

        registry = SourcePolicyRegistry(temp_odibi_root)
        count = registry.load_all()

        assert count == 2
        assert len(registry.list()) == 2

    def test_get_policy(self, temp_odibi_root, valid_policy_data):
        """Should retrieve policy by ID."""
        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"
        write_policy_file(policies_dir, "test_policy.yaml", valid_policy_data)

        registry = SourcePolicyRegistry(temp_odibi_root)
        registry.load_all()

        policy = registry.get("test_policy")
        assert policy.policy_id == "test_policy"
        assert policy.name == "Test Policy"

    def test_get_nonexistent_policy(self, temp_odibi_root):
        """Should raise error for nonexistent policy."""
        registry = SourcePolicyRegistry(temp_odibi_root)
        registry.load_all()

        with pytest.raises(PolicyRegistryError) as exc_info:
            registry.get("nonexistent")

        assert exc_info.value.error_type == "POLICY_NOT_FOUND"

    def test_exists_true(self, temp_odibi_root, valid_policy_data):
        """exists() should return True for existing policy."""
        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"
        write_policy_file(policies_dir, "test_policy.yaml", valid_policy_data)

        registry = SourcePolicyRegistry(temp_odibi_root)
        registry.load_all()

        assert registry.exists("test_policy") is True

    def test_exists_false(self, temp_odibi_root):
        """exists() should return False for nonexistent policy."""
        registry = SourcePolicyRegistry(temp_odibi_root)
        registry.load_all()

        assert registry.exists("nonexistent") is False

    def test_list_sorted(self, temp_odibi_root, valid_policy_data):
        """list() should return sorted policy IDs."""
        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"

        for name in ["zebra", "alpha", "beta"]:
            data = valid_policy_data.copy()
            data["policy_id"] = name
            data["name"] = name.title()
            write_policy_file(policies_dir, f"{name}.yaml", data)

        registry = SourcePolicyRegistry(temp_odibi_root)
        registry.load_all()

        assert registry.list() == ["alpha", "beta", "zebra"]

    def test_hash_stability(self, temp_odibi_root, valid_policy_data):
        """Same policy should produce same hash."""
        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"
        write_policy_file(policies_dir, "test_policy.yaml", valid_policy_data)

        registry1 = SourcePolicyRegistry(temp_odibi_root)
        registry1.load_all()
        hash1 = registry1.get_hash("test_policy")

        registry2 = SourcePolicyRegistry(temp_odibi_root)
        registry2.load_all()
        hash2 = registry2.get_hash("test_policy")

        assert hash1 == hash2

    def test_hash_changes_with_content(self, temp_odibi_root, valid_policy_data):
        """Different policy content should produce different hash."""
        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"
        write_policy_file(policies_dir, "test_policy.yaml", valid_policy_data)

        registry1 = SourcePolicyRegistry(temp_odibi_root)
        registry1.load_all()
        hash1 = registry1.get_hash("test_policy")

        # Modify policy
        valid_policy_data["max_pools_per_cycle"] = 5
        write_policy_file(policies_dir, "test_policy.yaml", valid_policy_data)

        registry2 = SourcePolicyRegistry(temp_odibi_root)
        registry2.load_all()
        hash2 = registry2.get_hash("test_policy")

        assert hash1 != hash2


# ============================================
# Registry Resolve Tests
# ============================================


class TestRegistryResolve:
    """Tests for registry.resolve() method."""

    def test_resolve_valid_policy(self, temp_odibi_root, scheduled_policy_data):
        """Should resolve valid policy successfully."""
        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"
        write_policy_file(policies_dir, "scheduled_test.yaml", scheduled_policy_data)

        registry = SourcePolicyRegistry(temp_odibi_root)
        registry.load_all()

        result = registry.resolve("scheduled_test", CycleMode.SCHEDULED)

        assert result.policy_name == "scheduled_test"
        assert result.validation_status == PolicyValidationStatus.VALID
        assert result.policy_hash != ""

    def test_resolve_invalid_policy_fails(self, temp_odibi_root, valid_policy_data):
        """Should raise error for invalid policy when fail_on_error=True."""
        # Create policy that's invalid for SCHEDULED mode
        valid_policy_data["require_frozen"] = False
        valid_policy_data["allowed_modes"] = ["scheduled"]

        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"
        write_policy_file(policies_dir, "test_policy.yaml", valid_policy_data)

        registry = SourcePolicyRegistry(temp_odibi_root)
        registry.load_all()

        with pytest.raises(PolicyRegistryError) as exc_info:
            registry.resolve("test_policy", CycleMode.SCHEDULED)

        assert exc_info.value.error_type == "POLICY_VALIDATION_FAILED"

    def test_resolve_with_warnings(self, temp_odibi_root, valid_policy_data):
        """Should include warnings in result."""
        # Remove disallowed_tiers to trigger warning
        valid_policy_data["disallowed_tiers"] = []
        valid_policy_data["allowed_modes"] = ["improvement"]

        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"
        write_policy_file(policies_dir, "test_policy.yaml", valid_policy_data)

        registry = SourcePolicyRegistry(temp_odibi_root)
        registry.load_all()

        result = registry.resolve("test_policy", CycleMode.IMPROVEMENT)

        assert result.validation_status == PolicyValidationStatus.WARNINGS
        assert len(result.warnings) > 0

    def test_get_selection_policy(self, temp_odibi_root, valid_policy_data):
        """Should return SourceSelectionPolicy for use in selection."""
        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"
        write_policy_file(policies_dir, "test_policy.yaml", valid_policy_data)

        registry = SourcePolicyRegistry(temp_odibi_root)
        registry.load_all()

        selection_policy = registry.get_selection_policy("test_policy")

        assert selection_policy.policy_id == "test_policy"
        assert selection_policy.max_pools_per_cycle == 3


# ============================================
# Invalid Policy Tests
# ============================================


class TestInvalidPolicies:
    """Tests for invalid policy rejection."""

    def test_missing_policy_id(self, temp_odibi_root):
        """Should reject policy without policy_id."""
        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"
        invalid_data = {
            "name": "Invalid Policy",
        }
        write_policy_file(policies_dir, "invalid.yaml", invalid_data)

        registry = SourcePolicyRegistry(temp_odibi_root)

        with pytest.raises(PolicyRegistryError) as exc_info:
            registry.load_all()

        assert exc_info.value.error_type == "POLICY_LOAD_ERROR"

    def test_invalid_max_pools(self, temp_odibi_root, valid_policy_data):
        """Should reject policy with invalid max_pools_per_cycle."""
        valid_policy_data["max_pools_per_cycle"] = 100  # Max is 10

        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"
        write_policy_file(policies_dir, "invalid.yaml", valid_policy_data)

        registry = SourcePolicyRegistry(temp_odibi_root)

        with pytest.raises(PolicyRegistryError) as exc_info:
            registry.load_all()

        assert exc_info.value.error_type == "POLICY_LOAD_ERROR"


# ============================================
# Determinism Tests
# ============================================


class TestDeterminism:
    """Tests for deterministic behavior."""

    def test_loading_order_deterministic(self, temp_odibi_root, valid_policy_data):
        """Loading should be deterministic regardless of file order."""
        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"

        for name in ["zulu", "alpha", "mike"]:
            data = valid_policy_data.copy()
            data["policy_id"] = name
            data["name"] = name.title()
            write_policy_file(policies_dir, f"{name}.yaml", data)

        # Load multiple times
        results = []
        for _ in range(3):
            registry = SourcePolicyRegistry(temp_odibi_root)
            registry.load_all()
            results.append(registry.list())

        # All loads should produce same order
        assert all(r == results[0] for r in results)

    def test_hash_deterministic(self, temp_odibi_root, valid_policy_data):
        """Policy hash should be deterministic."""
        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"
        write_policy_file(policies_dir, "test_policy.yaml", valid_policy_data)

        hashes = []
        for _ in range(5):
            registry = SourcePolicyRegistry(temp_odibi_root)
            registry.load_all()
            hashes.append(registry.get_hash("test_policy"))

        assert all(h == hashes[0] for h in hashes)


# ============================================
# Convenience Function Tests
# ============================================


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    def test_load_policy_registry(self, temp_odibi_root, valid_policy_data):
        """load_policy_registry should return loaded registry."""
        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"
        write_policy_file(policies_dir, "test_policy.yaml", valid_policy_data)

        registry = load_policy_registry(temp_odibi_root)

        assert registry.exists("test_policy")

    def test_resolve_policy_for_cycle(self, temp_odibi_root, scheduled_policy_data):
        """resolve_policy_for_cycle should return policy and result."""
        policies_dir = Path(temp_odibi_root) / ".odibi" / "selection_policies"
        write_policy_file(policies_dir, "scheduled_test.yaml", scheduled_policy_data)

        # Also create source_metadata directory for SourceSelector
        metadata_dir = Path(temp_odibi_root) / ".odibi" / "source_metadata"
        metadata_dir.mkdir(parents=True)

        policy, result = resolve_policy_for_cycle(
            temp_odibi_root, "scheduled_test", CycleMode.SCHEDULED
        )

        assert policy.policy_id == "scheduled_test"
        assert result.policy_name == "scheduled_test"
        assert result.validation_status == PolicyValidationStatus.VALID
