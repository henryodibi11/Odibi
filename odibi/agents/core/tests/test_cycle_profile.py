"""Tests for Phase 9.C Cycle Profile Schema and Loader.

These tests verify:
1. CycleProfile Pydantic schema validation
2. Profile loading from YAML files
3. Profile hash computation
4. Frozen profile creation
5. Profile loader discovery and loading
6. Learning mode invariant enforcement
"""

import os
import tempfile
from pathlib import Path

import pytest

from odibi.agents.core.cycle_profile import (
    CycleProfile,
    CycleProfileError,
    CycleProfileLoader,
    compute_profile_hash,
    freeze_profile,
    load_profile_yaml,
    validate_profile,
)


VALID_PROFILE_YAML = """
profile_id: test_learning
profile_name: "Test Learning Profile"
profile_version: "1.0.0"
description: |
  Test profile for unit tests.

mode: learning
max_improvements: 0

cycle_source_config:
  selection_policy: learning_default
  allowed_tiers:
    - tier100gb
    - tier600gb
  max_sources: 3
  require_frozen: true
  require_clean: false
  allow_messy: true
  deterministic: true
  allow_tier_mixing: false

guardrails:
  allow_execution: false
  allow_downloads: false
  allow_source_mutation: false
  max_duration_hours: 24

metadata:
  author: test
  created_at: "2024-01-01T00:00:00Z"
  intended_use: "Unit testing"
  review_required: true
"""


class TestCycleProfileSchema:
    """Test CycleProfile Pydantic schema validation."""

    def test_valid_profile_parses(self):
        """Valid profile YAML should parse without errors."""
        import yaml

        data = yaml.safe_load(VALID_PROFILE_YAML)
        profile = CycleProfile(**data)

        assert profile.profile_id == "test_learning"
        assert profile.profile_name == "Test Learning Profile"
        assert profile.mode == "learning"
        assert profile.max_improvements == 0
        assert profile.cycle_source_config.require_frozen is True
        assert profile.cycle_source_config.deterministic is True

    def test_mode_must_be_learning(self):
        """Profile mode must be 'learning'."""
        import yaml

        data = yaml.safe_load(VALID_PROFILE_YAML)
        data["mode"] = "improvement"

        with pytest.raises(ValueError, match="mode MUST be 'learning'"):
            CycleProfile(**data)

    def test_max_improvements_must_be_zero(self):
        """max_improvements must be 0 for learning mode."""
        import yaml

        data = yaml.safe_load(VALID_PROFILE_YAML)
        data["max_improvements"] = 5

        with pytest.raises(ValueError, match="max_improvements MUST be 0"):
            CycleProfile(**data)

    def test_require_frozen_must_be_true(self):
        """require_frozen must be True for learning mode."""
        import yaml

        data = yaml.safe_load(VALID_PROFILE_YAML)
        data["cycle_source_config"]["require_frozen"] = False

        with pytest.raises(ValueError, match="require_frozen MUST be True"):
            CycleProfile(**data)

    def test_deterministic_must_be_true(self):
        """deterministic must be True for learning mode."""
        import yaml

        data = yaml.safe_load(VALID_PROFILE_YAML)
        data["cycle_source_config"]["deterministic"] = False

        with pytest.raises(ValueError, match="deterministic MUST be True"):
            CycleProfile(**data)

    def test_allow_source_mutation_must_be_false(self):
        """allow_source_mutation must be False."""
        import yaml

        data = yaml.safe_load(VALID_PROFILE_YAML)
        data["guardrails"]["allow_source_mutation"] = True

        with pytest.raises(ValueError, match="allow_source_mutation MUST be False"):
            CycleProfile(**data)

    def test_minimal_valid_profile(self):
        """Profile with minimal fields should use defaults."""
        data = {
            "profile_id": "minimal",
            "profile_name": "Minimal Profile",
            "mode": "learning",
            "max_improvements": 0,
        }

        profile = CycleProfile(**data)

        assert profile.profile_id == "minimal"
        assert profile.mode == "learning"
        assert profile.cycle_source_config.require_frozen is True
        assert profile.guardrails.allow_source_mutation is False


class TestProfileHash:
    """Test profile hash computation."""

    def test_hash_is_deterministic(self):
        """Same content should produce same hash."""
        content = VALID_PROFILE_YAML.strip()
        hash1 = compute_profile_hash(content)
        hash2 = compute_profile_hash(content)

        assert hash1 == hash2
        assert len(hash1) == 16

    def test_different_content_different_hash(self):
        """Different content should produce different hash."""
        content1 = VALID_PROFILE_YAML.strip()
        content2 = content1.replace("test_learning", "test_other")

        hash1 = compute_profile_hash(content1)
        hash2 = compute_profile_hash(content2)

        assert hash1 != hash2

    def test_whitespace_normalization(self):
        """Leading/trailing whitespace should be normalized."""
        content = VALID_PROFILE_YAML
        content_with_whitespace = "  \n\n" + content + "\n\n  "

        hash1 = compute_profile_hash(content)
        hash2 = compute_profile_hash(content_with_whitespace)

        assert hash1 == hash2


class TestFrozenProfile:
    """Test FrozenCycleProfile creation and immutability."""

    def test_freeze_profile_creates_immutable(self):
        """freeze_profile should create an immutable dataclass."""
        import yaml

        data = yaml.safe_load(VALID_PROFILE_YAML)
        profile = CycleProfile(**data)
        content_hash = compute_profile_hash(VALID_PROFILE_YAML)

        frozen = freeze_profile(profile, content_hash, "/test/path.yaml")

        assert frozen.profile_id == "test_learning"
        assert frozen.content_hash == content_hash
        assert frozen.source_path == "/test/path.yaml"
        assert frozen.loaded_at is not None

        with pytest.raises(Exception):
            frozen.profile_id = "changed"

    def test_frozen_profile_to_dict(self):
        """FrozenCycleProfile.to_dict should return serializable dict."""
        import yaml

        data = yaml.safe_load(VALID_PROFILE_YAML)
        profile = CycleProfile(**data)
        content_hash = compute_profile_hash(VALID_PROFILE_YAML)

        frozen = freeze_profile(profile, content_hash, "/test/path.yaml")
        as_dict = frozen.to_dict()

        assert as_dict["profile_id"] == "test_learning"
        assert as_dict["content_hash"] == content_hash
        assert isinstance(as_dict["allowed_tiers"], list)

    def test_get_audit_summary(self):
        """get_audit_summary should return audit-relevant fields."""
        import yaml

        data = yaml.safe_load(VALID_PROFILE_YAML)
        profile = CycleProfile(**data)
        content_hash = compute_profile_hash(VALID_PROFILE_YAML)

        frozen = freeze_profile(profile, content_hash, "/test/path.yaml")
        summary = frozen.get_audit_summary()

        assert "profile_id" in summary
        assert "content_hash" in summary
        assert "source_path" in summary
        assert "loaded_at" in summary
        assert "mode" not in summary


class TestProfileLoader:
    """Test CycleProfileLoader functionality."""

    def test_list_profiles_empty_dir(self):
        """list_profiles should return empty list if no profiles."""
        with tempfile.TemporaryDirectory() as tmpdir:
            os.makedirs(os.path.join(tmpdir, ".odibi", "cycle_profiles"))
            loader = CycleProfileLoader(tmpdir)

            profiles = loader.list_profiles()

            assert profiles == []

    def test_list_profiles_finds_yaml_files(self):
        """list_profiles should find .yaml and .yml files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            profiles_dir = os.path.join(tmpdir, ".odibi", "cycle_profiles")
            os.makedirs(profiles_dir)

            Path(profiles_dir, "profile_a.yaml").write_text("dummy: true")
            Path(profiles_dir, "profile_b.yml").write_text("dummy: true")
            Path(profiles_dir, "not_a_profile.txt").write_text("dummy: true")

            loader = CycleProfileLoader(tmpdir)
            profiles = loader.list_profiles()

            assert "profile_a" in profiles
            assert "profile_b" in profiles
            assert "not_a_profile" not in profiles

    def test_load_profile_success(self):
        """load_profile should load and freeze a valid profile."""
        with tempfile.TemporaryDirectory() as tmpdir:
            profiles_dir = os.path.join(tmpdir, ".odibi", "cycle_profiles")
            os.makedirs(profiles_dir)

            profile_path = Path(profiles_dir, "test_learning.yaml")
            profile_path.write_text(VALID_PROFILE_YAML)

            loader = CycleProfileLoader(tmpdir)
            frozen = loader.load_profile("test_learning")

            assert frozen.profile_id == "test_learning"
            assert frozen.mode == "learning"
            assert len(frozen.content_hash) == 16

    def test_load_profile_not_found(self):
        """load_profile should raise error if profile not found."""
        with tempfile.TemporaryDirectory() as tmpdir:
            os.makedirs(os.path.join(tmpdir, ".odibi", "cycle_profiles"))
            loader = CycleProfileLoader(tmpdir)

            with pytest.raises(CycleProfileError, match="Profile not found"):
                loader.load_profile("nonexistent")

    def test_load_profile_invalid_yaml(self):
        """load_profile should raise error on invalid YAML."""
        with tempfile.TemporaryDirectory() as tmpdir:
            profiles_dir = os.path.join(tmpdir, ".odibi", "cycle_profiles")
            os.makedirs(profiles_dir)

            profile_path = Path(profiles_dir, "bad_yaml.yaml")
            profile_path.write_text("{ invalid yaml [")

            loader = CycleProfileLoader(tmpdir)

            with pytest.raises(CycleProfileError, match="Invalid YAML"):
                loader.load_profile("bad_yaml")

    def test_load_profile_validation_failure(self):
        """load_profile should raise error on validation failure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            profiles_dir = os.path.join(tmpdir, ".odibi", "cycle_profiles")
            os.makedirs(profiles_dir)

            invalid_profile = VALID_PROFILE_YAML.replace(
                "max_improvements: 0", "max_improvements: 5"
            )
            profile_path = Path(profiles_dir, "invalid.yaml")
            profile_path.write_text(invalid_profile)

            loader = CycleProfileLoader(tmpdir)

            with pytest.raises(CycleProfileError, match="validation failed"):
                loader.load_profile("invalid")

    def test_get_profile_summary(self):
        """get_profile_summary should return summary without full load."""
        with tempfile.TemporaryDirectory() as tmpdir:
            profiles_dir = os.path.join(tmpdir, ".odibi", "cycle_profiles")
            os.makedirs(profiles_dir)

            profile_path = Path(profiles_dir, "test_learning.yaml")
            profile_path.write_text(VALID_PROFILE_YAML)

            loader = CycleProfileLoader(tmpdir)
            summary = loader.get_profile_summary("test_learning")

            assert summary["profile_id"] == "test_learning"
            assert summary["profile_name"] == "Test Learning Profile"
            assert len(summary["content_hash"]) == 16
            assert "source_path" in summary


class TestLoadProfileYaml:
    """Test low-level YAML loading."""

    def test_load_nonexistent_file(self):
        """load_profile_yaml should raise error for missing file."""
        with pytest.raises(CycleProfileError, match="not found"):
            load_profile_yaml("/nonexistent/path.yaml")

    def test_load_valid_yaml(self):
        """load_profile_yaml should return parsed dict and raw content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            profile_path = os.path.join(tmpdir, "test.yaml")
            with open(profile_path, "w", encoding="utf-8") as f:
                f.write(VALID_PROFILE_YAML)

            parsed, raw = load_profile_yaml(profile_path)

            assert parsed["profile_id"] == "test_learning"
            assert "profile_name" in raw


class TestValidateProfile:
    """Test profile validation function."""

    def test_validate_valid_profile(self):
        """validate_profile should return CycleProfile for valid data."""
        import yaml

        data = yaml.safe_load(VALID_PROFILE_YAML)
        profile = validate_profile(data, "/test/path.yaml")

        assert isinstance(profile, CycleProfile)
        assert profile.profile_id == "test_learning"

    def test_validate_invalid_profile(self):
        """validate_profile should raise CycleProfileError with details."""
        data = {
            "profile_id": "test",
            "profile_name": "Test",
            "mode": "improvement",
            "max_improvements": 5,
        }

        with pytest.raises(CycleProfileError) as exc_info:
            validate_profile(data, "/test/path.yaml")

        assert len(exc_info.value.errors) > 0
