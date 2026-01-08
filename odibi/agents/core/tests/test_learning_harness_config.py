"""Tests for learning harness config validation.

Phase 9.E: Ensures learning harness YAML files are valid ProjectConfig files.

These tests validate:
1. Harness configs pass ProjectConfig schema validation
2. Source binding paths use ${BOUND_SOURCE_ROOT} placeholder
3. Configs can be loaded with environment variable substitution
"""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from odibi.config import ProjectConfig
from odibi.utils import load_yaml_with_env


# ============================================
# Fixtures
# ============================================


@pytest.fixture
def harness_dir():
    """Get the learning harness directory path."""
    # Navigate from test file to harness directory
    agents_tests = Path(__file__).parent
    agents_core = agents_tests.parent
    agents = agents_core.parent
    odibi_root = agents.parent
    harness_path = odibi_root / ".odibi" / "learning_harness"

    if not harness_path.exists():
        pytest.skip("Learning harness directory not found")

    return harness_path


@pytest.fixture
def harness_yaml_files(harness_dir):
    """List all harness YAML files."""
    return list(harness_dir.glob("*.odibi.yaml"))


@pytest.fixture
def temp_env_substituted_config(harness_dir):
    """Create temp directory with substituted configs for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Use forward slashes for YAML compatibility (works on Windows too)
        source_root = os.path.join(tmpdir, "source_cache").replace("\\", "/")
        artifacts_root = os.path.join(tmpdir, "artifacts").replace("\\", "/")

        # Set up environment variables
        os.environ["BOUND_SOURCE_ROOT"] = source_root
        os.environ["ARTIFACTS_ROOT"] = artifacts_root

        # Create the directories (use original paths for os.makedirs)
        os.makedirs(source_root.replace("/", os.sep), exist_ok=True)
        os.makedirs(artifacts_root.replace("/", os.sep), exist_ok=True)

        yield {
            "tmpdir": tmpdir,
            "harness_dir": harness_dir,
            "source_root": source_root,
            "artifacts_root": artifacts_root,
        }

        # Cleanup
        os.environ.pop("BOUND_SOURCE_ROOT", None)
        os.environ.pop("ARTIFACTS_ROOT", None)


# ============================================
# Schema Validation Tests
# ============================================


class TestHarnessConfigSchema:
    """Tests that harness configs are valid ProjectConfig files."""

    def test_harness_directory_exists(self, harness_dir):
        """Learning harness directory should exist."""
        assert harness_dir.exists()
        assert harness_dir.is_dir()

    def test_harness_yaml_files_exist(self, harness_yaml_files):
        """At least one harness YAML file should exist."""
        assert len(harness_yaml_files) >= 1

    def test_expected_harness_files_present(self, harness_dir):
        """Expected harness pipeline files should be present."""
        expected_files = [
            "scale_join.odibi.yaml",
            "skew_test.odibi.yaml",
            "schema_drift.odibi.yaml",
        ]
        for filename in expected_files:
            filepath = harness_dir / filename
            assert filepath.exists(), f"Expected harness file missing: {filename}"

    def test_harness_configs_are_valid_yaml(self, harness_yaml_files):
        """All harness configs should be valid YAML."""
        for yaml_file in harness_yaml_files:
            with open(yaml_file, "r") as f:
                try:
                    data = yaml.safe_load(f)
                    assert isinstance(data, dict), f"{yaml_file.name} should be a dict"
                except yaml.YAMLError as e:
                    pytest.fail(f"Invalid YAML in {yaml_file.name}: {e}")

    def test_harness_configs_have_required_fields(self, harness_yaml_files):
        """All harness configs should have ProjectConfig required fields."""
        required_fields = ["project", "connections", "pipelines", "story", "system"]

        for yaml_file in harness_yaml_files:
            with open(yaml_file, "r") as f:
                data = yaml.safe_load(f)

            for field in required_fields:
                assert field in data, f"{yaml_file.name} missing required field: {field}"

    def test_harness_configs_pass_pydantic_validation(self, temp_env_substituted_config):
        """Harness configs should pass full ProjectConfig validation."""
        harness_dir = temp_env_substituted_config["harness_dir"]

        for yaml_file in harness_dir.glob("*.odibi.yaml"):
            try:
                config_dict = load_yaml_with_env(str(yaml_file))
                config = ProjectConfig(**config_dict)

                # Verify key attributes
                assert config.project.startswith("learning_harness_")
                assert len(config.connections) >= 2  # bound_sources + artifacts
                assert len(config.pipelines) >= 1
                assert config.story is not None
                assert config.system is not None

            except Exception as e:
                pytest.fail(f"ProjectConfig validation failed for {yaml_file.name}: {e}")


# ============================================
# Source Binding Path Tests
# ============================================


class TestSourceBindingPaths:
    """Tests that harness configs use proper source binding paths."""

    def test_configs_use_bound_source_root(self, harness_yaml_files):
        """Configs should reference ${BOUND_SOURCE_ROOT} for bound sources."""
        for yaml_file in harness_yaml_files:
            content = yaml_file.read_text()
            assert "${BOUND_SOURCE_ROOT}" in content, (
                f"{yaml_file.name} should use ${{BOUND_SOURCE_ROOT}} placeholder"
            )

    def test_configs_use_artifacts_root(self, harness_yaml_files):
        """Configs should reference ${ARTIFACTS_ROOT} for outputs."""
        for yaml_file in harness_yaml_files:
            content = yaml_file.read_text()
            assert "${ARTIFACTS_ROOT}" in content, (
                f"{yaml_file.name} should use ${{ARTIFACTS_ROOT}} placeholder"
            )

    def test_no_hardcoded_absolute_paths(self, harness_yaml_files):
        """Configs should not have hardcoded absolute paths."""
        # Common patterns for hardcoded paths
        bad_patterns = [
            "/home/",
            "/Users/",
            "C:\\",
            "D:\\",
            "/mnt/",
        ]

        for yaml_file in harness_yaml_files:
            content = yaml_file.read_text()
            for pattern in bad_patterns:
                assert pattern not in content, (
                    f"{yaml_file.name} contains hardcoded path: {pattern}"
                )


# ============================================
# Pipeline Structure Tests
# ============================================


class TestPipelineStructure:
    """Tests for pipeline structure in harness configs."""

    def test_pipelines_have_nodes(self, harness_yaml_files):
        """Each pipeline should have at least one node."""
        for yaml_file in harness_yaml_files:
            with open(yaml_file, "r") as f:
                data = yaml.safe_load(f)

            for pipeline in data.get("pipelines", []):
                assert "nodes" in pipeline, f"{yaml_file.name} pipeline missing nodes"
                assert len(pipeline["nodes"]) >= 1, f"{yaml_file.name} pipeline has no nodes"

    def test_nodes_have_required_operations(self, harness_yaml_files):
        """Each node should have at least one operation (read, transform, write)."""
        for yaml_file in harness_yaml_files:
            with open(yaml_file, "r") as f:
                data = yaml.safe_load(f)

            for pipeline in data.get("pipelines", []):
                for node in pipeline.get("nodes", []):
                    has_operation = any(
                        [
                            node.get("read"),
                            node.get("transform"),
                            node.get("write"),
                            node.get("transformer"),
                        ]
                    )
                    assert has_operation, (
                        f"{yaml_file.name} node '{node.get('name')}' has no operations"
                    )


# ============================================
# Single Stressor Tests
# ============================================


class TestSingleStressor:
    """Tests that each harness pipeline focuses on a single stressor."""

    def test_scale_join_focuses_on_joins(self, harness_dir):
        """scale_join pipeline should focus on join operations."""
        yaml_file = harness_dir / "scale_join.odibi.yaml"
        if not yaml_file.exists():
            pytest.skip("scale_join.odibi.yaml not found")

        # Should have join-related transformations
        content = yaml_file.read_text().lower()
        assert "join" in content, "scale_join should contain join operations"

    def test_skew_test_focuses_on_nulls(self, harness_dir):
        """skew_test pipeline should focus on null/edge case handling."""
        yaml_file = harness_dir / "skew_test.odibi.yaml"
        if not yaml_file.exists():
            pytest.skip("skew_test.odibi.yaml not found")

        # Should have null-handling operations
        content = yaml_file.read_text().lower()
        assert "null" in content or "edge" in content, "skew_test should handle nulls or edge cases"

    def test_schema_drift_focuses_on_coercion(self, harness_dir):
        """schema_drift pipeline should focus on type coercion."""
        yaml_file = harness_dir / "schema_drift.odibi.yaml"
        if not yaml_file.exists():
            pytest.skip("schema_drift.odibi.yaml not found")

        # Should have type/schema operations
        content = yaml_file.read_text().lower()
        assert "cast" in content or "union" in content or "schema" in content, (
            "schema_drift should handle type coercion or schema operations"
        )


# ============================================
# Determinism Tests
# ============================================


class TestDeterminism:
    """Tests for deterministic execution properties."""

    def test_no_random_operations(self, harness_yaml_files):
        """Configs should not contain random/non-deterministic operations."""
        non_deterministic = [
            "random",
            "rand()",
            "uuid()",
            "now()",
            "current_timestamp",
        ]

        for yaml_file in harness_yaml_files:
            content = yaml_file.read_text().lower()
            for pattern in non_deterministic:
                # Allow in comments
                lines = [line for line in content.split("\n") if not line.strip().startswith("#")]
                code_content = "\n".join(lines)
                if pattern in code_content:
                    # Only fail if not in a comment context
                    assert False, f"{yaml_file.name} contains non-deterministic: {pattern}"

    def test_write_modes_are_overwrite(self, harness_yaml_files):
        """Write operations should use overwrite mode for determinism."""
        for yaml_file in harness_yaml_files:
            with open(yaml_file, "r") as f:
                data = yaml.safe_load(f)

            for pipeline in data.get("pipelines", []):
                for node in pipeline.get("nodes", []):
                    write = node.get("write")
                    if write:
                        mode = write.get("mode", "overwrite")
                        assert mode == "overwrite", (
                            f"{yaml_file.name} node '{node.get('name')}' "
                            f"should use overwrite mode, got: {mode}"
                        )
