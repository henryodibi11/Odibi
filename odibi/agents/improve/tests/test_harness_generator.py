"""Tests for HarnessGenerator."""

import shutil
import tempfile
from pathlib import Path

import pytest
import yaml

from odibi.agents.improve.harness_generator import (
    KNOWN_PATTERNS,
    KNOWN_TRANSFORMERS,
    EDGE_CASES,
    CoverageCategory,
    CoverageGap,
    HarnessGenerator,
)


@pytest.fixture
def temp_harness_dir():
    """Create a temporary harness directory."""
    temp = tempfile.mkdtemp(prefix="harness_test_")
    yield Path(temp)
    shutil.rmtree(temp, ignore_errors=True)


@pytest.fixture
def mock_harness_dir(temp_harness_dir: Path):
    """Create a mock harness directory with some configs."""
    # Create a config that covers join transformer
    join_config = """
project: test_join
pipelines:
  - pipeline: test
    nodes:
      - name: test_join
        transformer: join
        params:
          on: id
"""
    (temp_harness_dir / "join_test.yaml").write_text(join_config)

    # Create a config that covers scd2 pattern
    scd2_config = """
project: test_scd2
pipelines:
  - pipeline: test
    nodes:
      - name: apply_scd2
        pattern: scd2
"""
    (temp_harness_dir / "scd2_test.yaml").write_text(scd2_config)

    return temp_harness_dir


class TestKnownItems:
    """Tests for known transformers, patterns, and edge cases."""

    def test_known_transformers_not_empty(self):
        assert len(KNOWN_TRANSFORMERS) > 20

    def test_known_transformers_has_common_ones(self):
        common = ["filter_rows", "derive_columns", "join", "aggregate", "union"]
        for t in common:
            assert t in KNOWN_TRANSFORMERS

    def test_known_patterns_not_empty(self):
        assert len(KNOWN_PATTERNS) > 3

    def test_known_patterns_has_common_ones(self):
        common = ["scd2", "dimension", "fact"]
        for p in common:
            assert p in KNOWN_PATTERNS

    def test_edge_cases_not_empty(self):
        assert len(EDGE_CASES) > 5


class TestCoverageGap:
    """Tests for CoverageGap dataclass."""

    def test_create_coverage_gap(self):
        gap = CoverageGap(
            category=CoverageCategory.TRANSFORMER,
            name="test_transformer",
            description="Test description",
            priority="high",
        )
        assert gap.category == CoverageCategory.TRANSFORMER
        assert gap.name == "test_transformer"
        assert gap.priority == "high"

    def test_coverage_gap_defaults(self):
        gap = CoverageGap(
            category=CoverageCategory.EDGE_CASE,
            name="test",
            description="Test",
        )
        assert gap.priority == "medium"
        assert gap.existing_coverage == []


class TestHarnessGenerator:
    """Tests for HarnessGenerator."""

    def test_init_without_harness_dir(self):
        generator = HarnessGenerator()
        assert generator._harness_dir is None

    def test_init_with_harness_dir(self, temp_harness_dir: Path):
        generator = HarnessGenerator(temp_harness_dir)
        assert generator._harness_dir == temp_harness_dir

    def test_list_coverage_gaps_empty_dir(self, temp_harness_dir: Path):
        generator = HarnessGenerator(temp_harness_dir)
        gaps = generator.list_coverage_gaps()

        # Should find gaps for all transformers, patterns, and edge cases
        assert len(gaps) > 0
        assert any(g.category == CoverageCategory.TRANSFORMER for g in gaps)
        assert any(g.category == CoverageCategory.PATTERN for g in gaps)
        assert any(g.category == CoverageCategory.EDGE_CASE for g in gaps)

    def test_list_coverage_gaps_with_coverage(self, mock_harness_dir: Path):
        generator = HarnessGenerator(mock_harness_dir)
        gaps = generator.list_coverage_gaps()

        # Should NOT find gaps for join (covered) and scd2 (covered)
        transformer_gaps = [g.name for g in gaps if g.category == CoverageCategory.TRANSFORMER]
        pattern_gaps = [g.name for g in gaps if g.category == CoverageCategory.PATTERN]

        assert "join" not in transformer_gaps
        assert "scd2" not in pattern_gaps

    def test_generate_transformer_test(self):
        generator = HarnessGenerator()
        yaml_str = generator.generate_transformer_test("filter_rows")

        # Parse the YAML
        config = yaml.safe_load(yaml_str)

        assert "project" in config
        assert "learning_harness_transform_filter_rows" in config["project"]
        assert "pipelines" in config
        assert "connections" in config
        assert "story" in config
        assert "system" in config

        # Check pipeline structure
        pipeline = config["pipelines"][0]
        assert "nodes" in pipeline

        # Check for transformer node
        transformer_nodes = [n for n in pipeline["nodes"] if n.get("transformer") == "filter_rows"]
        assert len(transformer_nodes) == 1

    def test_generate_pattern_test(self):
        generator = HarnessGenerator()
        yaml_str = generator.generate_pattern_test("scd2")

        config = yaml.safe_load(yaml_str)

        assert "project" in config
        assert "scd2" in config["project"]

        pipeline = config["pipelines"][0]
        assert "nodes" in pipeline
        assert len(pipeline["nodes"]) > 0

    def test_generate_edge_case_test(self):
        generator = HarnessGenerator()
        yaml_str = generator.generate_edge_case_test("empty_data")

        config = yaml.safe_load(yaml_str)

        assert "project" in config
        assert "empty_data" in config["project"]

        pipeline = config["pipelines"][0]
        assert "nodes" in pipeline
        assert len(pipeline["nodes"]) > 0

    def test_generated_config_has_required_sections(self):
        generator = HarnessGenerator()

        for transformer in ["filter_rows", "join", "aggregate"]:
            yaml_str = generator.generate_transformer_test(transformer)
            config = yaml.safe_load(yaml_str)

            assert "project" in config
            assert "connections" in config
            assert "bound_sources" in config["connections"]
            assert "artifacts" in config["connections"]
            assert "pipelines" in config
            assert "story" in config
            assert "system" in config

    def test_generated_config_uses_env_vars(self):
        generator = HarnessGenerator()
        yaml_str = generator.generate_transformer_test("filter_rows")

        assert "${BOUND_SOURCE_ROOT}" in yaml_str
        assert "${ARTIFACTS_ROOT}" in yaml_str

    def test_get_coverage_report(self, temp_harness_dir: Path):
        generator = HarnessGenerator(temp_harness_dir)
        report = generator.get_coverage_report()

        assert "# Learning Harness Coverage Report" in report
        assert "## Coverage Gaps" in report
        assert "## Summary" in report
        assert "Transformer" in report
        assert "Pattern" in report

    def test_generate_all_pattern_tests(self):
        generator = HarnessGenerator()

        for pattern in KNOWN_PATTERNS:
            yaml_str = generator.generate_pattern_test(pattern)
            config = yaml.safe_load(yaml_str)
            assert config is not None
            assert "project" in config

    def test_generate_all_edge_case_tests(self):
        generator = HarnessGenerator()

        for edge_name, _ in EDGE_CASES:
            yaml_str = generator.generate_edge_case_test(edge_name)
            config = yaml.safe_load(yaml_str)
            assert config is not None
            assert "project" in config


class TestHarnessConfigFiles:
    """Tests for the actual harness config files created."""

    @pytest.fixture
    def harness_dir(self):
        """Get the actual harness directory."""
        return Path("d:/odibi/.odibi/learning_harness")

    def test_harness_dir_exists(self, harness_dir: Path):
        if not harness_dir.exists():
            pytest.skip("Harness directory not found")
        assert harness_dir.is_dir()

    def test_transformer_configs_exist(self, harness_dir: Path):
        if not harness_dir.exists():
            pytest.skip("Harness directory not found")

        expected = [
            "transform_filter.odibi.yaml",
            "transform_derive.odibi.yaml",
            "transform_aggregate.odibi.yaml",
            "transform_dedupe.odibi.yaml",
            "transform_window.odibi.yaml",
            "transform_cast.odibi.yaml",
        ]
        for name in expected:
            assert (harness_dir / name).exists(), f"Missing: {name}"

    def test_pattern_configs_exist(self, harness_dir: Path):
        if not harness_dir.exists():
            pytest.skip("Harness directory not found")

        expected = [
            "pattern_scd2.odibi.yaml",
            "pattern_dimension.odibi.yaml",
            "pattern_fact.odibi.yaml",
            "pattern_date_dim.odibi.yaml",
        ]
        for name in expected:
            assert (harness_dir / name).exists(), f"Missing: {name}"

    def test_edge_case_configs_exist(self, harness_dir: Path):
        if not harness_dir.exists():
            pytest.skip("Harness directory not found")

        expected = [
            "edge_empty_data.odibi.yaml",
            "edge_all_nulls.odibi.yaml",
            "edge_type_mismatch.odibi.yaml",
            "edge_unicode.odibi.yaml",
            "edge_large_schema.odibi.yaml",
        ]
        for name in expected:
            assert (harness_dir / name).exists(), f"Missing: {name}"

    def test_configs_are_valid_yaml(self, harness_dir: Path):
        if not harness_dir.exists():
            pytest.skip("Harness directory not found")

        for config_file in harness_dir.glob("*.yaml"):
            try:
                content = config_file.read_text(encoding="utf-8")
                config = yaml.safe_load(content)
                assert config is not None, f"Empty config: {config_file.name}"
                assert "project" in config, f"Missing project in: {config_file.name}"
            except yaml.YAMLError as e:
                pytest.fail(f"Invalid YAML in {config_file.name}: {e}")

    def test_configs_have_required_structure(self, harness_dir: Path):
        if not harness_dir.exists():
            pytest.skip("Harness directory not found")

        for config_file in harness_dir.glob("*.yaml"):
            if config_file.name.startswith("_"):
                continue

            content = config_file.read_text(encoding="utf-8")
            config = yaml.safe_load(content)

            assert "project" in config, f"Missing project in: {config_file.name}"
            assert "connections" in config, f"Missing connections in: {config_file.name}"
            assert "pipelines" in config, f"Missing pipelines in: {config_file.name}"
