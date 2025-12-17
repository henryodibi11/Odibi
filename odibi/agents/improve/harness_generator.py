"""HarnessGenerator: Generate learning harness configs for testing odibi.

Provides tools to:
- Identify coverage gaps in existing harness configs
- Generate new harness configs for transformers, patterns, and edge cases
- Validate generated configs against odibi schema

IMPORTANT: Generated configs are TEMPLATES that should be reviewed
and customized before use. They follow the harness invariants.
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)


class CoverageCategory(str, Enum):
    """Categories of test coverage."""

    TRANSFORMER = "transformer"
    PATTERN = "pattern"
    EDGE_CASE = "edge_case"
    ENGINE = "engine"
    CONNECTION = "connection"


@dataclass
class CoverageGap:
    """Represents a gap in test coverage."""

    category: CoverageCategory
    name: str
    description: str
    priority: str = "medium"  # high, medium, low
    existing_coverage: list[str] = field(default_factory=list)


# Known transformers in odibi
KNOWN_TRANSFORMERS = [
    # sql_core.py
    "filter_rows",
    "derive_columns",
    "cast_columns",
    "clean_text",
    "extract_date_parts",
    "normalize_schema",
    "sort",
    "limit",
    "sample",
    "distinct",
    "fill_nulls",
    "split_part",
    "date_add",
    "date_trunc",
    # relational.py
    "join",
    "union",
    "pivot",
    "unpivot",
    "aggregate",
    # advanced.py
    "deduplicate",
    "explode_list_column",
    "dict_based_mapping",
    "regex_replace",
    "unpack_struct",
    "hash_columns",
    "generate_surrogate_key",
    "parse_json",
    "validate_and_flag",
    "window_calculation",
    # scd.py
    "scd2",
    # merge_transformer.py
    "merge",
    # delete_detection.py
    "detect_deletes",
    # validation.py
    "cross_check",
]

# Known patterns in odibi
KNOWN_PATTERNS = [
    "scd2",
    "merge",
    "fact",
    "dimension",
    "date_dimension",
    "aggregation",
]

# Edge case categories
EDGE_CASES = [
    ("empty_data", "Empty DataFrame handling"),
    ("all_nulls", "Columns with all null values"),
    ("type_mismatch", "Type coercion edge cases"),
    ("unicode", "Unicode and special character handling"),
    ("large_schema", "Wide tables with 100+ columns"),
    ("duplicate_cols", "Duplicate column names"),
    ("special_chars", "Column names with special characters"),
    ("nested_data", "Nested structs and arrays"),
    ("date_edge", "Date boundary cases (epoch, future, null)"),
    ("numeric_edge", "Numeric edge cases (inf, nan, overflow)"),
]


class HarnessGenerator:
    """Generate learning harness configs for untested scenarios.

    This class helps identify gaps in test coverage and generates
    template harness configs that can be customized for specific tests.

    Attributes:
        harness_dir: Path to the learning harness directory.
    """

    def __init__(self, harness_dir: Optional[Path] = None) -> None:
        """Initialize the generator.

        Args:
            harness_dir: Path to harness directory (for coverage analysis).
        """
        self._harness_dir = harness_dir

    def list_coverage_gaps(self) -> list[CoverageGap]:
        """Identify what's not covered by existing harness configs.

        Returns:
            List of CoverageGap objects describing missing coverage.
        """
        gaps = []

        # Get existing coverage
        existing = self._analyze_existing_coverage()

        # Check transformers
        for transformer in KNOWN_TRANSFORMERS:
            if transformer not in existing.get("transformers", set()):
                gaps.append(
                    CoverageGap(
                        category=CoverageCategory.TRANSFORMER,
                        name=transformer,
                        description=f"Transformer '{transformer}' has no dedicated harness test",
                        priority=(
                            "high"
                            if transformer in ["join", "aggregate", "filter_rows", "derive_columns"]
                            else "medium"
                        ),
                    )
                )

        # Check patterns
        for pattern in KNOWN_PATTERNS:
            if pattern not in existing.get("patterns", set()):
                gaps.append(
                    CoverageGap(
                        category=CoverageCategory.PATTERN,
                        name=pattern,
                        description=f"Pattern '{pattern}' has no dedicated harness test",
                        priority="high",
                    )
                )

        # Check edge cases
        for edge_name, edge_desc in EDGE_CASES:
            if edge_name not in existing.get("edge_cases", set()):
                gaps.append(
                    CoverageGap(
                        category=CoverageCategory.EDGE_CASE,
                        name=edge_name,
                        description=edge_desc,
                        priority=(
                            "high"
                            if edge_name in ["empty_data", "all_nulls", "type_mismatch"]
                            else "medium"
                        ),
                    )
                )

        return gaps

    def _analyze_existing_coverage(self) -> dict[str, set[str]]:
        """Analyze existing harness configs for coverage.

        Returns:
            Dict mapping category to set of covered items.
        """
        coverage: dict[str, set[str]] = {
            "transformers": set(),
            "patterns": set(),
            "edge_cases": set(),
        }

        if not self._harness_dir or not self._harness_dir.exists():
            return coverage

        for config_file in self._harness_dir.glob("*.yaml"):
            try:
                content = config_file.read_text(encoding="utf-8")

                # Simple heuristic: look for transformer references
                for transformer in KNOWN_TRANSFORMERS:
                    if (
                        f"transformer: {transformer}" in content
                        or f"transformer:{transformer}" in content
                    ):
                        coverage["transformers"].add(transformer)

                # Look for pattern references
                for pattern in KNOWN_PATTERNS:
                    if f"pattern: {pattern}" in content or f"pattern:{pattern}" in content:
                        coverage["patterns"].add(pattern)

                # Look for edge case indicators in filename or content
                filename = config_file.stem.lower()
                for edge_name, _ in EDGE_CASES:
                    if edge_name in filename or edge_name.replace("_", "") in content.lower():
                        coverage["edge_cases"].add(edge_name)

            except Exception as e:
                logger.warning(f"Failed to analyze {config_file}: {e}")

        return coverage

    def generate_transformer_test(self, transformer: str) -> str:
        """Generate YAML config to test a specific transformer.

        Args:
            transformer: Name of the transformer to test.

        Returns:
            YAML string for the harness config.
        """
        config = self._get_base_config(f"transform_{transformer}")
        config["pipelines"][0]["description"] = f"Test {transformer} transformer edge cases"

        # Add transformer-specific nodes
        nodes = config["pipelines"][0]["nodes"]

        # Load source node
        nodes.append(self._make_load_node("load_test_data"))

        # Transformer node with common params
        transformer_node = {
            "name": f"test_{transformer}",
            "depends_on": ["load_test_data"],
            "transformer": transformer,
            "params": self._get_transformer_params(transformer),
            "write": {
                "connection": "artifacts",
                "format": "parquet",
                "path": f"learning_harness/output/{transformer}_result",
                "mode": "overwrite",
            },
        }
        nodes.append(transformer_node)

        return yaml.dump(config, default_flow_style=False, sort_keys=False)

    def generate_pattern_test(self, pattern: str) -> str:
        """Generate YAML config to test a specific pattern.

        Args:
            pattern: Name of the pattern to test.

        Returns:
            YAML string for the harness config.
        """
        config = self._get_base_config(f"pattern_{pattern}")
        config["pipelines"][0]["description"] = f"Test {pattern} pattern edge cases"

        nodes = config["pipelines"][0]["nodes"]

        # Pattern-specific setup
        if pattern == "scd2":
            nodes.extend(self._make_scd2_test_nodes())
        elif pattern == "dimension":
            nodes.extend(self._make_dimension_test_nodes())
        elif pattern == "fact":
            nodes.extend(self._make_fact_test_nodes())
        elif pattern == "date_dimension":
            nodes.extend(self._make_date_dimension_test_nodes())
        elif pattern == "aggregation":
            nodes.extend(self._make_aggregation_test_nodes())
        elif pattern == "merge":
            nodes.extend(self._make_merge_test_nodes())
        else:
            # Generic pattern test
            nodes.append(self._make_load_node("load_source"))
            nodes.append(
                {
                    "name": f"apply_{pattern}",
                    "depends_on": ["load_source"],
                    "pattern": pattern,
                    "params": {},
                    "write": {
                        "connection": "artifacts",
                        "format": "parquet",
                        "path": f"learning_harness/output/{pattern}_result",
                        "mode": "overwrite",
                    },
                }
            )

        return yaml.dump(config, default_flow_style=False, sort_keys=False)

    def generate_edge_case_test(self, edge_case: str) -> str:
        """Generate YAML config for edge case testing.

        Args:
            edge_case: Name of the edge case to test.

        Returns:
            YAML string for the harness config.
        """
        config = self._get_base_config(f"edge_{edge_case}")
        config["pipelines"][0]["description"] = f"Test {edge_case.replace('_', ' ')} handling"

        nodes = config["pipelines"][0]["nodes"]

        if edge_case == "empty_data":
            nodes.extend(self._make_empty_data_test_nodes())
        elif edge_case == "all_nulls":
            nodes.extend(self._make_all_nulls_test_nodes())
        elif edge_case == "type_mismatch":
            nodes.extend(self._make_type_mismatch_test_nodes())
        elif edge_case == "unicode":
            nodes.extend(self._make_unicode_test_nodes())
        elif edge_case == "large_schema":
            nodes.extend(self._make_large_schema_test_nodes())
        else:
            # Generic edge case test
            nodes.append(self._make_load_node("load_edge_data", "edge_cases/mixed/data.csv"))

        return yaml.dump(config, default_flow_style=False, sort_keys=False)

    def _get_base_config(self, project_name: str) -> dict:
        """Get base config structure."""
        return {
            "project": f"learning_harness_{project_name}",
            "connections": {
                "bound_sources": {
                    "type": "local",
                    "base_path": "${BOUND_SOURCE_ROOT}",
                },
                "artifacts": {
                    "type": "local",
                    "base_path": "${ARTIFACTS_ROOT}",
                },
            },
            "story": {
                "connection": "artifacts",
                "path": f"stories/{project_name}",
            },
            "system": {
                "connection": "artifacts",
                "path": "_system",
            },
            "pipelines": [
                {
                    "pipeline": f"{project_name}_test",
                    "description": f"Test {project_name}",
                    "nodes": [],
                }
            ],
        }

    def _make_load_node(
        self,
        name: str,
        path: str = "edge_cases/mixed/data.csv",
        format: str = "csv",
    ) -> dict:
        """Create a standard load node."""
        return {
            "name": name,
            "read": {
                "connection": "bound_sources",
                "format": format,
                "path": path,
            },
            "write": {
                "connection": "artifacts",
                "format": "parquet",
                "path": f"learning_harness/staging/{name}",
                "mode": "overwrite",
            },
        }

    def _get_transformer_params(self, transformer: str) -> dict:
        """Get default params for a transformer."""
        params_map = {
            "filter_rows": {"condition": "id IS NOT NULL"},
            "derive_columns": {"columns": {"new_col": "COALESCE(string_col, 'default')"}},
            "cast_columns": {"columns": {"int_col": "string"}},
            "fill_nulls": {"values": {"string_col": "UNKNOWN", "int_col": 0}},
            "sort": {"by": ["id"], "ascending": True},
            "limit": {"n": 100},
            "sample": {"fraction": 0.1},
            "distinct": {"columns": ["id"]},
            "join": {"right_dataset": "load_test_data", "on": "id", "how": "inner"},
            "union": {"datasets": ["load_test_data"], "by_name": True},
            "aggregate": {"group_by": ["category"], "aggregations": {"count": "COUNT(*)"}},
            "deduplicate": {"keys": ["id"], "keep": "first"},
            "hash_columns": {"columns": ["id"], "output_column": "hash_id"},
        }
        return params_map.get(transformer, {})

    def _make_scd2_test_nodes(self) -> list[dict]:
        """Create nodes for SCD2 pattern test."""
        return [
            self._make_load_node("load_source", "nyc_taxi/csv/clean/data.csv"),
            {
                "name": "apply_scd2",
                "depends_on": ["load_source"],
                "pattern": "scd2",
                "params": {
                    "business_keys": ["trip_id"],
                    "tracked_columns": ["fare_amount", "tip_amount"],
                },
                "write": {
                    "connection": "artifacts",
                    "format": "parquet",
                    "path": "learning_harness/output/scd2_result",
                    "mode": "overwrite",
                },
            },
        ]

    def _make_dimension_test_nodes(self) -> list[dict]:
        """Create nodes for dimension pattern test."""
        return [
            self._make_load_node("load_dim_source"),
            {
                "name": "build_dimension",
                "depends_on": ["load_dim_source"],
                "pattern": "dimension",
                "params": {
                    "business_keys": ["id"],
                    "attributes": ["string_col", "int_col"],
                },
                "write": {
                    "connection": "artifacts",
                    "format": "parquet",
                    "path": "learning_harness/output/dimension_result",
                    "mode": "overwrite",
                },
            },
        ]

    def _make_fact_test_nodes(self) -> list[dict]:
        """Create nodes for fact pattern test."""
        return [
            self._make_load_node("load_fact_source", "nyc_taxi/csv/clean/data.csv"),
            {
                "name": "build_fact",
                "depends_on": ["load_fact_source"],
                "pattern": "fact",
                "params": {
                    "grain_columns": ["trip_id", "pickup_datetime"],
                    "measure_columns": ["fare_amount", "tip_amount"],
                },
                "write": {
                    "connection": "artifacts",
                    "format": "parquet",
                    "path": "learning_harness/output/fact_result",
                    "mode": "overwrite",
                },
            },
        ]

    def _make_date_dimension_test_nodes(self) -> list[dict]:
        """Create nodes for date dimension pattern test."""
        return [
            {
                "name": "generate_date_dim",
                "pattern": "date_dimension",
                "params": {
                    "start_date": "2020-01-01",
                    "end_date": "2025-12-31",
                },
                "write": {
                    "connection": "artifacts",
                    "format": "parquet",
                    "path": "learning_harness/output/date_dim_result",
                    "mode": "overwrite",
                },
            },
        ]

    def _make_aggregation_test_nodes(self) -> list[dict]:
        """Create nodes for aggregation pattern test."""
        return [
            self._make_load_node("load_agg_source", "nyc_taxi/csv/clean/data.csv"),
            {
                "name": "apply_aggregation",
                "depends_on": ["load_agg_source"],
                "pattern": "aggregation",
                "params": {
                    "group_by": ["payment_type"],
                    "aggregations": {
                        "total_fare": "SUM(fare_amount)",
                        "avg_tip": "AVG(tip_amount)",
                        "trip_count": "COUNT(*)",
                    },
                },
                "write": {
                    "connection": "artifacts",
                    "format": "parquet",
                    "path": "learning_harness/output/aggregation_result",
                    "mode": "overwrite",
                },
            },
        ]

    def _make_merge_test_nodes(self) -> list[dict]:
        """Create nodes for merge pattern test."""
        return [
            self._make_load_node("load_source"),
            self._make_load_node("load_target", "nyc_taxi/csv/clean/data.csv"),
            {
                "name": "merge_data",
                "depends_on": ["load_source", "load_target"],
                "pattern": "merge",
                "params": {
                    "target": "load_target",
                    "keys": ["id"],
                    "strategy": "upsert",
                },
                "write": {
                    "connection": "artifacts",
                    "format": "parquet",
                    "path": "learning_harness/output/merge_result",
                    "mode": "overwrite",
                },
            },
        ]

    def _make_empty_data_test_nodes(self) -> list[dict]:
        """Create nodes for empty data edge case test."""
        return [
            self._make_load_node("load_source"),
            {
                "name": "filter_to_empty",
                "depends_on": ["load_source"],
                "transformer": "filter_rows",
                "params": {"condition": "1 = 0"},  # Always false - empty result
                "write": {
                    "connection": "artifacts",
                    "format": "parquet",
                    "path": "learning_harness/staging/empty_df",
                    "mode": "overwrite",
                },
            },
            {
                "name": "process_empty",
                "depends_on": ["filter_to_empty"],
                "transformer": "derive_columns",
                "params": {"columns": {"derived": "COALESCE(string_col, 'none')"}},
                "write": {
                    "connection": "artifacts",
                    "format": "parquet",
                    "path": "learning_harness/output/empty_processed",
                    "mode": "overwrite",
                },
            },
        ]

    def _make_all_nulls_test_nodes(self) -> list[dict]:
        """Create nodes for all-nulls edge case test."""
        return [
            self._make_load_node("load_source"),
            {
                "name": "create_all_nulls",
                "depends_on": ["load_source"],
                "transform": {
                    "steps": [
                        {"sql": "SELECT id, NULL as null_col, NULL as null_col2 FROM df LIMIT 10"},
                    ],
                },
                "write": {
                    "connection": "artifacts",
                    "format": "parquet",
                    "path": "learning_harness/staging/all_nulls",
                    "mode": "overwrite",
                },
            },
            {
                "name": "process_nulls",
                "depends_on": ["create_all_nulls"],
                "transformer": "fill_nulls",
                "params": {"values": {"null_col": "filled", "null_col2": 0}},
                "write": {
                    "connection": "artifacts",
                    "format": "parquet",
                    "path": "learning_harness/output/nulls_filled",
                    "mode": "overwrite",
                },
            },
        ]

    def _make_type_mismatch_test_nodes(self) -> list[dict]:
        """Create nodes for type mismatch edge case test."""
        return [
            self._make_load_node("load_source"),
            {
                "name": "cast_types",
                "depends_on": ["load_source"],
                "transformer": "cast_columns",
                "params": {
                    "columns": {
                        "int_col": "string",
                        "string_col": "int",  # May fail on non-numeric strings
                    },
                },
                "write": {
                    "connection": "artifacts",
                    "format": "parquet",
                    "path": "learning_harness/output/type_cast_result",
                    "mode": "overwrite",
                },
            },
        ]

    def _make_unicode_test_nodes(self) -> list[dict]:
        """Create nodes for unicode edge case test."""
        return [
            self._make_load_node("load_source", "edge_cases/mixed/data.csv"),
            {
                "name": "filter_unicode",
                "depends_on": ["load_source"],
                "transform": {
                    "steps": [
                        {"sql": "SELECT * FROM df WHERE edge_case_type = 'unicode'"},
                    ],
                },
                "write": {
                    "connection": "artifacts",
                    "format": "parquet",
                    "path": "learning_harness/staging/unicode_rows",
                    "mode": "overwrite",
                },
            },
            {
                "name": "process_unicode",
                "depends_on": ["filter_unicode"],
                "transformer": "clean_text",
                "params": {"columns": ["string_col"]},
                "write": {
                    "connection": "artifacts",
                    "format": "parquet",
                    "path": "learning_harness/output/unicode_cleaned",
                    "mode": "overwrite",
                },
            },
        ]

    def _make_large_schema_test_nodes(self) -> list[dict]:
        """Create nodes for large schema edge case test."""
        # Generate 100 derived columns
        derived_cols = {f"col_{i}": f"id + {i}" for i in range(100)}

        return [
            self._make_load_node("load_source"),
            {
                "name": "expand_schema",
                "depends_on": ["load_source"],
                "transformer": "derive_columns",
                "params": {"columns": derived_cols},
                "write": {
                    "connection": "artifacts",
                    "format": "parquet",
                    "path": "learning_harness/staging/wide_schema",
                    "mode": "overwrite",
                },
            },
            {
                "name": "select_subset",
                "depends_on": ["expand_schema"],
                "transform": {
                    "steps": [
                        {"sql": "SELECT id, col_0, col_50, col_99 FROM df"},
                    ],
                },
                "write": {
                    "connection": "artifacts",
                    "format": "parquet",
                    "path": "learning_harness/output/wide_schema_subset",
                    "mode": "overwrite",
                },
            },
        ]

    def get_coverage_report(self) -> str:
        """Generate a coverage report as markdown.

        Returns:
            Markdown-formatted coverage report.
        """
        gaps = self.list_coverage_gaps()

        lines = [
            "# Learning Harness Coverage Report",
            "",
            "## Coverage Gaps",
            "",
        ]

        # Group by category
        by_category: dict[CoverageCategory, list[CoverageGap]] = {}
        for gap in gaps:
            if gap.category not in by_category:
                by_category[gap.category] = []
            by_category[gap.category].append(gap)

        for category, category_gaps in by_category.items():
            lines.append(f"### {category.value.title()}s")
            lines.append("")
            lines.append("| Name | Priority | Description |")
            lines.append("|------|----------|-------------|")
            for gap in sorted(category_gaps, key=lambda g: (g.priority != "high", g.name)):
                lines.append(f"| {gap.name} | {gap.priority} | {gap.description} |")
            lines.append("")

        # Summary
        high_priority = len([g for g in gaps if g.priority == "high"])
        lines.extend(
            [
                "## Summary",
                "",
                f"- **Total gaps:** {len(gaps)}",
                f"- **High priority:** {high_priority}",
                f"- **Transformers uncovered:** {len(by_category.get(CoverageCategory.TRANSFORMER, []))}",
                f"- **Patterns uncovered:** {len(by_category.get(CoverageCategory.PATTERN, []))}",
                f"- **Edge cases uncovered:** {len(by_category.get(CoverageCategory.EDGE_CASE, []))}",
            ]
        )

        return "\n".join(lines)
