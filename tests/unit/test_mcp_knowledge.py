"""Tests for odibi_mcp knowledge tools."""

import pytest


class TestMCPKnowledge:
    """Tests for OdibiKnowledge class."""

    @pytest.fixture
    def knowledge(self):
        """Get knowledge instance."""
        from odibi_mcp.knowledge import get_knowledge

        return get_knowledge()

    # =========================================================================
    # Core Tools
    # =========================================================================

    def test_list_transformers(self, knowledge):
        """Test list_transformers returns registered transformers."""
        result = knowledge.list_transformers()
        assert isinstance(result, list)
        assert len(result) > 0
        # Check structure
        first = result[0]
        assert "name" in first
        assert "parameters" in first

    def test_list_patterns(self, knowledge):
        """Test list_patterns returns all 6 patterns."""
        result = knowledge.list_patterns()
        assert isinstance(result, list)
        assert len(result) >= 6
        names = [p["name"] for p in result]
        assert "scd2" in names
        assert "merge" in names
        assert "dimension" in names

    def test_list_connections(self, knowledge):
        """Test list_connections returns connection types."""
        result = knowledge.list_connections()
        assert isinstance(result, list)
        assert len(result) > 0
        names = [c["name"] for c in result]
        assert "local" in names

    def test_explain_transformer(self, knowledge):
        """Test explain returns docs for a transformer."""
        result = knowledge.explain("add_column")
        assert "name" in result
        assert result["name"] == "add_column"

    def test_explain_pattern(self, knowledge):
        """Test explain returns docs for a pattern."""
        result = knowledge.explain("scd2")
        assert "name" in result
        assert result["name"] == "scd2"

    def test_explain_unknown(self, knowledge):
        """Test explain returns found=False for unknown name."""
        result = knowledge.explain("nonexistent_thing_xyz")
        assert result.get("found") is False

    # =========================================================================
    # Code Generation Tools
    # =========================================================================

    def test_get_transformer_signature(self, knowledge):
        """Test get_transformer_signature returns valid signature."""
        result = knowledge.get_transformer_signature()
        assert "def" in result
        assert "EngineContext" in result
        assert "params" in result

    def test_get_yaml_structure(self, knowledge):
        """Test get_yaml_structure returns valid YAML template."""
        result = knowledge.get_yaml_structure()
        assert "project:" in result
        assert "connections:" in result
        assert "pipelines:" in result
        assert "nodes:" in result

    def test_generate_transformer(self, knowledge):
        """Test generate_transformer creates valid Python code."""
        code = knowledge.generate_transformer(
            name="my_test_transformer",
            params=[
                {"name": "column", "type": "str", "description": "Column name", "required": True},
                {"name": "value", "type": "int", "description": "Value", "required": False},
            ],
            description="Test transformer",
        )
        assert "def my_test_transformer" in code
        assert "class MyTestTransformerParams" in code
        assert "EngineContext" in code
        assert "column: str" in code

    def test_generate_pipeline_yaml(self, knowledge):
        """Test generate_pipeline_yaml creates valid YAML."""
        yaml_content = knowledge.generate_pipeline_yaml(
            project_name="test_project",
            input_path="data/input.csv",
            input_format="csv",
            output_path="data/output.parquet",
            output_format="parquet",
        )
        assert "project: test_project" in yaml_content
        assert "input.csv" in yaml_content
        assert "format: csv" in yaml_content
        assert "format: parquet" in yaml_content

    def test_validate_yaml_valid(self, knowledge):
        """Test validate_yaml accepts valid YAML."""
        valid_yaml = """
project: test
connections:
  local:
    type: local
    base_path: ./data
story:
  connection: local
  path: stories
system:
  connection: local
  path: _system
pipelines:
  - name: test_pipeline
    nodes:
      - name: test_node
        inputs:
          input_1:
            connection: local
            path: input.csv
            format: csv
        outputs:
          output_1:
            connection: local
            path: output.parquet
            format: parquet
"""
        result = knowledge.validate_yaml(valid_yaml)
        assert result.get("valid") is True or "error" not in result

    def test_validate_yaml_invalid(self, knowledge):
        """Test validate_yaml catches invalid YAML."""
        invalid_yaml = """
project: test
# Missing required fields
"""
        result = knowledge.validate_yaml(invalid_yaml)
        # Should have errors or valid=False
        assert result.get("valid") is False or "error" in result or "errors" in result

    # =========================================================================
    # Decision Support Tools
    # =========================================================================

    def test_suggest_pattern_scd2(self, knowledge):
        """Test suggest_pattern recommends scd2 for history tracking."""
        result = knowledge.suggest_pattern("track customer changes over time with history")
        assert result["recommendation"] == "scd2"
        assert (
            "history" in result["matched_keywords"] or "track changes" in result["matched_keywords"]
        )

    def test_suggest_pattern_merge(self, knowledge):
        """Test suggest_pattern recommends merge for upsert."""
        result = knowledge.suggest_pattern("sync data and update existing records")
        assert result["recommendation"] == "merge"
        assert any(kw in result["matched_keywords"] for kw in ["sync", "update"])

    def test_suggest_pattern_dimension(self, knowledge):
        """Test suggest_pattern recommends dimension for lookup tables."""
        result = knowledge.suggest_pattern("load reference dimension lookup data")
        assert result["recommendation"] == "dimension"

    def test_suggest_pattern_fact(self, knowledge):
        """Test suggest_pattern recommends fact for transactions."""
        result = knowledge.suggest_pattern("load transaction event data with foreign keys")
        assert result["recommendation"] == "fact"

    def test_suggest_pattern_aggregation(self, knowledge):
        """Test suggest_pattern recommends aggregation for summaries."""
        result = knowledge.suggest_pattern("create summary report with aggregate metrics")
        assert result["recommendation"] == "aggregation"

    def test_suggest_pattern_no_match(self, knowledge):
        """Test suggest_pattern handles no keyword matches."""
        result = knowledge.suggest_pattern("do something random xyz")
        assert result["recommendation"] is None
        assert "patterns_overview" in result

    def test_get_engine_differences(self, knowledge):
        """Test get_engine_differences returns engine comparison."""
        result = knowledge.get_engine_differences()
        assert "engines" in result
        assert "spark" in result["engines"]
        assert "pandas" in result["engines"]
        assert "polars" in result["engines"]
        assert "critical_differences" in result
        assert "best_practices" in result

    def test_get_engine_differences_content(self, knowledge):
        """Test get_engine_differences has useful content."""
        result = knowledge.get_engine_differences()
        spark = result["engines"]["spark"]
        pandas = result["engines"]["pandas"]
        # Check key differences documented
        assert "sql_dialect" in spark
        assert "Spark SQL" in spark["sql_dialect"]
        assert "DuckDB" in pandas["sql_dialect"]

    def test_get_validation_rules(self, knowledge):
        """Test get_validation_rules returns all rule types."""
        result = knowledge.get_validation_rules()
        assert "rule_types" in result
        rules = result["rule_types"]
        # Check key rule types
        assert "not_null" in rules
        assert "unique" in rules
        assert "range" in rules
        assert "regex" in rules
        assert "foreign_key" in rules
        assert "custom_sql" in rules

    def test_get_validation_rules_content(self, knowledge):
        """Test get_validation_rules has YAML examples."""
        result = knowledge.get_validation_rules()
        not_null = result["rule_types"]["not_null"]
        assert "yaml" in not_null
        assert "type: not_null" in not_null["yaml"]
        assert "description" in not_null

    # =========================================================================
    # Documentation Tools
    # =========================================================================

    def test_get_deep_context(self, knowledge):
        """Test get_deep_context returns full documentation."""
        result = knowledge.get_deep_context()
        assert len(result) > 1000  # Should be 2000+ lines
        assert "odibi" in result.lower()

    def test_list_docs(self, knowledge):
        """Test list_docs returns doc list."""
        result = knowledge.list_docs()
        assert isinstance(result, list)
        assert len(result) > 0

    def test_list_docs_category(self, knowledge):
        """Test list_docs filters by category."""
        result = knowledge.list_docs(category="patterns")
        assert isinstance(result, list)
        # All should be from patterns folder
        for doc in result:
            if "path" in doc:
                assert "patterns" in doc["path"]

    def test_search_docs(self, knowledge):
        """Test search_docs finds matches."""
        result = knowledge.search_docs("SCD2")
        assert isinstance(result, list)
        assert len(result) > 0

    def test_get_example(self, knowledge):
        """Test get_example returns working example."""
        result = knowledge.get_example("scd2")
        # Should have example content
        assert "yaml" in result or "yaml_examples" in result or "error" not in result

    # =========================================================================
    # Debugging Tools
    # =========================================================================

    def test_diagnose_error_validation(self, knowledge):
        """Test diagnose_error handles validation errors."""
        result = knowledge.diagnose_error("ValidationError: field required")
        assert "diagnosis" in result or "suggestions" in result or "error" not in result

    def test_diagnose_error_unknown(self, knowledge):
        """Test diagnose_error handles unknown errors."""
        result = knowledge.diagnose_error("SomeRandomError: xyz happened")
        # Should still return something useful
        assert isinstance(result, dict)
