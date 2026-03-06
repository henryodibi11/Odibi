"""Integration tests for PipelineManager scaffold and validation methods."""

import pytest

from odibi.pipeline import PipelineManager


@pytest.fixture
def temp_project_yaml(tmp_path):
    """Create a minimal project YAML for testing."""
    yaml_path = tmp_path / "project.yaml"
    yaml_content = """
project: test_project
engine: pandas

connections:
  local:
    type: local
    base_path: {base_path}

story:
  connection: local
  path: stories

system:
  connection: local
  path: _system

pipelines:
  - pipeline: test_pipeline
    layer: bronze
    nodes:
      - name: test_node
        read:
          connection: local
          format: csv
          path: test.csv
        write:
          connection: local
          format: delta
          table: output
""".format(base_path=str(tmp_path / "data").replace("\\", "/"))
    yaml_path.write_text(yaml_content)

    (tmp_path / "data").mkdir(exist_ok=True)
    csv_path = tmp_path / "data" / "test.csv"
    csv_path.write_text("id,name\n1,test\n")

    return yaml_path


class TestPipelineManagerScaffold:
    """Test PipelineManager scaffold methods."""

    def test_scaffold_project(self, temp_project_yaml):
        manager = PipelineManager.from_yaml(str(temp_project_yaml))

        connections = {
            "local": {"type": "local", "base_path": "data/"},
            "azure": {"type": "azure_blob", "account_name": "test"},
        }
        yaml_str = manager.scaffold_project("new_project", connections)

        assert "project: new_project" in yaml_str
        assert "local:" in yaml_str
        assert "azure:" in yaml_str
        assert "story:" in yaml_str

    def test_scaffold_sql_pipeline(self, temp_project_yaml):
        manager = PipelineManager.from_yaml(str(temp_project_yaml))

        tables = [
            {"schema": "dbo", "table": "customers", "primary_key": ["id"]},
            {"schema": "dbo", "table": "orders"},
        ]
        yaml_str = manager.scaffold_sql_pipeline("ingest", "sqldb", "lake", tables)

        assert "pipeline: ingest" in yaml_str
        assert "dbo_customers" in yaml_str
        assert "dbo_orders" in yaml_str

    def test_validate_yaml_valid(self, temp_project_yaml):
        manager = PipelineManager.from_yaml(str(temp_project_yaml))

        valid_yaml = """
pipelines:
  - pipeline: test
    nodes:
      - name: my_node
        read:
          connection: local
          format: csv
          path: data.csv
        write:
          connection: local
          format: delta
          table: output
"""
        result = manager.validate_yaml(valid_yaml)
        assert result["valid"] is True
        assert len(result["errors"]) == 0

    def test_validate_yaml_invalid(self, temp_project_yaml):
        manager = PipelineManager.from_yaml(str(temp_project_yaml))

        invalid_yaml = """
pipelines:
  - layer: bronze
    nodes: []
"""
        result = manager.validate_yaml(invalid_yaml)
        assert result["valid"] is False
        assert len(result["errors"]) > 0
        assert any(e["code"] == "MISSING_PIPELINE_NAME" for e in result["errors"])

    def test_scaffold_and_validate_roundtrip(self, temp_project_yaml):
        """Test that scaffolded YAML passes validation."""
        manager = PipelineManager.from_yaml(str(temp_project_yaml))

        # Generate project YAML
        connections = {"local": {"type": "local", "base_path": "data/"}}
        project_yaml = manager.scaffold_project("test", connections)

        # Validate it
        result = manager.validate_yaml(project_yaml)
        assert result["valid"] is True

        # Generate pipeline YAML
        tables = [{"schema": "dbo", "table": "customers"}]
        pipeline_yaml = manager.scaffold_sql_pipeline("ingest", "sqldb", "lake", tables)

        # Validate it
        result = manager.validate_yaml(pipeline_yaml)
        assert result["valid"] is True
