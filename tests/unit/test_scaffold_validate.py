"""Tests for scaffold and validation modules."""

from odibi.scaffold import generate_project_yaml, generate_sql_pipeline, sanitize_node_name
from odibi.validate import validate_yaml


class TestSanitizeNodeName:
    """Tests for sanitize_node_name."""

    def test_simple_name(self):
        assert sanitize_node_name("my_table") == "my_table"

    def test_with_hyphens(self):
        assert sanitize_node_name("my-table-name") == "my_table_name"

    def test_with_dots(self):
        assert sanitize_node_name("dbo.my_table") == "dbo_my_table"

    def test_with_spaces(self):
        assert sanitize_node_name("my table name") == "my_table_name"

    def test_starts_with_number(self):
        assert sanitize_node_name("123_table") == "_123_table"

    def test_mixed_invalid_chars(self):
        assert sanitize_node_name("my-table.2024 v1") == "my_table_2024_v1"

    def test_uppercase_to_lowercase(self):
        assert sanitize_node_name("MyTable") == "mytable"


class TestGenerateProjectYaml:
    """Tests for generate_project_yaml."""

    def test_minimal_project(self):
        connections = {"local": {"type": "local", "base_path": "data/"}}
        yaml_str = generate_project_yaml("test_project", connections)

        assert "project: test_project" in yaml_str
        assert "connections:" in yaml_str
        assert "local:" in yaml_str
        assert "type: local" in yaml_str
        assert "story:" in yaml_str
        assert "system:" in yaml_str

    def test_with_imports(self):
        connections = {"local": {"type": "local", "base_path": "data/"}}
        imports = ["pipelines/bronze/ingest.yaml", "pipelines/silver/transform.yaml"]
        yaml_str = generate_project_yaml("test_project", connections, imports=imports)

        assert "imports:" in yaml_str
        assert "pipelines/bronze/ingest.yaml" in yaml_str

    def test_with_story_connection(self):
        connections = {
            "local": {"type": "local", "base_path": "data/"},
            "azure": {"type": "azure_blob", "account_name": "test"},
        }
        yaml_str = generate_project_yaml("test_project", connections, story_connection="azure")

        assert "story:" in yaml_str
        assert "connection: azure" in yaml_str

    def test_multiple_connections(self):
        connections = {
            "local": {"type": "local", "base_path": "data/"},
            "sqldb": {"type": "sql_server", "host": "localhost", "database": "test"},
            "azure": {"type": "azure_blob", "account_name": "test"},
        }
        yaml_str = generate_project_yaml("test_project", connections)

        assert "local:" in yaml_str
        assert "sqldb:" in yaml_str
        assert "azure:" in yaml_str


class TestGenerateSqlPipeline:
    """Tests for generate_sql_pipeline."""

    def test_simple_table(self):
        tables = [{"schema": "dbo", "table": "customers"}]
        yaml_str = generate_sql_pipeline(
            "ingest_customers", "sqldb", "lake", tables, target_format="delta"
        )

        assert "pipelines:" in yaml_str
        assert "pipeline: ingest_customers" in yaml_str
        assert "name: dbo_customers" in yaml_str
        assert "connection: sqldb" in yaml_str
        assert "format: sql" in yaml_str
        assert "table: dbo.customers" in yaml_str
        assert "connection: lake" in yaml_str
        assert "format: delta" in yaml_str

    def test_table_with_where_clause(self):
        tables = [{"schema": "dbo", "table": "orders", "where": "status = 'active'"}]
        yaml_str = generate_sql_pipeline("ingest_orders", "sqldb", "lake", tables)

        assert "query: SELECT * FROM dbo.orders WHERE status = 'active'" in yaml_str

    def test_table_with_select_columns(self):
        tables = [{"schema": "dbo", "table": "users", "columns": ["id", "name", "email"]}]
        yaml_str = generate_sql_pipeline("ingest_users", "sqldb", "lake", tables)

        assert "SELECT id, name, email FROM dbo.users" in yaml_str

    def test_table_with_incremental(self):
        tables = [
            {
                "schema": "dbo",
                "table": "events",
                "incremental_column": "created_at",
                "incremental_mode": "rolling_window",
                "incremental_lookback": 7,
                "incremental_unit": "day",
            }
        ]
        yaml_str = generate_sql_pipeline("ingest_events", "sqldb", "lake", tables)

        assert "incremental:" in yaml_str
        assert "column: created_at" in yaml_str
        assert "mode: rolling_window" in yaml_str
        assert "lookback: 7" in yaml_str
        assert "unit: day" in yaml_str

    def test_multiple_tables(self):
        tables = [
            {"schema": "dbo", "table": "customers"},
            {"schema": "dbo", "table": "orders"},
            {"schema": "sales", "table": "transactions"},
        ]
        yaml_str = generate_sql_pipeline("ingest_all", "sqldb", "lake", tables)

        assert "dbo_customers" in yaml_str
        assert "dbo_orders" in yaml_str
        assert "sales_transactions" in yaml_str

    def test_with_target_schema(self):
        tables = [{"schema": "dbo", "table": "customers"}]
        yaml_str = generate_sql_pipeline("ingest", "sqldb", "lake", tables, target_schema="bronze")

        assert "table: bronze.customers" in yaml_str

    def test_custom_layer(self):
        tables = [{"schema": "dbo", "table": "customers"}]
        yaml_str = generate_sql_pipeline("ingest", "sqldb", "lake", tables, layer="raw")

        assert "layer: raw" in yaml_str

    def test_node_prefix(self):
        tables = [{"schema": "dbo", "table": "customers"}]
        yaml_str = generate_sql_pipeline("ingest", "sqldb", "lake", tables, node_prefix="src_")

        assert "name: src_dbo_customers" in yaml_str


class TestValidateYaml:
    """Tests for validate_yaml."""

    def test_valid_project_yaml(self):
        yaml_content = """
project: test_project
connections:
  local:
    type: local
    base_path: data/
story:
  connection: local
  path: stories
system:
  connection: local
  path: _system
"""
        result = validate_yaml(yaml_content)
        assert result["valid"] is True
        assert len(result["errors"]) == 0

    def test_valid_pipeline_yaml(self):
        yaml_content = """
pipelines:
  - pipeline: test_pipeline
    layer: bronze
    nodes:
      - name: read_data
        read:
          connection: local
          format: csv
          path: data.csv
        write:
          connection: local
          format: delta
          table: output
"""
        result = validate_yaml(yaml_content)
        assert result["valid"] is True
        assert len(result["errors"]) == 0

    def test_missing_project_key(self):
        yaml_content = """
connections:
  local:
    type: local
"""
        result = validate_yaml(yaml_content)
        assert result["valid"] is False
        assert any(e["code"] == "MISSING_KEY" for e in result["errors"])

    def test_missing_connections_key(self):
        yaml_content = """
project: test_project
"""
        result = validate_yaml(yaml_content)
        assert result["valid"] is False
        assert any(e["code"] == "MISSING_KEY" for e in result["errors"])

    def test_missing_pipeline_name(self):
        yaml_content = """
pipelines:
  - layer: bronze
    nodes: []
"""
        result = validate_yaml(yaml_content)
        assert result["valid"] is False
        assert any(e["code"] == "MISSING_PIPELINE_NAME" for e in result["errors"])

    def test_invalid_node_name(self):
        yaml_content = """
pipelines:
  - pipeline: test
    nodes:
      - name: my-invalid-name
        read:
          connection: local
          format: csv
          path: data.csv
"""
        result = validate_yaml(yaml_content)
        assert result["valid"] is False
        # Either the post-Pydantic _check_node_name surfaces INVALID_NODE_NAME, or
        # the new Pydantic validate_node_name_format validator (Task 25) raises a
        # PYDANTIC_VALIDATION_FAILED. Both indicate the bad name was rejected.
        assert any(
            e["code"] in ("INVALID_NODE_NAME", "PYDANTIC_VALIDATION_FAILED")
            and "my-invalid-name" in (e.get("message", "") + e.get("fix", ""))
            for e in result["errors"]
        )

    def test_wrong_key_source(self):
        yaml_content = """
pipelines:
  - pipeline: test
    nodes:
      - name: my_node
        source:
          connection: local
          format: csv
"""
        result = validate_yaml(yaml_content)
        assert result["valid"] is False
        # Pydantic catches the validation error
        assert len(result["errors"]) > 0

    def test_missing_dependency(self):
        yaml_content = """
pipelines:
  - pipeline: test
    nodes:
      - name: node_a
        depends_on: [nonexistent_node]
        read:
          connection: local
          format: csv
          path: data.csv
"""
        result = validate_yaml(yaml_content)
        assert result["valid"] is False
        assert any(e["code"] == "MISSING_DEPENDENCY" for e in result["errors"])

    def test_yaml_syntax_error(self):
        yaml_content = """
project: test
connections:
  local
    type: local
"""
        result = validate_yaml(yaml_content)
        assert result["valid"] is False
        assert any(e["code"] == "YAML_PARSE_ERROR" for e in result["errors"])

    def test_invalid_root_type(self):
        yaml_content = "just a string"
        result = validate_yaml(yaml_content)
        assert result["valid"] is False
        assert any(e["code"] == "INVALID_ROOT" for e in result["errors"])

    def test_missing_connection_type(self):
        yaml_content = """
project: test
connections:
  local:
    base_path: data/
"""
        result = validate_yaml(yaml_content)
        assert result["valid"] is False
        assert any(e["code"] == "MISSING_CONNECTION_TYPE" for e in result["errors"])

    def test_warnings_for_missing_recommended(self):
        yaml_content = """
project: test_project
connections:
  local:
    type: local
    base_path: data/
"""
        result = validate_yaml(yaml_content)
        assert result["valid"] is True
        assert len(result["warnings"]) > 0
        assert any("story" in w["message"].lower() for w in result["warnings"])
