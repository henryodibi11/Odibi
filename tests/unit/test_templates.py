"""Tests for the template generator module."""

import json

import pytest


class TestListTemplates:
    """Tests for list_templates function."""

    def test_returns_all_categories(self):
        from odibi.tools.templates import list_templates

        templates = list_templates()

        assert "connections" in templates
        assert "patterns" in templates
        assert "configs" in templates

    def test_connections_include_expected_types(self):
        from odibi.tools.templates import list_templates

        templates = list_templates()
        connections = templates["connections"]

        assert "local" in connections
        assert "azure_blob" in connections
        assert "sql_server" in connections
        assert "delta" in connections
        assert "http" in connections

    def test_patterns_include_expected_types(self):
        from odibi.tools.templates import list_templates

        templates = list_templates()
        patterns = templates["patterns"]

        assert "dimension" in patterns
        assert "fact" in patterns
        assert "scd2" in patterns
        assert "merge" in patterns
        assert "aggregation" in patterns
        assert "date_dimension" in patterns

    def test_configs_include_expected_types(self):
        from odibi.tools.templates import list_templates

        templates = list_templates()
        configs = templates["configs"]

        assert "project" in configs
        assert "pipeline" in configs
        assert "node" in configs
        assert "read" in configs
        assert "write" in configs
        assert "transform" in configs


class TestShowTemplate:
    """Tests for show_template function."""

    def test_local_connection_template(self):
        from odibi.tools.templates import show_template

        result = show_template("local")

        assert "type: local" in result
        assert "base_path" in result

    def test_azure_blob_connection_template(self):
        from odibi.tools.templates import show_template

        result = show_template("azure_blob")

        assert "type: azure_blob" in result
        assert "account_name" in result
        assert "container" in result
        assert "auth:" in result
        assert "REQUIRED" in result

    def test_sql_server_connection_template(self):
        from odibi.tools.templates import show_template

        result = show_template("sql_server")

        assert "type: sql_server" in result
        assert "host" in result
        assert "database" in result
        assert "port" in result
        assert "auth:" in result

    def test_dimension_pattern_template(self):
        from odibi.tools.templates import show_template

        result = show_template("dimension")

        assert "pattern:" in result
        assert "type: dimension" in result
        assert "params:" in result
        assert "natural_key" in result
        assert "surrogate_key" in result

    def test_unknown_template_raises_error(self):
        from odibi.tools.templates import show_template

        with pytest.raises(ValueError) as exc_info:
            show_template("nonexistent_type")

        assert "Unknown template type" in str(exc_info.value)
        assert "nonexistent_type" in str(exc_info.value)

    def test_required_only_option(self):
        from odibi.tools.templates import show_template

        full = show_template("sql_server", show_optional=True)
        required_only = show_template("sql_server", show_optional=False)

        assert len(full) >= len(required_only)

    def test_no_comments_option(self):
        from odibi.tools.templates import show_template

        with_comments = show_template("local", show_comments=True)
        without_comments = show_template("local", show_comments=False)

        assert "#" in with_comments
        assert "REQUIRED" in with_comments or "optional" in with_comments
        assert len(with_comments) >= len(without_comments)


class TestShowTransformer:
    """Tests for show_transformer function."""

    def test_scd2_transformer(self):
        from odibi.tools.templates import show_transformer

        result = show_transformer("scd2")

        assert "# scd2" in result
        assert "## Parameters" in result
        assert "keys" in result
        assert "track_cols" in result
        assert "## Example YAML" in result
        assert "- operation: scd2" in result

    def test_fluid_properties_transformer(self):
        from odibi.tools.templates import show_transformer

        result = show_transformer("fluid_properties")

        assert "# fluid_properties" in result
        assert "## Parameters" in result
        assert "pressure" in result.lower()
        assert "temperature" in result.lower()

    def test_unknown_transformer_raises_error(self):
        from odibi.tools.templates import show_transformer

        with pytest.raises(ValueError) as exc_info:
            show_transformer("nonexistent_transformer")

        assert "Unknown transformer" in str(exc_info.value)

    def test_no_example_option(self):
        from odibi.tools.templates import show_transformer

        with_example = show_transformer("scd2", show_example=True)
        no_example = show_transformer("scd2", show_example=False)

        assert "## Example YAML" in with_example
        assert "## Example YAML" not in no_example


class TestGenerateJsonSchema:
    """Tests for generate_json_schema function."""

    def test_returns_valid_schema(self):
        from odibi.tools.templates import generate_json_schema

        schema = generate_json_schema()

        assert "$schema" in schema
        assert "title" in schema
        assert schema["title"] == "Odibi Pipeline Configuration"

    def test_includes_definitions(self):
        from odibi.tools.templates import generate_json_schema

        schema = generate_json_schema()

        assert "$defs" in schema
        assert len(schema["$defs"]) > 0

    def test_includes_transformers_by_default(self):
        from odibi.tools.templates import generate_json_schema

        schema = generate_json_schema(include_transformers=True)

        assert "TransformOperation" in schema.get("$defs", {})
        transform_op = schema["$defs"]["TransformOperation"]
        assert "enum" in transform_op
        assert len(transform_op["enum"]) > 50

    def test_writes_to_file(self, tmp_path):
        from odibi.tools.templates import generate_json_schema

        output_path = tmp_path / "test_schema.json"
        schema = generate_json_schema(output_path=str(output_path))

        assert output_path.exists()
        with open(output_path) as f:
            loaded = json.load(f)
        assert loaded["title"] == schema["title"]


class TestDiscriminatedUnions:
    """Tests for discriminated union (multi-option) handling."""

    def test_azure_blob_shows_all_auth_options(self):
        from odibi.tools.templates import show_template

        result = show_template("azure_blob")

        assert "OPTIONS:" in result
        assert "key_vault" in result
        assert "account_key" in result
        assert "sas" in result
        assert "aad_msi" in result
        assert "connection_string" in result

    def test_sql_server_shows_all_auth_options(self):
        from odibi.tools.templates import show_template

        result = show_template("sql_server")

        assert "OPTIONS:" in result
        assert "sql_login" in result
        assert "aad_password" in result
        assert "aad_msi" in result
        assert "connection_string" in result

    def test_http_shows_all_auth_options(self):
        from odibi.tools.templates import show_template

        result = show_template("http")

        assert "OPTIONS:" in result
        assert "none" in result
        assert "basic" in result
        assert "bearer" in result
        assert "api_key" in result

    def test_union_shows_required_fields_per_option(self):
        from odibi.tools.templates import show_template

        result = show_template("azure_blob")

        assert "# key_vault: null  # REQUIRED" in result
        assert "# secret: null  # REQUIRED" in result
        assert "# account_key: null  # REQUIRED" in result

    def test_validation_tests_shows_all_test_types(self):
        from odibi.tools.templates import show_template

        result = show_template("validation")

        assert "List of:" in result
        assert "not_null" in result
        assert "unique" in result
        assert "accepted_values" in result
        assert "range" in result
        assert "custom_sql" in result
        assert "schema" in result
        assert "freshness" in result


class TestTemplateGenerator:
    """Tests for TemplateGenerator class."""

    def test_field_table_generation(self):
        from odibi.config import LocalConnectionConfig
        from odibi.tools.templates import TemplateGenerator

        gen = TemplateGenerator()
        table = gen.generate_field_table(LocalConnectionConfig)

        assert "| Field |" in table
        assert "type" in table
        assert "base_path" in table

    def test_handles_enum_types(self):
        from odibi.config import WriteConfig
        from odibi.tools.templates import TemplateGenerator

        gen = TemplateGenerator()
        result = gen.generate_template(WriteConfig)

        assert "mode" in result

    def test_handles_nested_models(self):
        from odibi.config import SQLServerConnectionConfig
        from odibi.tools.templates import TemplateGenerator

        gen = TemplateGenerator()
        result = gen.generate_template(SQLServerConnectionConfig)

        assert "auth:" in result
        assert "mode:" in result
