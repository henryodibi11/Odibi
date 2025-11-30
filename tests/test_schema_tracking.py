"""Tests for schema version tracking functionality."""

import json
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd
import pytest

from odibi.catalog import CatalogManager
from odibi.config import SystemConfig


class MockEngine:
    """Mock engine for testing."""

    name = "pandas"

    def write(self, df, **kwargs):
        return {"rows_written": len(df)}


@pytest.fixture
def mock_engine():
    return MockEngine()


@pytest.fixture
def system_config():
    return SystemConfig(connection="local", path="_odibi_system")


@pytest.fixture
def temp_catalog(tmp_path, system_config, mock_engine):
    """Create a CatalogManager with temp directory."""
    return CatalogManager(
        spark=None,
        config=system_config,
        base_path=str(tmp_path),
        engine=mock_engine,
    )


class TestSchemaHashing:
    """Tests for schema hashing functionality."""

    def test_hash_schema_deterministic(self, temp_catalog):
        """Same schema should produce same hash."""
        schema1 = {"id": "int", "name": "string", "email": "string"}
        schema2 = {"id": "int", "name": "string", "email": "string"}

        hash1 = temp_catalog._hash_schema(schema1)
        hash2 = temp_catalog._hash_schema(schema2)

        assert hash1 == hash2

    def test_hash_schema_order_independent(self, temp_catalog):
        """Schema hash should be independent of key order."""
        schema1 = {"id": "int", "name": "string"}
        schema2 = {"name": "string", "id": "int"}

        hash1 = temp_catalog._hash_schema(schema1)
        hash2 = temp_catalog._hash_schema(schema2)

        assert hash1 == hash2

    def test_hash_schema_different_for_different_schemas(self, temp_catalog):
        """Different schemas should produce different hashes."""
        schema1 = {"id": "int", "name": "string"}
        schema2 = {"id": "int", "name": "string", "email": "string"}

        hash1 = temp_catalog._hash_schema(schema1)
        hash2 = temp_catalog._hash_schema(schema2)

        assert hash1 != hash2


class TestTrackSchema:
    """Tests for track_schema method."""

    def test_track_schema_first_version(self, temp_catalog):
        """First schema version should be v1."""
        with patch.object(temp_catalog, "_get_latest_schema", return_value=None):
            with patch.object(temp_catalog.engine, "write"):
                result = temp_catalog.track_schema(
                    table_path="silver/customers",
                    schema={"id": "int", "name": "string"},
                    pipeline="test_pipeline",
                    node="test_node",
                    run_id="run-123",
                )

        assert result["changed"] is True
        assert result["version"] == 1
        assert result["previous_version"] is None

    def test_track_schema_no_change(self, temp_catalog):
        """Unchanged schema should return changed=False."""
        prev_schema = {"id": "int", "name": "string"}
        prev_hash = temp_catalog._hash_schema(prev_schema)

        mock_previous = {
            "schema_version": 5,
            "schema_hash": prev_hash,
            "columns": json.dumps(prev_schema),
        }

        with patch.object(temp_catalog, "_get_latest_schema", return_value=mock_previous):
            result = temp_catalog.track_schema(
                table_path="silver/customers",
                schema=prev_schema,
                pipeline="test_pipeline",
                node="test_node",
                run_id="run-123",
            )

        assert result["changed"] is False
        assert result["version"] == 5

    def test_track_schema_detects_added_columns(self, temp_catalog):
        """Should detect new columns."""
        prev_schema = {"id": "int", "name": "string"}
        new_schema = {"id": "int", "name": "string", "email": "string"}

        mock_previous = {
            "schema_version": 1,
            "schema_hash": temp_catalog._hash_schema(prev_schema),
            "columns": json.dumps(prev_schema),
        }

        with patch.object(temp_catalog, "_get_latest_schema", return_value=mock_previous):
            with patch.object(temp_catalog.engine, "write"):
                result = temp_catalog.track_schema(
                    table_path="silver/customers",
                    schema=new_schema,
                    pipeline="test_pipeline",
                    node="test_node",
                    run_id="run-123",
                )

        assert result["changed"] is True
        assert result["version"] == 2
        assert "email" in result["columns_added"]

    def test_track_schema_detects_removed_columns(self, temp_catalog):
        """Should detect removed columns."""
        prev_schema = {"id": "int", "name": "string", "email": "string"}
        new_schema = {"id": "int", "name": "string"}

        mock_previous = {
            "schema_version": 1,
            "schema_hash": temp_catalog._hash_schema(prev_schema),
            "columns": json.dumps(prev_schema),
        }

        with patch.object(temp_catalog, "_get_latest_schema", return_value=mock_previous):
            with patch.object(temp_catalog.engine, "write"):
                result = temp_catalog.track_schema(
                    table_path="silver/customers",
                    schema=new_schema,
                    pipeline="test_pipeline",
                    node="test_node",
                    run_id="run-123",
                )

        assert result["changed"] is True
        assert "email" in result["columns_removed"]

    def test_track_schema_detects_type_changes(self, temp_catalog):
        """Should detect column type changes."""
        prev_schema = {"id": "int", "name": "string"}
        new_schema = {"id": "bigint", "name": "string"}

        mock_previous = {
            "schema_version": 1,
            "schema_hash": temp_catalog._hash_schema(prev_schema),
            "columns": json.dumps(prev_schema),
        }

        with patch.object(temp_catalog, "_get_latest_schema", return_value=mock_previous):
            with patch.object(temp_catalog.engine, "write"):
                result = temp_catalog.track_schema(
                    table_path="silver/customers",
                    schema=new_schema,
                    pipeline="test_pipeline",
                    node="test_node",
                    run_id="run-123",
                )

        assert result["changed"] is True
        assert "id" in result["columns_type_changed"]


class TestGetSchemaHistory:
    """Tests for get_schema_history method."""

    def test_get_schema_history_empty(self, temp_catalog):
        """Should return empty list when no history exists."""
        with patch.object(temp_catalog, "_read_local_table", return_value=pd.DataFrame()):
            history = temp_catalog.get_schema_history("silver/customers")

        assert history == []

    def test_get_schema_history_returns_records(self, temp_catalog):
        """Should return schema history records."""
        mock_df = pd.DataFrame(
            [
                {
                    "table_path": "silver/customers",
                    "schema_version": 2,
                    "schema_hash": "abc123",
                    "columns": '{"id": "int", "name": "string"}',
                    "captured_at": datetime.now(timezone.utc),
                    "pipeline": "test",
                    "node": "node1",
                    "run_id": "run-123",
                },
                {
                    "table_path": "silver/customers",
                    "schema_version": 1,
                    "schema_hash": "xyz789",
                    "columns": '{"id": "int"}',
                    "captured_at": datetime.now(timezone.utc),
                    "pipeline": "test",
                    "node": "node1",
                    "run_id": "run-001",
                },
            ]
        )

        with patch.object(temp_catalog, "_read_local_table", return_value=mock_df):
            history = temp_catalog.get_schema_history("silver/customers", limit=10)

        assert len(history) == 2
        assert history[0]["schema_version"] == 2

    def test_get_schema_history_respects_limit(self, temp_catalog):
        """Should respect the limit parameter."""
        mock_df = pd.DataFrame(
            [
                {"table_path": "t", "schema_version": i, "schema_hash": f"h{i}", "columns": "{}"}
                for i in range(10, 0, -1)
            ]
        )

        with patch.object(temp_catalog, "_read_local_table", return_value=mock_df):
            history = temp_catalog.get_schema_history("t", limit=3)

        assert len(history) == 3


class TestMetaSchemasTableSchema:
    """Tests for meta_schemas table schema."""

    def test_meta_schemas_schema_has_required_fields(self, temp_catalog):
        """meta_schemas should have all required fields."""
        schema = temp_catalog._get_schema_meta_schemas()
        field_names = [f.name for f in schema.fields]

        assert "table_path" in field_names
        assert "schema_version" in field_names
        assert "schema_hash" in field_names
        assert "columns" in field_names
        assert "captured_at" in field_names
        assert "pipeline" in field_names
        assert "node" in field_names
        assert "run_id" in field_names
        assert "columns_added" in field_names
        assert "columns_removed" in field_names
        assert "columns_type_changed" in field_names
