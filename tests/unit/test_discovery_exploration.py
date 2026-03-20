"""Tests for data exploration / discovery methods.

Tests preview(), relationships(), freshness(), and cross-connection discover().
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from odibi.discovery.types import PreviewResult, DatasetRef
from odibi.connections.local import LocalConnection
from odibi.connections.azure_sql import AzureSQL


class TestPreviewResultModel:
    """Test PreviewResult Pydantic model."""

    def test_basic_preview_result(self):
        result = PreviewResult(
            dataset=DatasetRef(name="test", kind="table"),
            columns=["id", "name"],
            rows=[{"id": 1, "name": "Alice"}],
            total_rows=100,
            truncated=True,
        )
        assert result.columns == ["id", "name"]
        assert len(result.rows) == 1
        assert result.truncated is True

    def test_empty_preview_result(self):
        result = PreviewResult(
            dataset=DatasetRef(name="empty", kind="file"),
        )
        assert result.columns == []
        assert result.rows == []
        assert result.truncated is False

    def test_preview_result_serialization(self):
        result = PreviewResult(
            dataset=DatasetRef(name="test", kind="table"),
            columns=["id"],
            rows=[{"id": 1}],
        )
        d = result.model_dump()
        assert "dataset" in d
        assert "columns" in d
        assert "rows" in d


class TestLocalConnectionPreview:
    """Test LocalConnection.preview()."""

    @pytest.fixture
    def local_conn_with_csv(self, tmp_path):
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(
            "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n4,Dave,400\n5,Eve,500\n"
        )
        return LocalConnection(base_path=str(tmp_path))

    def test_preview_csv(self, local_conn_with_csv):
        result = local_conn_with_csv.preview("test.csv", rows=3)
        assert len(result["rows"]) == 3
        assert "id" in result["columns"]
        assert "name" in result["columns"]
        assert result["rows"][0]["name"] == "Alice"

    def test_preview_default_rows(self, local_conn_with_csv):
        result = local_conn_with_csv.preview("test.csv")
        assert len(result["rows"]) == 5

    def test_preview_specific_columns(self, local_conn_with_csv):
        result = local_conn_with_csv.preview("test.csv", columns=["id", "name"])
        assert result["columns"] == ["id", "name"]
        assert "value" not in result["rows"][0]

    def test_preview_nonexistent_file(self, tmp_path):
        conn = LocalConnection(base_path=str(tmp_path))
        result = conn.preview("does_not_exist.csv")
        assert result["rows"] == []

    def test_preview_caps_at_100(self, tmp_path):
        """Preview should cap at 100 rows even if more requested."""
        lines = ["id,name\n"] + [f"{i},name_{i}\n" for i in range(150)]
        csv_path = tmp_path / "big.csv"
        csv_path.write_text("".join(lines))
        conn = LocalConnection(base_path=str(tmp_path))
        result = conn.preview("big.csv", rows=200)
        assert len(result["rows"]) <= 100


class TestAzureSQLPreview:
    """Test AzureSQL.preview()."""

    @pytest.fixture
    def mock_sql_conn(self):
        with patch.object(AzureSQL, "get_engine"):
            conn = AzureSQL(
                server="test.database.windows.net",
                database="testdb",
                auth_mode="sql",
                username="testuser",
                password="testpass",
            )
            return conn

    def test_preview_table(self, mock_sql_conn):
        sample_df = pd.DataFrame(
            {
                "customer_id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        count_df = pd.DataFrame({"row_count": [1000]})

        def read_sql_side_effect(query, params=None):
            if "TOP" in query:
                return sample_df
            elif "SUM(p.rows)" in query:
                return count_df
            return pd.DataFrame()

        mock_sql_conn.read_sql = MagicMock(side_effect=read_sql_side_effect)

        result = mock_sql_conn.preview("dbo.customers", rows=3)
        assert len(result["rows"]) == 3
        assert result["total_rows"] == 1000
        assert result["truncated"] is True
        assert result["dataset"]["name"] == "customers"
        assert result["dataset"]["namespace"] == "dbo"

    def test_preview_specific_columns(self, mock_sql_conn):
        sample_df = pd.DataFrame({"customer_id": [1, 2]})
        count_df = pd.DataFrame({"row_count": [100]})

        def read_sql_side_effect(query, params=None):
            if "TOP" in query:
                return sample_df
            elif "SUM(p.rows)" in query:
                return count_df
            return pd.DataFrame()

        mock_sql_conn.read_sql = MagicMock(side_effect=read_sql_side_effect)

        mock_sql_conn.preview("dbo.customers", rows=5, columns=["customer_id"])
        # Should have used column filter in query
        call_args = mock_sql_conn.read_sql.call_args_list[0]
        assert "[customer_id]" in call_args[0][0]

    def test_preview_default_schema(self, mock_sql_conn):
        sample_df = pd.DataFrame({"id": [1]})
        count_df = pd.DataFrame({"row_count": [10]})

        def read_sql_side_effect(query, params=None):
            if "TOP" in query:
                return sample_df
            elif "SUM(p.rows)" in query:
                return count_df
            return pd.DataFrame()

        mock_sql_conn.read_sql = MagicMock(side_effect=read_sql_side_effect)

        result = mock_sql_conn.preview("customers")
        assert result["dataset"]["namespace"] == "dbo"


class TestAzureSQLRelationships:
    """Test AzureSQL.relationships()."""

    @pytest.fixture
    def mock_sql_conn(self):
        with patch.object(AzureSQL, "get_engine"):
            conn = AzureSQL(
                server="test.database.windows.net",
                database="testdb",
                auth_mode="sql",
                username="testuser",
                password="testpass",
            )
            return conn

    def test_relationships_found(self, mock_sql_conn):
        fk_df = pd.DataFrame(
            {
                "fk_name": ["FK_Orders_Customers", "FK_Orders_Customers"],
                "parent_schema": ["dbo", "dbo"],
                "parent_table": ["Customers", "Customers"],
                "parent_column": ["customer_id", "tenant_id"],
                "child_schema": ["dbo", "dbo"],
                "child_table": ["Orders", "Orders"],
                "child_column": ["customer_id", "tenant_id"],
            }
        )
        mock_sql_conn.read_sql = MagicMock(return_value=fk_df)

        rels = mock_sql_conn.relationships()
        assert len(rels) == 1
        assert rels[0]["parent"]["name"] == "Customers"
        assert rels[0]["child"]["name"] == "Orders"
        assert len(rels[0]["keys"]) == 2
        assert rels[0]["source"] == "declared"
        assert rels[0]["confidence"] == 1.0

    def test_relationships_empty(self, mock_sql_conn):
        mock_sql_conn.read_sql = MagicMock(return_value=pd.DataFrame())
        rels = mock_sql_conn.relationships()
        assert rels == []

    def test_relationships_with_schema_filter(self, mock_sql_conn):
        fk_df = pd.DataFrame(
            {
                "fk_name": ["FK_test"],
                "parent_schema": ["sales"],
                "parent_table": ["Products"],
                "parent_column": ["product_id"],
                "child_schema": ["sales"],
                "child_table": ["OrderItems"],
                "child_column": ["product_id"],
            }
        )
        mock_sql_conn.read_sql = MagicMock(return_value=fk_df)

        rels = mock_sql_conn.relationships(schema="sales")
        assert len(rels) == 1
        # Verify schema filter was passed
        call_args = mock_sql_conn.read_sql.call_args
        assert call_args[1]["params"] == {"schema": "sales"}


class TestPipelineManagerExploration:
    """Test PipelineManager exploration methods."""

    @pytest.fixture
    def mock_manager(self):
        """Create a mock PipelineManager with test connections."""
        from odibi.pipeline import PipelineManager

        manager = MagicMock(spec=PipelineManager)

        # Create mock connections
        mock_local = MagicMock()
        mock_local.discover_catalog.return_value = {
            "connection_type": "local",
            "total_datasets": 5,
        }
        mock_local.preview.return_value = {
            "dataset": {"name": "test.csv", "kind": "file"},
            "rows": [{"id": 1}],
        }
        mock_local.get_freshness.return_value = {
            "dataset": {"name": "test.csv", "kind": "file"},
            "age_hours": 2.5,
        }
        # LocalConnection doesn't have a relationships method
        del mock_local.relationships

        mock_sql = MagicMock()
        mock_sql.discover_catalog.return_value = {
            "connection_type": "azure_sql",
            "total_datasets": 10,
        }
        mock_sql.relationships.return_value = [
            {"parent": {"name": "Customers"}, "child": {"name": "Orders"}}
        ]

        manager.connections = {"local_data": mock_local, "crm_db": mock_sql}

        # Bind actual methods
        manager.discover = PipelineManager.discover.__get__(manager)
        manager.preview = PipelineManager.preview.__get__(manager)
        manager.freshness = PipelineManager.freshness.__get__(manager)
        manager.relationships = PipelineManager.relationships.__get__(manager)

        # Mock _ctx for error logging
        manager._ctx = MagicMock()

        return manager

    def test_discover_all_connections(self, mock_manager):
        result = mock_manager.discover()
        assert result["connections_scanned"] == 2
        assert "local_data" in result["results"]
        assert "crm_db" in result["results"]

    def test_discover_single_connection(self, mock_manager):
        result = mock_manager.discover("crm_db")
        assert result["total_datasets"] == 10

    def test_discover_connection_not_found(self, mock_manager):
        result = mock_manager.discover("nonexistent")
        assert "error" in result
        assert result["error"]["code"] == "CONNECTION_NOT_FOUND"

    def test_preview_delegates(self, mock_manager):
        result = mock_manager.preview("local_data", "test.csv")
        assert result["rows"] == [{"id": 1}]

    def test_preview_connection_not_found(self, mock_manager):
        result = mock_manager.preview("nonexistent", "test.csv")
        assert "error" in result

    def test_freshness_delegates(self, mock_manager):
        result = mock_manager.freshness("local_data", "test.csv")
        assert result["age_hours"] == 2.5

    def test_freshness_connection_not_found(self, mock_manager):
        result = mock_manager.freshness("nonexistent", "test.csv")
        assert "error" in result

    def test_relationships_delegates(self, mock_manager):
        result = mock_manager.relationships("crm_db")
        assert len(result) == 1
        assert result[0]["parent"]["name"] == "Customers"

    def test_relationships_not_supported(self, mock_manager):
        result = mock_manager.relationships("local_data")
        assert "error" in result
        assert result["error"]["code"] == "NOT_SUPPORTED"

    def test_relationships_connection_not_found(self, mock_manager):
        result = mock_manager.relationships("nonexistent")
        assert "error" in result
        assert result["error"]["code"] == "CONNECTION_NOT_FOUND"
