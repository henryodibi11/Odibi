"""Test discovery methods for AzureSQL connection.

These tests demonstrate the discovery functionality but require a real Azure SQL database.
Run them manually when you have access to an Azure SQL instance.
"""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime

from odibi.connections.azure_sql import AzureSQL


class TestAzureSQLDiscovery:
    """Test discovery methods for AzureSQL."""

    @pytest.fixture
    def mock_conn(self):
        """Create a mock AzureSQL connection."""
        with patch.object(AzureSQL, "get_engine"):
            conn = AzureSQL(
                server="test.database.windows.net",
                database="testdb",
                auth_mode="sql",
                username="testuser",
                password="testpass",
            )
            return conn

    def test_list_schemas(self, mock_conn):
        """Test list_schemas returns schema list."""
        # Mock read_sql to return schema data
        mock_df = pd.DataFrame({"SCHEMA_NAME": ["dbo", "staging", "warehouse"]})
        mock_conn.read_sql = MagicMock(return_value=mock_df)

        schemas = mock_conn.list_schemas()

        assert schemas == ["dbo", "staging", "warehouse"]
        assert mock_conn.read_sql.called

    def test_list_tables(self, mock_conn):
        """Test list_tables returns table list."""
        # Mock read_sql to return table data
        mock_df = pd.DataFrame(
            {
                "TABLE_NAME": ["customers", "orders"],
                "TABLE_TYPE": ["BASE TABLE", "BASE TABLE"],
                "TABLE_SCHEMA": ["dbo", "dbo"],
            }
        )
        mock_conn.read_sql = MagicMock(return_value=mock_df)

        tables = mock_conn.list_tables("dbo")

        assert len(tables) == 2
        assert tables[0]["name"] == "customers"
        assert tables[0]["type"] == "table"
        assert tables[0]["schema"] == "dbo"

    def test_get_table_info(self, mock_conn):
        """Test get_table_info returns schema information."""
        # Mock column information
        mock_columns_df = pd.DataFrame(
            {
                "COLUMN_NAME": ["customer_id", "name", "email"],
                "DATA_TYPE": ["int", "varchar", "varchar"],
                "IS_NULLABLE": ["NO", "YES", "YES"],
                "ORDINAL_POSITION": [1, 2, 3],
            }
        )

        # Mock row count
        mock_count_df = pd.DataFrame({"row_count": [1000]})

        def read_sql_side_effect(query, params=None):
            if "INFORMATION_SCHEMA.COLUMNS" in query:
                return mock_columns_df
            elif "row_count" in query.lower():
                return mock_count_df
            return pd.DataFrame()

        mock_conn.read_sql = MagicMock(side_effect=read_sql_side_effect)

        info = mock_conn.get_table_info("dbo.customers")

        assert "dataset" in info
        assert "columns" in info
        assert info["dataset"]["name"] == "customers"
        assert info["dataset"]["namespace"] == "dbo"
        assert info["dataset"]["row_count"] == 1000
        assert len(info["columns"]) == 3

    def test_discover_catalog(self, mock_conn):
        """Test discover_catalog returns full catalog."""
        # Mock schemas
        schemas_df = pd.DataFrame({"SCHEMA_NAME": ["dbo"]})

        # Mock tables
        tables_df = pd.DataFrame(
            {
                "TABLE_NAME": ["customers", "orders"],
                "TABLE_TYPE": ["BASE TABLE", "BASE TABLE"],
                "TABLE_SCHEMA": ["dbo", "dbo"],
            }
        )

        def read_sql_side_effect(query, params=None):
            if "INFORMATION_SCHEMA.SCHEMATA" in query:
                return schemas_df
            elif "INFORMATION_SCHEMA.TABLES" in query:
                return tables_df
            return pd.DataFrame()

        mock_conn.read_sql = MagicMock(side_effect=read_sql_side_effect)

        catalog = mock_conn.discover_catalog()

        assert "connection_name" in catalog
        assert "schemas" in catalog
        assert "tables" in catalog
        assert catalog["total_datasets"] == 2
        assert len(catalog["schemas"]) == 1

    def test_profile(self, mock_conn):
        """Test profile returns profiling statistics."""
        # Mock sample data
        sample_df = pd.DataFrame(
            {
                "customer_id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", None, "Eve"],
                "created_at": pd.date_range("2024-01-01", periods=5),
            }
        )

        # Mock row count
        count_df = pd.DataFrame({"row_count": [1000]})

        def read_sql_side_effect(query, params=None):
            if "TOP" in query and "SELECT" in query:
                return sample_df
            elif "SUM(p.rows)" in query:
                return count_df
            return pd.DataFrame()

        mock_conn.read_sql = MagicMock(side_effect=read_sql_side_effect)

        profile = mock_conn.profile("dbo.customers", sample_rows=5)

        assert "dataset" in profile
        assert "columns" in profile
        assert "candidate_keys" in profile
        assert "candidate_watermarks" in profile
        assert profile["rows_sampled"] == 5
        assert profile["total_rows"] == 1000
        assert "customer_id" in profile["candidate_keys"]  # All unique
        assert "created_at" in profile["candidate_watermarks"]  # Datetime column

    def test_get_freshness_with_timestamp(self, mock_conn):
        """Test get_freshness with timestamp column."""
        # Mock max timestamp query
        max_ts_df = pd.DataFrame({"max_ts": [datetime(2024, 3, 15, 10, 30)]})
        mock_conn.read_sql = MagicMock(return_value=max_ts_df)

        freshness = mock_conn.get_freshness("dbo.orders", timestamp_column="order_date")

        assert "dataset" in freshness
        assert "last_updated" in freshness
        assert freshness["source"] == "data"
        assert "age_hours" in freshness

    def test_get_freshness_from_metadata(self, mock_conn):
        """Test get_freshness from table metadata."""
        # Mock metadata query
        metadata_df = pd.DataFrame({"modify_date": [datetime(2024, 3, 15, 10, 30)]})
        mock_conn.read_sql = MagicMock(return_value=metadata_df)

        freshness = mock_conn.get_freshness("dbo.customers")

        assert "dataset" in freshness
        assert "last_updated" in freshness
        assert freshness["source"] == "metadata"

    def test_schema_parsing(self, mock_conn):
        """Test that schema.table parsing works correctly."""
        mock_df = pd.DataFrame(
            {
                "COLUMN_NAME": ["id"],
                "DATA_TYPE": ["int"],
                "IS_NULLABLE": ["NO"],
                "ORDINAL_POSITION": [1],
            }
        )
        mock_conn.read_sql = MagicMock(return_value=mock_df)

        # Test with explicit schema
        info1 = mock_conn.get_table_info("staging.users")
        assert info1["dataset"]["namespace"] == "staging"
        assert info1["dataset"]["name"] == "users"

        # Test without schema (defaults to dbo)
        info2 = mock_conn.get_table_info("customers")
        assert info2["dataset"]["namespace"] == "dbo"
        assert info2["dataset"]["name"] == "customers"


def test_discovery_integration_example():
    """
    Example integration test (requires real Azure SQL connection).

    To run this test manually:
    1. Set up Azure SQL credentials
    2. Uncomment and modify the connection details
    3. Run: pytest tests/integration/test_azure_sql_discovery.py::test_discovery_integration_example -v
    """
    pytest.skip("Requires real Azure SQL connection - run manually")

    # Uncomment and configure for real testing:
    # conn = AzureSQL(
    #     server="your-server.database.windows.net",
    #     database="your-database",
    #     auth_mode="sql",
    #     username="your-username",
    #     password="your-password",
    # )
    #
    # # Test discovery methods
    # schemas = conn.list_schemas()
    # print(f"Found schemas: {schemas}")
    #
    # tables = conn.list_tables("dbo")
    # print(f"Found tables in dbo: {[t['name'] for t in tables]}")
    #
    # if tables:
    #     table_name = f"dbo.{tables[0]['name']}"
    #     info = conn.get_table_info(table_name)
    #     print(f"Table info: {info['dataset']}")
    #
    #     profile = conn.profile(table_name, sample_rows=100)
    #     print(f"Profile: {profile['candidate_keys']}, {profile['candidate_watermarks']}")
    #
    #     freshness = conn.get_freshness(table_name)
    #     print(f"Freshness: {freshness.get('last_updated')}")
    #
    # catalog = conn.discover_catalog(include_schema=True, limit=5)
    # print(f"Catalog summary: {catalog['total_datasets']} datasets")
    #
    # conn.close()


if __name__ == "__main__":
    print("AzureSQL Discovery Methods - Unit Tests")
    print("=" * 60)
    print("\nRun with: pytest tests/integration/test_azure_sql_discovery.py -v")
    print("\nAvailable discovery methods:")
    print("  - list_schemas() → List[str]")
    print("  - list_tables(schema) → List[Dict]")
    print("  - get_table_info(table) → Dict (Schema)")
    print("  - discover_catalog(...) → Dict (CatalogSummary)")
    print("  - profile(dataset, ...) → Dict (TableProfile)")
    print("  - get_freshness(dataset, ...) → Dict (FreshnessResult)")
    print("\n" + "=" * 60)
