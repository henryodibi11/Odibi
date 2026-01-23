# tests/integration/mcp/test_discovery_e2e.py
"""End-to-end tests for discovery tools."""

from odibi_mcp.tools.discovery import (
    list_files,
    list_tables,
    infer_schema,
    describe_table,
    preview_source,
)
from odibi_mcp.discovery.limits import DiscoveryLimits


def test_list_files_returns_empty():
    """Test list_files with no backend returns empty result."""
    result = list_files(connection="test_conn", path="/data")

    assert result.connection == "test_conn"
    assert result.path == "/data"
    assert result.files == []
    assert result.truncated is False


def test_list_tables_returns_empty():
    """Test list_tables with no backend returns empty result."""
    result = list_tables(connection="test_db", schema="dbo")

    assert result.connection == "test_db"
    assert result.tables == []
    assert result.truncated is False


def test_infer_schema_returns_empty():
    """Test infer_schema with no backend returns empty schema."""
    result = infer_schema(connection="test_conn", path="/data/file.csv")

    assert result.columns == []
    assert result.row_count is None


def test_describe_table_returns_empty():
    """Test describe_table with no backend returns empty schema."""
    result = describe_table(connection="test_db", table="users")

    assert result.columns == []


def test_preview_source_returns_empty():
    """Test preview_source with no backend returns empty preview."""
    result = preview_source(connection="test_conn", path="/data/file.csv")

    assert result.connection == "test_conn"
    assert result.path == "/data/file.csv"
    assert result.rows == []
    assert result.truncated is False


def test_discovery_limits_validation():
    """Test DiscoveryLimits configuration."""
    limits = DiscoveryLimits(
        max_files_per_request=500,
        max_preview_rows=50,
    )

    assert limits.validate_file_count(400) is True
    assert limits.validate_file_count(600) is False
    assert limits.max_preview_rows == 50
