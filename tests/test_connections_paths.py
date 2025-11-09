"""Test connection path resolution (no network I/O)."""

from odibi.connections.azure_adls import AzureADLS
from odibi.connections.azure_sql import AzureSQL
from odibi.connections.local_dbfs import LocalDBFS


def test_azure_adls_uri_basic():
    """Test ADLS URI generation."""
    conn = AzureADLS(
        account="myaccount",
        container="data",
        auth_mode="direct_key",
        account_key="test-key",
    )
    uri = conn.uri("folder/file.csv")

    assert uri == "abfss://data@myaccount.dfs.core.windows.net/folder/file.csv"


def test_azure_adls_uri_with_prefix():
    """Test ADLS URI with path prefix."""
    conn = AzureADLS(
        account="myaccount",
        container="data",
        path_prefix="raw/",
        auth_mode="direct_key",
        account_key="test-key",
    )
    uri = conn.uri("file.csv")

    assert uri == "abfss://data@myaccount.dfs.core.windows.net/raw/file.csv"


def test_azure_adls_uri_with_leading_slash():
    """Test ADLS URI strips leading slashes correctly."""
    conn = AzureADLS(
        account="myaccount",
        container="data",
        auth_mode="direct_key",
        account_key="test-key",
    )
    uri = conn.uri("/folder/file.csv")

    assert uri == "abfss://data@myaccount.dfs.core.windows.net/folder/file.csv"


def test_azure_sql_dsn_with_managed_identity():
    """Test Azure SQL DSN with managed identity."""
    conn = AzureSQL(server="myserver.database.windows.net", database="mydb")
    dsn = conn.odbc_dsn()

    assert "Driver={ODBC Driver 18 for SQL Server}" in dsn
    assert "Server=tcp:myserver.database.windows.net,1433" in dsn
    assert "Database=mydb" in dsn
    assert "Authentication=ActiveDirectoryMsi" in dsn
    assert "Encrypt=yes" in dsn


def test_azure_sql_dsn_with_sql_auth():
    """Test Azure SQL DSN with SQL authentication."""
    conn = AzureSQL(
        server="myserver.database.windows.net",
        database="mydb",
        username="testuser",
        password="testpass",
    )
    dsn = conn.odbc_dsn()

    assert "UID=testuser" in dsn
    assert "PWD=testpass" in dsn
    assert "Authentication=ActiveDirectoryMsi" not in dsn


def test_local_dbfs_resolve():
    """Test DBFS path resolution to local filesystem."""
    import os

    conn = LocalDBFS(root="/tmp/dbfs")
    path = conn.resolve("dbfs:/FileStore/data.csv")

    # Should map to local path (handle both Unix and Windows separators)
    expected_end = os.path.join("FileStore", "data.csv")
    assert path.endswith(expected_end)
    assert "tmp" in path and "dbfs" in path


def test_local_dbfs_resolve_without_prefix():
    """Test DBFS path resolution works without dbfs:/ prefix."""
    import os

    conn = LocalDBFS(root="/tmp/dbfs")
    path = conn.resolve("FileStore/data.csv")

    expected_end = os.path.join("FileStore", "data.csv")
    assert path.endswith(expected_end)


def test_local_dbfs_default_root():
    """Test LocalDBFS uses .dbfs as default root."""
    conn = LocalDBFS()

    assert conn.root.name == ".dbfs"
