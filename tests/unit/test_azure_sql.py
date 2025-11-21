"""Tests for Azure SQL connection."""

import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock, patch

from odibi.connections.azure_sql import AzureSQL


class TestAzureSQLConnection:
    """Tests for AzureSQL connection class."""

    def test_create_connection_basic(self):
        """Should create Azure SQL connection with basic params."""
        conn = AzureSQL(server="myserver.database.windows.net", database="mydb")

        assert conn.server == "myserver.database.windows.net"
        assert conn.database == "mydb"
        assert conn.driver == "ODBC Driver 18 for SQL Server"
        assert conn.auth_mode == "aad_msi"

    def test_create_connection_with_sql_auth(self):
        """Should create connection with SQL authentication."""
        conn = AzureSQL(
            server="myserver.database.windows.net",
            database="mydb",
            username="admin",
            password="SecurePass123",
            auth_mode="sql",
        )

        assert conn.username == "admin"
        assert conn.password == "SecurePass123"
        assert conn.auth_mode == "sql"

    def test_odbc_dsn_with_aad_msi(self):
        """Should build ODBC DSN with AAD Managed Identity."""
        conn = AzureSQL(
            server="myserver.database.windows.net", database="mydb", auth_mode="aad_msi"
        )

        dsn = conn.odbc_dsn()

        assert "Driver={ODBC Driver 18 for SQL Server}" in dsn
        assert "Server=tcp:myserver.database.windows.net,1433" in dsn
        assert "Database=mydb" in dsn
        assert "Authentication=ActiveDirectoryMsi" in dsn
        assert "Encrypt=yes" in dsn

    def test_odbc_dsn_with_sql_auth(self):
        """Should build ODBC DSN with SQL authentication."""
        conn = AzureSQL(
            server="myserver.database.windows.net",
            database="mydb",
            username="admin",
            password="SecurePass123",
            auth_mode="sql",
        )

        dsn = conn.odbc_dsn()

        assert "UID=admin" in dsn
        assert "PWD=SecurePass123" in dsn
        assert "Authentication=ActiveDirectoryMsi" not in dsn

    def test_get_path(self):
        """Should return table reference for path."""
        conn = AzureSQL(server="myserver.database.windows.net", database="mydb")

        path = conn.get_path("users")
        assert path == "users"

    def test_validate_success(self):
        """Should validate valid configuration."""
        conn = AzureSQL(server="myserver.database.windows.net", database="mydb")

        # Should not raise
        conn.validate()

    def test_validate_missing_server(self):
        """Should fail validation if server missing."""
        with pytest.raises(ValueError, match="requires 'server'"):
            conn = AzureSQL(server="", database="mydb")
            conn.validate()

    def test_validate_missing_database(self):
        """Should fail validation if database missing."""
        with pytest.raises(ValueError, match="requires 'database'"):
            conn = AzureSQL(server="myserver", database="")
            conn.validate()

    def test_validate_sql_auth_missing_credentials(self):
        """Should fail validation if SQL auth missing credentials."""
        conn = AzureSQL(server="myserver.database.windows.net", database="mydb", auth_mode="sql")

        with pytest.raises(ValueError, match="requires username"):
            conn.validate()


class TestAzureSQLOperations:
    """Tests for Azure SQL read/write operations."""

    @patch("sqlalchemy.create_engine")
    def test_get_engine_creates_engine(self, mock_create_engine):
        """Should create SQLAlchemy engine."""
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = Mock()
        mock_create_engine.return_value = mock_engine

        conn = AzureSQL(server="myserver.database.windows.net", database="mydb")

        engine = conn.get_engine()

        assert engine == mock_engine
        mock_create_engine.assert_called_once()

    @patch("sqlalchemy.create_engine")
    def test_get_engine_caches_engine(self, mock_create_engine):
        """Should cache engine instance."""
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = Mock()
        mock_create_engine.return_value = mock_engine

        conn = AzureSQL(server="myserver.database.windows.net", database="mydb")

        engine1 = conn.get_engine()
        engine2 = conn.get_engine()

        assert engine1 == engine2
        mock_create_engine.assert_called_once()  # Called only once

    @patch("pandas.read_sql")
    @patch("sqlalchemy.create_engine")
    def test_read_sql(self, mock_create_engine, mock_read_sql):
        """Should execute SQL query and return DataFrame."""
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = Mock()
        mock_create_engine.return_value = mock_engine

        mock_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        mock_read_sql.return_value = mock_df

        conn = AzureSQL(server="myserver.database.windows.net", database="mydb")

        result = conn.read_sql("SELECT * FROM users")

        assert result.equals(mock_df)
        mock_read_sql.assert_called_once()

    @patch("pandas.read_sql")
    @patch("sqlalchemy.create_engine")
    def test_read_sql_with_params(self, mock_create_engine, mock_read_sql):
        """Should support parameterized queries."""
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = Mock()
        mock_create_engine.return_value = mock_engine
        mock_read_sql.return_value = pd.DataFrame()

        conn = AzureSQL(server="myserver.database.windows.net", database="mydb")

        conn.read_sql("SELECT * FROM users WHERE age > :min_age", {"min_age": 18})

        call_args = mock_read_sql.call_args
        assert call_args[1]["params"] == {"min_age": 18}

    @patch("pandas.read_sql")
    @patch("sqlalchemy.create_engine")
    def test_read_table(self, mock_create_engine, mock_read_sql):
        """Should read entire table."""
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = Mock()
        mock_create_engine.return_value = mock_engine
        mock_read_sql.return_value = pd.DataFrame()

        conn = AzureSQL(server="myserver.database.windows.net", database="mydb")

        conn.read_table("users", schema="dbo")

        # Should execute SELECT * FROM [dbo].[users]
        call_args = mock_read_sql.call_args
        assert "[dbo].[users]" in call_args[0][0]

    @patch("sqlalchemy.create_engine")
    def test_write_table(self, mock_create_engine):
        """Should write DataFrame to table."""
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = Mock()
        mock_create_engine.return_value = mock_engine

        conn = AzureSQL(server="myserver.database.windows.net", database="mydb")

        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

        with patch.object(df, "to_sql", return_value=2) as mock_to_sql:
            rows = conn.write_table(df, "users", if_exists="append")

            assert rows == 2
            mock_to_sql.assert_called_once()
            call_kwargs = mock_to_sql.call_args[1]
            assert call_kwargs["name"] == "users"
            assert call_kwargs["if_exists"] == "append"
            assert call_kwargs["chunksize"] == 1000

    @patch("sqlalchemy.create_engine")
    def test_write_table_with_schema(self, mock_create_engine):
        """Should write to specific schema."""
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = Mock()
        mock_create_engine.return_value = mock_engine

        conn = AzureSQL(server="myserver.database.windows.net", database="mydb")

        df = pd.DataFrame({"id": [1]})

        with patch.object(df, "to_sql", return_value=1) as mock_to_sql:
            conn.write_table(df, "users", schema="custom_schema")

            call_kwargs = mock_to_sql.call_args[1]
            assert call_kwargs["schema"] == "custom_schema"

    @patch("sqlalchemy.create_engine")
    def test_close_disposes_engine(self, mock_create_engine):
        """Should dispose of engine when closing."""
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = Mock()
        mock_create_engine.return_value = mock_engine

        conn = AzureSQL(server="myserver.database.windows.net", database="mydb")

        # Get engine
        conn.get_engine()

        # Close connection
        conn.close()

        mock_engine.dispose.assert_called_once()
        assert conn._engine is None


class TestAzureSQLErrorHandling:
    """Tests for error handling in Azure SQL connection."""

    def test_get_engine_without_sqlalchemy(self):
        """Should raise error if SQLAlchemy not installed."""
        conn = AzureSQL(server="myserver.database.windows.net", database="mydb")

        from odibi.exceptions import ConnectionError

        with patch.dict("sys.modules", {"sqlalchemy": None}):
            with pytest.raises(ConnectionError, match="Required packages"):
                conn.get_engine()

    @patch("pandas.read_sql")
    @patch("sqlalchemy.create_engine")
    def test_read_sql_handles_connection_error(self, mock_create_engine, mock_read_sql):
        """Should propagate connection errors."""
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = Mock()
        mock_create_engine.return_value = mock_engine
        mock_read_sql.side_effect = Exception("Connection failed")

        from odibi.exceptions import ConnectionError

        conn = AzureSQL(server="myserver.database.windows.net", database="mydb")

        # Matches our nicely formatted error message
        with pytest.raises(ConnectionError, match="Connection validation failed"):
            conn.read_sql("SELECT 1")
