"""Unit tests for Azure SQL connection."""

from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from odibi.connections.azure_sql import AzureSQL
from odibi.exceptions import ConnectionError


class TestAzureSQLInit:
    """Tests for AzureSQL initialization."""

    def test_init_with_defaults(self):
        """Test initialization with minimal required parameters."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )
        assert conn.server == "test.database.windows.net"
        assert conn.database == "testdb"
        assert conn.driver == "ODBC Driver 18 for SQL Server"
        assert conn.auth_mode == "aad_msi"
        assert conn.port == 1433
        assert conn.timeout == 30
        assert conn._engine is None

    def test_init_with_sql_auth(self):
        """Test initialization with SQL authentication."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            username="testuser",
            password="testpass",
            auth_mode="sql",
        )
        assert conn.username == "testuser"
        assert conn.password == "testpass"
        assert conn.auth_mode == "sql"

    def test_init_with_key_vault(self):
        """Test initialization with Key Vault configuration."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            username="testuser",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )
        assert conn.auth_mode == "key_vault"
        assert conn.key_vault_name == "myvault"
        assert conn.secret_name == "mysecret"

    def test_init_with_custom_port_and_timeout(self):
        """Test initialization with custom port and timeout."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            port=1234,
            timeout=60,
        )
        assert conn.port == 1234
        assert conn.timeout == 60


class TestGetPassword:
    """Tests for get_password method."""

    def test_get_password_returns_provided_password(self):
        """Test that provided password is returned."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            password="mypassword",
        )
        assert conn.get_password() == "mypassword"

    def test_get_password_returns_cached_key(self):
        """Test that cached key is returned if available."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )
        conn._cached_key = "cached_secret"
        assert conn.get_password() == "cached_secret"

    def test_get_password_fetches_from_key_vault(self):
        """Test fetching password from Azure Key Vault."""
        from types import ModuleType

        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            username="testuser",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )

        mock_secret = Mock()
        mock_secret.value = "vault_password"
        mock_client = Mock()
        mock_client.get_secret.return_value = mock_secret

        # Create mock modules that don't require actual azure package
        mock_credential_class = MagicMock()
        mock_client_class = MagicMock(return_value=mock_client)

        # Create mock azure modules
        azure_identity = ModuleType("azure.identity")
        azure_identity.DefaultAzureCredential = mock_credential_class
        azure_keyvault = ModuleType("azure.keyvault")
        azure_keyvault_secrets = ModuleType("azure.keyvault.secrets")
        azure_keyvault_secrets.SecretClient = mock_client_class

        # Use patch.dict to properly mock sys.modules (handles cleanup automatically)
        with patch.dict(
            "sys.modules",
            {
                "azure.identity": azure_identity,
                "azure.keyvault": azure_keyvault,
                "azure.keyvault.secrets": azure_keyvault_secrets,
            },
        ):
            with (
                patch.object(conn, "_cached_key", None),
                patch("odibi.connections.azure_sql.logger") as mock_logger,
            ):
                password = conn.get_password()

                assert password == "vault_password"
                assert conn._cached_key == "vault_password"
                mock_client_class.assert_called_once_with(
                    vault_url="https://myvault.vault.azure.net",
                    credential=mock_credential_class.return_value,
                )
                mock_client.get_secret.assert_called_once_with("mysecret")
                mock_logger.register_secret.assert_called_once_with("vault_password")

    def test_get_password_key_vault_missing_config_raises_error(self):
        """Test that key_vault mode without config raises ValueError."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            auth_mode="key_vault",
            # Missing key_vault_name and secret_name
        )

        with pytest.raises(
            ValueError, match="key_vault mode requires 'key_vault_name' and 'secret_name'"
        ):
            conn.get_password()

    def test_get_password_key_vault_import_error(self):
        """Test that missing azure libraries raises ImportError."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            username="testuser",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )

        # Simulate import error by making the import fail
        import builtins

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name in ("azure.identity", "azure.keyvault.secrets"):
                raise ImportError("No module named azure")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(
                ImportError,
                match="Key Vault support requires 'azure-identity' and 'azure-keyvault-secrets'",
            ):
                conn.get_password()

    def test_get_password_returns_none_for_aad_msi(self):
        """Test that aad_msi mode returns None (no password needed)."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            auth_mode="aad_msi",
        )
        assert conn.get_password() is None


class TestOdbcDsn:
    """Tests for odbc_dsn connection string building."""

    def test_odbc_dsn_with_aad_msi(self):
        """Test ODBC DSN string for AAD Managed Identity."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            auth_mode="aad_msi",
            port=1433,
            timeout=30,
        )

        dsn = conn.odbc_dsn()

        assert "Driver={ODBC Driver 18 for SQL Server}" in dsn
        assert "Server=tcp:test.database.windows.net,1433" in dsn
        assert "Database=testdb" in dsn
        assert "Encrypt=yes" in dsn
        assert "TrustServerCertificate=yes" in dsn
        assert "Connection Timeout=30" in dsn
        assert "Authentication=ActiveDirectoryMsi" in dsn
        assert "UID=" not in dsn
        assert "PWD=" not in dsn

    def test_odbc_dsn_with_sql_auth(self):
        """Test ODBC DSN string for SQL authentication."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            username="testuser",
            password="testpass",
            auth_mode="sql",
        )

        dsn = conn.odbc_dsn()

        assert "Driver={ODBC Driver 18 for SQL Server}" in dsn
        assert "Server=tcp:test.database.windows.net,1433" in dsn
        assert "Database=testdb" in dsn
        assert "UID=testuser" in dsn
        assert "PWD=testpass" in dsn
        assert "Authentication=ActiveDirectoryMsi" not in dsn

    def test_odbc_dsn_with_custom_driver(self):
        """Test ODBC DSN string with custom driver."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            driver="ODBC Driver 17 for SQL Server",
        )

        dsn = conn.odbc_dsn()

        assert "Driver={ODBC Driver 17 for SQL Server}" in dsn

    def test_odbc_dsn_with_custom_port_and_timeout(self):
        """Test ODBC DSN string with custom port and timeout."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            port=1234,
            timeout=60,
        )

        dsn = conn.odbc_dsn()

        assert "Server=tcp:test.database.windows.net,1234" in dsn
        assert "Connection Timeout=60" in dsn


class TestValidate:
    """Tests for connection validation."""

    def test_validate_success_with_aad_msi(self):
        """Test validation succeeds with AAD MSI."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            auth_mode="aad_msi",
        )
        # Should not raise
        conn.validate()

    def test_validate_success_with_sql_auth(self):
        """Test validation succeeds with SQL authentication."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            username="testuser",
            password="testpass",
            auth_mode="sql",
        )
        # Should not raise
        conn.validate()

    def test_validate_success_with_key_vault(self):
        """Test validation succeeds with Key Vault."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            username="testuser",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )
        # Should not raise
        conn.validate()

    def test_validate_fails_without_server(self):
        """Test validation fails when server is missing."""
        with pytest.raises(ValueError, match="Azure SQL connection requires 'server'"):
            conn = AzureSQL(server="", database="testdb")
            conn.validate()

    def test_validate_fails_without_database(self):
        """Test validation fails when database is missing."""
        with pytest.raises(ValueError, match="Azure SQL connection requires 'database'"):
            conn = AzureSQL(server="test.database.windows.net", database="")
            conn.validate()

    def test_validate_fails_sql_auth_without_username(self):
        """Test validation fails for SQL auth without username."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            auth_mode="sql",
            password="testpass",
        )
        with pytest.raises(ValueError, match="auth_mode='sql' requires 'username'"):
            conn.validate()

    def test_validate_fails_sql_auth_without_password(self):
        """Test validation fails for SQL auth without password or Key Vault."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            auth_mode="sql",
            username="testuser",
        )
        with pytest.raises(ValueError, match="auth_mode='sql' requires password"):
            conn.validate()

    def test_validate_fails_key_vault_without_vault_name(self):
        """Test validation fails for key_vault without vault name."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            username="testuser",
            auth_mode="key_vault",
            secret_name="mysecret",
        )
        with pytest.raises(
            ValueError, match="auth_mode='key_vault' requires key_vault_name and secret_name"
        ):
            conn.validate()

    def test_validate_fails_key_vault_without_username(self):
        """Test validation fails for key_vault without username."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )
        with pytest.raises(ValueError, match="auth_mode='key_vault' requires username"):
            conn.validate()


class TestGetEngine:
    """Tests for get_engine method."""

    def test_get_engine_returns_cached_engine(self):
        """Test that cached engine is returned if available."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )
        mock_engine = Mock()
        conn._engine = mock_engine

        engine = conn.get_engine()

        assert engine is mock_engine

    def test_get_engine_creates_new_engine(self):
        """Test creating a new SQLAlchemy engine."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            auth_mode="aad_msi",
        )

        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=False)

        # Mock the create_engine function at the point where it's imported in azure_sql
        import sys
        from types import ModuleType

        # Create mock sqlalchemy module
        sqlalchemy_mod = ModuleType("sqlalchemy")
        sqlalchemy_mod.create_engine = MagicMock(return_value=mock_engine)
        sys.modules["sqlalchemy"] = sqlalchemy_mod

        # Also need urllib.parse for quote_plus
        if "urllib" not in sys.modules:
            urllib_mod = ModuleType("urllib")
            urllib_parse = ModuleType("urllib.parse")
            urllib_parse.quote_plus = MagicMock(
                side_effect=lambda x: x.replace("=", "%3D").replace(";", "%3B").replace(":", "%3A")
            )
            urllib_mod.parse = urllib_parse
            sys.modules["urllib"] = urllib_mod
            sys.modules["urllib.parse"] = urllib_parse

        try:
            engine = conn.get_engine()

            assert engine is mock_engine
            assert conn._engine is mock_engine
            sqlalchemy_mod.create_engine.assert_called_once()

            # Verify connection URL format
            call_args = sqlalchemy_mod.create_engine.call_args
            connection_url = call_args[0][0]
            assert connection_url.startswith("mssql+pyodbc:///?odbc_connect=")
        finally:
            # Cleanup
            if "sqlalchemy" in sys.modules:
                del sys.modules["sqlalchemy"]

    def test_get_engine_import_error(self):
        """Test that missing sqlalchemy raises ConnectionError."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        # Simulate import error by making the import fail
        import builtins

        original_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name in ("sqlalchemy", "urllib.parse"):
                raise ImportError("No module named sqlalchemy")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with pytest.raises(
                ConnectionError, match="Required packages 'sqlalchemy' or 'pyodbc' not found"
            ):
                conn.get_engine()

    def test_get_engine_connection_error(self):
        """Test that connection failure raises ConnectionError."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        mock_engine = Mock()
        mock_engine.connect.side_effect = Exception("Connection refused")

        import sys
        from types import ModuleType

        # Create mock sqlalchemy module
        sqlalchemy_mod = ModuleType("sqlalchemy")
        sqlalchemy_mod.create_engine = MagicMock(return_value=mock_engine)
        sys.modules["sqlalchemy"] = sqlalchemy_mod

        # Also need urllib.parse
        if "urllib" not in sys.modules:
            urllib_mod = ModuleType("urllib")
            urllib_parse = ModuleType("urllib.parse")
            urllib_parse.quote_plus = MagicMock(side_effect=lambda x: x)
            urllib_mod.parse = urllib_parse
            sys.modules["urllib"] = urllib_mod
            sys.modules["urllib.parse"] = urllib_parse

        try:
            with pytest.raises(ConnectionError, match="Failed to create engine"):
                conn.get_engine()
        finally:
            # Cleanup
            if "sqlalchemy" in sys.modules:
                del sys.modules["sqlalchemy"]


class TestReadSql:
    """Tests for read_sql method."""

    def test_read_sql_success(self):
        """Test successful SQL query execution."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        mock_engine = Mock()
        mock_connection = Mock()
        mock_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        mock_engine.connect.return_value = mock_connection

        conn._engine = mock_engine

        with patch("pandas.read_sql") as mock_read_sql:
            mock_read_sql.return_value = mock_df

            result = conn.read_sql("SELECT * FROM users")

            assert len(result) == 2
            assert list(result.columns) == ["id", "name"]
            mock_read_sql.assert_called_once_with(
                "SELECT * FROM users", mock_connection, params=None
            )

    def test_read_sql_with_params(self):
        """Test SQL query with parameters."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        mock_engine = Mock()
        mock_connection = Mock()
        mock_df = pd.DataFrame({"id": [1], "name": ["Alice"]})

        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        mock_engine.connect.return_value = mock_connection

        conn._engine = mock_engine

        with patch("pandas.read_sql") as mock_read_sql:
            mock_read_sql.return_value = mock_df

            result = conn.read_sql("SELECT * FROM users WHERE id = :id", params={"id": 1})

            assert len(result) == 1
            mock_read_sql.assert_called_once()
            assert mock_read_sql.call_args[0][1] == mock_connection
            assert mock_read_sql.call_args[1]["params"] == {"id": 1}

    def test_read_sql_error(self):
        """Test SQL query execution error."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        mock_engine = Mock()
        mock_connection = Mock()
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        mock_engine.connect.return_value = mock_connection

        conn._engine = mock_engine

        with patch("pandas.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = Exception("Invalid SQL syntax")

            with pytest.raises(ConnectionError, match="Query execution failed"):
                conn.read_sql("SELECT * FROM invalid_table")


class TestReadTable:
    """Tests for read_table method."""

    def test_read_table_with_schema(self):
        """Test reading table with schema."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        mock_df = pd.DataFrame({"id": [1, 2]})

        with patch.object(conn, "read_sql", return_value=mock_df) as mock_read_sql:
            result = conn.read_table("users", schema="dbo")

            assert len(result) == 2
            mock_read_sql.assert_called_once_with("SELECT * FROM [dbo].[users]")

    def test_read_table_without_schema(self):
        """Test reading table without schema."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        mock_df = pd.DataFrame({"id": [1, 2]})

        with patch.object(conn, "read_sql", return_value=mock_df) as mock_read_sql:
            result = conn.read_table("users", schema=None)

            assert len(result) == 2
            mock_read_sql.assert_called_once_with("SELECT * FROM [users]")


class TestWriteTable:
    """Tests for write_table method."""

    def test_write_table_success(self):
        """Test successful table write."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        mock_engine = Mock()
        conn._engine = mock_engine

        with patch.object(df, "to_sql", return_value=2) as mock_to_sql:
            result = conn.write_table(df, "users", schema="dbo", if_exists="replace")

            assert result == 2
            mock_to_sql.assert_called_once_with(
                name="users",
                con=mock_engine,
                schema="dbo",
                if_exists="replace",
                index=False,
                chunksize=1000,
                method="multi",
            )

    def test_write_table_with_custom_chunksize(self):
        """Test table write with custom chunksize."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        df = pd.DataFrame({"id": [1, 2]})
        mock_engine = Mock()
        conn._engine = mock_engine

        with patch.object(df, "to_sql", return_value=None) as mock_to_sql:
            result = conn.write_table(df, "users", chunksize=500)

            # If to_sql returns None, should return len(df)
            assert result == 2
            mock_to_sql.assert_called_once()
            assert mock_to_sql.call_args[1]["chunksize"] == 500

    def test_write_table_error(self):
        """Test table write error."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        df = pd.DataFrame({"id": [1, 2]})
        mock_engine = Mock()
        conn._engine = mock_engine

        with patch.object(df, "to_sql", side_effect=Exception("Write failed")):
            with pytest.raises(ConnectionError, match="Write operation failed"):
                conn.write_table(df, "users")


class TestExecute:
    """Tests for execute and execute_sql methods."""

    def test_execute_success(self):
        """Test successful SQL statement execution."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.returns_rows = False

        mock_connection.execute.return_value = mock_result
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        mock_engine.begin.return_value = mock_connection

        conn._engine = mock_engine

        # Mock the text function from sqlalchemy
        import sys
        from types import ModuleType

        sqlalchemy_mod = ModuleType("sqlalchemy")
        sqlalchemy_mod.text = MagicMock(return_value="INSERT INTO users VALUES (1, 'Alice')")
        sys.modules["sqlalchemy"] = sqlalchemy_mod

        try:
            result = conn.execute("INSERT INTO users VALUES (1, 'Alice')")

            assert result is None  # Non-SELECT returns None
            mock_connection.execute.assert_called_once()
        finally:
            if "sqlalchemy" in sys.modules:
                del sys.modules["sqlalchemy"]

    def test_execute_with_params(self):
        """Test SQL statement execution with parameters."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.returns_rows = False

        mock_connection.execute.return_value = mock_result
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        mock_engine.begin.return_value = mock_connection

        conn._engine = mock_engine

        import sys
        from types import ModuleType

        sqlalchemy_mod = ModuleType("sqlalchemy")
        sqlalchemy_mod.text = MagicMock(return_value="UPDATE users SET name = :name WHERE id = :id")
        sys.modules["sqlalchemy"] = sqlalchemy_mod

        try:
            conn.execute(
                "UPDATE users SET name = :name WHERE id = :id",
                params={"name": "Alice", "id": 1},
            )

            mock_connection.execute.assert_called_once()
            call_args = mock_connection.execute.call_args
            assert call_args[0][1] == {"name": "Alice", "id": 1}
        finally:
            if "sqlalchemy" in sys.modules:
                del sys.modules["sqlalchemy"]

    def test_execute_with_results(self):
        """Test SQL statement that returns rows."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        mock_engine = Mock()
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.returns_rows = True
        mock_result.fetchall.return_value = [(1, "Alice"), (2, "Bob")]

        mock_connection.execute.return_value = mock_result
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        mock_engine.begin.return_value = mock_connection

        conn._engine = mock_engine

        import sys
        from types import ModuleType

        sqlalchemy_mod = ModuleType("sqlalchemy")
        sqlalchemy_mod.text = MagicMock(return_value="SELECT * FROM users")
        sys.modules["sqlalchemy"] = sqlalchemy_mod

        try:
            result = conn.execute("SELECT * FROM users")

            assert result == [(1, "Alice"), (2, "Bob")]
            mock_result.fetchall.assert_called_once()
        finally:
            if "sqlalchemy" in sys.modules:
                del sys.modules["sqlalchemy"]

    def test_execute_error(self):
        """Test SQL statement execution error."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        mock_engine = Mock()
        mock_connection = Mock()
        mock_connection.execute.side_effect = Exception("Execution failed")
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        mock_engine.begin.return_value = mock_connection

        conn._engine = mock_engine

        import sys
        from types import ModuleType

        sqlalchemy_mod = ModuleType("sqlalchemy")
        sqlalchemy_mod.text = MagicMock(return_value="INVALID SQL")
        sys.modules["sqlalchemy"] = sqlalchemy_mod

        try:
            with pytest.raises(ConnectionError, match="Statement execution failed"):
                conn.execute("INVALID SQL")
        finally:
            if "sqlalchemy" in sys.modules:
                del sys.modules["sqlalchemy"]

    def test_execute_sql_is_alias(self):
        """Test that execute_sql is an alias for execute."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        with patch.object(conn, "execute", return_value=None) as mock_execute:
            conn.execute_sql("DELETE FROM users", params={"id": 1})

            mock_execute.assert_called_once_with("DELETE FROM users", {"id": 1})


class TestClose:
    """Tests for close method."""

    def test_close_disposes_engine(self):
        """Test that close disposes of the engine."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        mock_engine = Mock()
        conn._engine = mock_engine

        conn.close()

        mock_engine.dispose.assert_called_once()
        assert conn._engine is None

    def test_close_without_engine(self):
        """Test that close works when no engine exists."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        # Should not raise
        conn.close()


class TestGetSparkOptions:
    """Tests for get_spark_options method."""

    def test_get_spark_options_with_aad_msi(self):
        """Test Spark options for AAD Managed Identity."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            auth_mode="aad_msi",
            port=1433,
        )

        options = conn.get_spark_options()

        assert "url" in options
        assert "driver" in options
        assert options["driver"] == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        assert "jdbc:sqlserver://test.database.windows.net:1433" in options["url"]
        assert "databaseName=testdb" in options["url"]
        assert "encrypt=true" in options["url"]
        assert "authentication=ActiveDirectoryMsi" in options["url"]
        assert "user" not in options
        assert "password" not in options

    def test_get_spark_options_with_sql_auth(self):
        """Test Spark options for SQL authentication."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            username="testuser",
            password="testpass",
            auth_mode="sql",
        )

        options = conn.get_spark_options()

        assert "url" in options
        assert "user" in options
        assert "password" in options
        assert options["user"] == "testuser"
        assert options["password"] == "testpass"
        assert "authentication=ActiveDirectoryMsi" not in options["url"]

    def test_get_spark_options_with_key_vault(self):
        """Test Spark options with Key Vault password."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            username="testuser",
            auth_mode="key_vault",
            key_vault_name="myvault",
            secret_name="mysecret",
        )

        conn._cached_key = "vault_password"

        options = conn.get_spark_options()

        assert options["user"] == "testuser"
        assert options["password"] == "vault_password"

    def test_get_spark_options_with_custom_port(self):
        """Test Spark options with custom port."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
            port=1234,
        )

        options = conn.get_spark_options()

        assert "jdbc:sqlserver://test.database.windows.net:1234" in options["url"]


class TestGetPath:
    """Tests for get_path method."""

    def test_get_path_returns_relative_path(self):
        """Test that get_path returns the relative path as-is."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        assert conn.get_path("users") == "users"
        assert conn.get_path("dbo.users") == "dbo.users"


class TestReadSqlQuery:
    """Tests for read_sql_query method."""

    def test_read_sql_query_is_alias(self):
        """Test that read_sql_query is an alias for read_sql."""
        conn = AzureSQL(
            server="test.database.windows.net",
            database="testdb",
        )

        mock_df = pd.DataFrame({"id": [1]})

        with patch.object(conn, "read_sql", return_value=mock_df) as mock_read_sql:
            result = conn.read_sql_query("SELECT * FROM users", params={"id": 1})

            assert len(result) == 1
            mock_read_sql.assert_called_once_with("SELECT * FROM users", {"id": 1})
