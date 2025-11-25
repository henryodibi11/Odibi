import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Import PipelineManager - this is safe as it doesn't import AzureADLS at module level
from odibi.pipeline import PipelineManager


class TestPipelineManagerConnections:
    """Test connection building logic in PipelineManager."""

    def setup_method(self):
        # Create a mock for AzureADLS class
        self.mock_adls_class = MagicMock()
        # Create a mock module
        self.mock_adls_module = MagicMock()
        # Assign the mock class to the module
        self.mock_adls_module.AzureADLS = self.mock_adls_class

        # Patch the module in sys.modules so that 'from odibi.connections.azure_adls import AzureADLS'
        # inside the method returns our mock
        self.patcher = patch.dict(
            sys.modules, {"odibi.connections.azure_adls": self.mock_adls_module}
        )
        self.patcher.start()

    def teardown_method(self):
        self.patcher.stop()

    def test_build_local_connection(self):
        """Test building a local connection."""
        conn_configs = {"my_local": {"type": "local", "base_path": "/tmp/data"}}

        # We assume LocalConnection is available and works (it's a simple class)
        # If we wanted to be strict we could mock it too, but testing real one is fine for integration
        connections = PipelineManager._build_connections(conn_configs)

        assert "my_local" in connections
        conn = connections["my_local"]
        # Check attribute if possible, or just existence
        assert str(conn.base_path) == str(Path("/tmp/data"))

    def test_build_azure_adls_legacy_flat_config(self):
        """Test Azure ADLS with legacy flat config (no auth dict)."""
        conn_configs = {
            "my_adls": {
                "type": "azure_adls",
                "account_name": "myaccount",  # legacy name
                "container": "mycontainer",
                "key_vault_name": "kv-test",
                "secret_name": "secret-test",
            }
        }

        connections = PipelineManager._build_connections(conn_configs)

        assert "my_adls" in connections

        # Verify AzureADLS was initialized with correct params
        self.mock_adls_class.assert_called_with(
            account="myaccount",
            container="mycontainer",
            path_prefix="",
            auth_mode="key_vault",  # default
            key_vault_name="kv-test",
            secret_name="secret-test",
            account_key=None,
            sas_token=None,
            tenant_id=None,
            client_id=None,
            client_secret=None,
            validate=False,
        )

    def test_build_azure_adls_auth_dict(self):
        """Test Azure ADLS with auth dictionary (new structure)."""
        conn_configs = {
            "my_adls": {
                "type": "azure_adls",
                "account": "newaccount",  # new alias
                "container": "newcontainer",
                "auth": {"key_vault_name": "kv-new", "secret_name": "secret-new"},
            }
        }

        PipelineManager._build_connections(conn_configs)

        self.mock_adls_class.assert_called_with(
            account="newaccount",
            container="newcontainer",
            path_prefix="",
            auth_mode="key_vault",
            key_vault_name="kv-new",
            secret_name="secret-new",
            account_key=None,
            sas_token=None,
            tenant_id=None,
            client_id=None,
            client_secret=None,
            validate=False,
        )

    def test_build_azure_adls_direct_key(self):
        """Test Azure ADLS with direct key auth."""
        conn_configs = {
            "my_adls": {
                "type": "azure_adls",
                "account": "directaccount",
                "container": "data",
                "auth_mode": "direct_key",
                "auth": {"account_key": "fake-key-123"},
            }
        }

        PipelineManager._build_connections(conn_configs)

        self.mock_adls_class.assert_called_with(
            account="directaccount",
            container="data",
            path_prefix="",
            auth_mode="direct_key",
            key_vault_name=None,
            secret_name=None,
            account_key="fake-key-123",
            sas_token=None,
            tenant_id=None,
            client_id=None,
            client_secret=None,
            validate=False,
        )

    def test_build_azure_adls_path_prefix(self):
        """Test Azure ADLS with path prefix."""
        conn_configs = {
            "my_adls": {
                "type": "azure_adls",
                "account": "acc",
                "container": "cont",
                "path_prefix": "/mnt/data",
                "account_name": "acc",  # Should be ignored if account is present or just serve as backup
                "key_vault_name": "kv",
                "secret_name": "sec",
            }
        }

        PipelineManager._build_connections(conn_configs)

        self.mock_adls_class.assert_called_with(
            account="acc",
            container="cont",
            path_prefix="/mnt/data",
            auth_mode="key_vault",
            key_vault_name="kv",
            secret_name="sec",
            account_key=None,
            sas_token=None,
            tenant_id=None,
            client_id=None,
            client_secret=None,
            validate=False,
        )

    def test_build_azure_adls_service_principal(self):
        """Test Azure ADLS with service principal auth."""
        conn_configs = {
            "my_sp_adls": {
                "type": "azure_adls",
                "account": "spaccount",
                "container": "spdata",
                "auth_mode": "service_principal",
                "auth": {"tenant_id": "tid", "client_id": "cid", "client_secret": "csecret"},
            }
        }

        PipelineManager._build_connections(conn_configs)

        self.mock_adls_class.assert_called_with(
            account="spaccount",
            container="spdata",
            path_prefix="",
            auth_mode="service_principal",
            key_vault_name=None,
            secret_name=None,
            account_key=None,
            sas_token=None,
            tenant_id="tid",
            client_id="cid",
            client_secret="csecret",
            validate=False,
        )

    def test_missing_account_name_raises_error(self):
        """Test that missing account name raises ValueError."""
        conn_configs = {
            "bad_adls": {
                "type": "azure_adls",
                "container": "data",
                # Missing account
            }
        }

        with pytest.raises(ValueError, match="missing 'account_name'"):
            PipelineManager._build_connections(conn_configs)

    def test_unsupported_connection_type(self):
        """Test validation of unsupported connection types."""
        conn_configs = {"bad_conn": {"type": "ftp_server", "host": "localhost"}}

        with pytest.raises(ValueError, match="Unsupported connection type"):
            PipelineManager._build_connections(conn_configs)
