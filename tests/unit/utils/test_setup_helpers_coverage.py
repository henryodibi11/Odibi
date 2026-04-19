"""Comprehensive unit tests for odibi.utils.setup_helpers."""

import sys
from unittest.mock import MagicMock, patch


from odibi.utils.setup_helpers import (
    KeyVaultFetchResult,
    configure_connections_parallel,
    fetch_keyvault_secret,
    fetch_keyvault_secrets_parallel,
    validate_databricks_environment,
)


# ===================================================================
# 1. KeyVaultFetchResult dataclass
# ===================================================================


class TestKeyVaultFetchResult:
    def test_all_fields_defaults(self):
        r = KeyVaultFetchResult(connection_name="c1", account="acct1", success=True)
        assert r.connection_name == "c1"
        assert r.account == "acct1"
        assert r.success is True
        assert r.secret_value is None
        assert r.error is None
        assert r.duration_ms is None

    def test_all_fields_populated(self):
        err = RuntimeError("boom")
        r = KeyVaultFetchResult(
            connection_name="c2",
            account="acct2",
            success=False,
            secret_value="secret123",
            error=err,
            duration_ms=42.5,
        )
        assert r.connection_name == "c2"
        assert r.account == "acct2"
        assert r.success is False
        assert r.secret_value == "secret123"
        assert r.error is err
        assert r.duration_ms == 42.5


# ===================================================================
# 2. fetch_keyvault_secret()
# ===================================================================


class TestFetchKeyvaultSecret:
    def test_success_path(self):
        mock_identity = MagicMock()
        mock_kv = MagicMock()
        mock_secret = MagicMock()
        mock_secret.value = "my-secret-value"
        mock_kv.SecretClient.return_value.get_secret.return_value = mock_secret

        with patch.dict(
            "sys.modules",
            {
                "azure": MagicMock(),
                "azure.identity": mock_identity,
                "azure.keyvault": MagicMock(),
                "azure.keyvault.secrets": mock_kv,
            },
        ):
            # Need to ensure the from-import inside the function picks up our mocks
            result = fetch_keyvault_secret("conn1", "myvault", "mysecret")

        assert result.success is True
        assert result.secret_value == "my-secret-value"
        assert result.connection_name == "conn1"
        assert result.account == "myvault"
        assert result.duration_ms is not None

    def test_import_error_path(self):
        # Remove azure modules so the import fails
        saved = {}
        mods_to_remove = [k for k in sys.modules if k.startswith("azure")]
        for k in mods_to_remove:
            saved[k] = sys.modules.pop(k)

        try:
            with patch.dict(
                "sys.modules",
                {
                    "azure": None,
                    "azure.identity": None,
                    "azure.keyvault": None,
                    "azure.keyvault.secrets": None,
                },
            ):
                result = fetch_keyvault_secret("conn2", "vault2", "secret2")
        finally:
            sys.modules.update(saved)

        assert result.success is False
        assert isinstance(result.error, ImportError)
        assert result.duration_ms is not None

    def test_exception_path(self):
        mock_identity = MagicMock()
        mock_kv = MagicMock()
        mock_kv.SecretClient.return_value.get_secret.side_effect = RuntimeError("timeout")

        with patch.dict(
            "sys.modules",
            {
                "azure": MagicMock(),
                "azure.identity": mock_identity,
                "azure.keyvault": MagicMock(),
                "azure.keyvault.secrets": mock_kv,
            },
        ):
            result = fetch_keyvault_secret("conn3", "vault3", "secret3")

        assert result.success is False
        assert isinstance(result.error, RuntimeError)
        assert "timeout" in str(result.error)
        assert result.duration_ms is not None


# ===================================================================
# 3. fetch_keyvault_secrets_parallel()
# ===================================================================


class TestFetchKeyvaultSecretsParallel:
    def test_kv_connections_parallel_fetch(self):
        conn = MagicMock()
        conn.key_vault_name = "vault1"
        conn.secret_name = "secret1"
        conn.account = "acct1"

        fake_result = KeyVaultFetchResult(
            connection_name="c1",
            account="vault1",
            success=True,
            secret_value="val1",
            duration_ms=10.0,
        )

        with patch("odibi.utils.setup_helpers.fetch_keyvault_secret", return_value=fake_result):
            results = fetch_keyvault_secrets_parallel({"c1": conn}, verbose=False)

        assert "c1" in results
        assert results["c1"].success is True
        assert results["c1"].secret_value == "val1"

    def test_connections_without_kv_skipped(self):
        conn = MagicMock(spec=[])  # no key_vault_name or secret_name attributes
        results = fetch_keyvault_secrets_parallel({"c1": conn}, verbose=False)

        assert "c1" in results
        assert results["c1"].success is True
        assert results["c1"].secret_value is None

    def test_empty_connections_early_return(self):
        results = fetch_keyvault_secrets_parallel({}, verbose=True)
        assert results == {}

    def test_verbose_true_success(self):
        conn = MagicMock()
        conn.key_vault_name = "vault"
        conn.secret_name = "secret"

        fake_result = KeyVaultFetchResult(
            connection_name="c1",
            account="vault",
            success=True,
            secret_value="v",
            duration_ms=5.0,
        )

        with patch("odibi.utils.setup_helpers.fetch_keyvault_secret", return_value=fake_result):
            results = fetch_keyvault_secrets_parallel({"c1": conn}, verbose=True)

        assert results["c1"].success is True

    def test_verbose_true_failure(self):
        conn = MagicMock()
        conn.key_vault_name = "vault"
        conn.secret_name = "secret"

        fake_result = KeyVaultFetchResult(
            connection_name="c1",
            account="vault",
            success=False,
            error=RuntimeError("err"),
            duration_ms=5.0,
        )

        with patch("odibi.utils.setup_helpers.fetch_keyvault_secret", return_value=fake_result):
            results = fetch_keyvault_secrets_parallel({"c1": conn}, verbose=True)

        assert results["c1"].success is False

    def test_verbose_false_no_log(self):
        conn = MagicMock()
        conn.key_vault_name = "vault"
        conn.secret_name = "secret"

        fake_result = KeyVaultFetchResult(
            connection_name="c1",
            account="vault",
            success=True,
            secret_value="v",
            duration_ms=5.0,
        )

        with patch("odibi.utils.setup_helpers.fetch_keyvault_secret", return_value=fake_result):
            results = fetch_keyvault_secrets_parallel({"c1": conn}, verbose=False)

        assert results["c1"].success is True

    def test_no_kv_connections_verbose_true(self):
        conn = MagicMock(spec=[])
        results = fetch_keyvault_secrets_parallel({"c1": conn}, verbose=True)
        assert results["c1"].success is True


# ===================================================================
# 4. configure_connections_parallel()
# ===================================================================


class TestConfigureConnectionsParallel:
    def test_prefetch_false_returns_unchanged(self):
        conns = {"c1": MagicMock()}
        result_conns, errors = configure_connections_parallel(conns, prefetch_secrets=False)
        assert result_conns is conns
        assert errors == []

    def test_prefetch_true_success_cached_storage_key(self):
        conn = MagicMock()
        conn.key_vault_name = "vault"
        conn.secret_name = "secret"
        conn._cached_storage_key = None

        fake_result = KeyVaultFetchResult(
            connection_name="c1",
            account="vault",
            success=True,
            secret_value="the-key",
            duration_ms=5.0,
        )

        with patch(
            "odibi.utils.setup_helpers.fetch_keyvault_secrets_parallel",
            return_value={"c1": fake_result},
        ):
            result_conns, errors = configure_connections_parallel(
                {"c1": conn}, prefetch_secrets=True, verbose=False
            )

        assert errors == []
        assert conn._cached_storage_key == "the-key"

    def test_prefetch_true_success_cached_key(self):
        conn = MagicMock(spec=["_cached_key"])
        conn._cached_key = None

        fake_result = KeyVaultFetchResult(
            connection_name="c1",
            account="vault",
            success=True,
            secret_value="the-key",
            duration_ms=5.0,
        )

        with patch(
            "odibi.utils.setup_helpers.fetch_keyvault_secrets_parallel",
            return_value={"c1": fake_result},
        ):
            result_conns, errors = configure_connections_parallel(
                {"c1": conn}, prefetch_secrets=True, verbose=False
            )

        assert errors == []
        assert conn._cached_key == "the-key"

    def test_prefetch_true_failure_appends_error(self):
        conn = MagicMock()
        err = RuntimeError("kv-fail")
        fake_result = KeyVaultFetchResult(
            connection_name="c1",
            account="vault",
            success=False,
            error=err,
            duration_ms=5.0,
        )

        with patch(
            "odibi.utils.setup_helpers.fetch_keyvault_secrets_parallel",
            return_value={"c1": fake_result},
        ):
            result_conns, errors = configure_connections_parallel(
                {"c1": conn}, prefetch_secrets=True, verbose=True
            )

        assert len(errors) == 1
        assert "c1" in errors[0]

    def test_prefetch_true_failure_verbose_false(self):
        conn = MagicMock()
        fake_result = KeyVaultFetchResult(
            connection_name="c1",
            account="vault",
            success=False,
            error=RuntimeError("err"),
            duration_ms=5.0,
        )

        with patch(
            "odibi.utils.setup_helpers.fetch_keyvault_secrets_parallel",
            return_value={"c1": fake_result},
        ):
            result_conns, errors = configure_connections_parallel(
                {"c1": conn}, prefetch_secrets=True, verbose=False
            )

        assert len(errors) == 1


# ===================================================================
# 5. validate_databricks_environment()
# ===================================================================


class TestValidateDatabricksEnvironment:
    def test_not_in_databricks(self):
        with patch.dict("os.environ", {}, clear=True):
            with patch.dict("sys.modules", {"pyspark": None, "pyspark.sql": None}):
                result = validate_databricks_environment(verbose=False)

        assert result["is_databricks"] is False
        assert result["spark_available"] is False

    def test_with_databricks_runtime_env(self):
        with patch.dict("os.environ", {"DATABRICKS_RUNTIME_VERSION": "14.3"}):
            with patch.dict("sys.modules", {"pyspark": None, "pyspark.sql": None}):
                result = validate_databricks_environment(verbose=False)

        assert result["is_databricks"] is True
        assert result["runtime_version"] == "14.3"

    def test_spark_check_raises(self):
        with patch.dict("os.environ", {}, clear=True):
            mock_pyspark = MagicMock()
            mock_pyspark_sql = MagicMock()
            mock_pyspark_sql.SparkSession.getActiveSession.side_effect = RuntimeError("no spark")
            with patch.dict(
                "sys.modules", {"pyspark": mock_pyspark, "pyspark.sql": mock_pyspark_sql}
            ):
                result = validate_databricks_environment(verbose=False)

        assert any("Spark check failed" in e for e in result["errors"])

    def test_verbose_true_with_runtime(self):
        with patch.dict("os.environ", {"DATABRICKS_RUNTIME_VERSION": "13.0"}):
            with patch.dict("sys.modules", {"pyspark": None, "pyspark.sql": None}):
                result = validate_databricks_environment(verbose=True)

        assert result["is_databricks"] is True

    def test_verbose_false(self):
        with patch.dict("os.environ", {}, clear=True):
            with patch.dict("sys.modules", {"pyspark": None, "pyspark.sql": None}):
                result = validate_databricks_environment(verbose=False)

        assert result["is_databricks"] is False

    def test_verbose_true_with_errors(self):
        with patch.dict("os.environ", {}, clear=True):
            mock_pyspark = MagicMock()
            mock_pyspark_sql = MagicMock()
            mock_pyspark_sql.SparkSession.getActiveSession.side_effect = RuntimeError("boom")
            with patch.dict(
                "sys.modules", {"pyspark": mock_pyspark, "pyspark.sql": mock_pyspark_sql}
            ):
                result = validate_databricks_environment(verbose=True)

        assert len(result["errors"]) >= 1
