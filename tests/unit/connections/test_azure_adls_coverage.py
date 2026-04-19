"""Additional coverage tests for azure_adls.py (~251 missed statements).

Covers: _get_fs, list_files, list_folders, discover_catalog, get_schema,
profile, preview, detect_partitions, get_freshness, get_storage_key edge cases,
validate edge cases.
"""

import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

_mock_ctx = MagicMock()


@pytest.fixture(autouse=True)
def _patch_logging():
    with patch("odibi.connections.azure_adls.get_logging_context", return_value=_mock_ctx):
        with patch("odibi.connections.azure_adls.logger"):
            yield


from odibi.connections.azure_adls import AzureADLS  # noqa: E402


def _make_conn(**overrides):
    defaults = dict(
        account="testacct",
        container="testcont",
        auth_mode="direct_key",
        account_key="testkey",
        validate=False,
    )
    defaults.update(overrides)
    return AzureADLS(**defaults)


# ===========================================================================
# 1. get_storage_key edge cases
# ===========================================================================
class TestGetStorageKeyEdgeCases:
    def test_cached_key_returned(self):
        conn = _make_conn()
        conn._cached_storage_key = "cached123"
        result = conn.get_storage_key()
        assert result == "cached123"

    def test_direct_key_returns_account_key(self):
        conn = _make_conn(account_key="mykey")
        result = conn.get_storage_key()
        assert result == "mykey"

    def test_sas_token_returns_sas(self):
        conn = _make_conn(auth_mode="sas_token", sas_token="?sv=2020", account_key=None)
        result = conn.get_storage_key()
        assert result == "?sv=2020"

    def test_sas_token_cached(self):
        conn = _make_conn(auth_mode="sas_token", sas_token="?sv=2020", account_key=None)
        conn._cached_storage_key = "cached_sas"
        result = conn.get_storage_key()
        assert result == "cached_sas"

    def test_service_principal_returns_none(self):
        conn = _make_conn(
            auth_mode="service_principal",
            tenant_id="t",
            client_id="c",
            client_secret="s",
            account_key=None,
        )
        result = conn.get_storage_key()
        assert result is None

    def test_managed_identity_returns_none(self):
        conn = _make_conn(auth_mode="managed_identity", account_key=None)
        result = conn.get_storage_key()
        assert result is None

    def test_key_vault_import_error(self):
        conn = _make_conn(
            auth_mode="key_vault",
            key_vault_name="vault",
            secret_name="secret",
            account_key=None,
        )
        # Simulate azure not installed by making import fail
        with patch.dict(sys.modules, {"azure.identity": None, "azure.keyvault.secrets": None}):
            with pytest.raises(ImportError, match="azure-identity"):
                conn.get_storage_key()

    def test_key_vault_success(self):
        conn = _make_conn(
            auth_mode="key_vault",
            key_vault_name="vault",
            secret_name="secret",
            account_key=None,
        )
        mock_secret = MagicMock()
        mock_secret.value = "fetched_key_123"
        mock_client = MagicMock()
        mock_client.get_secret.return_value = mock_secret

        mock_identity_mod = MagicMock()
        mock_kv_mod = MagicMock()
        mock_kv_mod.SecretClient.return_value = mock_client

        with patch.dict(
            sys.modules,
            {
                "azure": MagicMock(),
                "azure.identity": mock_identity_mod,
                "azure.keyvault": MagicMock(),
                "azure.keyvault.secrets": mock_kv_mod,
            },
        ):
            result = conn.get_storage_key(timeout=5)
        assert result == "fetched_key_123"
        assert conn._cached_storage_key == "fetched_key_123"

    def test_key_vault_timeout(self):
        conn = _make_conn(
            auth_mode="key_vault",
            key_vault_name="vault",
            secret_name="secret",
            account_key=None,
        )

        mock_identity_mod = MagicMock()
        mock_kv_mod = MagicMock()
        # Make the client hang
        mock_client = MagicMock()
        import time

        def slow_get(name):
            time.sleep(10)

        mock_client.get_secret = slow_get
        mock_kv_mod.SecretClient.return_value = mock_client

        with patch.dict(
            sys.modules,
            {
                "azure": MagicMock(),
                "azure.identity": mock_identity_mod,
                "azure.keyvault": MagicMock(),
                "azure.keyvault.secrets": mock_kv_mod,
            },
        ):
            with pytest.raises(TimeoutError, match="timed out"):
                conn.get_storage_key(timeout=0.1)


# ===========================================================================
# 2. get_client_secret
# ===========================================================================
class TestGetClientSecret:
    def test_cached_secret(self):
        conn = _make_conn()
        conn._cached_client_secret = "cached_cs"
        assert conn.get_client_secret() == "cached_cs"

    def test_literal_secret(self):
        conn = _make_conn(client_secret="literal")
        assert conn.get_client_secret() == "literal"

    def test_no_secret(self):
        conn = _make_conn()
        assert conn.get_client_secret() is None


# ===========================================================================
# 3. validate edge cases
# ===========================================================================
class TestValidateEdgeCases:
    def test_unsupported_auth_mode(self):
        conn = _make_conn(auth_mode="unknown_mode")
        with pytest.raises(ValueError, match="Unsupported auth_mode"):
            conn.validate()

    def test_direct_key_production_warning(self):
        conn = _make_conn()
        with patch.dict(os.environ, {"ODIBI_ENV": "production"}):
            with pytest.warns(UserWarning, match="direct_key in production"):
                conn.validate()

    def test_sas_token_no_token_no_kv(self):
        conn = _make_conn(auth_mode="sas_token", sas_token=None, account_key=None)
        with pytest.raises(ValueError, match="sas_token mode requires"):
            conn.validate()

    def test_sp_missing_tenant_client(self):
        conn = _make_conn(
            auth_mode="service_principal",
            tenant_id=None,
            client_id=None,
            account_key=None,
        )
        with pytest.raises(ValueError, match="tenant_id.*client_id"):
            conn.validate()

    def test_sp_missing_secret_no_kv(self):
        conn = _make_conn(
            auth_mode="service_principal",
            tenant_id="t",
            client_id="c",
            client_secret=None,
            account_key=None,
        )
        with pytest.raises(ValueError, match="client_secret"):
            conn.validate()


# ===========================================================================
# 4. _get_fs
# ===========================================================================
class TestGetFs:
    def test_import_error(self):
        conn = _make_conn()
        with patch.dict(sys.modules, {"adlfs": None}):
            with pytest.raises(ImportError, match="adlfs"):
                conn._get_fs()

    def test_success(self):
        conn = _make_conn()
        mock_adlfs = MagicMock()
        mock_fs = MagicMock()
        mock_adlfs.AzureBlobFileSystem.return_value = mock_fs
        with patch.dict(sys.modules, {"adlfs": mock_adlfs}):
            result = conn._get_fs()
        assert result == mock_fs


# ===========================================================================
# 5. list_files
# ===========================================================================
class TestListFiles:
    def test_basic_listing(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [
            {
                "name": "cont/path/file1.csv",
                "type": "file",
                "size": 100,
                "last_modified": "2024-01-01",
            },
            {"name": "cont/path/dir1", "type": "directory"},
            {"name": "cont/path/file2.parquet", "type": "file", "size": 200},
        ]
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            result = conn.list_files()
        assert len(result) == 2
        assert result[0]["name"] == "file1.csv"
        assert result[1]["name"] == "file2.parquet"

    def test_pattern_filtering(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [
            {"name": "cont/a.csv", "type": "file"},
            {"name": "cont/b.json", "type": "file"},
        ]
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            result = conn.list_files(pattern="*.csv")
        assert len(result) == 1
        assert result[0]["name"] == "a.csv"

    def test_limit(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [{"name": f"cont/f{i}.csv", "type": "file"} for i in range(10)]
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            result = conn.list_files(limit=3)
        assert len(result) == 3

    def test_error_returns_empty(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs", side_effect=Exception("fail")):
            result = conn.list_files()
        assert result == []


# ===========================================================================
# 6. list_folders
# ===========================================================================
class TestListFolders:
    def test_basic_listing(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [
            {"name": "cont/dir1", "type": "directory"},
            {"name": "cont/dir2", "type": "directory"},
            {"name": "cont/file.csv", "type": "file"},
        ]
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            result = conn.list_folders()
        assert len(result) == 2

    def test_limit(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [{"name": f"cont/d{i}", "type": "directory"} for i in range(10)]
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            result = conn.list_folders(limit=2)
        assert len(result) == 2

    def test_error_returns_empty(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs", side_effect=Exception("fail")):
            result = conn.list_folders()
        assert result == []


# ===========================================================================
# 7. discover_catalog
# ===========================================================================
class TestDiscoverCatalog:
    def test_non_recursive(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [
            {"name": "cont/dir1", "type": "directory", "size": 0},
            {"name": "cont/file1.csv", "type": "file", "size": 100},
        ]
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="csv"):
                result = conn.discover_catalog(recursive=False)
        assert result["total_datasets"] == 2

    def test_recursive_walk(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.walk.return_value = [
            (
                "cont/testcont",
                {"sub": {"name": "cont/testcont/sub", "type": "directory"}},
                {"f.csv": {"name": "cont/testcont/f.csv", "type": "file", "size": 50}},
            ),
        ]
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value=None):
                result = conn.discover_catalog(recursive=True)
        assert result["total_datasets"] >= 1

    def test_pattern_filter(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [
            {"name": "cont/data.csv", "type": "file", "size": 10},
            {"name": "cont/data.json", "type": "file", "size": 20},
        ]
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            result = conn.discover_catalog(recursive=False, pattern="*.csv")
        file_names = [f["name"] for f in result.get("files", [])]
        assert "data.csv" in file_names
        assert "data.json" not in file_names

    def test_limit_enforcement(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [
            {"name": f"cont/f{i}.csv", "type": "file", "size": 10} for i in range(20)
        ]
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            result = conn.discover_catalog(recursive=False, limit=5)
        assert result["total_datasets"] <= 5

    def test_error_returns_summary(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs", side_effect=Exception("auth fail")):
            result = conn.discover_catalog()
        assert result["total_datasets"] == 0
        assert "Error" in result["next_step"]

    def test_with_path_scope(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.ls.return_value = []
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            result = conn.discover_catalog(recursive=False, path="subfolder")
        assert result["total_datasets"] == 0


# ===========================================================================
# 8. get_schema
# ===========================================================================
class TestGetSchema:
    def test_parquet(self):
        conn = _make_conn()
        df = pd.DataFrame({"a": pd.array([], dtype="int64"), "b": pd.array([], dtype="object")})
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="parquet"):
                with patch("pandas.read_parquet", return_value=df):
                    result = conn.get_schema("data.parquet")
        assert len(result["columns"]) == 2

    def test_csv(self):
        conn = _make_conn()
        df = pd.DataFrame({"x": pd.array([], dtype="float64")})
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="csv"):
                with patch("pandas.read_csv", return_value=df):
                    result = conn.get_schema("data.csv")
        assert len(result["columns"]) == 1

    def test_json(self):
        conn = _make_conn()
        df = pd.DataFrame({"j": pd.array([], dtype="object")})
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="json"):
                with patch("pandas.read_json", return_value=df):
                    result = conn.get_schema("data.json")
        assert len(result["columns"]) == 1

    def test_unsupported(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="avro"):
                result = conn.get_schema("data.avro")
        assert result["columns"] == []

    def test_error(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs", side_effect=Exception("fail")):
            result = conn.get_schema("data.parquet")
        assert result["columns"] == []


# ===========================================================================
# 9. profile
# ===========================================================================
class TestProfile:
    def _make_df(self):
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["a", "b", "c", "d", "e"],
                "cat": ["x", "x", "x", "x", "x"],
                "ts": pd.to_datetime(["2024-01-01"] * 5),
            }
        )

    def test_parquet_profiling(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="parquet"):
                with patch("pandas.read_parquet", return_value=self._make_df()):
                    result = conn.profile("data.parquet")
        assert result["rows_sampled"] == 5
        assert len(result["columns"]) == 4

    def test_csv_profiling(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="csv"):
                with patch("pandas.read_csv", return_value=self._make_df()):
                    result = conn.profile("data.csv")
        assert result["rows_sampled"] == 5

    def test_json_profiling(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="json"):
                with patch("pandas.read_json", return_value=self._make_df()):
                    result = conn.profile("data.json")
        assert result["rows_sampled"] == 5

    def test_unsupported_format(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="avro"):
                result = conn.profile("data.avro")
        assert result["rows_sampled"] == 0

    def test_error_path(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs", side_effect=Exception("fail")):
            result = conn.profile("data.parquet")
        assert result["rows_sampled"] == 0

    def test_candidate_keys_and_watermarks(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="parquet"):
                with patch("pandas.read_parquet", return_value=self._make_df()):
                    result = conn.profile("data.parquet")
        assert "id" in result["candidate_keys"]
        assert "ts" in result["candidate_watermarks"]

    def test_cardinality_low(self):
        conn = _make_conn()
        df = pd.DataFrame({"cat": ["x"] * 20})
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="parquet"):
                with patch("pandas.read_parquet", return_value=df):
                    result = conn.profile("data.parquet")
        col = result["columns"][0]
        assert col["cardinality"] == "low"

    def test_columns_filter(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="parquet"):
                with patch("pandas.read_parquet", return_value=self._make_df()):
                    result = conn.profile("data.parquet", columns=["id", "name"])
        col_names = [c["name"] for c in result["columns"]]
        assert col_names == ["id", "name"]

    def test_sample_rows_cap(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="parquet"):
                with patch("pandas.read_parquet", return_value=self._make_df()):
                    result = conn.profile("data.parquet", sample_rows=20000)
        # Should not fail; cap enforced at 10000 internally
        assert result["rows_sampled"] == 5


# ===========================================================================
# 10. preview
# ===========================================================================
class TestPreview:
    def test_parquet_preview(self):
        conn = _make_conn()
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="parquet"):
                with patch("pandas.read_parquet", return_value=df):
                    result = conn.preview("data.parquet")
        assert result["columns"] == ["a", "b"]
        assert len(result["rows"]) == 3

    def test_csv_preview(self):
        conn = _make_conn()
        df = pd.DataFrame({"x": [1]})
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="csv"):
                with patch("pandas.read_csv", return_value=df):
                    result = conn.preview("data.csv")
        assert result["columns"] == ["x"]

    def test_json_preview(self):
        conn = _make_conn()
        df = pd.DataFrame({"j": [1]})
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="json"):
                with patch("pandas.read_json", return_value=df):
                    result = conn.preview("data.json")
        assert result["columns"] == ["j"]

    def test_column_filtering(self):
        conn = _make_conn()
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="parquet"):
                with patch("pandas.read_parquet", return_value=df):
                    result = conn.preview("data.parquet", columns=["a", "c"])
        assert result["columns"] == ["a", "c"]

    def test_max_rows_cap(self):
        conn = _make_conn()
        df = pd.DataFrame({"a": list(range(200))})
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="parquet"):
                with patch("pandas.read_parquet", return_value=df):
                    result = conn.preview("data.parquet", rows=200)
        assert len(result["rows"]) <= 100

    def test_unsupported_format(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs"):
            with patch("odibi.connections.azure_adls.detect_file_format", return_value="avro"):
                result = conn.preview("data.avro")
        assert result.get("rows") is None or result.get("rows") == []

    def test_error_path(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs", side_effect=Exception("fail")):
            result = conn.preview("data.parquet")
        assert result.get("rows") is None or result.get("rows") == []


# ===========================================================================
# 11. detect_partitions
# ===========================================================================
class TestDetectPartitions:
    def test_basic(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [
            {"name": "cont/year=2024/m=01/f.parquet", "type": "file"},
        ]
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            with patch(
                "odibi.connections.azure_adls.detect_partitions",
                return_value={"keys": ["year", "m"], "example_values": {}, "format": "hive"},
            ):
                result = conn.detect_partitions()
        assert "keys" in result

    def test_error_path(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs", side_effect=Exception("fail")):
            result = conn.detect_partitions()
        assert result["keys"] == []

    def test_ls_exception_empty_paths(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.ls.side_effect = Exception("forbidden")
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            with patch(
                "odibi.connections.azure_adls.detect_partitions",
                return_value={"keys": [], "example_values": {}},
            ):
                result = conn.detect_partitions()
        assert result["keys"] == []


# ===========================================================================
# 12. get_freshness
# ===========================================================================
class TestGetFreshness:
    def test_string_last_modified(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.info.return_value = {"last_modified": "2024-01-01T00:00:00Z"}
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            result = conn.get_freshness("data.parquet")
        assert result["age_hours"] is not None
        assert result["age_hours"] > 0

    def test_datetime_last_modified(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.info.return_value = {"last_modified": datetime(2024, 1, 1, tzinfo=timezone.utc)}
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            result = conn.get_freshness("data.parquet")
        assert result["age_hours"] > 0

    def test_naive_datetime(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.info.return_value = {"last_modified": datetime(2024, 1, 1)}
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            result = conn.get_freshness("data.parquet")
        assert result["age_hours"] > 0

    def test_no_last_modified(self):
        conn = _make_conn()
        mock_fs = MagicMock()
        mock_fs.info.return_value = {}
        with patch.object(AzureADLS, "_get_fs", return_value=mock_fs):
            result = conn.get_freshness("data.parquet")
        assert result.get("age_hours") is None

    def test_error_path(self):
        conn = _make_conn()
        with patch.object(AzureADLS, "_get_fs", side_effect=Exception("fail")):
            result = conn.get_freshness("data.parquet")
        assert result.get("age_hours") is None
