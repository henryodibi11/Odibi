"""
Tests for odibi.story.lineage_utils
====================================
Covers: get_full_stories_path, get_storage_options, get_write_file, generate_lineage
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helper: build a fake ProjectConfig with SimpleNamespace
# ---------------------------------------------------------------------------


def _make_config(story_path="stories/", conn_name="my_conn", conn_type="local", **conn_kwargs):
    """Build a minimal ProjectConfig-like SimpleNamespace."""
    conn = SimpleNamespace(type=SimpleNamespace(value=conn_type), **conn_kwargs)
    story = SimpleNamespace(path=story_path, connection=conn_name)
    return SimpleNamespace(story=story, connections={conn_name: conn})


def _make_config_no_conn(story_path="stories/", conn_name="missing"):
    """Config where the connection name doesn't match any connection."""
    story = SimpleNamespace(path=story_path, connection=conn_name)
    return SimpleNamespace(story=story, connections={})


def _make_config_no_type(story_path="stories/", conn_name="my_conn"):
    """Config where the connection has no type attribute."""
    conn = SimpleNamespace()  # no 'type'
    story = SimpleNamespace(path=story_path, connection=conn_name)
    return SimpleNamespace(story=story, connections={conn_name: conn})


# ===========================================================================
# get_full_stories_path
# ===========================================================================


class TestGetFullStoriesPath:
    def test_already_full_url(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(story_path="abfs://container@acct.dfs.core.windows.net/stories")
        assert get_full_stories_path(cfg) == "abfs://container@acct.dfs.core.windows.net/stories"

    def test_azure_blob_connection(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="azure_blob", account_name="myacct", container="mycontainer")
        result = get_full_stories_path(cfg)
        assert result == "abfs://mycontainer@myacct.dfs.core.windows.net/stories"

    def test_delta_connection(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="delta", account_name="acct", container="ctr")
        result = get_full_stories_path(cfg)
        assert result == "abfs://ctr@acct.dfs.core.windows.net/stories"

    def test_azure_blob_missing_account(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="azure_blob", container="ctr")
        assert get_full_stories_path(cfg) == "stories/"

    def test_s3_connection(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="s3", bucket="mybucket")
        assert get_full_stories_path(cfg) == "s3://mybucket/stories"

    def test_aws_s3_connection(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="aws_s3", bucket="mybucket")
        assert get_full_stories_path(cfg) == "s3://mybucket/stories"

    def test_s3_no_bucket(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="s3")
        assert get_full_stories_path(cfg) == "stories/"

    def test_gcs_connection(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="gcs", bucket="gcsbucket")
        assert get_full_stories_path(cfg) == "gs://gcsbucket/stories"

    def test_google_cloud_storage_connection(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="google_cloud_storage", bucket="gcsbucket")
        assert get_full_stories_path(cfg) == "gs://gcsbucket/stories"

    def test_gcs_no_bucket(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="gcs")
        assert get_full_stories_path(cfg) == "stories/"

    def test_hdfs_connection(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="hdfs", host="namenode.example.com", port=9000)
        assert get_full_stories_path(cfg) == "hdfs://namenode.example.com:9000/stories"

    def test_hdfs_default_port(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="hdfs", host="namenode.example.com")
        assert get_full_stories_path(cfg) == "hdfs://namenode.example.com:8020/stories"

    def test_hdfs_no_host(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="hdfs")
        assert get_full_stories_path(cfg) == "stories/"

    def test_dbfs_connection(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="dbfs")
        assert get_full_stories_path(cfg) == "dbfs:/stories"

    def test_local_with_base_path(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="local", base_path="/data/project")
        result = get_full_stories_path(cfg)
        # Path joins base_path with clean_path
        assert "stories" in result
        assert "data" in result

    def test_local_no_base_path(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="local")
        assert get_full_stories_path(cfg) == "stories/"

    def test_no_connection_found(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config_no_conn()
        assert get_full_stories_path(cfg) == "stories/"

    def test_no_conn_type(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config_no_type()
        assert get_full_stories_path(cfg) == "stories/"

    def test_conn_type_as_plain_string(self):
        """conn.type is a plain string (no .value)."""
        from odibi.story.lineage_utils import get_full_stories_path

        conn = SimpleNamespace(type="dbfs")
        story = SimpleNamespace(path="stories/", connection="c")
        cfg = SimpleNamespace(story=story, connections={"c": conn})
        assert get_full_stories_path(cfg) == "dbfs:/stories"

    def test_unknown_conn_type_returns_raw_path(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(conn_type="ftp")
        assert get_full_stories_path(cfg) == "stories/"

    def test_strips_leading_trailing_slashes(self):
        from odibi.story.lineage_utils import get_full_stories_path

        cfg = _make_config(story_path="/stories/dir/", conn_type="dbfs")
        assert get_full_stories_path(cfg) == "dbfs:/stories/dir"


# ===========================================================================
# get_storage_options
# ===========================================================================


class TestGetStorageOptions:
    def test_no_connection(self):
        from odibi.story.lineage_utils import get_storage_options

        cfg = _make_config_no_conn()
        assert get_storage_options(cfg) == {}

    def test_direct_credentials(self):
        from odibi.story.lineage_utils import get_storage_options

        cfg = _make_config(credentials={"account_key": "abc", "sas": "xyz"})
        result = get_storage_options(cfg)
        assert result == {"account_key": "abc", "sas": "xyz"}

    def test_direct_account_key(self):
        from odibi.story.lineage_utils import get_storage_options

        cfg = _make_config(account_key="mykey123")
        assert get_storage_options(cfg) == {"account_key": "mykey123"}

    def test_direct_sas_token(self):
        from odibi.story.lineage_utils import get_storage_options

        cfg = _make_config(sas_token="mysas")
        assert get_storage_options(cfg) == {"sas_token": "mysas"}

    def test_nested_auth_account_key(self):
        from odibi.story.lineage_utils import get_storage_options

        auth = SimpleNamespace(
            mode=None, account_key="nestedkey", sas_token=None, connection_string=None
        )
        cfg = _make_config(auth=auth)
        assert get_storage_options(cfg) == {"account_key": "nestedkey"}

    def test_nested_auth_sas_token(self):
        from odibi.story.lineage_utils import get_storage_options

        auth = SimpleNamespace(
            mode=None, account_key=None, sas_token="nestedsas", connection_string=None
        )
        cfg = _make_config(auth=auth)
        assert get_storage_options(cfg) == {"sas_token": "nestedsas"}

    def test_nested_auth_connection_string(self):
        from odibi.story.lineage_utils import get_storage_options

        auth = SimpleNamespace(
            mode=None, account_key=None, sas_token=None, connection_string="DefaultEndpoints..."
        )
        cfg = _make_config(auth=auth)
        assert get_storage_options(cfg) == {"connection_string": "DefaultEndpoints..."}

    def test_nested_auth_dict_account_key(self):
        from odibi.story.lineage_utils import get_storage_options

        auth = {
            "mode": None,
            "account_key": "dictkey",
            "sas_token": None,
            "connection_string": None,
        }
        cfg = _make_config(auth=auth)
        assert get_storage_options(cfg) == {"account_key": "dictkey"}

    def test_nested_auth_dict_sas_token(self):
        from odibi.story.lineage_utils import get_storage_options

        auth = {"account_key": None, "sas_token": "dictsas"}
        cfg = _make_config(auth=auth)
        assert get_storage_options(cfg) == {"sas_token": "dictsas"}

    def test_nested_auth_dict_connection_string(self):
        from odibi.story.lineage_utils import get_storage_options

        auth = {"account_key": None, "sas_token": None, "connection_string": "myconn"}
        cfg = _make_config(auth=auth)
        assert get_storage_options(cfg) == {"connection_string": "myconn"}

    def test_aad_msi_mode_with_account_name(self):
        from odibi.story.lineage_utils import get_storage_options

        auth = SimpleNamespace(
            mode=SimpleNamespace(value="aad_msi"),
            account_key=None,
            sas_token=None,
            connection_string=None,
        )
        cfg = _make_config(auth=auth, account_name="msistore")
        assert get_storage_options(cfg) == {"account_name": "msistore"}

    def test_managed_identity_mode_no_account(self):
        from odibi.story.lineage_utils import get_storage_options

        auth = SimpleNamespace(
            mode=SimpleNamespace(value="managed_identity"),
            account_key=None,
            sas_token=None,
            connection_string=None,
        )
        cfg = _make_config(auth=auth)
        assert get_storage_options(cfg) == {}

    def test_key_vault_mode_returns_empty(self):
        from odibi.story.lineage_utils import get_storage_options

        auth = SimpleNamespace(
            mode=SimpleNamespace(value="key_vault"),
            account_key=None,
            sas_token=None,
            connection_string=None,
        )
        cfg = _make_config(auth=auth)
        assert get_storage_options(cfg) == {}

    def test_auth_mode_plain_string(self):
        """mode is a plain string, not an enum with .value."""
        from odibi.story.lineage_utils import get_storage_options

        auth = SimpleNamespace(
            mode="aad_msi", account_key=None, sas_token=None, connection_string=None
        )
        cfg = _make_config(auth=auth, account_name="acctname")
        assert get_storage_options(cfg) == {"account_name": "acctname"}

    def test_no_auth_no_creds_returns_empty(self):
        from odibi.story.lineage_utils import get_storage_options

        cfg = _make_config()
        assert get_storage_options(cfg) == {}

    def test_empty_credentials_skipped(self):
        """credentials exists but is empty/falsy – falls through."""
        from odibi.story.lineage_utils import get_storage_options

        cfg = _make_config(credentials={})
        assert get_storage_options(cfg) == {}

    def test_empty_account_key_skipped(self):
        from odibi.story.lineage_utils import get_storage_options

        cfg = _make_config(account_key="")
        assert get_storage_options(cfg) == {}


# ===========================================================================
# get_write_file
# ===========================================================================


class TestGetWriteFile:
    def test_local_returns_callable(self):
        from odibi.story.lineage_utils import get_write_file

        cfg = _make_config(conn_type="local")
        fn = get_write_file(cfg)
        assert callable(fn)

    def test_local_callable_writes_file(self, tmp_path):
        from odibi.story.lineage_utils import get_write_file

        cfg = _make_config(conn_type="local")
        fn = get_write_file(cfg)
        target = str(tmp_path / "subdir" / "out.json")
        fn(target, '{"hello":"world"}')
        with open(target) as f:
            assert f.read() == '{"hello":"world"}'

    def test_azure_blob_returns_callable(self):
        from odibi.story.lineage_utils import get_write_file

        cfg = _make_config(
            conn_type="azure_blob", account_name="acct", container="ctr", account_key="key123"
        )
        fn = get_write_file(cfg)
        assert callable(fn)

    def test_azure_blob_no_storage_options_returns_none(self):
        from odibi.story.lineage_utils import get_write_file

        cfg = _make_config(conn_type="azure_blob", account_name="acct", container="ctr")
        fn = get_write_file(cfg)
        assert fn is None

    def test_azure_blob_no_account_returns_none(self):
        from odibi.story.lineage_utils import get_write_file

        cfg = _make_config(conn_type="azure_blob", container="ctr", account_key="key")
        fn = get_write_file(cfg)
        assert fn is None

    def test_azure_blob_no_container_returns_none(self):
        from odibi.story.lineage_utils import get_write_file

        cfg = _make_config(conn_type="azure_blob", account_name="acct", account_key="key")
        fn = get_write_file(cfg)
        assert fn is None

    def test_s3_returns_callable(self):
        from odibi.story.lineage_utils import get_write_file

        cfg = _make_config(conn_type="s3", bucket="mybucket")
        fn = get_write_file(cfg)
        assert callable(fn)

    def test_s3_no_bucket_returns_none(self):
        from odibi.story.lineage_utils import get_write_file

        cfg = _make_config(conn_type="s3")
        fn = get_write_file(cfg)
        assert fn is None

    def test_gcs_returns_callable(self):
        from odibi.story.lineage_utils import get_write_file

        cfg = _make_config(conn_type="gcs", bucket="gcsbkt")
        fn = get_write_file(cfg)
        assert callable(fn)

    def test_gcs_no_bucket_returns_none(self):
        from odibi.story.lineage_utils import get_write_file

        cfg = _make_config(conn_type="gcs")
        fn = get_write_file(cfg)
        assert fn is None

    def test_no_connection_returns_none(self):
        from odibi.story.lineage_utils import get_write_file

        cfg = _make_config_no_conn()
        assert get_write_file(cfg) is None

    def test_no_conn_type_returns_none(self):
        from odibi.story.lineage_utils import get_write_file

        cfg = _make_config_no_type()
        assert get_write_file(cfg) is None

    def test_unknown_conn_type_returns_none(self):
        from odibi.story.lineage_utils import get_write_file

        cfg = _make_config(conn_type="ftp")
        assert get_write_file(cfg) is None

    def test_delta_returns_callable(self):
        from odibi.story.lineage_utils import get_write_file

        cfg = _make_config(conn_type="delta", account_name="acct", container="ctr", account_key="k")
        fn = get_write_file(cfg)
        assert callable(fn)

    def test_conn_type_plain_string_no_value(self):
        """conn.type is a plain string (no .value attribute)."""
        from odibi.story.lineage_utils import get_write_file

        conn = SimpleNamespace(type="local")
        story = SimpleNamespace(path="stories/", connection="c")
        cfg = SimpleNamespace(story=story, connections={"c": conn})
        fn = get_write_file(cfg)
        assert callable(fn)


# ===========================================================================
# get_write_file — closure body tests (mock fsspec)
# ===========================================================================


class TestWriteFileClosures:
    @patch("fsspec.filesystem")
    def test_azure_closure_with_abfs_prefix(self, mock_fs_factory):
        from odibi.story.lineage_utils import get_write_file

        mock_fs = MagicMock()
        mock_fs_factory.return_value = mock_fs
        cfg = _make_config(
            conn_type="azure_blob", account_name="acct", container="ctr", account_key="k"
        )
        fn = get_write_file(cfg)
        fn("abfs://ctr@acct.dfs.core.windows.net/file.json", "content")
        mock_fs_factory.assert_called_once()
        mock_fs.open.assert_called_once()

    @patch("fsspec.filesystem")
    def test_azure_closure_without_prefix(self, mock_fs_factory):
        from odibi.story.lineage_utils import get_write_file

        mock_fs = MagicMock()
        mock_fs_factory.return_value = mock_fs
        cfg = _make_config(
            conn_type="azure_blob", account_name="acct", container="ctr", account_key="k"
        )
        fn = get_write_file(cfg)
        fn("some/relative/path.json", "data")
        call_args = mock_fs.open.call_args[0][0]
        assert call_args.startswith("abfs://")

    @patch("fsspec.filesystem")
    def test_s3_closure_with_prefix(self, mock_fs_factory):
        from odibi.story.lineage_utils import get_write_file

        mock_fs = MagicMock()
        mock_fs_factory.return_value = mock_fs
        cfg = _make_config(conn_type="s3", bucket="mybucket")
        fn = get_write_file(cfg)
        fn("s3://mybucket/path.json", "data")
        mock_fs.open.assert_called_once()
        call_args = mock_fs.open.call_args[0][0]
        assert call_args == "s3://mybucket/path.json"

    @patch("fsspec.filesystem")
    def test_s3_closure_without_prefix(self, mock_fs_factory):
        from odibi.story.lineage_utils import get_write_file

        mock_fs = MagicMock()
        mock_fs_factory.return_value = mock_fs
        cfg = _make_config(conn_type="s3", bucket="mybucket")
        fn = get_write_file(cfg)
        fn("relative/path.json", "data")
        call_args = mock_fs.open.call_args[0][0]
        assert call_args == "s3://mybucket/relative/path.json"

    @patch("fsspec.filesystem")
    def test_gcs_closure_with_prefix(self, mock_fs_factory):
        from odibi.story.lineage_utils import get_write_file

        mock_fs = MagicMock()
        mock_fs_factory.return_value = mock_fs
        cfg = _make_config(conn_type="gcs", bucket="gcsbkt")
        fn = get_write_file(cfg)
        fn("gs://gcsbkt/path.json", "data")
        call_args = mock_fs.open.call_args[0][0]
        assert call_args == "gs://gcsbkt/path.json"

    @patch("fsspec.filesystem")
    def test_gcs_closure_without_prefix(self, mock_fs_factory):
        from odibi.story.lineage_utils import get_write_file

        mock_fs = MagicMock()
        mock_fs_factory.return_value = mock_fs
        cfg = _make_config(conn_type="gcs", bucket="gcsbkt")
        fn = get_write_file(cfg)
        fn("relative/path.json", "data")
        call_args = mock_fs.open.call_args[0][0]
        assert call_args == "gs://gcsbkt/relative/path.json"

    @patch("fsspec.filesystem")
    def test_azure_closure_az_prefix(self, mock_fs_factory):
        from odibi.story.lineage_utils import get_write_file

        mock_fs = MagicMock()
        mock_fs_factory.return_value = mock_fs
        cfg = _make_config(
            conn_type="azure_blob", account_name="acct", container="ctr", account_key="k"
        )
        fn = get_write_file(cfg)
        fn("az://ctr/file.json", "content")
        call_args = mock_fs.open.call_args[0][0]
        assert call_args == "az://ctr/file.json"


# ===========================================================================
# generate_lineage
# ===========================================================================


class TestGenerateLineage:
    @patch("odibi.story.lineage_utils.LineageGenerator")
    def test_success_returns_result(self, MockLG):
        from odibi.story.lineage_utils import generate_lineage

        mock_result = MagicMock()
        mock_result.nodes = [1, 2, 3]
        mock_result.edges = [1]
        mock_result.layers = [1, 2]
        MockLG.return_value.generate.return_value = mock_result

        cfg = _make_config(conn_type="local")
        result = generate_lineage(cfg, date="2025-01-01")
        assert result is mock_result
        MockLG.return_value.save.assert_called_once()

    @patch("odibi.story.lineage_utils.LineageGenerator")
    def test_exception_returns_none(self, MockLG):
        from odibi.story.lineage_utils import generate_lineage

        MockLG.return_value.generate.side_effect = RuntimeError("boom")

        cfg = _make_config(conn_type="local")
        result = generate_lineage(cfg)
        assert result is None

    @patch("odibi.story.lineage_utils.LineageGenerator")
    def test_write_file_auto_created(self, MockLG):
        from odibi.story.lineage_utils import generate_lineage

        mock_result = MagicMock()
        mock_result.nodes = []
        mock_result.edges = []
        mock_result.layers = []
        MockLG.return_value.generate.return_value = mock_result

        cfg = _make_config(conn_type="local")
        result = generate_lineage(cfg, write_file=None)
        assert result is mock_result

    @patch("odibi.story.lineage_utils.LineageGenerator")
    def test_explicit_write_file_passed_through(self, MockLG):
        from odibi.story.lineage_utils import generate_lineage

        mock_result = MagicMock()
        mock_result.nodes = []
        mock_result.edges = []
        mock_result.layers = []
        MockLG.return_value.generate.return_value = mock_result

        custom_writer = MagicMock()
        cfg = _make_config(conn_type="local")
        generate_lineage(cfg, write_file=custom_writer)
        MockLG.return_value.save.assert_called_once_with(mock_result, write_file=custom_writer)
