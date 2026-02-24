"""Unit tests for NodeExecutor catalog registration, content hash, and skip-if-unchanged.

Covers issue #286 — lines 2467-2825 of node.py:
  - _register_catalog_entries
  - _add_write_metadata
  - _check_skip_if_unchanged
  - _store_content_hash_after_write
  - _check_target_exists
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.config import NodeConfig, WriteConfig
from odibi.node import NodeExecutor


@pytest.fixture
def mock_context():
    return MagicMock()


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.name = "pandas"
    engine.materialize.side_effect = lambda df: df
    engine.count_rows.side_effect = lambda df: len(df) if df is not None else None
    return engine


@pytest.fixture
def connections():
    conn = MagicMock()
    conn.get_path.return_value = "/data/output"
    src = MagicMock()
    src.get_path.return_value = "/data/input"
    return {"src": src, "dst": conn}


def _make_executor(mock_context, mock_engine, connections, **kwargs):
    return NodeExecutor(mock_context, mock_engine, connections, **kwargs)


def _make_config(name="test_node", write_dict=None, **kwargs):
    write_dict = write_dict or {"connection": "dst", "format": "csv", "path": "out.csv"}
    return NodeConfig(
        name=name,
        read={"connection": "src", "format": "csv", "path": "input.csv"},
        write=write_dict,
        **kwargs,
    )


# ============================================================
# _register_catalog_entries
# ============================================================


class TestRegisterCatalogEntries:
    """Tests for _register_catalog_entries method."""

    def test_no_catalog_manager_returns_immediately(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=None)
        config = _make_config()
        write_config = config.write
        conn = connections["dst"]

        # Should return without error
        executor._register_catalog_entries(config, pd.DataFrame({"a": [1]}), conn, write_config)

    def test_registers_asset_with_correct_fields(self, mock_context, mock_engine, connections):
        catalog = MagicMock()
        mock_engine.get_schema.return_value = {"col1": "int64"}
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _make_config(write_dict={"connection": "dst", "format": "csv", "table": "my_tbl"})
        df = pd.DataFrame({"col1": [1, 2]})

        executor._register_catalog_entries(config, df, connections["dst"], config.write)

        catalog.register_asset.assert_called_once()
        call_kwargs = catalog.register_asset.call_args[1]
        assert call_kwargs["table_name"] == "my_tbl"
        assert call_kwargs["path"] == "/data/output"
        assert call_kwargs["format"] == "csv"
        assert call_kwargs["schema_hash"] != ""

    def test_project_config_provides_project_name(self, mock_context, mock_engine, connections):
        catalog = MagicMock()
        mock_engine.get_schema.return_value = {"c": "str"}
        proj = MagicMock()
        proj.project = "analytics"
        executor = _make_executor(
            mock_context,
            mock_engine,
            connections,
            catalog_manager=catalog,
            project_config=proj,
        )
        config = _make_config()
        executor._register_catalog_entries(
            config, pd.DataFrame({"c": ["x"]}), connections["dst"], config.write
        )

        call_kwargs = catalog.register_asset.call_args[1]
        assert call_kwargs["project_name"] == "analytics"

    def test_no_project_config_uses_unknown(self, mock_context, mock_engine, connections):
        catalog = MagicMock()
        mock_engine.get_schema.return_value = {"c": "str"}
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _make_config()
        executor._register_catalog_entries(
            config, pd.DataFrame({"c": ["x"]}), connections["dst"], config.write
        )

        call_kwargs = catalog.register_asset.call_args[1]
        assert call_kwargs["project_name"] == "unknown"

    def test_tracks_schema_when_df_and_table_path(self, mock_context, mock_engine, connections):
        catalog = MagicMock()
        mock_engine.get_schema.return_value = {"col": "int64"}
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _make_config()
        df = pd.DataFrame({"col": [1]})

        executor._register_catalog_entries(config, df, connections["dst"], config.write)

        catalog.track_schema.assert_called_once()
        call_kwargs = catalog.track_schema.call_args[1]
        assert call_kwargs["table_path"] == "/data/output"
        assert call_kwargs["schema"] == {"col": "int64"}
        assert call_kwargs["node"] == "test_node"

    def test_skips_schema_tracking_when_df_is_none(self, mock_context, mock_engine, connections):
        catalog = MagicMock()
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _make_config()

        executor._register_catalog_entries(config, None, connections["dst"], config.write)

        catalog.track_schema.assert_not_called()

    def test_logs_pattern_when_materialized_set(self, mock_context, mock_engine, connections):
        catalog = MagicMock()
        mock_engine.get_schema.return_value = {"c": "str"}
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _make_config(materialized="incremental")
        df = pd.DataFrame({"c": ["x"]})

        executor._register_catalog_entries(config, df, connections["dst"], config.write)

        catalog.log_pattern.assert_called_once()
        call_kwargs = catalog.log_pattern.call_args[1]
        assert call_kwargs["pattern_type"] == "incremental"

    def test_records_lineage_when_read_and_table_path(self, mock_context, mock_engine, connections):
        catalog = MagicMock()
        mock_engine.get_schema.return_value = {"c": "str"}
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _make_config()
        df = pd.DataFrame({"c": ["x"]})

        executor._register_catalog_entries(config, df, connections["dst"], config.write)

        catalog.record_lineage.assert_called_once()
        call_kwargs = catalog.record_lineage.call_args[1]
        assert call_kwargs["source_table"] == "/data/input"
        assert call_kwargs["target_table"] == "/data/output"
        assert call_kwargs["target_node"] == "test_node"

    def test_batch_mode_buffers_assets(self, mock_context, mock_engine, connections):
        catalog = MagicMock()
        mock_engine.get_schema.return_value = {"c": "str"}
        buffers = {"lineage": [], "assets": []}
        executor = _make_executor(
            mock_context,
            mock_engine,
            connections,
            catalog_manager=catalog,
            batch_write_buffers=buffers,
        )
        config = _make_config()
        df = pd.DataFrame({"c": ["x"]})

        executor._register_catalog_entries(config, df, connections["dst"], config.write)

        catalog.register_asset.assert_not_called()
        assert len(buffers["assets"]) == 1
        assert buffers["assets"][0]["table_name"] == "test_node"

    def test_batch_mode_buffers_lineage(self, mock_context, mock_engine, connections):
        catalog = MagicMock()
        mock_engine.get_schema.return_value = {"c": "str"}
        buffers = {"lineage": [], "assets": []}
        executor = _make_executor(
            mock_context,
            mock_engine,
            connections,
            catalog_manager=catalog,
            batch_write_buffers=buffers,
        )
        config = _make_config()
        df = pd.DataFrame({"c": ["x"]})

        executor._register_catalog_entries(config, df, connections["dst"], config.write)

        catalog.record_lineage.assert_not_called()
        assert len(buffers["lineage"]) == 1
        assert buffers["lineage"][0]["target_table"] == "/data/output"

    def test_error_in_register_asset_caught_silently(self, mock_context, mock_engine, connections):
        catalog = MagicMock()
        catalog.register_asset.side_effect = RuntimeError("db error")
        mock_engine.get_schema.return_value = {"c": "str"}
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _make_config()
        df = pd.DataFrame({"c": ["x"]})

        # Should not raise
        executor._register_catalog_entries(config, df, connections["dst"], config.write)

    def test_error_in_track_schema_caught_silently(self, mock_context, mock_engine, connections):
        catalog = MagicMock()
        catalog.track_schema.side_effect = RuntimeError("schema error")
        mock_engine.get_schema.return_value = {"c": "str"}
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _make_config()
        df = pd.DataFrame({"c": ["x"]})

        # Should not raise
        executor._register_catalog_entries(config, df, connections["dst"], config.write)

    def test_error_in_record_lineage_caught_silently(self, mock_context, mock_engine, connections):
        catalog = MagicMock()
        catalog.record_lineage.side_effect = RuntimeError("lineage error")
        mock_engine.get_schema.return_value = {"c": "str"}
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _make_config()
        df = pd.DataFrame({"c": ["x"]})

        # Should not raise
        executor._register_catalog_entries(config, df, connections["dst"], config.write)


# ============================================================
# _add_write_metadata
# ============================================================


class TestAddWriteMetadata:
    """Tests for _add_write_metadata method."""

    def test_calls_engine_with_correct_args(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            write_dict={
                "connection": "dst",
                "format": "csv",
                "path": "out.csv",
                "add_metadata": True,
            }
        )
        df = pd.DataFrame({"a": [1]})

        executor._add_write_metadata(config, df)

        mock_engine.add_write_metadata.assert_called_once()
        call_kwargs = mock_engine.add_write_metadata.call_args[1]
        assert call_kwargs["source_connection"] == "src"
        assert call_kwargs["is_file_source"] is True

    def test_detects_file_source_formats(self, mock_context, mock_engine, connections):
        for fmt in ["csv", "parquet", "json"]:
            executor = _make_executor(mock_context, mock_engine, connections)
            config = NodeConfig(
                name="test_node",
                read={"connection": "src", "format": fmt, "path": "input.dat"},
                write={
                    "connection": "dst",
                    "format": "csv",
                    "path": "out.csv",
                    "add_metadata": True,
                },
            )
            df = pd.DataFrame({"a": [1]})

            executor._add_write_metadata(config, df)

            call_kwargs = mock_engine.add_write_metadata.call_args[1]
            assert call_kwargs["is_file_source"] is True, f"Expected True for {fmt}"

    def test_non_file_format_sets_is_file_source_false(
        self, mock_context, mock_engine, connections
    ):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = NodeConfig(
            name="test_node",
            read={"connection": "src", "format": "sql", "table": "dbo.input"},
            write={
                "connection": "dst",
                "format": "csv",
                "path": "out.csv",
                "add_metadata": True,
            },
        )
        df = pd.DataFrame({"a": [1]})

        executor._add_write_metadata(config, df)

        call_kwargs = mock_engine.add_write_metadata.call_args[1]
        assert call_kwargs["is_file_source"] is False


# ============================================================
# _check_skip_if_unchanged
# ============================================================


class TestCheckSkipIfUnchanged:
    """Tests for _check_skip_if_unchanged method."""

    def _lakehouse_config(self, **extra_write):
        write = {
            "connection": "dst",
            "format": "delta",
            "table": "my_table",
            **extra_write,
        }
        return _make_config(write_dict=write)

    def test_non_lakehouse_format_warns_and_returns_no_skip(
        self, mock_context, mock_engine, connections
    ):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(write_dict={"connection": "dst", "format": "csv", "path": "out.csv"})
        df = pd.DataFrame({"a": [1]})

        result = executor._check_skip_if_unchanged(config, df, connections["dst"])

        assert result["should_skip"] is False
        assert result["hash"] is None

    @patch("odibi.utils.content_hash.compute_dataframe_hash", return_value="abc123")
    @patch("odibi.utils.content_hash.get_content_hash_from_state", return_value=None)
    def test_pandas_engine_calls_compute_dataframe_hash(
        self, mock_get, mock_compute, mock_context, mock_engine, connections
    ):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = self._lakehouse_config()
        df = pd.DataFrame({"a": [1]})

        executor._check_skip_if_unchanged(config, df, connections["dst"])

        mock_compute.assert_called_once()

    @patch("odibi.utils.content_hash.compute_dataframe_hash", return_value="hash_new")
    @patch("odibi.utils.content_hash.get_content_hash_from_state", return_value=None)
    def test_no_previous_hash_returns_no_skip(
        self, mock_get, mock_compute, mock_context, mock_engine, connections
    ):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = self._lakehouse_config()
        df = pd.DataFrame({"a": [1]})

        result = executor._check_skip_if_unchanged(config, df, connections["dst"])

        assert result["should_skip"] is False
        assert result["hash"] == "hash_new"

    @patch("odibi.utils.content_hash.compute_dataframe_hash", return_value="same_hash")
    @patch("odibi.utils.content_hash.get_content_hash_from_state", return_value="same_hash")
    def test_matching_hash_and_target_exists_returns_skip(
        self, mock_get, mock_compute, mock_context, mock_engine, connections
    ):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = self._lakehouse_config()
        df = pd.DataFrame({"a": [1]})
        # _check_target_exists returns True for non-engine (no spark attr)
        del mock_engine.spark  # ensure hasattr(engine, 'spark') is False

        result = executor._check_skip_if_unchanged(config, df, connections["dst"])

        assert result["should_skip"] is True
        assert result["hash"] == "same_hash"

    @patch("odibi.utils.content_hash.compute_dataframe_hash", return_value="same_hash")
    @patch("odibi.utils.content_hash.get_content_hash_from_state", return_value="same_hash")
    def test_matching_hash_but_target_missing_returns_no_skip(
        self, mock_get, mock_compute, mock_context, mock_engine, connections
    ):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = self._lakehouse_config()
        df = pd.DataFrame({"a": [1]})

        with patch.object(executor, "_check_target_exists", return_value=False):
            result = executor._check_skip_if_unchanged(config, df, connections["dst"])

        assert result["should_skip"] is False
        assert result["hash"] == "same_hash"

    @patch("odibi.utils.content_hash.compute_dataframe_hash", return_value="new_hash")
    @patch("odibi.utils.content_hash.get_content_hash_from_state", return_value="old_hash")
    def test_hash_mismatch_returns_no_skip_with_pending(
        self, mock_get, mock_compute, mock_context, mock_engine, connections
    ):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = self._lakehouse_config()
        df = pd.DataFrame({"a": [1]})

        result = executor._check_skip_if_unchanged(config, df, connections["dst"])

        assert result["should_skip"] is False
        assert result["hash"] == "new_hash"
        assert executor._pending_content_hash == "new_hash"

    @patch("odibi.utils.content_hash.compute_dataframe_hash", return_value="polars_hash")
    @patch("odibi.utils.content_hash.get_content_hash_from_state", return_value=None)
    def test_polars_df_with_to_pandas_conversion(
        self, mock_get, mock_compute, mock_context, mock_engine, connections
    ):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = self._lakehouse_config()
        # Simulate a Polars-like object with to_pandas
        polars_df = MagicMock()
        polars_df.to_pandas.return_value = pd.DataFrame({"a": [1]})

        result = executor._check_skip_if_unchanged(config, polars_df, connections["dst"])

        polars_df.to_pandas.assert_called_once()
        mock_compute.assert_called_once()
        assert result["should_skip"] is False

    @patch("odibi.utils.content_hash.compute_dataframe_hash", return_value="h1")
    @patch("odibi.utils.content_hash.get_content_hash_from_state", return_value=None)
    def test_skip_hash_columns_passed_through(
        self, mock_get, mock_compute, mock_context, mock_engine, connections
    ):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = self._lakehouse_config(
            skip_hash_columns=["col_a"],
            skip_hash_sort_columns=["col_b"],
        )
        df = pd.DataFrame({"a": [1]})

        executor._check_skip_if_unchanged(config, df, connections["dst"])

        call_kwargs = mock_compute.call_args[1]
        assert call_kwargs["columns"] == ["col_a"]
        assert call_kwargs["sort_columns"] == ["col_b"]


# ============================================================
# _store_content_hash_after_write
# ============================================================


class TestStoreContentHashAfterWrite:
    """Tests for _store_content_hash_after_write method."""

    def test_no_pending_hash_does_nothing(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = self._lakehouse_config()

        with patch("odibi.utils.content_hash.set_content_hash_in_state") as mock_set:
            executor._store_content_hash_after_write(config, connections["dst"])
            mock_set.assert_not_called()

    @patch("odibi.utils.content_hash.set_content_hash_in_state")
    def test_stores_hash_via_set_content_hash(
        self, mock_set, mock_context, mock_engine, connections
    ):
        state = MagicMock()
        state.backend = MagicMock()
        executor = _make_executor(mock_context, mock_engine, connections, state_manager=state)
        executor._pending_content_hash = "abc123"
        config = self._lakehouse_config()

        executor._store_content_hash_after_write(config, connections["dst"])

        mock_set.assert_called_once_with(state.backend, "test_node", "my_table", "abc123")

    @patch("odibi.utils.content_hash.set_content_hash_in_state")
    def test_clears_pending_hash_after_storing(
        self, mock_set, mock_context, mock_engine, connections
    ):
        executor = _make_executor(mock_context, mock_engine, connections)
        executor._pending_content_hash = "xyz"
        config = self._lakehouse_config()

        executor._store_content_hash_after_write(config, connections["dst"])

        assert executor._pending_content_hash is None

    @patch(
        "odibi.utils.content_hash.set_content_hash_in_state",
        side_effect=RuntimeError("write failed"),
    )
    def test_exception_during_store_is_caught(
        self, mock_set, mock_context, mock_engine, connections
    ):
        executor = _make_executor(mock_context, mock_engine, connections)
        executor._pending_content_hash = "xyz"
        config = self._lakehouse_config()

        # Should not raise
        executor._store_content_hash_after_write(config, connections["dst"])
        # Hash is cleared even on error (finally block)
        assert executor._pending_content_hash is None

    def _lakehouse_config(self):
        return _make_config(
            write_dict={"connection": "dst", "format": "delta", "table": "my_table"}
        )


# ============================================================
# _check_target_exists
# ============================================================


class TestCheckTargetExists:
    """Tests for _check_target_exists method."""

    def test_table_target_no_engine_features_returns_true(
        self, mock_context, mock_engine, connections
    ):
        """Table-based target without engine.has_attr returns True (non-engine path)."""
        del mock_engine.spark  # no spark attribute
        executor = _make_executor(mock_context, mock_engine, connections)
        write_config = WriteConfig(connection="dst", format="delta", table="my_table")

        result = executor._check_target_exists(write_config, connections["dst"])

        assert result is True

    def test_path_target_no_engine_features_returns_true(
        self, mock_context, mock_engine, connections
    ):
        """Path-based target without engine features returns True."""
        del mock_engine.spark
        executor = _make_executor(mock_context, mock_engine, connections)
        write_config = WriteConfig(connection="dst", format="delta", path="output/path")

        result = executor._check_target_exists(write_config, connections["dst"])

        assert result is True

    def test_no_table_or_path_returns_true(self, mock_context, mock_engine, connections):
        """No table or path specified returns True (assume exists)."""
        del mock_engine.spark
        executor = _make_executor(mock_context, mock_engine, connections)
        write_config = MagicMock()
        write_config.table = None
        write_config.path = None

        result = executor._check_target_exists(write_config, connections["dst"])

        assert result is True

    def test_file_not_found_error_returns_false(self, mock_context, mock_engine, connections):
        """FileNotFoundError returns False."""
        del mock_engine.spark
        executor = _make_executor(mock_context, mock_engine, connections)
        write_config = MagicMock()
        write_config.table = "missing_table"
        # Make the property access raise FileNotFoundError inside the try block
        type(write_config).table = property(
            lambda self: (_ for _ in ()).throw(FileNotFoundError("not found"))
        )

        result = executor._check_target_exists(write_config, connections["dst"])

        assert result is False

    def test_general_exception_returns_false(self, mock_context, mock_engine, connections):
        """General exception returns False (with warning)."""
        del mock_engine.spark
        executor = _make_executor(mock_context, mock_engine, connections)
        executor._ctx = MagicMock()  # _check_target_exists uses self._ctx.warning
        write_config = MagicMock()
        # Make table access raise a general exception
        type(write_config).table = property(
            lambda self: (_ for _ in ()).throw(ValueError("unexpected"))
        )

        result = executor._check_target_exists(write_config, connections["dst"])

        assert result is False
