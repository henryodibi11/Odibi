"""Tests for NodeExecutor._execute_inputs_phase (multi-input & cross-pipeline deps).

Covers: lines 673-822 of odibi/node.py (issue #287).
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.config import NodeConfig
from odibi.node import NodeExecutor


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_context():
    ctx = MagicMock()
    ctx.get.return_value = None
    return ctx


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.name = "pandas"
    engine.materialize.side_effect = lambda df: df
    engine.count_rows.return_value = 5
    engine.table_exists.return_value = True
    return engine


@pytest.fixture
def connections():
    conn = MagicMock()
    conn.get_path.return_value = "/data/resolved"
    return {"src": conn, "dst": MagicMock(), "other_conn": MagicMock()}


def _make_executor(context, engine, connections, **kwargs):
    return NodeExecutor(context, engine, connections, **kwargs)


def _node_with_inputs(inputs):
    """Create a minimal NodeConfig with given inputs dict."""
    return NodeConfig(
        name="test_node",
        inputs=inputs,
        write={"connection": "dst", "format": "csv", "path": "out.csv"},
    )


# ---------------------------------------------------------------------------
# No inputs
# ---------------------------------------------------------------------------


class TestNoInputs:
    def test_returns_empty_when_inputs_is_none(self, mock_context, mock_engine, connections):
        config = NodeConfig(
            name="test_node",
            write={"connection": "dst", "format": "csv", "path": "out.csv"},
        )
        executor = _make_executor(mock_context, mock_engine, connections)
        result = executor._execute_inputs_phase(config)
        assert result == {}

    def test_returns_empty_when_inputs_is_empty_dict(self, mock_context, mock_engine, connections):
        config = NodeConfig(
            name="test_node",
            inputs={},
            write={"connection": "dst", "format": "csv", "path": "out.csv"},
        )
        executor = _make_executor(mock_context, mock_engine, connections)
        result = executor._execute_inputs_phase(config)
        assert result == {}


# ---------------------------------------------------------------------------
# Pipeline reference — catalog lookup succeeds
# ---------------------------------------------------------------------------


class TestPipelineRefCatalog:
    def test_catalog_lookup_success(self, mock_context, mock_engine, connections):
        expected_df = pd.DataFrame({"a": [1, 2]})
        mock_engine.read.return_value = expected_df
        mock_engine.table_exists.return_value = True

        catalog = MagicMock()
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _node_with_inputs({"orders": "$sales.order_node"})

        with (
            patch(
                "odibi.references.resolve_input_reference",
                return_value={"connection": "src", "path": "bronze/orders", "format": "parquet"},
            ),
            patch("odibi.references.is_pipeline_reference", return_value=True),
        ):
            result = executor._execute_inputs_phase(config, current_pipeline="transform")

        assert "orders" in result
        pd.testing.assert_frame_equal(result["orders"], expected_df)

    def test_catalog_connection_not_found_falls_through(
        self, mock_context, mock_engine, connections
    ):
        """Missing connection ValueError is caught by broad except; falls to 'Cannot resolve'."""
        catalog = MagicMock()
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _node_with_inputs({"orders": "$sales.order_node"})

        with (
            patch(
                "odibi.references.resolve_input_reference",
                return_value={"connection": "nonexistent", "path": "x", "format": "parquet"},
            ),
            patch("odibi.references.is_pipeline_reference", return_value=True),
        ):
            with pytest.raises(ValueError, match="Cannot resolve reference"):
                executor._execute_inputs_phase(config, current_pipeline="transform")

    def test_table_not_exists_falls_through_to_cache(self, mock_context, mock_engine, connections):
        """When table_exists returns False the catalog path is skipped; cache fallback runs."""
        cached_df = pd.DataFrame({"x": [10]})
        mock_context.get.return_value = cached_df
        mock_engine.table_exists.return_value = False

        catalog = MagicMock()
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _node_with_inputs({"orders": "$mypipe.order_node"})

        with (
            patch(
                "odibi.references.resolve_input_reference",
                return_value={"connection": "src", "path": "bronze/orders", "format": "parquet"},
            ),
            patch("odibi.references.is_pipeline_reference", return_value=True),
        ):
            result = executor._execute_inputs_phase(config, current_pipeline="mypipe")

        assert "orders" in result
        pd.testing.assert_frame_equal(result["orders"], cached_df)
        mock_engine.read.assert_not_called()


# ---------------------------------------------------------------------------
# Pipeline reference — context cache fallback
# ---------------------------------------------------------------------------


class TestPipelineRefCacheFallback:
    def test_same_pipeline_cached_df(self, mock_context, mock_engine, connections):
        cached_df = pd.DataFrame({"v": [1]})
        mock_context.get.return_value = cached_df
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _node_with_inputs({"orders": "$mypipe.order_node"})

        with patch("odibi.references.is_pipeline_reference", return_value=True):
            result = executor._execute_inputs_phase(config, current_pipeline="mypipe")

        assert "orders" in result
        pd.testing.assert_frame_equal(result["orders"], cached_df)

    def test_same_pipeline_no_cache_raises(self, mock_context, mock_engine, connections):
        mock_context.get.return_value = None
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _node_with_inputs({"orders": "$mypipe.order_node"})

        with patch("odibi.references.is_pipeline_reference", return_value=True):
            with pytest.raises(ValueError, match="Cannot resolve reference"):
                executor._execute_inputs_phase(config, current_pipeline="mypipe")

    def test_different_pipeline_no_catalog_raises(self, mock_context, mock_engine, connections):
        mock_context.get.return_value = None
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _node_with_inputs({"orders": "$other.order_node"})

        with patch("odibi.references.is_pipeline_reference", return_value=True):
            with pytest.raises(ValueError, match="Cannot resolve reference"):
                executor._execute_inputs_phase(config, current_pipeline="mypipe")


# ---------------------------------------------------------------------------
# Pipeline reference — catalog throws, fallback to cache
# ---------------------------------------------------------------------------


class TestCatalogExceptionFallback:
    def test_catalog_exception_falls_back_to_cache_same_pipeline(
        self, mock_context, mock_engine, connections
    ):
        cached_df = pd.DataFrame({"c": [99]})
        mock_context.get.return_value = cached_df

        catalog = MagicMock()
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _node_with_inputs({"orders": "$mypipe.order_node"})

        with (
            patch(
                "odibi.references.resolve_input_reference",
                side_effect=RuntimeError("catalog boom"),
            ),
            patch("odibi.references.is_pipeline_reference", return_value=True),
        ):
            result = executor._execute_inputs_phase(config, current_pipeline="mypipe")

        pd.testing.assert_frame_equal(result["orders"], cached_df)

    def test_catalog_exception_different_pipeline_raises(
        self, mock_context, mock_engine, connections
    ):
        mock_context.get.return_value = None
        catalog = MagicMock()
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _node_with_inputs({"orders": "$other.order_node"})

        with (
            patch(
                "odibi.references.resolve_input_reference",
                side_effect=RuntimeError("catalog boom"),
            ),
            patch("odibi.references.is_pipeline_reference", return_value=True),
        ):
            with pytest.raises(ValueError, match="Cannot resolve reference"):
                executor._execute_inputs_phase(config, current_pipeline="mypipe")


# ---------------------------------------------------------------------------
# Dict input (inline read config)
# ---------------------------------------------------------------------------


class TestDictInput:
    def test_dict_read_success(self, mock_context, mock_engine, connections):
        expected_df = pd.DataFrame({"d": [1]})
        mock_engine.read.return_value = expected_df
        executor = _make_executor(mock_context, mock_engine, connections)

        ref = {"connection": "src", "format": "csv", "path": "data.csv"}
        config = _node_with_inputs({"raw": ref})

        with patch("odibi.references.is_pipeline_reference", return_value=False):
            result = executor._execute_inputs_phase(config)

        assert "raw" in result
        pd.testing.assert_frame_equal(result["raw"], expected_df)
        mock_engine.read.assert_called_once_with(
            connection=connections["src"],
            format="csv",
            table=None,
            path="data.csv",
        )

    def test_dict_missing_connection_raises(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        ref = {"connection": "missing_conn", "format": "csv", "path": "data.csv"}
        config = _node_with_inputs({"raw": ref})

        with patch("odibi.references.is_pipeline_reference", return_value=False):
            with pytest.raises(ValueError, match="Connection 'missing_conn' not found"):
                executor._execute_inputs_phase(config)

    def test_dict_no_connection_key(self, mock_context, mock_engine, connections):
        """Dict without a connection key — connection=None is passed to engine.read."""
        expected_df = pd.DataFrame({"x": [5]})
        mock_engine.read.return_value = expected_df
        executor = _make_executor(mock_context, mock_engine, connections)

        ref = {"format": "parquet", "table": "my_table"}
        config = _node_with_inputs({"raw": ref})

        with patch("odibi.references.is_pipeline_reference", return_value=False):
            result = executor._execute_inputs_phase(config)

        mock_engine.read.assert_called_once_with(
            connection=None,
            format="parquet",
            table="my_table",
            path=None,
        )
        pd.testing.assert_frame_equal(result["raw"], expected_df)


# ---------------------------------------------------------------------------
# Invalid input format
# ---------------------------------------------------------------------------


class TestInvalidInputFormat:
    def test_plain_string_not_pipeline_ref_raises(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _node_with_inputs({"bad": "just_a_string"})

        with patch("odibi.references.is_pipeline_reference", return_value=False):
            with pytest.raises(ValueError, match="Invalid input format"):
                executor._execute_inputs_phase(config)

    def test_integer_input_rejected_by_pydantic(self, mock_context, mock_engine, connections):
        """Non-str/non-dict values are rejected at config validation time."""
        from pydantic import ValidationError as PydanticValidationError

        with pytest.raises(PydanticValidationError):
            _node_with_inputs({"bad": 42})


# ---------------------------------------------------------------------------
# Input source path tracking
# ---------------------------------------------------------------------------


class TestInputSourcePath:
    def test_sets_current_input_path_from_catalog(self, mock_context, mock_engine, connections):
        expected_df = pd.DataFrame({"a": [1]})
        mock_engine.read.return_value = expected_df
        mock_engine.table_exists.return_value = True

        catalog = MagicMock()
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _node_with_inputs({"orders": "$sales.order_node"})

        with (
            patch(
                "odibi.references.resolve_input_reference",
                return_value={"connection": "src", "path": "bronze/orders", "format": "parquet"},
            ),
            patch("odibi.references.is_pipeline_reference", return_value=True),
        ):
            executor._execute_inputs_phase(config, current_pipeline="transform")

        # connection.get_path is called with the path from catalog
        connections["src"].get_path.assert_called_once_with("bronze/orders")
        assert mock_engine._current_input_path == "/data/resolved"

    def test_sets_current_input_path_table_fallback(self, mock_context, mock_engine, connections):
        """When path is None, table is used as input_path."""
        expected_df = pd.DataFrame({"a": [1]})
        mock_engine.read.return_value = expected_df
        mock_engine.table_exists.return_value = True

        catalog = MagicMock()
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _node_with_inputs({"orders": "$sales.order_node"})

        with (
            patch(
                "odibi.references.resolve_input_reference",
                return_value={"connection": "src", "table": "my_table", "format": "parquet"},
            ),
            patch("odibi.references.is_pipeline_reference", return_value=True),
        ):
            executor._execute_inputs_phase(config, current_pipeline="transform")

        connections["src"].get_path.assert_called_once_with("my_table")

    def test_no_path_tracking_when_no_connection_get_path(
        self, mock_context, mock_engine, connections
    ):
        """When connection has no get_path, raw path is stored."""
        expected_df = pd.DataFrame({"a": [1]})
        mock_engine.read.return_value = expected_df
        mock_engine.table_exists.return_value = True

        conn_no_get_path = MagicMock(spec=[])  # no get_path attribute
        conns = {"bare": conn_no_get_path, "dst": MagicMock()}
        catalog = MagicMock()
        executor = _make_executor(mock_context, mock_engine, conns, catalog_manager=catalog)
        config = _node_with_inputs({"orders": "$sales.order_node"})

        with (
            patch(
                "odibi.references.resolve_input_reference",
                return_value={"connection": "bare", "path": "raw/path", "format": "parquet"},
            ),
            patch("odibi.references.is_pipeline_reference", return_value=True),
        ):
            executor._execute_inputs_phase(config, current_pipeline="transform")

        assert mock_engine._current_input_path == "raw/path"


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_df_with_persist_is_persisted_and_tracked(self, mock_context, mock_engine, connections):
        mock_df = MagicMock()
        mock_df.persist.return_value = mock_df
        mock_df.isStreaming = False
        mock_engine.read.return_value = mock_df

        executor = _make_executor(mock_context, mock_engine, connections)
        ref = {"format": "parquet", "table": "t"}
        config = _node_with_inputs({"raw": ref})

        with patch("odibi.references.is_pipeline_reference", return_value=False):
            executor._execute_inputs_phase(config)

        mock_df.persist.assert_called_once()
        assert mock_df in executor._persisted_dfs

    def test_pandas_df_not_persisted(self, mock_context, mock_engine, connections):
        plain_df = pd.DataFrame({"a": [1]})
        mock_engine.read.return_value = plain_df

        executor = _make_executor(mock_context, mock_engine, connections)
        ref = {"format": "csv", "path": "f.csv"}
        config = _node_with_inputs({"raw": ref})

        with patch("odibi.references.is_pipeline_reference", return_value=False):
            executor._execute_inputs_phase(config)

        assert len(executor._persisted_dfs) == 0

    def test_streaming_df_not_persisted(self, mock_context, mock_engine, connections):
        mock_df = MagicMock()
        mock_df.isStreaming = True
        mock_engine.read.return_value = mock_df

        executor = _make_executor(mock_context, mock_engine, connections)
        ref = {"format": "parquet", "table": "t"}
        config = _node_with_inputs({"raw": ref})

        with patch("odibi.references.is_pipeline_reference", return_value=False):
            executor._execute_inputs_phase(config)

        mock_df.persist.assert_not_called()
        assert len(executor._persisted_dfs) == 0


# ---------------------------------------------------------------------------
# Execution tracking
# ---------------------------------------------------------------------------


class TestExecutionTracking:
    def test_execution_steps_updated(self, mock_context, mock_engine, connections):
        mock_engine.read.return_value = pd.DataFrame({"a": [1]})
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _node_with_inputs({"raw": {"format": "csv", "path": "f.csv"}})

        with patch("odibi.references.is_pipeline_reference", return_value=False):
            executor._execute_inputs_phase(config)

        assert any("Loaded input 'raw'" in s for s in executor._execution_steps)
        assert any("5 rows" in s for s in executor._execution_steps)

    def test_row_count_logged(self, mock_context, mock_engine, connections):
        mock_engine.read.return_value = pd.DataFrame({"a": range(10)})
        mock_engine.count_rows.return_value = 10
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _node_with_inputs({"raw": {"format": "csv", "path": "f.csv"}})

        with patch("odibi.references.is_pipeline_reference", return_value=False):
            executor._execute_inputs_phase(config)

        assert any("10 rows" in s for s in executor._execution_steps)


# ---------------------------------------------------------------------------
# Multiple inputs
# ---------------------------------------------------------------------------


class TestMultipleInputs:
    def test_two_inputs_both_succeed(self, mock_context, mock_engine, connections):
        df1 = pd.DataFrame({"a": [1]})
        df2 = pd.DataFrame({"b": [2]})
        mock_engine.read.side_effect = [df1, df2]

        executor = _make_executor(mock_context, mock_engine, connections)
        config = _node_with_inputs(
            {
                "first": {"format": "csv", "path": "a.csv"},
                "second": {"format": "csv", "path": "b.csv"},
            }
        )

        with patch("odibi.references.is_pipeline_reference", return_value=False):
            result = executor._execute_inputs_phase(config)

        assert len(result) == 2
        pd.testing.assert_frame_equal(result["first"], df1)
        pd.testing.assert_frame_equal(result["second"], df2)

    def test_first_input_fails_raises_immediately(self, mock_context, mock_engine, connections):
        """A dict input referencing a missing connection raises before the second input runs."""
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _node_with_inputs(
            {
                "bad": {"connection": "missing_conn", "format": "csv", "path": "a.csv"},
                "good": {"format": "csv", "path": "b.csv"},
            }
        )

        with patch("odibi.references.is_pipeline_reference", return_value=False):
            with pytest.raises(ValueError, match="Connection 'missing_conn' not found"):
                executor._execute_inputs_phase(config)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_catalog_present_but_connection_none_in_read_config(
        self, mock_context, mock_engine, connections
    ):
        """resolve_input_reference returns a config without a connection key (managed table)."""
        expected_df = pd.DataFrame({"m": [1]})
        mock_engine.read.return_value = expected_df
        mock_engine.table_exists.return_value = True

        catalog = MagicMock()
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _node_with_inputs({"tbl": "$sales.managed"})

        with (
            patch(
                "odibi.references.resolve_input_reference",
                return_value={"table": "my_managed_table", "format": "parquet"},
            ),
            patch("odibi.references.is_pipeline_reference", return_value=True),
        ):
            result = executor._execute_inputs_phase(config, current_pipeline="transform")

        mock_engine.read.assert_called_once_with(
            connection=None,
            format="parquet",
            table="my_managed_table",
            path=None,
        )
        pd.testing.assert_frame_equal(result["tbl"], expected_df)

    def test_ref_without_dot_has_no_cache_fallback(self, mock_context, mock_engine, connections):
        """A ref like '$single' (no dot) produces ref_pipeline=None, ref_node=None → no cache."""
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _node_with_inputs({"bad": "$single"})

        with patch("odibi.references.is_pipeline_reference", return_value=True):
            with pytest.raises(ValueError, match="Cannot resolve reference"):
                executor._execute_inputs_phase(config, current_pipeline="mypipe")

    def test_no_current_pipeline_skips_cache_fallback(self, mock_context, mock_engine, connections):
        """When current_pipeline is None, cache fallback is skipped even for same ref_pipeline."""
        mock_context.get.return_value = pd.DataFrame({"x": [1]})
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _node_with_inputs({"orders": "$mypipe.order_node"})

        with patch("odibi.references.is_pipeline_reference", return_value=True):
            with pytest.raises(ValueError, match="Cannot resolve reference"):
                executor._execute_inputs_phase(config, current_pipeline=None)

    def test_dict_input_source_in_log_step(self, mock_context, mock_engine, connections):
        """For dict inputs, the logged source comes from path or table in the dict."""
        mock_engine.read.return_value = pd.DataFrame({"a": [1]})
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _node_with_inputs({"raw": {"format": "csv", "path": "my/file.csv"}})

        with patch("odibi.references.is_pipeline_reference", return_value=False):
            executor._execute_inputs_phase(config)

        assert any("Loaded input 'raw'" in s for s in executor._execution_steps)
