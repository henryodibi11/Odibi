"""Unit tests for NodeExecutor write phase: modes, partitioning, schema evolution.

Covers issue #285 — lines 2121-2465 of node.py.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.config import NodeConfig, WriteMode
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
    engine.write.return_value = None
    return engine


@pytest.fixture
def connections():
    return {"src": MagicMock(), "dst": MagicMock()}


def _make_executor(mock_context, mock_engine, connections, **kwargs):
    return NodeExecutor(mock_context, mock_engine, connections, **kwargs)


def _make_config(write_dict, name="test_node"):
    return NodeConfig(
        name=name,
        read={"connection": "src", "format": "csv", "path": "input.csv"},
        write=write_dict,
    )


# ============================================================
# _execute_write_phase — early returns
# ============================================================


class TestWritePhaseEarlyReturns:
    """Test early exit paths in _execute_write_phase."""

    def test_no_write_config_returns_immediately(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = NodeConfig(
            name="test_node",
            read={"connection": "src", "format": "csv", "path": "input.csv"},
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_write_phase(config, df)
        mock_engine.write.assert_not_called()

    def test_none_df_skips_write(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config({"connection": "dst", "format": "csv", "path": "out.csv"})

        executor._execute_write_phase(config, None)
        mock_engine.write.assert_not_called()
        assert any("no data" in s for s in executor._execution_steps)

    def test_missing_connection_raises(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config({"connection": "nonexistent", "format": "csv", "path": "out.csv"})
        df = pd.DataFrame({"id": [1]})

        with pytest.raises(ValueError, match="nonexistent"):
            executor._execute_write_phase(config, df)


# ============================================================
# Basic write operations
# ============================================================


class TestWritePhaseBasic:
    """Test basic write operations."""

    def test_basic_csv_write(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config({"connection": "dst", "format": "csv", "path": "out.csv"})
        df = pd.DataFrame({"id": [1, 2, 3]})

        executor._execute_write_phase(config, df)

        mock_engine.write.assert_called_once()
        _, kwargs = mock_engine.write.call_args
        assert kwargs["format"] == "csv"
        assert kwargs["path"] == "out.csv"
        assert kwargs["connection"] == connections["dst"]

    def test_override_mode_used(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {"connection": "dst", "format": "csv", "path": "out.csv", "mode": "append"}
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_write_phase(config, df, override_mode=WriteMode.OVERWRITE)

        _, kwargs = mock_engine.write.call_args
        assert kwargs["mode"] == WriteMode.OVERWRITE


# ============================================================
# Partitioning
# ============================================================


class TestWritePhasePartitioning:
    """Test partition_by handling."""

    def test_partition_by_passed_to_options(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "delta",
                "path": "out_delta",
                "partition_by": ["year", "month"],
            }
        )
        df = pd.DataFrame({"id": [1], "year": [2024], "month": [1]})

        executor._execute_write_phase(config, df)

        _, kwargs = mock_engine.write.call_args
        assert kwargs["options"]["partition_by"] == ["year", "month"]
        assert any("Partition by" in s for s in executor._execution_steps)


# ============================================================
# Z-ordering
# ============================================================


class TestWritePhaseZOrder:
    """Test zorder_by handling."""

    def test_zorder_by_passed_for_lakehouse_format(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "delta",
                "path": "out_delta",
                "zorder_by": ["customer_id"],
            }
        )
        df = pd.DataFrame({"id": [1], "customer_id": [100]})

        executor._execute_write_phase(config, df)

        _, kwargs = mock_engine.write.call_args
        assert kwargs["options"]["zorder_by"] == ["customer_id"]
        assert any("Z-Order by" in s for s in executor._execution_steps)

    def test_zorder_by_ignored_for_parquet(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "parquet",
                "path": "out.parquet",
                "zorder_by": ["customer_id"],
            }
        )
        df = pd.DataFrame({"id": [1], "customer_id": [100]})

        executor._execute_write_phase(config, df)

        _, kwargs = mock_engine.write.call_args
        assert "zorder_by" not in kwargs["options"]


# ============================================================
# Schema evolution (merge_schema / overwrite_schema)
# ============================================================


class TestWritePhaseSchemaEvolution:
    """Test merge_schema and overwrite_schema options."""

    def test_merge_schema_lakehouse(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "delta",
                "path": "out_delta",
                "merge_schema": True,
            }
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_write_phase(config, df)

        _, kwargs = mock_engine.write.call_args
        assert kwargs["options"]["mergeSchema"] is True
        assert any("mergeSchema" in s for s in executor._execution_steps)

    def test_merge_schema_parquet_uses_schema_mode(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "parquet",
                "path": "out.parquet",
                "merge_schema": True,
            }
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_write_phase(config, df)

        _, kwargs = mock_engine.write.call_args
        assert kwargs["options"]["schema_mode"] == "merge"

    def test_overwrite_schema_lakehouse(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "delta",
                "path": "out_delta",
                "overwrite_schema": True,
            }
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_write_phase(config, df)

        _, kwargs = mock_engine.write.call_args
        assert kwargs["options"]["overwriteSchema"] is True
        assert kwargs["options"]["overwrite_schema"] is True
        assert any("overwriteSchema" in s for s in executor._execution_steps)


# ============================================================
# Merge keys
# ============================================================


class TestWritePhaseMergeKeys:
    """Test merge_keys passed to write options."""

    def test_merge_keys_passed_to_options(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "delta",
                "path": "out_delta",
                "mode": "upsert",
                "merge_keys": ["id", "date"],
            }
        )
        df = pd.DataFrame({"id": [1], "date": ["2024-01-01"]})

        executor._execute_write_phase(config, df)

        _, kwargs = mock_engine.write.call_args
        assert kwargs["options"]["merge_keys"] == ["id", "date"]


# ============================================================
# Add metadata (Bronze layer)
# ============================================================


class TestWritePhaseMetadata:
    """Test add_metadata for Bronze layer lineage columns."""

    def test_add_metadata_calls_add_write_metadata(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "csv",
                "path": "out.csv",
                "add_metadata": True,
            }
        )
        df = pd.DataFrame({"id": [1]})

        with patch.object(executor, "_add_write_metadata", return_value=df) as mock_meta:
            executor._execute_write_phase(config, df)
            mock_meta.assert_called_once()

        assert any("Bronze metadata" in s for s in executor._execution_steps)


# ============================================================
# Skip if unchanged
# ============================================================


class TestWritePhaseSkipIfUnchanged:
    """Test skip_if_unchanged content hash logic."""

    def test_skips_write_when_content_unchanged(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "delta",
                "path": "out_delta",
                "skip_if_unchanged": True,
            }
        )
        df = pd.DataFrame({"id": [1, 2]})

        with patch.object(
            executor,
            "_check_skip_if_unchanged",
            return_value={"should_skip": True, "hash": "abc123def456"},
        ):
            executor._execute_write_phase(config, df)

        mock_engine.write.assert_not_called()
        assert any("content unchanged" in s for s in executor._execution_steps)

    def test_writes_when_content_changed(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "delta",
                "path": "out_delta",
                "skip_if_unchanged": True,
            }
        )
        df = pd.DataFrame({"id": [1, 2]})

        with patch.object(
            executor,
            "_check_skip_if_unchanged",
            return_value={"should_skip": False, "hash": "newHash123"},
        ):
            executor._execute_write_phase(config, df)

        mock_engine.write.assert_called_once()


# ============================================================
# Materialized strategies (view, incremental, table)
# ============================================================


class TestWritePhaseMaterialized:
    """Test materialized strategy handling."""

    def test_materialized_view_creates_view(self, mock_context, mock_engine, connections):
        mock_engine.create_view = MagicMock()
        executor = _make_executor(mock_context, mock_engine, connections)
        config = NodeConfig(
            name="test_node",
            read={"connection": "src", "format": "csv", "path": "input.csv"},
            write={"connection": "dst", "format": "delta", "table": "my_view"},
            materialized="view",
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_write_phase(config, df)

        mock_engine.create_view.assert_called_once()
        # write should NOT be called for view materialization
        mock_engine.write.assert_not_called()
        assert any("Created view" in s for s in executor._execution_steps)

    def test_materialized_incremental_uses_append(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = NodeConfig(
            name="test_node",
            read={"connection": "src", "format": "csv", "path": "input.csv"},
            write={"connection": "dst", "format": "delta", "path": "out_delta"},
            materialized="incremental",
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_write_phase(config, df)

        _, kwargs = mock_engine.write.call_args
        assert kwargs["mode"] == WriteMode.APPEND
        assert any("incremental" in s for s in executor._execution_steps)

    def test_materialized_table_default_behavior(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = NodeConfig(
            name="test_node",
            read={"connection": "src", "format": "csv", "path": "input.csv"},
            write={
                "connection": "dst",
                "format": "delta",
                "path": "out_delta",
                "mode": "overwrite",
            },
            materialized="table",
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_write_phase(config, df)

        mock_engine.write.assert_called_once()
        assert any("Materialized: table" in s for s in executor._execution_steps)


# ============================================================
# Delta commit metrics extraction
# ============================================================


class TestWritePhaseCommitMetrics:
    """Test row count extraction from lakehouse commit metadata."""

    def test_row_count_from_commit_metrics(self, mock_context, mock_engine, connections):
        mock_engine.write.return_value = {
            "operation_metrics": {"numOutputRows": "100"},
        }
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "delta",
                "path": "out_lakehouse",
            }
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_write_phase(config, df)

        assert executor._delta_write_info is not None
        assert executor._delta_write_info.get("_cached_row_count") == 100

    def test_row_count_fallback_to_count(self, mock_context, mock_engine, connections):
        mock_engine.write.return_value = {"operation_metrics": {}}
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "delta",
                "path": "out_lakehouse",
            }
        )
        df = pd.DataFrame({"id": [1, 2, 3]})

        executor._execute_write_phase(config, df)

        assert executor._delta_write_info["_cached_row_count"] == 3

    def test_cached_row_count_from_streaming(self, mock_context, mock_engine, connections):
        mock_engine.write.return_value = {"_cached_row_count": 42}
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "delta",
                "path": "out_lakehouse",
            }
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_write_phase(config, df)

        assert executor._delta_write_info["_cached_row_count"] == 42


# ============================================================
# _determine_write_mode
# ============================================================


class TestDetermineWriteMode:
    """Test _determine_write_mode for first-run detection."""

    def test_returns_none_when_no_first_run_query(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config({"connection": "dst", "format": "csv", "path": "out.csv"})

        result = executor._determine_write_mode(config)
        assert result is None

    def test_returns_overwrite_when_table_not_exists(self, mock_context, mock_engine, connections):
        mock_engine.table_exists.return_value = False
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "delta",
                "path": "out_delta",
                "first_run_query": "SELECT * FROM source",
            }
        )

        result = executor._determine_write_mode(config)
        assert result == WriteMode.OVERWRITE

    def test_returns_none_when_table_exists(self, mock_context, mock_engine, connections):
        mock_engine.table_exists.return_value = True
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "delta",
                "path": "out_delta",
                "first_run_query": "SELECT * FROM source",
            }
        )

        result = executor._determine_write_mode(config)
        assert result is None

    def test_returns_none_when_connection_missing(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "nonexistent",
                "format": "delta",
                "path": "out_delta",
                "first_run_query": "SELECT * FROM source",
            }
        )

        result = executor._determine_write_mode(config)
        assert result is None


# ============================================================
# Auto-optimize
# ============================================================


class TestWritePhaseAutoOptimize:
    """Test auto-optimize for Delta tables."""

    def test_auto_optimize_calls_maintain_table(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "delta",
                "path": "out_delta",
                "auto_optimize": True,
            }
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_write_phase(config, df)

        mock_engine.maintain_table.assert_called_once()
        _, kwargs = mock_engine.maintain_table.call_args
        assert kwargs["format"] == "delta"

    def test_auto_optimize_skipped_for_parquet(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "parquet",
                "path": "out.parquet",
                "auto_optimize": True,
            }
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_write_phase(config, df)

        mock_engine.maintain_table.assert_not_called()


# ============================================================
# Delta table properties
# ============================================================


class TestWritePhaseTableProperties:
    """Test Delta table_properties merging."""

    def test_table_properties_from_write_config(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            {
                "connection": "dst",
                "format": "delta",
                "path": "out_delta",
                "table_properties": {"delta.autoOptimize.optimizeWrite": "true"},
            }
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_write_phase(config, df)

        _, kwargs = mock_engine.write.call_args
        assert kwargs["options"]["table_properties"] == {"delta.autoOptimize.optimizeWrite": "true"}

    def test_table_properties_merged_with_performance_config(
        self, mock_context, mock_engine, connections
    ):
        perf_config = MagicMock()
        perf_config.delta_table_properties = {"delta.autoCompact": "true"}
        perf_config.skip_catalog_writes = False
        executor = _make_executor(
            mock_context, mock_engine, connections, performance_config=perf_config
        )
        config = _make_config(
            {
                "connection": "dst",
                "format": "delta",
                "path": "out_delta",
                "table_properties": {"delta.autoOptimize.optimizeWrite": "true"},
            }
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_write_phase(config, df)

        _, kwargs = mock_engine.write.call_args
        props = kwargs["options"]["table_properties"]
        assert props["delta.autoCompact"] == "true"
        assert props["delta.autoOptimize.optimizeWrite"] == "true"


# ============================================================
# Catalog write skip
# ============================================================


class TestWritePhaseCatalogSkip:
    """Test skip_catalog_writes in performance config."""

    def test_skip_catalog_writes(self, mock_context, mock_engine, connections):
        perf_config = MagicMock()
        perf_config.skip_catalog_writes = True
        perf_config.delta_table_properties = None
        executor = _make_executor(
            mock_context, mock_engine, connections, performance_config=perf_config
        )
        config = _make_config({"connection": "dst", "format": "csv", "path": "out.csv"})
        df = pd.DataFrame({"id": [1]})

        with patch.object(executor, "_register_catalog_entries") as mock_reg:
            executor._execute_write_phase(config, df)
            mock_reg.assert_not_called()
