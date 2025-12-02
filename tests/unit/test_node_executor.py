from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.config import (
    ColumnMetadata,
    NodeConfig,
    PrivacyConfig,
    PrivacyMethod,
    SchemaMode,
    SchemaPolicyConfig,
)
from odibi.node import NodeExecutor


@pytest.fixture
def mock_context():
    return MagicMock()


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.name = "pandas"
    return engine


@pytest.fixture
def connections():
    return {"src": MagicMock(), "dst": MagicMock()}


def test_schema_policy_application(mock_context, mock_engine, connections):
    """Test that schema policy triggers harmonize_schema in write phase."""
    # Setup Input DF
    df = pd.DataFrame({"id": [1], "new_col": ["x"]})

    # Mock engine methods
    # 1. Table Exists (for first run check) - say it exists
    mock_engine.table_exists.return_value = True

    # 2. Get Schema - return target schema
    target_schema = {"id": "int64", "existing_col": "object"}
    mock_engine.get_table_schema.return_value = target_schema

    # 3. Harmonize - return modified df
    harmonized_df = pd.DataFrame({"id": [1], "existing_col": [None]})
    mock_engine.harmonize_schema.return_value = harmonized_df

    # Config with Schema Policy
    config = NodeConfig(
        name="test_node",
        read={"connection": "src", "format": "csv", "path": "src.csv"},
        write={"connection": "dst", "format": "csv", "path": "dst.csv"},
        schema_policy=SchemaPolicyConfig(mode=SchemaMode.ENFORCE),
    )

    executor = NodeExecutor(mock_context, mock_engine, connections)

    # Execute (Patch read to skip IO)
    with patch.object(executor, "_execute_read_phase", return_value=(df, None)):
        result = executor.execute(config)

    assert result.success

    # Verify get_table_schema called
    mock_engine.get_table_schema.assert_called_with(
        connection=connections["dst"], table=None, path="dst.csv", format="csv"
    )

    # Verify harmonize_schema called
    mock_engine.harmonize_schema.assert_called_once()
    args, _ = mock_engine.harmonize_schema.call_args
    # args: (df, target_schema, policy)
    pd.testing.assert_frame_equal(args[0], df)
    assert args[1] == target_schema
    assert args[2].mode == SchemaMode.ENFORCE

    # Verify write called with harmonized df
    mock_engine.write.assert_called_once()
    _, kwargs = mock_engine.write.call_args
    pd.testing.assert_frame_equal(kwargs["df"], harmonized_df)


def test_schema_policy_skipped_if_no_target(mock_context, mock_engine, connections):
    """Test that schema policy is skipped if target table doesn't exist."""
    df = pd.DataFrame({"id": [1]})

    # Mock table not existing or schema not found
    mock_engine.get_table_schema.return_value = None

    config = NodeConfig(
        name="test_node",
        read={"connection": "src", "format": "csv", "path": "src.csv"},
        write={"connection": "dst", "format": "csv", "path": "dst.csv"},
        schema_policy=SchemaPolicyConfig(mode=SchemaMode.ENFORCE),
    )

    executor = NodeExecutor(mock_context, mock_engine, connections)

    with patch.object(executor, "_execute_read_phase", return_value=(df, None)):
        result = executor.execute(config)

    assert result.success
    mock_engine.harmonize_schema.assert_not_called()

    # Write should use original df
    mock_engine.write.assert_called_once()
    _, kwargs = mock_engine.write.call_args
    pd.testing.assert_frame_equal(kwargs["df"], df)


def test_privacy_anonymization(mock_context, mock_engine, connections):
    """Test that privacy config triggers engine.anonymize."""
    df = pd.DataFrame({"email": ["a@b.com"]})
    anonymized_df = pd.DataFrame({"email": ["hash123"]})

    mock_engine.anonymize.return_value = anonymized_df

    config = NodeConfig(
        name="privacy_node",
        read={"connection": "src", "format": "csv", "path": "src.csv"},
        columns={"email": ColumnMetadata(pii=True)},
        privacy=PrivacyConfig(method=PrivacyMethod.HASH, salt="salty"),
        # Must have write or transform to be valid node? No, checks "at least one of read, transform, write"
    )

    executor = NodeExecutor(mock_context, mock_engine, connections)

    with patch.object(executor, "_execute_read_phase", return_value=(df, None)):
        result = executor.execute(config)

    assert result.success

    # Verify anonymize called
    mock_engine.anonymize.assert_called_once()
    args, _ = mock_engine.anonymize.call_args
    # args: (df, columns, method, salt)
    pd.testing.assert_frame_equal(args[0], df)
    assert args[1] == ["email"]
    assert args[2] == PrivacyMethod.HASH
    assert args[3] == "salty"


class TestWatermarkLag:
    """Tests for watermark_lag in IncrementalConfig."""

    def test_watermark_lag_subtracts_from_hwm(self, mock_context, mock_engine, connections):
        """Test that watermark_lag subtracts duration from HWM for stateful mode."""
        from datetime import datetime, timedelta

        df = pd.DataFrame({"id": [1, 2], "updated_at": [datetime.now(), datetime.now()]})
        mock_engine.read.return_value = df
        mock_engine.filter_greater_than.return_value = df

        # Set HWM to a known datetime
        hwm_value = datetime(2024, 1, 15, 12, 0, 0)
        hwm_state = ("test_node_hwm", hwm_value)

        config = NodeConfig(
            name="test_node",
            read={
                "connection": "src",
                "format": "delta",
                "table": "source_table",
                "incremental": {
                    "mode": "stateful",
                    "column": "updated_at",
                    "watermark_lag": "2h",
                },
            },
        )

        executor = NodeExecutor(mock_context, mock_engine, connections)

        with patch.object(executor, "_get_column_max", return_value=datetime.now()):
            executor.execute(config, hwm_state=hwm_state)

        # Verify filter was called with adjusted HWM (2 hours earlier)
        expected_hwm = hwm_value - timedelta(hours=2)
        mock_engine.filter_greater_than.assert_called()
        args, _ = mock_engine.filter_greater_than.call_args
        assert args[1] == "updated_at"
        assert args[2] == expected_hwm

    def test_watermark_lag_parses_string_hwm(self, mock_context, mock_engine, connections):
        """Test that watermark_lag correctly parses string HWM from state storage."""
        from datetime import datetime, timedelta

        df = pd.DataFrame({"id": [1, 2], "updated_at": [datetime.now(), datetime.now()]})
        mock_engine.read.return_value = df
        mock_engine.filter_greater_than.return_value = df

        # HWM stored as ISO string (as it would be from JSON deserialization)
        hwm_value = datetime(2024, 1, 15, 12, 0, 0)
        hwm_state = ("test_node_hwm", hwm_value.isoformat())

        config = NodeConfig(
            name="test_node",
            read={
                "connection": "src",
                "format": "delta",
                "table": "source_table",
                "incremental": {
                    "mode": "stateful",
                    "column": "updated_at",
                    "watermark_lag": "5d",
                },
            },
        )

        executor = NodeExecutor(mock_context, mock_engine, connections)

        with patch.object(executor, "_get_column_max", return_value=datetime.now()):
            executor.execute(config, hwm_state=hwm_state)

        # Verify filter was called with adjusted HWM (5 days earlier)
        expected_hwm = hwm_value - timedelta(days=5)
        mock_engine.filter_greater_than.assert_called()
        args, _ = mock_engine.filter_greater_than.call_args
        assert args[1] == "updated_at"
        assert args[2] == expected_hwm


class TestStreamingRead:
    """Tests for streaming config in ReadConfig."""

    def test_streaming_passed_to_engine_read(self, mock_context, mock_engine, connections):
        """Test that streaming flag is passed to engine.read()."""
        df = pd.DataFrame({"id": [1]})
        mock_engine.read.return_value = df

        config = NodeConfig(
            name="streaming_node",
            read={
                "connection": "src",
                "format": "delta",
                "table": "events",
                "streaming": True,
            },
        )

        executor = NodeExecutor(mock_context, mock_engine, connections)
        executor.execute(config)

        mock_engine.read.assert_called_once()
        _, kwargs = mock_engine.read.call_args
        assert kwargs.get("streaming") is True


class TestArchiveOptions:
    """Tests for archive_options in ReadConfig."""

    def test_archive_options_merged_into_read_options(self, mock_context, mock_engine, connections):
        """Test that archive_options are merged into read options."""
        df = pd.DataFrame({"id": [1]})
        mock_engine.read.return_value = df

        config = NodeConfig(
            name="archive_node",
            read={
                "connection": "src",
                "format": "csv",
                "path": "/data/input.csv",
                "archive_options": {"badRecordsPath": "/data/bad_records"},
            },
        )

        executor = NodeExecutor(mock_context, mock_engine, connections)
        executor.execute(config)

        mock_engine.read.assert_called_once()
        _, kwargs = mock_engine.read.call_args
        assert kwargs["options"].get("badRecordsPath") == "/data/bad_records"


class TestWriteConfigFields:
    """Tests for partition_by, zorder_by, and merge_schema in WriteConfig."""

    def test_partition_by_added_to_write_options(self, mock_context, mock_engine, connections):
        """Test that partition_by is extracted and added to write_options."""
        df = pd.DataFrame({"id": [1], "date": ["2024-01-01"]})
        mock_engine.table_exists.return_value = True

        config = NodeConfig(
            name="partition_node",
            read={"connection": "src", "format": "csv", "path": "src.csv"},
            write={
                "connection": "dst",
                "format": "delta",
                "table": "output_table",
                "partition_by": ["date"],
            },
        )

        executor = NodeExecutor(mock_context, mock_engine, connections)

        with patch.object(executor, "_execute_read_phase", return_value=(df, None)):
            executor.execute(config)

        mock_engine.write.assert_called_once()
        _, kwargs = mock_engine.write.call_args
        assert kwargs["options"].get("partition_by") == ["date"]

    def test_zorder_by_added_to_write_options_for_delta(
        self, mock_context, mock_engine, connections
    ):
        """Test that zorder_by is added to write_options for Delta format."""
        df = pd.DataFrame({"id": [1], "customer_id": [100]})
        mock_engine.table_exists.return_value = True

        config = NodeConfig(
            name="zorder_node",
            read={"connection": "src", "format": "csv", "path": "src.csv"},
            write={
                "connection": "dst",
                "format": "delta",
                "table": "output_table",
                "zorder_by": ["customer_id"],
            },
        )

        executor = NodeExecutor(mock_context, mock_engine, connections)

        with patch.object(executor, "_execute_read_phase", return_value=(df, None)):
            executor.execute(config)

        mock_engine.write.assert_called_once()
        _, kwargs = mock_engine.write.call_args
        assert kwargs["options"].get("zorder_by") == ["customer_id"]

    def test_merge_schema_sets_mergeSchema_option_for_delta(
        self, mock_context, mock_engine, connections
    ):
        """Test that merge_schema sets mergeSchema option for Delta format."""
        df = pd.DataFrame({"id": [1], "new_col": ["x"]})
        mock_engine.table_exists.return_value = True

        config = NodeConfig(
            name="merge_schema_node",
            read={"connection": "src", "format": "csv", "path": "src.csv"},
            write={
                "connection": "dst",
                "format": "delta",
                "table": "output_table",
                "merge_schema": True,
            },
        )

        executor = NodeExecutor(mock_context, mock_engine, connections)

        with patch.object(executor, "_execute_read_phase", return_value=(df, None)):
            executor.execute(config)

        mock_engine.write.assert_called_once()
        _, kwargs = mock_engine.write.call_args
        assert kwargs["options"].get("mergeSchema") is True


class TestPrePostSql:
    """Tests for pre_sql and post_sql in NodeConfig."""

    def test_pre_sql_executed_before_read(self, mock_context, mock_engine, connections):
        """Test that pre_sql statements are executed before node runs."""
        df = pd.DataFrame({"id": [1]})
        mock_engine.read.return_value = df
        mock_engine.execute_sql.return_value = None

        config = NodeConfig(
            name="pre_sql_node",
            pre_sql=["CREATE TABLE IF NOT EXISTS temp_table (id INT)"],
            read={"connection": "src", "format": "csv", "path": "src.csv"},
        )

        executor = NodeExecutor(mock_context, mock_engine, connections)
        executor.execute(config)

        # Verify execute_sql was called with pre_sql
        mock_engine.execute_sql.assert_called()
        calls = mock_engine.execute_sql.call_args_list
        assert any("CREATE TABLE IF NOT EXISTS temp_table" in str(call) for call in calls)

    def test_post_sql_executed_after_write(self, mock_context, mock_engine, connections):
        """Test that post_sql statements are executed after node completes."""
        df = pd.DataFrame({"id": [1]})
        mock_engine.execute_sql.return_value = None
        mock_engine.table_exists.return_value = True

        config = NodeConfig(
            name="post_sql_node",
            read={"connection": "src", "format": "csv", "path": "src.csv"},
            write={"connection": "dst", "format": "csv", "path": "dst.csv"},
            post_sql=["ANALYZE TABLE output_table COMPUTE STATISTICS"],
        )

        executor = NodeExecutor(mock_context, mock_engine, connections)

        with patch.object(executor, "_execute_read_phase", return_value=(df, None)):
            executor.execute(config)

        # Verify execute_sql was called with post_sql
        mock_engine.execute_sql.assert_called()
        calls = mock_engine.execute_sql.call_args_list
        assert any("ANALYZE TABLE output_table" in str(call) for call in calls)


class TestMaterialized:
    """Tests for materialized option in NodeConfig."""

    def test_materialized_view_creates_view(self, mock_context, mock_engine, connections):
        """Test that materialized='view' creates a view instead of writing table."""
        df = pd.DataFrame({"id": [1]})
        mock_engine.create_view = MagicMock()
        mock_engine.table_exists.return_value = True

        config = NodeConfig(
            name="view_node",
            read={"connection": "src", "format": "csv", "path": "src.csv"},
            write={"connection": "dst", "format": "delta", "table": "my_view"},
            materialized="view",
        )

        executor = NodeExecutor(mock_context, mock_engine, connections)

        with patch.object(executor, "_execute_read_phase", return_value=(df, None)):
            executor.execute(config)

        # Verify create_view was called
        mock_engine.create_view.assert_called_once()
        _, kwargs = mock_engine.create_view.call_args
        assert kwargs.get("view_name") == "my_view"

        # Verify write was NOT called
        mock_engine.write.assert_not_called()

    def test_materialized_incremental_uses_append_mode(
        self, mock_context, mock_engine, connections
    ):
        """Test that materialized='incremental' uses append mode for writes."""
        from odibi.config import WriteMode

        df = pd.DataFrame({"id": [1]})
        mock_engine.table_exists.return_value = True

        config = NodeConfig(
            name="incremental_node",
            read={"connection": "src", "format": "csv", "path": "src.csv"},
            write={
                "connection": "dst",
                "format": "delta",
                "table": "output_table",
                "mode": "overwrite",
            },
            materialized="incremental",
        )

        executor = NodeExecutor(mock_context, mock_engine, connections)

        with patch.object(executor, "_execute_read_phase", return_value=(df, None)):
            executor.execute(config)

        mock_engine.write.assert_called_once()
        _, kwargs = mock_engine.write.call_args
        # Mode should be overridden to APPEND
        assert kwargs["mode"] == WriteMode.APPEND

    def test_materialized_table_uses_default_behavior(self, mock_context, mock_engine, connections):
        """Test that materialized='table' uses default write behavior."""
        df = pd.DataFrame({"id": [1]})
        mock_engine.table_exists.return_value = True

        config = NodeConfig(
            name="table_node",
            read={"connection": "src", "format": "csv", "path": "src.csv"},
            write={
                "connection": "dst",
                "format": "delta",
                "table": "output_table",
                "mode": "overwrite",
            },
            materialized="table",
        )

        executor = NodeExecutor(mock_context, mock_engine, connections)

        with patch.object(executor, "_execute_read_phase", return_value=(df, None)):
            executor.execute(config)

        mock_engine.write.assert_called_once()
