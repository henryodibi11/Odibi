from unittest.mock import MagicMock, patch

import pytest

from odibi.config import (
    AutoOptimizeConfig,
    NodeConfig,
)
from odibi.node import Node


@pytest.fixture
def mock_context():
    return MagicMock()


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    # Engine must have a name attribute for logic checks (e.g. Spark vs Pandas)
    engine.name = "pandas"
    # Mock table_exists to return True so write mode calculation proceeds if needed
    engine.table_exists.return_value = True
    return engine


@pytest.fixture
def mock_connections():
    return {"output_conn": MagicMock(get_path=lambda p: f"/tmp/{p}")}


def test_auto_optimize_enabled_boolean(mock_context, mock_engine, mock_connections):
    """Test auto_optimize: true shorthand."""
    config = NodeConfig(
        name="test_node",
        write={
            "connection": "output_conn",
            "format": "delta",
            "path": "output_table",
            "auto_optimize": True,
        },
    )

    node = Node(config, mock_context, mock_engine, mock_connections)

    # Mock execute_write to do nothing
    # node._execute_write = MagicMock() # Removed as it doesn't exist on Node

    # Mock engine write to avoid side effects
    mock_engine.write = MagicMock()

    # Execute write phase
    node.executor._execute_write_phase(config, MagicMock(), None)

    # Verify maintain_table called
    mock_engine.maintain_table.assert_called_once()
    call_args = mock_engine.maintain_table.call_args

    # Check config argument
    opt_config = call_args.kwargs["config"]
    assert isinstance(opt_config, AutoOptimizeConfig)
    assert opt_config.enabled is True
    assert opt_config.vacuum_retention_hours == 168  # default


def test_auto_optimize_custom_config(mock_context, mock_engine, mock_connections):
    """Test auto_optimize with custom config."""
    config = NodeConfig(
        name="test_node",
        write={
            "connection": "output_conn",
            "format": "delta",
            "path": "output_table",
            "auto_optimize": {"enabled": True, "vacuum_retention_hours": 24},
        },
    )

    node = Node(config, mock_context, mock_engine, mock_connections)
    # node._execute_write = MagicMock() # Removed
    mock_engine.write = MagicMock()
    node.executor._execute_write_phase(config, MagicMock(), None)

    mock_engine.maintain_table.assert_called_once()
    opt_config = mock_engine.maintain_table.call_args.kwargs["config"]
    assert opt_config.vacuum_retention_hours == 24


def test_auto_optimize_disabled(mock_context, mock_engine, mock_connections):
    """Test auto_optimize: false."""
    config = NodeConfig(
        name="test_node",
        write={
            "connection": "output_conn",
            "format": "delta",
            "path": "output_table",
            "auto_optimize": False,
        },
    )

    node = Node(config, mock_context, mock_engine, mock_connections)
    # node._execute_write = MagicMock() # Removed
    mock_engine.write = MagicMock()
    node.executor._execute_write_phase(config, MagicMock(), None)

    mock_engine.maintain_table.assert_not_called()


def test_auto_optimize_none(mock_context, mock_engine, mock_connections):
    """Test auto_optimize: None (default)."""
    config = NodeConfig(
        name="test_node",
        write={"connection": "output_conn", "format": "delta", "path": "output_table"},
    )

    node = Node(config, mock_context, mock_engine, mock_connections)
    # node._execute_write = MagicMock() # Removed
    mock_engine.write = MagicMock()
    node.executor._execute_write_phase(config, MagicMock(), None)

    mock_engine.maintain_table.assert_not_called()


# --- Engine Implementation Tests ---


def test_spark_engine_maintain():
    """Test SparkEngine.maintain_table SQL generation."""
    from odibi.engine.spark_engine import SparkEngine

    mock_spark = MagicMock()
    engine = SparkEngine(spark_session=mock_spark)

    conn = MagicMock()
    conn.get_path.return_value = "/mnt/delta/table"

    config = AutoOptimizeConfig(enabled=True, vacuum_retention_hours=48)

    engine.maintain_table(conn, "delta", path="table", config=config)

    # Verify OPTIMIZE and VACUUM calls
    assert mock_spark.sql.call_count == 2
    calls = [c[0][0] for c in mock_spark.sql.call_args_list]
    assert "OPTIMIZE delta.`/mnt/delta/table`" in calls
    assert "VACUUM delta.`/mnt/delta/table` RETAIN 48 HOURS" in calls


def test_pandas_engine_maintain_deltalake():
    """Test PandasEngine.maintain_table using deltalake library."""
    import sys

    from odibi.engine.pandas_engine import PandasEngine

    engine = PandasEngine()
    conn = MagicMock()
    conn.get_path.return_value = "/tmp/delta/table"

    config = AutoOptimizeConfig(enabled=True, vacuum_retention_hours=48)

    # Mock the deltalake module
    mock_deltalake = MagicMock()
    MockDT = MagicMock()
    mock_deltalake.DeltaTable = MockDT

    # We need to ensure deltalake is not in sys.modules or is replaced
    with patch.dict(sys.modules, {"deltalake": mock_deltalake}):
        engine.maintain_table(conn, "delta", path="table", config=config)

        mock_dt_instance = MockDT.return_value

        # Verify optimize.compact() called
        mock_dt_instance.optimize.compact.assert_called_once()

        # Verify vacuum called
        mock_dt_instance.vacuum.assert_called_once_with(
            retention_hours=48, enforce_retention_duration=True, dry_run=False
        )
