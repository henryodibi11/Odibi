from unittest.mock import Mock, patch

import pytest

from odibi.config import (
    IncrementalConfig,
    IncrementalMode,
    NodeConfig,
    ReadConfig,
    WriteConfig,
)
from odibi.node import Node
from odibi.state import StateManager


class TestStatefulIncremental:
    @pytest.fixture
    def mock_engine(self):
        engine = Mock()
        engine.name = "pandas"
        engine.read.return_value = Mock()  # Return a dummy df
        return engine

    @pytest.fixture
    def mock_connections(self):
        return {"src": Mock(), "dst": Mock()}

    def test_stateful_first_run(self, mock_engine, mock_connections):
        """Test first run (no HWM) loads everything."""
        read_config = ReadConfig(
            connection="src",
            format="sql",
            table="source_table",
            incremental=IncrementalConfig(mode=IncrementalMode.STATEFUL, key_column="updated_at"),
        )
        config = NodeConfig(
            name="test_node",
            read=read_config,
            write=WriteConfig(connection="dst", format="delta", table="target"),
        )

        node = Node(config, Mock(), mock_engine, mock_connections)

        # Mock StateManager
        node.state_manager = Mock(spec=StateManager)
        node.state_manager.get_hwm.return_value = None  # No state

        # Patch get_max to return a value so pending_hwm is preserved
        with patch.object(node, "_get_column_max", return_value="2023-01-01"):
            node._execute_read()

            # Verify read options
            call_args = mock_engine.read.call_args
            options = call_args[1]["options"]

            # Should select * (Full Load)
            assert options["query"] == "SELECT * FROM source_table"

            # Verify pending HWM set with NEW max value
            assert node._pending_hwm == ("test_node", "2023-01-01")

    def test_stateful_subsequent_run(self, mock_engine, mock_connections):
        """Test subsequent run filters by HWM."""
        read_config = ReadConfig(
            connection="src",
            format="sql",
            table="source_table",
            incremental=IncrementalConfig(mode=IncrementalMode.STATEFUL, key_column="updated_at"),
        )
        config = NodeConfig(
            name="test_node",
            read=read_config,
            write=WriteConfig(connection="dst", format="delta", table="target"),
        )

        node = Node(config, Mock(), mock_engine, mock_connections)

        # Mock StateManager with existing state
        node.state_manager = Mock(spec=StateManager)
        node.state_manager.get_hwm.return_value = "2023-01-01 10:00:00"

        node._execute_read()

        # Verify read options
        call_args = mock_engine.read.call_args
        options = call_args[1]["options"]

        # Should filter
        assert "WHERE updated_at > '2023-01-01 10:00:00'" in options["query"]

    def test_stateful_with_lag(self, mock_engine, mock_connections):
        """Test watermark lag logic."""
        read_config = ReadConfig(
            connection="src",
            format="sql",
            table="source_table",
            incremental=IncrementalConfig(
                mode=IncrementalMode.STATEFUL, key_column="updated_at", watermark_lag="2h"
            ),
        )
        config = NodeConfig(
            name="test_node",
            read=read_config,
            write=WriteConfig(connection="dst", format="delta", table="target"),
        )

        node = Node(config, Mock(), mock_engine, mock_connections)

        # Mock StateManager
        node.state_manager = Mock(spec=StateManager)
        node.state_manager.get_hwm.return_value = "2023-01-01 12:00:00"

        node._execute_read()

        # Verify read options
        call_args = mock_engine.read.call_args
        options = call_args[1]["options"]

        # 12:00 - 2h = 10:00
        # We expect roughly 10:00:00. Date parsing might vary slightly but string check should pass
        assert "WHERE updated_at > '2023-01-01 10:00:00" in options["query"]

    def test_hwm_update_flow(self, mock_engine, mock_connections):
        """Test that HWM is updated after execution."""
        read_config = ReadConfig(
            connection="src",
            format="sql",
            table="source_table",
            incremental=IncrementalConfig(mode=IncrementalMode.STATEFUL, key_column="id"),
        )
        config = NodeConfig(
            name="test_node",
            read=read_config,
            write=WriteConfig(connection="dst", format="delta", table="target"),
        )

        # Mock DataFrame max value
        mock_df = Mock()
        mock_engine.read.return_value = mock_df

        # Create node
        node = Node(config, Mock(), mock_engine, mock_connections)
        node.state_manager = Mock(spec=StateManager)
        node.state_manager.get_hwm.return_value = 100

        # Mock helper _get_column_max
        with patch.object(node, "_get_column_max", return_value=150) as mock_get_max:
            # 1. Execute Read (should calculate max)
            node._execute_read()

            assert node._pending_hwm == ("test_node", 150)
            mock_get_max.assert_called_with(mock_df, "id")

            # 2. Execute Attempt (should commit state)
            # We need to mock other phases to pass
            node._execute_transform_phase = Mock(return_value=mock_df)
            node._execute_validation_phase = Mock()
            node._execute_write_phase = Mock()
            node._collect_metadata = Mock(return_value={})
            node._get_schema = Mock(return_value={})  # used in execute_attempt

            node._execute_attempt()

            # Verify state_manager.set_hwm called
            node.state_manager.set_hwm.assert_called_with("test_node", 150)
