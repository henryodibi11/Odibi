import pytest
from unittest.mock import MagicMock
from odibi.config import NodeConfig, ReadConfig, RetryConfig
from odibi.context import PandasContext
from odibi.node import Node
from odibi.exceptions import NodeExecutionError


class TestNodeRetry:
    @pytest.fixture
    def mock_engine(self):
        engine = MagicMock()
        # Configure engine to fail twice then succeed
        engine.read.side_effect = [
            Exception("Network glitch"),
            Exception("Network glitch 2"),
            MagicMock(name="dataframe"),
        ]
        # Add required schema methods
        engine.get_schema.return_value = ["col1"]
        engine.get_shape.return_value = (10, 1)
        engine.count_rows.return_value = 10
        return engine

    @pytest.fixture
    def context(self):
        return PandasContext()

    @pytest.fixture
    def connections(self):
        return {"local": MagicMock()}

    @pytest.fixture
    def node_config(self):
        return NodeConfig(
            name="retry_node", read=ReadConfig(connection="local", format="csv", path="data.csv")
        )

    def test_node_retries_on_failure(self, mock_engine, context, connections, node_config):
        """Test that node retries execution when configured."""
        retry_config = RetryConfig(enabled=True, max_attempts=3, backoff="constant")

        node = Node(
            config=node_config,
            context=context,
            engine=mock_engine,
            connections=connections,
            retry_config=retry_config,
        )

        # Execute
        result = node.execute()

        # Should succeed after 3 attempts (2 failures, 1 success)
        assert result.success is True
        assert mock_engine.read.call_count == 3
        assert result.metadata["attempts"] == 3

    def test_node_fails_after_max_attempts(self, mock_engine, context, connections, node_config):
        """Test that node fails if max attempts reached."""
        retry_config = RetryConfig(enabled=True, max_attempts=2, backoff="constant")

        node = Node(
            config=node_config,
            context=context,
            engine=mock_engine,
            connections=connections,
            retry_config=retry_config,
        )

        # Execute (mock fails twice, so with max_attempts=2 it should fail)
        result = node.execute()

        assert result.success is False
        assert mock_engine.read.call_count == 2
        assert isinstance(result.error, NodeExecutionError)
