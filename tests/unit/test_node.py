"""Unit tests for Node class - orchestration and retry logic."""

from unittest.mock import MagicMock, call, patch

import pytest

from odibi.config import NodeConfig, RetryConfig
from odibi.node import Node, NodeResult


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


@pytest.fixture
def mock_executor():
    return MagicMock()


@pytest.fixture
def basic_config():
    return NodeConfig(
        name="test_node",
        read={"connection": "src", "format": "csv", "path": "input.csv"},
        write={"connection": "dst", "format": "csv", "path": "output.csv"},
    )


class TestNodeInitialization:
    """Tests for Node class initialization."""

    def test_init_basic(self, mock_context, mock_engine, connections, basic_config):
        """Test basic Node initialization."""
        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
        )

        assert node.config == basic_config
        assert node.context == mock_context
        assert node.engine == mock_engine
        assert node.connections == connections
        assert node.dry_run is False
        assert isinstance(node.retry_config, RetryConfig)
        assert node._cached_result is None

    def test_init_with_catalog_manager(self, mock_context, mock_engine, connections, basic_config):
        """Test Node initialization with catalog manager."""
        catalog_manager = MagicMock()
        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
            catalog_manager=catalog_manager,
        )

        assert node.catalog_manager == catalog_manager

    def test_init_with_retry_config(self, mock_context, mock_engine, connections, basic_config):
        """Test Node initialization with custom retry config."""
        retry_config = RetryConfig(enabled=True, max_attempts=5)
        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
            retry_config=retry_config,
        )

        assert node.retry_config == retry_config


class TestNodeVersionHash:
    """Tests for get_version_hash method."""

    def test_get_version_hash_basic(self, mock_context, mock_engine, connections, basic_config):
        """Test version hash generation."""
        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
        )

        hash1 = node.get_version_hash()
        hash2 = node.get_version_hash()

        assert isinstance(hash1, str)
        assert len(hash1) == 32  # MD5 hex length
        assert hash1 == hash2  # Deterministic

    def test_get_version_hash_changes_with_config(
        self, mock_context, mock_engine, connections, basic_config
    ):
        """Test that version hash changes when config changes."""
        node1 = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
        )

        # Modify config
        modified_config = basic_config.model_copy(update={"name": "different_name"})
        node2 = Node(
            config=modified_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
        )

        assert node1.get_version_hash() != node2.get_version_hash()


class TestNodeRestore:
    """Tests for restore method."""

    def test_restore_no_write_config(self, mock_context, mock_engine, connections):
        """Test restore when node has no write config."""
        config = NodeConfig(
            name="read_only_node",
            read={"connection": "src", "format": "csv", "path": "input.csv"},
        )

        node = Node(
            config=config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
        )

        result = node.restore()
        assert result is False

    def test_restore_successful(self, mock_context, mock_engine, connections, basic_config):
        """Test successful restore from persisted state."""
        df = MagicMock()
        mock_engine.read.return_value = df
        mock_engine.count_rows.return_value = 100

        # Enable caching for restore to set _cached_result
        config_with_cache = basic_config.model_copy(update={"cache": True})
        node = Node(
            config=config_with_cache,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
        )

        result = node.restore()

        assert result is True
        mock_context.register.assert_called_once_with(basic_config.name, df)
        assert node._cached_result == df

    def test_restore_with_cache_enabled(self, mock_context, mock_engine, connections, basic_config):
        """Test restore with caching enabled."""
        df = MagicMock()
        mock_engine.read.return_value = df

        config_with_cache = basic_config.model_copy(update={"cache": True})
        node = Node(
            config=config_with_cache,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
        )

        result = node.restore()

        assert result is True
        assert node._cached_result == df

    def test_restore_failed(self, mock_context, mock_engine, connections, basic_config):
        """Test restore when read fails."""
        mock_engine.read.side_effect = Exception("Read failed")

        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
        )

        result = node.restore()

        assert result is False
        mock_context.register.assert_not_called()


class TestNodeExecute:
    """Tests for execute method."""

    def test_execute_successful(self, mock_context, mock_engine, connections, basic_config):
        """Test successful node execution."""
        mock_result = NodeResult(
            node_name="test_node",
            success=True,
            duration=1.5,
            rows_processed=100,
        )

        with patch.object(Node, "_execute_with_retries", return_value=mock_result) as mock_execute:
            node = Node(
                config=basic_config,
                context=mock_context,
                engine=mock_engine,
                connections=connections,
            )

            result = node.execute()

            assert result == mock_result
            assert result.metadata["version_hash"] == node.get_version_hash()
            mock_execute.assert_called_once()

    def test_execute_with_run_id(self, mock_context, mock_engine, connections, basic_config):
        """Test execution with run_id generates run record."""
        mock_result = NodeResult(
            node_name="test_node",
            success=True,
            duration=1.0,
            rows_processed=50,
        )

        with patch.object(Node, "_execute_with_retries", return_value=mock_result):
            node = Node(
                config=basic_config,
                context=mock_context,
                engine=mock_engine,
                connections=connections,
                run_id="test-run-123",
            )

            result = node.execute()

            assert "_run_record" in result.metadata
            run_record = result.metadata["_run_record"]
            # run_id in run_record is always a new UUID, not the passed run_id
            assert isinstance(run_record["run_id"], str)
            assert len(run_record["run_id"]) == 36  # UUID length
            assert run_record["node_name"] == "test_node"
            assert run_record["status"] == "SUCCESS"

    def test_execute_failure_logs_to_catalog(
        self, mock_context, mock_engine, connections, basic_config
    ):
        """Test that execution failure logs to catalog manager."""
        catalog_manager = MagicMock()
        mock_result = NodeResult(
            node_name="test_node",
            success=False,
            duration=0.5,
            error=Exception("Test error"),
        )

        with patch.object(Node, "_execute_with_retries", return_value=mock_result):
            node = Node(
                config=basic_config,
                context=mock_context,
                engine=mock_engine,
                connections=connections,
                catalog_manager=catalog_manager,
                run_id="test-run-456",
                pipeline_name="test_pipeline",
            )

            node.execute()

            # Verify failure was logged
            catalog_manager.log_failure.assert_called_once()
            call_args = catalog_manager.log_failure.call_args
            assert call_args[1]["run_id"] == "test-run-456"
            assert call_args[1]["pipeline_name"] == "test_pipeline"
            assert call_args[1]["node_name"] == "test_node"
            assert "Test error" in call_args[1]["error_message"]

    def test_execute_dry_run(self, mock_context, mock_engine, connections, basic_config):
        """Test dry run execution."""
        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
            dry_run=True,
        )

        with patch.object(node.executor, "execute") as mock_executor_execute:
            mock_executor_execute.return_value = NodeResult(
                node_name="test_node",
                success=True,
                duration=0.0,
                rows_processed=0,
                metadata={"dry_run": True},
            )

            result = node.execute()

            assert result.success is True
            assert result.metadata.get("dry_run") is True
            mock_executor_execute.assert_called_once_with(
                basic_config,
                dry_run=True,
                hwm_state=None,
                suppress_error_log=False,
                current_pipeline=None,
            )


class TestNodeExecuteWithRetries:
    """Tests for _execute_with_retries method."""

    def test_execute_with_retries_success_first_attempt(
        self, mock_context, mock_engine, connections, basic_config
    ):
        """Test successful execution on first attempt."""
        mock_result = NodeResult(
            node_name="test_node",
            success=True,
            duration=1.0,
            rows_processed=100,
        )

        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
        )

        with patch.object(node.executor, "execute", return_value=mock_result) as mock_execute:
            result = node._execute_with_retries()

            assert result == mock_result
            assert result.metadata["attempts"] == 1
            assert len(result.metadata["retry_history"]) == 1
            mock_execute.assert_called_once()

    def test_execute_with_retries_success_after_retry(
        self, mock_context, mock_engine, connections, basic_config
    ):
        """Test successful execution after one retry."""
        retry_config = RetryConfig(enabled=True, max_attempts=3)

        # First call fails, second succeeds
        failed_result = NodeResult(
            node_name="test_node",
            success=False,
            duration=0.5,
            error=Exception("Temporary failure"),
        )
        success_result = NodeResult(
            node_name="test_node",
            success=True,
            duration=1.0,
            rows_processed=100,
        )

        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
            retry_config=retry_config,
        )

        with patch.object(
            node.executor, "execute", side_effect=[failed_result, success_result]
        ) as mock_execute:
            result = node._execute_with_retries()

            assert result == success_result
            assert result.metadata["attempts"] == 2
            assert len(result.metadata["retry_history"]) == 2
            assert result.metadata["retry_history"][0]["success"] is False
            assert result.metadata["retry_history"][1]["success"] is True
            assert mock_execute.call_count == 2

    def test_execute_with_retries_all_fail(
        self, mock_context, mock_engine, connections, basic_config
    ):
        """Test execution when all retry attempts fail."""
        retry_config = RetryConfig(enabled=True, max_attempts=2)

        failed_result = NodeResult(
            node_name="test_node",
            success=False,
            duration=0.5,
            error=Exception("Persistent failure"),
        )

        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
            retry_config=retry_config,
        )

        with patch.object(node.executor, "execute", return_value=failed_result) as mock_execute:
            result = node._execute_with_retries()

            assert result.success is False
            assert result.metadata["attempts"] == 2
            assert len(result.metadata["retry_history"]) == 2
            mock_execute.assert_called()

    def test_execute_with_retries_exponential_backoff(
        self, mock_context, mock_engine, connections, basic_config
    ):
        """Test exponential backoff timing."""
        retry_config = RetryConfig(enabled=True, max_attempts=3, backoff="exponential")

        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
            retry_config=retry_config,
        )

        with patch.object(node.executor, "execute", side_effect=Exception("fail")):
            with patch("time.sleep") as mock_sleep:
                node._execute_with_retries()

                # Should have slept for 1s (2^(1-1)), then 2s (2^(2-1))
                mock_sleep.assert_has_calls([call(1), call(2)])

    def test_execute_with_retries_linear_backoff(
        self, mock_context, mock_engine, connections, basic_config
    ):
        """Test linear backoff timing."""
        retry_config = RetryConfig(enabled=True, max_attempts=3, backoff="linear")

        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
            retry_config=retry_config,
        )

        with patch.object(node.executor, "execute", side_effect=Exception("fail")):
            with patch("time.sleep") as mock_sleep:
                node._execute_with_retries()

                # Should have slept for 1s (attempt 1), then 2s (attempt 2)
                mock_sleep.assert_has_calls([call(1), call(2)])

    def test_execute_with_retries_hwm_state(
        self, mock_context, mock_engine, connections, basic_config
    ):
        """Test that HWM state is passed to executor."""
        pytest.skip("HWM state functionality not implemented", allow_module_level=True)

    def test_execute_with_retries_hwm_update_failure_marks_result_failed(
        self, mock_context, mock_engine, connections, basic_config
    ):
        """Test that HWM update failure sets result.success to False."""
        mock_result = NodeResult(
            node_name="test_node",
            success=True,
            duration=1.0,
            rows_processed=100,
            metadata={
                "hwm_pending": True,
                "hwm_update": {"key": "test_hwm", "value": "2024-01-01"},
            },
        )

        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
        )
        node.state_manager = MagicMock()
        hwm_error = RuntimeError("HWM write failed")
        node.state_manager.set_hwm.side_effect = hwm_error

        with patch.object(node.executor, "execute", return_value=mock_result):
            result = node._execute_with_retries()

            assert result.success is False
            assert result.error is hwm_error
            assert result.metadata["hwm_error"] == "HWM write failed"

    def test_execute_with_retries_caches_result_when_enabled(
        self, mock_context, mock_engine, connections, basic_config
    ):
        """Test that successful result is cached when cache is enabled."""
        config_with_cache = basic_config.model_copy(update={"cache": True})
        mock_result = NodeResult(
            node_name="test_node",
            success=True,
            duration=1.0,
            rows_processed=100,
        )
        cached_df = MagicMock()  # The df that gets cached

        node = Node(
            config=config_with_cache,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
        )

        # Mock context.get to return the cached df
        mock_context.get.return_value = cached_df

        with patch.object(node.executor, "execute", return_value=mock_result):
            node._execute_with_retries()

            assert node._cached_result == cached_df
            # Verify result comes from context cache
            mock_context.get.assert_called_with(basic_config.name)

    def test_graceful_failure_applies_exponential_backoff(
        self, mock_context, mock_engine, connections, basic_config
    ):
        """Test that graceful failures (result.success=False) apply exponential backoff."""
        retry_config = RetryConfig(enabled=True, max_attempts=3, backoff="exponential")

        failed_result = NodeResult(
            node_name="test_node",
            success=False,
            duration=0.5,
            error=Exception("Graceful failure"),
        )

        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
            retry_config=retry_config,
        )

        with patch.object(node.executor, "execute", return_value=failed_result):
            with patch("odibi.node.time.sleep") as mock_sleep:
                node._execute_with_retries()

                # Should have slept for 1.0s (2^0), then 2.0s (2^1)
                mock_sleep.assert_has_calls([call(1.0), call(2.0)])
                assert mock_sleep.call_count == 2

    def test_graceful_failure_applies_linear_backoff(
        self, mock_context, mock_engine, connections, basic_config
    ):
        """Test that graceful failures apply linear backoff."""
        retry_config = RetryConfig(enabled=True, max_attempts=3, backoff="linear")

        failed_result = NodeResult(
            node_name="test_node",
            success=False,
            duration=0.5,
            error=Exception("Graceful failure"),
        )

        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
            retry_config=retry_config,
        )

        with patch.object(node.executor, "execute", return_value=failed_result):
            with patch("odibi.node.time.sleep") as mock_sleep:
                node._execute_with_retries()

                # Should have slept for 1.0s (attempt 1), then 2.0s (attempt 2)
                mock_sleep.assert_has_calls([call(1.0), call(2.0)])
                assert mock_sleep.call_count == 2

    def test_graceful_failure_applies_constant_backoff(
        self, mock_context, mock_engine, connections, basic_config
    ):
        """Test that graceful failures apply constant backoff."""
        retry_config = RetryConfig(enabled=True, max_attempts=3, backoff="constant")

        failed_result = NodeResult(
            node_name="test_node",
            success=False,
            duration=0.5,
            error=Exception("Graceful failure"),
        )

        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
            retry_config=retry_config,
        )

        with patch.object(node.executor, "execute", return_value=failed_result):
            with patch("odibi.node.time.sleep") as mock_sleep:
                node._execute_with_retries()

                # Should have slept for 1.0s each time
                mock_sleep.assert_has_calls([call(1.0), call(1.0)])
                assert mock_sleep.call_count == 2

    def test_graceful_failure_no_backoff_on_last_attempt(
        self, mock_context, mock_engine, connections, basic_config
    ):
        """Test that no backoff is applied on the final attempt."""
        retry_config = RetryConfig(enabled=True, max_attempts=2, backoff="exponential")

        failed_result = NodeResult(
            node_name="test_node",
            success=False,
            duration=0.5,
            error=Exception("Graceful failure"),
        )

        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
            retry_config=retry_config,
        )

        with patch.object(node.executor, "execute", return_value=failed_result):
            with patch("odibi.node.time.sleep") as mock_sleep:
                node._execute_with_retries()

                # Only 1 sleep (after attempt 1, not after attempt 2)
                mock_sleep.assert_called_once_with(1.0)


class TestCalculateBackoff:
    """Tests for _calculate_backoff method."""

    @pytest.fixture
    def node(self, mock_context, mock_engine, connections, basic_config):
        """Create a node with exponential backoff by default."""
        return Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
            retry_config=RetryConfig(enabled=True, max_attempts=5, backoff="exponential"),
        )

    def test_exponential_backoff_values(self, node):
        """Test exponential backoff returns 2^(attempt-1)."""
        assert node._calculate_backoff(1) == 1.0
        assert node._calculate_backoff(2) == 2.0
        assert node._calculate_backoff(3) == 4.0
        assert node._calculate_backoff(4) == 8.0

    def test_linear_backoff_values(self, mock_context, mock_engine, connections, basic_config):
        """Test linear backoff returns attempt number as float."""
        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
            retry_config=RetryConfig(enabled=True, max_attempts=5, backoff="linear"),
        )
        assert node._calculate_backoff(1) == 1.0
        assert node._calculate_backoff(2) == 2.0
        assert node._calculate_backoff(3) == 3.0

    def test_constant_backoff_values(self, mock_context, mock_engine, connections, basic_config):
        """Test constant backoff always returns 1.0."""
        node = Node(
            config=basic_config,
            context=mock_context,
            engine=mock_engine,
            connections=connections,
            retry_config=RetryConfig(enabled=True, max_attempts=5, backoff="constant"),
        )
        assert node._calculate_backoff(1) == 1.0
        assert node._calculate_backoff(2) == 1.0
        assert node._calculate_backoff(5) == 1.0

    def test_backoff_returns_float(self, node):
        """Test that backoff always returns a float."""
        result = node._calculate_backoff(1)
        assert isinstance(result, float)
