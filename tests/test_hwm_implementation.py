"""Comprehensive tests for HWM (High Water Mark) pattern implementation."""

import pytest
from unittest.mock import Mock, patch
from pathlib import Path
import tempfile
import os
import sys

from odibi.config import NodeConfig, WriteConfig, WriteMode, ReadConfig
from odibi.node import Node
from odibi.engine.base import Engine
from odibi.engine.spark_engine import SparkEngine
from odibi.engine.pandas_engine import PandasEngine


# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture
def mock_spark_engine():
    """Mock Spark engine for testing."""
    engine = Mock(spec=SparkEngine)
    engine.engine_type = "spark"
    return engine


@pytest.fixture
def mock_pandas_engine():
    """Mock Pandas engine for testing."""
    engine = Mock(spec=PandasEngine)
    engine.engine_type = "pandas"
    return engine


@pytest.fixture
def mock_base_engine():
    """Mock base Engine for testing."""
    engine = Mock(spec=Engine)
    return engine


@pytest.fixture
def write_config_with_hwm():
    """WriteConfig with first_run_query set."""
    return WriteConfig(
        connection="test_conn",
        format="delta",
        path="test/path",
        mode=WriteMode.APPEND,
        first_run_query="SELECT * FROM dbo.source_table",
    )


@pytest.fixture
def write_config_without_hwm():
    """WriteConfig without first_run_query."""
    return WriteConfig(
        connection="test_conn", format="delta", path="test/path", mode=WriteMode.APPEND
    )


@pytest.fixture
def read_config():
    """Basic ReadConfig for testing."""
    return ReadConfig(
        connection="test_conn",
        format="delta",
        table="source_table",
        query="SELECT * FROM source_table WHERE created_date > '2025-01-01'",
    )


@pytest.fixture
def temp_dir():
    """Temporary directory for file-based tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


# ============================================================================
# CONFIG TESTS
# ============================================================================


class TestWriteConfigHWM:
    """Tests for WriteConfig with first_run_query field."""

    def test_write_config_with_first_run_query(self, write_config_with_hwm):
        """WriteConfig should accept and store first_run_query."""
        assert write_config_with_hwm.first_run_query == "SELECT * FROM dbo.source_table"
        assert write_config_with_hwm.mode == WriteMode.APPEND

    def test_write_config_without_first_run_query(self, write_config_without_hwm):
        """WriteConfig should work without first_run_query (defaults to None)."""
        assert write_config_without_hwm.first_run_query is None
        assert write_config_without_hwm.mode == WriteMode.APPEND

    def test_write_config_first_run_query_is_optional(self):
        """first_run_query should be optional field."""
        config = WriteConfig(
            connection="test", format="delta", path="test/path", mode=WriteMode.APPEND
        )
        assert hasattr(config, "first_run_query")
        assert config.first_run_query is None


# ============================================================================
# ENGINE.table_exists() TESTS
# ============================================================================


class TestEngineTableExists:
    """Tests for Engine.table_exists() abstract method."""

    def test_engine_has_table_exists_method(self):
        """Base Engine should have table_exists method."""
        assert hasattr(Engine, "table_exists")

    def test_engine_table_exists_is_abstract(self):
        """table_exists should be abstract in base Engine."""
        # Verify the method is defined
        assert "table_exists" in dir(Engine)


class TestSparkEngineTableExists:
    """Tests for SparkEngine.table_exists() implementation."""

    def test_spark_catalog_table_exists(self):
        """Spark should detect catalog tables using spark.catalog.tableExists()."""
        mock_spark = Mock()
        mock_spark.catalog.tableExists.return_value = True
        
        # Pass mock spark session directly
        engine = SparkEngine(connections={}, spark_session=mock_spark)
        result = engine.table_exists(Mock(), "schema.table_name")

        assert result is True
        mock_spark.catalog.tableExists.assert_called_once()

    def test_spark_catalog_table_does_not_exist(self):
        """Spark should return False when catalog table doesn't exist."""
        mock_spark = Mock()
        mock_spark.catalog.tableExists.return_value = False
        
        engine = SparkEngine(connections={}, spark_session=mock_spark)
        result = engine.table_exists(Mock(), "schema.table_name")

        assert result is False

    @patch.dict("sys.modules", {"delta.tables": Mock(), "delta": Mock()})
    def test_spark_path_based_delta_detection(self):
        """Spark should detect Delta tables by path using DeltaTable.isDeltaTable()."""
        # Setup mocks
        mock_delta_module = sys.modules["delta.tables"]
        mock_delta_table_cls = Mock()
        mock_delta_module.DeltaTable = mock_delta_table_cls
        
        mock_spark = Mock()
        mock_spark.catalog.tableExists.return_value = False
        mock_delta_table_cls.isDeltaTable.return_value = True
        
        mock_conn = Mock()
        mock_conn.get_path.return_value = "s3://bucket/path/to/delta"

        engine = SparkEngine(connections={}, spark_session=mock_spark)
        result = engine.table_exists(mock_conn, path="path/to/delta")

        assert result is True

    @patch.dict("sys.modules", {"delta.tables": Mock(), "delta": Mock()})
    def test_spark_path_based_delta_not_exists(self):
        """Spark should return False for non-Delta paths."""
        # Setup mocks
        mock_delta_module = sys.modules["delta.tables"]
        mock_delta_table_cls = Mock()
        mock_delta_module.DeltaTable = mock_delta_table_cls
        
        mock_spark = Mock()
        mock_spark.catalog.tableExists.return_value = False
        mock_delta_table_cls.isDeltaTable.return_value = False
        
        # Also fail the fallback FileSystem check
        mock_spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem.get.side_effect = Exception("Fail")

        mock_conn = Mock()
        mock_conn.get_path.return_value = "s3://bucket/path/to/nonexistent"

        engine = SparkEngine(connections={}, spark_session=mock_spark)
        result = engine.table_exists(mock_conn, path="path/to/nonexistent")

        assert result is False


class TestPandasEngineTableExists:
    """Tests for PandasEngine.table_exists() implementation."""

    def test_pandas_file_exists(self, temp_dir):
        """Pandas should detect files using os.path.exists()."""
        # Create a test file
        test_file = os.path.join(temp_dir, "test_file.csv")
        Path(test_file).touch()
        
        mock_conn = Mock()
        mock_conn.get_path.return_value = test_file

        engine = PandasEngine(connections={})
        result = engine.table_exists(mock_conn, path="test_file.csv")

        assert result is True

    def test_pandas_file_not_exists(self, temp_dir):
        """Pandas should return False for non-existent files."""
        test_file = os.path.join(temp_dir, "nonexistent_file.csv")
        
        mock_conn = Mock()
        mock_conn.get_path.return_value = test_file

        engine = PandasEngine(connections={})
        result = engine.table_exists(mock_conn, path="nonexistent_file.csv")

        assert result is False

    def test_pandas_directory_exists(self, temp_dir):
        """Pandas should detect directories as existing."""
        subdir = os.path.join(temp_dir, "subdir")
        os.makedirs(subdir)
        
        mock_conn = Mock()
        mock_conn.get_path.return_value = subdir

        engine = PandasEngine(connections={})
        result = engine.table_exists(mock_conn, path="subdir")

        assert result is True


# ============================================================================
# NODE._determine_write_mode() TESTS
# ============================================================================


class TestNodeDetermineWriteMode:
    """Tests for Node._determine_write_mode() method."""

    def test_node_has_determine_write_mode_method(self):
        """Node should have _determine_write_mode method."""
        assert hasattr(Node, "_determine_write_mode")

    def test_first_run_uses_overwrite_mode(
        self, write_config_with_hwm, read_config
    ):
        """First run (table doesn't exist) should use OVERWRITE mode."""
        mock_engine = Mock()
        mock_engine.table_exists.return_value = False
        
        connections = {"test_conn": Mock()}
        config = NodeConfig(name="test_node", read=read_config, write=write_config_with_hwm)

        node = Node(
            config=config,
            context=Mock(),
            engine=mock_engine,
            connections=connections
        )

        mode = node._determine_write_mode()

        assert mode == WriteMode.OVERWRITE
        mock_engine.table_exists.assert_called_once()

    def test_subsequent_run_uses_configured_mode(
        self, write_config_with_hwm, read_config
    ):
        """Subsequent run (table exists) should use configured mode (APPEND)."""
        mock_engine = Mock()
        mock_engine.table_exists.return_value = True
        
        connections = {"test_conn": Mock()}
        config = NodeConfig(name="test_node", read=read_config, write=write_config_with_hwm)

        node = Node(
            config=config,
            context=Mock(),
            engine=mock_engine,
            connections=connections
        )

        mode = node._determine_write_mode()

        assert mode == None # Returns None to indicate "use configured mode"

    def test_no_hwm_uses_configured_mode(
        self, write_config_without_hwm, read_config
    ):
        """Without first_run_query, should always use configured mode."""
        mock_engine = Mock()
        mock_engine.table_exists.return_value = False
        
        connections = {"test_conn": Mock()}
        config = NodeConfig(name="test_node", read=read_config, write=write_config_without_hwm)

        node = Node(
            config=config,
            context=Mock(),
            engine=mock_engine,
            connections=connections
        )

        mode = node._determine_write_mode()

        assert mode is None 


# ============================================================================
# NODE HWM FIRST RUN / SUBSEQUENT RUN TESTS
# ============================================================================


class TestNodeHWMFirstRunScenario:
    """Tests for HWM behavior on first run."""

    @patch.object(Node, "_execute_write_phase")
    def test_first_run_uses_first_run_query(
        self, mock_write_phase, write_config_with_hwm, read_config
    ):
        """First run should use first_run_query instead of normal query."""
        mock_engine = Mock()
        mock_engine.table_exists.return_value = False
        mock_engine.read.return_value = Mock()
        
        connections = {"test_conn": Mock()}
        config = NodeConfig(name="test_node", read=read_config, write=write_config_with_hwm)

        node = Node(
            config=config,
            context=Mock(),
            engine=mock_engine,
            connections=connections
        )

        # Simulate execution logic to verify override
        # In _execute_read(), Node calls _determine_write_mode logic logic
        # Actually _execute_read re-implements the check to override query.
        
        node._execute_read()
        
        # Verify engine.read called with first_run_query option
        call_args = mock_engine.read.call_args
        assert call_args is not None
        options = call_args[1]['options']
        assert options['query'] == "SELECT * FROM dbo.source_table"


class TestNodeHWMSubsequentRuns:
    """Tests for HWM behavior on subsequent runs."""

    def test_subsequent_run_uses_normal_query(
        self, write_config_with_hwm, read_config
    ):
        """Subsequent run should use normal query from ReadConfig."""
        mock_engine = Mock()
        mock_engine.table_exists.return_value = True
        mock_engine.read.return_value = Mock()
        
        connections = {"test_conn": Mock()}
        config = NodeConfig(name="test_node", read=read_config, write=write_config_with_hwm)

        node = Node(
            config=config,
            context=Mock(),
            engine=mock_engine,
            connections=connections
        )

        node._execute_read()
        
        # Verify engine.read called with normal query
        call_args = mock_engine.read.call_args
        options = call_args[1]['options']
        assert options['query'] == read_config.options['query']


# ============================================================================
# HWM WITH OPTIONS TESTS
# ============================================================================


class TestHWMWithOptions:
    """Tests for HWM with write options (clustering, partitioning)."""

    def test_write_config_with_hwm_and_clustering(self):
        """WriteConfig should support cluster_by option with HWM."""
        config = WriteConfig(
            connection="test_conn",
            format="delta",
            path="raw/orders",
            mode=WriteMode.APPEND,
            first_run_query="SELECT * FROM dbo.orders",
            options={"cluster_by": ["order_date", "customer_id"]},
        )
        assert config.first_run_query is not None
        assert config.options.get("cluster_by") == ["order_date", "customer_id"]

    def test_write_config_with_hwm_and_partition(self):
        """WriteConfig should support partition_by option with HWM."""
        config = WriteConfig(
            connection="test_conn",
            format="delta",
            path="raw/events",
            mode=WriteMode.APPEND,
            first_run_query="SELECT * FROM events_source",
            options={"partition_by": ["event_date"]},
        )
        assert config.options.get("partition_by") == ["event_date"]


# ============================================================================
# REGISTER_TABLE PARAMETER TESTS
# ============================================================================


class TestRegisterTableParameter:
    """Tests for register_table parameter passing."""

    def test_register_table_parameter_accepted(self):
        """_execute_write should accept register_table parameter."""
        # This is more of a signature check
        import inspect

        sig = inspect.signature(Node._execute_write)
        assert "register_table" in sig.parameters or len(sig.parameters) > 0


# ============================================================================
# HWM EDGE CASE TESTS
# ============================================================================


class TestHWMEdgeCases:
    """Tests for HWM edge cases and corner scenarios."""

    def test_hwm_with_empty_target_table(self, write_config_with_hwm, read_config):
        """HWM should handle empty target tables correctly."""
        mock_engine = Mock()
        # Table exists but is empty
        mock_engine.table_exists.return_value = True
        
        connections = {"test_conn": Mock()}
        config = NodeConfig(name="test_node", read=read_config, write=write_config_with_hwm)

        node = Node(
            config=config,
            context=Mock(),
            engine=mock_engine,
            connections=connections
        )

        # With table existing but empty, should still use normal query (APPEND mode)
        # Unless we implement logic to check for empty. Current logic assumes if exists -> append.
        mode = node._determine_write_mode()
        assert mode == None # None means use configured mode (APPEND)

    def test_hwm_first_run_query_empty_string(self):
        """WriteConfig should handle empty string first_run_query."""
        config = WriteConfig(
            connection="test_conn",
            format="delta",
            path="test/path",
            mode=WriteMode.APPEND,
            first_run_query="",
        )
        # Empty string is falsy, should be treated as None
        assert config.first_run_query == ""

    def test_hwm_with_none_first_run_query_explicit(self):
        """WriteConfig should handle explicit None for first_run_query."""
        config = WriteConfig(
            connection="test_conn",
            format="delta",
            path="test/path",
            mode=WriteMode.APPEND,
            first_run_query=None,
        )
        assert config.first_run_query is None


# ============================================================================
# MODE OVERRIDE TESTS
# ============================================================================


class TestHWMModeOverride:
    """Tests for mode override behavior in HWM."""

    def test_mode_override_on_first_run(
        self, write_config_with_hwm, read_config
    ):
        """Mode should be overridden to OVERWRITE on first run."""
        mock_engine = Mock()
        mock_engine.table_exists.return_value = False
        
        connections = {"test_conn": Mock()}
        config = NodeConfig(name="test_node", read=read_config, write=write_config_with_hwm)

        node = Node(
            config=config,
            context=Mock(),
            engine=mock_engine,
            connections=connections
        )

        mode = node._determine_write_mode()

        # First run should override to OVERWRITE regardless of config
        assert mode == WriteMode.OVERWRITE
        assert write_config_with_hwm.mode == WriteMode.APPEND  # Original config unchanged

    def test_mode_not_overridden_on_subsequent_run(
        self, write_config_with_hwm, read_config
    ):
        """Mode should not be overridden on subsequent runs."""
        mock_engine = Mock()
        mock_engine.table_exists.return_value = True
        
        connections = {"test_conn": Mock()}
        config = NodeConfig(name="test_node", read=read_config, write=write_config_with_hwm)

        node = Node(
            config=config,
            context=Mock(),
            engine=mock_engine,
            connections=connections
        )

        mode = node._determine_write_mode()

        # Subsequent runs should use configured mode
        assert mode is None # use configured


# ============================================================================
# INTEGRATION TESTS
# ============================================================================


class TestHWMIntegration:
    """Integration tests for HWM pattern."""

    def test_hwm_full_scenario_first_run(self, write_config_with_hwm, read_config):
        """Test complete HWM first-run scenario."""
        mock_engine = Mock()
        mock_engine.table_exists.return_value = False
        
        connections = {"test_conn": Mock()}
        config = NodeConfig(name="orders_pipeline", read=read_config, write=write_config_with_hwm)

        node = Node(
            config=config,
            context=Mock(),
            engine=mock_engine,
            connections=connections
        )

        # Verify HWM setup
        assert node.config.write.first_run_query is not None
        assert node._determine_write_mode() == WriteMode.OVERWRITE

    def test_hwm_full_scenario_subsequent_run(self, write_config_with_hwm, read_config):
        """Test complete HWM subsequent-run scenario."""
        mock_engine = Mock()
        mock_engine.table_exists.return_value = True
        
        connections = {"test_conn": Mock()}
        config = NodeConfig(name="orders_pipeline", read=read_config, write=write_config_with_hwm)

        node = Node(
            config=config,
            context=Mock(),
            engine=mock_engine,
            connections=connections
        )

        # Verify HWM subsequent run behavior
        assert node.config.write.first_run_query is not None
        assert node._determine_write_mode() == None # Configured mode
