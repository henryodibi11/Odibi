import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from odibi.config import IncrementalConfig, NodeConfig, ReadConfig, WriteConfig
from odibi.context import Context
from odibi.engine.spark_engine import SparkEngine
from odibi.node import Node


class MockDataFrame:
    """Mock DataFrame class for isinstance checks."""

    pass


@pytest.fixture(scope="module")
def mock_pyspark_modules():
    """Mock PySpark/Delta modules for the duration of these tests."""
    mock_pyspark = MagicMock()
    mock_spark_session_cls = MagicMock()
    mock_pyspark.SparkSession = mock_spark_session_cls

    # Fix for isinstance(df, pyspark.sql.DataFrame)
    mock_pyspark.sql.DataFrame = MockDataFrame

    mock_delta = MagicMock()

    with patch.dict(
        sys.modules,
        {
            "pyspark": mock_pyspark,
            "pyspark.sql": mock_pyspark,
            "delta": mock_delta,
            "delta.tables": mock_delta,
        },
    ):
        yield mock_pyspark


@pytest.fixture
def mock_context(mock_pyspark_modules):
    """Mock Context for Spark tests."""
    context = MagicMock(spec=Context)
    # Mock list_names and get for execute_sql usage
    context.list_names.return_value = []
    return context


@pytest.fixture
def mock_spark_session(mock_pyspark_modules):
    """Mock SparkSession instance."""
    spark = MagicMock()
    spark.version = "3.5.0"
    # Mock catalog
    spark.catalog.tableExists.return_value = False
    # Mock read
    spark.read.format.return_value.options.return_value.load.return_value = MagicMock(
        name="df_load"
    )
    spark.read.format.return_value.option.return_value = spark.read.format.return_value

    # Mock table read
    spark.read.format.return_value.table.return_value = MagicMock(name="df_table")

    return spark


@pytest.fixture
def mock_engine(mock_spark_session):
    """Mock SparkEngine with injected mock session."""
    # Since we mocked pyspark in sys.modules, SparkEngine.__init__ should succeed
    # but we want to bypass the actual builder logic to inject our mock session easily
    # or we can just let it run if our mock_pyspark.SparkSession works.

    # Let's just instantiate it and set the session manually to ensure control
    engine = SparkEngine(spark_session=mock_spark_session)

    # Override table_exists to use our mock logic (default False)
    engine.table_exists = MagicMock(return_value=False)

    # Mock write behavior to return commit info (avoiding delta logic import issues if any)
    # Actually, we want to test that Delta logic IS called.
    # But _get_last_delta_commit_info uses DeltaTable.forPath which we mocked.
    # So we can let it run or mock it. Let's mock it to simplify.
    engine._get_last_delta_commit_info = MagicMock(
        return_value={"version": 1, "timestamp": 0, "operation": "WRITE", "operation_metrics": {}}
    )

    # Mock materialize to be identity (pass-through) to handle MagicMock dataframes
    # This ensures that if NodeExecutor calls materialize(), we get back our mock_df
    engine.materialize = MagicMock(side_effect=lambda df: df)

    return engine


@pytest.fixture
def mock_connections():
    conn = MagicMock()
    conn.type = "local"
    conn.get_path.side_effect = lambda p: f"/tmp/{p}"
    return {"local": conn, "source_db": conn, "bronze_lake": conn}


@pytest.fixture
def frozen_time():
    return datetime(2023, 10, 25, 12, 0, 0)


def test_spark_smart_read_first_run(mock_context, mock_engine, mock_connections, frozen_time):
    """
    Test Smart Read "First Run" on Spark (Target missing -> Full Load).
    """
    # Setup: Target does NOT exist
    mock_engine.table_exists.return_value = False

    # Mock the read DataFrame
    mock_df = MagicMock()
    mock_engine.spark.read.format.return_value.load.return_value = mock_df
    mock_engine.spark.read.format.return_value.option.return_value = (
        mock_engine.spark.read.format.return_value
    )

    config = NodeConfig(
        name="smart_read_first",
        read=ReadConfig(
            connection="local",
            format="parquet",
            path="orders.parquet",
            incremental=IncrementalConfig(column="updated_at", lookback=1, unit="day"),
        ),
        write=WriteConfig(
            connection="local", format="parquet", path="bronze_orders.parquet", mode="append"
        ),
    )

    with patch("odibi.node.datetime") as mock_datetime:
        mock_datetime.now.return_value = frozen_time

        node = Node(config, mock_context, mock_engine, mock_connections)
        result = node.execute()
        if not result.success:
            pytest.fail(f"Node execution failed: {result.error}")

    # Verify: Read called without filter
    # The 'filter' option should NOT be present in the read call options if it's first run
    # SparkEngine.read() applies filter if 'filter' is in options.
    # Node logic: if not target_exists => options['filter'] is NOT set.

    # Check Node passed options to engine.read
    # We can't easily spy on engine.read because we're using the real method (mostly).
    # But we can spy on the spark.read chain.

    # Verify spark.read.load called
    mock_engine.spark.read.format.return_value.load.assert_called()

    # Verify .filter() was NOT called on the dataframe
    mock_df.filter.assert_not_called()


def test_spark_smart_read_subsequent_run(mock_context, mock_engine, mock_connections, frozen_time):
    """
    Test Smart Read "Subsequent Run" on Spark (Target exists -> Incremental Filter).
    Verifies filter_greater_than is called with correct arguments for incremental read.
    """
    mock_engine.table_exists = MagicMock(return_value=True)

    mock_df = MagicMock()
    mock_engine.spark.read.format.return_value.load.return_value = mock_df

    config = NodeConfig(
        name="smart_read_incremental",
        read=ReadConfig(
            connection="local",
            format="parquet",
            path="orders.parquet",
            incremental=IncrementalConfig(column="updated_at", lookback=1, unit="day"),
        ),
        write=WriteConfig(
            connection="local", format="parquet", path="bronze_orders.parquet", mode="append"
        ),
    )

    with (
        patch("odibi.node.datetime") as mock_datetime,
        patch.object(mock_engine, "filter_greater_than", return_value=mock_df) as mock_filter,
    ):
        mock_datetime.now.return_value = frozen_time

        node = Node(config, mock_context, mock_engine, mock_connections)
        node.execute()

        mock_filter.assert_called_once()
        call_args = mock_filter.call_args
        assert call_args[0][1] == "updated_at"


def test_spark_upsert_merge(mock_context, mock_engine, mock_connections):
    """
    Test Upsert (Merge) pattern on Spark.
    Verifies DeltaTable.merge logic is triggered.
    """
    # Setup Inputs
    mock_df = MagicMock()  # Input DF
    mock_engine.read = MagicMock(return_value=mock_df)

    # Access the mocked DeltaTable from sys.modules
    # We imported it at the top via sys.modules injection
    from delta.tables import DeltaTable as MockDeltaTable

    # isDeltaTable = True
    MockDeltaTable.isDeltaTable.return_value = True

    mock_dt = MagicMock()
    MockDeltaTable.forPath.return_value = mock_dt

    # Setup fluent interface for merge
    # alias().merge().whenMatched...
    mock_merge_builder = MagicMock()
    mock_dt.alias.return_value.merge.return_value = mock_merge_builder
    mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
    mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder

    config = NodeConfig(
        name="upsert_node",
        read=ReadConfig(connection="local", format="parquet", path="source.parquet"),
        write=WriteConfig(
            connection="local",
            format="delta",
            path="target_table",
            mode="upsert",
            options={"keys": ["id"]},
        ),
    )

    # Verify Merge Logic
    # target_dt was mocked from DeltaTable.forPath(...)
    # In test_patterns_spark_mock.py, we set up:
    # mock_dt = MagicMock()
    # MockDeltaTable.forPath.return_value = mock_dt

    # SparkEngine.write logic:
    # target_dt = DeltaTable.forPath(...)
    # merge_builder = target_dt.alias("target").merge(df.alias("source"), condition)

    # So alias("target") SHOULD be called on mock_dt.

    # Why fail?
    # "Expected: alias('target')"
    # "Actual: not called."

    # Maybe because 'exists' check returned False?
    # If not exists, it falls back to "overwrite" mode and calls standard write.
    # exists = self.table_exists(...)

    # In test_spark_upsert_merge, we didn't mock table_exists explicitly?
    # mock_engine fixture has `engine.table_exists = MagicMock(return_value=False)` by default.

    # So for this test, we MUST override it to return True.
    # The test didn't do that. So it went to fallback path.

    mock_engine.table_exists.return_value = True

    node = Node(config, mock_context, mock_engine, mock_connections)
    node.execute()

    # merge(source, condition)
    # condition: "target.`id` = source.`id`" (backticks added for safety in implementation)
    call_args = mock_dt.alias.return_value.merge.call_args
    assert call_args is not None
    source_arg, condition_arg = call_args[0]

    # Source should be aliased "source"
    # verify mock_df.alias("source") was called
    mock_df.alias.assert_called_with("source")

    assert condition_arg == "target.`id` = source.`id`"

    # verify execute()
    mock_merge_builder.execute.assert_called()


def test_spark_smart_read_fallback_column(mock_context, mock_engine, mock_connections, frozen_time):
    """
    Test Smart Read fallback (COALESCE) on Spark.
    Verifies filter_coalesce is called with correct arguments for incremental read.
    """
    mock_engine.table_exists = MagicMock(return_value=True)

    mock_df = MagicMock()
    mock_engine.spark.read.format.return_value.load.return_value = mock_df

    config = NodeConfig(
        name="smart_read_fallback",
        read=ReadConfig(
            connection="local",
            format="parquet",
            path="orders.parquet",
            incremental=IncrementalConfig(
                column="updated_at", fallback_column="created_at", lookback=1, unit="day"
            ),
        ),
        write=WriteConfig(
            connection="local", format="parquet", path="bronze_orders.parquet", mode="append"
        ),
    )

    with (
        patch("odibi.node.datetime") as mock_datetime,
        patch.object(mock_engine, "filter_coalesce", return_value=mock_df) as mock_filter,
    ):
        mock_datetime.now.return_value = frozen_time

        node = Node(config, mock_context, mock_engine, mock_connections)
        node.execute()

        mock_filter.assert_called_once()
        call_args = mock_filter.call_args
        assert call_args[0][1] == "updated_at"
        assert call_args[0][2] == "created_at"
        assert call_args[0][3] == ">="


def test_spark_smart_read_sql_jdbc(mock_context, mock_engine, mock_connections, frozen_time):
    """
    Test Smart Read with Spark JDBC (format='sql').
    For SQL sources with incremental config, the filter is pushed down at read time
    via the 'query' option (SQL pushdown) rather than calling df.filter() afterward.
    """
    mock_engine.table_exists.return_value = True

    # Mock JDBC reader chain
    mock_reader = MagicMock()
    mock_engine.spark.read.format.return_value = mock_reader
    mock_reader.options.return_value.load.return_value = MagicMock()

    # Inject get_spark_options on connection mock
    mock_connections["source_db"].get_spark_options = MagicMock(
        return_value={"url": "jdbc:...", "dbtable": "schema.orders", "user": "u"}
    )

    config = NodeConfig(
        name="smart_read_sql",
        read=ReadConfig(
            connection="source_db",
            format="sql",
            table="schema.orders",
            incremental=IncrementalConfig(column="updated_at", lookback=1, unit="day"),
        ),
        write=WriteConfig(connection="local", format="delta", path="bronze_orders", mode="append"),
    )

    with patch("odibi.node.datetime") as mock_datetime:
        mock_datetime.now.return_value = frozen_time
        node = Node(config, mock_context, mock_engine, mock_connections)
        node.execute()

    # Verify spark.read.format("jdbc") was used
    mock_engine.spark.read.format.assert_called_with("jdbc")

    # Verify options passed to options()
    call_args = mock_reader.options.call_args
    assert call_args is not None
    passed_options = call_args.kwargs

    # With SQL pushdown, SparkEngine.read receives 'filter' option and converts it to 'query'
    # The filter should be "`updated_at` >= '2023-10-24 12:00:00'" (rolling window uses >=)
    # SparkEngine builds query: "SELECT * FROM schema.orders WHERE <filter>"
    expected_date = "2023-10-24 12:00:00"
    assert "query" in passed_options, f"Expected 'query' in options but got: {passed_options}"
    assert f"`updated_at` >= '{expected_date}'" in passed_options["query"]
    # dbtable should NOT be present when query is used
    assert "dbtable" not in passed_options


def test_spark_hwm_legacy(mock_context, mock_engine, mock_connections):
    """
    Test Legacy HWM on Spark (first_run_query precedence).
    """
    mock_engine.table_exists.return_value = False  # First run

    mock_reader = MagicMock()
    mock_engine.spark.read.format.return_value = mock_reader
    mock_connections["source_db"].get_spark_options = MagicMock(return_value={"dbtable": "orders"})

    config = NodeConfig(
        name="hwm_legacy",
        read=ReadConfig(
            connection="source_db",
            format="sql",
            query="SELECT * FROM orders WHERE 1=0",  # Standard query
        ),
        write=WriteConfig(
            connection="local",
            format="delta",
            path="bronze",
            mode="append",
            first_run_query="SELECT * FROM orders",  # Override
        ),
    )

    node = Node(config, mock_context, mock_engine, mock_connections)
    node.execute()

    # Verify options
    call_args = mock_reader.options.call_args
    passed_options = call_args.kwargs

    assert passed_options["query"] == "SELECT * FROM orders"
    assert "dbtable" not in passed_options
