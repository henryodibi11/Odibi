import pytest
from unittest.mock import MagicMock, patch
from odibi.engine.spark_engine import SparkEngine


@pytest.fixture
def mock_spark_session():
    # Create a mock SparkSession
    mock = MagicMock()
    # Mock builder pattern
    mock.builder.appName.return_value.getOrCreate.return_value = mock

    # Mock catalog
    mock.catalog = MagicMock()

    return mock


class DataFrame:
    """Mock Spark DataFrame class to pass type checks."""

    def __init__(self):
        self.columns = ["id", "group", "value", "category"]
        self.count = MagicMock(return_value=10)
        self.createOrReplaceTempView = MagicMock()
        self.groupBy = MagicMock()
        self.dropDuplicates = MagicMock()
        self.orderBy = MagicMock()
        self.withColumnRenamed = MagicMock()
        self.fillna = MagicMock()
        self.sample = MagicMock()
        self.drop = MagicMock()
        # Chainable returns
        self.groupBy.return_value = MagicMock()
        self.groupBy.return_value.pivot.return_value = MagicMock()
        self.groupBy.return_value.pivot.return_value.agg.return_value = MagicMock()
        self.withColumnRenamed.return_value = self
        self.dropDuplicates.return_value = self
        self.orderBy.return_value = self
        self.fillna.return_value = self
        self.drop.return_value = self
        self.sample.return_value = self
        self.limit = MagicMock()
        self.limit.return_value = self
        self.collect = MagicMock(return_value=[])
        self.isEmpty = MagicMock(return_value=False)
        self.filter = MagicMock(return_value=self)


@pytest.fixture
def mock_spark_df():
    # Create an instance of our DataFrame class
    return DataFrame()


@pytest.fixture
def spark_engine(mock_spark_session):
    # Pass the mock session directly
    return SparkEngine(spark_session=mock_spark_session)


class TestSparkEngineLogic:

    def test_pivot_logic(self, spark_engine, mock_spark_df):
        """Verify pivot calls correct Spark methods."""
        params = {
            "group_by": ["group"],
            "pivot_column": "category",
            "value_column": "value",
            "agg_func": "sum",
        }

        # Setup mock return chain
        grouped = MagicMock()
        pivoted = MagicMock()

        mock_spark_df.groupBy.return_value = grouped
        grouped.pivot.return_value = pivoted
        pivoted.agg.return_value = MagicMock()

        # Execute
        spark_engine.execute_operation("pivot", params, mock_spark_df)

        # Verify calls
        mock_spark_df.groupBy.assert_called_with("group")
        grouped.pivot.assert_called_with("category")
        pivoted.agg.assert_called_with({"value": "sum"})

    def test_drop_duplicates_logic(self, spark_engine, mock_spark_df):
        """Verify drop_duplicates logic."""
        # 1. No subset
        spark_engine.execute_operation("drop_duplicates", {}, mock_spark_df)
        mock_spark_df.dropDuplicates.assert_called_with()

        # 2. With subset
        params = {"subset": ["group"]}
        spark_engine.execute_operation("drop_duplicates", params, mock_spark_df)
        mock_spark_df.dropDuplicates.assert_called_with(subset=["group"])

    def test_header_normalization(self, spark_engine):
        """Verify boolean header is normalized to string for CSV."""
        mock_reader = MagicMock()
        spark_engine.spark.read.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader  # chaining

        mock_conn = MagicMock()
        mock_conn.get_path.return_value = "s3://bucket/file.csv"

        # Test with boolean header=True
        options = {"header": True, "sep": ","}
        spark_engine.read(mock_conn, "csv", path="file.csv", options=options)

        # Check calls
        # option("header", "true") should be called
        # Note: We can't easily check order without deeper inspection,
        # but we can check if 'true' (str) was passed, not True (bool).

        calls = mock_reader.option.call_args_list

        # Find header call
        header_call = None
        for call in calls:
            if call[0][0] == "header":
                header_call = call
                break

        assert header_call is not None
        assert header_call[0][1] == "true"
        assert isinstance(header_call[0][1], str)

    def test_auto_encoding_trigger(self, spark_engine):
        """Verify auto_encoding logic triggers detection."""
        mock_conn = MagicMock()
        mock_conn.get_path.return_value = "file.csv"

        # Patch the detect_encoding utility
        # Note: We need to patch where it is IMPORTED, which is inside the method
        # Or we can patch sys.modules or similar.
        # Since the import is inside the method `from odibi.utils.encoding import detect_encoding`
        # simple patch("odibi.engine.spark_engine.detect_encoding") won't work if it's not top-level
        # However, patch.dict('sys.modules', ...) is hard.
        # Wait, the code does: `from odibi.utils.encoding import detect_encoding`.
        # If I patch `odibi.utils.encoding.detect_encoding`, it should work.

        with patch("odibi.utils.encoding.detect_encoding") as mock_detect:
            mock_detect.return_value = "latin1"

            options = {"auto_encoding": True}
            spark_engine.read(mock_conn, "csv", path="file.csv", options=options)

            # Should call detect_encoding
            mock_detect.assert_called_once()

            # Should call spark.read...option("encoding", "latin1")
            # Verify call args of option
            mock_reader = spark_engine.spark.read.format.return_value

            encoding_call_found = False
            for call in mock_reader.option.call_args_list:
                if call[0][0] == "encoding" and call[0][1] == "latin1":
                    encoding_call_found = True
                    break

            assert encoding_call_found

    def test_validate_data_logic(self, spark_engine, mock_spark_df):
        """Verify validation logic."""
        # Mock count behavior
        mock_spark_df.isEmpty.return_value = False

        # 1. Check no_nulls
        # We need to mock count_nulls method of engine OR mock sql functions
        # Easier to mock engine.count_nulls if we only test validate_data flow

        with patch.object(spark_engine, "count_nulls") as mock_count_nulls:
            mock_count_nulls.return_value = {"col1": 5}  # 5 nulls

            config = MagicMock()
            config.not_empty = True
            config.no_nulls = ["col1"]
            config.schema_validation = None
            config.ranges = None
            config.allowed_values = None

            failures = spark_engine.validate_data(mock_spark_df, config)

            assert len(failures) == 1
            assert "has 5 null values" in failures[0]

    def test_sort_logic(self, spark_engine, mock_spark_df):
        """Verify sort logic."""
        params = {"by": ["col1"], "ascending": False}

        # Mock desc function import inside method
        with patch("pyspark.sql.functions.desc") as mock_desc:
            mock_col_desc = MagicMock()
            mock_desc.return_value = mock_col_desc

            spark_engine.execute_operation("sort", params, mock_spark_df)

            mock_spark_df.orderBy.assert_called()
            # Check if called with result of desc()
            args = mock_spark_df.orderBy.call_args[0]
            assert args[0] == mock_col_desc

    def test_drop_columns_logic(self, spark_engine, mock_spark_df):
        """Verify drop columns logic."""
        # 1. List of columns
        params = {"columns": ["col1", "col2"]}
        spark_engine.execute_operation("drop", params, mock_spark_df)
        mock_spark_df.drop.assert_called_with("col1", "col2")

        # 2. Single string
        params = {"columns": "col1"}
        spark_engine.execute_operation("drop", params, mock_spark_df)
        mock_spark_df.drop.assert_called_with("col1")

    def test_rename_logic(self, spark_engine, mock_spark_df):
        """Verify rename logic."""
        params = {"columns": {"old1": "new1", "old2": "new2"}}

        # withColumnRenamed returns a new df, so we need to chain mocks
        df1 = MagicMock()
        df2 = MagicMock()
        mock_spark_df.withColumnRenamed.side_effect = [df1, df2]
        df1.withColumnRenamed.return_value = df2

        spark_engine.execute_operation("rename", params, mock_spark_df)

        # Should be called twice
        assert mock_spark_df.withColumnRenamed.call_count >= 1
        # Since dict iteration order, check args exist
        calls = (
            mock_spark_df.withColumnRenamed.call_args_list + df1.withColumnRenamed.call_args_list
        )

        has_old1 = any(c[0][0] == "old1" and c[0][1] == "new1" for c in calls)
        has_old2 = any(c[0][0] == "old2" and c[0][1] == "new2" for c in calls)

        assert has_old1 and has_old2

    def test_fillna_logic(self, spark_engine, mock_spark_df):
        """Verify fillna logic."""
        # 1. Value only
        params = {"value": 0}
        spark_engine.execute_operation("fillna", params, mock_spark_df)
        mock_spark_df.fillna.assert_called_with(0, subset=None)

        # 2. Value and subset
        params = {"value": 0, "subset": ["col1"]}
        spark_engine.execute_operation("fillna", params, mock_spark_df)
        mock_spark_df.fillna.assert_called_with(0, subset=["col1"])

    def test_sample_logic(self, spark_engine, mock_spark_df):
        """Verify sample logic."""
        params = {"frac": 0.1, "random_state": 42, "replace": True}
        spark_engine.execute_operation("sample", params, mock_spark_df)

        mock_spark_df.sample.assert_called_with(withReplacement=True, fraction=0.1, seed=42)

    def test_pipeline_integration_logic(self, mock_spark_session, mock_spark_df):
        """Verify Pipeline orchestrator uses SparkEngine correctly."""
        from odibi.pipeline import Pipeline
        from odibi.config import (
            PipelineConfig,
            NodeConfig,
            ReadConfig,
            WriteConfig,
            TransformConfig,
            TransformStep,
        )

        # Ensure spark.table() returns a DataFrame-like object (for Context.get calls)
        mock_spark_session.table.return_value = mock_spark_df

        # Setup config
        node1 = NodeConfig(
            name="node1",
            read=ReadConfig(connection="local", path="input.csv", format="csv"),
            transform=TransformConfig(
                steps=[TransformStep(operation="drop", params={"columns": ["col1"]})]
            ),
        )
        node2 = NodeConfig(
            name="node2",
            depends_on=["node1"],
            write=WriteConfig(connection="local", path="output.parquet", format="parquet"),
        )

        pipeline_config = PipelineConfig(pipeline="test_spark", nodes=[node1, node2])

        # Mock connections
        connections = {"local": MagicMock()}
        connections["local"].get_path.side_effect = lambda p: p  # identity

        # Initialize Pipeline with mocked engine injection
        # Since Pipeline creates Engine internally, we need to patch the class
        with patch("odibi.pipeline.SparkEngine") as MockSparkEngineClass:
            # Configure the mock engine instance that Pipeline will get
            mock_engine_instance = MockSparkEngineClass.return_value
            mock_engine_instance.spark = mock_spark_session
            mock_engine_instance.name = "spark"

            # Setup read return
            mock_engine_instance.read.return_value = mock_spark_df
            # Setup operation return (chaining)
            mock_engine_instance.execute_operation.return_value = mock_spark_df

            # Run Pipeline
            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                engine="spark",
                connections=connections,
                generate_story=False,
            )

            results = pipeline.run()

            # Verification
            assert len(results.completed) == 2
            assert not results.failed

            # Check Engine calls
            # Node 1: Read -> Drop
            mock_engine_instance.read.assert_called()
            mock_engine_instance.execute_operation.assert_called_with(
                "drop", {"columns": ["col1"]}, mock_spark_df
            )

            # Node 2: Write
            mock_engine_instance.write.assert_called()
