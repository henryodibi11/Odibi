import pytest
from unittest.mock import MagicMock, patch
from odibi.engine.spark_engine import SparkEngine
from odibi.exceptions import TransformError


class TestSparkErrorStandardization:
    @pytest.fixture
    def mock_spark_session(self):
        return MagicMock()

    @pytest.fixture
    def spark_engine(self, mock_spark_session):
        # Patch imports so SparkEngine can be instantiated without real pyspark
        with patch.dict(
            "sys.modules",
            {
                "pyspark": MagicMock(),
                "pyspark.sql": MagicMock(),
                "delta": MagicMock(),
            },
        ):
            engine = SparkEngine(spark_session=mock_spark_session)
            return engine

    def test_execute_sql_analysis_exception(self, spark_engine, mock_spark_session):
        """Test that Spark AnalysisException is handled/wrapped."""

        # Simulate AnalysisException from pyspark
        class AnalysisException(Exception):
            pass

        mock_spark_session.sql.side_effect = AnalysisException("Table not found")

        # Context needs to support list_names()
        mock_context = MagicMock()
        mock_context.list_names.return_value = []

        # Now expecting TransformError
        with pytest.raises(TransformError, match="Spark SQL Analysis Error"):
            spark_engine.execute_sql("SELECT * FROM bad_table", mock_context)

    def test_read_missing_path_error(self, spark_engine):
        """Test error when path is missing."""
        connection = MagicMock()
        connection.get_path.return_value = "some/path"

        # If read fails (e.g. file not found), Spark raises AnalysisException usually
        class AnalysisException(Exception):
            pass

        spark_engine.spark.read.format.return_value.load.side_effect = AnalysisException(
            "Path does not exist"
        )

        with pytest.raises(AnalysisException):
            spark_engine.read(connection, "parquet", path="data.parquet")
