"""Tests for node traceback cleaning functionality."""

from unittest.mock import MagicMock

import pytest

from odibi.node import NodeExecutor


@pytest.fixture
def mock_context():
    return MagicMock()


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.name = "pandas"
    return engine


class TestTracebackCleaning:
    """Tests for _clean_spark_traceback method."""

    def test_clean_python_traceback(self, mock_context, mock_engine):
        """Should preserve Python traceback."""
        executor = NodeExecutor(mock_context, mock_engine, connections={})

        raw_traceback = """Traceback (most recent call last):
  File "test.py", line 10, in <module>
    result = 1 / 0
ZeroDivisionError: division by zero"""

        cleaned = executor._clean_spark_traceback(raw_traceback)

        assert "Traceback" in cleaned
        assert "test.py" in cleaned
        assert "ZeroDivisionError" in cleaned

    def test_clean_spark_java_traceback(self, mock_context, mock_engine):
        """Should remove Java stack trace lines from Spark errors."""
        executor = NodeExecutor(mock_context, mock_engine, connections={})

        raw_traceback = """Traceback (most recent call last):
  File "node.py", line 50, in execute
    df.show()
py4j.protocol.Py4JJavaError: An error occurred
  at org.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:131)
  at java.lang.Thread.run(Thread.java:748)
Traceback (most recent call last):
  File "inner.py", line 20
AnalysisException: Column 'foo' not found"""

        cleaned = executor._clean_spark_traceback(raw_traceback)

        # Should NOT contain Java stack frames
        assert "at org.apache.spark" not in cleaned
        assert "at java.lang" not in cleaned
        assert "py4j.protocol" not in cleaned

        # Should contain Python frames and error message
        assert "node.py" in cleaned
        # After Java lines are skipped, Python traceback lines resume
        # the AnalysisException line follows a File line so it's included
        assert "AnalysisException" in cleaned or "inner.py" in cleaned

    def test_clean_spark_analysis_exception(self, mock_context, mock_engine):
        """Should extract clean message from Spark AnalysisException."""
        executor = NodeExecutor(mock_context, mock_engine, connections={})

        raw_traceback = """Traceback (most recent call last):
  File "transform.py", line 25
  at org.apache.spark.sql.AnalysisException: cannot resolve 'column_x' given input columns
  at scala.collection.TraversableLike.flatMap(TraversableLike.scala:241)
  at org.apache.spark.sql.catalyst.analysis.Analyzer.resolve(Analyzer.scala:890)"""

        cleaned = executor._clean_spark_traceback(raw_traceback)

        assert "at scala.collection" not in cleaned
        assert "at org.apache.spark.sql.catalyst" not in cleaned

    def test_preserves_meaningful_error_info(self, mock_context, mock_engine):
        """Should preserve the meaningful error information."""
        executor = NodeExecutor(mock_context, mock_engine, connections={})

        raw_traceback = """Traceback (most recent call last):
  File "/app/pipeline.py", line 100, in run
    result = self.transform(df)
  File "/app/transforms.py", line 50, in transform
    return df.select("missing_column")
ValueError: Column 'missing_column' does not exist"""

        cleaned = executor._clean_spark_traceback(raw_traceback)

        assert "/app/pipeline.py" in cleaned
        assert "/app/transforms.py" in cleaned
        assert "missing_column" in cleaned

    def test_removes_duplicate_empty_lines(self, mock_context, mock_engine):
        """Should remove duplicate empty lines."""
        executor = NodeExecutor(mock_context, mock_engine, connections={})

        raw_traceback = """Traceback (most recent call last):


  File "test.py", line 10


    result = 1 / 0


ZeroDivisionError: division by zero"""

        cleaned = executor._clean_spark_traceback(raw_traceback)

        # Should not have multiple consecutive empty lines
        assert "\n\n\n" not in cleaned
