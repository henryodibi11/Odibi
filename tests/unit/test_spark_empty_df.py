from unittest.mock import MagicMock, patch

from odibi.engine.spark_engine import SparkEngine

# Create mock pyspark.sql.functions so the local imports inside
# count_nulls / profile_nulls resolve without a live SparkContext.
_mock_functions = MagicMock()


def _fake_col(name):
    m = MagicMock(name=f"col({name})")
    return m


_mock_functions.col = _fake_col
_mock_functions.count = MagicMock(side_effect=lambda x: MagicMock())
_mock_functions.when = MagicMock(side_effect=lambda cond, val: MagicMock())
_mock_functions.mean = MagicMock(side_effect=lambda x: MagicMock())

_PATCH = patch.dict("sys.modules", {"pyspark.sql.functions": _mock_functions})
_PATCH.start()


class TestSparkEmptyDataFrame:
    """Tests for #257: Spark methods crash on empty DataFrame."""

    def setup_method(self):
        self.engine = SparkEngine.__new__(SparkEngine)

    def test_count_nulls_empty_df(self):
        """count_nulls returns zeros for all columns on empty DataFrame."""
        mock_df = MagicMock()
        mock_df.columns = ["a", "b"]
        mock_df.select.return_value.collect.return_value = []
        result = self.engine.count_nulls(mock_df, ["a", "b"])
        assert result == {"a": 0, "b": 0}

    def test_count_nulls_nonempty_df(self):
        """count_nulls returns actual counts on non-empty DataFrame."""
        mock_df = MagicMock()
        mock_df.columns = ["a", "b"]
        mock_row = MagicMock()
        mock_row.asDict.return_value = {"a": 1, "b": 0}
        mock_df.select.return_value.collect.return_value = [mock_row]
        result = self.engine.count_nulls(mock_df, ["a", "b"])
        assert result == {"a": 1, "b": 0}

    def test_profile_nulls_empty_df(self):
        """profile_nulls returns empty dict when collect() is empty."""
        mock_df = MagicMock()
        mock_df.columns = ["x"]
        mock_df.select.return_value.collect.return_value = []
        result = self.engine.profile_nulls(mock_df)
        assert result == {}

    def test_profile_nulls_no_columns(self):
        """profile_nulls returns empty dict when df has no columns."""
        mock_df = MagicMock()
        mock_df.columns = []
        result = self.engine.profile_nulls(mock_df)
        assert result == {}
