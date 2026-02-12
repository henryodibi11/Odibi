import pytest
import pandas as pd

from odibi.context import EngineContext, PandasContext
from odibi.engine.pandas_engine import PandasEngine
from odibi.enums import EngineType
from odibi.exceptions import ValidationError
from odibi.transformers.validation import cross_check, CrossCheckParams


@pytest.fixture
def sample_df_a():
    """Sample DataFrame A with 5 rows."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "value": [100, 200, 300, 400, 500],
        }
    )


@pytest.fixture
def sample_df_b():
    """Sample DataFrame B with 5 rows (same count as A)."""
    return pd.DataFrame(
        {
            "id": [10, 20, 30, 40, 50],
            "name": ["Frank", "Grace", "Henry", "Iris", "Jack"],
            "value": [150, 250, 350, 450, 550],
        }
    )


@pytest.fixture
def sample_df_c():
    """Sample DataFrame C with 3 rows (different count than A/B)."""
    return pd.DataFrame(
        {
            "id": [100, 200, 300],
            "name": ["Kate", "Liam", "Mary"],
            "value": [1000, 2000, 3000],
        }
    )


@pytest.fixture
def sample_df_different_schema():
    """Sample DataFrame with different schema."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "description": ["A", "B", "C"],  # Different column name
            "amount": [100, 200, 300],  # Different column name
        }
    )


@pytest.fixture
def empty_df():
    """Empty DataFrame."""
    return pd.DataFrame({"id": [], "name": [], "value": []})


@pytest.fixture
def pandas_engine():
    """Create PandasEngine instance for testing."""
    return PandasEngine()


def _make_engine_context(context, engine):
    """Helper to create EngineContext with PandasEngine."""
    return EngineContext(
        context=context,
        df=None,
        engine_type=EngineType.PANDAS,
        engine=engine,
    )


class TestCrossCheckRowCountDiff:
    """Tests for cross_check transformer with row_count_diff type."""

    def test_row_count_match_no_threshold(self, sample_df_a, sample_df_b, pandas_engine):
        """Test that matching row counts pass validation."""
        context = PandasContext()
        context.register("node_a", sample_df_a)
        context.register("node_b", sample_df_b)

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="row_count_diff", inputs=["node_a", "node_b"], threshold=0.0)

        # Should not raise any exception
        result = cross_check(eng_context, params)
        assert result is None

    def test_row_count_mismatch_zero_threshold(self, sample_df_a, sample_df_c, pandas_engine):
        """Test that mismatched row counts fail validation with zero threshold."""
        context = PandasContext()
        context.register("node_a", sample_df_a)  # 5 rows
        context.register("node_c", sample_df_c)  # 3 rows

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="row_count_diff", inputs=["node_a", "node_c"], threshold=0.0)

        # Should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            cross_check(eng_context, params)

        assert "cross_check" in str(exc_info.value)
        assert "Row count mismatch" in str(exc_info.value)

    def test_row_count_within_threshold(self, sample_df_a, sample_df_c, pandas_engine):
        """Test that row count difference within threshold passes."""
        context = PandasContext()
        context.register("node_a", sample_df_a)  # 5 rows
        context.register("node_c", sample_df_c)  # 3 rows

        eng_context = _make_engine_context(context, pandas_engine)
        # Difference is (5-3)/5 = 40%, so threshold of 0.5 (50%) should pass
        params = CrossCheckParams(type="row_count_diff", inputs=["node_a", "node_c"], threshold=0.5)

        # Should not raise any exception
        result = cross_check(eng_context, params)
        assert result is None

    def test_row_count_exceeds_threshold(self, sample_df_a, sample_df_c, pandas_engine):
        """Test that row count difference exceeding threshold fails."""
        context = PandasContext()
        context.register("node_a", sample_df_a)  # 5 rows
        context.register("node_c", sample_df_c)  # 3 rows

        eng_context = _make_engine_context(context, pandas_engine)
        # Difference is (5-3)/5 = 40%, so threshold of 0.3 (30%) should fail
        params = CrossCheckParams(type="row_count_diff", inputs=["node_a", "node_c"], threshold=0.3)

        # Should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            cross_check(eng_context, params)

        assert "Row count mismatch" in str(exc_info.value)

    def test_row_count_multiple_inputs(self, sample_df_a, sample_df_b, sample_df_c, pandas_engine):
        """Test row count validation with multiple inputs."""
        context = PandasContext()
        context.register("node_a", sample_df_a)  # 5 rows
        context.register("node_b", sample_df_b)  # 5 rows
        context.register("node_c", sample_df_c)  # 3 rows

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(
            type="row_count_diff", inputs=["node_a", "node_b", "node_c"], threshold=0.0
        )

        # Should fail because node_c has different count
        with pytest.raises(ValidationError) as exc_info:
            cross_check(eng_context, params)

        assert "Row count mismatch" in str(exc_info.value)

    def test_row_count_empty_base(self, empty_df, sample_df_a, pandas_engine):
        """Test row count validation when base dataset is empty."""
        context = PandasContext()
        context.register("empty", empty_df)  # 0 rows
        context.register("node_a", sample_df_a)  # 5 rows

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="row_count_diff", inputs=["empty", "node_a"], threshold=0.0)

        # Should fail: base has 0 rows, comparison has > 0 rows
        with pytest.raises(ValidationError) as exc_info:
            cross_check(eng_context, params)

        assert "Row count mismatch" in str(exc_info.value)

    def test_row_count_both_empty(self, empty_df, pandas_engine):
        """Test row count validation when both datasets are empty."""
        empty_df2 = pd.DataFrame({"id": [], "name": [], "value": []})

        context = PandasContext()
        context.register("empty1", empty_df)
        context.register("empty2", empty_df2)

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="row_count_diff", inputs=["empty1", "empty2"], threshold=0.0)

        # Should pass: both have 0 rows
        result = cross_check(eng_context, params)
        assert result is None


class TestCrossCheckSchemaMatch:
    """Tests for cross_check transformer with schema_match type."""

    def test_schema_match_identical(self, sample_df_a, sample_df_b, pandas_engine):
        """Test that identical schemas pass validation."""
        context = PandasContext()
        context.register("node_a", sample_df_a)
        context.register("node_b", sample_df_b)

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="schema_match", inputs=["node_a", "node_b"])

        # Should not raise any exception
        result = cross_check(eng_context, params)
        assert result is None

    def test_schema_mismatch_different_columns(
        self, sample_df_a, sample_df_different_schema, pandas_engine
    ):
        """Test that different schemas fail validation."""
        context = PandasContext()
        context.register("node_a", sample_df_a)
        context.register("node_diff", sample_df_different_schema)

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="schema_match", inputs=["node_a", "node_diff"])

        # Should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            cross_check(eng_context, params)

        assert "Schema mismatch" in str(exc_info.value)

    def test_schema_match_multiple_inputs(
        self, sample_df_a, sample_df_b, sample_df_c, pandas_engine
    ):
        """Test schema validation with multiple inputs (all same schema)."""
        context = PandasContext()
        context.register("node_a", sample_df_a)
        context.register("node_b", sample_df_b)
        context.register("node_c", sample_df_c)

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="schema_match", inputs=["node_a", "node_b", "node_c"])

        # Should pass: all have same schema
        result = cross_check(eng_context, params)
        assert result is None

    def test_schema_mismatch_multiple_inputs(
        self, sample_df_a, sample_df_b, sample_df_different_schema, pandas_engine
    ):
        """Test schema validation with multiple inputs where one differs."""
        context = PandasContext()
        context.register("node_a", sample_df_a)
        context.register("node_b", sample_df_b)
        context.register("node_diff", sample_df_different_schema)

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="schema_match", inputs=["node_a", "node_b", "node_diff"])

        # Should fail: node_diff has different schema
        with pytest.raises(ValidationError) as exc_info:
            cross_check(eng_context, params)

        assert "Schema mismatch" in str(exc_info.value)

    def test_schema_match_different_types(self, pandas_engine):
        """Test schema validation with same columns but different types."""
        df_int = pd.DataFrame({"id": [1, 2, 3], "value": [100, 200, 300]})
        df_str = pd.DataFrame({"id": ["1", "2", "3"], "value": ["100", "200", "300"]})

        context = PandasContext()
        context.register("df_int", df_int)
        context.register("df_str", df_str)

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="schema_match", inputs=["df_int", "df_str"])

        # Should fail: same columns but different types
        with pytest.raises(ValidationError) as exc_info:
            cross_check(eng_context, params)

        assert "Schema mismatch" in str(exc_info.value)

    def test_schema_match_missing_columns(self, pandas_engine):
        """Test schema validation when second dataset is missing columns."""
        df_full = pd.DataFrame({"id": [1, 2], "name": ["A", "B"], "value": [100, 200]})
        df_partial = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})

        context = PandasContext()
        context.register("full", df_full)
        context.register("partial", df_partial)

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="schema_match", inputs=["full", "partial"])

        # Should fail: partial is missing 'value' column
        with pytest.raises(ValidationError) as exc_info:
            cross_check(eng_context, params)

        assert "Schema mismatch" in str(exc_info.value)
        assert "Missing/Changed fields" in str(exc_info.value)

    def test_schema_match_extra_columns(self, pandas_engine):
        """Test schema validation when second dataset has extra columns."""
        df_partial = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        df_full = pd.DataFrame({"id": [1, 2], "name": ["A", "B"], "extra": [100, 200]})

        context = PandasContext()
        context.register("partial", df_partial)
        context.register("full", df_full)

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="schema_match", inputs=["partial", "full"])

        # Should fail: full has extra 'extra' column
        with pytest.raises(ValidationError) as exc_info:
            cross_check(eng_context, params)

        assert "Schema mismatch" in str(exc_info.value)
        assert "Extra/Changed fields" in str(exc_info.value)


class TestCrossCheckErrorCases:
    """Tests for cross_check error handling."""

    def test_insufficient_inputs_zero(self, pandas_engine):
        """Test that zero inputs raises ValueError."""
        context = PandasContext()
        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="row_count_diff", inputs=[], threshold=0.0)

        with pytest.raises(ValueError) as exc_info:
            cross_check(eng_context, params)

        assert "at least 2 inputs" in str(exc_info.value)

    def test_insufficient_inputs_one(self, sample_df_a, pandas_engine):
        """Test that one input raises ValueError."""
        context = PandasContext()
        context.register("node_a", sample_df_a)

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="row_count_diff", inputs=["node_a"], threshold=0.0)

        with pytest.raises(ValueError) as exc_info:
            cross_check(eng_context, params)

        assert "at least 2 inputs" in str(exc_info.value)

    def test_missing_input(self, sample_df_a, pandas_engine):
        """Test that missing input raises error."""
        context = PandasContext()
        context.register("node_a", sample_df_a)
        # node_b is not set

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="row_count_diff", inputs=["node_a", "node_b"], threshold=0.0)

        # The context.get() method raises KeyError when key not found
        with pytest.raises(KeyError) as exc_info:
            cross_check(eng_context, params)

        assert "node_b" in str(exc_info.value)

    def test_invalid_check_type(self, sample_df_a, sample_df_b, pandas_engine):
        """Test that invalid check type does not raise error but is silently ignored."""
        context = PandasContext()
        context.register("node_a", sample_df_a)
        context.register("node_b", sample_df_b)

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="invalid_type", inputs=["node_a", "node_b"], threshold=0.0)

        # The function should complete without error (no validation for this type)
        result = cross_check(eng_context, params)
        assert result is None


class TestCrossCheckNullHandling:
    """Tests for cross_check with null values."""

    def test_row_count_with_nulls(self, pandas_engine):
        """Test row count validation with null values in data."""
        df_a = pd.DataFrame({"id": [1, 2, None, 4, 5], "value": [100, None, 300, 400, 500]})
        df_b = pd.DataFrame({"id": [10, 20, 30, None, 50], "value": [None, 250, 350, 450, 550]})

        context = PandasContext()
        context.register("df_a", df_a)
        context.register("df_b", df_b)

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="row_count_diff", inputs=["df_a", "df_b"], threshold=0.0)

        # Should pass: both have 5 rows regardless of nulls
        result = cross_check(eng_context, params)
        assert result is None

    def test_schema_match_with_nulls(self, pandas_engine):
        """Test schema validation with null values in data."""
        df_a = pd.DataFrame(
            {"id": [1, None, 3], "name": ["A", "B", None], "value": [100, 200, None]}
        )
        df_b = pd.DataFrame(
            {"id": [10, 20, None], "name": [None, "X", "Y"], "value": [None, None, 300]}
        )

        context = PandasContext()
        context.register("df_a", df_a)
        context.register("df_b", df_b)

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="schema_match", inputs=["df_a", "df_b"])

        # Should pass: schemas match regardless of null values
        result = cross_check(eng_context, params)
        assert result is None


class TestCrossCheckEdgeCases:
    """Tests for cross_check edge cases."""

    def test_single_row_dataframes(self, pandas_engine):
        """Test with single-row DataFrames."""
        df_a = pd.DataFrame({"id": [1], "value": [100]})
        df_b = pd.DataFrame({"id": [2], "value": [200]})

        context = PandasContext()
        context.register("df_a", df_a)
        context.register("df_b", df_b)

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(type="row_count_diff", inputs=["df_a", "df_b"], threshold=0.0)

        # Should pass: both have 1 row
        result = cross_check(eng_context, params)
        assert result is None

    def test_large_threshold(self, sample_df_a, pandas_engine):
        """Test with threshold of 1.0 (100%)."""
        df_single = pd.DataFrame({"id": [1], "name": ["A"], "value": [100]})

        context = PandasContext()
        context.register("df_a", sample_df_a)  # 5 rows
        context.register("df_single", df_single)  # 1 row

        eng_context = _make_engine_context(context, pandas_engine)
        # Difference is (5-1)/5 = 80%, threshold is 100%
        params = CrossCheckParams(
            type="row_count_diff", inputs=["df_a", "df_single"], threshold=1.0
        )

        # Should pass: difference is within 100% threshold
        result = cross_check(eng_context, params)
        assert result is None

    def test_very_small_threshold(self, sample_df_a, sample_df_b, pandas_engine):
        """Test with very small threshold."""
        context = PandasContext()
        context.register("df_a", sample_df_a)
        context.register("df_b", sample_df_b)

        eng_context = _make_engine_context(context, pandas_engine)
        params = CrossCheckParams(
            type="row_count_diff",
            inputs=["df_a", "df_b"],
            threshold=0.0001,  # 0.01%
        )

        # Should pass: both have exactly 5 rows
        result = cross_check(eng_context, params)
        assert result is None
