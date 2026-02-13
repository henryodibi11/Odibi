"""Extended unit tests for odibi.validation.quarantine module."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

try:
    import polars as pl
except ImportError:
    pl = None  # type: ignore[assignment]

from odibi.config import (
    AcceptedValuesTest,
    ContractSeverity,
    CustomSQLTest,
    NotNullTest,
    QuarantineColumnsConfig,
    QuarantineConfig,
    RangeTest,
    RegexMatchTest,
    UniqueTest,
)
from odibi.validation.quarantine import (
    QuarantineResult,
    _apply_sampling,
    _evaluate_test_mask,
    add_quarantine_metadata,
    has_quarantine_tests,
    split_valid_invalid,
    write_quarantine,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def pandas_df():
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", None, "Dave", "Eve"],
            "age": [25, 17, 30, -1, 200],
            "email": ["a@b.com", "bad", "c@d.com", None, "e@f.org"],
            "status": ["active", "inactive", "active", "unknown", "active"],
        }
    )


@pytest.fixture()
def polars_df():
    if pl is None:
        pytest.skip("polars not installed")
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", None, "Dave", "Eve"],
            "age": [25, 17, 30, -1, 200],
            "email": ["a@b.com", "bad", "c@d.com", None, "e@f.org"],
            "status": ["active", "inactive", "active", "unknown", "active"],
        }
    )


class _FakeEngine:
    """Minimal engine stub that does NOT trigger the Spark detection path."""

    def __init__(self):
        self.write = MagicMock()

    def count_rows(self, df):
        return len(df)


@pytest.fixture()
def mock_engine():
    return _FakeEngine()


# ===========================================================================
# _evaluate_test_mask — Pandas path
# ===========================================================================


class TestEvaluateTestMaskPandas:
    """Tests for _evaluate_test_mask with Pandas DataFrames."""

    def test_not_null_column_found(self, pandas_df):
        test = NotNullTest(columns=["name"], on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(pandas_df, test, is_spark=False, is_polars=False)
        assert list(mask) == [True, True, False, True, True]

    def test_not_null_column_missing(self, pandas_df):
        test = NotNullTest(columns=["nonexistent"], on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(pandas_df, test, is_spark=False, is_polars=False)
        assert all(mask)

    def test_not_null_multiple_columns(self, pandas_df):
        test = NotNullTest(columns=["name", "email"], on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(pandas_df, test, is_spark=False, is_polars=False)
        # row 2 (name=None), row 3 (email=None)
        assert list(mask) == [True, True, False, False, True]

    def test_accepted_values_column_found(self, pandas_df):
        test = AcceptedValuesTest(
            column="status",
            values=["active", "inactive"],
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(pandas_df, test, is_spark=False, is_polars=False)
        assert list(mask) == [True, True, True, False, True]

    def test_accepted_values_column_missing(self, pandas_df):
        test = AcceptedValuesTest(
            column="missing_col",
            values=["x"],
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(pandas_df, test, is_spark=False, is_polars=False)
        assert all(mask)

    def test_range_min_only(self, pandas_df):
        test = RangeTest(column="age", min=0, on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(pandas_df, test, is_spark=False, is_polars=False)
        # age -1 fails
        assert list(mask) == [True, True, True, False, True]

    def test_range_max_only(self, pandas_df):
        test = RangeTest(column="age", max=150, on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(pandas_df, test, is_spark=False, is_polars=False)
        # age 200 fails
        assert list(mask) == [True, True, True, True, False]

    def test_range_min_and_max(self, pandas_df):
        test = RangeTest(column="age", min=0, max=150, on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(pandas_df, test, is_spark=False, is_polars=False)
        assert list(mask) == [True, True, True, False, False]

    def test_range_column_missing(self, pandas_df):
        test = RangeTest(column="nonexistent", min=0, max=10, on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(pandas_df, test, is_spark=False, is_polars=False)
        assert all(mask)

    def test_regex_match_column_found(self, pandas_df):
        test = RegexMatchTest(
            column="email",
            pattern=r"^.+@.+\..+$",
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(pandas_df, test, is_spark=False, is_polars=False)
        # "bad" doesn't match; None passes (isna path)
        assert mask.iloc[0] is True or mask.iloc[0]  # a@b.com matches
        assert not mask.iloc[1]  # "bad" fails
        assert mask.iloc[3]  # None passes

    def test_regex_match_column_missing(self, pandas_df):
        test = RegexMatchTest(
            column="nonexistent",
            pattern=r".*",
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(pandas_df, test, is_spark=False, is_polars=False)
        assert all(mask)

    def test_custom_sql(self, pandas_df):
        test = CustomSQLTest(condition="age >= 18", on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(pandas_df, test, is_spark=False, is_polars=False)
        # age 17 and -1 fail
        assert not mask.iloc[1]
        assert not mask.iloc[3]

    def test_custom_sql_invalid_expression(self, pandas_df):
        test = CustomSQLTest(condition="INVALID SYNTAX !!!", on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(pandas_df, test, is_spark=False, is_polars=False)
        assert all(mask)

    def test_unique_returns_all_true(self, pandas_df):
        test = UniqueTest(columns=["id"], on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(pandas_df, test, is_spark=False, is_polars=False)
        assert all(mask)


# ===========================================================================
# _evaluate_test_mask — Polars path
# ===========================================================================


@pytest.mark.skipif(pl is None, reason="polars not installed")
class TestEvaluateTestMaskPolars:
    """Tests for _evaluate_test_mask with Polars DataFrames."""

    def _eval(self, df, mask_expr):
        """Evaluate a Polars expression mask against a DataFrame."""
        return df.select(mask_expr.alias("mask"))["mask"].to_list()

    def test_not_null_column_found(self, polars_df):
        test = NotNullTest(columns=["name"], on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = self._eval(polars_df, mask)
        assert result == [True, True, False, True, True]

    def test_not_null_column_missing(self, polars_df):
        test = NotNullTest(columns=["nonexistent"], on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = self._eval(polars_df, mask)
        assert all(result)

    def test_not_null_multiple_columns(self, polars_df):
        test = NotNullTest(columns=["name", "email"], on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = self._eval(polars_df, mask)
        assert result == [True, True, False, False, True]

    def test_accepted_values_column_found(self, polars_df):
        test = AcceptedValuesTest(
            column="status",
            values=["active", "inactive"],
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = self._eval(polars_df, mask)
        assert result == [True, True, True, False, True]

    def test_accepted_values_column_missing(self, polars_df):
        test = AcceptedValuesTest(
            column="missing_col",
            values=["x"],
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = self._eval(polars_df, mask)
        assert all(result)

    def test_range_min_only(self, polars_df):
        test = RangeTest(column="age", min=0, on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = self._eval(polars_df, mask)
        assert result == [True, True, True, False, True]

    def test_range_max_only(self, polars_df):
        test = RangeTest(column="age", max=150, on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = self._eval(polars_df, mask)
        assert result == [True, True, True, True, False]

    def test_range_min_and_max(self, polars_df):
        test = RangeTest(column="age", min=0, max=150, on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = self._eval(polars_df, mask)
        assert result == [True, True, True, False, False]

    def test_range_column_missing(self, polars_df):
        test = RangeTest(column="nonexistent", min=0, max=10, on_fail=ContractSeverity.QUARANTINE)
        mask = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = self._eval(polars_df, mask)
        assert all(result)

    def test_regex_match_column_found(self, polars_df):
        test = RegexMatchTest(
            column="email",
            pattern=r"^.+@.+\..+$",
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = self._eval(polars_df, mask)
        assert result[0]  # a@b.com matches
        assert not result[1]  # "bad" fails
        assert result[3]  # None passes

    def test_regex_match_column_missing(self, polars_df):
        test = RegexMatchTest(
            column="nonexistent",
            pattern=r".*",
            on_fail=ContractSeverity.QUARANTINE,
        )
        mask = _evaluate_test_mask(polars_df, test, is_spark=False, is_polars=True)
        result = self._eval(polars_df, mask)
        assert all(result)


# ===========================================================================
# split_valid_invalid
# ===========================================================================


class TestSplitValidInvalid:
    """Tests for split_valid_invalid with Pandas and Polars."""

    def test_pandas_multiple_tests(self, pandas_df, mock_engine):
        tests = [
            NotNullTest(columns=["name"], on_fail=ContractSeverity.QUARANTINE),
            RangeTest(column="age", min=0, max=150, on_fail=ContractSeverity.QUARANTINE),
        ]
        result = split_valid_invalid(pandas_df, tests, mock_engine)

        assert isinstance(result, QuarantineResult)
        assert result.rows_valid + result.rows_quarantined == len(pandas_df)
        # Row 2 (name=None), row 3 (age=-1), row 4 (age=200) are invalid
        assert result.rows_quarantined == 3
        assert result.rows_valid == 2
        assert len(result.valid_df) == 2
        assert len(result.invalid_df) == 3

    def test_pandas_no_quarantine_tests(self, pandas_df, mock_engine):
        tests = [
            NotNullTest(columns=["name"], on_fail=ContractSeverity.FAIL),
        ]
        result = split_valid_invalid(pandas_df, tests, mock_engine)
        assert result.rows_quarantined == 0
        assert result.rows_valid == len(pandas_df)

    def test_pandas_test_results_populated(self, pandas_df, mock_engine):
        tests = [
            NotNullTest(
                columns=["name"],
                name="name_not_null",
                on_fail=ContractSeverity.QUARANTINE,
            ),
        ]
        result = split_valid_invalid(pandas_df, tests, mock_engine)
        assert "name_not_null" in result.test_results
        tr = result.test_results["name_not_null"]
        assert tr["pass_count"] == 4
        assert tr["fail_count"] == 1

    def test_polars_multiple_tests(self, polars_df, mock_engine):
        tests = [
            NotNullTest(columns=["name"], on_fail=ContractSeverity.QUARANTINE),
            RangeTest(column="age", min=0, max=150, on_fail=ContractSeverity.QUARANTINE),
        ]
        result = split_valid_invalid(polars_df, tests, mock_engine)

        assert isinstance(result, QuarantineResult)
        assert result.rows_valid + result.rows_quarantined == len(polars_df)
        assert result.rows_quarantined == 3
        assert result.rows_valid == 2

    def test_polars_no_quarantine_tests(self, polars_df, mock_engine):
        tests = [
            NotNullTest(columns=["name"], on_fail=ContractSeverity.FAIL),
        ]
        result = split_valid_invalid(polars_df, tests, mock_engine)
        assert result.rows_quarantined == 0
        assert result.rows_valid == len(polars_df)


# ===========================================================================
# add_quarantine_metadata
# ===========================================================================


class TestAddQuarantineMetadata:
    """Tests for add_quarantine_metadata with Pandas and Polars."""

    def _make_config(self, **overrides):
        defaults = {
            "rejection_reason": True,
            "rejected_at": True,
            "source_batch_id": True,
            "failed_tests": True,
            "original_node": True,
        }
        defaults.update(overrides)
        return QuarantineColumnsConfig(**defaults)

    def test_pandas_all_columns_enabled(self, pandas_df, mock_engine):
        config = self._make_config()
        tests = [
            NotNullTest(
                columns=["name"],
                name="nn_check",
                on_fail=ContractSeverity.QUARANTINE,
            ),
        ]
        result = add_quarantine_metadata(
            invalid_df=pandas_df,
            test_results={},
            config=config,
            engine=mock_engine,
            node_name="test_node",
            run_id="run-123",
            tests=tests,
        )
        assert "_rejection_reason" in result.columns
        assert "_rejected_at" in result.columns
        assert "_source_batch_id" in result.columns
        assert "_failed_tests" in result.columns
        assert "_original_node" in result.columns
        assert result["_source_batch_id"].iloc[0] == "run-123"
        assert result["_original_node"].iloc[0] == "test_node"
        assert "nn_check" in result["_failed_tests"].iloc[0]

    def test_pandas_partial_columns(self, pandas_df, mock_engine):
        config = self._make_config(rejected_at=False, source_batch_id=False, original_node=False)
        tests = [
            NotNullTest(
                columns=["name"],
                on_fail=ContractSeverity.QUARANTINE,
            ),
        ]
        result = add_quarantine_metadata(
            invalid_df=pandas_df,
            test_results={},
            config=config,
            engine=mock_engine,
            node_name="node",
            run_id="run-1",
            tests=tests,
        )
        assert "_rejection_reason" in result.columns
        assert "_failed_tests" in result.columns
        assert "_rejected_at" not in result.columns
        assert "_source_batch_id" not in result.columns
        assert "_original_node" not in result.columns

    def test_polars_all_columns_enabled(self, polars_df, mock_engine):
        config = self._make_config()
        tests = [
            NotNullTest(
                columns=["name"],
                name="nn_check",
                on_fail=ContractSeverity.QUARANTINE,
            ),
        ]
        result = add_quarantine_metadata(
            invalid_df=polars_df,
            test_results={},
            config=config,
            engine=mock_engine,
            node_name="test_node",
            run_id="run-123",
            tests=tests,
        )
        assert "_rejection_reason" in result.columns
        assert "_rejected_at" in result.columns
        assert "_source_batch_id" in result.columns
        assert "_failed_tests" in result.columns
        assert "_original_node" in result.columns
        assert result["_source_batch_id"][0] == "run-123"
        assert result["_original_node"][0] == "test_node"

    def test_polars_partial_columns(self, polars_df, mock_engine):
        config = self._make_config(rejection_reason=False, failed_tests=False, original_node=False)
        tests = [
            NotNullTest(
                columns=["name"],
                on_fail=ContractSeverity.QUARANTINE,
            ),
        ]
        result = add_quarantine_metadata(
            invalid_df=polars_df,
            test_results={},
            config=config,
            engine=mock_engine,
            node_name="node",
            run_id="run-1",
            tests=tests,
        )
        assert "_rejected_at" in result.columns
        assert "_source_batch_id" in result.columns
        assert "_rejection_reason" not in result.columns
        assert "_failed_tests" not in result.columns
        assert "_original_node" not in result.columns


# ===========================================================================
# _apply_sampling
# ===========================================================================


class TestApplySampling:
    """Tests for _apply_sampling with Pandas and Polars."""

    def _make_config(self, **kwargs):
        defaults = {"connection": "test", "path": "/quarantine"}
        defaults.update(kwargs)
        return QuarantineConfig(**defaults)

    # -- Pandas --

    def test_pandas_no_sampling(self, pandas_df):
        config = self._make_config()
        result = _apply_sampling(pandas_df, config, is_spark=False, is_polars=False)
        assert len(result) == len(pandas_df)

    def test_pandas_sample_fraction(self):
        df = pd.DataFrame({"a": range(100)})
        config = self._make_config(sample_fraction=0.1)
        result = _apply_sampling(df, config, is_spark=False, is_polars=False)
        assert len(result) <= 100
        assert len(result) > 0

    def test_pandas_max_rows(self, pandas_df):
        config = self._make_config(max_rows=2)
        result = _apply_sampling(pandas_df, config, is_spark=False, is_polars=False)
        assert len(result) == 2

    def test_pandas_both_fraction_and_max(self):
        df = pd.DataFrame({"a": range(100)})
        config = self._make_config(sample_fraction=0.5, max_rows=3)
        result = _apply_sampling(df, config, is_spark=False, is_polars=False)
        assert len(result) <= 3

    # -- Polars --

    @pytest.mark.skipif(pl is None, reason="polars not installed")
    def test_polars_no_sampling(self, polars_df):
        config = self._make_config()
        result = _apply_sampling(polars_df, config, is_spark=False, is_polars=True)
        assert len(result) == len(polars_df)

    @pytest.mark.skipif(pl is None, reason="polars not installed")
    def test_polars_sample_fraction(self):
        df = pl.DataFrame({"a": range(100)})
        config = self._make_config(sample_fraction=0.1)
        result = _apply_sampling(df, config, is_spark=False, is_polars=True)
        assert len(result) <= 100
        assert len(result) > 0

    @pytest.mark.skipif(pl is None, reason="polars not installed")
    def test_polars_max_rows(self, polars_df):
        config = self._make_config(max_rows=2)
        result = _apply_sampling(polars_df, config, is_spark=False, is_polars=True)
        assert len(result) == 2

    @pytest.mark.skipif(pl is None, reason="polars not installed")
    def test_polars_both_fraction_and_max(self):
        df = pl.DataFrame({"a": range(100)})
        config = self._make_config(sample_fraction=0.5, max_rows=3)
        result = _apply_sampling(df, config, is_spark=False, is_polars=True)
        assert len(result) <= 3


# ===========================================================================
# write_quarantine
# ===========================================================================


class TestWriteQuarantine:
    """Tests for write_quarantine."""

    def _make_config(self, **kwargs):
        defaults = {"connection": "silver", "path": "/quarantine/data"}
        defaults.update(kwargs)
        return QuarantineConfig(**defaults)

    def test_writes_via_engine(self, pandas_df, mock_engine):
        config = self._make_config()
        mock_engine.write.return_value = {"status": "ok"}
        connections = {"silver": {"type": "local", "base_path": "/data"}}

        result = write_quarantine(pandas_df, config, mock_engine, connections)

        mock_engine.write.assert_called_once()
        call_kwargs = mock_engine.write.call_args
        assert call_kwargs.kwargs.get("mode") == "append" or call_kwargs[1].get("mode") == "append"
        assert result["rows_quarantined"] == len(pandas_df)
        assert result["quarantine_path"] == "/quarantine/data"
        assert result["write_info"] == {"status": "ok"}

    def test_empty_df_skips_write(self, mock_engine):
        config = self._make_config()
        empty_df = pd.DataFrame({"a": []})
        connections = {"silver": {}}

        result = write_quarantine(empty_df, config, mock_engine, connections)

        mock_engine.write.assert_not_called()
        assert result["rows_quarantined"] == 0

    def test_missing_connection_raises(self, pandas_df, mock_engine):
        config = self._make_config()
        with pytest.raises(ValueError, match="not found"):
            write_quarantine(pandas_df, config, mock_engine, {})

    def test_uses_table_name_when_no_path(self, pandas_df, mock_engine):
        config = self._make_config(path=None, table="quarantine_tbl")
        mock_engine.write.return_value = {}
        connections = {"silver": {}}

        result = write_quarantine(pandas_df, config, mock_engine, connections)

        assert result["quarantine_path"] == "quarantine_tbl"
        call_kwargs = mock_engine.write.call_args
        assert (
            call_kwargs.kwargs.get("format") == "delta" or call_kwargs[1].get("format") == "delta"
        )

    def test_write_failure_raises(self, pandas_df, mock_engine):
        config = self._make_config()
        mock_engine.write.side_effect = RuntimeError("disk full")
        connections = {"silver": {}}

        with pytest.raises(RuntimeError, match="disk full"):
            write_quarantine(pandas_df, config, mock_engine, connections)


# ===========================================================================
# has_quarantine_tests
# ===========================================================================


class TestHasQuarantineTests:
    """Tests for has_quarantine_tests."""

    def test_returns_true_when_quarantine_test_exists(self):
        tests = [
            NotNullTest(columns=["a"], on_fail=ContractSeverity.QUARANTINE),
            RangeTest(column="b", min=0, on_fail=ContractSeverity.FAIL),
        ]
        assert has_quarantine_tests(tests) is True

    def test_returns_false_when_no_quarantine_tests(self):
        tests = [
            NotNullTest(columns=["a"], on_fail=ContractSeverity.FAIL),
            RangeTest(column="b", min=0, on_fail=ContractSeverity.WARN),
        ]
        assert has_quarantine_tests(tests) is False

    def test_returns_false_for_empty_list(self):
        assert has_quarantine_tests([]) is False
