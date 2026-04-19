"""Tests for odibi.validation.quarantine — Pandas and Polars paths."""

import pandas as pd
import polars as pl
import pytest
from unittest.mock import MagicMock

from odibi.config import (
    ContractSeverity,
    NotNullTest,
    UniqueTest,
    AcceptedValuesTest,
    RangeTest,
    RegexMatchTest,
    CustomSQLTest,
    QuarantineConfig,
    QuarantineColumnsConfig,
)
from odibi.validation.quarantine import (
    _evaluate_test_mask,
    split_valid_invalid,
    add_quarantine_metadata,
    _apply_sampling,
    write_quarantine,
    has_quarantine_tests,
)


# ── _evaluate_test_mask Pandas ───────────────────────────────────


def test_mask_pandas_not_null_with_nulls():
    df = pd.DataFrame({"a": [1, None, 3], "b": [None, 2, 3]})
    test = NotNullTest(columns=["a", "b"])
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert list(mask) == [False, False, True]


def test_mask_pandas_not_null_single_col():
    df = pd.DataFrame({"a": [1, None, 3]})
    test = NotNullTest(columns=["a"])
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert list(mask) == [True, False, True]


def test_mask_pandas_not_null_missing_col():
    df = pd.DataFrame({"a": [1, 2]})
    test = NotNullTest(columns=["missing"])
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert all(mask)


def test_mask_pandas_unique():
    df = pd.DataFrame({"id": [1, 1, 2, 3]})
    test = UniqueTest(columns=["id"])
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert mask[2] is True or mask[2]
    assert mask[3] is True or mask[3]
    assert not mask[0]
    assert not mask[1]


def test_mask_pandas_unique_all_unique():
    df = pd.DataFrame({"id": [1, 2, 3]})
    test = UniqueTest(columns=["id"])
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert all(mask)


def test_mask_pandas_unique_missing_col():
    df = pd.DataFrame({"a": [1, 2]})
    test = UniqueTest(columns=["missing"])
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert all(mask)


def test_mask_pandas_accepted_values_valid():
    df = pd.DataFrame({"s": ["A", "B", "A"]})
    test = AcceptedValuesTest(column="s", values=["A", "B"])
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert all(mask)


def test_mask_pandas_accepted_values_invalid():
    df = pd.DataFrame({"s": ["A", "X", "B"]})
    test = AcceptedValuesTest(column="s", values=["A", "B"])
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert list(mask) == [True, False, True]


def test_mask_pandas_accepted_values_missing_col():
    df = pd.DataFrame({"a": [1]})
    test = AcceptedValuesTest(column="missing", values=["A"])
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert all(mask)


def test_mask_pandas_range_in_range():
    df = pd.DataFrame({"v": [5, 10, 15]})
    test = RangeTest(column="v", min=0, max=20)
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert all(mask)


def test_mask_pandas_range_out_of_range():
    df = pd.DataFrame({"v": [-1, 10, 25]})
    test = RangeTest(column="v", min=0, max=20)
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert list(mask) == [False, True, False]


def test_mask_pandas_range_min_only():
    df = pd.DataFrame({"v": [-5, 0, 10]})
    test = RangeTest(column="v", min=0)
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert not mask[0]
    assert mask[1]
    assert mask[2]


def test_mask_pandas_range_max_only():
    df = pd.DataFrame({"v": [5, 10, 25]})
    test = RangeTest(column="v", max=20)
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert mask[0]
    assert not mask[2]


def test_mask_pandas_range_missing_col():
    df = pd.DataFrame({"a": [1]})
    test = RangeTest(column="missing", min=0)
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert all(mask)


def test_mask_pandas_regex_match():
    df = pd.DataFrame({"c": ["ABC", "123", "XYZ"]})
    test = RegexMatchTest(column="c", pattern=r"^[A-Z]+$")
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert mask[0]
    assert not mask[1]
    assert mask[2]


def test_mask_pandas_regex_missing_col():
    df = pd.DataFrame({"a": ["x"]})
    test = RegexMatchTest(column="missing", pattern=r".*")
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert all(mask)


def test_mask_pandas_custom_sql_valid():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
    test = CustomSQLTest(condition="a > 0")
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert all(mask)


def test_mask_pandas_custom_sql_error():
    df = pd.DataFrame({"a": [1]})
    test = CustomSQLTest(condition="nonexistent > 0")
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert all(mask)


# ── _evaluate_test_mask Polars ───────────────────────────────────


def test_mask_polars_not_null():
    df = pl.DataFrame({"a": [1, None, 3]})
    test = NotNullTest(columns=["a"])
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=True)
    result = df.select(mask.alias("m"))["m"].to_list()
    assert result == [True, False, True]


def test_mask_polars_not_null_multi():
    df = pl.DataFrame({"a": [1, None, 3], "b": [None, 2, 3]})
    test = NotNullTest(columns=["a", "b"])
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=True)
    result = df.select(mask.alias("m"))["m"].to_list()
    assert result == [False, False, True]


def test_mask_polars_accepted_values():
    df = pl.DataFrame({"s": ["A", "X", "B"]})
    test = AcceptedValuesTest(column="s", values=["A", "B"])
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=True)
    result = df.select(mask.alias("m"))["m"].to_list()
    assert result == [True, False, True]


def test_mask_polars_range():
    df = pl.DataFrame({"v": [-1, 10, 25]})
    test = RangeTest(column="v", min=0, max=20)
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=True)
    result = df.select(mask.alias("m"))["m"].to_list()
    assert result == [False, True, False]


def test_mask_polars_regex():
    df = pl.DataFrame({"c": ["ABC", "123"]})
    test = RegexMatchTest(column="c", pattern=r"^[A-Z]+$")
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=True)
    result = df.select(mask.alias("m"))["m"].to_list()
    assert result == [True, False]


def test_mask_polars_default_returns_true():
    """Unknown test type returns pl.lit(True)."""
    df = pl.DataFrame({"a": [1, 2]})
    test = MagicMock()
    test.type = "unknown_type"
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=True)
    result = df.select(mask.alias("m"))["m"].to_list()
    assert all(result)


# ── split_valid_invalid ──────────────────────────────────────────


def _make_engine():
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    engine.count_rows = MagicMock(side_effect=lambda df: len(df))
    return engine


def test_split_no_quarantine_tests():
    df = pd.DataFrame({"a": [1, None, 3]})
    tests = [NotNullTest(columns=["a"], on_fail=ContractSeverity.FAIL)]
    result = split_valid_invalid(df, tests, _make_engine())
    assert result.rows_valid == 3
    assert result.rows_quarantined == 0
    assert len(result.invalid_df) == 0


def test_split_single_quarantine_test():
    df = pd.DataFrame({"a": [1, None, 3]})
    tests = [NotNullTest(columns=["a"], on_fail=ContractSeverity.QUARANTINE)]
    result = split_valid_invalid(df, tests, _make_engine())
    assert result.rows_valid == 2
    assert result.rows_quarantined == 1
    assert len(result.valid_df) == 2
    assert len(result.invalid_df) == 1


def test_split_multiple_quarantine_tests():
    df = pd.DataFrame({"a": [1, None, 3], "b": ["X", "A", "A"]})
    tests = [
        NotNullTest(columns=["a"], on_fail=ContractSeverity.QUARANTINE),
        AcceptedValuesTest(column="b", values=["A"], on_fail=ContractSeverity.QUARANTINE),
    ]
    result = split_valid_invalid(df, tests, _make_engine())
    assert result.rows_quarantined == 2
    assert result.rows_valid == 1


def test_split_all_valid():
    df = pd.DataFrame({"a": [1, 2, 3]})
    tests = [NotNullTest(columns=["a"], on_fail=ContractSeverity.QUARANTINE)]
    result = split_valid_invalid(df, tests, _make_engine())
    assert result.rows_valid == 3
    assert result.rows_quarantined == 0


def test_split_all_invalid():
    df = pd.DataFrame({"a": [None, None]})
    tests = [NotNullTest(columns=["a"], on_fail=ContractSeverity.QUARANTINE)]
    result = split_valid_invalid(df, tests, _make_engine())
    assert result.rows_valid == 0
    assert result.rows_quarantined == 2


def test_split_test_results_populated():
    df = pd.DataFrame({"a": [1, None, 3]})
    tests = [NotNullTest(columns=["a"], name="null_check", on_fail=ContractSeverity.QUARANTINE)]
    result = split_valid_invalid(df, tests, _make_engine())
    assert "null_check" in result.test_results
    assert result.test_results["null_check"]["pass_count"] == 2
    assert result.test_results["null_check"]["fail_count"] == 1


def test_split_polars():
    df = pl.DataFrame({"a": [1, None, 3]})
    tests = [NotNullTest(columns=["a"], on_fail=ContractSeverity.QUARANTINE)]
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    result = split_valid_invalid(df, tests, engine)
    assert result.rows_valid == 2
    assert result.rows_quarantined == 1


def test_split_polars_no_quarantine_tests():
    df = pl.DataFrame({"a": [1, None, 3]})
    tests = [NotNullTest(columns=["a"], on_fail=ContractSeverity.FAIL)]
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    engine.count_rows.return_value = 3
    result = split_valid_invalid(df, tests, engine)
    assert result.rows_valid == 3
    assert result.rows_quarantined == 0


# ── add_quarantine_metadata ──────────────────────────────────────


def test_metadata_all_columns_pandas():
    df = pd.DataFrame({"a": [None, None], "b": [1, 2]})
    tests = [NotNullTest(columns=["a"], name="null_a", on_fail=ContractSeverity.QUARANTINE)]
    config = QuarantineColumnsConfig(
        rejection_reason=True,
        rejected_at=True,
        source_batch_id=True,
        failed_tests=True,
        original_node=True,
    )
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    result = add_quarantine_metadata(
        df, {}, config, engine, node_name="test_node", run_id="run-123", tests=tests
    )
    assert "_rejection_reason" in result.columns
    assert "_rejected_at" in result.columns
    assert "_source_batch_id" in result.columns
    assert "_failed_tests" in result.columns
    assert "_original_node" in result.columns
    assert result["_source_batch_id"].iloc[0] == "run-123"
    assert result["_original_node"].iloc[0] == "test_node"


def test_metadata_selective_columns_pandas():
    df = pd.DataFrame({"a": [None]})
    tests = [NotNullTest(columns=["a"], on_fail=ContractSeverity.QUARANTINE)]
    config = QuarantineColumnsConfig(
        rejection_reason=True,
        rejected_at=True,
        source_batch_id=False,
        failed_tests=False,
        original_node=False,
    )
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    result = add_quarantine_metadata(df, {}, config, engine, node_name="n", run_id="r", tests=tests)
    assert "_rejection_reason" in result.columns
    assert "_rejected_at" in result.columns
    assert "_source_batch_id" not in result.columns
    assert "_failed_tests" not in result.columns
    assert "_original_node" not in result.columns


def test_metadata_polars():
    df = pl.DataFrame({"a": [None, None]})
    tests = [NotNullTest(columns=["a"], name="null_a", on_fail=ContractSeverity.QUARANTINE)]
    config = QuarantineColumnsConfig(
        rejection_reason=True,
        rejected_at=True,
        source_batch_id=True,
        failed_tests=True,
        original_node=True,
    )
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    result = add_quarantine_metadata(
        df, {}, config, engine, node_name="test_node", run_id="run-123", tests=tests
    )
    assert "_rejection_reason" in result.columns
    assert "_rejected_at" in result.columns
    assert "_original_node" in result.columns


# ── _apply_sampling ──────────────────────────────────────────────


def test_sampling_no_config():
    df = pd.DataFrame({"a": range(10)})
    config = MagicMock()
    config.sample_fraction = None
    config.max_rows = None
    result = _apply_sampling(df, config, is_spark=False, is_polars=False)
    assert len(result) == 10


def test_sampling_max_rows_pandas():
    df = pd.DataFrame({"a": range(100)})
    config = MagicMock()
    config.sample_fraction = None
    config.max_rows = 10
    result = _apply_sampling(df, config, is_spark=False, is_polars=False)
    assert len(result) == 10


def test_sampling_sample_fraction_pandas():
    df = pd.DataFrame({"a": range(100)})
    config = MagicMock()
    config.sample_fraction = 0.1
    config.max_rows = None
    result = _apply_sampling(df, config, is_spark=False, is_polars=False)
    assert len(result) <= 20  # roughly 10% but random


def test_sampling_both_pandas():
    df = pd.DataFrame({"a": range(100)})
    config = MagicMock()
    config.sample_fraction = 0.5
    config.max_rows = 5
    result = _apply_sampling(df, config, is_spark=False, is_polars=False)
    assert len(result) <= 5


def test_sampling_max_rows_polars():
    df = pl.DataFrame({"a": list(range(100))})
    config = MagicMock()
    config.sample_fraction = None
    config.max_rows = 10
    result = _apply_sampling(df, config, is_spark=False, is_polars=True)
    assert len(result) == 10


def test_sampling_sample_fraction_polars():
    df = pl.DataFrame({"a": list(range(100))})
    config = MagicMock()
    config.sample_fraction = 0.1
    config.max_rows = None
    result = _apply_sampling(df, config, is_spark=False, is_polars=True)
    assert len(result) <= 20


# ── write_quarantine ─────────────────────────────────────────────


def test_write_quarantine_empty_df():
    df = pd.DataFrame({"a": pd.Series([], dtype="float64")})
    config = QuarantineConfig(connection="local", path="q_path")
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    result = write_quarantine(df, config, engine, {"local": {}})
    assert result["rows_quarantined"] == 0
    assert result["write_info"] is None


def test_write_quarantine_success():
    df = pd.DataFrame({"a": [1, 2, 3]})
    config = QuarantineConfig(connection="local", path="q_path")
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    engine.write.return_value = {"status": "ok"}
    result = write_quarantine(df, config, engine, {"local": {}})
    assert result["rows_quarantined"] == 3
    engine.write.assert_called_once()


def test_write_quarantine_missing_connection():
    df = pd.DataFrame({"a": [1]})
    config = QuarantineConfig(connection="missing", path="q_path")
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    with pytest.raises(ValueError, match="not found"):
        write_quarantine(df, config, engine, {"local": {}})


def test_write_quarantine_engine_error():
    df = pd.DataFrame({"a": [1]})
    config = QuarantineConfig(connection="local", path="q_path")
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    engine.write.side_effect = RuntimeError("write failed")
    with pytest.raises(RuntimeError, match="write failed"):
        write_quarantine(df, config, engine, {"local": {}})


def test_write_quarantine_table_format():
    df = pd.DataFrame({"a": [1]})
    config = QuarantineConfig(connection="local", table="q_table")
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    engine.write.return_value = {}
    write_quarantine(df, config, engine, {"local": {}})
    call_kwargs = engine.write.call_args
    assert call_kwargs[1].get("format") == "delta" or call_kwargs.kwargs.get("format") == "delta"


# ── has_quarantine_tests ─────────────────────────────────────────


def test_has_quarantine_tests_true():
    tests = [
        NotNullTest(columns=["a"], on_fail=ContractSeverity.QUARANTINE),
        NotNullTest(columns=["b"], on_fail=ContractSeverity.FAIL),
    ]
    assert has_quarantine_tests(tests) is True


def test_has_quarantine_tests_false():
    tests = [
        NotNullTest(columns=["a"], on_fail=ContractSeverity.FAIL),
        NotNullTest(columns=["b"], on_fail=ContractSeverity.WARN),
    ]
    assert has_quarantine_tests(tests) is False


def test_has_quarantine_tests_empty():
    assert has_quarantine_tests([]) is False
