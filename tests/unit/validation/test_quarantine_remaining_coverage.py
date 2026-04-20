"""Tests for remaining non-Spark coverage gaps in odibi.validation.quarantine."""

import pandas as pd
import pytest

pl = pytest.importorskip("polars")
from unittest.mock import MagicMock

from odibi.config import (
    ContractSeverity,
    NotNullTest,
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
)


# ── helpers ──────────────────────────────────────────────────────


def _make_engine():
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    engine.count_rows = MagicMock(side_effect=lambda df: len(df))
    return engine


# ── _evaluate_test_mask: Pandas fallthrough (line 218) ───────────


def test_mask_pandas_unknown_test_type_returns_all_true():
    """Unknown test type hits the final `return pd.Series(True...)` at line 218."""
    df = pd.DataFrame({"a": [1, 2, 3]})
    test = MagicMock()
    test.type = "some_future_type"
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert list(mask) == [True, True, True]


# ── _evaluate_test_mask: Polars missing-column branches ─────────


def test_mask_polars_not_null_missing_col():
    df = pl.DataFrame({"a": [1, 2]})
    test = NotNullTest(columns=["missing"])
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=True)
    result = df.select(mask.alias("m"))["m"].to_list()
    assert all(result)


def test_mask_polars_accepted_values_missing_col():
    df = pl.DataFrame({"a": [1]})
    test = AcceptedValuesTest(column="missing", values=["x"])
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=True)
    result = df.select(mask.alias("m"))["m"].to_list()
    assert all(result)


def test_mask_polars_range_missing_col():
    df = pl.DataFrame({"a": [1]})
    test = RangeTest(column="missing", min=0, max=10)
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=True)
    result = df.select(mask.alias("m"))["m"].to_list()
    assert all(result)


def test_mask_polars_range_min_only():
    df = pl.DataFrame({"v": [-5, 0, 10]})
    test = RangeTest(column="v", min=0)
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=True)
    result = df.select(mask.alias("m"))["m"].to_list()
    assert result == [False, True, True]


def test_mask_polars_range_max_only():
    df = pl.DataFrame({"v": [5, 10, 25]})
    test = RangeTest(column="v", max=20)
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=True)
    result = df.select(mask.alias("m"))["m"].to_list()
    assert result == [True, True, False]


def test_mask_polars_regex_missing_col():
    df = pl.DataFrame({"a": ["x"]})
    test = RegexMatchTest(column="missing", pattern=r".*")
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=True)
    result = df.select(mask.alias("m"))["m"].to_list()
    assert all(result)


def test_mask_polars_regex_with_null():
    """Regex match with null values should pass (null || regex)."""
    df = pl.DataFrame({"c": ["ABC", None, "123"]})
    test = RegexMatchTest(column="c", pattern=r"^[A-Z]+$")
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=True)
    result = df.select(mask.alias("m"))["m"].to_list()
    assert result[0] is True
    assert result[1] is True  # null passes
    assert result[2] is False


# ── split_valid_invalid: duplicate test name de-duplication ──────


def test_split_duplicate_test_names_get_suffixed():
    """When two tests share the same name, the second gets _idx suffix (line 292)."""
    df = pd.DataFrame({"a": [1, None, 3], "b": [None, 2, 3]})
    tests = [
        NotNullTest(columns=["a"], name="null_check", on_fail=ContractSeverity.QUARANTINE),
        NotNullTest(columns=["b"], name="null_check", on_fail=ContractSeverity.QUARANTINE),
    ]
    result = split_valid_invalid(df, tests, _make_engine())
    names = list(result.test_results.keys())
    assert "null_check" in names
    assert any("null_check_" in n for n in names)


def test_split_unnamed_tests_use_type_value():
    """Tests without name use test.type.value as name."""
    df = pd.DataFrame({"a": [1, None, 3]})
    tests = [NotNullTest(columns=["a"], on_fail=ContractSeverity.QUARANTINE)]
    result = split_valid_invalid(df, tests, _make_engine())
    assert "not_null" in result.test_results


def test_split_polars_multiple_quarantine_tests():
    """Polars path with multiple quarantine tests."""
    df = pl.DataFrame({"a": [1, None, 3], "b": ["X", "Y", "Z"]})
    tests = [
        NotNullTest(columns=["a"], on_fail=ContractSeverity.QUARANTINE),
        AcceptedValuesTest(column="b", values=["X", "Z"], on_fail=ContractSeverity.QUARANTINE),
    ]
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    result = split_valid_invalid(df, tests, engine)
    # Row 1 (None, Y) fails both; only row 0 and 2 pass not_null but row 1 fails accepted_values too
    assert result.rows_quarantined >= 1


def test_split_engine_without_count_rows():
    """Engine without count_rows attribute uses len(df) for row_count (line 277)."""
    df = pd.DataFrame({"a": [1, 2, 3]})
    tests = [NotNullTest(columns=["a"], on_fail=ContractSeverity.FAIL)]
    engine = MagicMock(spec=[])  # no count_rows
    if hasattr(engine, "spark"):
        del engine.spark
    result = split_valid_invalid(df, tests, engine)
    assert result.rows_valid == 3


# ── add_quarantine_metadata: Polars selective columns ────────────


def test_metadata_polars_selective_columns():
    """Polars path with some columns disabled (lines 448-462)."""
    df = pl.DataFrame({"a": [None, None]})
    tests = [NotNullTest(columns=["a"], on_fail=ContractSeverity.QUARANTINE)]
    config = QuarantineColumnsConfig(
        rejection_reason=True,
        rejected_at=False,
        source_batch_id=True,
        failed_tests=False,
        original_node=True,
    )
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    result = add_quarantine_metadata(
        df, {}, config, engine, node_name="n", run_id="r1", tests=tests
    )
    assert "_rejection_reason" in result.columns
    assert "_rejected_at" not in result.columns
    assert "_source_batch_id" in result.columns
    assert "_failed_tests" not in result.columns
    assert "_original_node" in result.columns


def test_metadata_polars_no_columns():
    """Polars path with all metadata columns disabled."""
    df = pl.DataFrame({"a": [None]})
    tests = [NotNullTest(columns=["a"], on_fail=ContractSeverity.QUARANTINE)]
    config = QuarantineColumnsConfig(
        rejection_reason=False,
        rejected_at=False,
        source_batch_id=False,
        failed_tests=False,
        original_node=False,
    )
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    result = add_quarantine_metadata(df, {}, config, engine, node_name="n", run_id="r", tests=tests)
    assert set(result.columns) == {"a"}


def test_metadata_pandas_no_columns():
    """Pandas path with all metadata columns disabled."""
    df = pd.DataFrame({"a": [None]})
    tests = [NotNullTest(columns=["a"], on_fail=ContractSeverity.QUARANTINE)]
    config = QuarantineColumnsConfig(
        rejection_reason=False,
        rejected_at=False,
        source_batch_id=False,
        failed_tests=False,
        original_node=False,
    )
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    result = add_quarantine_metadata(df, {}, config, engine, node_name="n", run_id="r", tests=tests)
    assert list(result.columns) == ["a"]


# ── _apply_sampling: edge cases ─────────────────────────────────


def test_sampling_polars_both_fraction_and_max():
    df = pl.DataFrame({"a": list(range(100))})
    config = MagicMock()
    config.sample_fraction = 0.5
    config.max_rows = 5
    result = _apply_sampling(df, config, is_spark=False, is_polars=True)
    assert len(result) <= 5


def test_sampling_pandas_fraction_larger_than_df():
    """Sample fraction that produces more rows than exist."""
    df = pd.DataFrame({"a": [1, 2]})
    config = MagicMock()
    config.sample_fraction = 1.0
    config.max_rows = None
    result = _apply_sampling(df, config, is_spark=False, is_polars=False)
    assert len(result) == 2


def test_sampling_polars_fraction_small_df():
    """With a very small df, sample fraction still returns at least 1."""
    df = pl.DataFrame({"a": [1, 2]})
    config = MagicMock()
    config.sample_fraction = 0.1
    config.max_rows = None
    result = _apply_sampling(df, config, is_spark=False, is_polars=True)
    assert len(result) >= 1


# ── write_quarantine: Polars path ────────────────────────────────


def test_write_quarantine_polars_empty():
    df = pl.DataFrame({"a": pl.Series([], dtype=pl.Float64)})
    config = QuarantineConfig(connection="local", path="q_path")
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    result = write_quarantine(df, config, engine, {"local": {}})
    assert result["rows_quarantined"] == 0
    assert result["write_info"] is None


def test_write_quarantine_polars_success():
    df = pl.DataFrame({"a": [1, 2, 3]})
    config = QuarantineConfig(connection="local", path="q_path")
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    engine.write.return_value = {"status": "ok"}
    result = write_quarantine(df, config, engine, {"local": {}})
    assert result["rows_quarantined"] == 3
    assert result["quarantine_path"] == "q_path"
    engine.write.assert_called_once()


def test_write_quarantine_polars_missing_connection():
    df = pl.DataFrame({"a": [1]})
    config = QuarantineConfig(connection="missing", path="q_path")
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    with pytest.raises(ValueError, match="not found"):
        write_quarantine(df, config, engine, {"local": {}})


def test_write_quarantine_polars_with_sampling():
    df = pl.DataFrame({"a": list(range(100))})
    config = QuarantineConfig(connection="local", path="q_path", max_rows=5)
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    engine.write.return_value = {}
    result = write_quarantine(df, config, engine, {"local": {}})
    assert result["rows_quarantined"] == 5


def test_write_quarantine_table_uses_delta_format():
    """When config has table (not path), format should be 'delta'."""
    df = pl.DataFrame({"a": [1]})
    config = QuarantineConfig(connection="local", table="q_table")
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    engine.write.return_value = {}
    write_quarantine(df, config, engine, {"local": {}})
    call_kwargs = engine.write.call_args
    assert call_kwargs[1].get("format") == "delta" or call_kwargs.kwargs.get("format") == "delta"


def test_write_quarantine_path_uses_parquet_format():
    """When config has path (not table), format should be 'parquet'."""
    df = pd.DataFrame({"a": [1]})
    config = QuarantineConfig(connection="local", path="q_path")
    engine = MagicMock()
    if hasattr(engine, "spark"):
        del engine.spark
    engine.write.return_value = {}
    write_quarantine(df, config, engine, {"local": {}})
    call_kwargs = engine.write.call_args
    assert (
        call_kwargs[1].get("format") == "parquet" or call_kwargs.kwargs.get("format") == "parquet"
    )


# ── Pandas custom SQL with partial match (line 207-211) ──────────


def test_mask_pandas_custom_sql_partial():
    """Custom SQL condition that filters some rows."""
    df = pd.DataFrame({"a": [1, 2, 3, 4]})
    test = CustomSQLTest(condition="a > 2")
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert list(mask) == [False, False, True, True]


# ── Pandas regex with null values (line 204) ─────────────────────


def test_mask_pandas_regex_with_null():
    df = pd.DataFrame({"c": ["ABC", None, "123"]})
    test = RegexMatchTest(column="c", pattern=r"^[A-Z]+$")
    mask = _evaluate_test_mask(df, test, is_spark=False, is_polars=False)
    assert mask.iloc[0]
    assert mask.iloc[1]  # null passes
    assert not mask.iloc[2]
