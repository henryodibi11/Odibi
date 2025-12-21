"""
Quarantine table support for routing failed validation rows.

This module provides functionality to:
1. Split DataFrames into valid and invalid portions based on test results
2. Add metadata columns to quarantined rows
3. Write quarantined rows to a dedicated table
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List

from odibi.config import (
    ContractSeverity,
    QuarantineColumnsConfig,
    QuarantineConfig,
    TestConfig,
    TestType,
)

logger = logging.getLogger(__name__)


@dataclass
class QuarantineResult:
    """Result of quarantine operation."""

    valid_df: Any
    invalid_df: Any
    rows_quarantined: int
    rows_valid: int
    test_results: Dict[str, List[bool]] = field(default_factory=dict)
    failed_test_details: Dict[int, List[str]] = field(default_factory=dict)


def _evaluate_test_mask(
    df: Any,
    test: TestConfig,
    is_spark: bool,
    is_polars: bool,
) -> Any:
    """
    Evaluate a single test and return a boolean mask (True = passed).

    Args:
        df: DataFrame to evaluate
        test: Test configuration
        is_spark: Whether using Spark engine
        is_polars: Whether using Polars engine

    Returns:
        Boolean mask where True means the row passed the test
    """
    if is_spark:
        from pyspark.sql import functions as F

        if test.type == TestType.NOT_NULL:
            masks = []
            for col in test.columns:
                if col in df.columns:
                    masks.append(F.col(col).isNotNull())
            if masks:
                combined = masks[0]
                for m in masks[1:]:
                    combined = combined & m
                return combined
            return F.lit(True)

        elif test.type == TestType.UNIQUE:
            return F.lit(True)

        elif test.type == TestType.ACCEPTED_VALUES:
            col = test.column
            if col in df.columns:
                return F.col(col).isin(test.values)
            return F.lit(True)

        elif test.type == TestType.RANGE:
            col = test.column
            if col in df.columns:
                cond = F.lit(True)
                if test.min is not None:
                    cond = cond & (F.col(col) >= test.min)
                if test.max is not None:
                    cond = cond & (F.col(col) <= test.max)
                return cond
            return F.lit(True)

        elif test.type == TestType.REGEX_MATCH:
            col = test.column
            if col in df.columns:
                return F.col(col).rlike(test.pattern) | F.col(col).isNull()
            return F.lit(True)

        elif test.type == TestType.CUSTOM_SQL:
            try:
                return F.expr(test.condition)
            except Exception:
                return F.lit(True)

        return F.lit(True)

    elif is_polars:
        import polars as pl

        if test.type == TestType.NOT_NULL:
            masks = []
            for col in test.columns:
                if col in df.columns:
                    masks.append(pl.col(col).is_not_null())
            if masks:
                combined = masks[0]
                for m in masks[1:]:
                    combined = combined & m
                return combined
            return pl.lit(True)

        elif test.type == TestType.ACCEPTED_VALUES:
            col = test.column
            if col in df.columns:
                return pl.col(col).is_in(test.values)
            return pl.lit(True)

        elif test.type == TestType.RANGE:
            col = test.column
            if col in df.columns:
                cond = pl.lit(True)
                if test.min is not None:
                    cond = cond & (pl.col(col) >= test.min)
                if test.max is not None:
                    cond = cond & (pl.col(col) <= test.max)
                return cond
            return pl.lit(True)

        return pl.lit(True)

    else:
        import pandas as pd

        if test.type == TestType.NOT_NULL:
            masks = []
            for col in test.columns:
                if col in df.columns:
                    masks.append(df[col].notna())
            if masks:
                combined = masks[0]
                for m in masks[1:]:
                    combined = combined & m
                return combined
            return pd.Series([True] * len(df), index=df.index)

        elif test.type == TestType.UNIQUE:
            return pd.Series([True] * len(df), index=df.index)

        elif test.type == TestType.ACCEPTED_VALUES:
            col = test.column
            if col in df.columns:
                return df[col].isin(test.values)
            return pd.Series([True] * len(df), index=df.index)

        elif test.type == TestType.RANGE:
            col = test.column
            if col in df.columns:
                mask = pd.Series([True] * len(df), index=df.index)
                if test.min is not None:
                    mask = mask & (df[col] >= test.min)
                if test.max is not None:
                    mask = mask & (df[col] <= test.max)
                return mask
            return pd.Series([True] * len(df), index=df.index)

        elif test.type == TestType.REGEX_MATCH:
            col = test.column
            if col in df.columns:
                return df[col].isna() | df[col].astype(str).str.match(test.pattern, na=True)
            return pd.Series([True] * len(df), index=df.index)

        elif test.type == TestType.CUSTOM_SQL:
            try:
                valid = df.query(test.condition)
                mask = df.index.isin(valid.index)
                return pd.Series(mask, index=df.index)
            except Exception:
                return pd.Series([True] * len(df), index=df.index)

        return pd.Series([True] * len(df), index=df.index)


def split_valid_invalid(
    df: Any,
    tests: List[TestConfig],
    engine: Any,
) -> QuarantineResult:
    """
    Split DataFrame into valid and invalid portions based on quarantine tests.

    Only tests with on_fail == QUARANTINE are evaluated for splitting.
    A row is invalid if it fails ANY quarantine test.

    Args:
        df: DataFrame to split
        tests: List of test configurations
        engine: Engine instance (Spark, Pandas, or Polars)

    Returns:
        QuarantineResult with valid_df, invalid_df, and test metadata
    """
    is_spark = False
    is_polars = False

    try:
        import pyspark

        if hasattr(engine, "spark") or isinstance(df, pyspark.sql.DataFrame):
            is_spark = True
    except ImportError:
        pass

    if not is_spark:
        try:
            import polars as pl

            if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
                is_polars = True
        except ImportError:
            pass

    quarantine_tests = [t for t in tests if t.on_fail == ContractSeverity.QUARANTINE]

    if not quarantine_tests:
        if is_spark:
            from pyspark.sql import functions as F

            empty_df = df.filter(F.lit(False))
        elif is_polars:
            import polars as pl

            empty_df = df.filter(pl.lit(False))
        else:
            empty_df = df.iloc[0:0].copy()

        row_count = engine.count_rows(df) if hasattr(engine, "count_rows") else len(df)
        return QuarantineResult(
            valid_df=df,
            invalid_df=empty_df,
            rows_quarantined=0,
            rows_valid=row_count,
            test_results={},
            failed_test_details={},
        )

    test_masks = {}
    test_names = []

    for idx, test in enumerate(quarantine_tests):
        base_name = test.name or f"{test.type.value}"
        test_name = base_name if base_name not in test_masks else f"{base_name}_{idx}"
        test_names.append(test_name)
        mask = _evaluate_test_mask(df, test, is_spark, is_polars)
        test_masks[test_name] = mask

    if is_spark:
        from pyspark.sql import functions as F

        combined_valid_mask = F.lit(True)
        for mask in test_masks.values():
            combined_valid_mask = combined_valid_mask & mask

        df_cached = df.cache()

        valid_df = df_cached.filter(combined_valid_mask)
        invalid_df = df_cached.filter(~combined_valid_mask)

        valid_df = valid_df.cache()
        invalid_df = invalid_df.cache()

        rows_valid = valid_df.count()
        rows_quarantined = invalid_df.count()
        total = rows_valid + rows_quarantined

        test_results = {}
        for name, mask in test_masks.items():
            pass_count = df_cached.filter(mask).count()
            test_results[name] = [True] * pass_count + [False] * (total - pass_count)

        df_cached.unpersist()

    elif is_polars:
        import polars as pl

        combined_valid_mask = pl.lit(True)
        for mask in test_masks.values():
            combined_valid_mask = combined_valid_mask & mask

        valid_df = df.filter(combined_valid_mask)
        invalid_df = df.filter(~combined_valid_mask)

        rows_valid = len(valid_df)
        rows_quarantined = len(invalid_df)

        test_results = {}

    else:
        import pandas as pd

        combined_valid_mask = pd.Series([True] * len(df), index=df.index)
        for mask in test_masks.values():
            combined_valid_mask = combined_valid_mask & mask

        valid_df = df[combined_valid_mask].copy()
        invalid_df = df[~combined_valid_mask].copy()

        rows_valid = len(valid_df)
        rows_quarantined = len(invalid_df)

        test_results = {}
        for name, mask in test_masks.items():
            test_results[name] = mask.tolist()

    logger.info(f"Quarantine split: {rows_valid} valid, {rows_quarantined} invalid")

    return QuarantineResult(
        valid_df=valid_df,
        invalid_df=invalid_df,
        rows_quarantined=rows_quarantined,
        rows_valid=rows_valid,
        test_results=test_results,
        failed_test_details={},
    )


def add_quarantine_metadata(
    invalid_df: Any,
    test_results: Dict[str, List[bool]],
    config: QuarantineColumnsConfig,
    engine: Any,
    node_name: str,
    run_id: str,
    tests: List[TestConfig],
) -> Any:
    """
    Add metadata columns to quarantined rows.

    Args:
        invalid_df: DataFrame of invalid rows
        test_results: Dict of test_name -> per-row boolean results
        config: QuarantineColumnsConfig specifying which columns to add
        engine: Engine instance
        node_name: Name of the originating node
        run_id: Current run ID
        tests: List of test configurations (for building failure reasons)

    Returns:
        DataFrame with added metadata columns
    """
    is_spark = False
    is_polars = False

    try:
        import pyspark

        if hasattr(engine, "spark") or isinstance(invalid_df, pyspark.sql.DataFrame):
            is_spark = True
    except ImportError:
        pass

    if not is_spark:
        try:
            import polars as pl

            if isinstance(invalid_df, (pl.DataFrame, pl.LazyFrame)):
                is_polars = True
        except ImportError:
            pass

    rejected_at = datetime.now(timezone.utc).isoformat()

    quarantine_tests = [t for t in tests if t.on_fail == ContractSeverity.QUARANTINE]
    test_names = [t.name or f"{t.type.value}" for t in quarantine_tests]
    failed_tests_str = ",".join(test_names)
    rejection_reason = f"Failed tests: {failed_tests_str}"

    if is_spark:
        from pyspark.sql import functions as F

        result_df = invalid_df

        if config.rejection_reason:
            result_df = result_df.withColumn("_rejection_reason", F.lit(rejection_reason))

        if config.rejected_at:
            result_df = result_df.withColumn("_rejected_at", F.lit(rejected_at))

        if config.source_batch_id:
            result_df = result_df.withColumn("_source_batch_id", F.lit(run_id))

        if config.failed_tests:
            result_df = result_df.withColumn("_failed_tests", F.lit(failed_tests_str))

        if config.original_node:
            result_df = result_df.withColumn("_original_node", F.lit(node_name))

        return result_df

    elif is_polars:
        import polars as pl

        result_df = invalid_df

        if config.rejection_reason:
            result_df = result_df.with_columns(pl.lit(rejection_reason).alias("_rejection_reason"))

        if config.rejected_at:
            result_df = result_df.with_columns(pl.lit(rejected_at).alias("_rejected_at"))

        if config.source_batch_id:
            result_df = result_df.with_columns(pl.lit(run_id).alias("_source_batch_id"))

        if config.failed_tests:
            result_df = result_df.with_columns(pl.lit(failed_tests_str).alias("_failed_tests"))

        if config.original_node:
            result_df = result_df.with_columns(pl.lit(node_name).alias("_original_node"))

        return result_df

    else:
        result_df = invalid_df.copy()

        if config.rejection_reason:
            result_df["_rejection_reason"] = rejection_reason

        if config.rejected_at:
            result_df["_rejected_at"] = rejected_at

        if config.source_batch_id:
            result_df["_source_batch_id"] = run_id

        if config.failed_tests:
            result_df["_failed_tests"] = failed_tests_str

        if config.original_node:
            result_df["_original_node"] = node_name

        return result_df


def write_quarantine(
    invalid_df: Any,
    config: QuarantineConfig,
    engine: Any,
    connections: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Write quarantined rows to destination (always append mode).

    Args:
        invalid_df: DataFrame of invalid rows with metadata
        config: QuarantineConfig specifying destination
        engine: Engine instance
        connections: Dict of connection configurations

    Returns:
        Dict with write result metadata
    """
    is_spark = False
    is_polars = False

    try:
        import pyspark

        if hasattr(engine, "spark") or isinstance(invalid_df, pyspark.sql.DataFrame):
            is_spark = True
    except ImportError:
        pass

    if not is_spark:
        try:
            import polars as pl

            if isinstance(invalid_df, (pl.DataFrame, pl.LazyFrame)):
                is_polars = True
        except ImportError:
            pass

    if is_spark:
        row_count = invalid_df.count()
    elif is_polars:
        row_count = len(invalid_df)
    else:
        row_count = len(invalid_df)

    if row_count == 0:
        return {
            "rows_quarantined": 0,
            "quarantine_path": config.path or config.table,
            "write_info": None,
        }

    connection = connections.get(config.connection)
    if connection is None:
        raise ValueError(
            f"Quarantine connection '{config.connection}' not found. "
            f"Available: {', '.join(connections.keys())}"
        )

    try:
        write_result = engine.write(
            invalid_df,
            connection=connection,
            format="delta" if config.table else "parquet",
            path=config.path,
            table=config.table,
            mode="append",
        )
    except Exception as e:
        logger.error(f"Failed to write quarantine data: {e}")
        raise

    logger.info(f"Wrote {row_count} rows to quarantine: {config.path or config.table}")

    return {
        "rows_quarantined": row_count,
        "quarantine_path": config.path or config.table,
        "write_info": write_result,
    }


def has_quarantine_tests(tests: List[TestConfig]) -> bool:
    """Check if any tests use quarantine severity."""
    return any(t.on_fail == ContractSeverity.QUARANTINE for t in tests)
