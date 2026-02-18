"""
Optimized validation engine for executing declarative data quality tests.

Performance optimizations:
- Fail-fast mode for early exit on first failure
- DataFrame caching for Spark with many tests
- Lazy evaluation for Polars (avoids early .collect())
- Batched null count aggregation (single scan for NOT_NULL)
- Vectorized operations (no Python loops over rows)
- Memory-efficient mask operations (no full DataFrame copies)
"""

from typing import Any, Dict, List, Optional

from odibi.config import (
    ContractSeverity,
    TestType,
    ValidationConfig,
)
from odibi.utils.logging_context import get_logging_context


class Validator:
    """
    Validation engine for executing declarative data quality tests.
    Supports Spark, Pandas, and Polars engines with performance optimizations.
    """

    def validate(
        self, df: Any, config: ValidationConfig, context: Dict[str, Any] = None
    ) -> List[str]:
        """
        Run validation checks against a DataFrame.

        Args:
            df: Spark, Pandas, or Polars DataFrame
            config: Validation configuration
            context: Optional context (e.g. {'columns': ...}) for contracts

        Returns:
            List of error messages (empty if all checks pass)
        """
        ctx = get_logging_context()
        test_count = len(config.tests)
        failures = []
        is_spark = False
        is_polars = False
        engine_type = "pandas"

        try:
            import pyspark

            if isinstance(df, pyspark.sql.DataFrame):
                is_spark = True
                engine_type = "spark"
        except ImportError:
            pass

        if not is_spark:
            try:
                import polars as pl

                if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
                    is_polars = True
                    engine_type = "polars"
            except ImportError:
                pass

        ctx.debug(
            "Starting validation",
            test_count=test_count,
            engine=engine_type,
            df_type=type(df).__name__,
            fail_fast=getattr(config, "fail_fast", False),
        )

        if is_spark:
            failures = self._validate_spark(df, config, context)
        elif is_polars:
            failures = self._validate_polars(df, config, context)
        else:
            failures = self._validate_pandas(df, config, context)

        tests_passed = test_count - len(failures)
        ctx.info(
            "Validation complete",
            total_tests=test_count,
            tests_passed=tests_passed,
            tests_failed=len(failures),
            engine=engine_type,
        )

        ctx.log_validation_result(
            passed=len(failures) == 0,
            rule_name="batch_validation",
            failures=failures[:5] if failures else None,
            total_tests=test_count,
            tests_passed=tests_passed,
            tests_failed=len(failures),
        )

        return failures

    def _handle_failure(self, message: str, test: Any) -> Optional[str]:
        """Handle failure based on severity."""
        ctx = get_logging_context()
        severity = getattr(test, "on_fail", ContractSeverity.FAIL)
        test_type = getattr(test, "type", "unknown")

        if severity == ContractSeverity.WARN:
            ctx.warning(
                f"Validation Warning: {message}",
                test_type=str(test_type),
                severity="warn",
            )
            return None

        ctx.error(
            f"Validation Failed: {message}",
            test_type=str(test_type),
            severity="fail",
            test_config=str(test),
        )
        return message

    def _validate_polars(
        self, df: Any, config: ValidationConfig, context: Dict[str, Any] = None
    ) -> List[str]:
        """
        Execute checks using Polars with lazy evaluation where possible.

        Optimization: Avoids collecting full LazyFrame. Uses lazy aggregations
        and only collects scalar results.
        """
        import polars as pl

        ctx = get_logging_context()
        fail_fast = getattr(config, "fail_fast", False)
        is_lazy = isinstance(df, pl.LazyFrame)

        if is_lazy:
            row_count = df.select(pl.len()).collect().item()
            columns = df.collect_schema().names()
        else:
            row_count = len(df)
            columns = df.columns

        ctx.debug("Validating Polars DataFrame", row_count=row_count, is_lazy=is_lazy)

        failures = []

        for test in config.tests:
            msg = None
            test_type = getattr(test, "type", "unknown")
            ctx.debug("Executing test", test_type=str(test_type))

            if test.type == TestType.SCHEMA:
                if context and "columns" in context:
                    expected = set(context["columns"].keys())
                    actual = set(columns)
                    if getattr(test, "strict", True):
                        if actual != expected:
                            msg = f"Schema mismatch. Expected {expected}, got {actual}"
                    else:
                        missing = expected - actual
                        if missing:
                            msg = f"Schema mismatch. Missing columns: {missing}"

            elif test.type == TestType.ROW_COUNT:
                if test.min is not None and row_count < test.min:
                    msg = f"Row count {row_count} < min {test.min}"
                elif test.max is not None and row_count > test.max:
                    msg = f"Row count {row_count} > max {test.max}"

            elif test.type == TestType.FRESHNESS:
                col = getattr(test, "column", "updated_at")
                if col in columns:
                    if is_lazy:
                        max_ts = df.select(pl.col(col).max()).collect().item()
                    else:
                        max_ts = df[col].max()
                    if max_ts:
                        from datetime import datetime, timedelta, timezone

                        duration_str = test.max_age
                        delta = None
                        if duration_str.endswith("h"):
                            delta = timedelta(hours=int(duration_str[:-1]))
                        elif duration_str.endswith("d"):
                            delta = timedelta(days=int(duration_str[:-1]))
                        elif duration_str.endswith("m"):
                            delta = timedelta(minutes=int(duration_str[:-1]))

                        if delta:
                            # Always compare in UTC — treat naive timestamps as UTC
                            now = datetime.now(timezone.utc)
                            if hasattr(max_ts, "tzinfo") and max_ts.tzinfo is None:
                                max_ts = max_ts.replace(tzinfo=timezone.utc)
                            if now - max_ts > delta:
                                msg = (
                                    f"Data too old. Max timestamp {max_ts} "
                                    f"is older than {test.max_age}"
                                )
                else:
                    msg = f"Freshness check failed: Column '{col}' not found"

            elif test.type == TestType.NOT_NULL:
                for col in test.columns:
                    if col in columns:
                        if is_lazy:
                            null_count = df.select(pl.col(col).is_null().sum()).collect().item()
                        else:
                            null_count = df[col].null_count()
                        if null_count > 0:
                            col_msg = f"Column '{col}' contains {null_count} NULLs"
                            ctx.debug(
                                "NOT_NULL check failed",
                                column=col,
                                null_count=null_count,
                                row_count=row_count,
                            )
                            res = self._handle_failure(col_msg, test)
                            if res:
                                failures.append(res)
                                if fail_fast:
                                    return [f for f in failures if f]
                continue

            elif test.type == TestType.UNIQUE:
                cols = [c for c in test.columns if c in columns]
                if len(cols) != len(test.columns):
                    msg = f"Unique check failed: Columns {set(test.columns) - set(cols)} not found"
                else:
                    if is_lazy:
                        dup_count = (
                            df.group_by(cols)
                            .agg(pl.len().alias("cnt"))
                            .filter(pl.col("cnt") > 1)
                            .select(pl.len())
                            .collect()
                            .item()
                        )
                    else:
                        dup_count = (
                            df.group_by(cols)
                            .agg(pl.len().alias("cnt"))
                            .filter(pl.col("cnt") > 1)
                            .height
                        )
                    if dup_count > 0:
                        msg = f"Column '{', '.join(cols)}' is not unique"
                        ctx.debug(
                            "UNIQUE check failed",
                            columns=cols,
                            duplicate_groups=dup_count,
                        )

            elif test.type == TestType.ACCEPTED_VALUES:
                col = test.column
                if col in columns:
                    if is_lazy:
                        invalid_count = (
                            df.filter(~pl.col(col).is_in(test.values))
                            .select(pl.len())
                            .collect()
                            .item()
                        )
                    else:
                        invalid_count = df.filter(~pl.col(col).is_in(test.values)).height
                    if invalid_count > 0:
                        if is_lazy:
                            examples = (
                                df.filter(~pl.col(col).is_in(test.values))
                                .select(pl.col(col))
                                .limit(3)
                                .collect()[col]
                                .to_list()
                            )
                        else:
                            invalid_rows = df.filter(~pl.col(col).is_in(test.values))
                            examples = invalid_rows[col].head(3).to_list()
                        msg = f"Column '{col}' contains invalid values. Found: {examples}"
                        ctx.debug(
                            "ACCEPTED_VALUES check failed",
                            column=col,
                            invalid_count=invalid_count,
                            examples=examples,
                        )
                else:
                    msg = f"Accepted values check failed: Column '{col}' not found"

            elif test.type == TestType.RANGE:
                col = test.column
                if col in columns:
                    cond = pl.lit(False)
                    if test.min is not None:
                        cond = cond | (pl.col(col) < test.min)
                    if test.max is not None:
                        cond = cond | (pl.col(col) > test.max)
                    if is_lazy:
                        invalid_count = df.filter(cond).select(pl.len()).collect().item()
                    else:
                        invalid_count = df.filter(cond).height
                    if invalid_count > 0:
                        msg = f"Column '{col}' contains {invalid_count} values out of range"
                        ctx.debug(
                            "RANGE check failed",
                            column=col,
                            invalid_count=invalid_count,
                            min=test.min,
                            max=test.max,
                        )
                else:
                    msg = f"Range check failed: Column '{col}' not found"

            elif test.type == TestType.REGEX_MATCH:
                col = test.column
                if col in columns:
                    regex_cond = pl.col(col).is_not_null() & ~pl.col(col).str.contains(test.pattern)
                    if is_lazy:
                        invalid_count = df.filter(regex_cond).select(pl.len()).collect().item()
                    else:
                        invalid_count = df.filter(regex_cond).height
                    if invalid_count > 0:
                        msg = (
                            f"Column '{col}' contains {invalid_count} values "
                            f"that does not match pattern '{test.pattern}'"
                        )
                        ctx.debug(
                            "REGEX_MATCH check failed",
                            column=col,
                            invalid_count=invalid_count,
                            pattern=test.pattern,
                        )
                else:
                    msg = f"Regex check failed: Column '{col}' not found"

            elif test.type == TestType.CUSTOM_SQL:
                ctx.warning(
                    "CUSTOM_SQL not fully supported in Polars; skipping",
                    test_name=getattr(test, "name", "custom_sql"),
                )
                continue

            if msg:
                res = self._handle_failure(msg, test)
                if res:
                    failures.append(res)
                    if fail_fast:
                        break

        return [f for f in failures if f]

    def _validate_spark(
        self, df: Any, config: ValidationConfig, context: Dict[str, Any] = None
    ) -> List[str]:
        """
        Execute checks using Spark SQL with optimizations.

        Optimizations:
        - Optional DataFrame caching when cache_df=True
        - Batched null count aggregation (single scan for all NOT_NULL columns)
        - Fail-fast mode to skip remaining tests
        - Reuses row_count instead of re-counting
        """
        from pyspark.sql import functions as F

        ctx = get_logging_context()
        failures = []
        fail_fast = getattr(config, "fail_fast", False)
        cache_df = getattr(config, "cache_df", False)

        df_work = df
        if cache_df:
            df_work = df.cache()
            ctx.debug("DataFrame cached for validation")

        row_count = df_work.count()
        ctx.debug("Validating Spark DataFrame", row_count=row_count)

        for test in config.tests:
            msg = None
            test_type = getattr(test, "type", "unknown")
            ctx.debug("Executing test", test_type=str(test_type))

            if test.type == TestType.ROW_COUNT:
                if test.min is not None and row_count < test.min:
                    msg = f"Row count {row_count} < min {test.min}"
                elif test.max is not None and row_count > test.max:
                    msg = f"Row count {row_count} > max {test.max}"

            elif test.type == TestType.SCHEMA:
                if context and "columns" in context:
                    expected = set(context["columns"].keys())
                    actual = set(df_work.columns)
                    if getattr(test, "strict", True):
                        if actual != expected:
                            msg = f"Schema mismatch. Expected {expected}, got {actual}"
                    else:
                        missing = expected - actual
                        if missing:
                            msg = f"Schema mismatch. Missing columns: {missing}"

            elif test.type == TestType.FRESHNESS:
                col = getattr(test, "column", "updated_at")
                if col in df_work.columns:
                    max_ts = df_work.agg(F.max(col)).collect()[0][0]
                    if max_ts:
                        from datetime import datetime, timedelta, timezone

                        duration_str = test.max_age
                        delta = None
                        if duration_str.endswith("h"):
                            delta = timedelta(hours=int(duration_str[:-1]))
                        elif duration_str.endswith("d"):
                            delta = timedelta(days=int(duration_str[:-1]))
                        elif duration_str.endswith("m"):
                            delta = timedelta(minutes=int(duration_str[:-1]))

                        if delta:
                            # Handle timezone-naive timestamps by using naive now()
                            if hasattr(max_ts, "tzinfo") and max_ts.tzinfo is not None:
                                now = datetime.now(timezone.utc)
                            else:
                                now = datetime.now()
                            if now - max_ts > delta:
                                msg = f"Data too old. Max timestamp {max_ts} is older than {test.max_age}"
                else:
                    msg = f"Freshness check failed: Column '{col}' not found"

            elif test.type == TestType.NOT_NULL:
                valid_cols = [c for c in test.columns if c in df_work.columns]
                if valid_cols:
                    null_aggs = [
                        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
                        for c in valid_cols
                    ]
                    null_counts = df_work.agg(*null_aggs).collect()[0].asDict()
                    for col in valid_cols:
                        null_count = null_counts.get(col, 0) or 0
                        if null_count > 0:
                            col_msg = f"Column '{col}' contains {null_count} NULLs"
                            ctx.debug(
                                "NOT_NULL check failed",
                                column=col,
                                null_count=null_count,
                                row_count=row_count,
                            )
                            res = self._handle_failure(col_msg, test)
                            if res:
                                failures.append(res)
                                if fail_fast:
                                    if cache_df:
                                        df_work.unpersist()
                                    return failures
                continue

            elif test.type == TestType.UNIQUE:
                cols = [c for c in test.columns if c in df_work.columns]
                if len(cols) != len(test.columns):
                    msg = f"Unique check failed: Columns {set(test.columns) - set(cols)} not found"
                else:
                    dup_count = df_work.groupBy(*cols).count().filter("count > 1").count()
                    if dup_count > 0:
                        msg = f"Column '{', '.join(cols)}' is not unique"
                        ctx.debug(
                            "UNIQUE check failed",
                            columns=cols,
                            duplicate_groups=dup_count,
                        )

            elif test.type == TestType.ACCEPTED_VALUES:
                col = test.column
                if col in df_work.columns:
                    invalid_df = df_work.filter(~F.col(col).isin(test.values))
                    invalid_count = invalid_df.count()
                    if invalid_count > 0:
                        examples_rows = invalid_df.select(col).limit(3).collect()
                        examples = [r[0] for r in examples_rows]
                        msg = f"Column '{col}' contains invalid values. Found: {examples}"
                        ctx.debug(
                            "ACCEPTED_VALUES check failed",
                            column=col,
                            invalid_count=invalid_count,
                            examples=examples,
                        )
                else:
                    msg = f"Accepted values check failed: Column '{col}' not found"

            elif test.type == TestType.RANGE:
                col = test.column
                if col in df_work.columns:
                    cond = F.lit(False)
                    if test.min is not None:
                        cond = cond | (F.col(col) < test.min)
                    if test.max is not None:
                        cond = cond | (F.col(col) > test.max)

                    invalid_count = df_work.filter(cond).count()
                    if invalid_count > 0:
                        msg = f"Column '{col}' contains {invalid_count} values out of range"
                        ctx.debug(
                            "RANGE check failed",
                            column=col,
                            invalid_count=invalid_count,
                            min=test.min,
                            max=test.max,
                        )
                else:
                    msg = f"Range check failed: Column '{col}' not found"

            elif test.type == TestType.REGEX_MATCH:
                col = test.column
                if col in df_work.columns:
                    invalid_count = df_work.filter(
                        F.col(col).isNotNull() & ~F.col(col).rlike(test.pattern)
                    ).count()
                    if invalid_count > 0:
                        msg = (
                            f"Column '{col}' contains {invalid_count} values "
                            f"that does not match pattern '{test.pattern}'"
                        )
                        ctx.debug(
                            "REGEX_MATCH check failed",
                            column=col,
                            invalid_count=invalid_count,
                            pattern=test.pattern,
                        )
                else:
                    msg = f"Regex check failed: Column '{col}' not found"

            elif test.type == TestType.CUSTOM_SQL:
                try:
                    invalid_count = df_work.filter(f"NOT ({test.condition})").count()
                    if invalid_count > 0:
                        msg = (
                            f"Custom check '{getattr(test, 'name', 'custom_sql')}' failed. "
                            f"Found {invalid_count} invalid rows."
                        )
                        ctx.debug(
                            "CUSTOM_SQL check failed",
                            condition=test.condition,
                            invalid_count=invalid_count,
                        )
                except Exception as e:
                    msg = f"Failed to execute custom SQL '{test.condition}': {e}"
                    ctx.error(
                        "CUSTOM_SQL execution error",
                        condition=test.condition,
                        error=str(e),
                    )

            if msg:
                res = self._handle_failure(msg, test)
                if res:
                    failures.append(res)
                    if fail_fast:
                        break

        if cache_df:
            df_work.unpersist()

        return failures

    def _validate_pandas(
        self, df: Any, config: ValidationConfig, context: Dict[str, Any] = None
    ) -> List[str]:
        """
        Execute checks using Pandas with optimizations.

        Optimizations:
        - Single pass for UNIQUE (no double .duplicated() call)
        - Mask-based operations (no full DataFrame copies for invalid rows)
        - Memory-efficient example extraction
        - Fail-fast mode support
        """
        ctx = get_logging_context()
        failures = []
        row_count = len(df)
        fail_fast = getattr(config, "fail_fast", False)

        ctx.debug("Validating Pandas DataFrame", row_count=row_count)

        for test in config.tests:
            msg = None
            test_type = getattr(test, "type", "unknown")
            ctx.debug("Executing test", test_type=str(test_type))

            if test.type == TestType.SCHEMA:
                if context and "columns" in context:
                    expected = set(context["columns"].keys())
                    actual = set(df.columns)
                    if getattr(test, "strict", True):
                        if actual != expected:
                            msg = f"Schema mismatch. Expected {expected}, got {actual}"
                    else:
                        missing = expected - actual
                        if missing:
                            msg = f"Schema mismatch. Missing columns: {missing}"

            elif test.type == TestType.FRESHNESS:
                col = getattr(test, "column", "updated_at")
                if col in df.columns:
                    import pandas as pd

                    if not pd.api.types.is_datetime64_any_dtype(df[col]):
                        try:
                            s = pd.to_datetime(df[col])
                            max_ts = s.max()
                        except Exception as e:
                            logger = get_logging_context()
                            logger.debug(
                                f"Failed to convert column '{col}' to datetime for max_age check: {type(e).__name__}: {e}"
                            )
                            max_ts = None
                    else:
                        max_ts = df[col].max()

                    if max_ts is not None and max_ts is not pd.NaT:
                        from datetime import datetime, timedelta, timezone

                        duration_str = test.max_age
                        delta = None
                        if duration_str.endswith("h"):
                            delta = timedelta(hours=int(duration_str[:-1]))
                        elif duration_str.endswith("d"):
                            delta = timedelta(days=int(duration_str[:-1]))
                        elif duration_str.endswith("m"):
                            delta = timedelta(minutes=int(duration_str[:-1]))

                        if delta:
                            # Handle timezone-naive timestamps by using naive now()
                            if hasattr(max_ts, "tzinfo") and max_ts.tzinfo is not None:
                                now = datetime.now(timezone.utc)
                            else:
                                now = datetime.now()
                            if now - max_ts > delta:
                                msg = f"Data too old. Max timestamp {max_ts} is older than {test.max_age}"
                else:
                    msg = f"Freshness check failed: Column '{col}' not found"

            elif test.type == TestType.ROW_COUNT:
                if test.min is not None and row_count < test.min:
                    msg = f"Row count {row_count} < min {test.min}"
                elif test.max is not None and row_count > test.max:
                    msg = f"Row count {row_count} > max {test.max}"

            elif test.type == TestType.NOT_NULL:
                for col in test.columns:
                    if col in df.columns:
                        null_count = int(df[col].isnull().sum())
                        if null_count > 0:
                            col_msg = f"Column '{col}' contains {null_count} NULLs"
                            ctx.debug(
                                "NOT_NULL check failed",
                                column=col,
                                null_count=null_count,
                                row_count=row_count,
                            )
                            res = self._handle_failure(col_msg, test)
                            if res:
                                failures.append(res)
                                if fail_fast:
                                    return [f for f in failures if f]
                    else:
                        col_msg = f"Column '{col}' not found in DataFrame"
                        ctx.debug(
                            "NOT_NULL check failed - column missing",
                            column=col,
                        )
                        res = self._handle_failure(col_msg, test)
                        if res:
                            failures.append(res)
                            if fail_fast:
                                return [f for f in failures if f]
                continue

            elif test.type == TestType.UNIQUE:
                cols = [c for c in test.columns if c in df.columns]
                if len(cols) != len(test.columns):
                    msg = f"Unique check failed: Columns {set(test.columns) - set(cols)} not found"
                else:
                    dups = df.duplicated(subset=cols)
                    dup_count = int(dups.sum())
                    if dup_count > 0:
                        msg = f"Column '{', '.join(cols)}' is not unique"
                        ctx.debug(
                            "UNIQUE check failed",
                            columns=cols,
                            duplicate_rows=dup_count,
                        )

            elif test.type == TestType.ACCEPTED_VALUES:
                col = test.column
                if col in df.columns:
                    mask = ~df[col].isin(test.values)
                    invalid_count = int(mask.sum())
                    if invalid_count > 0:
                        examples = df.loc[mask, col].dropna().unique()[:3]
                        msg = f"Column '{col}' contains invalid values. Found: {list(examples)}"
                        ctx.debug(
                            "ACCEPTED_VALUES check failed",
                            column=col,
                            invalid_count=invalid_count,
                            examples=list(examples),
                        )
                else:
                    msg = f"Accepted values check failed: Column '{col}' not found"

            elif test.type == TestType.RANGE:
                col = test.column
                if col in df.columns:
                    invalid_count = 0
                    if test.min is not None:
                        invalid_count += int((df[col] < test.min).sum())
                    if test.max is not None:
                        invalid_count += int((df[col] > test.max).sum())

                    if invalid_count > 0:
                        msg = f"Column '{col}' contains {invalid_count} values out of range"
                        ctx.debug(
                            "RANGE check failed",
                            column=col,
                            invalid_count=invalid_count,
                            min=test.min,
                            max=test.max,
                        )
                else:
                    msg = f"Range check failed: Column '{col}' not found"

            elif test.type == TestType.REGEX_MATCH:
                col = test.column
                if col in df.columns:
                    valid_series = df[col].dropna().astype(str)
                    if not valid_series.empty:
                        matches = valid_series.str.match(test.pattern)
                        invalid_count = int((~matches).sum())
                        if invalid_count > 0:
                            msg = (
                                f"Column '{col}' contains {invalid_count} values "
                                f"that does not match pattern '{test.pattern}'"
                            )
                            ctx.debug(
                                "REGEX_MATCH check failed",
                                column=col,
                                invalid_count=invalid_count,
                                pattern=test.pattern,
                            )
                else:
                    msg = f"Regex check failed: Column '{col}' not found"

            elif test.type == TestType.CUSTOM_SQL:
                try:
                    mask = ~df.eval(test.condition)
                    invalid_count = int(mask.sum())
                    if invalid_count > 0:
                        msg = (
                            f"Custom check '{getattr(test, 'name', 'custom_sql')}' failed. "
                            f"Found {invalid_count} invalid rows."
                        )
                        ctx.debug(
                            "CUSTOM_SQL check failed",
                            condition=test.condition,
                            invalid_count=invalid_count,
                        )
                except Exception as e:
                    msg = f"Failed to execute custom SQL '{test.condition}': {e}"
                    ctx.error(
                        "CUSTOM_SQL execution error",
                        condition=test.condition,
                        error=str(e),
                    )

            if msg:
                res = self._handle_failure(msg, test)
                if res:
                    failures.append(res)
                    if fail_fast:
                        break

        return [f for f in failures if f]
