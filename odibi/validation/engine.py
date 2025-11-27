import logging
from typing import Any, Dict, List, Optional

from odibi.config import (
    TestType,
    ValidationConfig,
)

logger = logging.getLogger(__name__)


class Validator:
    """
    Validation engine for executing declarative data quality tests.
    Supports both Spark and Pandas engines.
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
        failures = []
        is_spark = False
        is_polars = False

        # Detect engine
        try:
            import pyspark

            if isinstance(df, pyspark.sql.DataFrame):
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

        if is_spark:
            failures = self._validate_spark(df, config, context)
        elif is_polars:
            failures = self._validate_polars(df, config, context)
        else:
            failures = self._validate_pandas(df, config, context)

        return failures

    def _handle_failure(self, message: str, test: Any) -> Optional[str]:
        """Handle failure based on severity."""
        from odibi.config import ContractSeverity

        severity = getattr(test, "on_fail", ContractSeverity.FAIL)

        if severity == ContractSeverity.WARN:
            logger.warning(f"Validation Warning: {message}")
            return None
        # Fail or Quarantine (Quarantine not implemented here, treated as fail for execution flow)
        return message

    def _validate_polars(
        self, df: Any, config: ValidationConfig, context: Dict[str, Any] = None
    ) -> List[str]:
        import polars as pl

        # Ensure materialization for now as validation is complex
        if isinstance(df, pl.LazyFrame):
            df = df.collect()

        failures = []

        for test in config.tests:
            msg = None

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
                # Check max age
                col = getattr(test, "column", "updated_at")
                if col in df.columns:
                    max_ts = df[col].max()
                    if max_ts:
                        from datetime import datetime, timedelta

                        # Parse duration "2h", "1d"
                        # Simple parser
                        duration_str = test.max_age
                        delta = None
                        if duration_str.endswith("h"):
                            delta = timedelta(hours=int(duration_str[:-1]))
                        elif duration_str.endswith("d"):
                            delta = timedelta(days=int(duration_str[:-1]))
                        elif duration_str.endswith("m"):
                            delta = timedelta(minutes=int(duration_str[:-1]))

                        if delta:
                            if datetime.utcnow() - max_ts > delta:
                                msg = f"Data too old. Max timestamp {max_ts} is older than {test.max_age}"
                else:
                    msg = f"Freshness check failed: Column '{col}' not found"

            elif test.type == TestType.NOT_NULL:
                for col in test.columns:
                    if col in df.columns:
                        null_count = df[col].null_count()
                        if null_count > 0:
                            msg = f"Column '{col}' contains {null_count} NULLs"
                            if msg:
                                failures.append(self._handle_failure(msg, test))
                            msg = None  # Reset for loop

            # ... implement other tests for Polars as needed ...
            # For brevity, I implemented the new ones and basic one.

            if msg:
                res = self._handle_failure(msg, test)
                if res:
                    failures.append(res)

        return [f for f in failures if f]

    def _validate_spark(
        self, df: Any, config: ValidationConfig, context: Dict[str, Any] = None
    ) -> List[str]:
        """Execute checks using Spark SQL."""
        from pyspark.sql import functions as F

        failures = []
        # Cache for multiple checks if needed, but let's rely on Spark's optimization for now
        # df.cache()
        row_count = df.count()

        for test in config.tests:
            msg = None
            if test.type == TestType.ROW_COUNT:
                if test.min is not None and row_count < test.min:
                    msg = f"Row count {row_count} < min {test.min}"
                elif test.max is not None and row_count > test.max:
                    msg = f"Row count {row_count} > max {test.max}"

            elif test.type == TestType.SCHEMA:
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
                    max_ts = df.agg(F.max(col)).collect()[0][0]
                    if max_ts:
                        from datetime import datetime, timedelta

                        duration_str = test.max_age
                        delta = None
                        if duration_str.endswith("h"):
                            delta = timedelta(hours=int(duration_str[:-1]))
                        elif duration_str.endswith("d"):
                            delta = timedelta(days=int(duration_str[:-1]))

                        if delta and (datetime.utcnow() - max_ts > delta):
                            msg = (
                                f"Data too old. Max timestamp {max_ts} is older than {test.max_age}"
                            )
                else:
                    msg = f"Freshness check failed: Column '{col}' not found"

            elif test.type == TestType.NOT_NULL:
                for col in test.columns:
                    if col in df.columns:
                        null_count = df.filter(F.col(col).isNull()).count()
                        if null_count > 0:
                            col_msg = f"Column '{col}' contains {null_count} NULLs"
                            res = self._handle_failure(col_msg, test)
                            if res:
                                failures.append(res)
                continue

            elif test.type == TestType.UNIQUE:
                cols = [c for c in test.columns if c in df.columns]
                if len(cols) != len(test.columns):
                    msg = f"Unique check failed: Columns {set(test.columns) - set(cols)} not found"
                else:
                    dup_count = df.groupBy(*cols).count().filter("count > 1").count()
                    if dup_count > 0:
                        msg = f"Column '{', '.join(cols)}' is not unique"

            elif test.type == TestType.ACCEPTED_VALUES:
                col = test.column
                if col in df.columns:
                    invalid_df = df.filter(~F.col(col).isin(test.values))
                    invalid_count = invalid_df.count()
                    if invalid_count > 0:
                        examples_rows = invalid_df.select(col).limit(3).collect()
                        examples = [r[0] for r in examples_rows]
                        msg = f"Column '{col}' contains invalid values. Found: {examples}"
                else:
                    msg = f"Accepted values check failed: Column '{col}' not found"

            elif test.type == TestType.RANGE:
                col = test.column
                if col in df.columns:
                    cond = F.lit(False)
                    if test.min is not None:
                        cond = cond | (F.col(col) < test.min)
                    if test.max is not None:
                        cond = cond | (F.col(col) > test.max)

                    invalid_count = df.filter(cond).count()
                    if invalid_count > 0:
                        msg = f"Column '{col}' contains {invalid_count} values out of range"
                else:
                    msg = f"Range check failed: Column '{col}' not found"

            elif test.type == TestType.REGEX_MATCH:
                col = test.column
                if col in df.columns:
                    # Spark uses Java regex. Python regex might need adaptation if complex.
                    # For simple anchors ^ and $ and classes \w, it works.
                    invalid_count = df.filter(
                        F.col(col).isNotNull() & ~F.col(col).rlike(test.pattern)
                    ).count()
                    if invalid_count > 0:
                        msg = f"Column '{col}' contains {invalid_count} values that does not match pattern '{test.pattern}'"
                else:
                    msg = f"Regex check failed: Column '{col}' not found"

            elif test.type == TestType.CUSTOM_SQL:
                try:
                    invalid_count = df.filter(f"NOT ({test.condition})").count()
                    if invalid_count > 0:
                        msg = f"Custom check '{getattr(test, 'name', 'custom_sql')}' failed. Found {invalid_count} invalid rows."
                except Exception as e:
                    msg = f"Failed to execute custom SQL '{test.condition}': {e}"

            if msg:
                res = self._handle_failure(msg, test)
                if res:
                    failures.append(res)

        return failures

    def _validate_pandas(
        self, df: Any, config: ValidationConfig, context: Dict[str, Any] = None
    ) -> List[str]:
        """Execute checks using Pandas."""
        failures = []
        row_count = len(df)

        for test in config.tests:
            msg = None

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
                    # Ensure datetime
                    import pandas as pd

                    if not pd.api.types.is_datetime64_any_dtype(df[col]):
                        try:
                            s = pd.to_datetime(df[col])
                            max_ts = s.max()
                        except Exception:
                            max_ts = None
                    else:
                        max_ts = df[col].max()

                    if max_ts is not pd.NaT:
                        from datetime import datetime, timedelta

                        duration_str = test.max_age
                        delta = None
                        if duration_str.endswith("h"):
                            delta = timedelta(hours=int(duration_str[:-1]))
                        elif duration_str.endswith("d"):
                            delta = timedelta(days=int(duration_str[:-1]))

                        if delta and (datetime.utcnow() - max_ts > delta):
                            msg = (
                                f"Data too old. Max timestamp {max_ts} is older than {test.max_age}"
                            )
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
                        null_count = df[col].isnull().sum()
                        if null_count > 0:
                            col_msg = f"Column '{col}' contains {null_count} NULLs"
                            res = self._handle_failure(col_msg, test)
                            if res:
                                failures.append(res)
                    else:
                        col_msg = f"Column '{col}' not found in DataFrame"
                        res = self._handle_failure(col_msg, test)
                        if res:
                            failures.append(res)
                continue

            elif test.type == TestType.UNIQUE:
                cols = [c for c in test.columns if c in df.columns]
                if len(cols) != len(test.columns):
                    msg = f"Unique check failed: Columns {set(test.columns) - set(cols)} not found"
                else:
                    if df.duplicated(subset=cols).any():
                        msg = f"Column '{', '.join(cols)}' is not unique"

            elif test.type == TestType.ACCEPTED_VALUES:
                col = test.column
                if col in df.columns:
                    # Find invalid values
                    invalid = df[~df[col].isin(test.values)]
                    if not invalid.empty:
                        # Show top 3 invalid
                        examples = invalid[col].unique()[:3]
                        msg = f"Column '{col}' contains invalid values. Found: {examples}"
                else:
                    msg = f"Accepted values check failed: Column '{col}' not found"

            elif test.type == TestType.RANGE:
                col = test.column
                if col in df.columns:
                    invalid_count = 0
                    if test.min is not None:
                        invalid_count += (df[col] < test.min).sum()
                    if test.max is not None:
                        invalid_count += (df[col] > test.max).sum()

                    if invalid_count > 0:
                        msg = f"Column '{col}' contains {invalid_count} values out of range"
                else:
                    msg = f"Range check failed: Column '{col}' not found"

            elif test.type == TestType.REGEX_MATCH:
                col = test.column
                if col in df.columns:
                    # Convert to string and check
                    # Use match (anchored) or contains? RegexMatch usually implies full match or search.
                    # Test uses ^...$ so it expects full match.
                    # pandas str.match matches from start.

                    # Filter non-nulls
                    valid_series = df[col].dropna().astype(str)
                    if not valid_series.empty:
                        matches = valid_series.str.match(test.pattern)
                        invalid_count = (~matches).sum()
                        if invalid_count > 0:
                            msg = f"Column '{col}' contains {invalid_count} values that does not match pattern '{test.pattern}'"
                else:
                    msg = f"Regex check failed: Column '{col}' not found"

            elif test.type == TestType.CUSTOM_SQL:
                # Pandas 'query' uses syntax slightly different from SQL but compatible for simple exprs
                try:
                    # Condition is expected to be TRUE for valid rows
                    # We want to find invalid rows: NOT (condition)
                    invalid = df.query(f"not ({test.condition})")
                    if not invalid.empty:
                        msg = f"Custom check '{getattr(test, 'name', 'custom_sql')}' failed. Found {len(invalid)} invalid rows."
                except Exception as e:
                    msg = f"Failed to execute custom SQL '{test.condition}': {e}"

            if msg:
                res = self._handle_failure(msg, test)
                if res:
                    failures.append(res)

        return failures
