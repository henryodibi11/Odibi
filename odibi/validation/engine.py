import logging
from typing import Any, List

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

    def validate(self, df: Any, config: ValidationConfig) -> List[str]:
        """
        Run validation checks against a DataFrame.

        Args:
            df: Spark or Pandas DataFrame
            config: Validation configuration

        Returns:
            List of error messages (empty if all checks pass)
        """
        failures = []
        is_spark = False

        # Detect engine
        try:
            import pyspark

            if isinstance(df, pyspark.sql.DataFrame):
                is_spark = True
        except ImportError:
            pass

        if is_spark:
            failures = self._validate_spark(df, config)
        else:
            failures = self._validate_pandas(df, config)

        return failures

    def _validate_spark(self, df: Any, config: ValidationConfig) -> List[str]:
        """Execute checks using Spark SQL."""
        from pyspark.sql import functions as F

        failures = []
        row_count = df.count()

        # 1. Row Count Checks (Fastest, no scan needed sometimes)
        for test in config.tests:
            if test.type == TestType.ROW_COUNT:
                if test.min is not None and row_count < test.min:
                    failures.append(f"Row count {row_count} < min {test.min}")
                if test.max is not None and row_count > test.max:
                    failures.append(f"Row count {row_count} > max {test.max}")

        # 2. Batchable Row-Level Checks (Single Pass)
        agg_exprs = []
        check_map = {}  # Map alias -> (error_message_template, test_object)

        for i, test in enumerate(config.tests):
            alias_base = f"check_{i}"

            if test.type == TestType.NOT_NULL:
                for col in test.columns:
                    col_alias = f"{alias_base}_{col}"
                    # sum(CASE WHEN col IS NULL THEN 1 ELSE 0 END)
                    agg_exprs.append(
                        F.sum(F.when(F.col(col).isNull(), 1).otherwise(0)).alias(col_alias)
                    )
                    check_map[col_alias] = (f"Column '{col}' contains NULLs", test)

            elif test.type == TestType.ACCEPTED_VALUES:
                # sum(CASE WHEN col IS NOT NULL AND col NOT IN values THEN 1 ELSE 0 END)
                values = test.values
                agg_exprs.append(
                    F.sum(
                        F.when(
                            F.col(test.column).isNotNull() & (~F.col(test.column).isin(values)), 1
                        ).otherwise(0)
                    ).alias(alias_base)
                )
                check_map[alias_base] = (
                    f"Column '{test.column}' has invalid values (allowed: {values})",
                    test,
                )

            elif test.type == TestType.RANGE:
                # sum(CASE WHEN col < min OR col > max THEN 1 ELSE 0 END)
                cond = F.lit(False)
                if test.min is not None:
                    cond = cond | (F.col(test.column) < test.min)
                if test.max is not None:
                    cond = cond | (F.col(test.column) > test.max)

                agg_exprs.append(F.sum(F.when(cond, 1).otherwise(0)).alias(alias_base))
                check_map[alias_base] = (
                    f"Column '{test.column}' out of range [{test.min}, {test.max}]",
                    test,
                )

            elif test.type == TestType.REGEX_MATCH:
                # sum(CASE WHEN col IS NOT NULL AND NOT col RLIKE pattern THEN 1 ELSE 0 END)
                agg_exprs.append(
                    F.sum(
                        F.when(
                            F.col(test.column).isNotNull()
                            & (~F.col(test.column).rlike(test.pattern)),
                            1,
                        ).otherwise(0)
                    ).alias(alias_base)
                )
                check_map[alias_base] = (
                    f"Column '{test.column}' does not match pattern '{test.pattern}'",
                    test,
                )

            elif test.type == TestType.CUSTOM_SQL:
                # sum(CASE WHEN NOT (condition) THEN 1 ELSE 0 END)
                agg_exprs.append(
                    F.sum(F.when(~F.expr(test.condition), 1).otherwise(0)).alias(alias_base)
                )
                check_map[alias_base] = (
                    f"Custom check '{test.name or 'custom_sql'}' failed: {test.condition}",
                    test,
                )

        if agg_exprs:
            results = df.select(agg_exprs).head()
            if results:
                row = results.asDict()
                for alias, fail_count in row.items():
                    if fail_count and fail_count > 0:
                        msg, test = check_map[alias]

                        # Check thresholds for custom_sql
                        if test.type == TestType.CUSTOM_SQL:
                            fail_rate = fail_count / row_count if row_count > 0 else 0
                            if fail_rate > test.threshold:
                                failures.append(
                                    f"{msg} (Failure rate: {fail_rate:.2%} > {test.threshold:.2%})"
                                )
                        else:
                            failures.append(f"{msg} (Count: {fail_count})")

        # 3. Unique Checks (Separate Pass per check)
        for test in config.tests:
            if test.type == TestType.UNIQUE:
                for col in test.columns:
                    # count(col) > count(distinct col) ? No, that just means duplicates exist.
                    # We need to know IF duplicates exist.
                    # Efficient check: df.groupBy(col).count().filter("count > 1").limit(1).count() > 0
                    dup_exists = df.groupBy(col).count().filter("count > 1").limit(1).count() > 0
                    if dup_exists:
                        failures.append(f"Column '{col}' is not unique")

        return failures

    def _validate_pandas(self, df: Any, config: ValidationConfig) -> List[str]:
        """Execute checks using Pandas."""
        failures = []
        row_count = len(df)

        for test in config.tests:
            if test.type == TestType.ROW_COUNT:
                if test.min is not None and row_count < test.min:
                    failures.append(f"Row count {row_count} < min {test.min}")
                if test.max is not None and row_count > test.max:
                    failures.append(f"Row count {row_count} > max {test.max}")

            elif test.type == TestType.NOT_NULL:
                for col in test.columns:
                    if col in df.columns:
                        null_count = df[col].isnull().sum()
                        if null_count > 0:
                            failures.append(f"Column '{col}' contains {null_count} NULLs")
                    else:
                        failures.append(f"Column '{col}' not found in DataFrame")

            elif test.type == TestType.UNIQUE:
                for col in test.columns:
                    if col in df.columns:
                        if not df[col].is_unique:
                            dup_count = df[col].duplicated().sum()
                            failures.append(
                                f"Column '{col}' is not unique ({dup_count} duplicates)"
                            )
                    else:
                        failures.append(f"Column '{col}' not found")

            elif test.type == TestType.ACCEPTED_VALUES:
                if test.column in df.columns:
                    # Check for invalid values
                    # Note: isin works on Series. ~isin gives NOT in.
                    invalid = df[~df[test.column].isin(test.values) & df[test.column].notnull()]
                    if not invalid.empty:
                        failures.append(
                            f"Column '{test.column}' has {len(invalid)} invalid values (allowed: {test.values})"
                        )
                else:
                    failures.append(f"Column '{test.column}' not found")

            elif test.type == TestType.RANGE:
                if test.column in df.columns:
                    col_data = df[test.column]
                    invalid_count = 0
                    if test.min is not None:
                        invalid_count += (col_data < test.min).sum()
                    if test.max is not None:
                        invalid_count += (col_data > test.max).sum()

                    if invalid_count > 0:
                        failures.append(
                            f"Column '{test.column}' out of range [{test.min}, {test.max}]"
                        )
                else:
                    failures.append(f"Column '{test.column}' not found")

            elif test.type == TestType.REGEX_MATCH:
                if test.column in df.columns:
                    # coerce to string and match
                    # match returns boolean series (True if match)
                    # so we want ~match
                    invalid_count = (
                        df[test.column].notnull()
                        & ~df[test.column].astype(str).str.match(test.pattern)
                    ).sum()

                    if invalid_count > 0:
                        failures.append(
                            f"Column '{test.column}' does not match pattern '{test.pattern}'"
                        )
                else:
                    failures.append(f"Column '{test.column}' not found")

            elif test.type == TestType.CUSTOM_SQL:
                try:
                    # use DataFrame.query()
                    # query() returns rows that satisfy the condition
                    passing_count = len(df.query(test.condition))
                    fail_count = row_count - passing_count

                    fail_rate = fail_count / row_count if row_count > 0 else 0

                    if fail_rate > test.threshold:
                        failures.append(
                            f"Custom check '{test.name or 'custom_sql'}' failed: {test.condition} (Failure rate: {fail_rate:.2%} > {test.threshold:.2%})"
                        )
                except Exception as e:
                    failures.append(f"Custom check '{test.name or 'custom_sql'}' error: {e}")

        return failures
