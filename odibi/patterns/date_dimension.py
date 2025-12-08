import time
from datetime import date, datetime
from typing import Any, Optional

import pandas as pd

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.patterns.base import Pattern
from odibi.utils.logging_context import get_logging_context


class DateDimensionPattern(Pattern):
    """
    Date Dimension Pattern: Generates a complete date dimension table.

    Creates a date dimension with pre-calculated attributes useful for
    BI/reporting including day of week, quarter, fiscal year, etc.

    Params:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        date_key_format (str): Format for date_sk (default: "yyyyMMdd" -> 20240115)
        fiscal_year_start_month (int): Month when fiscal year starts (1-12, default: 1)
        include_time (bool): If true, generate time dimension (not implemented yet)
        unknown_member (bool): If true, add unknown date row with date_sk=0

    Generated Columns:
        - date_sk: Integer surrogate key (YYYYMMDD format)
        - full_date: The actual date
        - day_of_week: Day name (Monday, Tuesday, etc.)
        - day_of_week_num: Day number (1=Monday, 7=Sunday)
        - day_of_month: Day of month (1-31)
        - day_of_year: Day of year (1-366)
        - is_weekend: Boolean flag
        - week_of_year: ISO week number (1-53)
        - month: Month number (1-12)
        - month_name: Month name (January, February, etc.)
        - quarter: Calendar quarter (1-4)
        - quarter_name: Q1, Q2, Q3, Q4
        - year: Calendar year
        - fiscal_year: Fiscal year
        - fiscal_quarter: Fiscal quarter (1-4)
        - is_month_start: First day of month
        - is_month_end: Last day of month
        - is_year_start: First day of year
        - is_year_end: Last day of year
    """

    def validate(self) -> None:
        ctx = get_logging_context()
        ctx.debug(
            "DateDimensionPattern validation starting",
            pattern="DateDimensionPattern",
            params=self.params,
        )

        if not self.params.get("start_date"):
            ctx.error(
                "DateDimensionPattern validation failed: 'start_date' is required",
                pattern="DateDimensionPattern",
            )
            raise ValueError("DateDimensionPattern: 'start_date' parameter is required.")

        if not self.params.get("end_date"):
            ctx.error(
                "DateDimensionPattern validation failed: 'end_date' is required",
                pattern="DateDimensionPattern",
            )
            raise ValueError("DateDimensionPattern: 'end_date' parameter is required.")

        try:
            start = self._parse_date(self.params["start_date"])
            end = self._parse_date(self.params["end_date"])
            if start > end:
                raise ValueError("start_date must be before or equal to end_date")
        except Exception as e:
            ctx.error(
                f"DateDimensionPattern validation failed: {e}",
                pattern="DateDimensionPattern",
            )
            raise ValueError(f"DateDimensionPattern: Invalid date parameters: {e}")

        fiscal_month = self.params.get("fiscal_year_start_month", 1)
        if not isinstance(fiscal_month, int) or fiscal_month < 1 or fiscal_month > 12:
            ctx.error(
                "DateDimensionPattern validation failed: invalid fiscal_year_start_month",
                pattern="DateDimensionPattern",
            )
            raise ValueError(
                "DateDimensionPattern: 'fiscal_year_start_month' must be an integer 1-12."
            )

        ctx.debug(
            "DateDimensionPattern validation passed",
            pattern="DateDimensionPattern",
        )

    def _parse_date(self, date_str: str) -> date:
        """Parse a date string in YYYY-MM-DD format."""
        if isinstance(date_str, (date, datetime)):
            return date_str if isinstance(date_str, date) else date_str.date()
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    def execute(self, context: EngineContext) -> Any:
        ctx = get_logging_context()
        start_time = time.time()

        start_date = self._parse_date(self.params["start_date"])
        end_date = self._parse_date(self.params["end_date"])
        fiscal_year_start_month = self.params.get("fiscal_year_start_month", 1)
        unknown_member = self.params.get("unknown_member", False)

        ctx.debug(
            "DateDimensionPattern starting",
            pattern="DateDimensionPattern",
            start_date=str(start_date),
            end_date=str(end_date),
            fiscal_year_start_month=fiscal_year_start_month,
        )

        try:
            if context.engine_type == EngineType.SPARK:
                result_df = self._generate_spark(
                    context, start_date, end_date, fiscal_year_start_month
                )
            else:
                result_df = self._generate_pandas(start_date, end_date, fiscal_year_start_month)

            if unknown_member:
                result_df = self._add_unknown_member(context, result_df)

            row_count = self._get_row_count(result_df, context.engine_type)
            elapsed_ms = (time.time() - start_time) * 1000

            ctx.info(
                "DateDimensionPattern completed",
                pattern="DateDimensionPattern",
                elapsed_ms=round(elapsed_ms, 2),
                rows_generated=row_count,
                start_date=str(start_date),
                end_date=str(end_date),
            )

            return result_df

        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            ctx.error(
                f"DateDimensionPattern failed: {e}",
                pattern="DateDimensionPattern",
                error_type=type(e).__name__,
                elapsed_ms=round(elapsed_ms, 2),
            )
            raise

    def _get_row_count(self, df, engine_type) -> Optional[int]:
        try:
            if engine_type == EngineType.SPARK:
                return df.count()
            else:
                return len(df)
        except Exception:
            return None

    def _generate_pandas(
        self, start_date: date, end_date: date, fiscal_year_start_month: int
    ) -> pd.DataFrame:
        """Generate date dimension using Pandas."""
        dates = pd.date_range(start=start_date, end=end_date, freq="D")

        df = pd.DataFrame({"full_date": dates})

        df["date_sk"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)

        df["day_of_week"] = df["full_date"].dt.day_name()
        df["day_of_week_num"] = df["full_date"].dt.dayofweek + 1
        df["day_of_month"] = df["full_date"].dt.day
        df["day_of_year"] = df["full_date"].dt.dayofyear

        df["is_weekend"] = df["day_of_week_num"].isin([6, 7])

        df["week_of_year"] = df["full_date"].dt.isocalendar().week.astype(int)

        df["month"] = df["full_date"].dt.month
        df["month_name"] = df["full_date"].dt.month_name()

        df["quarter"] = df["full_date"].dt.quarter
        df["quarter_name"] = "Q" + df["quarter"].astype(str)

        df["year"] = df["full_date"].dt.year

        df["fiscal_year"] = df.apply(
            lambda row: self._calc_fiscal_year(row["full_date"], fiscal_year_start_month),
            axis=1,
        )
        df["fiscal_quarter"] = df.apply(
            lambda row: self._calc_fiscal_quarter(row["full_date"], fiscal_year_start_month),
            axis=1,
        )

        df["is_month_start"] = df["full_date"].dt.is_month_start
        df["is_month_end"] = df["full_date"].dt.is_month_end
        df["is_year_start"] = (df["month"] == 1) & (df["day_of_month"] == 1)
        df["is_year_end"] = (df["month"] == 12) & (df["day_of_month"] == 31)

        df["full_date"] = df["full_date"].dt.date

        column_order = [
            "date_sk",
            "full_date",
            "day_of_week",
            "day_of_week_num",
            "day_of_month",
            "day_of_year",
            "is_weekend",
            "week_of_year",
            "month",
            "month_name",
            "quarter",
            "quarter_name",
            "year",
            "fiscal_year",
            "fiscal_quarter",
            "is_month_start",
            "is_month_end",
            "is_year_start",
            "is_year_end",
        ]
        return df[column_order]

    def _calc_fiscal_year(self, dt, fiscal_start_month: int) -> int:
        """Calculate fiscal year based on fiscal start month."""
        if isinstance(dt, pd.Timestamp):
            month = dt.month
            year = dt.year
        else:
            month = dt.month
            year = dt.year

        if fiscal_start_month == 1:
            return year
        if month >= fiscal_start_month:
            return year + 1
        return year

    def _calc_fiscal_quarter(self, dt, fiscal_start_month: int) -> int:
        """Calculate fiscal quarter based on fiscal start month."""
        if isinstance(dt, pd.Timestamp):
            month = dt.month
        else:
            month = dt.month

        adjusted_month = (month - fiscal_start_month) % 12
        return (adjusted_month // 3) + 1

    def _generate_spark(
        self, context: EngineContext, start_date: date, end_date: date, fiscal_year_start_month: int
    ):
        """Generate date dimension using Spark."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import IntegerType

        spark = context.spark

        num_days = (end_date - start_date).days + 1
        start_date_str = start_date.strftime("%Y-%m-%d")

        df = spark.range(num_days).select(
            F.date_add(F.lit(start_date_str), F.col("id").cast(IntegerType())).alias("full_date")
        )

        df = df.withColumn("date_sk", F.date_format("full_date", "yyyyMMdd").cast(IntegerType()))

        df = df.withColumn("day_of_week", F.date_format("full_date", "EEEE"))
        df = df.withColumn("day_of_week_num", F.dayofweek("full_date"))
        df = df.withColumn(
            "day_of_week_num",
            F.when(F.col("day_of_week_num") == 1, 7).otherwise(F.col("day_of_week_num") - 1),
        )
        df = df.withColumn("day_of_month", F.dayofmonth("full_date"))
        df = df.withColumn("day_of_year", F.dayofyear("full_date"))

        df = df.withColumn("is_weekend", F.col("day_of_week_num").isin([6, 7]))

        df = df.withColumn("week_of_year", F.weekofyear("full_date"))

        df = df.withColumn("month", F.month("full_date"))
        df = df.withColumn("month_name", F.date_format("full_date", "MMMM"))

        df = df.withColumn("quarter", F.quarter("full_date"))
        df = df.withColumn("quarter_name", F.concat(F.lit("Q"), F.col("quarter")))

        df = df.withColumn("year", F.year("full_date"))

        if fiscal_year_start_month == 1:
            df = df.withColumn("fiscal_year", F.col("year"))
            df = df.withColumn("fiscal_quarter", F.col("quarter"))
        else:
            df = df.withColumn(
                "fiscal_year",
                F.when(F.col("month") >= fiscal_year_start_month, F.col("year") + 1).otherwise(
                    F.col("year")
                ),
            )
            adjusted_month = (F.col("month") - fiscal_year_start_month + 12) % 12
            df = df.withColumn("fiscal_quarter", (adjusted_month / 3).cast(IntegerType()) + 1)

        df = df.withColumn(
            "is_month_start",
            F.col("day_of_month") == 1,
        )
        df = df.withColumn(
            "is_month_end",
            F.col("full_date") == F.last_day("full_date"),
        )
        df = df.withColumn(
            "is_year_start",
            (F.col("month") == 1) & (F.col("day_of_month") == 1),
        )
        df = df.withColumn(
            "is_year_end",
            (F.col("month") == 12) & (F.col("day_of_month") == 31),
        )

        column_order = [
            "date_sk",
            "full_date",
            "day_of_week",
            "day_of_week_num",
            "day_of_month",
            "day_of_year",
            "is_weekend",
            "week_of_year",
            "month",
            "month_name",
            "quarter",
            "quarter_name",
            "year",
            "fiscal_year",
            "fiscal_quarter",
            "is_month_start",
            "is_month_end",
            "is_year_start",
            "is_year_end",
        ]
        return df.select(column_order)

    def _add_unknown_member(self, context: EngineContext, df):
        """Add unknown member row with date_sk=0."""
        if context.engine_type == EngineType.SPARK:
            from pyspark.sql import Row

            unknown_data = {
                "date_sk": 0,
                "full_date": date(1900, 1, 1),
                "day_of_week": "Unknown",
                "day_of_week_num": 0,
                "day_of_month": 0,
                "day_of_year": 0,
                "is_weekend": False,
                "week_of_year": 0,
                "month": 0,
                "month_name": "Unknown",
                "quarter": 0,
                "quarter_name": "Unknown",
                "year": 0,
                "fiscal_year": 0,
                "fiscal_quarter": 0,
                "is_month_start": False,
                "is_month_end": False,
                "is_year_start": False,
                "is_year_end": False,
            }
            unknown_row = context.spark.createDataFrame([Row(**unknown_data)])
            return unknown_row.unionByName(df)
        else:
            unknown_row = pd.DataFrame(
                [
                    {
                        "date_sk": 0,
                        "full_date": date(1900, 1, 1),
                        "day_of_week": "Unknown",
                        "day_of_week_num": 0,
                        "day_of_month": 0,
                        "day_of_year": 0,
                        "is_weekend": False,
                        "week_of_year": 0,
                        "month": 0,
                        "month_name": "Unknown",
                        "quarter": 0,
                        "quarter_name": "Unknown",
                        "year": 0,
                        "fiscal_year": 0,
                        "fiscal_quarter": 0,
                        "is_month_start": False,
                        "is_month_end": False,
                        "is_year_start": False,
                        "is_year_end": False,
                    }
                ]
            )
            return pd.concat([unknown_row, df], ignore_index=True)
