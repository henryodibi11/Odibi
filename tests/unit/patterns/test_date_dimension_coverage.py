"""Comprehensive unit tests for DateDimensionPattern (Pandas paths)."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.patterns.date_dimension import DateDimensionPattern


# ---------------------------------------------------------------------------
# Helpers / Fixtures
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS = [
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


def _make_context(df=None, engine=None):
    return EngineContext(
        context=PandasContext(),
        df=df,
        engine_type=EngineType.PANDAS,
        engine=engine,
    )


@pytest.fixture
def engine():
    return MagicMock()


def _make_pattern(engine, params):
    cfg = MagicMock()
    cfg.name = "dim_date"
    cfg.params = params
    return DateDimensionPattern(engine=engine, config=cfg)


# ===================================================================
# 1. _calc_fiscal_year
# ===================================================================


class TestCalcFiscalYear:
    def test_fiscal_start_jan_returns_same_year(self, engine):
        p = _make_pattern(engine, {"start_date": "2024-01-01", "end_date": "2024-01-01"})
        dt = pd.Timestamp("2024-06-15")
        assert p._calc_fiscal_year(dt, fiscal_start_month=1) == 2024

    def test_fiscal_start_july_month_after(self, engine):
        p = _make_pattern(engine, {"start_date": "2024-01-01", "end_date": "2024-01-01"})
        # July is >= 7 → fiscal year = year + 1
        assert p._calc_fiscal_year(pd.Timestamp("2024-07-01"), fiscal_start_month=7) == 2025

    def test_fiscal_start_july_month_before(self, engine):
        p = _make_pattern(engine, {"start_date": "2024-01-01", "end_date": "2024-01-01"})
        # March < 7 → fiscal year = year
        assert p._calc_fiscal_year(pd.Timestamp("2024-03-15"), fiscal_start_month=7) == 2024

    def test_with_date_object(self, engine):
        p = _make_pattern(engine, {"start_date": "2024-01-01", "end_date": "2024-01-01"})
        assert p._calc_fiscal_year(date(2024, 10, 1), fiscal_start_month=7) == 2025


# ===================================================================
# 2. _calc_fiscal_quarter
# ===================================================================


class TestCalcFiscalQuarter:
    def test_fiscal_start_jan(self, engine):
        p = _make_pattern(engine, {"start_date": "2024-01-01", "end_date": "2024-01-01"})
        assert p._calc_fiscal_quarter(pd.Timestamp("2024-01-15"), fiscal_start_month=1) == 1
        assert p._calc_fiscal_quarter(pd.Timestamp("2024-04-15"), fiscal_start_month=1) == 2
        assert p._calc_fiscal_quarter(pd.Timestamp("2024-07-15"), fiscal_start_month=1) == 3
        assert p._calc_fiscal_quarter(pd.Timestamp("2024-10-15"), fiscal_start_month=1) == 4

    def test_fiscal_start_july(self, engine):
        p = _make_pattern(engine, {"start_date": "2024-01-01", "end_date": "2024-01-01"})
        # July → adjusted_month = (7-7)%12 = 0 → Q1
        assert p._calc_fiscal_quarter(pd.Timestamp("2024-07-01"), fiscal_start_month=7) == 1
        # October → adjusted = (10-7)%12 = 3 → Q2
        assert p._calc_fiscal_quarter(pd.Timestamp("2024-10-01"), fiscal_start_month=7) == 2
        # January → adjusted = (1-7)%12 = 6 → Q3
        assert p._calc_fiscal_quarter(pd.Timestamp("2024-01-01"), fiscal_start_month=7) == 3
        # April → adjusted = (4-7)%12 = 9 → Q4
        assert p._calc_fiscal_quarter(pd.Timestamp("2024-04-01"), fiscal_start_month=7) == 4

    def test_with_date_object(self, engine):
        p = _make_pattern(engine, {"start_date": "2024-01-01", "end_date": "2024-01-01"})
        assert p._calc_fiscal_quarter(date(2024, 3, 1), fiscal_start_month=1) == 1


# ===================================================================
# 3. _generate_pandas
# ===================================================================


class TestGeneratePandas:
    def test_small_range_columns_and_values(self, engine):
        p = _make_pattern(engine, {"start_date": "2024-01-01", "end_date": "2024-01-03"})
        df = p._generate_pandas(date(2024, 1, 1), date(2024, 1, 3), fiscal_year_start_month=1)

        assert len(df) == 3
        assert list(df.columns) == EXPECTED_COLUMNS

        # date_sk format check
        assert df["date_sk"].iloc[0] == 20240101
        assert df["date_sk"].iloc[2] == 20240103

        # full_date values
        assert df["full_date"].iloc[0] == date(2024, 1, 1)

        # day_of_week: Jan 1, 2024 is Monday
        assert df["day_of_week"].iloc[0] == "Monday"
        assert df["day_of_week_num"].iloc[0] == 1

        # weekend check: Jan 1 Mon = not weekend
        assert not df["is_weekend"].iloc[0]

        # month/year
        assert df["month"].iloc[0] == 1
        assert df["month_name"].iloc[0] == "January"
        assert df["year"].iloc[0] == 2024
        assert df["quarter"].iloc[0] == 1
        assert df["quarter_name"].iloc[0] == "Q1"

        # fiscal columns with start_month=1
        assert df["fiscal_year"].iloc[0] == 2024
        assert df["fiscal_quarter"].iloc[0] == 1

        # boundary flags
        assert df["is_month_start"].iloc[0]
        assert df["is_year_start"].iloc[0]

    def test_weekend_detection(self, engine):
        p = _make_pattern(engine, {"start_date": "2024-01-06", "end_date": "2024-01-07"})
        df = p._generate_pandas(date(2024, 1, 6), date(2024, 1, 7), fiscal_year_start_month=1)
        # Jan 6, 2024 is Saturday (day_of_week_num=6), Jan 7 is Sunday (7)
        assert df["is_weekend"].iloc[0]
        assert df["is_weekend"].iloc[1]

    def test_fiscal_year_non_jan_start(self, engine):
        p = _make_pattern(engine, {"start_date": "2024-07-01", "end_date": "2024-07-01"})
        df = p._generate_pandas(date(2024, 7, 1), date(2024, 7, 1), fiscal_year_start_month=7)
        assert df["fiscal_year"].iloc[0] == 2025
        assert df["fiscal_quarter"].iloc[0] == 1


# ===================================================================
# 4. _add_unknown_member (Pandas path)
# ===================================================================


class TestAddUnknownMember:
    def test_adds_unknown_row_pandas(self, engine):
        p = _make_pattern(engine, {"start_date": "2024-01-01", "end_date": "2024-01-01"})
        context = _make_context()

        base_df = p._generate_pandas(date(2024, 1, 1), date(2024, 1, 1), 1)
        result = p._add_unknown_member(context, base_df)

        assert len(result) == len(base_df) + 1
        first_row = result.iloc[0]
        assert first_row["date_sk"] == 0
        assert first_row["full_date"] == date(1900, 1, 1)
        assert first_row["day_of_week"] == "Unknown"
        assert first_row["month_name"] == "Unknown"


# ===================================================================
# 5. _get_row_count
# ===================================================================


class TestGetRowCount:
    def test_pandas_df(self, engine):
        p = _make_pattern(engine, {"start_date": "2024-01-01", "end_date": "2024-01-01"})
        df = pd.DataFrame({"a": [1, 2, 3]})
        assert p._get_row_count(df, EngineType.PANDAS) == 3

    def test_exception_returns_none(self, engine):
        p = _make_pattern(engine, {"start_date": "2024-01-01", "end_date": "2024-01-01"})
        bad_df = MagicMock()
        bad_df.__len__ = MagicMock(side_effect=RuntimeError("oops"))
        assert p._get_row_count(bad_df, EngineType.PANDAS) is None


# ===================================================================
# 6. execute() — Pandas path
# ===================================================================


class TestExecutePandas:
    @patch("odibi.patterns.date_dimension.get_logging_context")
    def test_execute_without_unknown_member(self, mock_ctx, engine):
        mock_ctx.return_value = MagicMock()
        p = _make_pattern(
            engine,
            {
                "start_date": "2024-01-01",
                "end_date": "2024-01-05",
            },
        )
        context = _make_context()
        result = p.execute(context)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5
        assert list(result.columns) == EXPECTED_COLUMNS

    @patch("odibi.patterns.date_dimension.get_logging_context")
    def test_execute_with_unknown_member(self, mock_ctx, engine):
        mock_ctx.return_value = MagicMock()
        p = _make_pattern(
            engine,
            {
                "start_date": "2024-01-01",
                "end_date": "2024-01-03",
                "unknown_member": True,
            },
        )
        context = _make_context()
        result = p.execute(context)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4  # 3 dates + 1 unknown
        assert result.iloc[0]["date_sk"] == 0

    @patch("odibi.patterns.date_dimension.get_logging_context")
    def test_execute_with_fiscal_year_start(self, mock_ctx, engine):
        mock_ctx.return_value = MagicMock()
        p = _make_pattern(
            engine,
            {
                "start_date": "2024-07-01",
                "end_date": "2024-07-01",
                "fiscal_year_start_month": 7,
            },
        )
        context = _make_context()
        result = p.execute(context)
        assert result["fiscal_year"].iloc[0] == 2025

    @patch("odibi.patterns.date_dimension.get_logging_context")
    def test_execute_raises_on_error(self, mock_ctx, engine):
        mock_ctx.return_value = MagicMock()
        p = _make_pattern(
            engine,
            {
                "start_date": "invalid-date",
                "end_date": "2024-01-01",
            },
        )
        context = _make_context()
        with pytest.raises(Exception):
            p.execute(context)
