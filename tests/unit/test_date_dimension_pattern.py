"""Unit tests for DateDimensionPattern."""

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from odibi.config import NodeConfig
from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.patterns.date_dimension import DateDimensionPattern


def create_pandas_context(df=None, engine=None):
    """Helper to create a PandasContext with optional DataFrame."""
    pandas_context = PandasContext()
    ctx = EngineContext(
        context=pandas_context,
        df=df if df is not None else pd.DataFrame(),
        engine_type=EngineType.PANDAS,
        engine=engine,
    )
    return ctx


@pytest.fixture
def mock_engine():
    """Create a mock engine."""
    engine = MagicMock()
    engine.connections = {}
    return engine


@pytest.fixture
def mock_config():
    """Create a basic mock NodeConfig."""
    config = MagicMock(spec=NodeConfig)
    config.params = {}
    return config


class TestDateDimensionPatternValidation:
    """Test DateDimensionPattern validation."""

    def test_validate_requires_start_date(self, mock_engine, mock_config):
        """Test that start_date is required."""
        mock_config.params = {"end_date": "2024-12-31"}

        pattern = DateDimensionPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="start_date"):
            pattern.validate()

    def test_validate_requires_end_date(self, mock_engine, mock_config):
        """Test that end_date is required."""
        mock_config.params = {"start_date": "2024-01-01"}

        pattern = DateDimensionPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="end_date"):
            pattern.validate()

    def test_validate_start_before_end(self, mock_engine, mock_config):
        """Test that start_date must be before end_date."""
        mock_config.params = {
            "start_date": "2024-12-31",
            "end_date": "2024-01-01",
        }

        pattern = DateDimensionPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="start_date must be before"):
            pattern.validate()

    def test_validate_invalid_fiscal_month(self, mock_engine, mock_config):
        """Test that fiscal_year_start_month must be 1-12."""
        mock_config.params = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "fiscal_year_start_month": 13,
        }

        pattern = DateDimensionPattern(mock_engine, mock_config)

        with pytest.raises(ValueError, match="fiscal_year_start_month"):
            pattern.validate()

    def test_validate_success(self, mock_engine, mock_config):
        """Test validation passes with valid params."""
        mock_config.params = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "fiscal_year_start_month": 7,
        }

        pattern = DateDimensionPattern(mock_engine, mock_config)
        pattern.validate()


class TestDateDimensionPatternGeneration:
    """Test date dimension generation."""

    def test_generates_correct_number_of_rows(self, mock_engine, mock_config):
        """Test that correct number of dates are generated."""
        mock_config.params = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
        }

        context = create_pandas_context(engine=mock_engine)
        pattern = DateDimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 31

    def test_generates_all_columns(self, mock_engine, mock_config):
        """Test that all expected columns are generated."""
        mock_config.params = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-01",
        }

        context = create_pandas_context(engine=mock_engine)
        pattern = DateDimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        expected_columns = [
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
        assert list(result.columns) == expected_columns

    def test_date_sk_format(self, mock_engine, mock_config):
        """Test that date_sk is in YYYYMMDD format."""
        mock_config.params = {
            "start_date": "2024-03-15",
            "end_date": "2024-03-15",
        }

        context = create_pandas_context(engine=mock_engine)
        pattern = DateDimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert result.iloc[0]["date_sk"] == 20240315

    def test_day_of_week_calculation(self, mock_engine, mock_config):
        """Test day of week calculation."""
        mock_config.params = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-07",
        }

        context = create_pandas_context(engine=mock_engine)
        pattern = DateDimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        monday_row = result[result["full_date"] == date(2024, 1, 1)]
        assert monday_row.iloc[0]["day_of_week"] == "Monday"
        assert monday_row.iloc[0]["day_of_week_num"] == 1

        sunday_row = result[result["full_date"] == date(2024, 1, 7)]
        assert sunday_row.iloc[0]["day_of_week"] == "Sunday"
        assert sunday_row.iloc[0]["day_of_week_num"] == 7

    def test_weekend_flag(self, mock_engine, mock_config):
        """Test weekend detection."""
        mock_config.params = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-07",
        }

        context = create_pandas_context(engine=mock_engine)
        pattern = DateDimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        weekday = result[result["full_date"] == date(2024, 1, 1)]
        assert weekday.iloc[0]["is_weekend"] == False  # noqa: E712

        saturday = result[result["full_date"] == date(2024, 1, 6)]
        assert saturday.iloc[0]["is_weekend"] == True  # noqa: E712

        sunday = result[result["full_date"] == date(2024, 1, 7)]
        assert sunday.iloc[0]["is_weekend"] == True  # noqa: E712

    def test_quarter_calculation(self, mock_engine, mock_config):
        """Test quarter calculation."""
        mock_config.params = {
            "start_date": "2024-01-15",
            "end_date": "2024-10-15",
        }

        context = create_pandas_context(engine=mock_engine)
        pattern = DateDimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        jan = result[result["full_date"] == date(2024, 1, 15)]
        assert jan.iloc[0]["quarter"] == 1
        assert jan.iloc[0]["quarter_name"] == "Q1"

        apr = result[result["full_date"] == date(2024, 4, 15)]
        assert apr.iloc[0]["quarter"] == 2

        jul = result[result["full_date"] == date(2024, 7, 15)]
        assert jul.iloc[0]["quarter"] == 3

        oct = result[result["full_date"] == date(2024, 10, 15)]
        assert oct.iloc[0]["quarter"] == 4

    def test_month_boundaries(self, mock_engine, mock_config):
        """Test month start/end flags."""
        mock_config.params = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
        }

        context = create_pandas_context(engine=mock_engine)
        pattern = DateDimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        first = result[result["full_date"] == date(2024, 1, 1)]
        assert first.iloc[0]["is_month_start"] == True  # noqa: E712
        assert first.iloc[0]["is_month_end"] == False  # noqa: E712

        last = result[result["full_date"] == date(2024, 1, 31)]
        assert last.iloc[0]["is_month_start"] == False  # noqa: E712
        assert last.iloc[0]["is_month_end"] == True  # noqa: E712

        middle = result[result["full_date"] == date(2024, 1, 15)]
        assert middle.iloc[0]["is_month_start"] == False  # noqa: E712
        assert middle.iloc[0]["is_month_end"] == False  # noqa: E712

    def test_year_boundaries(self, mock_engine, mock_config):
        """Test year start/end flags."""
        mock_config.params = {
            "start_date": "2023-12-31",
            "end_date": "2024-01-01",
        }

        context = create_pandas_context(engine=mock_engine)
        pattern = DateDimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        dec31 = result[result["full_date"] == date(2023, 12, 31)]
        assert dec31.iloc[0]["is_year_end"] == True  # noqa: E712
        assert dec31.iloc[0]["is_year_start"] == False  # noqa: E712

        jan1 = result[result["full_date"] == date(2024, 1, 1)]
        assert jan1.iloc[0]["is_year_start"] == True  # noqa: E712
        assert jan1.iloc[0]["is_year_end"] == False  # noqa: E712


class TestDateDimensionPatternFiscalYear:
    """Test fiscal year calculations."""

    def test_fiscal_year_calendar_aligned(self, mock_engine, mock_config):
        """Test fiscal year when aligned with calendar year."""
        mock_config.params = {
            "start_date": "2024-06-15",
            "end_date": "2024-06-15",
            "fiscal_year_start_month": 1,
        }

        context = create_pandas_context(engine=mock_engine)
        pattern = DateDimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert result.iloc[0]["fiscal_year"] == 2024
        assert result.iloc[0]["fiscal_quarter"] == 2

    def test_fiscal_year_july_start(self, mock_engine, mock_config):
        """Test fiscal year starting in July."""
        mock_config.params = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "fiscal_year_start_month": 7,
        }

        context = create_pandas_context(engine=mock_engine)
        pattern = DateDimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        jan = result[result["full_date"] == date(2024, 1, 15)]
        assert jan.iloc[0]["fiscal_year"] == 2024
        assert jan.iloc[0]["fiscal_quarter"] == 3

        jul = result[result["full_date"] == date(2024, 7, 15)]
        assert jul.iloc[0]["fiscal_year"] == 2025
        assert jul.iloc[0]["fiscal_quarter"] == 1

        oct = result[result["full_date"] == date(2024, 10, 15)]
        assert oct.iloc[0]["fiscal_year"] == 2025
        assert oct.iloc[0]["fiscal_quarter"] == 2

    def test_fiscal_year_april_start(self, mock_engine, mock_config):
        """Test fiscal year starting in April (UK style)."""
        mock_config.params = {
            "start_date": "2024-03-15",
            "end_date": "2024-04-15",
            "fiscal_year_start_month": 4,
        }

        context = create_pandas_context(engine=mock_engine)
        pattern = DateDimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        march = result[result["full_date"] == date(2024, 3, 15)]
        assert march.iloc[0]["fiscal_year"] == 2024
        assert march.iloc[0]["fiscal_quarter"] == 4

        april = result[result["full_date"] == date(2024, 4, 15)]
        assert april.iloc[0]["fiscal_year"] == 2025
        assert april.iloc[0]["fiscal_quarter"] == 1


class TestDateDimensionPatternUnknownMember:
    """Test unknown member functionality."""

    def test_adds_unknown_member_row(self, mock_engine, mock_config):
        """Test that unknown member row is added with date_sk=0."""
        mock_config.params = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-03",
            "unknown_member": True,
        }

        context = create_pandas_context(engine=mock_engine)
        pattern = DateDimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 4

        unknown = result[result["date_sk"] == 0]
        assert len(unknown) == 1
        assert unknown.iloc[0]["full_date"] == date(1900, 1, 1)
        assert unknown.iloc[0]["day_of_week"] == "Unknown"
        assert unknown.iloc[0]["month_name"] == "Unknown"

    def test_unknown_member_is_first_row(self, mock_engine, mock_config):
        """Test that unknown member is the first row."""
        mock_config.params = {
            "start_date": "2024-01-01",
            "end_date": "2024-01-01",
            "unknown_member": True,
        }

        context = create_pandas_context(engine=mock_engine)
        pattern = DateDimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert result.iloc[0]["date_sk"] == 0


class TestDateDimensionPatternIntegration:
    """Integration tests for full date dimension workflows."""

    def test_full_year_generation(self, mock_engine, mock_config):
        """Test generating a full year of dates."""
        mock_config.params = {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "fiscal_year_start_month": 7,
            "unknown_member": True,
        }

        context = create_pandas_context(engine=mock_engine)
        pattern = DateDimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        assert len(result) == 367

        unique_date_sks = result["date_sk"].nunique()
        assert unique_date_sks == 367

        weekends = result[result["is_weekend"] == True]  # noqa: E712
        assert len(weekends) > 100

    def test_pattern_registered(self):
        """Test that DateDimensionPattern is registered in patterns registry."""
        from odibi.patterns import get_pattern_class

        pattern_class = get_pattern_class("date_dimension")
        assert pattern_class == DateDimensionPattern

    def test_multi_year_span(self, mock_engine, mock_config):
        """Test generating dates spanning multiple years."""
        mock_config.params = {
            "start_date": "2020-01-01",
            "end_date": "2025-12-31",
        }

        context = create_pandas_context(engine=mock_engine)
        pattern = DateDimensionPattern(mock_engine, mock_config)
        result = pattern.execute(context)

        years = result["year"].unique()
        assert set(years) == {2020, 2021, 2022, 2023, 2024, 2025}

        first_row = result[result["date_sk"] == 20200101]
        assert len(first_row) == 1

        last_row = result[result["date_sk"] == 20251231]
        assert len(last_row) == 1
