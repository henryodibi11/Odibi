"""Tests for date variable substitution in config_loader."""

import re
from datetime import datetime, timedelta

import pytest

from odibi.utils.config_loader import (
    DATE_PATTERN,
    _resolve_date_expression,
    _substitute_dates,
)


class TestDatePattern:
    """Tests for the DATE_PATTERN regex."""

    def test_matches_simple_expression(self):
        match = DATE_PATTERN.search("${date:today}")
        assert match is not None
        assert match.group(1) == "today"
        assert match.group(2) is None

    def test_matches_expression_with_format(self):
        match = DATE_PATTERN.search("${date:today:%Y%m%d}")
        assert match is not None
        assert match.group(1) == "today"
        assert match.group(2) == "%Y%m%d"

    def test_matches_relative_expression(self):
        match = DATE_PATTERN.search("${date:-7d}")
        assert match is not None
        assert match.group(1) == "-7d"

    def test_matches_relative_with_format(self):
        match = DATE_PATTERN.search("${date:-30d:%Y-%m-%d}")
        assert match is not None
        assert match.group(1) == "-30d"
        assert match.group(2) == "%Y-%m-%d"


class TestResolveDateExpression:
    """Tests for _resolve_date_expression function."""

    def test_today(self):
        result = _resolve_date_expression("today")
        expected = datetime.now().strftime("%Y-%m-%d")
        assert result == expected

    def test_yesterday(self):
        result = _resolve_date_expression("yesterday")
        expected = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        assert result == expected

    def test_now_includes_time(self):
        result = _resolve_date_expression("now")
        # Should include time component
        assert re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", result)

    def test_start_of_month(self):
        result = _resolve_date_expression("start_of_month")
        now = datetime.now()
        expected = now.replace(day=1).strftime("%Y-%m-%d")
        assert result == expected

    def test_end_of_month(self):
        result = _resolve_date_expression("end_of_month")
        # Should be a valid date at end of current month
        parsed = datetime.strptime(result, "%Y-%m-%d")
        # Next day should be day 1 of next month
        next_day = parsed + timedelta(days=1)
        assert next_day.day == 1

    def test_start_of_year(self):
        result = _resolve_date_expression("start_of_year")
        now = datetime.now()
        expected = now.replace(month=1, day=1).strftime("%Y-%m-%d")
        assert result == expected

    def test_relative_days_negative(self):
        result = _resolve_date_expression("-7d")
        expected = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        assert result == expected

    def test_relative_days_positive(self):
        result = _resolve_date_expression("+30d")
        expected = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        assert result == expected

    def test_relative_weeks(self):
        result = _resolve_date_expression("-2w")
        expected = (datetime.now() - timedelta(weeks=2)).strftime("%Y-%m-%d")
        assert result == expected

    def test_relative_months(self):
        result = _resolve_date_expression("-1m")
        # Just check it's a valid date roughly 1 month ago
        parsed = datetime.strptime(result, "%Y-%m-%d")
        now = datetime.now()
        diff = now - parsed
        assert 28 <= diff.days <= 31

    def test_relative_years(self):
        result = _resolve_date_expression("-1y")
        parsed = datetime.strptime(result, "%Y-%m-%d")
        now = datetime.now()
        assert parsed.year == now.year - 1

    def test_custom_format(self):
        result = _resolve_date_expression("today", "%Y%m%d")
        expected = datetime.now().strftime("%Y%m%d")
        assert result == expected

    def test_custom_format_with_slashes(self):
        result = _resolve_date_expression("today", "%m/%d/%Y")
        expected = datetime.now().strftime("%m/%d/%Y")
        assert result == expected

    def test_unknown_expression_returns_as_is(self):
        result = _resolve_date_expression("unknown_expr")
        assert result == "unknown_expr"


class TestSubstituteDates:
    """Tests for _substitute_dates function."""

    def test_substitutes_in_string(self):
        data = "Report for ${date:today}"
        result = _substitute_dates(data)
        expected = f"Report for {datetime.now().strftime('%Y-%m-%d')}"
        assert result == expected

    def test_substitutes_in_dict(self):
        data = {"date": "${date:today}"}
        result = _substitute_dates(data)
        assert result["date"] == datetime.now().strftime("%Y-%m-%d")

    def test_substitutes_in_nested_dict(self):
        data = {"params": {"start": "${date:-7d}", "end": "${date:today}"}}
        result = _substitute_dates(data)
        assert result["params"]["start"] == (datetime.now() - timedelta(days=7)).strftime(
            "%Y-%m-%d"
        )
        assert result["params"]["end"] == datetime.now().strftime("%Y-%m-%d")

    def test_substitutes_in_list(self):
        data = ["${date:today}", "${date:yesterday}"]
        result = _substitute_dates(data)
        assert result[0] == datetime.now().strftime("%Y-%m-%d")
        assert result[1] == (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    def test_substitutes_with_format(self):
        data = {"compact_date": "${date:today:%Y%m%d}"}
        result = _substitute_dates(data)
        assert result["compact_date"] == datetime.now().strftime("%Y%m%d")

    def test_multiple_substitutions_in_string(self):
        data = "From ${date:-7d} to ${date:today}"
        result = _substitute_dates(data)
        expected = (
            f"From {(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')} "
            f"to {datetime.now().strftime('%Y-%m-%d')}"
        )
        assert result == expected

    def test_preserves_non_date_content(self):
        data = {"name": "test", "count": 42, "flag": True}
        result = _substitute_dates(data)
        assert result == data

    def test_preserves_other_variables(self):
        data = {"env": "${MY_VAR}", "date": "${date:today}"}
        result = _substitute_dates(data)
        assert result["env"] == "${MY_VAR}"  # Should not be touched
        assert result["date"] == datetime.now().strftime("%Y-%m-%d")
