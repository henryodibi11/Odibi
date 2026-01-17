"""Unit tests for odibi/utils/duration.py."""

from datetime import timedelta


from odibi.utils.duration import parse_duration


class TestParseDuration:
    """Tests for parse_duration function."""

    def test_parse_duration_hours(self):
        """Test parsing hours."""
        result = parse_duration("2h")
        assert result == timedelta(hours=2)

    def test_parse_duration_minutes(self):
        """Test parsing minutes."""
        result = parse_duration("30m")
        assert result == timedelta(minutes=30)

    def test_parse_duration_days(self):
        """Test parsing days."""
        result = parse_duration("1d")
        assert result == timedelta(days=1)

    def test_parse_duration_seconds(self):
        """Test parsing seconds."""
        result = parse_duration("45s")
        assert result == timedelta(seconds=45)

    def test_parse_duration_weeks(self):
        """Test parsing weeks."""
        result = parse_duration("2w")
        assert result == timedelta(weeks=2)

    def test_parse_duration_empty_string_returns_none(self):
        """Test empty string returns None."""
        assert parse_duration("") is None

    def test_parse_duration_none_returns_none(self):
        """Test None input returns None."""
        assert parse_duration(None) is None

    def test_parse_duration_invalid_suffix_returns_none(self):
        """Test invalid suffix returns None."""
        assert parse_duration("10x") is None

    def test_parse_duration_invalid_number_returns_none(self):
        """Test invalid number returns None."""
        assert parse_duration("abch") is None

    def test_parse_duration_case_insensitive(self):
        """Test parsing is case insensitive."""
        assert parse_duration("2H") == timedelta(hours=2)
        assert parse_duration("30M") == timedelta(minutes=30)

    def test_parse_duration_with_whitespace(self):
        """Test parsing with leading/trailing whitespace."""
        assert parse_duration("  2h  ") == timedelta(hours=2)
