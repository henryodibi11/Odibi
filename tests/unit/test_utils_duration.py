import pytest
from odibi.utils.duration import parse_duration

# Try to import format_duration; if unavailable, skip its tests.
try:
    from odibi.utils.duration import format_duration

    skip_format_tests = False
except ImportError:
    skip_format_tests = True

# Tests for parse_duration


def test_parse_duration_valid():
    """
    test_parse_duration_valid:
    Given a valid duration string in the format "HH:MM:SS", parse_duration should return the correct total seconds.
    Example: "01:02:03" -> 3723 seconds.
    """
    assert parse_duration("01:02:03") == 3723


def test_parse_duration_invalid_format():
    """
    test_parse_duration_invalid_format:
    If the duration string does not follow the "HH:MM:SS" format, parse_duration should raise a ValueError.
    """
    with pytest.raises(ValueError) as exc_info:
        parse_duration("1:02")  # Missing one component
    assert "Invalid duration format" in str(exc_info.value)


def test_parse_duration_invalid_values():
    """
    test_parse_duration_invalid_values:
    If the duration string contains non-numeric values, parse_duration should raise a ValueError.
    """
    with pytest.raises(ValueError) as exc_info:
        parse_duration("01:xx:03")
    assert "Invalid duration values" in str(exc_info.value)


def test_parse_duration_none():
    """
    test_parse_duration_none:
    If None is passed as the duration string, parse_duration should raise a ValueError.
    """
    with pytest.raises(ValueError) as exc_info:
        parse_duration(None)
    assert "cannot be None" in str(exc_info.value)


# Tests for format_duration (skipped if not implemented)


@pytest.mark.skipif(skip_format_tests, reason="format_duration not implemented")
def test_format_duration_valid():
    """
    test_format_duration_valid:
    Given a valid duration in seconds, format_duration should return a string in "HH:MM:SS" format.
    Example: 3723 seconds -> "01:02:03".
    """
    assert format_duration(3723) == "01:02:03"


@pytest.mark.skipif(skip_format_tests, reason="format_duration not implemented")
def test_format_duration_zero():
    """
    test_format_duration_zero:
    Formatting a duration of zero seconds should return "00:00:00".
    """
    assert format_duration(0) == "00:00:00"


@pytest.mark.skipif(skip_format_tests, reason="format_duration not implemented")
def test_format_duration_negative():
    """
    test_format_duration_negative:
    If a negative duration is provided, format_duration should raise a ValueError.
    """
    with pytest.raises(ValueError) as exc_info:
        format_duration(-10)
    assert "negative" in str(exc_info.value)


@pytest.mark.skipif(skip_format_tests, reason="format_duration not implemented")
def test_format_duration_none():
    """
    test_format_duration_none:
    If None is passed as the duration, format_duration should raise a ValueError.
    """
    with pytest.raises(ValueError) as exc_info:
        format_duration(None)
    assert "cannot be None" in str(exc_info.value)
