"""Duration parsing utilities."""

from datetime import timedelta
from typing import Optional


def parse_duration(duration_str: str) -> Optional[timedelta]:
    """Parse a duration string like '2h', '30m', '1d' into a timedelta.

    Args:
        duration_str: Duration string with suffix (h=hours, m=minutes, d=days, s=seconds)

    Returns:
        timedelta object or None if parsing fails

    Examples:
        >>> parse_duration("2h")
        datetime.timedelta(seconds=7200)
        >>> parse_duration("30m")
        datetime.timedelta(seconds=1800)
        >>> parse_duration("1d")
        datetime.timedelta(days=1)
    """
    if not duration_str:
        return None

    duration_str = duration_str.strip().lower()

    try:
        if duration_str.endswith("h"):
            return timedelta(hours=int(duration_str[:-1]))
        elif duration_str.endswith("d"):
            return timedelta(days=int(duration_str[:-1]))
        elif duration_str.endswith("m"):
            return timedelta(minutes=int(duration_str[:-1]))
        elif duration_str.endswith("s"):
            return timedelta(seconds=int(duration_str[:-1]))
        elif duration_str.endswith("w"):
            return timedelta(weeks=int(duration_str[:-1]))
        else:
            return None
    except ValueError:
        return None
