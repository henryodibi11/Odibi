import pandas as pd
import re
from odibi.registry import transform

# 192.168.1.1 - - [20/Nov/2025:19:27:50 +0000] "GET /index.html HTTP/1.1" 200 123 "-" "Mozilla/5.0"
LOG_PATTERN = re.compile(
    r'(?P<ip>[\d\.]+) - - \[(?P<timestamp>.*?)\] "(?P<method>\w+) (?P<url>.*?) HTTP/1.1" (?P<status>\d+) (?P<size>\d+)'
)


@transform
def parse_logs(context, current):
    """Parse access.log lines."""
    df = current

    # Assuming read with header=None, columns are integers
    if 0 in df.columns:
        series = df[0]
    else:
        # If header was read, the first col name is the line
        series = df[df.columns[0]]

    def parse_line(line):
        m = LOG_PATTERN.match(str(line))
        if m:
            return m.groupdict()
        return {}

    parsed = series.apply(parse_line).apply(pd.Series)
    return parsed.dropna()
