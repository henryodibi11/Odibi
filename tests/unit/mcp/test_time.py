from odibi_mcp.contracts.time import TimeWindow
from datetime import datetime


def test_time_window_python_objects():
    window = TimeWindow(start="2023-01-01T00:00:00", end="2023-01-02T00:00:00")
    dump = window.model_dump()
    assert dump["start"] == datetime(2023, 1, 1, 0, 0)
    assert dump["end"] == datetime(2023, 1, 2, 0, 0)


def test_time_window_json_serialization():
    window = TimeWindow(start="2023-01-01T00:00:00", end="2023-01-02T00:00:00")
    dump = window.model_dump(mode="json")
    assert dump["start"] == "2023-01-01T00:00:00"
    assert dump["end"] == "2023-01-02T00:00:00"
