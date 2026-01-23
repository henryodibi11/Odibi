from odibi_mcp.contracts.stats import StatPoint, NodeStatsResponse
from datetime import datetime
from odibi_mcp.contracts.time import TimeWindow


def test_stat_point():
    pt = StatPoint(timestamp=datetime(2023, 1, 1, 1, 1), value=5.0)
    dump = pt.model_dump()
    assert dump["timestamp"] == datetime(2023, 1, 1, 1, 1)
    assert dump["value"] == 5.0


def test_node_stats_response():
    window = TimeWindow(days=7)
    pt = StatPoint(timestamp=datetime(2023, 1, 1, 2, 2), value=6.66)
    resp = NodeStatsResponse(
        pipeline="main",
        node="my_node",
        window=window,
        run_count=21,
        success_count=20,
        failure_count=1,
        failure_rate=0.05,
        avg_duration_seconds=16.0,
        duration_trend=[pt],
        row_count_trend=[pt],
    )
    dump = resp.model_dump()
    assert dump["run_count"] == 21
    assert dump["window"]["days"] == 7
    assert dump["pipeline"] == "main"
