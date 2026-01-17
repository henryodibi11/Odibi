import pytest

from odibi.utils.progress import PipelineProgress, NodeStatus


# Fixture for plain text environment (Rich not available, not in notebook)
@pytest.fixture
def pipeline_progress_plain(monkeypatch):
    # Force plain mode by overriding the functions imported in progress.py
    monkeypatch.setattr("odibi.utils.progress.is_rich_available", lambda: False)
    monkeypatch.setattr("odibi.utils.progress._is_notebook_environment", lambda: False)
    return PipelineProgress("test_pipeline", ["node1", "node2"], engine="pandas")


# Fixture for rich environment simulation (Rich available, non-notebook)
@pytest.fixture
def pipeline_progress_rich(monkeypatch):
    monkeypatch.setattr("odibi.utils.progress.is_rich_available", lambda: True)
    monkeypatch.setattr("odibi.utils.progress._is_notebook_environment", lambda: False)
    return PipelineProgress("test_pipeline", ["node1", "node2"], engine="spark")


def test_start_plain_prints_header(pipeline_progress_plain, capsys):
    # Test that start() in plain mode prints the header with pipeline details
    pipeline_progress_plain.start()
    captured = capsys.readouterr().out
    assert "Pipeline: test_pipeline" in captured
    assert "Engine: pandas" in captured
    assert "Nodes: 2" in captured


def test_start_rich_calls_rich_start(monkeypatch, pipeline_progress_rich):
    # Patch the _start_rich method to verify it gets called
    called = False

    def dummy_start_rich():
        nonlocal called
        called = True

    monkeypatch.setattr(pipeline_progress_rich, "_start_rich", dummy_start_rich)
    pipeline_progress_rich.start()
    assert called is True


def test_update_node_invalid_node(pipeline_progress_plain, capsys):
    # Updating a node that does not exist should not change state or print update
    original_status = pipeline_progress_plain._node_statuses.copy()
    pipeline_progress_plain.update_node("nonexistent", NodeStatus.SUCCESS, duration=2.0, rows=100)
    # The node statuses dictionary should remain unchanged for nonexistent nodes.
    assert pipeline_progress_plain._node_statuses == original_status


def test_update_node_valid(pipeline_progress_plain):
    # Update a valid node and check that its internal status is updated.
    pipeline_progress_plain.update_node("node1", NodeStatus.SUCCESS, duration=2.5, rows=500)
    node_info = pipeline_progress_plain._node_statuses.get("node1")
    assert node_info is not None
    assert node_info["status"] == NodeStatus.SUCCESS
    assert node_info["duration"] == 2.5
    assert node_info["rows"] == 500


def test_get_phase_timing_summary(pipeline_progress_plain):
    # Update one node with phase timings and verify the summary.
    phase_timings = {"phase1": 120.0, "phase2": 80.0}
    pipeline_progress_plain.update_node(
        "node1", NodeStatus.SUCCESS, duration=1.0, rows=100, phase_timings=phase_timings
    )
    summary = pipeline_progress_plain.get_phase_timing_summary()
    assert "node1" in summary
    assert summary["node1"] == phase_timings
    # Node2 has no phase timings
    assert "node2" not in summary


def test_get_aggregate_phase_timings(pipeline_progress_plain):
    # Update two nodes with overlapping phase timings and check aggregate maximum is computed.
    pipeline_progress_plain.update_node(
        "node1",
        NodeStatus.SUCCESS,
        duration=1.0,
        rows=100,
        phase_timings={"phase1": 100, "phase2": 200},
    )
    pipeline_progress_plain.update_node(
        "node2",
        NodeStatus.SUCCESS,
        duration=1.5,
        rows=150,
        phase_timings={"phase1": 150, "phase3": 50},
    )
    aggregate = pipeline_progress_plain.get_aggregate_phase_timings()
    # For phase1, maximum should be 150; phase2 is 200; phase3 is 50.
    assert aggregate.get("phase1") == 150.00
    assert aggregate.get("phase2") == 200.00
    assert aggregate.get("phase3") == 50.00


def test_finish_calls_plain_finish(monkeypatch, pipeline_progress_plain):
    # Patch _finish_plain to check that finish() calls it in plain mode.
    called = False

    def dummy_finish(completed, failed, skipped, duration):
        nonlocal called
        called = True

    monkeypatch.setattr(pipeline_progress_plain, "_finish_plain", dummy_finish)
    # Ensure _live is None so that plain finish is used.
    pipeline_progress_plain._live = None
    pipeline_progress_plain.finish(completed=1, failed=0, skipped=0)
    assert called is True


def test_print_phase_timing_report_plain(monkeypatch, pipeline_progress_plain, capsys):
    # Test the plain phase timing report.
    # Update node with phase timings.
    pipeline_progress_plain.update_node(
        "node1",
        NodeStatus.SUCCESS,
        duration=1.0,
        rows=100,
        phase_timings={"phase1": 100, "phase2": 50},
    )
    # Patch _print_phase_timing_plain to capture call.
    printed = []

    def dummy_print_phase_timing_plain(aggregate, total_ms):
        printed.append((aggregate, total_ms))

    monkeypatch.setattr(
        pipeline_progress_plain, "_print_phase_timing_plain", dummy_print_phase_timing_plain
    )
    pipeline_progress_plain.print_phase_timing_report(pipeline_duration_s=1.0)
    assert len(printed) == 1
    aggregate, total_ms = printed[0]
    # Total milliseconds should be 1000 (1.0 sec * 1000) as provided.
    assert total_ms == 1000
    # The aggregate should contain the phase timings from node1.
    assert aggregate.get("phase1") == 100
    assert aggregate.get("phase2") == 50


def test_print_phase_timing_report_no_aggregate(pipeline_progress_plain, capsys):
    # If no nodes have phase timings, the report should do nothing.
    # Capture output from print_phase_timing_report; since _print_phase_timing_plain isn't called,
    # nothing should be printed.
    capsys.readouterr()  # clear buffer
    pipeline_progress_plain.print_phase_timing_report()
    final_out = capsys.readouterr().out
    # Since no phase timings exist, there should be no change.
    assert final_out == ""
