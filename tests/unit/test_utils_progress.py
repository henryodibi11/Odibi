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


@pytest.fixture
def pipeline_progress_plain_with_layers(monkeypatch):
    monkeypatch.setattr("odibi.utils.progress.is_rich_available", lambda: False)
    monkeypatch.setattr("odibi.utils.progress._is_notebook_environment", lambda: False)
    return PipelineProgress(
        "layered_pipeline",
        ["node1", "node2", "node3"],
        engine="pandas",
        layers=[["node1"], ["node2", "node3"]],
    )


class TestFormatDuration:
    def test_none(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_duration(None) == "-"

    def test_sub_second(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_duration(0.5) == "500ms"

    def test_seconds(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_duration(2.345) == "2.35s"

    def test_exactly_one(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_duration(1.0) == "1.00s"


class TestFormatRows:
    def test_none(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_rows(None) == "-"

    def test_small(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_rows(50) == "50"

    def test_thousands(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_rows(5000) == "5.0K"

    def test_millions(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_rows(2000000) == "2.0M"


class TestFormatStatusPlain:
    def test_pending(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_status_plain(NodeStatus.PENDING) == "○ pending"

    def test_running(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_status_plain(NodeStatus.RUNNING) == "◉ running"

    def test_success(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_status_plain(NodeStatus.SUCCESS) == "✓ success"

    def test_failed(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_status_plain(NodeStatus.FAILED) == "✗ failed"

    def test_skipped(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_status_plain(NodeStatus.SKIPPED) == "⏭ skipped"

    def test_unknown(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_status_plain("unknown") == "unknown"


class TestFormatStatus:
    def test_pending(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_status(NodeStatus.PENDING) == "[dim]○ pending[/dim]"

    def test_running(self, pipeline_progress_plain):
        assert (
            pipeline_progress_plain._format_status(NodeStatus.RUNNING)
            == "[yellow]◉ running[/yellow]"
        )

    def test_success(self, pipeline_progress_plain):
        assert (
            pipeline_progress_plain._format_status(NodeStatus.SUCCESS) == "[green]✓ success[/green]"
        )

    def test_failed(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_status(NodeStatus.FAILED) == "[red]✗ failed[/red]"

    def test_skipped(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_status(NodeStatus.SKIPPED) == "[dim]⏭ skipped[/dim]"

    def test_unknown(self, pipeline_progress_plain):
        assert pipeline_progress_plain._format_status("unknown") == "unknown"


class TestLayersInit:
    def test_node_to_layer_mapping(self, pipeline_progress_plain_with_layers):
        pp = pipeline_progress_plain_with_layers
        assert pp._node_to_layer == {"node1": 0, "node2": 1, "node3": 1}

    def test_layers_stored(self, pipeline_progress_plain_with_layers):
        pp = pipeline_progress_plain_with_layers
        assert pp.layers == [["node1"], ["node2", "node3"]]


class TestUpdatePlainWithLayers:
    def test_wave_header_prints(self, pipeline_progress_plain_with_layers, capsys):
        pp = pipeline_progress_plain_with_layers
        pp.start()
        capsys.readouterr()

        pp.update_node("node1", NodeStatus.SUCCESS, duration=1.0, rows=100)
        out1 = capsys.readouterr().out
        assert "Wave 1" in out1
        assert "(parallel)" not in out1

        pp.update_node("node2", NodeStatus.SUCCESS, duration=0.5, rows=200)
        out2 = capsys.readouterr().out
        assert "Wave 2" in out2
        assert "(parallel)" in out2

        pp.update_node("node3", NodeStatus.RUNNING, duration=0.3, rows=50)
        out3 = capsys.readouterr().out
        assert "Wave" not in out3


class TestFinishPlain:
    def test_success_output(self, pipeline_progress_plain, capsys):
        pipeline_progress_plain.start()
        capsys.readouterr()
        pipeline_progress_plain._finish_plain(completed=2, failed=0, skipped=0, duration=3.14)
        out = capsys.readouterr().out
        assert "SUCCESS" in out
        assert "3.14s" in out
        assert "Completed: 2" in out
        assert "Failed: 0" in out

    def test_failure_output(self, pipeline_progress_plain, capsys):
        pipeline_progress_plain.start()
        capsys.readouterr()
        pipeline_progress_plain._finish_plain(completed=1, failed=1, skipped=0, duration=2.0)
        out = capsys.readouterr().out
        assert "FAILED" in out


class TestPrintPhaseTimingPlain:
    def test_full_output(self, pipeline_progress_plain, capsys):
        aggregate = {"load": 1500.0, "transform": 800.0, "validate": 200.0}
        total_ms = 2500.0
        pipeline_progress_plain._print_phase_timing_plain(aggregate, total_ms)
        out = capsys.readouterr().out
        assert "Phase Bottlenecks" in out
        assert "load: 1.50s (60.0% of pipeline)" in out
        assert "transform: 800ms (32.0% of pipeline)" in out
        assert "validate: 200ms (8.0% of pipeline)" in out

    def test_zero_total(self, pipeline_progress_plain, capsys):
        aggregate = {"load": 0.0}
        pipeline_progress_plain._print_phase_timing_plain(aggregate, 0.0)
        out = capsys.readouterr().out
        assert "0.0%" in out


class TestPrintPhaseTimingReportFallback:
    def test_fallback_to_sum(self, monkeypatch, capsys):
        monkeypatch.setattr("odibi.utils.progress.is_rich_available", lambda: False)
        monkeypatch.setattr("odibi.utils.progress._is_notebook_environment", lambda: False)
        pp = PipelineProgress("test", ["n1"], engine="pandas")
        pp.update_node(
            "n1", NodeStatus.SUCCESS, duration=1.0, rows=10, phase_timings={"a": 300.0, "b": 200.0}
        )
        pp.print_phase_timing_report()
        out = capsys.readouterr().out
        assert "a: 300ms (60.0% of pipeline)" in out
        assert "b: 200ms (40.0% of pipeline)" in out


class TestFinishStopsLive:
    def test_live_stopped(self, monkeypatch, pipeline_progress_rich):
        stopped = []

        class FakeLive:
            def stop(self):
                stopped.append(True)

        pipeline_progress_rich._live = FakeLive()
        monkeypatch.setattr(pipeline_progress_rich, "_finish_rich", lambda *a: None)
        pipeline_progress_rich.finish(completed=2, failed=0, skipped=0)
        assert len(stopped) == 1
        assert pipeline_progress_rich._live is None
