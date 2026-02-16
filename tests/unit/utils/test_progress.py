"""Unit tests for odibi/utils/progress.py."""

from unittest.mock import MagicMock, patch

from odibi.utils.progress import NodeStatus, PipelineProgress


# ---------------------------------------------------------------------------
# NodeStatus constants
# ---------------------------------------------------------------------------


class TestNodeStatus:
    """Tests for NodeStatus constants."""

    def test_pending(self):
        assert NodeStatus.PENDING == "pending"

    def test_running(self):
        assert NodeStatus.RUNNING == "running"

    def test_success(self):
        assert NodeStatus.SUCCESS == "success"

    def test_failed(self):
        assert NodeStatus.FAILED == "failed"

    def test_skipped(self):
        assert NodeStatus.SKIPPED == "skipped"


# ---------------------------------------------------------------------------
# PipelineProgress.__init__
# ---------------------------------------------------------------------------


class TestPipelineProgressInit:
    """Tests for PipelineProgress initialisation."""

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_init_without_layers(self, _nb, _rich):
        p = PipelineProgress("pipe", ["a", "b"])
        assert p.pipeline_name == "pipe"
        assert p.node_names == ["a", "b"]
        assert p.engine == "pandas"
        assert p.layers is None
        assert p._node_to_layer == {}
        assert set(p._node_statuses.keys()) == {"a", "b"}
        for info in p._node_statuses.values():
            assert info["status"] == NodeStatus.PENDING

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_init_with_layers(self, _nb, _rich):
        layers = [["a", "b"], ["c"]]
        p = PipelineProgress("pipe", ["a", "b", "c"], layers=layers)
        assert p._node_to_layer == {"a": 0, "b": 0, "c": 1}

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_init_engine_override(self, _nb, _rich):
        p = PipelineProgress("pipe", ["a"], engine="spark")
        assert p.engine == "spark"


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


class TestFormatStatus:
    """Tests for _format_status (Rich markup)."""

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_all_statuses(self, _nb, _rich):
        p = PipelineProgress("p", ["n"])
        assert "pending" in p._format_status(NodeStatus.PENDING)
        assert "running" in p._format_status(NodeStatus.RUNNING)
        assert "success" in p._format_status(NodeStatus.SUCCESS)
        assert "failed" in p._format_status(NodeStatus.FAILED)
        assert "skipped" in p._format_status(NodeStatus.SKIPPED)

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_unknown_status(self, _nb, _rich):
        p = PipelineProgress("p", ["n"])
        assert p._format_status("exploded") == "exploded"


class TestFormatStatusPlain:
    """Tests for _format_status_plain."""

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_all_statuses(self, _nb, _rich):
        p = PipelineProgress("p", ["n"])
        assert p._format_status_plain(NodeStatus.PENDING) == "○ pending"
        assert p._format_status_plain(NodeStatus.RUNNING) == "◉ running"
        assert p._format_status_plain(NodeStatus.SUCCESS) == "✓ success"
        assert p._format_status_plain(NodeStatus.FAILED) == "✗ failed"
        assert p._format_status_plain(NodeStatus.SKIPPED) == "⏭ skipped"

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_unknown_status(self, _nb, _rich):
        p = PipelineProgress("p", ["n"])
        assert p._format_status_plain("boom") == "boom"


class TestFormatDuration:
    """Tests for _format_duration."""

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_none(self, _nb, _rich):
        p = PipelineProgress("p", ["n"])
        assert p._format_duration(None) == "-"

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_sub_second(self, _nb, _rich):
        p = PipelineProgress("p", ["n"])
        assert p._format_duration(0.123) == "123ms"

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_seconds(self, _nb, _rich):
        p = PipelineProgress("p", ["n"])
        assert p._format_duration(2.5) == "2.50s"


class TestFormatRows:
    """Tests for _format_rows."""

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_none(self, _nb, _rich):
        p = PipelineProgress("p", ["n"])
        assert p._format_rows(None) == "-"

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_small(self, _nb, _rich):
        p = PipelineProgress("p", ["n"])
        assert p._format_rows(42) == "42"

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_thousands(self, _nb, _rich):
        p = PipelineProgress("p", ["n"])
        assert p._format_rows(5_500) == "5.5K"

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_millions(self, _nb, _rich):
        p = PipelineProgress("p", ["n"])
        assert p._format_rows(2_300_000) == "2.3M"


# ---------------------------------------------------------------------------
# start / _start_plain
# ---------------------------------------------------------------------------


class TestStartPlain:
    """Tests for start() in plain-text mode."""

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_start_plain_output(self, _nb, _rich, capsys):
        p = PipelineProgress("test_pipe", ["a", "b", "c"])
        p.start()
        out = capsys.readouterr().out
        assert "Pipeline: test_pipe" in out
        assert "Engine: pandas" in out
        assert "Nodes: 3" in out
        assert "=" * 60 in out


# ---------------------------------------------------------------------------
# update_node
# ---------------------------------------------------------------------------


class TestUpdateNode:
    """Tests for update_node."""

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_unknown_node_ignored(self, _nb, _rich, capsys):
        p = PipelineProgress("p", ["a"])
        p.update_node("ghost", NodeStatus.SUCCESS)
        out = capsys.readouterr().out
        assert out == ""
        assert "ghost" not in p._node_statuses

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_update_stores_values(self, _nb, _rich, capsys):
        p = PipelineProgress("p", ["a"])
        p.update_node("a", NodeStatus.SUCCESS, duration=1.0, rows=100)
        info = p._node_statuses["a"]
        assert info["status"] == NodeStatus.SUCCESS
        assert info["duration"] == 1.0
        assert info["rows"] == 100

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_update_stores_phase_timings(self, _nb, _rich, capsys):
        p = PipelineProgress("p", ["a"])
        timings = {"read": 100.0, "transform": 200.0}
        p.update_node("a", NodeStatus.SUCCESS, phase_timings=timings)
        assert p._node_statuses["a"]["phase_timings"] == timings


# ---------------------------------------------------------------------------
# _update_plain
# ---------------------------------------------------------------------------


class TestUpdatePlain:
    """Tests for _update_plain output."""

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_update_plain_no_layers(self, _nb, _rich, capsys):
        p = PipelineProgress("p", ["a"])
        p.update_node("a", NodeStatus.SUCCESS, duration=2.0, rows=500)
        out = capsys.readouterr().out
        assert "a:" in out
        assert "✓ success" in out
        assert "2.00s" in out
        assert "500 rows" in out

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_update_plain_with_layers(self, _nb, _rich, capsys):
        layers = [["a", "b"], ["c"]]
        p = PipelineProgress("p", ["a", "b", "c"], layers=layers)
        p.update_node("a", NodeStatus.SUCCESS, duration=0.1)
        p.update_node("b", NodeStatus.SUCCESS, duration=0.2)
        p.update_node("c", NodeStatus.SUCCESS, duration=0.3)
        out = capsys.readouterr().out
        assert "Wave 1 (parallel):" in out
        assert "Wave 2:" in out

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_wave_header_printed_once_per_layer(self, _nb, _rich, capsys):
        layers = [["a", "b"]]
        p = PipelineProgress("p", ["a", "b"], layers=layers)
        p.update_node("a", NodeStatus.SUCCESS)
        p.update_node("b", NodeStatus.SUCCESS)
        out = capsys.readouterr().out
        assert out.count("Wave 1") == 1


# ---------------------------------------------------------------------------
# finish / _finish_plain
# ---------------------------------------------------------------------------


class TestFinishPlain:
    """Tests for finish() in plain-text mode."""

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_finish_success(self, _nb, _rich, capsys):
        p = PipelineProgress("p", ["a"])
        p.finish(completed=1, failed=0, skipped=0, duration=1.23)
        out = capsys.readouterr().out
        assert "SUCCESS" in out
        assert "1.23s" in out
        assert "Completed: 1" in out

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_finish_failure(self, _nb, _rich, capsys):
        p = PipelineProgress("p", ["a"])
        p.finish(completed=0, failed=1, skipped=0, duration=0.5)
        out = capsys.readouterr().out
        assert "FAILED" in out

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_finish_uses_elapsed_when_no_duration(self, _nb, _rich, capsys):
        p = PipelineProgress("p", ["a"])
        p._start_time = 1000.0
        with patch("time.time", return_value=1002.5):
            p.finish(completed=1)
        out = capsys.readouterr().out
        assert "2.50s" in out

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_finish_stops_live(self, _nb, _rich):
        p = PipelineProgress("p", ["a"])
        mock_live = MagicMock()
        p._live = mock_live
        p.finish(completed=1, duration=1.0)
        mock_live.stop.assert_called_once()
        assert p._live is None


# ---------------------------------------------------------------------------
# Phase timing helpers
# ---------------------------------------------------------------------------


class TestPhaseTimingSummary:
    """Tests for get_phase_timing_summary."""

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_returns_nodes_with_timings(self, _nb, _rich):
        p = PipelineProgress("p", ["a", "b"])
        p._node_statuses["a"]["phase_timings"] = {"read": 50.0}
        result = p.get_phase_timing_summary()
        assert "a" in result
        assert result["a"] == {"read": 50.0}
        assert "b" not in result

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_empty_when_no_timings(self, _nb, _rich):
        p = PipelineProgress("p", ["a"])
        assert p.get_phase_timing_summary() == {}


class TestAggregatePhaseTimings:
    """Tests for get_aggregate_phase_timings."""

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_max_across_nodes(self, _nb, _rich):
        p = PipelineProgress("p", ["a", "b"])
        p._node_statuses["a"]["phase_timings"] = {"read": 50.0, "write": 30.0}
        p._node_statuses["b"]["phase_timings"] = {"read": 80.0, "write": 10.0}
        result = p.get_aggregate_phase_timings()
        assert result == {"read": 80.0, "write": 30.0}

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_empty_when_no_timings(self, _nb, _rich):
        p = PipelineProgress("p", ["a"])
        assert p.get_aggregate_phase_timings() == {}

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_rounds_values(self, _nb, _rich):
        p = PipelineProgress("p", ["a"])
        p._node_statuses["a"]["phase_timings"] = {"read": 50.556}
        result = p.get_aggregate_phase_timings()
        assert result == {"read": 50.56}


# ---------------------------------------------------------------------------
# print_phase_timing_report / _print_phase_timing_plain
# ---------------------------------------------------------------------------


class TestPrintPhaseTimingPlain:
    """Tests for print_phase_timing_report in plain-text mode."""

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_plain_report(self, _nb, _rich, capsys):
        p = PipelineProgress("p", ["a"])
        p._node_statuses["a"]["phase_timings"] = {"read": 200.0, "write": 800.0}
        p.print_phase_timing_report()
        out = capsys.readouterr().out
        assert "Phase Bottlenecks" in out
        assert "read" in out
        assert "write" in out
        assert "800ms" in out
        assert "200ms" in out

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_plain_report_with_pipeline_duration(self, _nb, _rich, capsys):
        p = PipelineProgress("p", ["a"])
        p._node_statuses["a"]["phase_timings"] = {"read": 500.0}
        p.print_phase_timing_report(pipeline_duration_s=2.0)
        out = capsys.readouterr().out
        assert "25.0%" in out

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_no_output_when_no_timings(self, _nb, _rich, capsys):
        p = PipelineProgress("p", ["a"])
        p.print_phase_timing_report()
        out = capsys.readouterr().out
        assert out == ""

    @patch("odibi.utils.progress.is_rich_available", return_value=False)
    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    def test_seconds_formatting_for_large_durations(self, _nb, _rich, capsys):
        p = PipelineProgress("p", ["a"])
        p._node_statuses["a"]["phase_timings"] = {"write": 2500.0}
        p.print_phase_timing_report()
        out = capsys.readouterr().out
        assert "2.50s" in out


# ---------------------------------------------------------------------------
# Rich path (mocked)
# ---------------------------------------------------------------------------


class TestRichPath:
    """Tests verifying Rich methods are called when Rich is available."""

    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    @patch("odibi.utils.progress.is_rich_available", return_value=True)
    def test_start_calls_start_rich(self, _rich, _nb):
        p = PipelineProgress("p", ["a"])
        with patch.object(p, "_start_rich") as mock_sr:
            p.start()
            mock_sr.assert_called_once()

    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    @patch("odibi.utils.progress.is_rich_available", return_value=True)
    def test_update_calls_update_rich(self, _rich, _nb):
        p = PipelineProgress("p", ["a"])
        with patch.object(p, "_update_rich") as mock_ur:
            p.update_node("a", NodeStatus.SUCCESS, duration=1.0, rows=10)
            mock_ur.assert_called_once_with("a", NodeStatus.SUCCESS, 1.0, 10)

    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    @patch("odibi.utils.progress.is_rich_available", return_value=True)
    def test_finish_calls_finish_rich(self, _rich, _nb):
        p = PipelineProgress("p", ["a"])
        with patch.object(p, "_finish_rich") as mock_fr:
            p.finish(completed=1, failed=0, skipped=0, duration=5.0)
            mock_fr.assert_called_once_with(1, 0, 0, 5.0)

    @patch("odibi.utils.progress._is_notebook_environment", return_value=False)
    @patch("odibi.utils.progress.is_rich_available", return_value=True)
    def test_print_phase_timing_calls_rich(self, _rich, _nb):
        p = PipelineProgress("p", ["a"])
        p._node_statuses["a"]["phase_timings"] = {"read": 100.0}
        with patch.object(p, "_print_phase_timing_rich") as mock_ptr:
            p.print_phase_timing_report()
            mock_ptr.assert_called_once()
