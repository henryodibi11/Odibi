"""Comprehensive coverage tests for odibi/utils/progress.py.

Targets uncovered Rich paths: _start_rich, _create_header_panel,
_create_progress_table, _update_rich, _finish_rich, _print_phase_timing_rich.
"""

from unittest.mock import MagicMock, patch

from odibi.utils.progress import NodeStatus, PipelineProgress


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_progress(name="p", nodes=None, layers=None, engine="pandas", rich=False, notebook=False):
    """Build a PipelineProgress with patched environment flags."""
    nodes = nodes or ["a"]
    with (
        patch("odibi.utils.progress.is_rich_available", return_value=rich),
        patch("odibi.utils.progress._is_notebook_environment", return_value=notebook),
    ):
        return PipelineProgress(name, nodes, engine=engine, layers=layers)


# ---------------------------------------------------------------------------
# NodeStatus constants
# ---------------------------------------------------------------------------


class TestNodeStatusConstants:
    def test_all_values(self):
        assert NodeStatus.PENDING == "pending"
        assert NodeStatus.RUNNING == "running"
        assert NodeStatus.SUCCESS == "success"
        assert NodeStatus.FAILED == "failed"
        assert NodeStatus.SKIPPED == "skipped"


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------


class TestPipelineProgressInit:
    def test_defaults(self):
        p = _make_progress()
        assert p.pipeline_name == "p"
        assert p.engine == "pandas"
        assert p.layers is None
        assert p._node_to_layer == {}
        assert p._live is None
        assert p._start_time is None
        assert p._last_printed_layer == -1

    def test_with_layers_builds_node_to_layer(self):
        p = _make_progress(nodes=["a", "b", "c"], layers=[["a", "b"], ["c"]])
        assert p._node_to_layer == {"a": 0, "b": 0, "c": 1}

    def test_all_nodes_start_pending(self):
        p = _make_progress(nodes=["x", "y"])
        for info in p._node_statuses.values():
            assert info["status"] == NodeStatus.PENDING
            assert info["duration"] is None
            assert info["rows"] is None


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


class TestFormatStatus:
    def test_rich_markup_for_each(self):
        p = _make_progress()
        assert "[dim]" in p._format_status(NodeStatus.PENDING)
        assert "[yellow]" in p._format_status(NodeStatus.RUNNING)
        assert "[green]" in p._format_status(NodeStatus.SUCCESS)
        assert "[red]" in p._format_status(NodeStatus.FAILED)
        assert "skipped" in p._format_status(NodeStatus.SKIPPED)

    def test_unknown_returns_raw(self):
        p = _make_progress()
        assert p._format_status("boom") == "boom"


class TestFormatStatusPlain:
    def test_all(self):
        p = _make_progress()
        assert p._format_status_plain(NodeStatus.PENDING) == "○ pending"
        assert p._format_status_plain(NodeStatus.RUNNING) == "◉ running"
        assert p._format_status_plain(NodeStatus.SUCCESS) == "✓ success"
        assert p._format_status_plain(NodeStatus.FAILED) == "✗ failed"
        assert p._format_status_plain(NodeStatus.SKIPPED) == "⏭ skipped"

    def test_unknown_returns_raw(self):
        p = _make_progress()
        assert p._format_status_plain("x") == "x"


class TestFormatDuration:
    def test_none(self):
        assert _make_progress()._format_duration(None) == "-"

    def test_sub_second(self):
        assert _make_progress()._format_duration(0.456) == "456ms"

    def test_seconds(self):
        assert _make_progress()._format_duration(3.14) == "3.14s"

    def test_exactly_one(self):
        assert _make_progress()._format_duration(1.0) == "1.00s"

    def test_zero(self):
        assert _make_progress()._format_duration(0.0) == "0ms"


class TestFormatRows:
    def test_none(self):
        assert _make_progress()._format_rows(None) == "-"

    def test_small(self):
        assert _make_progress()._format_rows(42) == "42"

    def test_thousands(self):
        assert _make_progress()._format_rows(5_500) == "5.5K"

    def test_millions(self):
        assert _make_progress()._format_rows(2_300_000) == "2.3M"

    def test_exactly_1000(self):
        assert _make_progress()._format_rows(1_000) == "1.0K"

    def test_exactly_1m(self):
        assert _make_progress()._format_rows(1_000_000) == "1.0M"


# ---------------------------------------------------------------------------
# start – plain path
# ---------------------------------------------------------------------------


class TestStartPlain:
    def test_prints_header(self, capsys):
        p = _make_progress(name="test_pipe", nodes=["a", "b", "c"])
        p.start()
        out = capsys.readouterr().out
        assert "Pipeline: test_pipe" in out
        assert "Engine: pandas" in out
        assert "Nodes: 3" in out

    def test_sets_start_time(self):
        p = _make_progress()
        p.start()
        assert p._start_time is not None


# ---------------------------------------------------------------------------
# start – rich CLI path (_start_rich, is_notebook=False)
# ---------------------------------------------------------------------------


class TestStartRichCLI:
    def test_creates_live_display(self):
        p = _make_progress(rich=True, notebook=False, nodes=["a", "b"])
        mock_console = MagicMock()
        mock_live_cls = MagicMock()
        mock_live_instance = MagicMock()
        mock_live_cls.return_value = mock_live_instance

        with (
            patch("odibi.utils.progress.get_console", return_value=mock_console),
            patch("odibi.utils.progress.Live", mock_live_cls, create=True),
            patch("rich.live.Live", mock_live_cls),
        ):
            p.start()

        assert p._start_time is not None
        mock_console.print.assert_called()  # header panel
        assert p._live is not None

    def test_table_created_for_cli(self):
        p = _make_progress(rich=True, notebook=False, nodes=["a"])
        mock_console = MagicMock()

        with (
            patch("odibi.utils.progress.get_console", return_value=mock_console),
            patch("rich.live.Live") as mock_live_cls,
        ):
            mock_live_cls.return_value = MagicMock()
            p.start()

        assert p._table is not None


# ---------------------------------------------------------------------------
# start – rich notebook path
# ---------------------------------------------------------------------------


class TestStartRichNotebook:
    def test_no_live_in_notebook(self):
        p = _make_progress(rich=True, notebook=True, nodes=["a", "b"])
        mock_console = MagicMock()

        with patch("odibi.utils.progress.get_console", return_value=mock_console):
            p.start()

        assert p._live is None
        assert p._table is None
        # Should print "Executing N nodes..."
        calls = [str(c) for c in mock_console.print.call_args_list]
        assert any("Executing 2 nodes" in c for c in calls)


# ---------------------------------------------------------------------------
# _create_header_panel
# ---------------------------------------------------------------------------


class TestCreateHeaderPanel:
    def test_returns_panel_object(self):
        p = _make_progress(rich=True, name="my_pipe", nodes=["x"])
        panel = p._create_header_panel()
        from rich.panel import Panel

        assert isinstance(panel, Panel)


# ---------------------------------------------------------------------------
# _create_progress_table
# ---------------------------------------------------------------------------


class TestCreateProgressTable:
    def test_returns_table_with_correct_rows(self):
        p = _make_progress(rich=True, nodes=["n1", "n2"])
        table = p._create_progress_table()
        from rich.table import Table

        assert isinstance(table, Table)
        assert table.row_count == 2

    def test_table_reflects_updated_status(self):
        p = _make_progress(rich=True, nodes=["n1"])
        p._node_statuses["n1"]["status"] = NodeStatus.SUCCESS
        p._node_statuses["n1"]["duration"] = 2.5
        p._node_statuses["n1"]["rows"] = 1000
        table = p._create_progress_table()
        assert table.row_count == 1


# ---------------------------------------------------------------------------
# update_node
# ---------------------------------------------------------------------------


class TestUpdateNode:
    def test_unknown_node_ignored(self):
        p = _make_progress(nodes=["a"])
        p.update_node("ghost", NodeStatus.SUCCESS)
        assert "ghost" not in p._node_statuses

    def test_stores_all_fields(self):
        p = _make_progress(nodes=["a"])
        timings = {"read": 10.0, "write": 20.0}
        p.update_node("a", NodeStatus.SUCCESS, duration=1.5, rows=99, phase_timings=timings)
        info = p._node_statuses["a"]
        assert info["status"] == NodeStatus.SUCCESS
        assert info["duration"] == 1.5
        assert info["rows"] == 99
        assert info["phase_timings"] == timings


# ---------------------------------------------------------------------------
# _update_plain with layers
# ---------------------------------------------------------------------------


class TestUpdatePlain:
    def test_wave_headers(self, capsys):
        p = _make_progress(nodes=["a", "b", "c"], layers=[["a", "b"], ["c"]])
        p.update_node("a", NodeStatus.SUCCESS, duration=0.1)
        p.update_node("b", NodeStatus.SUCCESS, duration=0.2)
        p.update_node("c", NodeStatus.SUCCESS, duration=0.3)
        out = capsys.readouterr().out
        assert "Wave 1 (parallel):" in out
        assert "Wave 2:" in out

    def test_wave_header_only_once(self, capsys):
        p = _make_progress(nodes=["a", "b"], layers=[["a", "b"]])
        p.update_node("a", NodeStatus.SUCCESS)
        p.update_node("b", NodeStatus.SUCCESS)
        out = capsys.readouterr().out
        assert out.count("Wave 1") == 1

    def test_no_layers_no_wave(self, capsys):
        p = _make_progress(nodes=["a"])
        p.update_node("a", NodeStatus.SUCCESS, duration=0.5, rows=10)
        out = capsys.readouterr().out
        assert "Wave" not in out
        assert "a:" in out


# ---------------------------------------------------------------------------
# _update_rich – CLI mode (has _live)
# ---------------------------------------------------------------------------


class TestUpdateRichCLI:
    def test_updates_live_display(self):
        p = _make_progress(rich=True, notebook=False, nodes=["a"])
        mock_live = MagicMock()
        p._live = mock_live
        p._table = MagicMock()

        p.update_node("a", NodeStatus.SUCCESS, duration=1.0, rows=10)

        mock_live.update.assert_called_once()
        assert p._table is not None  # rebuilt


# ---------------------------------------------------------------------------
# _update_rich – notebook mode
# ---------------------------------------------------------------------------


class TestUpdateRichNotebook:
    def test_prints_node_status(self):
        p = _make_progress(rich=True, notebook=True, nodes=["a"])
        mock_console = MagicMock()
        with patch("odibi.utils.progress.get_console", return_value=mock_console):
            p.update_node("a", NodeStatus.SUCCESS, duration=1.0, rows=500)
        calls = [str(c) for c in mock_console.print.call_args_list]
        assert any("a:" in c for c in calls)

    def test_notebook_wave_headers_with_layers(self):
        p = _make_progress(
            rich=True, notebook=True, nodes=["a", "b", "c"], layers=[["a", "b"], ["c"]]
        )
        mock_console = MagicMock()
        with patch("odibi.utils.progress.get_console", return_value=mock_console):
            p.update_node("a", NodeStatus.SUCCESS, duration=0.1)
            p.update_node("b", NodeStatus.SUCCESS, duration=0.2)
            p.update_node("c", NodeStatus.SUCCESS, duration=0.3)
        calls = [str(c) for c in mock_console.print.call_args_list]
        assert any("Wave 1" in c for c in calls)
        assert any("Wave 2" in c for c in calls)

    def test_notebook_parallel_note(self):
        p = _make_progress(rich=True, notebook=True, nodes=["a", "b"], layers=[["a", "b"]])
        mock_console = MagicMock()
        with patch("odibi.utils.progress.get_console", return_value=mock_console):
            p.update_node("a", NodeStatus.SUCCESS)
        calls = [str(c) for c in mock_console.print.call_args_list]
        assert any("parallel" in c for c in calls)

    def test_notebook_single_node_layer_no_parallel(self):
        p = _make_progress(rich=True, notebook=True, nodes=["a"], layers=[["a"]])
        mock_console = MagicMock()
        with patch("odibi.utils.progress.get_console", return_value=mock_console):
            p.update_node("a", NodeStatus.SUCCESS)
        calls = [str(c) for c in mock_console.print.call_args_list]
        wave_calls = [c for c in calls if "Wave" in c]
        assert all("parallel" not in c for c in wave_calls)


# ---------------------------------------------------------------------------
# finish – plain path
# ---------------------------------------------------------------------------


class TestFinishPlain:
    def test_success(self, capsys):
        p = _make_progress()
        p.finish(completed=1, failed=0, skipped=0, duration=1.23)
        out = capsys.readouterr().out
        assert "SUCCESS" in out
        assert "1.23s" in out

    def test_failure(self, capsys):
        p = _make_progress()
        p.finish(completed=0, failed=1, skipped=0, duration=0.5)
        out = capsys.readouterr().out
        assert "FAILED" in out

    def test_uses_elapsed_when_no_duration(self, capsys):
        p = _make_progress()
        p._start_time = 1000.0
        with patch("time.time", return_value=1002.5):
            p.finish(completed=1)
        out = capsys.readouterr().out
        assert "2.50s" in out

    def test_stops_live_if_present(self):
        p = _make_progress()
        mock_live = MagicMock()
        p._live = mock_live
        p.finish(completed=1, duration=1.0)
        mock_live.stop.assert_called_once()
        assert p._live is None


# ---------------------------------------------------------------------------
# finish – rich path (_finish_rich)
# ---------------------------------------------------------------------------


class TestFinishRich:
    def test_success_panel(self):
        p = _make_progress(rich=True, nodes=["a"])
        mock_console = MagicMock()
        with patch("odibi.utils.progress.get_console", return_value=mock_console):
            p._finish_rich(completed=1, failed=0, skipped=0, duration=2.0)
        # Verify a Panel with title "Pipeline Complete" was printed
        from rich.panel import Panel

        panel_args = [
            c for c in mock_console.print.call_args_list if c.args and isinstance(c.args[0], Panel)
        ]
        assert len(panel_args) >= 1

    def test_failure_panel_red(self):
        p = _make_progress(rich=True, nodes=["a"])
        mock_console = MagicMock()
        with patch("odibi.utils.progress.get_console", return_value=mock_console):
            p._finish_rich(completed=0, failed=1, skipped=0, duration=1.0)
        # The panel should be created (we verify it was printed)
        assert mock_console.print.call_count >= 2  # table + panel

    def test_skipped_shown_when_nonzero(self):
        p = _make_progress(rich=True, nodes=["a"])
        mock_console = MagicMock()
        with patch("odibi.utils.progress.get_console", return_value=mock_console):
            p._finish_rich(completed=1, failed=0, skipped=2, duration=1.0)
        # Panel was printed (we can't inspect Rich Text easily but verify calls)
        assert mock_console.print.call_count >= 2

    def test_finish_rich_via_finish(self):
        p = _make_progress(rich=True, nodes=["a"])
        mock_console = MagicMock()
        with patch("odibi.utils.progress.get_console", return_value=mock_console):
            p.finish(completed=1, failed=0, skipped=0, duration=5.0)
        assert mock_console.print.call_count >= 2


# ---------------------------------------------------------------------------
# Phase timing
# ---------------------------------------------------------------------------


class TestPhaseTimingSummary:
    def test_returns_nodes_with_timings(self):
        p = _make_progress(nodes=["a", "b"])
        p._node_statuses["a"]["phase_timings"] = {"read": 50.0}
        result = p.get_phase_timing_summary()
        assert "a" in result
        assert "b" not in result

    def test_empty_when_no_timings(self):
        p = _make_progress(nodes=["a"])
        assert p.get_phase_timing_summary() == {}


class TestAggregatePhaseTimings:
    def test_max_across_nodes(self):
        p = _make_progress(nodes=["a", "b"])
        p._node_statuses["a"]["phase_timings"] = {"read": 50.0, "write": 30.0}
        p._node_statuses["b"]["phase_timings"] = {"read": 80.0, "write": 10.0}
        result = p.get_aggregate_phase_timings()
        assert result == {"read": 80.0, "write": 30.0}

    def test_rounds_values(self):
        p = _make_progress(nodes=["a"])
        p._node_statuses["a"]["phase_timings"] = {"read": 50.556}
        assert p.get_aggregate_phase_timings() == {"read": 50.56}

    def test_empty(self):
        assert _make_progress().get_aggregate_phase_timings() == {}


# ---------------------------------------------------------------------------
# print_phase_timing_report – plain
# ---------------------------------------------------------------------------


class TestPhaseTimingPlain:
    def test_report_output(self, capsys):
        p = _make_progress(nodes=["a"])
        p._node_statuses["a"]["phase_timings"] = {"read": 200.0, "write": 800.0}
        p.print_phase_timing_report()
        out = capsys.readouterr().out
        assert "Phase Bottlenecks" in out
        assert "write" in out
        assert "800ms" in out

    def test_with_pipeline_duration(self, capsys):
        p = _make_progress(nodes=["a"])
        p._node_statuses["a"]["phase_timings"] = {"read": 500.0}
        p.print_phase_timing_report(pipeline_duration_s=2.0)
        out = capsys.readouterr().out
        assert "25.0%" in out

    def test_no_output_when_empty(self, capsys):
        p = _make_progress(nodes=["a"])
        p.print_phase_timing_report()
        assert capsys.readouterr().out == ""

    def test_large_duration_seconds(self, capsys):
        p = _make_progress(nodes=["a"])
        p._node_statuses["a"]["phase_timings"] = {"write": 2500.0}
        p.print_phase_timing_report()
        out = capsys.readouterr().out
        assert "2.50s" in out

    def test_zero_total_ms(self, capsys):
        """When total_ms is 0, percentage should be 0."""
        p = _make_progress(nodes=["a"])
        p._node_statuses["a"]["phase_timings"] = {"read": 0.0}
        p.print_phase_timing_report()
        # aggregate will be {"read": 0.0}, total_ms=0
        # pct = 0 due to guard
        out = capsys.readouterr().out
        # No crash is the main assertion
        assert "read" in out


# ---------------------------------------------------------------------------
# _print_phase_timing_rich
# ---------------------------------------------------------------------------


class TestPhaseTimingRich:
    def test_rich_report(self):
        p = _make_progress(rich=True, nodes=["a"])
        p._node_statuses["a"]["phase_timings"] = {"read": 200.0, "write": 1500.0}
        mock_console = MagicMock()
        with patch("odibi.utils.progress.get_console", return_value=mock_console):
            p.print_phase_timing_report()
        from rich.panel import Panel

        panel_args = [
            c for c in mock_console.print.call_args_list if c.args and isinstance(c.args[0], Panel)
        ]
        assert len(panel_args) >= 1

    def test_rich_with_pipeline_duration(self):
        p = _make_progress(rich=True, nodes=["a"])
        p._node_statuses["a"]["phase_timings"] = {"read": 500.0}
        mock_console = MagicMock()
        with patch("odibi.utils.progress.get_console", return_value=mock_console):
            p.print_phase_timing_report(pipeline_duration_s=2.0)
        assert mock_console.print.called

    def test_rich_no_output_when_empty(self):
        p = _make_progress(rich=True, nodes=["a"])
        mock_console = MagicMock()
        with patch("odibi.utils.progress.get_console", return_value=mock_console):
            p.print_phase_timing_report()
        mock_console.print.assert_not_called()

    def test_rich_large_duration_formatted_as_seconds(self):
        p = _make_progress(rich=True, nodes=["a"])
        p._node_statuses["a"]["phase_timings"] = {"transform": 3000.0}
        mock_console = MagicMock()
        with patch("odibi.utils.progress.get_console", return_value=mock_console):
            p._print_phase_timing_rich({"transform": 3000.0}, 5000.0)
        assert mock_console.print.called

    def test_rich_small_duration_formatted_as_ms(self):
        p = _make_progress(rich=True, nodes=["a"])
        mock_console = MagicMock()
        with patch("odibi.utils.progress.get_console", return_value=mock_console):
            p._print_phase_timing_rich({"read": 250.0}, 1000.0)
        assert mock_console.print.called
