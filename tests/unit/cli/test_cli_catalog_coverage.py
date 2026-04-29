"""Tests for odibi.cli.catalog — catalog CLI subcommands."""

import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from odibi.cli.catalog import (
    catalog_command,
    _format_table,
    _format_output,
    _runs_command,
    _pipelines_command,
    _nodes_command,
    _state_command,
    _tables_command,
    _metrics_command,
    _patterns_command,
    _stats_command,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_args(**overrides):
    args = MagicMock()
    args.config = "fake.yaml"
    args.format = "table"
    args.pipeline = None
    args.node = None
    args.status = None
    args.days = 7
    args.limit = 20
    args.project = None
    for k, v in overrides.items():
        setattr(args, k, v)
    return args


def _mock_catalog_with_df(df):
    catalog = MagicMock()
    catalog._read_local_table.return_value = df
    catalog.tables = {
        "meta_runs": "meta_runs",
        "meta_pipelines": "meta_pipelines",
        "meta_nodes": "meta_nodes",
        "meta_state": "meta_state",
        "meta_tables": "meta_tables",
        "meta_metrics": "meta_metrics",
        "meta_patterns": "meta_patterns",
    }
    return catalog


# ---------------------------------------------------------------------------
# _format_table
# ---------------------------------------------------------------------------


class TestFormatTable:
    def test_empty_rows_returns_no_data(self):
        result = _format_table(["a", "b"], [])
        assert result == "No data found."

    def test_single_row(self):
        result = _format_table(["name", "age"], [["Alice", 30]])
        assert "name" in result
        assert "age" in result
        assert "Alice" in result
        assert "30" in result

    def test_multiple_rows(self):
        rows = [["Alice", 30], ["Bob", 25]]
        result = _format_table(["name", "age"], rows)
        assert "Alice" in result
        assert "Bob" in result

    def test_truncation_at_max_width(self):
        long_val = "x" * 60
        result = _format_table(["col"], [[long_val]], max_width=20)
        assert "..." in result
        assert long_val not in result

    def test_column_widths_adapt_to_content(self):
        result = _format_table(["a"], [["short"]])
        lines = result.split("\n")
        header_line = lines[0]
        # Header "a" is shorter than "short", so column width includes padding
        assert len(header_line) >= 5

    def test_none_values_handled(self):
        result = _format_table(["col"], [[None]])
        assert "None" not in result or result  # Should not crash
        lines = result.split("\n")
        assert len(lines) == 3  # header + separator + 1 row

    def test_separator_line_present(self):
        result = _format_table(["a", "b"], [["x", "y"]])
        lines = result.split("\n")
        assert "-+-" in lines[1]


# ---------------------------------------------------------------------------
# _format_output
# ---------------------------------------------------------------------------


class TestFormatOutput:
    def test_json_format(self):
        result = _format_output(["name", "age"], [["Alice", 30]], "json")
        parsed = json.loads(result)
        assert isinstance(parsed, list)
        assert parsed[0]["name"] == "Alice"
        assert parsed[0]["age"] == 30

    def test_json_format_multiple_rows(self):
        rows = [["Alice", 30], ["Bob", 25]]
        result = _format_output(["name", "age"], rows, "json")
        parsed = json.loads(result)
        assert len(parsed) == 2

    def test_table_format(self):
        result = _format_output(["name"], [["Alice"]], "table")
        assert "name" in result
        assert "Alice" in result

    def test_table_format_empty(self):
        result = _format_output(["name"], [], "table")
        assert result == "No data found."


# ---------------------------------------------------------------------------
# catalog_command — dispatch
# ---------------------------------------------------------------------------


class TestCatalogCommand:
    def test_no_catalog_command_attr_returns_1(self):
        args = MagicMock(spec=[])  # no attributes at all
        result = catalog_command(args)
        assert result == 1

    def test_catalog_command_none_returns_1(self):
        args = MagicMock()
        args.catalog_command = None
        result = catalog_command(args)
        assert result == 1

    def test_unknown_command_returns_1(self):
        args = MagicMock()
        args.catalog_command = "nonexistent"
        result = catalog_command(args)
        assert result == 1

    def test_valid_command_dispatches(self):
        args = MagicMock()
        args.catalog_command = "runs"
        with patch("odibi.cli.catalog._runs_command", return_value=0) as mock_handler:
            result = catalog_command(args)
        assert result == 0
        mock_handler.assert_called_once_with(args)

    def test_valid_command_stats_dispatches(self):
        args = MagicMock()
        args.catalog_command = "stats"
        with patch("odibi.cli.catalog._stats_command", return_value=0) as mock_handler:
            result = catalog_command(args)
        assert result == 0
        mock_handler.assert_called_once_with(args)


# ---------------------------------------------------------------------------
# Individual commands — shared patterns
# ---------------------------------------------------------------------------

_SIMPLE_COMMANDS = [
    ("_runs_command", _runs_command, "meta_runs", "No runs found"),
    ("_pipelines_command", _pipelines_command, "meta_pipelines", "No pipelines registered"),
    ("_nodes_command", _nodes_command, "meta_nodes", "No nodes registered"),
    ("_state_command", _state_command, "meta_state", "No state checkpoints"),
    ("_tables_command", _tables_command, "meta_tables", "No assets registered"),
    ("_metrics_command", _metrics_command, "meta_metrics", "No metrics defined"),
    ("_patterns_command", _patterns_command, "meta_patterns", "No pattern data"),
]


class TestCommandsCatalogNone:
    @pytest.mark.parametrize(
        "name,func,table,empty_msg", _SIMPLE_COMMANDS, ids=[c[0] for c in _SIMPLE_COMMANDS]
    )
    def test_catalog_none_returns_1(self, name, func, table, empty_msg):
        args = _make_args()
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=None):
            result = func(args)
        assert result == 1


class TestCommandsEmptyDataFrame:
    @pytest.mark.parametrize(
        "name,func,table,empty_msg", _SIMPLE_COMMANDS, ids=[c[0] for c in _SIMPLE_COMMANDS]
    )
    def test_empty_df_returns_0(self, name, func, table, empty_msg, capsys):
        args = _make_args()
        catalog = _mock_catalog_with_df(pd.DataFrame())
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=catalog):
            result = func(args)
        assert result == 0
        captured = capsys.readouterr()
        assert empty_msg in captured.out


class TestCommandsWithData:
    def test_runs_with_data(self, capsys):
        df = pd.DataFrame(
            {
                "run_id": ["r1"],
                "pipeline_name": ["pipe1"],
                "node_name": ["node1"],
                "status": ["SUCCESS"],
                "rows_processed": [100],
                "duration_ms": [500],
                "timestamp": [pd.Timestamp(datetime.now(timezone.utc) - timedelta(hours=1))],
            }
        )
        args = _make_args()
        catalog = _mock_catalog_with_df(df)
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=catalog):
            result = _runs_command(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "r1" in captured.out

    def test_pipelines_with_data(self, capsys):
        df = pd.DataFrame(
            {
                "pipeline_name": ["pipe1"],
                "layer": ["silver"],
                "description": ["desc"],
                "version_hash": ["abc"],
                "updated_at": ["2026-04-17"],
            }
        )
        args = _make_args()
        catalog = _mock_catalog_with_df(df)
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=catalog):
            result = _pipelines_command(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "pipe1" in captured.out

    def test_nodes_with_data(self, capsys):
        df = pd.DataFrame(
            {
                "pipeline_name": ["pipe1"],
                "node_name": ["node1"],
                "type": ["extract"],
                "version_hash": ["abc"],
                "updated_at": ["2026-04-17"],
            }
        )
        args = _make_args()
        catalog = _mock_catalog_with_df(df)
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=catalog):
            result = _nodes_command(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "node1" in captured.out

    def test_state_with_data(self, capsys):
        df = pd.DataFrame(
            {
                "key": ["pipeline:hwm"],
                "value": ["2026-04-17"],
                "updated_at": ["2026-04-17"],
            }
        )
        args = _make_args()
        catalog = _mock_catalog_with_df(df)
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=catalog):
            result = _state_command(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "pipeline:hwm" in captured.out

    def test_tables_with_data(self, capsys):
        df = pd.DataFrame(
            {
                "project_name": ["proj1"],
                "table_name": ["customers"],
                "path": ["/data/customers"],
                "format": ["delta"],
                "pattern_type": ["scd2"],
                "updated_at": ["2026-04-17"],
            }
        )
        args = _make_args()
        catalog = _mock_catalog_with_df(df)
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=catalog):
            result = _tables_command(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "customers" in captured.out

    def test_metrics_with_data(self, capsys):
        df = pd.DataFrame(
            {
                "metric_name": ["row_count"],
                "source_table": ["customers"],
                "definition_sql": ["COUNT(*)"],
                "dimensions": ["region"],
            }
        )
        args = _make_args()
        catalog = _mock_catalog_with_df(df)
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=catalog):
            result = _metrics_command(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "row_count" in captured.out

    def test_patterns_with_data(self, capsys):
        df = pd.DataFrame(
            {
                "table_name": ["customers"],
                "pattern_type": ["scd2"],
                "compliance_score": [0.95],
                "configuration": ["{}"],
            }
        )
        args = _make_args()
        catalog = _mock_catalog_with_df(df)
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=catalog):
            result = _patterns_command(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "customers" in captured.out


class TestCommandsException:
    @pytest.mark.parametrize(
        "name,func,table,empty_msg", _SIMPLE_COMMANDS, ids=[c[0] for c in _SIMPLE_COMMANDS]
    )
    def test_exception_returns_1(self, name, func, table, empty_msg):
        args = _make_args()
        catalog = MagicMock()
        catalog._read_local_table.side_effect = RuntimeError("boom")
        catalog.tables = {table: table}
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=catalog):
            result = func(args)
        assert result == 1


# ---------------------------------------------------------------------------
# _stats_command
# ---------------------------------------------------------------------------


class TestStatsCommand:
    def test_catalog_none_returns_1(self):
        args = _make_args()
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=None):
            result = _stats_command(args)
        assert result == 1

    def test_empty_df_returns_0(self, capsys):
        args = _make_args()
        catalog = _mock_catalog_with_df(pd.DataFrame())
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=catalog):
            result = _stats_command(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "No runs found" in captured.out

    def test_stats_with_data(self, capsys):
        df = pd.DataFrame(
            {
                "run_id": ["r1", "r2", "r3"],
                "pipeline_name": ["pipe1", "pipe1", "pipe2"],
                "node_name": ["n1", "n2", "n1"],
                "status": ["SUCCESS", "FAILED", "SUCCESS"],
                "rows_processed": [100, 200, 300],
                "duration_ms": [1000, 2000, 3000],
                "timestamp": pd.to_datetime(
                    [
                        datetime.now(timezone.utc) - timedelta(hours=1),
                        datetime.now(timezone.utc) - timedelta(hours=2),
                        datetime.now(timezone.utc) - timedelta(hours=3),
                    ]
                )
                .tz_localize(None)
                .tz_localize("UTC"),
            }
        )
        args = _make_args()
        catalog = _mock_catalog_with_df(df)
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=catalog):
            result = _stats_command(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "Total Runs:" in captured.out
        assert "Successful:" in captured.out
        assert "Failed:" in captured.out
        assert "Success Rate:" in captured.out
        assert "Total Rows:" in captured.out
        assert "Avg Duration:" in captured.out
        assert "pipe1" in captured.out
        assert "Most Failed Nodes" in captured.out

    def test_stats_exception_returns_1(self):
        args = _make_args()
        catalog = MagicMock()
        catalog._read_local_table.side_effect = RuntimeError("boom")
        catalog.tables = {"meta_runs": "meta_runs"}
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=catalog):
            result = _stats_command(args)
        assert result == 1

    def test_stats_no_runs_in_date_range(self, capsys):
        df = pd.DataFrame(
            {
                "run_id": ["r1"],
                "status": ["SUCCESS"],
                "timestamp": pd.to_datetime(["2020-01-01"]).tz_localize("UTC"),
            }
        )
        args = _make_args(days=1)
        catalog = _mock_catalog_with_df(df)
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=catalog):
            result = _stats_command(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "No runs in the last" in captured.out


# ---------------------------------------------------------------------------
# JSON output for individual commands
# ---------------------------------------------------------------------------


class TestCommandsJsonOutput:
    def test_runs_json(self, capsys):
        df = pd.DataFrame(
            {
                "run_id": ["r1"],
                "pipeline_name": ["pipe1"],
                "status": ["SUCCESS"],
                "timestamp": [pd.Timestamp(datetime.now(timezone.utc) - timedelta(hours=1))],
            }
        )
        args = _make_args(format="json")
        catalog = _mock_catalog_with_df(df)
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=catalog):
            result = _runs_command(args)
        assert result == 0
        captured = capsys.readouterr()
        # The JSON part should be parseable
        json_part = captured.out.split("\n\n")[0]
        parsed = json.loads(json_part)
        assert isinstance(parsed, list)

    def test_pipelines_json(self, capsys):
        df = pd.DataFrame(
            {
                "pipeline_name": ["pipe1"],
                "layer": ["silver"],
            }
        )
        args = _make_args(format="json")
        catalog = _mock_catalog_with_df(df)
        with patch("odibi.cli.catalog._get_catalog_manager", return_value=catalog):
            result = _pipelines_command(args)
        assert result == 0
        captured = capsys.readouterr()
        json_part = captured.out.split("\n\n")[0]
        parsed = json.loads(json_part)
        assert parsed[0]["pipeline_name"] == "pipe1"
