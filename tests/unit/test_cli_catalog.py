"""Tests for catalog CLI command."""

from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pandas as pd

from odibi.cli.catalog import (
    _format_output,
    _format_table,
    add_catalog_parser,
    catalog_command,
)


class TestCatalogCLIImports:
    """Test that catalog CLI modules are importable."""

    def test_catalog_command_import(self):
        """Catalog command should be importable."""
        assert catalog_command is not None
        assert callable(catalog_command)

    def test_add_catalog_parser_import(self):
        """add_catalog_parser should be importable."""
        assert add_catalog_parser is not None
        assert callable(add_catalog_parser)


class TestCatalogParserSetup:
    """Test catalog parser configuration."""

    def test_add_catalog_parser(self):
        """Catalog parser should add subcommands."""
        import argparse

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_catalog_parser(subparsers)

        # Parse catalog runs command
        args = parser.parse_args(["catalog", "runs", "config.yaml"])
        assert args.command == "catalog"
        assert args.catalog_command == "runs"
        assert args.config == "config.yaml"

    def test_catalog_runs_options(self):
        """Runs subcommand should accept filter options."""
        import argparse

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_catalog_parser(subparsers)

        args = parser.parse_args(
            [
                "catalog",
                "runs",
                "config.yaml",
                "--pipeline",
                "my_pipeline",
                "--status",
                "SUCCESS",
                "--days",
                "14",
                "--limit",
                "50",
                "--format",
                "json",
            ]
        )
        assert args.pipeline == "my_pipeline"
        assert args.status == "SUCCESS"
        assert args.days == 14
        assert args.limit == 50
        assert args.format == "json"

    def test_catalog_pipelines_options(self):
        """Pipelines subcommand should accept format option."""
        import argparse

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_catalog_parser(subparsers)

        args = parser.parse_args(["catalog", "pipelines", "config.yaml", "--format", "json"])
        assert args.catalog_command == "pipelines"
        assert args.format == "json"

    def test_catalog_nodes_options(self):
        """Nodes subcommand should accept pipeline filter."""
        import argparse

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_catalog_parser(subparsers)

        args = parser.parse_args(["catalog", "nodes", "config.yaml", "--pipeline", "etl_pipeline"])
        assert args.catalog_command == "nodes"
        assert args.pipeline == "etl_pipeline"

    def test_catalog_stats_options(self):
        """Stats subcommand should accept days and pipeline options."""
        import argparse

        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")
        add_catalog_parser(subparsers)

        args = parser.parse_args(
            ["catalog", "stats", "config.yaml", "--days", "30", "--pipeline", "daily_etl"]
        )
        assert args.catalog_command == "stats"
        assert args.days == 30
        assert args.pipeline == "daily_etl"


class TestFormatTable:
    """Test table formatting."""

    def test_format_table_empty(self):
        """Empty rows should show no data message."""
        result = _format_table(["col1", "col2"], [])
        assert result == "No data found."

    def test_format_table_simple(self):
        """Simple table should format correctly."""
        headers = ["name", "value"]
        rows = [["foo", "bar"], ["hello", "world"]]
        result = _format_table(headers, rows)

        assert "name" in result
        assert "value" in result
        assert "foo" in result
        assert "bar" in result
        assert "hello" in result
        assert "world" in result
        assert "---" in result  # separator

    def test_format_table_truncates_long_values(self):
        """Long values should be truncated."""
        headers = ["name"]
        rows = [["a" * 100]]
        result = _format_table(headers, rows, max_width=20)

        assert "..." in result
        assert len(result.split("\n")[-1].strip()) <= 20


class TestFormatOutput:
    """Test output formatting."""

    def test_format_output_json(self):
        """JSON format should produce valid JSON."""
        import json

        headers = ["name", "value"]
        rows = [["foo", 123]]
        result = _format_output(headers, rows, "json")

        parsed = json.loads(result)
        assert len(parsed) == 1
        assert parsed[0]["name"] == "foo"
        assert parsed[0]["value"] == 123

    def test_format_output_table(self):
        """Table format should produce ASCII table."""
        headers = ["col1"]
        rows = [["val1"]]
        result = _format_output(headers, rows, "table")

        assert "col1" in result
        assert "val1" in result


class TestCatalogCommand:
    """Test catalog command dispatch."""

    def test_catalog_no_subcommand(self, capsys):
        """Catalog without subcommand shows help."""
        args = Mock()
        args.catalog_command = None

        result = catalog_command(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "runs" in captured.out
        assert "pipelines" in captured.out

    def test_catalog_unknown_subcommand(self, capsys):
        """Unknown subcommand shows error."""
        args = Mock()
        args.catalog_command = "unknown"

        result = catalog_command(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Unknown" in captured.out


class TestCatalogRunsCommand:
    """Test catalog runs subcommand."""

    def test_runs_no_catalog_configured(self, capsys):
        """Should fail if catalog not configured."""
        args = Mock()
        args.catalog_command = "runs"
        args.config = "test.yaml"

        with patch("odibi.cli.catalog._get_catalog_manager") as mock_get:
            mock_get.return_value = None
            result = catalog_command(args)

        assert result == 1

    def test_runs_empty_catalog(self, capsys):
        """Should show message if no runs found."""
        args = Mock()
        args.catalog_command = "runs"
        args.config = "test.yaml"
        args.days = 7
        args.pipeline = None
        args.node = None
        args.status = None
        args.limit = 20
        args.format = "table"

        mock_catalog = Mock()
        mock_catalog.tables = {"meta_runs": "/path/to/meta_runs"}
        mock_catalog._read_local_table.return_value = pd.DataFrame()

        with patch("odibi.cli.catalog._get_catalog_manager") as mock_get:
            mock_get.return_value = mock_catalog
            result = catalog_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "No runs found" in captured.out

    def test_runs_with_data(self, capsys):
        """Should display runs data correctly."""
        args = Mock()
        args.catalog_command = "runs"
        args.config = "test.yaml"
        args.days = 7
        args.pipeline = None
        args.node = None
        args.status = None
        args.limit = 20
        args.format = "table"

        mock_catalog = Mock()
        mock_catalog.tables = {"meta_runs": "/path/to/meta_runs"}

        runs_df = pd.DataFrame(
            {
                "run_id": ["run-1", "run-2"],
                "pipeline_name": ["etl", "etl"],
                "node_name": ["node1", "node2"],
                "status": ["SUCCESS", "SUCCESS"],
                "rows_processed": [100, 200],
                "duration_ms": [1000, 2000],
                "timestamp": [datetime.now(timezone.utc), datetime.now(timezone.utc)],
            }
        )
        mock_catalog._read_local_table.return_value = runs_df

        with patch("odibi.cli.catalog._get_catalog_manager") as mock_get:
            mock_get.return_value = mock_catalog
            result = catalog_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "run-1" in captured.out or "etl" in captured.out


class TestCatalogPipelinesCommand:
    """Test catalog pipelines subcommand."""

    def test_pipelines_with_data(self, capsys):
        """Should display pipeline data correctly."""
        args = Mock()
        args.catalog_command = "pipelines"
        args.config = "test.yaml"
        args.format = "table"

        mock_catalog = Mock()
        mock_catalog.tables = {"meta_pipelines": "/path/to/meta_pipelines"}

        pipelines_df = pd.DataFrame(
            {
                "pipeline_name": ["daily_etl", "weekly_report"],
                "layer": ["gold", "silver"],
                "description": ["Daily ETL", "Weekly reports"],
                "version_hash": ["abc123", "def456"],
                "updated_at": [datetime.now(timezone.utc), datetime.now(timezone.utc)],
            }
        )
        mock_catalog._read_local_table.return_value = pipelines_df

        with patch("odibi.cli.catalog._get_catalog_manager") as mock_get:
            mock_get.return_value = mock_catalog
            result = catalog_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "daily_etl" in captured.out
        assert "2 pipeline(s)" in captured.out


class TestCatalogStatsCommand:
    """Test catalog stats subcommand."""

    def test_stats_with_data(self, capsys):
        """Should display statistics correctly."""
        args = Mock()
        args.catalog_command = "stats"
        args.config = "test.yaml"
        args.days = 7
        args.pipeline = None

        mock_catalog = Mock()
        mock_catalog.tables = {"meta_runs": "/path/to/meta_runs"}

        runs_df = pd.DataFrame(
            {
                "run_id": ["run-1", "run-2", "run-3"],
                "pipeline_name": ["etl", "etl", "report"],
                "node_name": ["node1", "node2", "node1"],
                "status": ["SUCCESS", "SUCCESS", "FAILED"],
                "rows_processed": [100, 200, 50],
                "duration_ms": [1000, 2000, 500],
                "timestamp": [
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc),
                ],
            }
        )
        mock_catalog._read_local_table.return_value = runs_df

        with patch("odibi.cli.catalog._get_catalog_manager") as mock_get:
            mock_get.return_value = mock_catalog
            result = catalog_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Total Runs:" in captured.out
        assert "Successful:" in captured.out
        assert "Failed:" in captured.out
        assert "Success Rate:" in captured.out


class TestCatalogNodesCommand:
    """Test catalog nodes subcommand."""

    def test_nodes_with_pipeline_filter(self, capsys):
        """Should filter nodes by pipeline."""
        args = Mock()
        args.catalog_command = "nodes"
        args.config = "test.yaml"
        args.pipeline = "etl"
        args.format = "table"

        mock_catalog = Mock()
        mock_catalog.tables = {"meta_nodes": "/path/to/meta_nodes"}

        nodes_df = pd.DataFrame(
            {
                "pipeline_name": ["etl", "etl", "report"],
                "node_name": ["read_data", "transform", "generate"],
                "type": ["read", "transform", "write"],
                "version_hash": ["a1", "b2", "c3"],
                "updated_at": [
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc),
                ],
            }
        )
        mock_catalog._read_local_table.return_value = nodes_df

        with patch("odibi.cli.catalog._get_catalog_manager") as mock_get:
            mock_get.return_value = mock_catalog
            result = catalog_command(args)

        assert result == 0
        captured = capsys.readouterr()
        # Should only show 2 nodes (etl pipeline)
        assert "2 node(s)" in captured.out


class TestCatalogStateCommand:
    """Test catalog state subcommand."""

    def test_state_empty(self, capsys):
        """Should show message if no state checkpoints."""
        args = Mock()
        args.catalog_command = "state"
        args.config = "test.yaml"
        args.pipeline = None
        args.format = "table"

        mock_catalog = Mock()
        mock_catalog.tables = {"meta_state": "/path/to/meta_state"}
        mock_catalog._read_local_table.return_value = pd.DataFrame()

        with patch("odibi.cli.catalog._get_catalog_manager") as mock_get:
            mock_get.return_value = mock_catalog
            result = catalog_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "No state checkpoints" in captured.out
