"""Tests for CLI list and explain commands."""

import json
import sys
from argparse import Namespace
from unittest.mock import patch


from odibi.cli.list_cmd import (
    explain_command,
    list_command,
    list_connections_command,
    list_patterns_command,
    list_transformers_command,
)


class TestListTransformers:
    """Test odibi list transformers command."""

    def test_list_transformers_table_format(self, capsys):
        """List transformers in table format."""
        args = Namespace(format="table")
        result = list_transformers_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Available Transformers" in captured.out
        assert "fill_nulls" in captured.out
        assert "derive_columns" in captured.out

    def test_list_transformers_json_format(self, capsys):
        """List transformers in JSON format."""
        args = Namespace(format="json")
        result = list_transformers_command(args)

        assert result == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)

        assert isinstance(data, list)
        assert len(data) > 40  # We have 52+ transformers

        # Check structure
        transformer_names = [t["name"] for t in data]
        assert "fill_nulls" in transformer_names
        assert "derive_columns" in transformer_names
        assert "filter_rows" in transformer_names

        # Check each item has required fields
        for item in data:
            assert "name" in item
            assert "parameters" in item
            assert "docstring" in item


class TestListPatterns:
    """Test odibi list patterns command."""

    def test_list_patterns_table_format(self, capsys):
        """List patterns in table format."""
        args = Namespace(format="table")
        result = list_patterns_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Available Patterns" in captured.out
        assert "dimension" in captured.out
        assert "fact" in captured.out
        assert "scd2" in captured.out

    def test_list_patterns_json_format(self, capsys):
        """List patterns in JSON format."""
        args = Namespace(format="json")
        result = list_patterns_command(args)

        assert result == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)

        assert isinstance(data, list)
        assert len(data) == 6  # We have 6 patterns

        pattern_names = [p["name"] for p in data]
        assert "dimension" in pattern_names
        assert "fact" in pattern_names
        assert "scd2" in pattern_names
        assert "merge" in pattern_names
        assert "aggregation" in pattern_names
        assert "date_dimension" in pattern_names

        # Check structure
        for item in data:
            assert "name" in item
            assert "class" in item
            assert "parameters" in item


class TestListConnections:
    """Test odibi list connections command."""

    def test_list_connections_table_format(self, capsys):
        """List connections in table format."""
        args = Namespace(format="table")
        result = list_connections_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Available Connection Types" in captured.out
        assert "local" in captured.out
        assert "azure_sql" in captured.out

    def test_list_connections_json_format(self, capsys):
        """List connections in JSON format."""
        args = Namespace(format="json")
        result = list_connections_command(args)

        assert result == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)

        assert isinstance(data, list)
        assert len(data) >= 6  # We have 7 connection types

        connection_names = [c["name"] for c in data]
        assert "local" in connection_names
        assert "sql_server" in connection_names or "azure_sql" in connection_names


class TestListCommand:
    """Test odibi list subcommand dispatch."""

    def test_list_command_transformers(self, capsys):
        """List command dispatches to transformers."""
        args = Namespace(list_command="transformers", format="table")
        result = list_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Transformers" in captured.out

    def test_list_command_patterns(self, capsys):
        """List command dispatches to patterns."""
        args = Namespace(list_command="patterns", format="table")
        result = list_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Patterns" in captured.out

    def test_list_command_connections(self, capsys):
        """List command dispatches to connections."""
        args = Namespace(list_command="connections", format="table")
        result = list_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Connection" in captured.out

    def test_list_command_invalid(self, capsys):
        """List command shows usage for invalid subcommand."""
        args = Namespace(list_command=None, format="table")
        result = list_command(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Usage" in captured.out


class TestExplainCommand:
    """Test odibi explain command."""

    def test_explain_transformer(self, capsys):
        """Explain a transformer."""
        args = Namespace(name="fill_nulls")
        result = explain_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Transformer: fill_nulls" in captured.out
        assert "Parameters:" in captured.out
        assert "Example YAML:" in captured.out

    def test_explain_pattern(self, capsys):
        """Explain a pattern."""
        args = Namespace(name="dimension")
        result = explain_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Pattern: dimension" in captured.out
        assert "natural_key" in captured.out or "surrogate_key" in captured.out

    def test_explain_connection(self, capsys):
        """Explain a connection type."""
        args = Namespace(name="local")
        result = explain_command(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "Connection: local" in captured.out
        assert "Example YAML:" in captured.out

    def test_explain_unknown(self, capsys):
        """Explain unknown name returns error."""
        args = Namespace(name="nonexistent_thing")
        result = explain_command(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Unknown" in captured.out
        assert "Try one of" in captured.out


class TestCLIIntegration:
    """Test CLI integration with main."""

    def test_cli_list_transformers(self, capsys):
        """CLI list transformers via main."""
        from odibi.cli.main import main

        with patch.object(sys, "argv", ["odibi", "list", "transformers"]):
            result = main()

        assert result == 0
        captured = capsys.readouterr()
        assert "Transformers" in captured.out

    def test_cli_list_patterns_json(self, capsys):
        """CLI list patterns with JSON format via main."""
        from odibi.cli.main import main

        with patch.object(sys, "argv", ["odibi", "list", "patterns", "--format", "json"]):
            result = main()

        assert result == 0
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert len(data) == 6

    def test_cli_explain(self, capsys):
        """CLI explain via main."""
        from odibi.cli.main import main

        with patch.object(sys, "argv", ["odibi", "explain", "scd2"]):
            result = main()

        assert result == 0
        captured = capsys.readouterr()
        assert "scd2" in captured.out.lower()
