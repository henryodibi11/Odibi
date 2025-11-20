"""Tests for CLI module (Phase 2.5)."""

import pytest
import sys
from unittest.mock import Mock, patch
from odibi.cli import main
from odibi.cli.run import run_command
from odibi.cli.validate import validate_command


class TestCLIImports:
    """Test that CLI modules are importable."""

    def test_cli_main_import(self):
        """CLI main should be importable."""
        assert main is not None
        assert callable(main)

    def test_run_command_import(self):
        """Run command should be importable."""
        assert run_command is not None
        assert callable(run_command)

    def test_validate_command_import(self):
        """Validate command should be importable."""
        assert validate_command is not None
        assert callable(validate_command)


class TestCLIMain:
    """Test CLI main entry point."""

    def test_cli_main_no_args_shows_help(self, capsys):
        """CLI should show help when no args provided."""
        with patch.object(sys, "argv", ["odibi"]):
            result = main()
            assert result == 1  # Returns 1 (failure) when no command

        captured = capsys.readouterr()
        output = captured.out + captured.err
        assert "usage:" in output.lower() or "odibi" in output.lower()

    def test_cli_main_help_flag(self, capsys):
        """CLI should show help with --help flag."""
        with patch.object(sys, "argv", ["odibi", "--help"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

        captured = capsys.readouterr()
        output = captured.out + captured.err
        assert "run" in output.lower()
        assert "validate" in output.lower()

    def test_cli_main_invalid_command(self, capsys):
        """CLI should show help with invalid command."""
        with patch.object(sys, "argv", ["odibi", "invalid"]):
            with pytest.raises(SystemExit):
                main()

        captured = capsys.readouterr()
        output = captured.out + captured.err
        assert "invalid" in output.lower() or "usage" in output.lower()


class TestRunCommand:
    """Test run command."""

    def test_run_command_missing_file(self):
        """Run command should fail gracefully with missing file."""
        args = Mock()
        args.config = "nonexistent.yaml"

        result = run_command(args)
        assert result == 1  # Failure exit code

    def test_run_command_with_mock_manager(self):
        """Run command should work with valid config."""
        args = Mock()
        args.config = "test.yaml"
        args.dry_run = False

        mock_result = Mock()
        mock_result.failed = []

        mock_manager = Mock()
        mock_manager.run.return_value = mock_result

        with patch("odibi.cli.run.PipelineManager") as MockManager:
            MockManager.from_yaml.return_value = mock_manager
            result = run_command(args)

            assert result == 0  # Success
            MockManager.from_yaml.assert_called_once_with("test.yaml")
            mock_manager.run.assert_called_once()


class TestValidateCommand:
    """Test validate command."""

    def test_validate_command_missing_file(self):
        """Validate command should fail gracefully with missing file."""
        args = Mock()
        args.config = "nonexistent.yaml"

        result = validate_command(args)
        assert result == 1  # Failure exit code

    def test_validate_command_invalid_yaml(self, tmp_path):
        """Validate command should fail with invalid YAML."""
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text("invalid: yaml: content: ][")

        args = Mock()
        args.config = str(config_file)

        result = validate_command(args)
        assert result == 1  # Failure exit code

    def test_validate_command_valid_config(self, tmp_path):
        """Validate command should succeed with valid config."""
        config_file = tmp_path / "valid.yaml"
        config_file.write_text(
            """
project: Test Project
engine: pandas
connections:
  local:
    type: local
    base_path: ./data
story:
  connection: local
  path: stories/
pipelines:
  - pipeline: test
    nodes:
      - name: test_node
        read:
          connection: local
          path: test.csv
          format: csv
"""
        )

        args = Mock()
        args.config = str(config_file)

        result = validate_command(args)
        assert result == 0  # Success
