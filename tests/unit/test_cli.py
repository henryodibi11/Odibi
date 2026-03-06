"""Tests for odibi.cli module."""

import sys
from unittest.mock import MagicMock, patch

import pytest

from odibi.cli.main import main


def test_cli_no_args(capsys):
    """Test CLI with no arguments shows help."""
    with patch.object(sys, "argv", ["odibi"]):
        exit_code = main()

    assert exit_code == 1
    captured = capsys.readouterr()
    assert "odibi" in captured.out or "usage" in captured.out.lower()


def test_cli_doctor_command():
    """Test CLI doctor command is callable."""
    with patch.object(sys, "argv", ["odibi", "doctor", "--format", "json"]):
        with patch("odibi.doctor.doctor") as mock_doctor:
            mock_doctor.return_value = {
                "status": "healthy",
                "python_version": "3.9.0",
                "packages": {},
                "environment": {},
                "project_loaded": False,
                "connections": {},
                "issues": [],
                "suggestions": [],
            }
            exit_code = main()

    assert exit_code == 0
    mock_doctor.assert_called_once()


def test_cli_doctor_path_command(tmp_path):
    """Test CLI doctor-path command."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")

    with patch.object(sys, "argv", ["odibi", "doctor-path", str(test_file), "--format", "json"]):
        exit_code = main()

    assert exit_code == 0


def test_cli_validate_command_valid_file(tmp_path):
    """Test CLI validate command with valid file."""
    yaml_file = tmp_path / "test.yaml"
    yaml_file.write_text("""
project:
  name: test_project
  engine: pandas

connections:
  local:
    type: local
    base_path: /tmp

pipelines:
  test_pipeline:
    nodes:
      - name: test_node
        type: dimension
        source:
          connection: local
          path: test.csv
        target:
          connection: local
          path: output.parquet
""")

    with patch.object(sys, "argv", ["odibi", "validate", str(yaml_file)]):
        with patch("odibi.pipeline.PipelineManager") as mock_pm:
            mock_instance = MagicMock()
            mock_instance.validate_yaml.return_value = {"valid": True, "errors": []}
            mock_pm.return_value = mock_instance

            exit_code = main()

    assert exit_code == 0


def test_cli_validate_command_invalid_file(tmp_path):
    """Test CLI validate command with invalid file."""
    yaml_file = tmp_path / "invalid.yaml"
    yaml_file.write_text("invalid: yaml: content:")

    with patch.object(sys, "argv", ["odibi", "validate", str(yaml_file)]):
        with patch("odibi.pipeline.PipelineManager") as mock_pm:
            mock_instance = MagicMock()
            mock_instance.validate_yaml.return_value = {
                "valid": False,
                "errors": [{"field_path": "project", "message": "Missing required field"}],
            }
            mock_pm.return_value = mock_instance

            exit_code = main()

    assert exit_code == 1


def test_cli_scaffold_project_command(tmp_path):
    """Test CLI scaffold project command."""
    with patch.object(sys, "argv", ["odibi", "scaffold", "project", "test_project"]):
        with patch("odibi.pipeline.PipelineManager") as mock_pm:
            mock_instance = MagicMock()
            mock_instance.scaffold_project.return_value = "project:\n  name: test_project"
            mock_pm.return_value = mock_instance

            with patch("pathlib.Path.exists", return_value=False):
                with patch("pathlib.Path.write_text"):
                    exit_code = main()

    assert exit_code == 0


def test_cli_version(capsys):
    """Test CLI --version flag."""
    with patch.object(sys, "argv", ["odibi", "--version"]):
        with pytest.raises(SystemExit) as exc_info:
            main()

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "2.18.0" in captured.out
