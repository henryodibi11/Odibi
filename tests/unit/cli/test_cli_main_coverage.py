"""Tests for odibi.cli.main — core CLI functions."""

import json

import pytest
from unittest.mock import MagicMock, patch

from odibi.cli.main import (
    format_output,
    print_table,
    cmd_validate,
    cmd_doctor,
    cmd_doctor_path,
    main,
)
from odibi import __version__


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_args(**overrides):
    args = MagicMock()
    args.format = "auto"
    for k, v in overrides.items():
        setattr(args, k, v)
    return args


# ---------------------------------------------------------------------------
# format_output
# ---------------------------------------------------------------------------


class TestFormatOutput:
    def test_json_format_returns_json_string(self):
        data = {"key": "value"}
        result = format_output(data, "json")
        assert json.loads(result) == data

    def test_auto_format_returns_str(self):
        data = {"key": "value"}
        result = format_output(data, "auto")
        assert result == str(data)

    def test_json_format_with_dict(self):
        data = {"a": 1, "b": 2}
        result = format_output(data, "json")
        parsed = json.loads(result)
        assert parsed == {"a": 1, "b": 2}

    def test_json_format_with_list(self):
        data = [1, 2, 3]
        result = format_output(data, "json")
        parsed = json.loads(result)
        assert parsed == [1, 2, 3]

    def test_auto_format_with_dict(self):
        data = {"x": 10}
        result = format_output(data, "auto")
        assert "x" in result
        assert "10" in result

    def test_auto_format_with_list(self):
        data = [1, 2, 3]
        result = format_output(data, "auto")
        assert result == "[1, 2, 3]"

    def test_json_format_indented(self):
        data = {"k": "v"}
        result = format_output(data, "json")
        assert "\n" in result  # indent=2 produces newlines

    def test_auto_format_with_string_data(self):
        result = format_output("hello", "auto")
        assert result == "hello"


# ---------------------------------------------------------------------------
# print_table
# ---------------------------------------------------------------------------


class TestPrintTable:
    def test_rich_table_prints_output(self, capsys):
        data = {"Name": "Alice", "Age": "30"}
        with patch("odibi.cli.main.RICH_AVAILABLE", True):
            print_table(data, title="Test Title")
        captured = capsys.readouterr()
        assert "Alice" in captured.out
        assert "30" in captured.out

    def test_plain_table_prints_output(self, capsys):
        data = {"Name": "Alice", "Age": "30"}
        with patch("odibi.cli.main.RICH_AVAILABLE", False):
            print_table(data, title="People")
        captured = capsys.readouterr()
        assert "People" in captured.out
        assert "Alice" in captured.out
        assert "30" in captured.out

    def test_plain_table_with_title(self, capsys):
        data = {"k": "v"}
        with patch("odibi.cli.main.RICH_AVAILABLE", False):
            print_table(data, title="My Title")
        captured = capsys.readouterr()
        assert "My Title" in captured.out
        assert "=" in captured.out

    def test_plain_table_without_title(self, capsys):
        data = {"k": "v"}
        with patch("odibi.cli.main.RICH_AVAILABLE", False):
            print_table(data, title="")
        captured = capsys.readouterr()
        assert "=" not in captured.out
        assert "k" in captured.out

    def test_rich_table_with_title(self, capsys):
        data = {"Key1": "Val1"}
        with patch("odibi.cli.main.RICH_AVAILABLE", True):
            print_table(data, title="Rich Title")
        captured = capsys.readouterr()
        assert "Val1" in captured.out

    def test_plain_table_multiple_rows(self, capsys):
        data = {"a": "1", "b": "2", "c": "3"}
        with patch("odibi.cli.main.RICH_AVAILABLE", False):
            print_table(data)
        captured = capsys.readouterr()
        assert "a" in captured.out
        assert "b" in captured.out
        assert "c" in captured.out


# ---------------------------------------------------------------------------
# cmd_validate
# ---------------------------------------------------------------------------


class TestCmdValidate:
    def test_file_not_found_returns_1(self, capsys):
        args = _make_args(file="nonexistent_file_xyz.yaml", format="auto")
        result = cmd_validate(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "File not found" in captured.err

    def test_valid_yaml_returns_0(self, tmp_path, capsys):
        yaml_file = tmp_path / "valid.yaml"
        yaml_file.write_text("key: value")
        args = _make_args(file=str(yaml_file), format="auto")
        with patch("odibi.validate.pipeline.validate_yaml", return_value={"valid": True}):
            result = cmd_validate(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "valid" in captured.out.lower() or "✓" in captured.out

    def test_invalid_yaml_returns_1(self, tmp_path, capsys):
        yaml_file = tmp_path / "invalid.yaml"
        yaml_file.write_text("bad: content")
        args = _make_args(file=str(yaml_file), format="auto")
        mock_result = {
            "valid": False,
            "errors": [{"field_path": "x", "message": "bad"}],
        }
        with patch("odibi.validate.pipeline.validate_yaml", return_value=mock_result):
            result = cmd_validate(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "bad" in captured.out

    def test_json_format_output(self, tmp_path, capsys):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("key: value")
        args = _make_args(file=str(yaml_file), format="json")
        mock_result = {"valid": True}
        with patch("odibi.validate.pipeline.validate_yaml", return_value=mock_result):
            cmd_validate(args)
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert parsed["valid"] is True

    def test_exception_returns_1(self, tmp_path, capsys):
        yaml_file = tmp_path / "err.yaml"
        yaml_file.write_text("key: value")
        args = _make_args(file=str(yaml_file), format="auto")
        with patch("odibi.validate.pipeline.validate_yaml", side_effect=RuntimeError("boom")):
            result = cmd_validate(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "boom" in captured.err

    def test_invalid_yaml_shows_field_path(self, tmp_path, capsys):
        yaml_file = tmp_path / "bad2.yaml"
        yaml_file.write_text("a: b")
        args = _make_args(file=str(yaml_file), format="auto")
        mock_result = {
            "valid": False,
            "errors": [{"field_path": "connections.db", "message": "missing"}],
        }
        with patch("odibi.validate.pipeline.validate_yaml", return_value=mock_result):
            result = cmd_validate(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "connections.db" in captured.out


# ---------------------------------------------------------------------------
# cmd_doctor
# ---------------------------------------------------------------------------


class TestCmdDoctor:
    @staticmethod
    def _healthy_result(**overrides):
        result = {
            "status": "healthy",
            "python_version": "3.12.0",
            "packages": {"odibi": "1.0.0"},
            "environment": {"ODIBI_HOME": "/home/user"},
        }
        result.update(overrides)
        return result

    def test_healthy_returns_0(self, capsys):
        args = _make_args(format="auto")
        with patch("odibi.doctor.doctor", return_value=self._healthy_result()):
            result = cmd_doctor(args)
        assert result == 0

    def test_unhealthy_returns_1(self, capsys):
        args = _make_args(format="auto")
        with patch("odibi.doctor.doctor", return_value=self._healthy_result(status="unhealthy")):
            result = cmd_doctor(args)
        assert result == 1

    def test_json_format_returns_0(self, capsys):
        args = _make_args(format="json")
        with patch("odibi.doctor.doctor", return_value=self._healthy_result()):
            result = cmd_doctor(args)
        assert result == 0
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert parsed["status"] == "healthy"

    def test_with_issues_and_suggestions(self, capsys):
        args = _make_args(format="auto")
        doctor_result = self._healthy_result(
            issues=[{"severity": "warning", "message": "Low disk", "fix": "Free space"}],
            suggestions=["Upgrade Python"],
        )
        with patch("odibi.doctor.doctor", return_value=doctor_result):
            result = cmd_doctor(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "Low disk" in captured.out
        assert "Free space" in captured.out
        assert "Upgrade Python" in captured.out

    def test_exception_returns_1(self, capsys):
        args = _make_args(format="auto")
        with patch("odibi.doctor.doctor", side_effect=RuntimeError("fail")):
            result = cmd_doctor(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "fail" in captured.err

    def test_auto_format_prints_diagnostics_header(self, capsys):
        args = _make_args(format="auto")
        with patch("odibi.doctor.doctor", return_value=self._healthy_result()):
            cmd_doctor(args)
        captured = capsys.readouterr()
        assert "Diagnostics" in captured.out

    def test_project_loaded_shows_connections(self, capsys):
        args = _make_args(format="auto")
        doctor_result = self._healthy_result(
            project_loaded=True,
            connections={"db": "connected"},
        )
        with patch("odibi.doctor.doctor", return_value=doctor_result):
            result = cmd_doctor(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "Project loaded" in captured.out or "✓" in captured.out

    def test_no_project_loaded(self, capsys):
        args = _make_args(format="auto")
        doctor_result = self._healthy_result(project_loaded=False)
        with patch("odibi.doctor.doctor", return_value=doctor_result):
            result = cmd_doctor(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "No project loaded" in captured.out or "✗" in captured.out


# ---------------------------------------------------------------------------
# cmd_doctor_path
# ---------------------------------------------------------------------------


class TestCmdDoctorPath:
    def test_path_exists_is_file_returns_0(self, capsys):
        args = _make_args(path="/some/file.txt", format="auto")
        mock_result = {
            "input_path": "/some/file.txt",
            "resolved_path": "/some/file.txt",
            "exists": True,
            "is_directory": False,
            "is_file": True,
            "readable": True,
            "writable": True,
            "size": 1024,
            "format": "text",
        }
        with patch("odibi.doctor.diagnose_path", return_value=mock_result):
            result = cmd_doctor_path(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "File" in captured.out
        assert "1024" in captured.out

    def test_path_not_exists_returns_0(self, capsys):
        args = _make_args(path="/missing/path", format="auto")
        mock_result = {
            "input_path": "/missing/path",
            "resolved_path": "/missing/path",
            "exists": False,
        }
        with patch("odibi.doctor.diagnose_path", return_value=mock_result):
            result = cmd_doctor_path(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "False" in captured.out

    def test_error_in_result_returns_1(self, capsys):
        args = _make_args(path="/bad/path", format="auto")
        mock_result = {
            "input_path": "/bad/path",
            "resolved_path": "/bad/path",
            "exists": True,
            "is_directory": False,
            "is_file": True,
            "readable": False,
            "writable": False,
            "error": "Permission denied",
        }
        with patch("odibi.doctor.diagnose_path", return_value=mock_result):
            result = cmd_doctor_path(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "Permission denied" in captured.out

    def test_directory_with_contents(self, capsys):
        args = _make_args(path="/some/dir", format="auto")
        mock_result = {
            "input_path": "/some/dir",
            "resolved_path": "/some/dir",
            "exists": True,
            "is_directory": True,
            "is_file": False,
            "readable": True,
            "writable": True,
            "contents": [
                {"name": "file1.txt", "is_dir": False, "size": 100},
                {"name": "subdir", "is_dir": True},
            ],
        }
        with patch("odibi.doctor.diagnose_path", return_value=mock_result):
            result = cmd_doctor_path(args)
        assert result == 0
        captured = capsys.readouterr()
        assert "file1.txt" in captured.out
        assert "subdir" in captured.out
        assert "DIR" in captured.out
        assert "FILE" in captured.out

    def test_json_format_returns_0(self, capsys):
        args = _make_args(path="/some/path", format="json")
        mock_result = {
            "input_path": "/some/path",
            "resolved_path": "/some/path",
            "exists": True,
            "is_directory": False,
            "is_file": True,
        }
        with patch("odibi.doctor.diagnose_path", return_value=mock_result):
            result = cmd_doctor_path(args)
        assert result == 0
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert parsed["input_path"] == "/some/path"

    def test_exception_returns_1(self, capsys):
        args = _make_args(path="/err", format="auto")
        with patch("odibi.doctor.diagnose_path", side_effect=RuntimeError("oops")):
            result = cmd_doctor_path(args)
        assert result == 1
        captured = capsys.readouterr()
        assert "oops" in captured.err


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


class TestMain:
    def test_no_command_returns_1(self):
        with patch("sys.argv", ["odibi"]):
            result = main()
        assert result == 1

    def test_version_flag(self, capsys):
        with patch("sys.argv", ["odibi", "--version"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "odibi" in captured.out

    def test_version_contains_version_string(self, capsys):
        with patch("sys.argv", ["odibi", "--version"]):
            with pytest.raises(SystemExit):
                main()
        captured = capsys.readouterr()
        assert __version__ in captured.out
