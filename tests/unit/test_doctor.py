"""Tests for odibi.doctor module."""

import os


from odibi.doctor import diagnose_path, doctor


def test_doctor_returns_expected_structure():
    """Test that doctor() returns expected structure."""
    result = doctor()

    assert "status" in result
    assert result["status"] in ["healthy", "warnings", "errors"]
    assert "python_version" in result
    assert "packages" in result
    assert "environment" in result
    assert "project_loaded" in result
    assert "connections" in result
    assert "issues" in result
    assert "suggestions" in result

    assert isinstance(result["packages"], dict)
    assert isinstance(result["environment"], dict)
    assert isinstance(result["issues"], list)
    assert isinstance(result["suggestions"], list)


def test_doctor_checks_python_version():
    """Test that doctor() includes Python version."""
    result = doctor()
    assert result["python_version"]
    assert "." in result["python_version"]


def test_doctor_checks_required_packages():
    """Test that doctor() checks for required packages."""
    result = doctor()

    assert "pandas" in result["packages"]
    assert "pydantic" in result["packages"]
    assert "pyyaml" in result["packages"]


def test_doctor_checks_environment_vars():
    """Test that doctor() checks environment variables."""
    result = doctor()

    assert "ODIBI_CONFIG" in result["environment"]
    assert "ODIBI_PROJECTS_DIR" in result["environment"]
    assert "cwd" in result["environment"]


def test_doctor_with_valid_config(tmp_path):
    """Test doctor() with a valid config file."""
    config_file = tmp_path / "test_project.yaml"
    config_file.write_text("""
project:
  name: test_project
  engine: pandas

connections:
  local:
    type: local
    base_path: /tmp
""")

    os.environ["ODIBI_CONFIG"] = str(config_file)

    try:
        result = doctor()
        assert result["environment"]["ODIBI_CONFIG"] == str(config_file)
    finally:
        os.environ.pop("ODIBI_CONFIG", None)


def test_diagnose_path_existing_file(tmp_path):
    """Test diagnose_path() with existing file."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("Hello, World!")

    result = diagnose_path(str(test_file))

    assert result["exists"] is True
    assert result["is_file"] is True
    assert result["is_directory"] is False
    assert result["readable"] is True
    assert result["size"] == len("Hello, World!")
    assert result["error"] is None


def test_diagnose_path_existing_directory(tmp_path):
    """Test diagnose_path() with existing directory."""
    (tmp_path / "file1.txt").write_text("test1")
    (tmp_path / "file2.txt").write_text("test2")

    result = diagnose_path(str(tmp_path))

    assert result["exists"] is True
    assert result["is_file"] is False
    assert result["is_directory"] is True
    assert result["readable"] is True
    assert len(result["contents"]) == 2


def test_diagnose_path_nonexistent():
    """Test diagnose_path() with nonexistent path."""
    result = diagnose_path("/nonexistent/path/to/nowhere")

    assert result["exists"] is False
    assert result["is_file"] is False
    assert result["is_directory"] is False


def test_diagnose_path_detects_format(tmp_path):
    """Test that diagnose_path() detects file format."""
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("a,b,c\n1,2,3")

    result = diagnose_path(str(csv_file))

    assert result["format"] == "csv"
    assert result["suffix"] == ".csv"


def test_pipeline_manager_doctor_method():
    """Test that PipelineManager.doctor() delegates correctly."""
    from unittest.mock import MagicMock, patch
    from odibi.pipeline import PipelineManager

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

        pm = MagicMock(spec=PipelineManager)
        pm.doctor = PipelineManager.doctor.__get__(pm, PipelineManager)
        result = pm.doctor()

        assert "status" in result
        assert "python_version" in result
        mock_doctor.assert_called_once()
