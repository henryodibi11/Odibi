"""Tests for the folder picker component.

Note: These tests only test the pure Python functions, not the Gradio components.
"""

import importlib.util
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

sys.modules["gradio"] = MagicMock()

folder_picker_path = Path(__file__).parent.parent / "components" / "folder_picker.py"
spec = importlib.util.spec_from_file_location("folder_picker", folder_picker_path)
folder_picker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(folder_picker)

ProjectState = folder_picker.ProjectState
get_project_info = folder_picker.get_project_info
validate_project_path = folder_picker.validate_project_path


class TestProjectState:
    """Tests for ProjectState class."""

    def test_initial_state(self):
        """Test initial empty state."""
        state = ProjectState()
        assert state.active_path == ""
        assert state.recent_projects == []

    def test_set_active_path(self):
        """Test setting active project."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state = ProjectState()
            state.set_active(tmpdir)

            assert state.active_path == str(Path(tmpdir).resolve())
            assert len(state.recent_projects) == 1
            assert state.recent_projects[0].path == str(Path(tmpdir).resolve())

    def test_recent_projects_order(self):
        """Test that recent projects are ordered by last used."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dir1 = Path(tmpdir) / "project1"
            dir2 = Path(tmpdir) / "project2"
            dir1.mkdir()
            dir2.mkdir()

            state = ProjectState()
            state.set_active(str(dir1))
            state.set_active(str(dir2))
            state.set_active(str(dir1))

            assert len(state.recent_projects) == 2
            assert state.recent_projects[0].path == str(dir1.resolve())
            assert state.recent_projects[1].path == str(dir2.resolve())

    def test_max_recent_limit(self):
        """Test that recent projects are limited."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state = ProjectState(max_recent=3)

            for i in range(5):
                d = Path(tmpdir) / f"project{i}"
                d.mkdir()
                state.set_active(str(d))

            assert len(state.recent_projects) == 3

    def test_get_recent_choices(self):
        """Test getting recent projects as dropdown choices."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state = ProjectState()
            state.set_active(tmpdir)

            choices = state.get_recent_choices()
            assert len(choices) == 1
            assert choices[0][1] == str(Path(tmpdir).resolve())

    def test_mark_indexed(self):
        """Test marking a project as indexed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state = ProjectState()
            state.set_active(tmpdir)
            state.mark_indexed(str(Path(tmpdir).resolve()))

            assert state.recent_projects[0].indexed is True


class TestValidateProjectPath:
    """Tests for path validation."""

    def test_empty_path(self):
        """Test empty path validation."""
        is_valid, msg = validate_project_path("")
        assert is_valid is False
        assert "enter a path" in msg.lower()

    def test_nonexistent_path(self):
        """Test non-existent path."""
        is_valid, msg = validate_project_path("/nonexistent/path/12345")
        assert is_valid is False
        assert "does not exist" in msg

    def test_valid_directory(self):
        """Test valid directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            is_valid, msg = validate_project_path(tmpdir)
            assert is_valid is True

    def test_git_repo_detection(self):
        """Test Git repository detection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / ".git").mkdir()
            is_valid, msg = validate_project_path(tmpdir)
            assert is_valid is True
            assert "Git repository" in msg

    def test_python_project_detection(self):
        """Test Python project detection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "main.py").write_text("print('hello')")
            is_valid, msg = validate_project_path(tmpdir)
            assert is_valid is True
            assert "Python project" in msg


class TestGetProjectInfo:
    """Tests for project info extraction."""

    def test_nonexistent_project(self):
        """Test info for non-existent path."""
        info = get_project_info("/nonexistent/path")
        assert info["exists"] is False

    def test_basic_project_info(self):
        """Test basic project info."""
        with tempfile.TemporaryDirectory() as tmpdir:
            info = get_project_info(tmpdir)
            assert info["exists"] is True
            assert info["path"] == str(Path(tmpdir))
            assert info["name"] == Path(tmpdir).name

    def test_git_repo_info(self):
        """Test Git repository info."""
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / ".git").mkdir()
            info = get_project_info(tmpdir)
            assert info["is_git"] is True

    def test_language_detection(self):
        """Test programming language detection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "main.py").write_text("print('hello')")
            (Path(tmpdir) / "app.js").write_text("console.log('hello')")

            info = get_project_info(tmpdir)
            assert "Python" in info["languages"]
            assert "JavaScript" in info["languages"]
