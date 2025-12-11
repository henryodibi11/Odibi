"""Tests for UI tools (file, search, shell)."""

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.file_tools import (
    FileResult,
    format_file_for_display,
    list_directory,
    read_file,
    write_file,
)
from tools.search_tools import (
    SearchResult,
    format_search_results,
    glob_files,
    grep_search,
)
from tools.shell_tools import (
    CommandResult,
    format_command_result,
    run_command,
)


class TestReadFile:
    """Tests for read_file function."""

    def test_read_existing_file(self):
        """Test reading an existing file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.py"
            path.write_text("line 1\nline 2\nline 3\n")

            result = read_file(str(path))
            assert result.success
            assert "1: line 1" in result.content
            assert "2: line 2" in result.content
            assert result.line_count == 3

    def test_read_missing_file(self):
        """Test reading a non-existent file."""
        result = read_file("/nonexistent/file.py")
        assert not result.success
        assert "not found" in result.error.lower()

    def test_read_with_line_range(self):
        """Test reading specific line range."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.py"
            content = "\n".join(f"line {i+1}" for i in range(10)) + "\n"
            path.write_text(content)

            result = read_file(str(path), start_line=3, end_line=5)
            assert result.success
            assert "3: line 3" in result.content
            assert "5: line 5" in result.content
            assert "1: line 1" not in result.content
            assert "6: line 6" not in result.content


class TestWriteFile:
    """Tests for write_file function."""

    def test_write_new_file(self):
        """Test writing a new file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "new_file.txt")
            result = write_file(path, "hello world")

            assert result.success
            assert Path(path).exists()
            assert Path(path).read_text() == "hello world"

    def test_write_creates_directories(self):
        """Test that write_file creates parent directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "nested", "dir", "file.txt")
            result = write_file(path, "content")

            assert result.success
            assert Path(path).exists()

    def test_overwrite_existing_file(self):
        """Test overwriting an existing file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.txt"
            path.write_text("old content")

            result = write_file(str(path), "new content")
            assert result.success
            assert path.read_text() == "new content"


class TestListDirectory:
    """Tests for list_directory function."""

    def test_list_existing_directory(self):
        """Test listing an existing directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "file1.py").touch()
            Path(tmpdir, "file2.txt").touch()
            Path(tmpdir, "subdir").mkdir()

            result = list_directory(tmpdir)
            assert result.success
            assert "file1.py" in result.content
            assert "file2.txt" in result.content
            assert "subdir/" in result.content

    def test_list_missing_directory(self):
        """Test listing a non-existent directory."""
        result = list_directory("/nonexistent/directory")
        assert not result.success
        assert "not found" in result.error.lower()

    def test_list_with_pattern(self):
        """Test listing with glob pattern."""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "file1.py").touch()
            Path(tmpdir, "file2.py").touch()
            Path(tmpdir, "file3.txt").touch()

            result = list_directory(tmpdir, pattern="*.py")
            assert result.success
            assert "file1.py" in result.content
            assert "file2.py" in result.content
            assert "file3.txt" not in result.content


class TestGrepSearch:
    """Tests for grep_search function."""

    def test_search_finds_matches(self):
        """Test that grep finds matching lines."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir, "test.py")
            file_path.write_text("def hello():\n    print('world')\n")

            result = grep_search("hello", tmpdir, file_pattern="*.py")
            assert result.success
            assert result.total_matches >= 1
            assert any("hello" in m.line_content for m in result.matches)

    def test_search_no_matches(self):
        """Test grep with no matches."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir, "test.py")
            file_path.write_text("def foo():\n    pass\n")

            result = grep_search("nonexistent_pattern", tmpdir, file_pattern="*.py")
            assert result.success
            assert result.total_matches == 0

    def test_search_case_insensitive(self):
        """Test case-insensitive search."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir, "test.py")
            file_path.write_text("HELLO world\n")

            result = grep_search("hello", tmpdir, case_sensitive=False)
            assert result.success
            assert result.total_matches >= 1

    def test_search_regex(self):
        """Test regex search."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir, "test.py")
            file_path.write_text("def test_foo():\ndef test_bar():\ndef other():\n")

            result = grep_search(r"def test_\w+\(\)", tmpdir, is_regex=True)
            assert result.success
            assert result.total_matches >= 2


class TestGlobFiles:
    """Tests for glob_files function."""

    def test_glob_finds_files(self):
        """Test glob finds matching files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "file1.py").touch()
            Path(tmpdir, "file2.py").touch()
            Path(tmpdir, "other.txt").touch()

            result = glob_files("*.py", tmpdir)
            assert result.success
            assert result.total_matches == 2

    def test_glob_recursive(self):
        """Test recursive glob pattern."""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "root.py").touch()
            subdir = Path(tmpdir, "subdir")
            subdir.mkdir()
            Path(subdir, "nested.py").touch()

            result = glob_files("**/*.py", tmpdir)
            assert result.success
            assert result.total_matches >= 2


class TestRunCommand:
    """Tests for run_command function."""

    def test_successful_command(self):
        """Test running a successful command."""
        result = run_command("echo hello")
        assert result.success
        assert "hello" in result.stdout.lower()
        assert result.return_code == 0

    def test_failed_command(self):
        """Test running a failing command."""
        result = run_command("exit 1", shell=True)
        assert not result.success
        assert result.return_code != 0

    def test_command_with_working_dir(self):
        """Test running command in specific directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_command("cd", working_dir=tmpdir)
            assert result.working_dir == tmpdir


class TestFormatters:
    """Tests for output formatters."""

    def test_format_file_for_display(self):
        """Test file result formatting."""
        result = FileResult(
            success=True,
            content="1: line 1\n2: line 2\n",
            path="/test/file.py",
            line_count=2,
        )
        formatted = format_file_for_display(result)
        assert "/test/file.py" in formatted
        assert "```" in formatted

    def test_format_file_error(self):
        """Test file error formatting."""
        result = FileResult(
            success=False,
            content="",
            path="/missing.py",
            error="File not found",
        )
        formatted = format_file_for_display(result)
        assert "Error" in formatted

    def test_format_search_results(self):
        """Test search result formatting."""
        result = SearchResult(
            success=True,
            matches=[],
            total_matches=0,
            files_searched=10,
        )
        formatted = format_search_results(result)
        assert "No matches" in formatted

    def test_format_command_result(self):
        """Test command result formatting."""
        result = CommandResult(
            success=True,
            stdout="test output",
            stderr="",
            return_code=0,
            command="echo test",
            working_dir="/tmp",
        )
        formatted = format_command_result(result)
        assert "echo test" in formatted
        assert "✅" in formatted
