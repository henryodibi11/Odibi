"""Tests for extension loading utilities."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from odibi.utils import extensions
from odibi.utils.extensions import load_extensions


@pytest.fixture(autouse=True)
def _cleanup_sys(tmp_path):
    """Remove test artifacts from sys.path and sys.modules after each test."""
    original_path = sys.path.copy()
    original_modules = set(sys.modules.keys())
    yield
    sys.path[:] = original_path
    for key in list(sys.modules.keys()):
        if key not in original_modules:
            del sys.modules[key]


@patch("odibi.utils.extensions.logger")
class TestLoadExtensions:
    def test_loads_transforms(self, mock_logger, tmp_path):
        (tmp_path / "transforms.py").write_text("LOADED = True\n")
        load_extensions(tmp_path)
        assert "transforms" in sys.modules
        assert sys.modules["transforms"].LOADED is True
        mock_logger.info.assert_called_once()

    def test_loads_plugins(self, mock_logger, tmp_path):
        (tmp_path / "plugins.py").write_text("PLUGIN_LOADED = True\n")
        load_extensions(tmp_path)
        assert "plugins" in sys.modules
        assert sys.modules["plugins"].PLUGIN_LOADED is True

    def test_loads_both(self, mock_logger, tmp_path):
        (tmp_path / "transforms.py").write_text("T = 1\n")
        (tmp_path / "plugins.py").write_text("P = 2\n")
        load_extensions(tmp_path)
        assert sys.modules["transforms"].T == 1
        assert sys.modules["plugins"].P == 2
        assert mock_logger.info.call_count == 2

    def test_no_files_no_error(self, mock_logger, tmp_path):
        load_extensions(tmp_path)
        assert "transforms" not in sys.modules
        assert "plugins" not in sys.modules
        mock_logger.info.assert_not_called()
        mock_logger.warning.assert_not_called()

    def test_import_error_logged_not_raised(self, mock_logger, tmp_path):
        (tmp_path / "transforms.py").write_text("def bad(\n")
        load_extensions(tmp_path)
        mock_logger.warning.assert_called_once()
        args = mock_logger.warning.call_args
        assert "Failed to load transforms.py" in args[0][0]

    def test_adds_path_to_sys_path(self, mock_logger, tmp_path):
        load_extensions(tmp_path)
        assert str(tmp_path) in sys.path

    def test_no_duplicate_sys_path(self, mock_logger, tmp_path):
        load_extensions(tmp_path)
        load_extensions(tmp_path)
        assert sys.path.count(str(tmp_path)) == 1


class TestLoadExtensionsEdgeCases:
    """Comprehensive edge case tests for load_extensions function."""

    def test_load_extensions_with_valid_transforms_py(self, tmp_path):
        """Test loading valid transforms.py file."""
        transforms_file = tmp_path / "transforms.py"
        transforms_file.write_text(
            """
def custom_transform(data):
    return data * 2
"""
        )

        with patch("odibi.utils.extensions.logger") as mock_logger:
            extensions.load_extensions(tmp_path)
            mock_logger.info.assert_called()
            assert any("Loaded extension" in str(call) for call in mock_logger.info.call_args_list)

    def test_load_extensions_with_valid_plugins_py(self, tmp_path):
        """Test loading valid plugins.py file."""
        plugins_file = tmp_path / "plugins.py"
        plugins_file.write_text(
            """
class CustomPlugin:
    def run(self):
        return "plugin running"
"""
        )

        with patch("odibi.utils.extensions.logger") as mock_logger:
            extensions.load_extensions(tmp_path)
            mock_logger.info.assert_called()
            assert any("Loaded extension" in str(call) for call in mock_logger.info.call_args_list)

    def test_load_extensions_with_both_files(self, tmp_path):
        """Test loading both transforms.py and plugins.py."""
        transforms_file = tmp_path / "transforms.py"
        transforms_file.write_text("# transforms")

        plugins_file = tmp_path / "plugins.py"
        plugins_file.write_text("# plugins")

        with patch("odibi.utils.extensions.logger") as mock_logger:
            extensions.load_extensions(tmp_path)
            # Should log twice, once for each file
            assert mock_logger.info.call_count >= 2

    def test_load_extensions_with_no_files(self, tmp_path):
        """Test loading when neither file exists."""
        with patch("odibi.utils.extensions.logger") as mock_logger:
            extensions.load_extensions(tmp_path)
            # Should not call logger.info since no files to load
            mock_logger.info.assert_not_called()

    def test_load_extensions_with_invalid_python(self, tmp_path):
        """Test loading file with invalid Python syntax."""
        transforms_file = tmp_path / "transforms.py"
        transforms_file.write_text("this is not valid python syntax {{{")

        with patch("odibi.utils.extensions.logger") as mock_logger:
            extensions.load_extensions(tmp_path)
            # Should log a warning about failed load
            mock_logger.warning.assert_called()
            assert any("Failed to load" in str(call) for call in mock_logger.warning.call_args_list)

    def test_load_extensions_with_import_error(self, tmp_path):
        """Test loading file that has import errors."""
        transforms_file = tmp_path / "transforms.py"
        transforms_file.write_text("import nonexistent_module_xyz123")

        with patch("odibi.utils.extensions.logger") as mock_logger:
            extensions.load_extensions(tmp_path)
            # Should log a warning about failed load
            mock_logger.warning.assert_called()

    def test_load_extensions_adds_path_to_sys_path(self, tmp_path):
        """Test that the path is added to sys.path."""
        extensions.load_extensions(tmp_path)
        assert str(tmp_path) in sys.path

    def test_load_extensions_does_not_duplicate_path(self, tmp_path):
        """Test that the same path is not added to sys.path multiple times."""
        # Add the path first
        sys.path.append(str(tmp_path))
        initial_count = sys.path.count(str(tmp_path))

        extensions.load_extensions(tmp_path)
        final_count = sys.path.count(str(tmp_path))

        # Should not increase the count
        assert final_count == initial_count

    def test_load_extensions_module_added_to_sys_modules(self, tmp_path):
        """Test that loaded modules are added to sys.modules."""
        transforms_file = tmp_path / "transforms.py"
        transforms_file.write_text("# simple module")

        extensions.load_extensions(tmp_path)

        # Should have added 'transforms' to sys.modules
        assert "transforms" in sys.modules

    def test_load_extensions_with_runtime_error(self, tmp_path):
        """Test loading file that raises error during execution."""
        transforms_file = tmp_path / "transforms.py"
        transforms_file.write_text("raise RuntimeError('Test error')")

        with patch("odibi.utils.extensions.logger") as mock_logger:
            extensions.load_extensions(tmp_path)
            # Should log a warning about failed load
            mock_logger.warning.assert_called()
            warning_calls = mock_logger.warning.call_args_list
            assert any(
                "Failed to load" in str(call) and "transforms.py" in str(call)
                for call in warning_calls
            )

    def test_load_extensions_with_empty_file(self, tmp_path):
        """Test loading empty Python file."""
        transforms_file = tmp_path / "transforms.py"
        transforms_file.write_text("")

        with patch("odibi.utils.extensions.logger") as mock_logger:
            extensions.load_extensions(tmp_path)
            # Should successfully load empty file
            mock_logger.info.assert_called()
            assert any("Loaded extension" in str(call) for call in mock_logger.info.call_args_list)

    def test_load_extensions_with_special_characters_in_path(self, tmp_path):
        """Test loading from path with special characters."""
        special_dir = tmp_path / "test-dir_with.special"
        special_dir.mkdir()

        transforms_file = special_dir / "transforms.py"
        transforms_file.write_text("# test")

        with patch("odibi.utils.extensions.logger") as mock_logger:
            extensions.load_extensions(special_dir)
            mock_logger.info.assert_called()

    def test_load_extensions_with_complex_module(self, tmp_path):
        """Test loading module with imports and functions."""
        transforms_file = tmp_path / "transforms.py"
        transforms_file.write_text(
            """
import sys
import os

def process_data(x):
    return x + 1

class DataTransformer:
    def __init__(self):
        self.version = "1.0"
"""
        )

        with patch("odibi.utils.extensions.logger") as mock_logger:
            extensions.load_extensions(tmp_path)
            mock_logger.info.assert_called()

            # Verify the module was loaded and is accessible
            assert "transforms" in sys.modules

    def test_load_extensions_preserves_original_sys_path_order(self, tmp_path):
        """Test that load_extensions appends to sys.path, not prepends."""
        original_length = len(sys.path)

        extensions.load_extensions(tmp_path)
        # Path should be added at the end
        if str(tmp_path) not in sys.path[:original_length]:
            assert sys.path.index(str(tmp_path)) >= original_length

    def test_load_extensions_logs_with_exc_info(self, tmp_path):
        """Test that warnings include exception info."""
        transforms_file = tmp_path / "transforms.py"
        transforms_file.write_text("raise ValueError('test error')")

        with patch("odibi.utils.extensions.logger") as mock_logger:
            extensions.load_extensions(tmp_path)
            # Check that warning was called with exc_info=True
            mock_logger.warning.assert_called()
            call_kwargs = mock_logger.warning.call_args[1]
            assert call_kwargs.get("exc_info") is True

    def test_load_extensions_handles_none_spec(self, tmp_path):
        """Test handling when spec_from_file_location returns None."""
        transforms_file = tmp_path / "transforms.py"
        transforms_file.write_text("# test")

        with patch("importlib.util.spec_from_file_location", return_value=None):
            with patch("odibi.utils.extensions.logger") as mock_logger:
                extensions.load_extensions(tmp_path)
                # Should not attempt to load if spec is None
                # Should not log success
                for call in mock_logger.info.call_args_list:
                    assert "Loaded extension" not in str(call)

    def test_load_extensions_handles_none_loader(self, tmp_path):
        """Test handling when spec.loader is None."""
        transforms_file = tmp_path / "transforms.py"
        transforms_file.write_text("# test")

        mock_spec = MagicMock()
        mock_spec.loader = None

        with patch("importlib.util.spec_from_file_location", return_value=mock_spec):
            with patch("odibi.utils.extensions.logger") as mock_logger:
                extensions.load_extensions(tmp_path)
                # Should not attempt to load if loader is None
                for call in mock_logger.info.call_args_list:
                    assert "Loaded extension" not in str(call)

    def test_load_extensions_with_pathlib_path(self, tmp_path):
        """Test that function accepts pathlib.Path objects."""
        transforms_file = tmp_path / "transforms.py"
        transforms_file.write_text("# test")

        # Explicitly pass Path object
        path_obj = Path(tmp_path)
        with patch("odibi.utils.extensions.logger") as mock_logger:
            extensions.load_extensions(path_obj)
            mock_logger.info.assert_called()
