"""Tests for extension loading utilities."""

import sys
from unittest.mock import patch

import pytest

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
