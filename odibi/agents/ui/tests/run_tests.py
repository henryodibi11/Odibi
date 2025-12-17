#!/usr/bin/env python
"""Run UI tests directly without pytest package discovery issues.

Usage:
    python run_tests.py          # Run all tests
    python run_tests.py -v       # Verbose mode
    python run_tests.py -k test_name  # Run specific test
"""

import sys
from pathlib import Path

import pytest

ui_path = str(Path(__file__).parent.parent)
if ui_path not in sys.path:
    sys.path.insert(0, ui_path)

if __name__ == "__main__":
    test_dir = Path(__file__).parent

    args = [
        str(test_dir / "test_config.py"),
        str(test_dir / "test_tools.py"),
        "--tb=short",
    ] + sys.argv[1:]

    sys.exit(pytest.main(args))
