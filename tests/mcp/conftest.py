"""Pytest configuration for MCP tests."""

import pytest


def pytest_configure(config):
    """Add markers for MCP tests."""
    config.addinivalue_line(
        "markers", "requires_openai: mark test as requiring openai package"
    )


def pytest_collection_modifyitems(config, items):
    """Skip OpenAI tests if openai not installed."""
    try:
        import openai  # noqa: F401
    except ImportError:
        skip_openai = pytest.mark.skip(reason="openai package not installed")
        for item in items:
            if "test_ai" in item.nodeid or "openai" in str(item.fspath).lower():
                item.add_marker(skip_openai)
