import pytest

from odibi.plugins import (
    _CONNECTION_FACTORIES,
    get_connection_factory,
    load_plugins,
    register_connection_factory,
)


@pytest.fixture(autouse=True)
def clean_registry():
    _CONNECTION_FACTORIES.clear()
    yield
    _CONNECTION_FACTORIES.clear()


def _dummy_factory(name, config):
    return {"name": name, "config": config}


def test_register_connection_factory_stores_factory():
    register_connection_factory("postgres", _dummy_factory)
    assert "postgres" in _CONNECTION_FACTORIES
    assert _CONNECTION_FACTORIES["postgres"] is _dummy_factory


def test_get_connection_factory_returns_registered():
    register_connection_factory("mysql", _dummy_factory)
    result = get_connection_factory("mysql")
    assert result is _dummy_factory


def test_get_connection_factory_returns_none_for_unregistered():
    result = get_connection_factory("nonexistent")
    assert result is None


def test_register_overwrites_existing():
    def other_factory(name, config):
        return None

    register_connection_factory("pg", _dummy_factory)
    register_connection_factory("pg", other_factory)
    assert _CONNECTION_FACTORIES["pg"] is other_factory


def test_load_plugins_runs_without_error():
    load_plugins()


def test_load_plugins_registers_entry_point(monkeypatch):
    import logging
    from unittest.mock import MagicMock

    logging.getLogger("odibi.plugins").propagate = False

    mock_ep = MagicMock()
    mock_ep.name = "test_conn"
    mock_ep.load.return_value = _dummy_factory

    import odibi.plugins as plugins_mod

    monkeypatch.setattr(plugins_mod, "entry_points", lambda **kwargs: [mock_ep])
    load_plugins()
    assert get_connection_factory("test_conn") is _dummy_factory

    logging.getLogger("odibi.plugins").propagate = True


def test_load_plugins_handles_failing_entry_point(monkeypatch):
    import logging
    from unittest.mock import MagicMock

    logging.getLogger("odibi.plugins").propagate = False

    mock_ep = MagicMock()
    mock_ep.name = "bad_conn"
    mock_ep.load.side_effect = RuntimeError("boom")

    import odibi.plugins as plugins_mod

    monkeypatch.setattr(plugins_mod, "entry_points", lambda **kwargs: [mock_ep])
    load_plugins()
    assert get_connection_factory("bad_conn") is None

    logging.getLogger("odibi.plugins").propagate = True


def test_load_plugins_handles_discovery_error(monkeypatch):
    import logging

    logging.getLogger("odibi.plugins").propagate = False

    import odibi.plugins as plugins_mod

    def broken_entry_points(**kwargs):
        raise RuntimeError("discovery failed")

    monkeypatch.setattr(plugins_mod, "entry_points", broken_entry_points)
    load_plugins()

    logging.getLogger("odibi.plugins").propagate = True
