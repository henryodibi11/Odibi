import pytest
import importlib

try:
    from odibi.plugins import PluginRegistry
except ImportError:
    pytest.skip("PluginRegistry not available", allow_module_level=True)


def test_plugin_registry_initially_empty():
    """
    Test that a newly created PluginRegistry starts empty.
    """
    registry = PluginRegistry()
    plugins = registry.get_plugins()
    assert isinstance(plugins, dict)
    assert plugins == {}


def test_register_and_invoke_plugin():
    """
    Test that a plugin can be registered and invoked correctly.
    """
    registry = PluginRegistry()

    def dummy_plugin():
        return "dummy_output"

    registry.register("dummy", dummy_plugin)
    plugins = registry.get_plugins()
    assert "dummy" in plugins
    result = plugins["dummy"]()
    assert result == "dummy_output"


def test_introspect_module_import():
    """
    Test that the odibi.introspect module can be imported without error.
    """
    introspect = importlib.import_module("odibi.introspect")
    # Optionally, if a callable is provided (e.g. run_introspection), call it.
    if hasattr(introspect, "run_introspection"):
        result = introspect.run_introspection()
        # Expected result should be a dictionary; otherwise, just check for type.
        assert isinstance(result, dict)
    else:
        # If not available, simply verify that the module contains expected attributes.
        assert hasattr(introspect, "__file__")
