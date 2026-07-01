"""Smoke test for the FastMCP Odibi server (odibi_execute / odibi_help).

The server is a 2-tool facade over OdibiDispatcher. This drives those tool
functions the way an MCP client would (string action + JSON args, JSON out) and
asserts the discovery surface returns real data — the end-to-end equivalent of
the old per-tool smoke test, against the new dispatcher architecture.
"""

import json

import pytest

pytest.importorskip("fastmcp")

from odibi_mcp import mcp_server


def _execute(action, **kwargs):
    """Invoke the odibi_execute tool function and parse its JSON result."""
    fn = getattr(mcp_server.odibi_execute, "fn", mcp_server.odibi_execute)
    args_json = json.dumps(kwargs) if kwargs else None
    return json.loads(fn(action, args_json))


def _help(**kwargs):
    fn = getattr(mcp_server.odibi_help, "fn", mcp_server.odibi_help)
    return json.loads(fn(**kwargs))


def test_facade_tools_present():
    assert hasattr(mcp_server, "mcp")
    assert callable(getattr(mcp_server.odibi_execute, "fn", mcp_server.odibi_execute))
    assert callable(getattr(mcp_server.odibi_help, "fn", mcp_server.odibi_help))


def test_help_catalog():
    assert _help()["total_actions"] == 43


@pytest.mark.parametrize(
    "action,kwargs,key",
    [
        ("onboard", {}, "instructions"),
        ("list_skills", {}, "skills"),
        ("get_schema", {"component": "read"}, "schema"),
        ("search_docs", {"query": "simulation"}, "results"),
        ("list_examples", {"pattern": "simulation"}, "examples"),
    ],
)
def test_discovery_over_facade(action, kwargs, key):
    result = _execute(action, **kwargs)
    assert key in result, f"{action} returned: {list(result)}"


def test_bad_args_json_is_handled():
    fn = getattr(mcp_server.odibi_execute, "fn", mcp_server.odibi_execute)
    assert "error" in json.loads(fn("onboard", "not-json"))


def test_unknown_action_is_handled():
    assert "error" in _execute("nonexistent_action_xyz")
