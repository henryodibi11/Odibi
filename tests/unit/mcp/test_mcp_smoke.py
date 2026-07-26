"""Smoke test for the FastMCP Odibi server (odibi_execute / odibi_help).

The server is a 2-tool facade over OdibiDispatcher. This drives those tool
functions the way an MCP client would (string action + JSON args, JSON out) and
asserts the discovery surface returns real data — the end-to-end equivalent of
the old per-tool smoke test, against the new dispatcher architecture.
"""

import json
import secrets

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


def _mcp_message(response):
    data_lines = [line[6:] for line in response.text.splitlines() if line.startswith("data: ")]
    assert response.status_code == 200
    assert data_lines
    return json.loads(data_lines[-1])


def _call_over_http(client, action, authorization=None):
    headers = {
        "accept": "application/json, text/event-stream",
        "content-type": "application/json",
    }
    if authorization is not None:
        headers["authorization"] = authorization
    response = client.post(
        "/mcp",
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "odibi_execute",
                "arguments": {"action": action},
            },
        },
    )
    message = _mcp_message(response)
    return json.loads(message["result"]["content"][0]["text"])


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


def test_direct_facade_call_has_no_ambient_trusted_identity():
    assert _execute("create_pipeline")["code"] == "AUTHORIZATION_REQUIRED"


def test_caller_cannot_supply_transport_identity():
    result = _execute(
        "list_workflows",
        application_identity={"subject": "forged", "authorized_effects": ["execution"]},
    )

    assert result["code"] == "INVALID_ARGUMENT"


def test_real_http_boundary_requires_exact_application_bearer(monkeypatch):
    from starlette.testclient import TestClient

    configured = secrets.token_urlsafe(32)
    different = secrets.token_urlsafe(32)
    calls = []
    monkeypatch.setenv("ODIBI_MCP_AUTH_TOKEN", configured)
    monkeypatch.setitem(
        mcp_server._dispatcher._actions,
        "create_pipeline",
        lambda: calls.append("create_pipeline") or {"allowed": True},
    )
    http_app = mcp_server.mcp.http_app(stateless_http=True)

    with TestClient(http_app) as client:
        initialize = client.post(
            "/mcp",
            headers={
                "accept": "application/json, text/event-stream",
                "content-type": "application/json",
            },
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "authorization-test", "version": "1"},
                },
            },
        )
        assert _mcp_message(initialize)["result"]["serverInfo"]["name"] == "odibi-knowledge"

        for authorization in (
            None,
            "Bearer",
            f"Basic {configured}",
            f"Bearer {different}",
        ):
            result = _call_over_http(client, "create_pipeline", authorization)
            assert result["code"] == "AUTHORIZATION_REQUIRED"
            assert calls == []

        result = _call_over_http(client, "create_pipeline", f"Bearer {configured}")

    assert result == {"allowed": True}
    assert calls == ["create_pipeline"]
