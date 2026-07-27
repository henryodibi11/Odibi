# tests/integration/mcp/test_access_e2e.py
"""End-to-end tests for access enforcement."""

import json
import secrets

import pytest
import pandas as pd

import odibi.planning as planning
from odibi_mcp.contracts.access import AccessContext, ConnectionPolicy
from odibi_mcp.access.context import get_access_context
from odibi_mcp.access.path_validator import is_path_allowed
from odibi_mcp.access.physical_gate import can_include_physical_ref
from tests.fixtures.mcp_catalog import MockCatalogManager


PLANNED_LOGICAL_YAML = """
project: logical_test
pipelines:
  - pipeline: bounded
    nodes:
      - name: source
        read: {}
      - name: sink
        depends_on: [source]
        write: {}
"""


def _mcp_message(response):
    """Extract one JSON-RPC message from FastMCP JSON or SSE output."""
    assert response.status_code == 200
    if response.headers.get("content-type", "").startswith("application/json"):
        return response.json()
    data_lines = [line[6:] for line in response.text.splitlines() if line.startswith("data: ")]
    assert data_lines
    return json.loads(data_lines[-1])


def _call_over_http(client, action, authorization=None, **kwargs):
    """Call the universal action tool through the real FastMCP ASGI route."""
    headers = {
        "accept": "application/json, text/event-stream",
        "content-type": "application/json",
    }
    if authorization is not None:
        headers["authorization"] = authorization
    arguments = {"action": action}
    if kwargs:
        arguments["args_json"] = json.dumps(kwargs)
    response = client.post(
        "/mcp",
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "odibi_execute", "arguments": arguments},
        },
    )
    message = _mcp_message(response)
    return json.loads(message["result"]["content"][0]["text"])


def test_access_context_from_config():
    """Test creating AccessContext from config dict."""
    config = {
        "authorized_projects": ["project_a", "project_b"],
        "environment": "production",
        "physical_refs_enabled": True,
        "connection_policies": {
            "adls_main": {
                "connection": "adls_main",
                "allowed_path_prefixes": ["/data/bronze/", "/data/silver/"],
                "allow_physical_refs": True,
            }
        },
    }

    ctx = get_access_context(config)

    assert "project_a" in ctx.authorized_projects
    assert "project_b" in ctx.authorized_projects
    assert ctx.physical_refs_enabled is True
    assert "adls_main" in ctx.connection_policies


def test_project_scoping_filters_data():
    """Test that project scoping filters dataframes."""
    ctx = AccessContext(
        authorized_projects={"allowed_project"},
        environment="test",
    )

    catalog = MockCatalogManager()
    catalog.set_access_context(ctx)

    df = pd.DataFrame(
        {
            "project": ["allowed_project", "denied_project", "allowed_project"],
            "value": [1, 2, 3],
        }
    )

    filtered = catalog._apply_project_scope(df)

    assert len(filtered) == 2
    assert all(filtered["project"] == "allowed_project")


def test_path_validation_deny_by_default():
    """Test that paths are denied by default."""
    policy = ConnectionPolicy(
        connection="test_conn",
        allowed_path_prefixes=[],  # No allowed prefixes
        explicit_allow_all=False,
    )

    assert is_path_allowed(policy, "/any/path") is False
    assert is_path_allowed(policy, "/data/file.csv") is False


def test_path_validation_with_allowlist():
    """Test that allowed paths pass validation."""
    policy = ConnectionPolicy(
        connection="test_conn",
        allowed_path_prefixes=["/data/bronze/", "/data/silver/"],
    )

    assert is_path_allowed(policy, "/data/bronze/file.csv") is True
    assert is_path_allowed(policy, "/data/silver/table/") is True
    assert is_path_allowed(policy, "/data/gold/file.csv") is False


def test_physical_ref_gate():
    """Test physical ref gating with all conditions."""
    policy = ConnectionPolicy(
        connection="test_conn",
        allowed_path_prefixes=["/data/"],
        allow_physical_refs=True,
    )

    ctx = AccessContext(
        authorized_projects={"test"},
        physical_refs_enabled=True,
        connection_policies={"test_conn": policy},
    )

    # All gates pass
    assert (
        can_include_physical_ref(
            include_physical=True,
            connection="test_conn",
            access_context=ctx,
        )
        is True
    )

    # Flag is False
    assert (
        can_include_physical_ref(
            include_physical=False,
            connection="test_conn",
            access_context=ctx,
        )
        is False
    )


def test_access_context_check_project():
    """Test project access check."""
    ctx = AccessContext(
        authorized_projects={"project_a"},
        environment="test",
    )

    # Should pass
    ctx.check_project("project_a")

    # Should raise
    with pytest.raises(PermissionError):
        ctx.check_project("project_b")


def test_http_exact_bearer_planning_matches_package_without_runtime_binding(
    monkeypatch, immutable_planning_tripwires
):
    """The authorized HTTP route returns only the shared immutable planner schema."""
    pytest.importorskip("fastmcp")
    from starlette.testclient import TestClient

    from odibi_mcp import databricks_app, mcp_server

    configured = secrets.token_urlsafe(32)

    def unexpected_runtime(*args, **kwargs):
        pytest.fail("immutable HTTP planning bound project runtime context")

    monkeypatch.setenv("ODIBI_MCP_AUTH_TOKEN", configured)
    monkeypatch.setattr(mcp_server._dispatcher, "_bind_runtime_context", unexpected_runtime)
    monkeypatch.setattr(mcp_server._dispatcher, "_restore_runtime_context", unexpected_runtime)

    original_plan = planning.plan_pipeline_yaml
    effect_attempts = []

    def guarded_plan(yaml_text):
        with immutable_planning_tripwires() as attempts:
            response = original_plan(yaml_text)
        effect_attempts.extend(attempts)
        return response

    monkeypatch.setattr(planning, "plan_pipeline_yaml", guarded_plan)

    with TestClient(databricks_app.http_app) as client:
        results = [
            _call_over_http(
                client,
                "test_pipeline",
                f"Bearer {configured}",
                pipeline=PLANNED_LOGICAL_YAML,
            )
            for _ in range(25)
        ]

    expected = planning.plan_pipeline_yaml(PLANNED_LOGICAL_YAML).to_dict()
    assert results == [expected] * 25
    result = results[0]
    assert result == expected
    assert configured not in json.dumps(result)
    assert set(result) == {"schema_version", "status", "plan", "diagnostics", "truncated"}
    assert effect_attempts == []


@pytest.mark.parametrize(
    ("configured", "authorization"),
    [
        (None, None),
        (None, "Bearer caller-token"),
        ("configured-token", None),
        ("configured-token", "Bearer"),
        ("configured-token", "Basic configured-token"),
        ("configured-token", "Bearer wrong-token"),
    ],
)
def test_http_bad_bearer_denies_before_planning(monkeypatch, configured, authorization):
    """Every absent/malformed/wrong credential is denied before YAML reaches the planner."""
    pytest.importorskip("fastmcp")
    from starlette.testclient import TestClient

    from odibi_mcp import databricks_app

    calls = []

    def unexpected_plan(*args, **kwargs):
        calls.append((args, kwargs))
        pytest.fail("unauthorized HTTP input reached immutable planning")

    if configured is None:
        monkeypatch.delenv("ODIBI_MCP_AUTH_TOKEN", raising=False)
    else:
        monkeypatch.setenv("ODIBI_MCP_AUTH_TOKEN", configured)
    monkeypatch.setattr(planning, "plan_pipeline_yaml", unexpected_plan)

    with TestClient(databricks_app.http_app) as client:
        result = _call_over_http(
            client,
            "test_pipeline",
            authorization,
            pipeline="malformed: [ credential-canary",
        )

    assert result["code"] == "AUTHORIZATION_REQUIRED"
    assert calls == []
    assert "credential-canary" not in json.dumps(result)


def test_http_caller_identity_override_and_stdio_deny_before_planning(monkeypatch):
    """Transport authority remains nominal: callers cannot forge it and stdio has none."""
    pytest.importorskip("fastmcp")
    from starlette.testclient import TestClient

    from odibi_mcp import databricks_app, mcp_server

    calls = []

    def unexpected_plan(*args, **kwargs):
        calls.append((args, kwargs))
        pytest.fail("non-authoritative caller input reached planning")

    configured = secrets.token_urlsafe(32)
    monkeypatch.setenv("ODIBI_MCP_AUTH_TOKEN", configured)
    monkeypatch.setattr(planning, "plan_pipeline_yaml", unexpected_plan)

    with TestClient(databricks_app.http_app) as client:
        forged = _call_over_http(
            client,
            "test_pipeline",
            f"Bearer {configured}",
            pipeline=PLANNED_LOGICAL_YAML,
            application_identity={"subject": "forged"},
        )

    execute = getattr(mcp_server.odibi_execute, "fn", mcp_server.odibi_execute)
    stdio = json.loads(execute("test_pipeline", json.dumps({"pipeline": PLANNED_LOGICAL_YAML})))

    assert forged["code"] == "INVALID_ARGUMENT"
    assert stdio["code"] == "AUTHORIZATION_REQUIRED"
    assert calls == []
