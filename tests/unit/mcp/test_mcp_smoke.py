"""Smoke test for the FastMCP Odibi server (odibi_execute / odibi_help).

The server is a 2-tool facade over OdibiDispatcher. This drives those tool
functions the way an MCP client would (string action + JSON args, JSON out) and
asserts the discovery surface returns real data — the end-to-end equivalent of
the old per-tool smoke test, against the new dispatcher architecture.
"""

import json
import secrets
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("fastmcp")

from odibi_mcp import databricks_app, mcp_server
from odibi_mcp.contracts.access import ManagedProjectAccess, PreparedRuntimeCall


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


def _call_over_http(client, action, authorization=None, **kwargs):
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
            "params": {
                "name": "odibi_execute",
                "arguments": arguments,
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
    http_app = databricks_app.http_app

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

        assert "instructions" in _call_over_http(client, "onboard")

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


def test_real_http_runtime_data_requires_project_scope_after_bearer(tmp_path, monkeypatch):
    from starlette.testclient import TestClient

    root = tmp_path / "managed"
    (root / "data" / "folder").mkdir(parents=True)
    (root / "exports").mkdir()
    config = root / "odibi.yaml"
    config.write_text(
        """
project: managed
connections:
  local:
    type: local
    base_path: ./data
story:
  connection: local
  path: stories
system:
  connection: local
pipelines: []
""".lstrip(),
        encoding="utf-8",
    )
    configured = secrets.token_urlsafe(32)
    different = secrets.token_urlsafe(32)
    calls = []
    monkeypatch.setenv("ODIBI_MCP_AUTH_TOKEN", configured)
    monkeypatch.setattr(
        mcp_server._dispatcher,
        "_managed_access",
        ManagedProjectAccess("managed", root, config, root / "exports"),
    )
    monkeypatch.setattr(
        mcp_server._dispatcher,
        "_bind_runtime_context",
        lambda config_path: None,
    )
    monkeypatch.setitem(
        mcp_server._dispatcher._actions,
        "profile_source",
        lambda **kwargs: calls.append(kwargs) or {"path": kwargs["path"]},
    )

    with TestClient(databricks_app.http_app) as client:
        wrong_identity = _call_over_http(
            client,
            "profile_source",
            f"Bearer {different}",
            project="managed",
            connection="local",
            path="folder/input.csv",
        )
        identity_only = _call_over_http(
            client,
            "profile_source",
            f"Bearer {configured}",
            connection="local",
            path="folder/input.csv",
        )
        allowed = _call_over_http(
            client,
            "profile_source",
            f"Bearer {configured}",
            project="managed",
            connection="local",
            path="folder/input.csv",
            max_rows=19,
        )

    assert wrong_identity["code"] == "AUTHORIZATION_REQUIRED"
    assert identity_only["code"] == "PROJECT_SCOPE_REQUIRED"
    assert allowed["path"] == "folder/input.csv"
    assert allowed["policy_applied"] == {"project_scoped": True, "sample_capped": True}
    assert calls == [
        {
            "connection": "local",
            "path": "folder/input.csv",
            "max_rows": 19,
        }
    ]


def test_real_http_remote_lineage_returns_only_typed_logical_projection(
    tmp_path, monkeypatch, caplog
):
    from starlette.testclient import TestClient

    from odibi_mcp import context as context_module

    sentinels = (
        "HTTP_PASSWORD_SENTINEL_1a2b",
        "HTTP_UNKNOWN_SCALAR_SENTINEL_2b3c",
        "HTTP_PHYSICAL_URI_SENTINEL_3c4d",
        "HTTP_HOST_SENTINEL_4d5e",
        "HTTP_SQL_SENTINEL_5e6f",
        "HTTP_STALE_CONTEXT_SENTINEL_6f7a",
        "HTTP_IMPORT_SENTINEL_7a8b",
        "HTTP_STORY_EVENT_SENTINEL_8b9c",
        "HTTP_HELPER_EXCEPTION_SENTINEL_9c0d",
    )
    root = tmp_path / "managed"
    (root / "data" / "stories").mkdir(parents=True)
    (root / "exports").mkdir()
    config_path = root / "odibi.yaml"
    config_path.write_text(
        f"""
project: managed
connections:
  local:
    type: local
    base_path: ./data
    password: {sentinels[0]}
    host: {sentinels[3]}
    unknown: {{nested: {sentinels[1]}}}
story: {{connection: local, path: stories}}
system: {{connection: local}}
pipelines:
  - pipeline: bounded
    nodes:
      - name: source
        read:
          connection: local
          format: sql
          path: abfss://private/{sentinels[2]}
          query: SELECT '{sentinels[4]}' FROM private_table
      - name: clean
        depends_on: [source]
        transform:
          steps:
            - sql: SELECT '{sentinels[4]}' FROM df
        write:
          connection: local
          format: delta
          path: s3://private/{sentinels[2]}
""".lstrip(),
        encoding="utf-8",
    )
    (root / "imported.yaml").write_text(f"pipeline: {sentinels[6]}\n", encoding="utf-8")
    (root / "data" / "stories" / "bounded.json").write_text(
        json.dumps(
            {
                "lineage": {"nodes": [{"id": sentinels[2]}], "edges": []},
                "events": [{"message": sentinels[7]}],
            }
        ),
        encoding="utf-8",
    )
    stale_context = context_module.MCPProjectContext.from_config_snapshot(
        root / "stale.yaml",
        {
            "project": "stale",
            "unknown": {"ambient": sentinels[5]},
            "connections": {},
            "pipelines": [],
        },
    )
    previous_context = context_module.get_project_context()
    context_module.set_project_context(stale_context)
    configured = secrets.token_urlsafe(32)
    original_open = Path.open
    reads = []

    def config_only_open(path, *args, **kwargs):
        resolved = path.resolve()
        reads.append(resolved)
        if resolved != config_path.resolve():
            pytest.fail("HTTP logical lineage read a story, runtime, import, or other file")
        return original_open(path, *args, **kwargs)

    def unexpected_effect(*args, **kwargs):
        raise RuntimeError(sentinels[8])

    monkeypatch.setenv("ODIBI_MCP_AUTH_TOKEN", configured)
    monkeypatch.setattr(
        mcp_server._dispatcher,
        "_managed_access",
        ManagedProjectAccess("managed", root, config_path, root / "exports"),
    )
    monkeypatch.setattr(mcp_server._dispatcher, "_bind_runtime_context", unexpected_effect)
    monkeypatch.setattr(
        context_module.MCPProjectContext, "initialize_connections", unexpected_effect
    )
    monkeypatch.setattr(Path, "open", config_only_open)
    monkeypatch.setattr(Path, "glob", unexpected_effect)
    monkeypatch.setattr(Path, "rglob", unexpected_effect)
    monkeypatch.setattr(Path, "iterdir", unexpected_effect)

    try:
        with TestClient(databricks_app.http_app) as client:
            result = _call_over_http(
                client,
                "lineage_graph",
                f"Bearer {configured}",
                project="managed",
                pipeline="bounded",
            )
            rejected = _call_over_http(
                client,
                "lineage_graph",
                f"Bearer {configured}",
                project="managed",
                pipeline=f"rejected_{sentinels[6]}",
            )
    finally:
        monkeypatch.undo()
        context_module.set_project_context(previous_context)

    assert result["kind"] == "logical_lineage_graph"
    assert result["pipeline"] == "bounded"
    assert result["nodes"] == [
        {"id": "source", "type": "pipeline_node"},
        {"id": "clean", "type": "pipeline_node"},
    ]
    assert result["edges"] == [{"source": "source", "target": "clean", "kind": "dependency"}]
    assert result["counts"] == {
        "nodes_total": 2,
        "nodes_returned": 2,
        "edges_total": 1,
        "edges_returned": 1,
    }
    assert result["truncated"] is False
    assert result["policy_applied"]["logical_only"] is True
    assert rejected == {
        "error": "The logical lineage projection is unavailable",
        "code": "LOGICAL_PROJECTION_UNAVAILABLE",
        "action": "lineage_graph",
    }
    assert reads == [config_path.resolve(), config_path.resolve()]
    assert context_module.get_project_context() is previous_context
    serialized = json.dumps({"result": result, "rejected": rejected}, indent=2, sort_keys=True)
    assert len(json.dumps(result, indent=2).encode("utf-8")) <= 65536
    assert "state" not in result and "events" not in result
    assert all(sentinel not in serialized for sentinel in sentinels)
    assert all(sentinel not in caplog.text for sentinel in sentinels)


def test_real_http_remote_lineage_malformed_preparation_never_reaches_legacy_path(
    tmp_path, monkeypatch, caplog
):
    from starlette.testclient import TestClient

    from odibi_mcp import dispatcher as dispatcher_module

    sentinel = "HTTP_MALFORMED_PREPARATION_SENTINEL_a1b2"
    configured = secrets.token_urlsafe(32)
    prepared = PreparedRuntimeCall(
        action="lineage_graph",
        kwargs={"pipeline": sentinel},
        project_root=tmp_path,
        config_path=tmp_path / "unvalidated.yaml",
        config_snapshot={"unknown": sentinel},
        config_fingerprint=sentinel,
        logical_lineage=None,
    )
    access = ManagedProjectAccess("managed", tmp_path, tmp_path / "unvalidated.yaml")
    effects = []

    def unexpected(*args, **kwargs):
        effects.append(sentinel)
        return {"sentinel": sentinel}

    monkeypatch.setenv("ODIBI_MCP_AUTH_TOKEN", configured)
    monkeypatch.setattr(ManagedProjectAccess, "prepare", lambda self, action, kwargs: prepared)
    monkeypatch.setattr(mcp_server._dispatcher, "_managed_access", access)
    monkeypatch.setattr(mcp_server._dispatcher, "_bind_runtime_context", unexpected)
    monkeypatch.setattr(mcp_server._dispatcher, "_restore_runtime_context", unexpected)
    monkeypatch.setattr(dispatcher_module, "sanitize_runtime_result", unexpected)
    monkeypatch.setattr(dispatcher_module, "render_remote_logical_lineage_projection", unexpected)
    monkeypatch.setitem(mcp_server._dispatcher._actions, "lineage_graph", unexpected)

    with TestClient(databricks_app.http_app) as client:
        result = _call_over_http(
            client,
            "lineage_graph",
            f"Bearer {configured}",
            project="managed",
            pipeline="bounded",
        )

    assert result == {
        "error": "The logical lineage projection is unavailable",
        "code": "LOGICAL_PROJECTION_UNAVAILABLE",
        "action": "lineage_graph",
    }
    assert effects == []
    assert sentinel not in json.dumps(result, sort_keys=True)
    assert sentinel not in caplog.text


def test_databricks_app_propagates_startup_failure(tmp_path):
    """A deployment startup error must stop import, not become an HTTP response."""
    script = """
import importlib
import sys

from odibi_mcp import mcp_server


def fail_startup(*args, **kwargs):
    raise RuntimeError("generated startup sentinel")


mcp_server.mcp.http_app = fail_startup
try:
    importlib.import_module("odibi_mcp.databricks_app")
except RuntimeError as error:
    assert str(error) == "generated startup sentinel"
else:
    raise AssertionError("databricks_app masked the generated startup failure")
"""
    package_root = str(Path(mcp_server.__file__).resolve().parents[1])

    result = subprocess.run(
        [sys.executable, "-c", f"import sys; sys.path.insert(0, {package_root!r})\n{script}"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
