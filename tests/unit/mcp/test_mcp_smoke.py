"""Portable MCP smoke test.

Replaces the machine-specific test_all_tools.py (hardcoded to a local IPS_S_Curve
project). This test builds a tiny *simulation* project in tmp_path — no cloud, no
external files — so it is hermetic and runnable in CI. It exercises the core MCP tools
through the real call_tool() dispatch and asserts they return non-error envelopes, and
guards the two audit fixes (tool exposure + connection path resolution).
"""

import asyncio
import json

import pytest

pytest.importorskip("mcp")

from odibi_mcp import context as mcp_context
from odibi_mcp.server import call_tool, list_tools

SMOKE_CONFIG = """
project: smoke
engine: pandas
connections:
  out:
    type: local
    base_path: ./data
system:
  connection: out
pipelines:
  - pipeline: sim
    nodes:
      - name: readings
        read:
          connection: null
          format: simulation
          options:
            simulation:
              scope:
                start_time: "2026-01-01T00:00:00Z"
                row_count: 10
                timestep: "1h"
                seed: 42
              entities:
                count: 2
                id_prefix: "e_"
              columns:
                - {name: ts, data_type: timestamp, generator: {type: timestamp}}
                - {name: v, data_type: float, generator: {type: range, min: 0, max: 100}}
"""


@pytest.fixture
def smoke_project(tmp_path, monkeypatch):
    """Write a hermetic simulation project and initialize MCP context against it."""
    config_path = tmp_path / "odibi.yaml"
    config_path.write_text(SMOKE_CONFIG, encoding="utf-8")
    (tmp_path / "data").mkdir(exist_ok=True)

    monkeypatch.setenv("ODIBI_CONFIG", str(config_path))
    monkeypatch.delenv("MCP_CONFIG", raising=False)

    previous = mcp_context.get_project_context()
    ctx = mcp_context.initialize_from_env()
    assert ctx is not None, "initialize_from_env failed to load the smoke project"
    yield {"config_path": config_path, "config_text": SMOKE_CONFIG, "ctx": ctx}
    # Restore global context so this test doesn't pollute others.
    mcp_context.set_project_context(previous)


def _call(name, args):
    """Invoke a tool through the real dispatch and parse its JSON envelope."""
    result = asyncio.run(call_tool(name, args))
    text = result[0].text if result else ""
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return {"_raw": text}


def _looks_like_error(payload):
    if not isinstance(payload, dict):
        return False
    return "error" in payload or "_raised" in payload


# ── Audit fix #1: tool exposure ───────────────────────────────────────────────


def test_core_tools_are_advertised():
    """Tools whose handlers exist must be discoverable via list_tools (otherwise an MCP
    client can never call them)."""
    names = {t.name for t in asyncio.run(list_tools())}
    must_be_exposed = {
        "bootstrap_context",
        "explain",
        "list_connections",
        "get_validation_rules",
        "story_read",
        "node_sample",
        "node_failed_rows",
        "lineage_graph",
    }
    missing = must_be_exposed - names
    assert not missing, f"handlers exist but not advertised in list_tools: {missing}"


# ── Audit fix #2: connection path resolution ──────────────────────────────────


def test_relative_base_path_is_rebased_to_config_dir(smoke_project):
    """A relative local base_path must resolve relative to the config file, not the
    process CWD."""
    import os

    ctx = smoke_project["ctx"]
    conn = ctx.get_connection("out")
    base = getattr(conn, "base_path", None)
    assert base is not None
    assert os.path.isabs(str(base)), f"base_path should be absolute, got {base!r}"
    expected = (smoke_project["config_path"].parent / "data").resolve()
    assert os.path.normcase(str(base)) == os.path.normcase(str(expected))


# ── Audit fix #3: run-inspection tools accept their advertised params ─────────
# These tools (story_read/node_sample/node_failed_rows/lineage_graph) were exposed
# in list_tools() but their call_tool() dispatch was stale — it passed kwargs
# (max_rows/include_external) the handlers don't accept, so any client call with a
# documented optional param raised TypeError. A live protocol dogfood caught it.
# The smoke project has no run, so a graceful "not found" is fine — what must NOT
# happen is a TypeError from a kwarg/signature mismatch.


@pytest.mark.parametrize(
    "name,args",
    [
        ("story_read", {"pipeline": "sim"}),
        ("story_read", {"pipeline": "sim", "run_id": "some-run"}),
        ("node_sample", {"pipeline": "sim", "node": "readings", "max_rows": 5}),
        ("node_sample", {"pipeline": "sim", "node": "readings", "max_rows": 5, "run_id": "r"}),
        ("node_failed_rows", {"pipeline": "sim", "node": "readings", "max_rows": 5}),
        ("lineage_graph", {"pipeline": "sim"}),
        ("lineage_graph", {"pipeline": "sim", "run_id": "r"}),
    ],
)
def test_inspection_tools_accept_advertised_params(smoke_project, name, args):
    """Calling these with their documented optional params must not raise a
    signature/kwarg TypeError (the bug the live dogfood exposed)."""
    payload = _call(name, args)
    blob = json.dumps(payload).lower()
    assert "unexpected keyword argument" not in blob, payload
    assert "typeerror" not in blob, payload


# ── Core tool smoke checks (real dispatch) ────────────────────────────────────


@pytest.mark.parametrize(
    "name,args",
    [
        ("list_transformers", {}),
        ("list_patterns", {}),
        ("list_task_types", {}),
        ("explain", {"name": "scd2"}),
    ],
)
def test_knowledge_tools_return_non_error(smoke_project, name, args):
    payload = _call(name, args)
    assert not _looks_like_error(payload), f"{name} returned an error envelope: {payload}"


def test_bootstrap_context_loads_project(smoke_project):
    payload = _call("bootstrap_context", {})
    project = payload.get("project", {})
    assert isinstance(project, dict) and "error" not in project, payload
    assert project.get("name") == "smoke"
    assert len(payload.get("connections", [])) == 1
    assert len(payload.get("pipelines", [])) == 1


def test_validate_yaml_accepts_simulation_config(smoke_project):
    payload = _call("validate_yaml", {"yaml_content": smoke_project["config_text"]})
    assert not _looks_like_error(payload), payload


def test_apply_pattern_template_generates_valid_yaml(smoke_project):
    payload = _call(
        "apply_pattern_template",
        {
            "pattern": "dimension",
            "pipeline_name": "dim_customer",
            "source_connection": "out",
            "target_connection": "out",
            "target_path": "dim/customer",
            "natural_key": "customer_id",
            "surrogate_key": "customer_sk",
            "source_table": "customers",
        },
    )
    assert payload.get("valid") is True, payload
    assert payload.get("yaml"), "expected generated YAML"
