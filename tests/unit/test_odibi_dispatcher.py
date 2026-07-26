"""Guards for the OdibiDispatcher facade (odibi_execute/odibi_help backend).

The dispatcher routes 43 actions; the onboarding/discovery actions delegate to the
OdibiKnowledge singleton. A prior version called get_knowledge("onboard") (wrong API),
which threw TypeError and silently broke ALL discovery — the catalog still listed the
actions, so a help-only test passed. This test actually DISPATCHES each action and
asserts it returns real data, so that regression can't recur.
"""

import sys
from types import ModuleType, SimpleNamespace

import pytest

from odibi_mcp.contracts.access import ActionEffect, ApplicationIdentity
from odibi_mcp.dispatcher import ACTION_EFFECTS, OdibiDispatcher
from odibi_mcp.tools import execution

D = OdibiDispatcher()
LOCAL_IDENTITY = ApplicationIdentity.trusted_local()

VALID_PIPELINE_YAML = """
project: bounded_test
connections:
  local: {type: local, base_path: ./data}
story: {connection: local, path: _stories}
system: {connection: local}
pipelines:
  - pipeline: bounded
    nodes:
      - name: source
        read: {connection: local, format: csv, path: input.csv}
        write: {connection: local, format: delta, table: output}
"""

# Discovery/onboarding actions that delegate to OdibiKnowledge — the ones that were broken.
DISCOVERY_CALLS = [
    ("onboard", {}, "instructions"),
    ("get_schema", {}, "project"),
    ("get_schema", {"component": "read"}, "schema"),
    ("search_docs", {"query": "simulation"}, "results"),
    ("list_docs", {"category": "patterns"}, "docs"),
    ("list_examples", {"pattern": "simulation"}, "examples"),
    ("get_example", {"pattern_name": "scd2"}, None),
    ("list_skills", {}, "skills"),
    ("get_skill", {"name": "odibi"}, "content"),
]


def _is_error(result):
    return isinstance(result, dict) and set(result.keys()) <= {"error", "tip", "available"}


def _trusted_dispatch(action, **kwargs):
    return D.dispatch(action, application_identity=LOCAL_IDENTITY, **kwargs)


@pytest.mark.parametrize("action,kwargs,expect_key", DISCOVERY_CALLS)
def test_discovery_actions_return_real_data(action, kwargs, expect_key):
    result = _trusted_dispatch(action, **kwargs)
    assert isinstance(result, dict), f"{action} did not return a dict"
    assert not _is_error(result), f"{action} returned an error: {result}"
    if expect_key:
        assert expect_key in result, f"{action} missing expected key '{expect_key}': {list(result)}"


def test_simulation_is_discoverable():
    """The exact thing Genie couldn't do: discover the simulation engine via the MCP."""
    docs = D.dispatch("search_docs", query="simulation")
    assert docs.get("results"), "search_docs('simulation') found nothing"
    examples = D.dispatch("list_examples", pattern="simulation")
    assert examples.get("examples"), "list_examples('simulation') found nothing"
    read_schema = D.dispatch("get_schema", component="read")
    assert "simulation" in str(read_schema), "read schema does not expose format: simulation"


@pytest.mark.parametrize(
    "action", ["list_workflows", "list_transformers", "list_patterns", "list_task_types"]
)
def test_no_arg_tool_actions_dispatch(action):
    result = D.dispatch(action)
    assert isinstance(result, dict) and not _is_error(result), f"{action} failed: {result}"


def test_help_catalog_and_unknown_action():
    cat = D.help()
    assert cat["total_actions"] == 43
    assert "error" in D.dispatch("nonexistent_action_xyz")


def test_get_doc_directory_is_graceful():
    # Passing a directory must return a clean error, not a raw OS exception.
    result = _trusted_dispatch("get_doc", doc_path="docs/simulation")
    assert "error" in result and "content" not in result


EXPECTED_EFFECT_ACTIONS = {
    ActionEffect.PUBLIC_READ: {
        "list_workflows",
        "get_workflow",
        "list_transformers",
        "list_patterns",
        "apply_pattern_template",
        "validate_yaml",
        "validate_pipeline",
        "get_task_guidance",
        "list_task_types",
        "onboard",
        "get_schema",
        "search_docs",
        "list_docs",
        "list_examples",
        "list_skills",
    },
    ActionEffect.SENSITIVE_READ: {
        "map_environment",
        "profile_source",
        "profile_folder",
        "story_read",
        "node_sample",
        "node_failed_rows",
        "lineage_graph",
        "suggest_pipeline",
        "diagnose",
        "get_doc",
        "get_example",
        "get_skill",
        "get_pipeline_state",
        "render_pipeline_yaml",
        "list_sessions",
    },
    ActionEffect.EXECUTION: {"run_workflow", "resume_workflow", "test_pipeline"},
    ActionEffect.FILE_WRITE: {"download_sql", "download_table", "download_file"},
    ActionEffect.SESSION_MUTATION: {
        "create_ingestion_pipeline",
        "create_pipeline",
        "add_node",
        "configure_read",
        "configure_write",
        "configure_transform",
        "discard_pipeline",
    },
}


def test_all_registered_actions_have_the_reviewed_effect_classification():
    expected = {
        action: effect for effect, actions in EXPECTED_EFFECT_ACTIONS.items() for action in actions
    }

    assert len(expected) == 43
    assert ACTION_EFFECTS == expected
    assert set(D._actions) == set(expected)


def test_registry_drift_fails_dispatcher_initialization():
    class DispatcherWithUnclassifiedAction(OdibiDispatcher):
        def _register_actions(self):
            actions = super()._register_actions()
            actions["unclassified_action"] = lambda: {"unexpected": True}
            return actions

    with pytest.raises(RuntimeError, match="Action effect policy mismatch"):
        DispatcherWithUnclassifiedAction()


@pytest.mark.parametrize(
    "action",
    ["profile_source", "test_pipeline", "download_file", "create_pipeline"],
)
@pytest.mark.parametrize("identity", [None, {"subject": "forged"}])
def test_restricted_actions_deny_absent_or_malformed_identity_before_handler(action, identity):
    dispatcher = OdibiDispatcher()

    def unexpected_handler():
        pytest.fail("authorization must run before the action handler")

    dispatcher._actions[action] = unexpected_handler

    result = dispatcher.dispatch(action, application_identity=identity)

    assert result["code"] == "AUTHORIZATION_REQUIRED"
    assert result["action"] == action


def test_authenticated_identity_without_effect_is_forbidden_before_handler():
    dispatcher = OdibiDispatcher()
    dispatcher._actions["download_file"] = lambda: pytest.fail(
        "authorization must run before the action handler"
    )
    identity = ApplicationIdentity(
        subject="read-only-application",
        authorized_effects=frozenset({ActionEffect.SENSITIVE_READ}),
    )

    result = dispatcher.dispatch("download_file", application_identity=identity)

    assert result == {
        "error": "Application identity is not authorized for this action",
        "code": "FORBIDDEN",
        "action": "download_file",
        "effect": "file_write",
    }


@pytest.mark.parametrize(
    "action,effect",
    [
        ("profile_source", ActionEffect.SENSITIVE_READ),
        ("test_pipeline", ActionEffect.EXECUTION),
        ("download_file", ActionEffect.FILE_WRITE),
        ("create_pipeline", ActionEffect.SESSION_MUTATION),
    ],
)
def test_identity_authorized_for_effect_invokes_handler_once(action, effect):
    dispatcher = OdibiDispatcher()
    calls = []
    dispatcher._actions[action] = lambda: calls.append(action) or {"allowed": True}
    identity = ApplicationIdentity(
        subject="bounded-application", authorized_effects=frozenset({effect})
    )

    result = dispatcher.dispatch(action, application_identity=identity)

    assert result == {"allowed": True}
    assert calls == [action]


def test_public_read_remains_anonymous():
    dispatcher = OdibiDispatcher()
    dispatcher._actions["list_workflows"] = lambda: {"public": True}

    assert dispatcher.dispatch("list_workflows") == {"public": True}


def test_help_exposes_every_action_effect():
    help_result = D.help()

    assert help_result["total_actions"] == 43
    assert help_result["action_effects"] == {
        action: effect.value for action, effect in sorted(ACTION_EFFECTS.items())
    }
    assert D.help(action="get_doc")["effect"] == "sensitive_read"


def test_validate_pipeline_does_not_expose_connection_checks():
    dispatcher = OdibiDispatcher()
    calls = []
    dispatcher._actions["validate_pipeline"] = lambda pipeline: calls.append(pipeline)

    result = dispatcher.dispatch(
        "validate_pipeline", pipeline="pipelines: []", check_connections=True
    )

    assert "error" in result
    assert calls == []


def test_direct_bootstrap_passes_explicit_trusted_local_identity(monkeypatch):
    from odibi_mcp import bootstrap

    identities = []

    def capture_dispatch(self, action, *args, application_identity=None, **kwargs):
        identities.append(application_identity)
        return {"action": action}

    monkeypatch.setattr(bootstrap.OdibiDispatcher, "dispatch", capture_dispatch)
    odibi, _ = bootstrap.init()

    assert odibi("test_pipeline") == {"action": "test_pipeline"}
    assert len(identities) == 1
    assert identities[0].authorizes(ActionEffect.EXECUTION)


@pytest.mark.parametrize("sample_size", [1, 37, 1000])
def test_registered_test_pipeline_binds_bounded_mode_by_keyword(monkeypatch, sample_size):
    calls = []
    fake_execution = ModuleType("tools.execution")

    def fake_test_pipeline(yaml_content, *, mode, max_rows):
        calls.append({"yaml_content": yaml_content, "mode": mode, "max_rows": max_rows})
        return {"valid": True, "mode": mode}

    fake_execution.test_pipeline = fake_test_pipeline
    monkeypatch.setitem(sys.modules, "tools.execution", fake_execution)

    result = D.dispatch(
        "test_pipeline",
        pipeline=VALID_PIPELINE_YAML,
        sample_size=sample_size,
        application_identity=LOCAL_IDENTITY,
    )

    assert result == {"valid": True, "mode": "dry-run"}
    assert calls == [
        {
            "yaml_content": VALID_PIPELINE_YAML,
            "mode": "dry-run",
            "max_rows": sample_size,
        }
    ]


def test_registered_test_pipeline_uses_bounded_defaults(monkeypatch):
    calls = []
    fake_execution = ModuleType("tools.execution")

    def fake_test_pipeline(yaml_content, *, mode, max_rows):
        calls.append((yaml_content, mode, max_rows))
        return {"valid": True, "mode": mode}

    fake_execution.test_pipeline = fake_test_pipeline
    monkeypatch.setitem(sys.modules, "tools.execution", fake_execution)

    result = D.dispatch(
        "test_pipeline",
        pipeline=VALID_PIPELINE_YAML,
        application_identity=LOCAL_IDENTITY,
    )

    assert result == {"valid": True, "mode": "dry-run"}
    assert calls == [(VALID_PIPELINE_YAML, "dry-run", 100)]


@pytest.mark.parametrize(
    "unexpected_kwargs",
    [
        {"mode": "sample"},
        {"max_rows": 37},
        {"sample_size": 37, "max_rows": 38},
    ],
)
def test_registered_test_pipeline_rejects_control_overrides_before_helper(
    monkeypatch, unexpected_kwargs
):
    fake_execution = ModuleType("tools.execution")

    def unexpected_call(*args, **kwargs):
        pytest.fail("registered test_pipeline must not expose helper-native controls")

    fake_execution.test_pipeline = unexpected_call
    monkeypatch.setitem(sys.modules, "tools.execution", fake_execution)

    result = D.dispatch(
        "test_pipeline",
        pipeline=VALID_PIPELINE_YAML,
        application_identity=LOCAL_IDENTITY,
        **unexpected_kwargs,
    )

    assert "error" in result


def test_help_describes_registered_test_pipeline_as_dry_run_only():
    validation_help = D.help(category="Validation")
    test_help = next(
        action for action in validation_help["actions"] if action["name"] == "test_pipeline"
    )

    assert test_help["signature"] == "pipeline, sample_size=100"
    assert "never performs ordinary pipeline execution" in test_help["description"]


@pytest.mark.parametrize("sample_size", [True, 1.5, "10", 0, -1, 1001])
def test_registered_test_pipeline_rejects_invalid_bounds_before_helper(monkeypatch, sample_size):
    fake_execution = ModuleType("tools.execution")

    def unexpected_call(*args, **kwargs):
        pytest.fail("execution helper must not be called for an invalid sample_size")

    fake_execution.test_pipeline = unexpected_call
    monkeypatch.setitem(sys.modules, "tools.execution", fake_execution)

    result = D.dispatch(
        "test_pipeline",
        pipeline=VALID_PIPELINE_YAML,
        sample_size=sample_size,
        application_identity=LOCAL_IDENTITY,
    )

    assert "error" in result


def test_execution_helper_builds_exact_dry_run_command(monkeypatch):
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout="dry-run plan", stderr="")

    monkeypatch.setattr(execution.subprocess, "run", fake_run)

    result = execution.test_pipeline(VALID_PIPELINE_YAML, mode="dry-run", max_rows=37)

    command, kwargs = calls[0]
    temp_path = command[-2]
    assert command == [
        sys.executable,
        "-m",
        "odibi",
        "run",
        temp_path,
        "--dry-run",
    ]
    assert kwargs == {
        "capture_output": True,
        "text": True,
        "timeout": 30,
        "cwd": execution.Path.cwd(),
    }
    assert result["valid"] is True
    assert result["mode"] == "dry-run"
    assert not execution.Path(temp_path).exists()


@pytest.mark.parametrize(
    "kwargs, error_type",
    [
        ({"mode": "sample"}, ValueError),
        ({"mode": "run"}, ValueError),
        ({"mode": 1}, ValueError),
        ({"max_rows": True}, TypeError),
        ({"max_rows": 1.5}, TypeError),
        ({"max_rows": "10"}, TypeError),
        ({"max_rows": 0}, ValueError),
        ({"max_rows": -1}, ValueError),
        ({"max_rows": 1001}, ValueError),
    ],
)
def test_execution_helper_rejects_invalid_controls_before_downstream_calls(
    monkeypatch, kwargs, error_type
):
    def unexpected_call(*args, **call_kwargs):
        pytest.fail("invalid controls must fail before parser, tempfile, or subprocess calls")

    monkeypatch.setattr(execution.yaml, "safe_load", unexpected_call)
    monkeypatch.setattr(execution.tempfile, "NamedTemporaryFile", unexpected_call)
    monkeypatch.setattr(execution.subprocess, "run", unexpected_call)

    with pytest.raises(error_type):
        execution.test_pipeline(VALID_PIPELINE_YAML, **kwargs)


def test_execution_helper_rejects_positional_mode_before_downstream_calls(monkeypatch):
    def unexpected_call(*args, **kwargs):
        pytest.fail("positional ambiguity must fail before parser, tempfile, or subprocess calls")

    monkeypatch.setattr(execution.yaml, "safe_load", unexpected_call)
    monkeypatch.setattr(execution.tempfile, "NamedTemporaryFile", unexpected_call)
    monkeypatch.setattr(execution.subprocess, "run", unexpected_call)

    with pytest.raises(TypeError):
        execution.test_pipeline(VALID_PIPELINE_YAML, "dry-run")
