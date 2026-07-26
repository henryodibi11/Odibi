"""Guards for the OdibiDispatcher facade (odibi_execute/odibi_help backend).

The dispatcher routes 43 actions; the onboarding/discovery actions delegate to the
OdibiKnowledge singleton. A prior version called get_knowledge("onboard") (wrong API),
which threw TypeError and silently broke ALL discovery — the catalog still listed the
actions, so a help-only test passed. This test actually DISPATCHES each action and
asserts it returns real data, so that regression can't recur.
"""

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from odibi_mcp.contracts.access import (
    RUNTIME_DATA_ACTIONS,
    ActionEffect,
    ApplicationIdentity,
    ManagedProjectAccess,
)
from odibi_mcp.dispatcher import ACTION_EFFECTS, _REMOTE_SAFE_WORKFLOWS, OdibiDispatcher
from odibi_mcp.knowledge import OdibiKnowledge
from odibi_mcp.tools import execution, smart, story

D = OdibiDispatcher()
LOCAL_IDENTITY = ApplicationIdentity.trusted_local()
REMOTE_IDENTITY = ApplicationIdentity.authenticated_application()

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
    ("get_doc", {"doc_path": "docs/patterns/scd2.md"}, "content"),
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


@pytest.fixture
def corpus_dispatcher(tmp_path, monkeypatch):
    """Dispatcher backed by a minimal configured-root-shaped corpus."""
    nested_doc = tmp_path / "docs" / "guides" / "nested" / "topic.md"
    nested_doc.parent.mkdir(parents=True)
    nested_doc.write_text("# Nested topic\n\nContained documentation.\n", encoding="utf-8")

    example = tmp_path / "docs" / "patterns" / "safe_example.md"
    example.parent.mkdir(parents=True)
    example.write_text(
        "# Safe example\n\nA contained example.\n\n```yaml\npipelines:\n  - pipeline: safe\n```\n",
        encoding="utf-8",
    )
    partial_example = tmp_path / "docs" / "patterns" / "prefix_partial_suffix.md"
    partial_example.write_text(
        "# Partial example\n\n```yaml\npipelines:\n  - pipeline: partial\n```\n",
        encoding="utf-8",
    )

    drive_trap = tmp_path / "docs" / "patterns" / "C:" / "outside.md"
    drive_trap.parent.mkdir(parents=True)
    drive_trap.write_text("drive-prefixed fallback trap", encoding="utf-8")
    relative_drive_trap = tmp_path / "docs" / "guides" / "C:outside.md"
    relative_drive_trap.parent.mkdir(parents=True, exist_ok=True)
    relative_drive_trap.write_text("relative-drive fallback trap", encoding="utf-8")
    ads_trap = tmp_path / "docs" / "guides" / "guide.md:secret.md"
    ads_trap.write_text("alternate data stream trap", encoding="utf-8")

    skill = tmp_path / ".assistant" / "skills" / "safe-skill" / "SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_text(
        "---\nname: safe-skill\ndescription: Contained skill\n---\nFollow safe guidance.\n",
        encoding="utf-8",
    )

    from odibi_mcp import knowledge

    monkeypatch.setattr(knowledge, "_knowledge", OdibiKnowledge(tmp_path))
    return OdibiDispatcher()


@pytest.fixture
def managed_dispatcher(tmp_path, monkeypatch):
    """Dispatcher with one explicit managed project and no context side effects."""
    root = tmp_path / "managed"
    (root / "data" / "folder").mkdir(parents=True)
    (root / "data" / "stories").mkdir()
    (root / "exports").mkdir()
    config = root / "odibi.yaml"
    config.write_text(
        """
project: managed
connections:
  local:
    type: local
    base_path: ./data
  sql:
    type: azure_sql
story:
  connection: local
  path: stories
system:
  connection: local
pipelines:
  - pipeline: bounded
    nodes: []
""".lstrip(),
        encoding="utf-8",
    )
    dispatcher = OdibiDispatcher(
        ManagedProjectAccess(
            project="managed",
            project_root=root,
            config_path=config,
            export_root=root / "exports",
        )
    )
    monkeypatch.setattr(dispatcher, "_bind_runtime_context", lambda config_path: None)
    return dispatcher, root


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


def test_corpus_content_handlers_preserve_documented_inputs(corpus_dispatcher):
    doc = corpus_dispatcher.dispatch(
        "get_doc",
        doc_path="docs/guides/nested/topic.md",
        application_identity=LOCAL_IDENTITY,
    )
    example = corpus_dispatcher.dispatch(
        "get_example", pattern_name="safe-example", application_identity=LOCAL_IDENTITY
    )
    partial_example = corpus_dispatcher.dispatch(
        "get_example", pattern_name="partial", application_identity=LOCAL_IDENTITY
    )
    skill = corpus_dispatcher.dispatch(
        "get_skill", name="safe-skill", application_identity=LOCAL_IDENTITY
    )

    assert doc == {
        "path": "docs/guides/nested/topic.md",
        "content": "# Nested topic\n\nContained documentation.\n",
    }
    assert example["source"] == "docs/patterns/safe_example.md"
    assert example["yaml"].startswith("pipelines:")
    assert partial_example["source"] == "docs/patterns/prefix_partial_suffix.md"
    assert partial_example["yaml"].startswith("pipelines:")
    assert skill["content"] == "Follow safe guidance.\n"


def test_odibi_docs_root_keeps_corpus_tree_semantics(tmp_path, monkeypatch):
    doc = tmp_path / "docs" / "guides" / "configured.md"
    doc.parent.mkdir(parents=True)
    doc.write_text("configured corpus", encoding="utf-8")
    monkeypatch.setenv("ODIBI_DOCS_ROOT", str(tmp_path))

    from odibi_mcp import knowledge

    monkeypatch.setattr(knowledge, "_knowledge", None)
    result = OdibiDispatcher().dispatch(
        "get_doc",
        doc_path="guides/configured.md",
        application_identity=LOCAL_IDENTITY,
    )

    assert result == {
        "path": "docs/guides/configured.md",
        "content": "configured corpus",
    }


def test_relative_odibi_docs_root_supports_exact_example(tmp_path, monkeypatch):
    example = tmp_path / "docs" / "patterns" / "configured_example.md"
    example.parent.mkdir(parents=True)
    example.write_text(
        "# Configured\n\n```yaml\npipelines:\n  - pipeline: configured\n```\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path.parent)
    monkeypatch.setenv("ODIBI_DOCS_ROOT", tmp_path.name)

    from odibi_mcp import knowledge

    monkeypatch.setattr(knowledge, "_knowledge", None)
    result = OdibiDispatcher().dispatch(
        "get_example",
        pattern_name="configured-example",
        application_identity=LOCAL_IDENTITY,
    )

    assert result["source"] == "docs/patterns/configured_example.md"


def test_symlinked_odibi_docs_root_supports_exact_example(tmp_path, monkeypatch):
    corpus = tmp_path / "corpus"
    example = corpus / "docs" / "patterns" / "linked_example.md"
    example.parent.mkdir(parents=True)
    example.write_text(
        "# Linked\n\n```yaml\npipelines:\n  - pipeline: linked\n```\n",
        encoding="utf-8",
    )
    root_link = tmp_path / "configured-root"
    try:
        root_link.symlink_to(corpus, target_is_directory=True)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"symlink creation is genuinely unavailable: {error!r}")
    monkeypatch.setenv("ODIBI_DOCS_ROOT", str(root_link))

    from odibi_mcp import knowledge

    monkeypatch.setattr(knowledge, "_knowledge", None)
    result = OdibiDispatcher().dispatch(
        "get_example", pattern_name="linked-example", application_identity=LOCAL_IDENTITY
    )

    assert result["source"] == "docs/patterns/linked_example.md"


def test_configured_docs_directory_symlink_is_the_operator_owned_root(tmp_path, monkeypatch):
    configured_root = tmp_path / "configured-root"
    configured_root.mkdir()
    owned_docs = tmp_path / "owned-docs"
    example = owned_docs / "patterns" / "operator_owned.md"
    example.parent.mkdir(parents=True)
    example.write_text(
        "# Operator owned\n\n```yaml\npipelines:\n  - pipeline: owned\n```\n",
        encoding="utf-8",
    )
    doc = owned_docs / "guides" / "operator-owned.md"
    doc.parent.mkdir()
    doc.write_text("operator-owned documentation", encoding="utf-8")
    try:
        (configured_root / "docs").symlink_to(owned_docs, target_is_directory=True)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"symlink creation is genuinely unavailable: {error!r}")
    monkeypatch.setenv("ODIBI_DOCS_ROOT", str(configured_root))

    from odibi_mcp import knowledge

    monkeypatch.setattr(knowledge, "_knowledge", None)
    result = OdibiDispatcher().dispatch(
        "get_example", pattern_name="operator-owned", application_identity=LOCAL_IDENTITY
    )
    doc_result = OdibiDispatcher().dispatch(
        "get_doc",
        doc_path="docs/guides/operator-owned.md",
        application_identity=LOCAL_IDENTITY,
    )

    assert result["source"] == "docs/patterns/operator_owned.md"
    assert doc_result == {
        "path": "docs/guides/operator-owned.md",
        "content": "operator-owned documentation",
    }


@pytest.mark.parametrize(
    "action,kwargs,expected_error",
    [
        ("get_doc", {"doc_path": "../outside.md"}, "Documentation file is unavailable."),
        ("get_doc", {"doc_path": "/tmp/outside.md"}, "Documentation file is unavailable."),
        ("get_doc", {"doc_path": "C:/outside.md"}, "Documentation file is unavailable."),
        ("get_doc", {"doc_path": "C:outside.md"}, "Documentation file is unavailable."),
        ("get_doc", {"doc_path": "//server/share/doc.md"}, "Documentation file is unavailable."),
        (
            "get_doc",
            {"doc_path": "docs/guides\\..\\outside.md"},
            "Documentation file is unavailable.",
        ),
        (
            "get_doc",
            {"doc_path": "docs/guides/nested"},
            "Documentation file is unavailable.",
        ),
        (
            "get_doc",
            {"doc_path": "docs/guides/missing.md"},
            "Documentation file is unavailable.",
        ),
        (
            "get_doc",
            {"doc_path": "docs/guides/nested/topic.txt"},
            "Documentation file is unavailable.",
        ),
        (
            "get_doc",
            {"doc_path": "docs/guides/guide.md:secret.md"},
            "Documentation file is unavailable.",
        ),
        ("get_example", {"pattern_name": "../../../outside"}, "Example is unavailable."),
        ("get_example", {"pattern_name": "/tmp/outside"}, "Example is unavailable."),
        ("get_example", {"pattern_name": "safe*"}, "Example is unavailable."),
        ("get_example", {"pattern_name": "safe\\outside"}, "Example is unavailable."),
        ("get_skill", {"name": "../../../outside"}, "Skill is unavailable."),
        ("get_skill", {"name": "/tmp/outside"}, "Skill is unavailable."),
        ("get_skill", {"name": "safe/skill"}, "Skill is unavailable."),
        ("get_skill", {"name": "safe\\skill"}, "Skill is unavailable."),
        ("get_skill", {"name": "missing-skill"}, "Skill is unavailable."),
    ],
)
def test_corpus_content_denials_happen_before_read(
    corpus_dispatcher, monkeypatch, action, kwargs, expected_error
):
    def unexpected_read(*args, **kwargs):
        pytest.fail("denied package content must not be read")

    monkeypatch.setattr(Path, "read_text", unexpected_read)

    result = corpus_dispatcher.dispatch(action, application_identity=LOCAL_IDENTITY, **kwargs)

    assert result["error"] == expected_error
    assert "content" not in result
    if action == "get_example":
        assert "escaped" not in result.get("available", [])
    assert not any(value in str(result) for value in kwargs.values())


@pytest.mark.parametrize("action", ["get_doc", "get_example", "get_skill"])
def test_corpus_content_symlink_escape_denied_before_read(
    corpus_dispatcher, tmp_path, monkeypatch, action
):
    outside = tmp_path.parent / f"outside-{action}.md"
    outside.write_text(
        "---\nname: escaped\n---\n# Outside\n\n```yaml\npipelines: []\n```\n",
        encoding="utf-8",
    )

    if action == "get_doc":
        link = tmp_path / "docs" / "guides" / "escaped.md"
        kwargs = {"doc_path": "docs/guides/escaped.md"}
        expected_error = "Documentation file is unavailable."
    elif action == "get_example":
        link = tmp_path / "docs" / "patterns" / "escaped.md"
        kwargs = {"pattern_name": "escaped"}
        expected_error = "Example is unavailable."
    else:
        link = tmp_path / ".assistant" / "skills" / "escaped" / "SKILL.md"
        link.parent.mkdir(parents=True)
        kwargs = {"name": "escaped"}
        expected_error = "Skill is unavailable."

    try:
        link.symlink_to(outside)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"symlink creation is genuinely unavailable: {error!r}")

    original_read = Path.read_text

    def guarded_read(path, *args, **kwargs):
        if path == outside:
            pytest.fail("escaped symlink content must not be read")
        return original_read(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", guarded_read)
    result = corpus_dispatcher.dispatch(action, application_identity=LOCAL_IDENTITY, **kwargs)

    assert result["error"] == expected_error
    assert "content" not in result
    if action == "get_example":
        assert "escaped" not in result.get("available", [])


def test_get_doc_descendant_directory_symlink_escape_denied_before_read(tmp_path, monkeypatch):
    docs_root = tmp_path / "corpus" / "docs"
    docs_root.mkdir(parents=True)
    outside = tmp_path / "outside-patterns"
    outside.mkdir()
    (outside / "escaped.md").write_text("outside content", encoding="utf-8")
    try:
        (docs_root / "patterns").symlink_to(outside, target_is_directory=True)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"symlink creation is genuinely unavailable: {error!r}")

    from odibi_mcp import knowledge

    monkeypatch.setattr(knowledge, "_knowledge", OdibiKnowledge(tmp_path / "corpus"))

    def unexpected_read(*args, **kwargs):
        pytest.fail("escaped descendant directory content must not be read")

    monkeypatch.setattr(Path, "read_text", unexpected_read)
    result = OdibiDispatcher().dispatch(
        "get_doc", doc_path="escaped.md", application_identity=LOCAL_IDENTITY
    )

    assert result == {"error": "Documentation file is unavailable."}


def test_get_example_escaped_directory_exposes_no_names_or_match_oracle(tmp_path, monkeypatch):
    docs_root = tmp_path / "corpus" / "docs"
    docs_root.mkdir(parents=True)
    outside = tmp_path / "outside-guides"
    outside.mkdir()
    (outside / "external_match.md").write_text(
        "# External\n\n```yaml\npipelines: []\n```\n", encoding="utf-8"
    )
    try:
        guides_link = docs_root / "guides"
        guides_link.symlink_to(outside, target_is_directory=True)
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"symlink creation is genuinely unavailable: {error!r}")

    from odibi_mcp import knowledge

    monkeypatch.setattr(knowledge, "_knowledge", OdibiKnowledge(tmp_path / "corpus"))

    def unexpected_read(*args, **kwargs):
        pytest.fail("escaped directory content must not be read")

    original_iterdir = Path.iterdir
    original_glob = Path.glob
    original_exists = Path.exists

    def guarded_iterdir(path, *args, **kwargs):
        if path in {guides_link, outside}:
            pytest.fail("escaped docs directory must not be enumerated")
        return original_iterdir(path, *args, **kwargs)

    def guarded_glob(path, *args, **kwargs):
        if path in {guides_link, outside}:
            pytest.fail("escaped docs directory must not be globbed")
        return original_glob(path, *args, **kwargs)

    def guarded_exists(path, *args, **kwargs):
        if (
            path == guides_link
            or guides_link in path.parents
            or path == outside
            or outside in path.parents
        ):
            pytest.fail("escaped docs directory must not be probed with exists")
        return original_exists(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", unexpected_read)
    monkeypatch.setattr(Path, "iterdir", guarded_iterdir)
    monkeypatch.setattr(Path, "glob", guarded_glob)
    monkeypatch.setattr(Path, "exists", guarded_exists)
    dispatcher = OdibiDispatcher()
    matching = dispatcher.dispatch(
        "get_example", pattern_name="external-match", application_identity=LOCAL_IDENTITY
    )
    nonmatching = dispatcher.dispatch(
        "get_example", pattern_name="external-absent", application_identity=LOCAL_IDENTITY
    )

    assert matching == nonmatching
    assert matching["error"] == "Example is unavailable."
    assert "external_match" not in matching.get("available", [])


def test_missing_skill_ignores_escaped_and_dangling_inventory_entries(
    corpus_dispatcher, tmp_path, monkeypatch
):
    outside = tmp_path.parent / "outside-skill.md"
    outside.write_text("outside skill", encoding="utf-8")
    escaped = tmp_path / ".assistant" / "skills" / "escaped" / "SKILL.md"
    dangling = tmp_path / ".assistant" / "skills" / "dangling" / "SKILL.md"
    escaped.parent.mkdir(parents=True)
    dangling.parent.mkdir(parents=True)
    try:
        escaped.symlink_to(outside)
        dangling.symlink_to(tmp_path.parent / "does-not-exist.md")
    except (NotImplementedError, OSError) as error:
        pytest.skip(f"symlink creation is genuinely unavailable: {error!r}")

    def unexpected_read(*args, **kwargs):
        pytest.fail("missing or escaped skill content must not be read")

    monkeypatch.setattr(Path, "read_text", unexpected_read)
    result = corpus_dispatcher.dispatch(
        "get_skill", name="missing-skill", application_identity=LOCAL_IDENTITY
    )

    assert result == {"error": "Skill is unavailable.", "available": ["safe-skill"]}


@pytest.mark.parametrize(
    "action,kwargs,expected_error,target_name",
    [
        (
            "get_doc",
            {"doc_path": "docs/guides/nested/topic.md"},
            "Documentation file is unavailable.",
            "topic.md",
        ),
        (
            "get_example",
            {"pattern_name": "safe-example"},
            "Example is unavailable.",
            "safe_example.md",
        ),
        ("get_skill", {"name": "safe-skill"}, "Skill is unavailable.", "SKILL.md"),
    ],
)
@pytest.mark.parametrize("error_type", [OSError, RuntimeError, ValueError])
def test_corpus_content_read_failures_are_sanitized(
    corpus_dispatcher, monkeypatch, action, kwargs, expected_error, target_name, error_type
):
    original_read = Path.read_text

    def failing_read(path, *args, **read_kwargs):
        if path.name == target_name:
            raise error_type("generated failure at /sensitive/host/corpus")
        return original_read(path, *args, **read_kwargs)

    monkeypatch.setattr(Path, "read_text", failing_read)
    result = corpus_dispatcher.dispatch(action, application_identity=LOCAL_IDENTITY, **kwargs)

    assert result == {"error": expected_error}
    assert "/sensitive/host/corpus" not in str(result)


@pytest.mark.parametrize("error_type", [OSError, RuntimeError, ValueError])
def test_skill_inventory_failures_are_sanitized(corpus_dispatcher, monkeypatch, error_type):
    def failing_iterdir(path):
        raise error_type("generated inventory failure at /sensitive/host/skills")

    monkeypatch.setattr(Path, "iterdir", failing_iterdir)
    result = corpus_dispatcher.dispatch(
        "get_skill", name="not-present", application_identity=LOCAL_IDENTITY
    )

    assert result == {"error": "Skill is unavailable.", "available": []}
    assert "/sensitive/host/skills" not in str(result)


def test_example_directory_failure_is_sanitized(corpus_dispatcher, monkeypatch):
    def failing_iterdir(path):
        raise OSError("generated failure at /sensitive/host/docs")

    monkeypatch.setattr(Path, "iterdir", failing_iterdir)
    result = corpus_dispatcher.dispatch(
        "get_example", pattern_name="not_present", application_identity=LOCAL_IDENTITY
    )

    assert result["error"] == "Example is unavailable."
    assert "/sensitive/host/docs" not in str(result)


@pytest.mark.parametrize("error_type", [OSError, RuntimeError, ValueError])
def test_example_resolution_probe_failure_is_sanitized(corpus_dispatcher, monkeypatch, error_type):
    original_resolve = Path.resolve

    def failing_resolve(path, *args, **kwargs):
        if path.name == "probe_failure.md":
            raise error_type("generated probe failure at /sensitive/host/docs")
        return original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", failing_resolve)
    result = corpus_dispatcher.dispatch(
        "get_example", pattern_name="probe-failure", application_identity=LOCAL_IDENTITY
    )

    assert result["error"] == "Example is unavailable."
    assert "/sensitive/host/docs" not in str(result)


EXPECTED_EFFECT_ACTIONS = {
    ActionEffect.PUBLIC_READ: {
        "list_workflows",
        "get_workflow",
        "list_transformers",
        "list_patterns",
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
        "apply_pattern_template",
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


def test_anonymous_pattern_template_denies_before_construction_render(monkeypatch):
    """Tripwire the delegate that can read and mutate shared project context."""
    fake_construction = ModuleType("tools.construction")

    def unexpected_apply_pattern_template(*args, **kwargs):
        pytest.fail("authorization must deny before construction reaches render_runnable_yaml")

    fake_construction.apply_pattern_template = unexpected_apply_pattern_template
    monkeypatch.setitem(sys.modules, "tools.construction", fake_construction)

    result = OdibiDispatcher().dispatch(
        "apply_pattern_template",
        pattern="fact",
        table_name="orders",
        connection="project_connection",
        source_path="orders",
    )

    assert result == {
        "error": "Application identity is required for this action",
        "code": "AUTHORIZATION_REQUIRED",
        "action": "apply_pattern_template",
        "effect": "sensitive_read",
    }


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
def test_identity_authorized_for_effect_still_requires_runtime_project_scope(action, effect):
    dispatcher = OdibiDispatcher()
    calls = []
    dispatcher._actions[action] = lambda: calls.append(action) or {"allowed": True}
    identity = ApplicationIdentity(
        subject="bounded-application", authorized_effects=frozenset({effect})
    )

    result = dispatcher.dispatch(action, application_identity=identity)

    if action in {"profile_source", "download_file"}:
        assert result["code"] == "PROJECT_SCOPE_REQUIRED"
        assert calls == []
    else:
        assert result == {"allowed": True}
        assert calls == [action]


@pytest.mark.parametrize(
    "action",
    [
        "map_environment",
        "profile_source",
        "profile_folder",
        "suggest_pipeline",
        "story_read",
        "node_sample",
        "node_failed_rows",
        "lineage_graph",
        "diagnose",
        "download_sql",
        "download_table",
        "download_file",
    ],
)
def test_valid_remote_identity_without_managed_scope_denies_before_all_effects(action):
    dispatcher = OdibiDispatcher()

    def unexpected_handler(*args, **kwargs):
        pytest.fail(
            "project denial must precede helper import, filesystem access, connection/query, "
            "subprocess, pipeline, and write effects"
        )

    dispatcher._actions[action] = unexpected_handler

    result = dispatcher.dispatch(action, application_identity=REMOTE_IDENTITY)

    assert result == {
        "error": "A valid managed project scope is required for this action",
        "code": "PROJECT_SCOPE_REQUIRED",
        "action": action,
    }


@pytest.mark.parametrize("action", sorted(RUNTIME_DATA_ACTIONS))
def test_trusted_local_runtime_actions_do_not_require_remote_project_arguments(action):
    dispatcher = OdibiDispatcher()
    calls = []
    dispatcher._actions[action] = lambda: calls.append(action) or {"allowed": True}

    result = dispatcher.dispatch(action, application_identity=LOCAL_IDENTITY)

    assert result == {"allowed": True}
    assert calls == [action]


@pytest.mark.parametrize(
    "action,kwargs",
    [
        (
            "profile_source",
            {"project": "managed", "connection": "local", "path": "../outside.csv"},
        ),
        (
            "profile_folder",
            {"project": "managed", "connection": "local", "folder_path": "/outside"},
        ),
        (
            "download_file",
            {
                "project": "managed",
                "connection": "local",
                "source_path": r"folder\outside.csv",
                "filename": "outside.csv",
            },
        ),
    ],
)
def test_remote_path_denials_precede_handler_and_effects(managed_dispatcher, action, kwargs):
    dispatcher, _ = managed_dispatcher
    dispatcher._actions[action] = lambda **call_kwargs: pytest.fail(
        "invalid remote path reached a handler or effect"
    )

    result = dispatcher.dispatch(action, application_identity=REMOTE_IDENTITY, **kwargs)

    assert result["code"] == "PATH_SCOPE_REQUIRED"


def test_remote_profile_source_preserves_exact_bounded_helper_kwargs(
    managed_dispatcher, monkeypatch
):
    dispatcher, _ = managed_dispatcher
    calls = []
    fake_smart = ModuleType("odibi_mcp.tools.smart")

    def fake_profile_source(**kwargs):
        calls.append(kwargs)
        return {"path": kwargs["path"], "sample_rows": []}

    fake_smart.profile_source = fake_profile_source
    monkeypatch.setitem(sys.modules, "odibi_mcp.tools.smart", fake_smart)

    result = dispatcher.dispatch(
        "profile_source",
        project="managed",
        connection="local",
        path="folder/input.csv",
        max_rows=37,
        application_identity=REMOTE_IDENTITY,
    )

    assert calls == [
        {
            "connection": "local",
            "path": "folder/input.csv",
            "max_attempts": 5,
            "use_cache": False,
            "sample_rows": 37,
        }
    ]
    assert result == {
        "path": "folder/input.csv",
        "sample_rows": [],
        "truncated": True,
        "truncated_reason": "sampling_only",
        "policy_applied": {"project_scoped": True, "sample_capped": True},
    }


def test_remote_profile_folder_preserves_exact_bounded_helper_kwargs(
    managed_dispatcher, monkeypatch
):
    dispatcher, _ = managed_dispatcher
    calls = []
    fake_smart = ModuleType("odibi_mcp.tools.smart")

    def fake_profile_folder(**kwargs):
        calls.append(kwargs)
        return {"total_files": 25, "profiled_count": 7, "file_profiles": []}

    fake_smart.profile_folder = fake_profile_folder
    monkeypatch.setitem(sys.modules, "odibi_mcp.tools.smart", fake_smart)

    result = dispatcher.dispatch(
        "profile_folder",
        project="managed",
        connection="local",
        folder_path="folder",
        pattern="*.csv",
        max_files=7,
        application_identity=REMOTE_IDENTITY,
    )

    assert calls == [
        {
            "connection": "local",
            "folder_path": "folder",
            "pattern": "*.csv",
            "max_files": 7,
        }
    ]
    assert result["truncated"] is True
    assert result["truncated_reason"] == "file_limit"
    assert result["policy_applied"] == {"project_scoped": True, "sample_capped": True}


def test_remote_map_environment_rejects_inline_connection_before_delegate(managed_dispatcher):
    dispatcher, _ = managed_dispatcher
    dispatcher._actions["map_environment"] = lambda **kwargs: pytest.fail(
        "inline connection reached catalog enumeration or outbound access"
    )

    result = dispatcher.dispatch(
        "map_environment",
        project="managed",
        connection={"type": "http", "base_url": "http://private.invalid"},
        application_identity=REMOTE_IDENTITY,
    )

    assert result["code"] == "INVALID_RUNTIME_ARGUMENT"


def test_remote_map_environment_preserves_bounded_helper_kwargs(managed_dispatcher, monkeypatch):
    dispatcher, _ = managed_dispatcher
    calls = []
    fake_smart = ModuleType("odibi_mcp.tools.smart")

    def fake_map_environment(**kwargs):
        calls.append(kwargs)
        return {"connection": kwargs["connection"], "structure": [], "errors": []}

    fake_smart.map_environment = fake_map_environment
    monkeypatch.setitem(sys.modules, "odibi_mcp.tools.smart", fake_smart)

    result = dispatcher.dispatch(
        "map_environment",
        project="managed",
        connection="local",
        path="folder",
        pattern="*.csv",
        limit=29,
        application_identity=REMOTE_IDENTITY,
    )

    assert calls == [{"connection": "local", "path": "folder", "pattern": "*.csv", "limit": 29}]
    assert result["policy_applied"] == {
        "project_scoped": True,
        "enumeration_capped": True,
    }


def test_remote_suggest_pipeline_profiles_with_bounded_kwargs(managed_dispatcher, monkeypatch):
    dispatcher, _ = managed_dispatcher
    calls = []
    fake_phase3 = ModuleType("odibi_mcp.tools.phase3_smart")

    def fake_suggest_pipeline(**kwargs):
        calls.append(kwargs)
        return {"suggested_pattern": "dimension"}

    fake_phase3.suggest_pipeline = fake_suggest_pipeline
    monkeypatch.setitem(sys.modules, "odibi_mcp.tools.phase3_smart", fake_phase3)

    result = dispatcher.dispatch(
        "suggest_pipeline",
        project="managed",
        connection="local",
        source_path="folder/input.csv",
        intent="build a dimension",
        application_identity=REMOTE_IDENTITY,
    )

    assert calls == [
        {
            "connection": "local",
            "source_path": "folder/input.csv",
            "intent": "build a dimension",
            "sample_rows": 100,
        }
    ]
    assert result == {
        "suggested_pattern": "dimension",
        "policy_applied": {"project_scoped": True, "sample_capped": True},
    }


@pytest.mark.parametrize("action", ["node_sample", "node_failed_rows"])
def test_remote_story_rows_bind_limit_by_keyword(managed_dispatcher, monkeypatch, action):
    dispatcher, _ = managed_dispatcher
    calls = []
    fake_story = ModuleType("odibi_mcp.tools.story")

    def fake_story_rows(**kwargs):
        calls.append(kwargs)
        return {
            "pipeline": kwargs["pipeline"],
            "node": kwargs["node"],
            "rows": [{"value": 1}],
            "row_count": 1,
            "truncated": False,
            "truncated_reason": None,
            "error": None,
        }

    setattr(fake_story, action, fake_story_rows)
    monkeypatch.setitem(sys.modules, "odibi_mcp.tools.story", fake_story)

    result = dispatcher.dispatch(
        action,
        project="managed",
        pipeline="bounded",
        node="source",
        limit=23,
        application_identity=REMOTE_IDENTITY,
    )

    assert calls == [{"pipeline": "bounded", "node": "source", "limit": 23}]
    assert result["rows"] == [{"value": 1}]
    assert result["policy_applied"] == {"project_scoped": True}


def test_remote_story_read_preserves_selector_shape(managed_dispatcher, monkeypatch):
    dispatcher, _ = managed_dispatcher
    calls = []
    fake_story = ModuleType("odibi_mcp.tools.story")

    def fake_story_read(**kwargs):
        calls.append(kwargs)
        return {"pipeline": kwargs["pipeline"], "status": "success", "error_message": None}

    fake_story.story_read = fake_story_read
    monkeypatch.setitem(sys.modules, "odibi_mcp.tools.story", fake_story)

    result = dispatcher.dispatch(
        "story_read",
        project="managed",
        pipeline="bounded",
        run_id="run-2026",
        application_identity=REMOTE_IDENTITY,
    )

    assert calls == [{"pipeline": "bounded", "run_selector": {"run_id": "run-2026"}}]
    assert result == {
        "pipeline": "bounded",
        "status": "success",
        "error_message": None,
        "policy_applied": {"project_scoped": True},
    }


@pytest.mark.parametrize(
    "action,caller_kwargs,helper_kwargs",
    [
        (
            "download_sql",
            {
                "connection": "sql",
                "query": "  SELECT * FROM Orders  ",
                "filename": "orders.csv",
                "limit": 41,
            },
            {"connection": "sql", "query": "SELECT * FROM Orders", "limit": 41},
        ),
        (
            "download_table",
            {
                "connection": "sql",
                "table": "dbo.Orders",
                "filename": "orders.json",
                "limit": 42,
            },
            {"connection": "sql", "table": "dbo.Orders", "limit": 42},
        ),
        (
            "download_file",
            {
                "connection": "local",
                "source_path": "folder/orders.parquet",
                "filename": "orders.parquet",
            },
            {"connection": "local", "source_path": "folder/orders.parquet"},
        ),
    ],
)
def test_remote_downloads_use_controlled_output_and_exact_helper_kwargs(
    managed_dispatcher, monkeypatch, action, caller_kwargs, helper_kwargs
):
    dispatcher, root = managed_dispatcher
    calls = []
    fake_smart = ModuleType("odibi_mcp.tools.smart")

    def fake_download(**kwargs):
        calls.append(kwargs)
        return {
            "status": "success",
            "output_path": kwargs["output_path"],
            "rows_saved": 1,
            "truncated": False,
            "truncated_reason": None,
        }

    setattr(fake_smart, action, fake_download)
    monkeypatch.setitem(sys.modules, "odibi_mcp.tools.smart", fake_smart)

    result = dispatcher.dispatch(
        action,
        project="managed",
        application_identity=REMOTE_IDENTITY,
        **caller_kwargs,
    )

    output_path = root / "exports" / caller_kwargs["filename"]
    expected = {**helper_kwargs, "output_path": str(output_path)}
    if action in {"download_sql", "download_table"}:
        expected.update(
            {
                "max_bytes": 10 * 1024 * 1024,
                "exclusive": True,
                "report_truncation": True,
            }
        )
    else:
        expected.update({"max_bytes": 10 * 1024 * 1024, "exclusive": True})
    assert calls == [expected]
    assert result["output_path"] == f"exports/{caller_kwargs['filename']}"
    assert str(root) not in str(result)
    assert result["policy_applied"] == {"project_scoped": True}


def test_remote_lineage_fails_closed_but_trusted_local_remains_available(
    managed_dispatcher, monkeypatch
):
    dispatcher, _ = managed_dispatcher
    calls = []
    fake_story = ModuleType("odibi_mcp.tools.story")

    def fake_lineage_graph(**kwargs):
        calls.append(kwargs)
        return {"nodes": [{"id": "/physical/path"}], "edges": []}

    fake_story.lineage_graph = fake_lineage_graph
    monkeypatch.setitem(sys.modules, "odibi_mcp.tools.story", fake_story)

    remote = dispatcher.dispatch(
        "lineage_graph",
        project="managed",
        pipeline="bounded",
        application_identity=REMOTE_IDENTITY,
    )
    local = dispatcher.dispatch(
        "lineage_graph",
        pipeline="bounded",
        application_identity=LOCAL_IDENTITY,
    )

    assert remote["code"] == "PHYSICAL_REFERENCES_DISABLED"
    assert local == {"nodes": [{"id": "/physical/path"}], "edges": []}
    assert calls == [{"pipeline": "bounded"}]


@pytest.mark.parametrize(
    "action,kwargs",
    [
        ("run_workflow", {"workflow_name": "inspect_pipeline_run", "params": {}}),
        ("run_workflow", {"workflow_name": "debug_failed_run", "params": {}}),
        ("resume_workflow", {"resume_token": "caller-controlled"}),
    ],
)
def test_remote_workflow_indirections_deny_before_story_helper_import(action, kwargs):
    dispatcher = OdibiDispatcher()
    dispatcher._actions[action] = lambda **call_kwargs: pytest.fail(
        "remote workflow reached an indirect runtime-data helper"
    )

    result = dispatcher.dispatch(action, application_identity=REMOTE_IDENTITY, **kwargs)

    assert result == {
        "error": "This workflow is unavailable over the remote transport",
        "code": "REMOTE_WORKFLOW_DISABLED",
        "action": action,
    }


def test_non_runtime_remote_workflow_and_trusted_local_workflows_remain_available():
    dispatcher = OdibiDispatcher()
    calls = []
    dispatcher._actions["run_workflow"] = lambda **kwargs: calls.append(("run", kwargs)) or {
        "allowed": True
    }
    dispatcher._actions["resume_workflow"] = lambda **kwargs: calls.append(("resume", kwargs)) or {
        "allowed": True
    }

    safe_remote = dispatcher.dispatch(
        "run_workflow",
        workflow_name="validate_yaml_simple",
        params={},
        application_identity=REMOTE_IDENTITY,
    )
    local_run = dispatcher.dispatch(
        "run_workflow",
        workflow_name="inspect_pipeline_run",
        params={},
        application_identity=LOCAL_IDENTITY,
    )
    local_resume = dispatcher.dispatch(
        "resume_workflow",
        resume_token="trusted-local-token",
        application_identity=LOCAL_IDENTITY,
    )

    assert safe_remote == local_run == local_resume == {"allowed": True}
    assert calls == [
        ("run", {"workflow_name": "validate_yaml_simple", "params": {}}),
        ("run", {"workflow_name": "inspect_pipeline_run", "params": {}}),
        ("resume", {"resume_token": "trusted-local-token"}),
    ]


def test_remote_workflow_allowlist_contains_no_runtime_data_tool_steps():
    from odibi_mcp.tools.workflows import WORKFLOWS

    def tool_names(value):
        if isinstance(value, dict):
            names = {value["tool"]} if isinstance(value.get("tool"), str) else set()
            return names | set().union(*(tool_names(item) for item in value.values()), set())
        if isinstance(value, list):
            return set().union(*(tool_names(item) for item in value), set())
        return set()

    assert _REMOTE_SAFE_WORKFLOWS <= set(WORKFLOWS)
    for workflow_name in _REMOTE_SAFE_WORKFLOWS:
        assert tool_names(WORKFLOWS[workflow_name]).isdisjoint(RUNTIME_DATA_ACTIONS)


def test_remote_helper_failure_and_payload_are_sanitized(managed_dispatcher, monkeypatch):
    dispatcher, root = managed_dispatcher
    fake_smart = ModuleType("odibi_mcp.tools.smart")

    def fake_profile_source(**kwargs):
        return {
            "path": str(root / "data" / "private.csv"),
            "errors": [f"failed at {root}/private using token secret"],
            "password": "generated-secret",
        }

    fake_smart.profile_source = fake_profile_source
    monkeypatch.setitem(sys.modules, "odibi_mcp.tools.smart", fake_smart)

    result = dispatcher.dispatch(
        "profile_source",
        project="managed",
        connection="local",
        path="folder/input.csv",
        application_identity=REMOTE_IDENTITY,
    )

    assert result["path"] == "[physical reference withheld]"
    assert result["errors"] == ["Runtime data is unavailable."]
    assert result["password"] == "[redacted]"
    assert str(root) not in str(result)


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


def test_download_sql_reports_truncation_and_atomically_publishes(tmp_path, monkeypatch):
    import pandas as pd

    queries = []

    class AzureSQL:
        def read_sql(self, query):
            queries.append(query)
            return pd.DataFrame({"id": [1, 2, 3]})

    from odibi_mcp import context

    monkeypatch.setattr(context, "resolve_connection", lambda name: (AzureSQL(), name))
    output = tmp_path / "orders.csv"

    result = smart.download_sql(
        "sql",
        "SELECT * FROM Orders",
        str(output),
        limit=2,
        max_bytes=1024,
        exclusive=True,
        report_truncation=True,
    )

    assert queries == ["SELECT TOP 3 * FROM Orders"]
    assert result["status"] == "success"
    assert result["rows_saved"] == 2
    assert result["truncated"] is True
    assert result["truncated_reason"] == "row_limit"
    assert output.read_text(encoding="utf-8").splitlines() == ["id", "1", "2"]
    assert not list(tmp_path.glob(".orders.csv.*"))


def test_download_sql_collision_preserves_existing_file_and_cleans_temporary(tmp_path, monkeypatch):
    import pandas as pd

    class AzureSQL:
        def read_sql(self, query):
            return pd.DataFrame({"id": [1]})

    from odibi_mcp import context

    monkeypatch.setattr(context, "resolve_connection", lambda name: (AzureSQL(), name))
    output = tmp_path / "orders.csv"
    output.write_text("operator-owned\n", encoding="utf-8")

    result = smart.download_sql(
        "sql",
        "SELECT * FROM Orders",
        str(output),
        limit=1,
        max_bytes=1024,
        exclusive=True,
        report_truncation=True,
    )

    assert result["status"] == "error"
    assert output.read_text(encoding="utf-8") == "operator-owned\n"
    assert not list(tmp_path.glob(".orders.csv.*"))


def test_download_table_quotes_each_qualified_identifier_part(monkeypatch):
    calls = []
    monkeypatch.setattr(
        smart,
        "download_sql",
        lambda *args, **kwargs: calls.append((args, kwargs)) or {"status": "success"},
    )

    result = smart.download_table(
        "sql",
        "warehouse.sales.Orders",
        "orders.csv",
        limit=17,
        max_bytes=100,
        exclusive=True,
        report_truncation=True,
    )

    assert result == {"status": "success"}
    assert calls == [
        (
            ("sql", "SELECT * FROM [warehouse].[sales].[Orders]", "orders.csv", 17),
            {"max_bytes": 100, "exclusive": True, "report_truncation": True},
        )
    ]


def test_download_sql_byte_limit_publishes_no_partial_file(tmp_path, monkeypatch):
    import pandas as pd

    class AzureSQL:
        def read_sql(self, query):
            return pd.DataFrame({"payload": ["large-value"]})

    from odibi_mcp import context

    monkeypatch.setattr(context, "resolve_connection", lambda name: (AzureSQL(), name))
    output = tmp_path / "oversize.csv"

    result = smart.download_sql(
        "sql",
        "SELECT * FROM Orders",
        str(output),
        limit=1,
        max_bytes=1,
        exclusive=True,
        report_truncation=True,
    )

    assert result["status"] == "error"
    assert not output.exists()
    assert not list(tmp_path.glob(".oversize.csv.*"))


def test_download_file_streams_to_byte_cap_without_partial_publish(tmp_path, monkeypatch):
    source = tmp_path / "source.csv"
    source.write_bytes(b"0123456789")
    output = tmp_path / "bounded.csv"

    class LocalConnection:
        def get_path(self, path):
            return str(source)

    from odibi_mcp import context

    monkeypatch.setattr(context, "resolve_connection", lambda name: (LocalConnection(), name))

    result = smart.download_file(
        "local",
        "source.csv",
        str(output),
        max_bytes=5,
        exclusive=True,
    )

    assert result["status"] == "error"
    assert not output.exists()
    assert not list(tmp_path.glob(".bounded.csv.*"))


@pytest.mark.parametrize("function_name", ["node_sample", "node_failed_rows"])
def test_story_row_helpers_report_truthful_truncation(monkeypatch, function_name):
    monkeypatch.setattr(story, "get_project_context", lambda: None)
    monkeypatch.setattr(story, "_find_story_file", lambda *args, **kwargs: (object(), "story"))
    monkeypatch.setattr(
        story,
        "_load_story",
        lambda *args: {
            "nodes": [
                {
                    "name": "source",
                    "sample_output": [{"id": 1}, {"id": 2}, {"id": 3}],
                    "failed_rows": [{"id": 1}, {"id": 2}, {"id": 3}],
                }
            ]
        },
    )

    result = getattr(story, function_name)("bounded", "source", limit=2)

    assert result.row_count == 2
    assert result.truncated is True
    assert result.truncated_reason == "row_limit"
