"""Guards for the OdibiDispatcher facade (odibi_execute/odibi_help backend).

The dispatcher routes 43 actions; the onboarding/discovery actions delegate to the
OdibiKnowledge singleton. A prior version called get_knowledge("onboard") (wrong API),
which threw TypeError and silently broke ALL discovery — the catalog still listed the
actions, so a help-only test passed. This test actually DISPATCHES each action and
asserts it returns real data, so that regression can't recur.
"""

import base64
import io
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
import yaml

import odibi.planning as planning
from odibi_mcp.contracts.access import (
    RUNTIME_DATA_ACTIONS,
    ActionEffect,
    ApplicationIdentity,
    ManagedProjectAccess,
    PreparedRuntimeCall,
    RemoteLogicalLineageProjection,
)
from odibi_mcp.dispatcher import (
    ACTION_EFFECTS,
    _REMOTE_DISABLED_RENDERING_ACTIONS,
    _REMOTE_SAFE_WORKFLOWS,
    OdibiDispatcher,
)
from odibi_mcp.knowledge import OdibiKnowledge
from odibi_mcp.tools import execution, smart, story

D = OdibiDispatcher()
LOCAL_IDENTITY = ApplicationIdentity.trusted_local()
REMOTE_IDENTITY = ApplicationIdentity.authenticated_application()
RENDER_SENTINELS = (
    "UNIQUE_PASSWORD_SENTINEL_7f3b",
    "UNIQUE_CONNECTION_STRING_SENTINEL_8c4d",
    "UNIQUE_ACCOUNT_KEY_SENTINEL_9d5e",
    "UNIQUE_UNKNOWN_HOST_SENTINEL_a06f",
)
PROJECTION_SENTINELS = RENDER_SENTINELS + (
    "UNIQUE_UNKNOWN_NESTED_SCALAR_SENTINEL_b17a",
    "UNIQUE_BUILDER_SESSION_SENTINEL_c28b",
    "UNIQUE_WORKFLOW_EVENT_SENTINEL_d39c",
    "UNIQUE_REJECTED_RENDER_VALUE_SENTINEL_e40d",
)

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
    from odibi_mcp import dispatcher as dispatcher_module

    fake_construction = ModuleType("tools.construction")

    def unexpected_apply_pattern_template(*args, **kwargs):
        pytest.fail("authorization must deny before construction reaches render_runnable_yaml")

    def unexpected_projection(*args, **kwargs):
        pytest.fail("authorization must deny before remote projection validation")

    fake_construction.apply_pattern_template = unexpected_apply_pattern_template
    monkeypatch.setitem(sys.modules, "tools.construction", fake_construction)
    monkeypatch.setattr(dispatcher_module, "prepare_remote_pattern_render", unexpected_projection)

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
        "enumeration_capped": False,
        "enumeration_limit": 29,
    }


@pytest.mark.parametrize("limit", [1, 100])
def test_remote_map_environment_caps_many_schema_identifiers_at_requested_limit(
    managed_dispatcher, monkeypatch, limit
):
    from odibi_mcp import context as context_module

    dispatcher, _ = managed_dispatcher

    class ManySchemaConnection:
        def discover_catalog(self, **kwargs):
            assert kwargs["limit"] == limit
            tables = [
                {"name": f"table_{index}", "namespace": f"schema_{index}", "kind": "table"}
                for index in range(101)
            ]
            return {
                "connection_name": "sql",
                "connection_type": "azure_sql",
                "generated_at": datetime.now(timezone.utc),
                "tables": tables,
                "total_datasets": len(tables),
                "suggestions": [f"inspect schema_{index}" for index in range(101)],
            }

    monkeypatch.setattr(
        context_module.MCPProjectContext,
        "get_connection",
        lambda self, name: ManySchemaConnection(),
    )

    result = dispatcher.dispatch(
        "map_environment",
        project="managed",
        connection="sql",
        limit=limit,
        application_identity=REMOTE_IDENTITY,
    )

    assert "error" not in result
    assert len(result["structure"]) == limit
    assert len(result["suggested_sources"]) <= limit
    assert len(result["recommendations"]) <= limit
    assert result["truncated"] is True
    assert result["truncated_reason"] == "enumeration_limit"
    assert result["policy_applied"] == {
        "project_scoped": True,
        "enumeration_capped": True,
        "enumeration_limit": limit,
    }


@pytest.mark.parametrize(
    ("limit", "expected_capped"),
    [
        (1, True),
        (100, False),
    ],
)
def test_remote_map_environment_missing_sql_schema_never_embeds_available_inventory(
    managed_dispatcher, monkeypatch, limit, expected_capped
):
    from odibi.connections.azure_sql import AzureSQL
    from odibi_mcp import context as context_module

    dispatcher, _ = managed_dispatcher
    schema_names = [f"private_schema_{index:03d}_sentinel" for index in range(101)]
    connection = AzureSQL(server="managed.invalid", database="managed")
    monkeypatch.setattr(connection, "list_schemas", lambda: schema_names)
    monkeypatch.setattr(
        connection,
        "list_tables",
        lambda *args, **kwargs: pytest.fail("missing schema must not enumerate tables"),
    )
    monkeypatch.setattr(
        context_module.MCPProjectContext,
        "get_connection",
        lambda self, name: connection,
    )

    result = dispatcher.dispatch(
        "map_environment",
        project="managed",
        connection="sql",
        path="missing_schema",
        limit=limit,
        application_identity=REMOTE_IDENTITY,
    )

    serialized = json.dumps(result, sort_keys=True, default=str)
    disclosed_identifiers = [name for name in schema_names if name in serialized]
    assert "error" not in result
    assert result["next_step"] == "Schema 'missing_schema' not found"
    assert len(disclosed_identifiers) <= limit
    assert disclosed_identifiers == []
    assert result["truncated"] is expected_capped
    assert result["truncated_reason"] == ("enumeration_limit" if expected_capped else None)
    assert len(result["recommendations"]) == min(2, limit)
    assert result["policy_applied"] == {
        "project_scoped": True,
        "enumeration_capped": expected_capped,
        "enumeration_limit": limit,
    }


def test_remote_map_environment_reports_legacy_suggestion_display_cap(
    managed_dispatcher, monkeypatch
):
    from odibi_mcp import context as context_module

    dispatcher, _ = managed_dispatcher

    class ElevenSchemaConnection:
        def discover_catalog(self, **kwargs):
            assert kwargs["limit"] == 100
            return {
                "connection_name": "sql",
                "connection_type": "azure_sql",
                "tables": [
                    {
                        "name": f"table_{index}",
                        "namespace": f"schema_{index}",
                        "kind": "table",
                    }
                    for index in range(11)
                ],
                "total_datasets": 11,
            }

    monkeypatch.setattr(
        context_module.MCPProjectContext,
        "get_connection",
        lambda self, name: ElevenSchemaConnection(),
    )

    result = dispatcher.dispatch(
        "map_environment",
        project="managed",
        connection="sql",
        limit=100,
        application_identity=REMOTE_IDENTITY,
    )

    assert len(result["structure"]) == 11
    assert len(result["suggested_sources"]) == 10
    assert result["truncated"] is True
    assert result["truncated_reason"] == "enumeration_limit"
    assert result["policy_applied"]["enumeration_capped"] is True


def test_remote_map_environment_rejects_non_mapping_helper_result(managed_dispatcher):
    dispatcher, _ = managed_dispatcher
    dispatcher._actions["map_environment"] = lambda **kwargs: ["schema_a", "schema_b"]

    result = dispatcher.dispatch(
        "map_environment",
        project="managed",
        connection="sql",
        limit=1,
        application_identity=REMOTE_IDENTITY,
    )

    assert result == {
        "error": "Runtime data is unavailable",
        "code": "RUNTIME_DATA_UNAVAILABLE",
        "action": "map_environment",
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


def test_remote_cloud_story_actions_resolve_only_inside_configured_prefix(tmp_path, monkeypatch):
    from odibi_mcp import context as context_module

    root = tmp_path / "managed"
    (root / "exports").mkdir(parents=True)
    config_path = root / "odibi.yaml"
    config = {
        "project": "managed",
        "connections": {
            "cloud": {
                "type": "azure_adls",
                "account": "managedaccount",
                "container": "managed-container",
                "path_prefix": "tenant/managed",
                "account_key": RENDER_SENTINELS[2],
            }
        },
        "story": {"connection": "cloud", "path": "stories"},
        "system": {"connection": "cloud"},
        "pipelines": [{"pipeline": "bounded", "nodes": []}],
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    dispatcher = OdibiDispatcher(
        ManagedProjectAccess("managed", root, config_path, root / "exports")
    )
    expected_prefix = "managed-container/tenant/managed/stories"
    outside_sentinel = "CONTAINER_ROOT_OUTSIDE_PROJECT_SENTINEL"
    observed_paths = []
    story_document = {
        "status": "failed",
        "run_id": "run-1",
        "nodes": [
            {
                "name": "source",
                "status": "failed",
                "sample_output": [{"scope": "inside-prefix"}],
                "failed_rows": [{"scope": "inside-prefix-failed"}],
                "validations": [{"test": "not_null", "passed": False}],
            }
        ],
    }

    class CloudConnection:
        container = "managed-container"
        path_prefix = "tenant/managed"

        def pandas_storage_options(self):
            return {}

        def get_path(self, relative_path):
            assert relative_path == "stories"
            return (
                "abfss://managed-container@managedaccount.dfs.core.windows.net/"
                f"tenant/managed/{relative_path}"
            )

    class PrefixOnlyFilesystem:
        def _observe(self, path):
            observed_paths.append(path)
            if not path.startswith(expected_prefix):
                pytest.fail(f"story action escaped configured cloud prefix: {outside_sentinel}")

        def exists(self, path):
            self._observe(path)
            return True

        def isdir(self, path):
            self._observe(path)
            return not path.endswith("story.json")

        def ls(self, path):
            self._observe(path)
            if path.endswith("/bounded"):
                return [f"{path}/2026-07-26"]
            return [f"{path}/story.json"]

        def open(self, path, mode):
            self._observe(path)
            assert mode == "r"
            return io.StringIO(json.dumps(story_document))

    monkeypatch.setattr(
        context_module.MCPProjectContext,
        "get_connection",
        lambda self, name: CloudConnection(),
    )
    monkeypatch.setattr(story.fsspec, "filesystem", lambda *args, **kwargs: PrefixOnlyFilesystem())

    results = [
        dispatcher.dispatch(
            "story_read",
            project="managed",
            pipeline="bounded",
            application_identity=REMOTE_IDENTITY,
        ),
        dispatcher.dispatch(
            "node_sample",
            project="managed",
            pipeline="bounded",
            node="source",
            limit=1,
            application_identity=REMOTE_IDENTITY,
        ),
        dispatcher.dispatch(
            "node_failed_rows",
            project="managed",
            pipeline="bounded",
            node="source",
            limit=1,
            application_identity=REMOTE_IDENTITY,
        ),
    ]

    serialized = json.dumps(results, sort_keys=True)
    assert observed_paths
    assert all(path.startswith(expected_prefix) for path in observed_paths)
    assert outside_sentinel not in serialized
    assert RENDER_SENTINELS[2] not in serialized
    assert results[1]["rows"] == [{"scope": "inside-prefix"}]
    assert results[2]["rows"] == [{"scope": "inside-prefix-failed"}]


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


@pytest.mark.parametrize(
    "action,kwargs",
    [
        (
            "profile_folder",
            {"connection": "sql", "folder_path": "folder", "pattern": "*.csv"},
        ),
        (
            "download_file",
            {
                "connection": "sql",
                "source_path": "folder/orders.csv",
                "filename": "orders.csv",
            },
        ),
        (
            "download_sql",
            {
                "connection": "local",
                "query": "SELECT * FROM Orders",
                "filename": "orders.csv",
            },
        ),
        (
            "download_table",
            {"connection": "local", "table": "Orders", "filename": "orders.csv"},
        ),
    ],
)
def test_remote_actions_reject_wrong_connection_type_before_delegate(
    managed_dispatcher, action, kwargs
):
    dispatcher, _ = managed_dispatcher
    dispatcher._actions[action] = lambda **call_kwargs: pytest.fail(
        "wrong connection type reached a runtime-data effect"
    )

    result = dispatcher.dispatch(
        action,
        project="managed",
        application_identity=REMOTE_IDENTITY,
        **kwargs,
    )

    assert result["code"] == "PROJECT_SCOPE_REQUIRED"


def test_remote_download_file_rejects_format_conversion_before_delegate(managed_dispatcher):
    dispatcher, _ = managed_dispatcher
    dispatcher._actions["download_file"] = lambda **kwargs: pytest.fail(
        "download format mismatch reached a file read or write"
    )

    result = dispatcher.dispatch(
        "download_file",
        project="managed",
        connection="local",
        source_path="folder/orders.csv",
        filename="orders.parquet",
        application_identity=REMOTE_IDENTITY,
    )

    assert result["code"] == "INVALID_RUNTIME_ARGUMENT"


def test_remote_lineage_uses_logical_projection_but_trusted_local_retains_legacy_helper(
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

    assert remote["kind"] == "logical_lineage_graph"
    assert remote["pipeline"] == "bounded"
    assert remote["nodes"] == []
    assert remote["edges"] == []
    assert remote["counts"] == {
        "nodes_total": 0,
        "nodes_returned": 0,
        "edges_total": 0,
        "edges_returned": 0,
    }
    assert remote["truncated"] is False
    assert remote["policy_applied"]["logical_only"] is True
    assert local == {"nodes": [{"id": "/physical/path"}], "edges": []}
    assert calls == [{"pipeline": "bounded"}]


def test_remote_logical_lineage_bypasses_all_legacy_and_ambient_effects(
    managed_dispatcher, monkeypatch, caplog
):
    from odibi_mcp import context as context_module
    from odibi_mcp.tools import builder, workflows

    dispatcher, root = managed_dispatcher
    config_path = root / "odibi.yaml"
    story_path = root / "data" / "stories" / "bounded.json"
    import_path = root / "imported-pipeline.yaml"
    story_path.write_text(
        json.dumps(
            {
                "lineage": {
                    "nodes": [{"id": f"abfss://container/{PROJECTION_SENTINELS[7]}"}],
                    "edges": [],
                },
                "events": [{"message": PROJECTION_SENTINELS[6]}],
            }
        ),
        encoding="utf-8",
    )
    import_path.write_text(
        yaml.safe_dump(
            {
                "pipelines": [
                    {
                        "pipeline": "imported",
                        "nodes": [{"name": PROJECTION_SENTINELS[4]}],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["connections"]["local"].update(
        {
            "password": RENDER_SENTINELS[0],
            "connection_string": RENDER_SENTINELS[1],
            "account_key": RENDER_SENTINELS[2],
            "token": f"token-{RENDER_SENTINELS[0]}",
            "host": f"host-{RENDER_SENTINELS[3]}",
            "account": f"account-{RENDER_SENTINELS[2]}",
            "container": f"container-{PROJECTION_SENTINELS[4]}",
            "unknown_nested": {
                "opaque": PROJECTION_SENTINELS[4],
                "list": [PROJECTION_SENTINELS[5], PROJECTION_SENTINELS[6]],
            },
        }
    )
    config["pipelines"][0]["nodes"] = [
        {
            "name": "source",
            "depends_on": [],
            "read": {
                "connection": "local",
                "format": "sql",
                "path": f"abfss://private/{PROJECTION_SENTINELS[7]}",
                "query": f"SELECT '{PROJECTION_SENTINELS[4]}' FROM private_table",
                "options": {"host": RENDER_SENTINELS[3]},
            },
            "unknown": {"value": PROJECTION_SENTINELS[5]},
        },
        {
            "name": "clean",
            "depends_on": ["source"],
            "transform": {"steps": [{"sql": f"SELECT '{PROJECTION_SENTINELS[6]}' FROM df"}]},
            "write": {
                "connection": "local",
                "format": "delta",
                "path": f"s3://private/{PROJECTION_SENTINELS[7]}",
            },
        },
    ]
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    stale_config = yaml.safe_load(VALID_PIPELINE_YAML)
    stale_config["unknown"] = {"ambient": PROJECTION_SENTINELS[4]}
    stale_context = context_module.MCPProjectContext.from_config_snapshot(
        root / "stale-context.yaml", stale_config
    )
    previous_context = context_module.get_project_context()
    previous_cache = dict(context_module._project_cache)
    context_module.set_project_context(stale_context)
    context_module._project_cache["bounded"] = stale_context
    session = builder.create_pipeline("bounded_lineage_tripwire")
    session_id = session["session_id"]
    state_before = builder.get_pipeline_state(session_id)
    workflow_before = dict(workflows.WORKFLOWS)
    original_open = Path.open
    reads = []

    def config_only_open(path, *args, **kwargs):
        resolved = path.resolve()
        reads.append(resolved)
        if resolved != config_path.resolve():
            pytest.fail("logical lineage read a story, runtime, import, or other file")
        return original_open(path, *args, **kwargs)

    def unexpected_effect(*args, **kwargs):
        raise RuntimeError(PROJECTION_SENTINELS[7])

    fake_lineage = ModuleType("odibi_mcp.tools.lineage")
    fake_lineage.lineage_graph = unexpected_effect
    fake_story = ModuleType("odibi_mcp.tools.story")
    fake_story.lineage_graph = unexpected_effect
    monkeypatch.setitem(sys.modules, "odibi_mcp.tools.lineage", fake_lineage)
    monkeypatch.setitem(sys.modules, "tools.lineage", fake_lineage)
    monkeypatch.setitem(sys.modules, "odibi_mcp.tools.story", fake_story)
    monkeypatch.setitem(sys.modules, "tools.story", fake_story)
    monkeypatch.setattr(Path, "open", config_only_open)
    monkeypatch.setattr(Path, "glob", unexpected_effect)
    monkeypatch.setattr(Path, "rglob", unexpected_effect)
    monkeypatch.setattr(Path, "iterdir", unexpected_effect)
    monkeypatch.setattr(dispatcher, "_bind_runtime_context", unexpected_effect)
    monkeypatch.setattr(context_module, "get_project_context", unexpected_effect)
    monkeypatch.setattr(context_module, "set_project_context", unexpected_effect)
    monkeypatch.setattr(
        context_module.MCPProjectContext, "initialize_connections", unexpected_effect
    )

    try:
        result = dispatcher.dispatch(
            "lineage_graph",
            project="managed",
            pipeline="bounded",
            application_identity=REMOTE_IDENTITY,
        )
        state_after = builder.get_pipeline_state(session_id)
    finally:
        monkeypatch.undo()
        context_module.set_project_context(previous_context)
        context_module._project_cache.clear()
        context_module._project_cache.update(previous_cache)
        builder.discard_pipeline(session_id)

    assert result["nodes"] == [
        {"id": "source", "type": "pipeline_node"},
        {"id": "clean", "type": "pipeline_node"},
    ]
    assert result["edges"] == [{"source": "source", "target": "clean", "kind": "dependency"}]
    assert result["policy_applied"] == {
        "project_scoped": True,
        "logical_only": True,
        "inline_snapshot_only": True,
        "node_limit": 64,
        "edge_limit": 128,
        "identifier_length_limit": 128,
        "response_byte_limit": 65536,
    }
    assert reads == [config_path.resolve()]
    assert state_after == state_before
    assert workflows.WORKFLOWS == workflow_before
    assert context_module.get_project_context() is previous_context
    assert context_module._project_cache == previous_cache
    serialized = json.dumps(result, indent=2, sort_keys=True)
    assert len(serialized.encode("utf-8")) <= 65536
    assert "state" not in result and "events" not in result
    assert all(sentinel not in serialized for sentinel in PROJECTION_SENTINELS)
    assert all(sentinel not in caplog.text for sentinel in PROJECTION_SENTINELS)


@pytest.mark.parametrize(
    "prepared_case",
    [
        "missing_projection",
        "mismatched_action",
        "duck_prepared",
        "duck_projection",
        "prepare_exception",
        "prepared_subclass",
        "projection_subclass",
    ],
)
def test_remote_logical_lineage_rejects_malformed_preparation_before_any_effect(
    tmp_path, monkeypatch, caplog, prepared_case
):
    from odibi_mcp import dispatcher as dispatcher_module

    class PreparedSubclass(PreparedRuntimeCall):
        pass

    class ProjectionSubclass(RemoteLogicalLineageProjection):
        pass

    projection_type = (
        ProjectionSubclass
        if prepared_case == "projection_subclass"
        else RemoteLogicalLineageProjection
    )
    projection = projection_type(pipeline="bounded", nodes=(), edges=())
    prepared_kwargs = {"pipeline": PROJECTION_SENTINELS[4]}
    if prepared_case == "prepare_exception":
        prepared = None
    elif prepared_case == "duck_prepared":
        prepared = SimpleNamespace(
            action="lineage_graph",
            kwargs=prepared_kwargs,
            logical_lineage=projection,
        )
    else:
        prepared_type = (
            PreparedSubclass if prepared_case == "prepared_subclass" else PreparedRuntimeCall
        )
        prepared = prepared_type(
            action="story_read" if prepared_case == "mismatched_action" else "lineage_graph",
            kwargs=prepared_kwargs,
            project_root=tmp_path,
            config_path=tmp_path / "odibi.yaml",
            config_snapshot={"unknown": PROJECTION_SENTINELS[5]},
            config_fingerprint=PROJECTION_SENTINELS[6],
            logical_lineage=(
                None
                if prepared_case == "missing_projection"
                else SimpleNamespace(pipeline=PROJECTION_SENTINELS[7])
                if prepared_case == "duck_projection"
                else projection
            ),
        )

    access = ManagedProjectAccess("managed", tmp_path, tmp_path / "odibi.yaml")

    def prepare(self, action, kwargs):
        if prepared_case == "prepare_exception":
            raise RuntimeError(PROJECTION_SENTINELS[7])
        return prepared

    monkeypatch.setattr(ManagedProjectAccess, "prepare", prepare)
    dispatcher = OdibiDispatcher(access)
    effects = []

    def tripwire(name):
        def unexpected(*args, **kwargs):
            effects.append(name)
            return {"sentinel": PROJECTION_SENTINELS[7]}

        return unexpected

    monkeypatch.setattr(dispatcher, "_bind_runtime_context", tripwire("bind_context"))
    monkeypatch.setattr(dispatcher, "_restore_runtime_context", tripwire("restore_context"))
    monkeypatch.setattr(dispatcher_module, "sanitize_runtime_result", tripwire("sanitizer"))
    monkeypatch.setattr(
        dispatcher_module,
        "render_remote_logical_lineage_projection",
        tripwire("projection_renderer"),
    )
    dispatcher._actions["lineage_graph"] = tripwire("legacy_lineage")

    rejected_selector = PROJECTION_SENTINELS[0]
    result = dispatcher.dispatch(
        "lineage_graph",
        project="managed",
        pipeline=rejected_selector,
        application_identity=REMOTE_IDENTITY,
    )

    assert result == {
        "error": "The logical lineage projection is unavailable",
        "code": "LOGICAL_PROJECTION_UNAVAILABLE",
        "action": "lineage_graph",
    }
    assert effects == []
    serialized = json.dumps(result, sort_keys=True)
    assert rejected_selector not in serialized
    assert rejected_selector not in caplog.text
    assert all(sentinel not in serialized for sentinel in PROJECTION_SENTINELS)
    assert all(sentinel not in caplog.text for sentinel in PROJECTION_SENTINELS)


def test_remote_logical_lineage_rejects_custom_managed_authority_before_projection_mint(
    tmp_path, monkeypatch, caplog
):
    from odibi_mcp import dispatcher as dispatcher_module

    calls = []
    projection = RemoteLogicalLineageProjection(pipeline="bounded", nodes=(), edges=())

    class MintingAccess:
        def prepare(self, action, kwargs):
            calls.append((action, kwargs))
            return PreparedRuntimeCall(
                action="lineage_graph",
                kwargs={"pipeline": PROJECTION_SENTINELS[4]},
                project_root=tmp_path,
                config_path=tmp_path / "unvalidated.yaml",
                config_snapshot={"unknown": PROJECTION_SENTINELS[5]},
                config_fingerprint=PROJECTION_SENTINELS[6],
                logical_lineage=projection,
            )

    dispatcher = OdibiDispatcher(MintingAccess())
    effects = []

    def unexpected(*args, **kwargs):
        effects.append(PROJECTION_SENTINELS[7])
        return {"sentinel": PROJECTION_SENTINELS[7]}

    monkeypatch.setattr(dispatcher, "_bind_runtime_context", unexpected)
    monkeypatch.setattr(dispatcher, "_restore_runtime_context", unexpected)
    monkeypatch.setattr(dispatcher_module, "sanitize_runtime_result", unexpected)
    monkeypatch.setattr(dispatcher_module, "render_remote_logical_lineage_projection", unexpected)
    dispatcher._actions["lineage_graph"] = unexpected

    result = dispatcher.dispatch(
        "lineage_graph",
        project="managed",
        pipeline="bounded",
        application_identity=REMOTE_IDENTITY,
    )

    assert result == {
        "error": "The logical lineage projection is unavailable",
        "code": "LOGICAL_PROJECTION_UNAVAILABLE",
        "action": "lineage_graph",
    }
    assert calls == []
    assert effects == []
    serialized = json.dumps(result, sort_keys=True)
    assert all(sentinel not in serialized for sentinel in PROJECTION_SENTINELS)
    assert all(sentinel not in caplog.text for sentinel in PROJECTION_SENTINELS)


def test_remote_logical_lineage_import_denial_is_fixed_and_reads_only_canonical_config(
    managed_dispatcher, monkeypatch, caplog
):
    dispatcher, root = managed_dispatcher
    config_path = root / "odibi.yaml"
    imported_path = root / "imported-pipeline.yaml"
    imported_path.write_text(
        yaml.safe_dump(
            {
                "pipelines": [
                    {
                        "pipeline": "bounded",
                        "nodes": [{"name": PROJECTION_SENTINELS[4]}],
                    }
                ],
                "password": RENDER_SENTINELS[0],
            }
        ),
        encoding="utf-8",
    )
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["imports"] = [f"imported-pipeline-{PROJECTION_SENTINELS[7]}.yaml"]
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    original_open = Path.open
    reads = []

    def config_only_open(path, *args, **kwargs):
        resolved = path.resolve()
        reads.append(resolved)
        if resolved != config_path.resolve():
            pytest.fail("remote logical lineage resolved or read an import")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", config_only_open)
    monkeypatch.setattr(
        dispatcher,
        "_bind_runtime_context",
        lambda *args, **kwargs: pytest.fail("import denial bound ambient context"),
    )
    dispatcher._actions["lineage_graph"] = lambda **kwargs: pytest.fail(
        "import denial reached the legacy lineage helper"
    )

    result = dispatcher.dispatch(
        "lineage_graph",
        project="managed",
        pipeline="bounded",
        application_identity=REMOTE_IDENTITY,
    )

    assert result == {
        "error": "The logical lineage projection is unavailable",
        "code": "LOGICAL_PROJECTION_UNAVAILABLE",
        "action": "lineage_graph",
    }
    assert reads == [config_path.resolve()]
    serialized = json.dumps(result, sort_keys=True)
    assert all(sentinel not in serialized for sentinel in PROJECTION_SENTINELS)
    assert all(sentinel not in caplog.text for sentinel in PROJECTION_SENTINELS)


def test_remote_logical_lineage_renderer_failure_is_fixed_without_exception_echo(
    managed_dispatcher, monkeypatch, caplog
):
    from odibi_mcp import dispatcher as dispatcher_module

    dispatcher, _ = managed_dispatcher

    def fail_with_sentinel(*args, **kwargs):
        raise RuntimeError(PROJECTION_SENTINELS[7])

    monkeypatch.setattr(
        dispatcher_module, "render_remote_logical_lineage_projection", fail_with_sentinel
    )

    result = dispatcher.dispatch(
        "lineage_graph",
        project="managed",
        pipeline="bounded",
        application_identity=REMOTE_IDENTITY,
    )

    assert result == {
        "error": "The logical lineage projection is unavailable",
        "code": "LOGICAL_PROJECTION_UNAVAILABLE",
        "action": "lineage_graph",
    }
    assert PROJECTION_SENTINELS[7] not in json.dumps(result, sort_keys=True)
    assert PROJECTION_SENTINELS[7] not in caplog.text


def test_remote_diagnose_stays_denied_before_config_and_legacy_helper(
    managed_dispatcher, monkeypatch, caplog
):
    dispatcher, _ = managed_dispatcher
    fake_diagnose = ModuleType("odibi_mcp.tools.diagnose")

    def unexpected_effect(*args, **kwargs):
        raise RuntimeError(PROJECTION_SENTINELS[7])

    fake_diagnose.diagnose = unexpected_effect
    monkeypatch.setitem(sys.modules, "odibi_mcp.tools.diagnose", fake_diagnose)
    monkeypatch.setitem(sys.modules, "tools.diagnose", fake_diagnose)
    monkeypatch.setattr(Path, "open", unexpected_effect)
    monkeypatch.setattr(Path, "glob", unexpected_effect)
    monkeypatch.setattr(Path, "rglob", unexpected_effect)
    monkeypatch.setattr(Path, "iterdir", unexpected_effect)

    result = dispatcher.dispatch(
        "diagnose",
        project="managed",
        application_identity=REMOTE_IDENTITY,
    )

    assert result == {
        "error": "Remote physical references are unavailable",
        "code": "PHYSICAL_REFERENCES_DISABLED",
        "action": "diagnose",
    }
    assert PROJECTION_SENTINELS[7] not in json.dumps(result, sort_keys=True)
    assert PROJECTION_SENTINELS[7] not in caplog.text


@pytest.mark.parametrize(
    "action,kwargs",
    [
        ("run_workflow", {"workflow_name": "build_and_validate", "params": {}}),
        ("run_workflow", {"workflow_name": "debug_pipeline", "params": {}}),
        ("run_workflow", {"workflow_name": "iterate_until_valid", "params": {}}),
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


def test_only_pure_validation_workflow_is_remotely_allowed():
    assert _REMOTE_SAFE_WORKFLOWS == {"validate_yaml_simple"}


def test_every_remote_perimeter_action_remains_identity_restricted():
    perimeter_actions = (
        RUNTIME_DATA_ACTIONS
        | _REMOTE_DISABLED_RENDERING_ACTIONS
        | {"apply_pattern_template", "run_workflow", "resume_workflow"}
    )

    assert _REMOTE_DISABLED_RENDERING_ACTIONS == {
        "create_ingestion_pipeline",
        "render_pipeline_yaml",
    }
    assert all(
        ACTION_EFFECTS[action] is not ActionEffect.PUBLIC_READ for action in perimeter_actions
    )


def test_remote_validate_workflow_has_transitive_effect_tripwires(monkeypatch):
    from odibi import catalog as catalog_module
    from odibi.connections import factory as connection_factory
    from odibi.story import generator as story_generator
    from odibi_mcp import context as context_module
    from odibi_mcp.tools import workflows

    def unexpected_effect(*args, **kwargs):
        pytest.fail("pure remote validation reached an external capability or effect")

    original_test_pipeline = execution.test_pipeline

    def validate_only(yaml_content, *, mode="dry-run", max_rows=100):
        assert mode == "validate"
        return original_test_pipeline(yaml_content, mode=mode, max_rows=max_rows)

    for tool_name in workflows.TOOL_REGISTRY:
        monkeypatch.setitem(workflows.TOOL_REGISTRY, tool_name, unexpected_effect)
    monkeypatch.setitem(workflows.TOOL_REGISTRY, "test_pipeline", validate_only)
    monkeypatch.setattr(execution.immutable_planning, "plan_pipeline_yaml", unexpected_effect)
    monkeypatch.setattr(context_module, "resolve_connection", unexpected_effect)
    monkeypatch.setattr(
        context_module.MCPProjectContext,
        "initialize_connections",
        unexpected_effect,
    )
    monkeypatch.setattr(connection_factory, "register_builtins", unexpected_effect)
    monkeypatch.setattr(catalog_module.CatalogManager, "bootstrap", unexpected_effect)
    monkeypatch.setattr(story_generator.StoryGenerator, "generate", unexpected_effect)
    for method_name in ("write_text", "write_bytes", "touch", "mkdir", "unlink"):
        monkeypatch.setattr(Path, method_name, unexpected_effect)

    previous = context_module.get_project_context()
    secret_config = yaml.safe_load(VALID_PIPELINE_YAML)
    secret_config["connections"]["local"].update(
        {
            "password": RENDER_SENTINELS[0],
            "connection_string": RENDER_SENTINELS[1],
            "account_key": RENDER_SENTINELS[2],
            "unknown_host": RENDER_SENTINELS[3],
        }
    )
    context_module.set_project_context(
        context_module.MCPProjectContext.from_config_snapshot("managed.yaml", secret_config)
    )
    try:
        result = OdibiDispatcher().dispatch(
            "run_workflow",
            workflow_name="validate_yaml_simple",
            params={"yaml": VALID_PIPELINE_YAML},
            application_identity=REMOTE_IDENTITY,
        )
    finally:
        context_module.set_project_context(previous)

    assert result["status"] == "COMPLETED"
    serialized = json.dumps(result, sort_keys=True)
    assert all(sentinel not in serialized for sentinel in RENDER_SENTINELS)


def test_remote_safe_fact_projection_is_constant_bounded_and_context_free(monkeypatch):
    from odibi_mcp import context as context_module
    from odibi_mcp.tools import render as render_module

    class UnexpectedManagedAccess:
        def prepare(self, action, kwargs):
            pytest.fail("constant remote projection must not prepare managed project context")

    def unexpected_effect(*args, **kwargs):
        pytest.fail("constant remote projection reached ambient context or the legacy delegate")

    previous = context_module.get_project_context()
    secret_config = yaml.safe_load(VALID_PIPELINE_YAML)
    secret_config["connections"]["local"].update(
        {
            "password": RENDER_SENTINELS[0],
            "connection_string": RENDER_SENTINELS[1],
            "account_key": RENDER_SENTINELS[2],
            "unknown_host": RENDER_SENTINELS[3],
            "unknown_nested": {
                "opaque": PROJECTION_SENTINELS[4],
                "builder_state": PROJECTION_SENTINELS[5],
                "workflow_event": PROJECTION_SENTINELS[6],
            },
        }
    )
    context_module.set_project_context(
        context_module.MCPProjectContext.from_config_snapshot("managed.yaml", secret_config)
    )
    dispatcher = OdibiDispatcher(UnexpectedManagedAccess())
    dispatcher._actions["apply_pattern_template"] = unexpected_effect
    monkeypatch.setattr(render_module, "get_project_context", unexpected_effect)
    try:
        result = dispatcher.dispatch(
            "apply_pattern_template",
            pattern="fact",
            table_name="fact_pipeline",
            connection="local_input",
            source_path="input.csv",
            application_identity=REMOTE_IDENTITY,
        )
    finally:
        context_module.set_project_context(previous)

    expected = {
        "project": "remote_safe_fact",
        "engine": "pandas",
        "connections": {
            "local_input": {"type": "local", "base_path": "./data"},
            "local_output": {"type": "local", "base_path": "./output"},
        },
        "pipelines": [
            {
                "pipeline": "fact_pipeline",
                "layer": "gold",
                "nodes": [
                    {
                        "name": "fact_node",
                        "read": {
                            "connection": "local_input",
                            "format": "csv",
                            "path": "input.csv",
                        },
                        "write": {
                            "connection": "local_output",
                            "format": "parquet",
                            "path": "facts",
                            "mode": "append",
                        },
                    }
                ],
            }
        ],
        "story": {"connection": "local_output", "path": "_stories"},
        "system": {"connection": "local_output", "path": "_system"},
    }
    assert result["valid"] is True
    assert result["errors"] == result["warnings"] == []
    assert yaml.safe_load(result["yaml"]) == expected
    assert len(result["yaml"].encode("utf-8")) <= 4 * 1024
    serialized = json.dumps(result, sort_keys=True)
    assert len(serialized.encode("utf-8")) <= 8 * 1024
    assert all(sentinel not in serialized for sentinel in PROJECTION_SENTINELS)

    def structure_size(value):
        if isinstance(value, dict):
            return 1 + sum(structure_size(item) for item in value.values())
        if isinstance(value, list):
            return 1 + sum(structure_size(item) for item in value)
        return 1

    assert structure_size(expected) <= 40


@pytest.mark.parametrize(
    "kwargs",
    [
        {
            "pattern": f"fact_{PROJECTION_SENTINELS[7]}",
            "table_name": "fact_pipeline",
            "connection": "local_input",
            "source_path": "input.csv",
        },
        {
            "pattern": "fact",
            "table_name": "fact_pipeline",
            "connection": "local_input",
            "source_path": "input.csv",
            "token": RENDER_SENTINELS[0],
        },
        {
            "pattern": "fact",
            "table_name": "fact_pipeline",
            "connection": "local_input",
            "source_path": "input.csv",
            "unknown": {"nested": {"opaque": PROJECTION_SENTINELS[4]}},
        },
        {
            "pattern": "fact",
            "table_name": "fact_pipeline",
            "connection": {"account_key": RENDER_SENTINELS[2]},
            "source_path": "input.csv",
        },
        {
            "pattern": "fact",
            "table_name": "fact_pipeline",
            "connection": "local_input",
            "source_path": f"/{PROJECTION_SENTINELS[7]}/input.csv",
        },
        {
            "pattern": "fact",
            "table_name": "fact_pipeline",
            "connection": "local_input",
            "source_path": f"C:\\{PROJECTION_SENTINELS[7]}\\input.csv",
        },
        {
            "pattern": "fact",
            "table_name": "fact_pipeline",
            "connection": "local_input",
            "source_path": f"https://unknown.example/{RENDER_SENTINELS[3]}",
        },
        {
            "pattern": "fact",
            "table_name": "fact_pipeline",
            "connection": "local_input",
            "source_path": f"{PROJECTION_SENTINELS[7]}\ninput.csv",
        },
        {
            "pattern": "fact",
            "table_name": "fact_pipeline",
            "connection": "local_input",
            "source_path": PROJECTION_SENTINELS[7] + ("a" * 4096),
        },
        {
            "pattern": "fact",
            "table_name": "fact_pipeline",
            "connection": "local_input",
        },
        {
            "pattern": "fact",
            "table_name": "fact_pipeline",
            "connection": "local_input",
            "source_path": ["input.csv", PROJECTION_SENTINELS[4]],
        },
        {
            "pattern": "fact",
            "table_name": "fact_pipeline",
            "connection": "local_input",
            "source_path": "input.csv",
            "root": PROJECTION_SENTINELS[7],
            "config": {"password": RENDER_SENTINELS[0]},
            "cwd": PROJECTION_SENTINELS[5],
        },
    ],
)
def test_invalid_remote_fact_projection_denies_before_all_helpers_without_echo(
    monkeypatch, caplog, kwargs
):
    class UnexpectedManagedAccess:
        def prepare(self, action, call_kwargs):
            pytest.fail("invalid projection reached managed project preparation")

    monkeypatch.setitem(sys.modules, "odibi_mcp.tools.render", None)
    monkeypatch.setitem(sys.modules, "tools.render", None)
    dispatcher = OdibiDispatcher(UnexpectedManagedAccess())
    dispatcher._actions["apply_pattern_template"] = lambda *args, **call_kwargs: pytest.fail(
        "invalid projection reached the legacy construction delegate"
    )
    monkeypatch.setattr(
        dispatcher,
        "_render_remote_pattern_projection",
        lambda projection: pytest.fail("invalid projection reached the renderer"),
    )

    result = dispatcher.dispatch(
        "apply_pattern_template",
        application_identity=REMOTE_IDENTITY,
        **kwargs,
    )

    assert result == {
        "error": "The remote-safe rendering projection is required",
        "code": "REMOTE_RENDER_PROJECTION_REQUIRED",
        "action": "apply_pattern_template",
    }
    serialized = json.dumps(result, sort_keys=True)
    assert "yaml" not in result
    assert "state" not in result
    assert "events" not in result
    assert all(sentinel not in serialized for sentinel in PROJECTION_SENTINELS)
    assert all(sentinel not in caplog.text for sentinel in PROJECTION_SENTINELS)


def test_positional_remote_fact_projection_denies_without_delegate_or_value_echo():
    dispatcher = OdibiDispatcher()
    dispatcher._actions["apply_pattern_template"] = lambda *args, **kwargs: pytest.fail(
        "positional projection reached the legacy construction delegate"
    )

    result = dispatcher.dispatch(
        "apply_pattern_template",
        "fact",
        "fact_pipeline",
        "local_input",
        PROJECTION_SENTINELS[7],
        application_identity=REMOTE_IDENTITY,
    )

    assert result["code"] == "REMOTE_RENDER_PROJECTION_REQUIRED"
    assert PROJECTION_SENTINELS[7] not in json.dumps(result, sort_keys=True)


def test_projected_render_failures_are_fixed_and_do_not_echo_exception(monkeypatch):
    from odibi_mcp.tools import render as render_module

    def fail_with_secret(*args, **kwargs):
        raise RuntimeError(PROJECTION_SENTINELS[7])

    monkeypatch.setattr(render_module.yaml, "safe_dump", fail_with_secret)
    result = OdibiDispatcher().dispatch(
        "apply_pattern_template",
        pattern="fact",
        table_name="fact_pipeline",
        connection="local_input",
        source_path="input.csv",
        application_identity=REMOTE_IDENTITY,
    )

    assert result == {
        "yaml": "",
        "valid": False,
        "errors": [
            {
                "code": "PROJECTED_RENDER_FAILED",
                "message": "The remote-safe template could not be rendered",
            }
        ],
        "warnings": [],
    }
    assert PROJECTION_SENTINELS[7] not in json.dumps(result, sort_keys=True)


def test_projected_renderer_rejects_non_projection_without_echo():
    from odibi_mcp.tools.render import render_remote_pattern_projection

    result = render_remote_pattern_projection({"secret": PROJECTION_SENTINELS[7]})

    assert result["valid"] is False
    assert result["errors"][0]["code"] == "PROJECTED_RENDER_FAILED"
    assert PROJECTION_SENTINELS[7] not in json.dumps(result, sort_keys=True)


def test_trusted_local_pattern_template_retains_legacy_delegate(monkeypatch):
    calls = []
    fake_construction = ModuleType("tools.construction")
    fake_construction.apply_pattern_template = lambda *args: calls.append(args) or {
        "trusted_local": True
    }
    monkeypatch.setitem(sys.modules, "tools.construction", fake_construction)

    result = OdibiDispatcher().dispatch(
        "apply_pattern_template",
        pattern="dimension",
        table_name="trusted_table",
        connection="trusted_connection",
        source_path="trusted_source",
        application_identity=LOCAL_IDENTITY,
    )

    assert result == {"trusted_local": True}
    assert calls == [("dimension", "trusted_table", "trusted_connection", "trusted_source")]


@pytest.mark.parametrize(
    "action,kwargs,module_name",
    [
        (
            "create_ingestion_pipeline",
            {
                "source_path": "input.csv",
                "connection": "local",
                "target_table": "bounded",
            },
            "tools.phase3_smart",
        ),
        ("render_pipeline_yaml", {"session_id": "session"}, "tools.builder"),
    ],
)
def test_remote_renderer_routes_deny_before_helper_import_and_never_disclose_config(
    monkeypatch, action, kwargs, module_name
):
    from odibi_mcp import context as context_module

    previous = context_module.get_project_context()
    secret_config = yaml.safe_load(VALID_PIPELINE_YAML)
    secret_config["connections"]["local"].update(
        {
            "password": RENDER_SENTINELS[0],
            "connection_string": RENDER_SENTINELS[1],
            "account_key": RENDER_SENTINELS[2],
            "unknown_host": RENDER_SENTINELS[3],
            "unknown_nested": {"opaque": PROJECTION_SENTINELS[4]},
        }
    )
    context_module.set_project_context(
        context_module.MCPProjectContext.from_config_snapshot("managed.yaml", secret_config)
    )
    monkeypatch.setitem(sys.modules, module_name, None)
    try:
        result = OdibiDispatcher().dispatch(
            action,
            application_identity=REMOTE_IDENTITY,
            **kwargs,
        )
    finally:
        context_module.set_project_context(previous)

    assert result == {
        "error": "This rendering action is unavailable over the remote transport",
        "code": "REMOTE_RENDERING_DISABLED",
        "action": action,
    }
    serialized = json.dumps(result, sort_keys=True)
    assert "yaml" not in result
    assert "state" not in result
    assert "events" not in result
    assert all(sentinel not in serialized for sentinel in PROJECTION_SENTINELS)


def test_remote_session_render_denial_preserves_secret_free_builder_state(monkeypatch):
    from odibi_mcp import context as context_module
    from odibi_mcp.tools import builder

    session = builder.create_pipeline("bounded_render_session")
    session_id = session["session_id"]
    state_before = builder.get_pipeline_state(session_id)
    previous = context_module.get_project_context()
    secret_config = yaml.safe_load(VALID_PIPELINE_YAML)
    secret_config["connections"]["local"].update(
        {
            "password": RENDER_SENTINELS[0],
            "connection_string": RENDER_SENTINELS[1],
            "account_key": RENDER_SENTINELS[2],
            "unknown_host": RENDER_SENTINELS[3],
            "unknown_nested": {"opaque": PROJECTION_SENTINELS[4]},
        }
    )
    context_module.set_project_context(
        context_module.MCPProjectContext.from_config_snapshot("managed.yaml", secret_config)
    )
    monkeypatch.setitem(sys.modules, "tools.builder", None)
    try:
        result = OdibiDispatcher().dispatch(
            "render_pipeline_yaml",
            session_id=session_id,
            unknown={"nested": PROJECTION_SENTINELS[5]},
            application_identity=REMOTE_IDENTITY,
        )
        state_after = builder.get_pipeline_state(session_id)
    finally:
        context_module.set_project_context(previous)
        builder.discard_pipeline(session_id)

    assert result["code"] == "REMOTE_RENDERING_DISABLED"
    assert state_after["session_id"] == state_before["session_id"]
    assert state_after["node_count"] == state_before["node_count"] == 0
    serialized = json.dumps(
        {"response": result, "state_before": state_before, "state_after": state_after},
        sort_keys=True,
    )
    assert all(sentinel not in serialized for sentinel in PROJECTION_SENTINELS)


@pytest.mark.parametrize(
    "workflow_name",
    ["build_and_validate", "debug_pipeline", "iterate_until_valid"],
)
def test_remote_disabled_workflows_deny_before_import_without_secret_state_or_events(
    monkeypatch, workflow_name
):
    from odibi_mcp import context as context_module

    previous = context_module.get_project_context()
    secret_config = yaml.safe_load(VALID_PIPELINE_YAML)
    secret_config["connections"]["local"].update(
        {
            "password": RENDER_SENTINELS[0],
            "connection_string": RENDER_SENTINELS[1],
            "account_key": RENDER_SENTINELS[2],
            "unknown_host": RENDER_SENTINELS[3],
        }
    )
    context_module.set_project_context(
        context_module.MCPProjectContext.from_config_snapshot("managed.yaml", secret_config)
    )
    monkeypatch.setitem(sys.modules, "odibi_mcp.tools.workflows", None)
    monkeypatch.setitem(sys.modules, "tools.workflows", None)
    try:
        result = OdibiDispatcher().dispatch(
            "run_workflow",
            workflow_name=workflow_name,
            params={"unknown": {"nested": PROJECTION_SENTINELS[6]}},
            application_identity=REMOTE_IDENTITY,
        )
    finally:
        context_module.set_project_context(previous)

    assert result["code"] == "REMOTE_WORKFLOW_DISABLED"
    assert "state" not in result
    assert "events" not in result
    serialized = json.dumps(result, sort_keys=True)
    assert all(sentinel not in serialized for sentinel in PROJECTION_SENTINELS)


def test_remote_resume_never_decodes_or_echoes_unsigned_secret_state(monkeypatch):
    resume_token = base64.b64encode(
        json.dumps(
            {
                "state": {"unknown": {"nested": PROJECTION_SENTINELS[4]}},
                "events": [{"message": PROJECTION_SENTINELS[6]}],
            }
        ).encode("utf-8")
    ).decode("ascii")
    monkeypatch.setitem(sys.modules, "odibi_mcp.tools.workflows", None)
    monkeypatch.setitem(sys.modules, "tools.workflows", None)

    result = OdibiDispatcher().dispatch(
        "resume_workflow",
        resume_token=resume_token,
        inputs={"secret": RENDER_SENTINELS[0]},
        application_identity=REMOTE_IDENTITY,
    )

    assert result == {
        "error": "This workflow is unavailable over the remote transport",
        "code": "REMOTE_WORKFLOW_DISABLED",
        "action": "resume_workflow",
    }
    serialized = json.dumps(result, sort_keys=True)
    assert "state" not in result
    assert "events" not in result
    assert all(sentinel not in serialized for sentinel in PROJECTION_SENTINELS)


def test_runtime_call_replaces_stale_same_path_context_with_validated_snapshot(managed_dispatcher):
    from odibi_mcp import context as context_module

    dispatcher, root = managed_dispatcher
    config_path = root / "odibi.yaml"
    current_config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    current_config["snapshot_marker"] = "validated-snapshot"
    config_path.write_text(yaml.safe_dump(current_config), encoding="utf-8")
    stale_config = dict(current_config)
    stale_config["snapshot_marker"] = "stale-same-path"
    stale_context = context_module.MCPProjectContext.from_config_snapshot(config_path, stale_config)
    previous = context_module.get_project_context()
    context_module.set_project_context(stale_context)
    dispatcher._actions["profile_source"] = lambda **kwargs: {
        "marker": context_module.get_project_context().config["snapshot_marker"]
    }

    try:
        result = dispatcher.dispatch(
            "profile_source",
            project="managed",
            connection="local",
            path="folder/input.csv",
            application_identity=REMOTE_IDENTITY,
        )
        restored = context_module.get_project_context()
    finally:
        context_module.set_project_context(previous)

    assert result["marker"] == "validated-snapshot"
    assert restored is stale_context


def test_runtime_call_uses_snapshot_when_config_mutates_between_prepare_and_bind(tmp_path):
    from odibi_mcp import context as context_module

    root = tmp_path / "managed"
    (root / "data").mkdir(parents=True)
    config_path = root / "odibi.yaml"
    original_config = {
        "project": "managed",
        "snapshot_marker": "validated-before-rotation",
        "connections": {"local": {"type": "local", "base_path": "./data"}},
        "story": {"connection": "local", "path": "stories"},
        "system": {"connection": "local"},
        "pipelines": [{"pipeline": "bounded", "nodes": []}],
    }
    config_path.write_text(yaml.safe_dump(original_config), encoding="utf-8")
    delegate = ManagedProjectAccess("managed", root, config_path)

    class MutatingAccess:
        def prepare(self, action, kwargs):
            prepared = delegate.prepare(action, kwargs)
            rotated = dict(original_config)
            rotated["snapshot_marker"] = "unvalidated-after-rotation"
            config_path.write_text(yaml.safe_dump(rotated), encoding="utf-8")
            return prepared

    dispatcher = OdibiDispatcher(MutatingAccess())
    dispatcher._actions["profile_source"] = lambda **kwargs: {
        "marker": context_module.get_project_context().config["snapshot_marker"]
    }

    result = dispatcher.dispatch(
        "profile_source",
        project="managed",
        connection="local",
        path="input.csv",
        application_identity=REMOTE_IDENTITY,
    )

    assert result["marker"] == "validated-before-rotation"
    assert yaml.safe_load(config_path.read_text(encoding="utf-8"))["snapshot_marker"] == (
        "unvalidated-after-rotation"
    )


def test_runtime_context_binding_is_concurrent_call_isolated_and_restored(tmp_path):
    from odibi_mcp import context as context_module

    previous = context_module.get_project_context()
    baseline = context_module.MCPProjectContext.from_config_snapshot(
        "baseline.yaml",
        {"project": "baseline", "connections": {}},
    )
    context_module.set_project_context(baseline)
    active = 0
    maximum_active = 0
    active_lock = threading.Lock()
    start = threading.Barrier(3)

    def make_dispatcher(project, marker):
        root = tmp_path / project
        (root / "data").mkdir(parents=True)
        config_path = root / "odibi.yaml"
        config_path.write_text(
            yaml.safe_dump(
                {
                    "project": project,
                    "snapshot_marker": marker,
                    "connections": {"local": {"type": "local", "base_path": "./data"}},
                    "story": {"connection": "local", "path": "stories"},
                    "system": {"connection": "local"},
                    "pipelines": [{"pipeline": "bounded", "nodes": []}],
                }
            ),
            encoding="utf-8",
        )
        dispatcher = OdibiDispatcher(ManagedProjectAccess(project, root, config_path))

        def handler(**kwargs):
            nonlocal active, maximum_active
            with active_lock:
                active += 1
                maximum_active = max(maximum_active, active)
            try:
                bound_before = context_module.get_project_context().config["snapshot_marker"]
                time.sleep(0.03)
                bound_after = context_module.get_project_context().config["snapshot_marker"]
                return {"before": bound_before, "after": bound_after}
            finally:
                with active_lock:
                    active -= 1

        dispatcher._actions["profile_source"] = handler
        return dispatcher

    dispatcher_a = make_dispatcher("project-a", "marker-a")
    dispatcher_b = make_dispatcher("project-b", "marker-b")

    def call(dispatcher, project):
        start.wait()
        return dispatcher.dispatch(
            "profile_source",
            project=project,
            connection="local",
            path="input.csv",
            application_identity=REMOTE_IDENTITY,
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_a = executor.submit(call, dispatcher_a, "project-a")
            future_b = executor.submit(call, dispatcher_b, "project-b")
            start.wait()
            result_a = future_a.result(timeout=5)
            result_b = future_b.result(timeout=5)
        restored = context_module.get_project_context()
    finally:
        context_module.set_project_context(previous)

    assert result_a["before"] == result_a["after"] == "marker-a"
    assert result_b["before"] == result_b["after"] == "marker-b"
    assert maximum_active == 1
    assert restored is baseline


def test_runtime_context_restores_after_handler_exception_without_initializing_connections(
    managed_dispatcher, monkeypatch
):
    from odibi_mcp import context as context_module

    dispatcher, _ = managed_dispatcher
    previous = context_module.get_project_context()
    baseline = context_module.MCPProjectContext.from_config_snapshot(
        "baseline.yaml",
        {"project": "baseline", "connections": {}},
    )
    context_module.set_project_context(baseline)

    def unexpected_initialization(*args, **kwargs):
        pytest.fail("connection initialization occurred before explicit helper access")

    monkeypatch.setattr(
        context_module.MCPProjectContext,
        "initialize_connections",
        unexpected_initialization,
    )
    dispatcher._actions["profile_source"] = lambda **kwargs: (_ for _ in ()).throw(
        RuntimeError("unique host detail must be sanitized")
    )
    try:
        result = dispatcher.dispatch(
            "profile_source",
            project="managed",
            connection="local",
            path="folder/input.csv",
            application_identity=REMOTE_IDENTITY,
        )
        restored = context_module.get_project_context()
    finally:
        context_module.set_project_context(previous)

    assert result == {
        "error": "Runtime data is unavailable",
        "code": "RUNTIME_DATA_UNAVAILABLE",
        "action": "profile_source",
    }
    assert restored is baseline


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


@pytest.mark.parametrize(
    "pipeline",
    [
        PLANNED_LOGICAL_YAML,
        VALID_PIPELINE_YAML,
        "project: [",
        "x" * (planning.DEFAULT_PLANNING_LIMITS.max_input_bytes + 1),
    ],
)
def test_trusted_dispatcher_planning_response_exactly_matches_package(
    pipeline, immutable_planning_tripwires
):
    expected = planning.plan_pipeline_yaml(pipeline).to_dict()

    with immutable_planning_tripwires() as attempts:
        result = D.dispatch(
            "test_pipeline",
            pipeline=pipeline,
            application_identity=LOCAL_IDENTITY,
        )

    assert result == expected
    assert set(result) == {"schema_version", "status", "plan", "diagnostics", "truncated"}
    assert attempts == []


def test_trusted_bootstrap_planning_response_exactly_matches_package():
    from odibi_mcp import bootstrap

    odibi, _ = bootstrap.init()

    assert (
        odibi("test_pipeline", pipeline=PLANNED_LOGICAL_YAML)
        == planning.plan_pipeline_yaml(PLANNED_LOGICAL_YAML).to_dict()
    )


def test_trusted_direct_adapter_repeats_without_effect_or_state_growth(
    immutable_planning_tripwires,
):
    expected = planning.plan_pipeline_yaml(PLANNED_LOGICAL_YAML).to_dict()

    with immutable_planning_tripwires() as attempts:
        results = [
            D.dispatch(
                "test_pipeline",
                pipeline=PLANNED_LOGICAL_YAML,
                application_identity=LOCAL_IDENTITY,
            )
            for _ in range(25)
        ]

    assert results == [expected] * 25
    assert attempts == []


@pytest.mark.parametrize(
    "identity",
    [
        None,
        object(),
        ApplicationIdentity(subject="unauthorized", authorized_effects=frozenset()),
    ],
)
def test_restricted_planning_denies_before_input_or_planner(monkeypatch, identity):
    def unexpected_plan(*args, **kwargs):
        pytest.fail("authorization denial must occur before parsing or planning")

    monkeypatch.setattr(planning, "plan_pipeline_yaml", unexpected_plan)

    result = D.dispatch(
        "test_pipeline",
        pipeline="malformed: [ credential-canary",
        application_identity=identity,
    )

    assert result["code"] in {"AUTHORIZATION_REQUIRED", "FORBIDDEN"}
    assert "credential-canary" not in json.dumps(result)


@pytest.mark.parametrize("sample_size", [1, 37, 1000])
def test_registered_test_pipeline_accepts_but_ignores_bounded_sample_size(monkeypatch, sample_size):
    calls = []
    expected = {"schema_version": "1.0", "status": "planned"}

    def fake_plan(yaml_content):
        calls.append(yaml_content)
        return SimpleNamespace(to_dict=lambda: expected)

    monkeypatch.setattr(planning, "plan_pipeline_yaml", fake_plan)

    result = D.dispatch(
        "test_pipeline",
        pipeline=VALID_PIPELINE_YAML,
        sample_size=sample_size,
        application_identity=LOCAL_IDENTITY,
    )

    assert result == expected
    assert calls == [VALID_PIPELINE_YAML]


def test_registered_test_pipeline_uses_bounded_defaults(monkeypatch):
    calls = []
    expected = {"schema_version": "1.0", "status": "planned"}

    def fake_plan(yaml_content):
        calls.append(yaml_content)
        return SimpleNamespace(to_dict=lambda: expected)

    monkeypatch.setattr(planning, "plan_pipeline_yaml", fake_plan)

    result = D.dispatch(
        "test_pipeline",
        pipeline=VALID_PIPELINE_YAML,
        application_identity=LOCAL_IDENTITY,
    )

    assert result == expected
    assert calls == [VALID_PIPELINE_YAML]


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
    def unexpected_call(*args, **kwargs):
        pytest.fail("registered test_pipeline must not expose helper-native controls")

    monkeypatch.setattr(planning, "plan_pipeline_yaml", unexpected_call)

    result = D.dispatch(
        "test_pipeline",
        pipeline=VALID_PIPELINE_YAML,
        application_identity=LOCAL_IDENTITY,
        **unexpected_kwargs,
    )

    assert "error" in result


def test_help_describes_registered_test_pipeline_as_immutable_schema():
    validation_help = D.help(category="Validation")
    test_help = next(
        action for action in validation_help["actions"] if action["name"] == "test_pipeline"
    )

    assert test_help["signature"] == "pipeline"
    assert "schema 1.0" in test_help["description"]
    assert "status=planned" in test_help["description"]
    assert "sample_size" not in json.dumps(test_help)


@pytest.mark.parametrize("sample_size", [True, 1.5, "10", 0, -1, 1001])
def test_registered_test_pipeline_rejects_invalid_bounds_before_helper(monkeypatch, sample_size):
    def unexpected_call(*args, **kwargs):
        pytest.fail("planner must not be called for an invalid sample_size")

    monkeypatch.setattr(planning, "plan_pipeline_yaml", unexpected_call)

    result = D.dispatch(
        "test_pipeline",
        pipeline=VALID_PIPELINE_YAML,
        sample_size=sample_size,
        application_identity=LOCAL_IDENTITY,
    )

    assert "error" in result


@pytest.mark.parametrize("max_rows", [1, 37, 1000])
def test_execution_helper_dry_run_returns_exact_shared_schema_and_ignores_rows(
    monkeypatch, max_rows
):
    calls = []
    expected = {"schema_version": "1.0", "status": "planned"}

    def fake_plan(yaml_content):
        calls.append(yaml_content)
        return SimpleNamespace(to_dict=lambda: expected)

    monkeypatch.setattr(planning, "plan_pipeline_yaml", fake_plan)

    result = execution.test_pipeline(
        VALID_PIPELINE_YAML,
        mode="dry-run",
        max_rows=max_rows,
    )

    assert result == expected
    assert calls == [VALID_PIPELINE_YAML]
    assert not hasattr(execution, "tempfile")
    assert not hasattr(execution, "subprocess")
    assert not hasattr(execution, "Path")


def test_execution_helper_validate_mode_preserves_validation_only_shape(monkeypatch):
    def unexpected_plan(*args, **kwargs):
        pytest.fail("validation-only mode must not invoke immutable planning")

    monkeypatch.setattr(planning, "plan_pipeline_yaml", unexpected_plan)

    result = execution.test_pipeline(VALID_PIPELINE_YAML, mode="validate", max_rows=37)

    assert result["valid"] is True
    assert result["mode"] == "validate"
    assert "schema_version" not in result


def test_execution_helper_has_one_canonical_module_identity():
    from odibi_mcp import dispatcher as dispatcher_module

    assert sys.modules["odibi_mcp.tools.execution"] is execution
    assert "tools.execution" not in sys.modules
    dispatcher_source = Path(dispatcher_module.__file__).read_text(encoding="utf-8")
    assert "from tools.execution" not in dispatcher_source


@pytest.mark.parametrize(
    ("pipeline", "status", "success_log"),
    [
        (PLANNED_LOGICAL_YAML, "planned", True),
        (VALID_PIPELINE_YAML, "unresolved", False),
        ("project: [", "invalid", False),
    ],
)
def test_trusted_debug_workflow_embeds_exact_plan_and_only_planned_succeeds(
    monkeypatch, immutable_planning_tripwires, pipeline, status, success_log
):
    from odibi_mcp.tools import workflows

    def unexpected_tool(*args, **kwargs):
        pytest.fail("planning workflow invoked a runtime or fallback tool")

    for name in workflows.TOOL_REGISTRY:
        if name != "validate_yaml_runnable":
            monkeypatch.setitem(workflows.TOOL_REGISTRY, name, unexpected_tool)

    with immutable_planning_tripwires() as attempts:
        result = D.dispatch(
            "run_workflow",
            workflow_name="debug_pipeline",
            params={"yaml": pipeline},
            application_identity=LOCAL_IDENTITY,
        )

    expected = planning.plan_pipeline_yaml(pipeline).to_dict()
    embedded = result["state"]["results"]["quick"]
    messages = [event["message"] for event in result["events"] if "message" in event]
    assert result["status"] == "COMPLETED"
    assert embedded == expected
    assert embedded["status"] == status
    assert ("Immutable logical plan completed." in messages) is success_log
    assert not {"valid", "execution_plan", "output"}.intersection(embedded)
    assert attempts == []


@pytest.mark.parametrize(
    ("pipeline", "success_log"),
    [
        (PLANNED_LOGICAL_YAML, True),
        (VALID_PIPELINE_YAML, False),
        ("project: [", False),
    ],
)
def test_trusted_build_workflow_embeds_exact_plan_and_only_planned_succeeds(
    monkeypatch, immutable_planning_tripwires, pipeline, success_log
):
    from odibi_mcp.tools import workflows

    def unexpected_tool(*args, **kwargs):
        pytest.fail("build planning workflow invoked an unregistered runtime or fallback tool")

    for name in workflows.TOOL_REGISTRY:
        if name not in {"apply_pattern_template", "test_pipeline"}:
            monkeypatch.setitem(workflows.TOOL_REGISTRY, name, unexpected_tool)
    monkeypatch.setitem(
        workflows.TOOL_REGISTRY,
        "apply_pattern_template",
        lambda **kwargs: {"valid": True, "yaml": pipeline},
    )

    with immutable_planning_tripwires() as attempts:
        result = D.dispatch(
            "run_workflow",
            workflow_name="build_and_validate",
            params={
                "pattern": "fact",
                "pipeline_name": "bounded",
                "source_connection": "source",
                "target_connection": "target",
                "target_path": "target",
                "source_table": "source",
            },
            application_identity=LOCAL_IDENTITY,
        )

    expected = planning.plan_pipeline_yaml(pipeline).to_dict()
    embedded = result["state"]["results"]["validation"]
    messages = [event["message"] for event in result["events"] if "message" in event]
    assert result["status"] == "COMPLETED"
    assert embedded == expected
    assert ("Immutable logical plan completed." in messages) is success_log
    assert attempts == []


def test_workflow_inventory_routes_only_planning_spelling_to_schema_one():
    from odibi_mcp.tools import workflows

    references = []

    def collect(value, workflow_name):
        if isinstance(value, dict):
            if value.get("type") == "call":
                references.append(
                    (workflow_name, value.get("tool"), value.get("args", {}).get("mode"))
                )
            for nested in value.values():
                collect(nested, workflow_name)
        elif isinstance(value, list):
            for nested in value:
                collect(nested, workflow_name)

    for workflow_name, definition in workflows.WORKFLOWS.items():
        collect(definition, workflow_name)

    planner_references = {
        reference
        for reference in references
        if reference[1] == "validate_yaml_runnable" or reference[2] == "dry-run"
    }
    validation_references = {reference for reference in references if reference[2] == "validate"}
    assert planner_references == {
        ("build_and_validate", "test_pipeline", "dry-run"),
        ("debug_pipeline", "validate_yaml_runnable", None),
    }
    assert validation_references == {
        ("validate_yaml_simple", "test_pipeline", "validate"),
        ("iterate_until_valid", "test_pipeline", "validate"),
    }
    assert _REMOTE_SAFE_WORKFLOWS == {"validate_yaml_simple"}


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
        pytest.fail("invalid controls must fail before validation or planning")

    monkeypatch.setattr(execution, "_validate_yaml_only", unexpected_call)
    monkeypatch.setattr(planning, "plan_pipeline_yaml", unexpected_call)

    with pytest.raises(error_type):
        execution.test_pipeline(VALID_PIPELINE_YAML, **kwargs)


def test_execution_helper_rejects_positional_mode_before_downstream_calls(monkeypatch):
    def unexpected_call(*args, **kwargs):
        pytest.fail("positional ambiguity must fail before validation or planning")

    monkeypatch.setattr(execution, "_validate_yaml_only", unexpected_call)
    monkeypatch.setattr(planning, "plan_pipeline_yaml", unexpected_call)

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
