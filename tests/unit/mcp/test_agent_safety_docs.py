"""Source and installed-wheel tripwires for agent-facing safety documentation."""

from __future__ import annotations

import ast
import importlib
import importlib.metadata
import importlib.util
import inspect
import json
from pathlib import Path
import re
import sys
from typing import get_args, get_type_hints

from odibi.cli.plan import _planning_exit_code
from odibi.planning import PlanningResponse, plan_pipeline_yaml
from odibi_mcp.contracts.access import RUNTIME_DATA_ACTIONS
from odibi_mcp.corpus import corpus_root
from odibi_mcp.dispatcher import ACTION_EFFECTS
from odibi_mcp.knowledge import OdibiKnowledge

HERE = Path(__file__).resolve()


def _find_repo(start: Path, max_parents: int = 6) -> Path | None:
    candidate = start.parent
    for _ in range(max_parents + 1):
        if (candidate / "pyproject.toml").is_file():
            return candidate
        if candidate.parent == candidate:
            break
        candidate = candidate.parent
    return None


REPO = _find_repo(HERE)
FIXTURE = (
    (REPO / "tests/fixtures/agent_safety_contract.json")
    if REPO
    else HERE.parent / "agent_safety_contract.json"
)


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _unique_string_list(value: object, *, name: str) -> list[str]:
    assert isinstance(value, list) and value, f"{name} must be a non-empty list"
    assert all(isinstance(item, str) and item for item in value)
    assert len(value) == len(set(value)), f"{name} must contain unique strings"
    return value


def _string_map(value: object, *, name: str) -> dict[str, str]:
    assert isinstance(value, dict) and value, f"{name} must be a non-empty object"
    assert all(isinstance(key, str) and key for key in value)
    assert all(isinstance(item, str) and item for item in value.values())
    return value


def _contract() -> dict[str, object]:
    data = json.loads(FIXTURE.read_text(encoding="utf-8"), object_pairs_hook=_reject_duplicate_keys)
    assert set(data) == {
        "schema_version",
        "cli_selectors",
        "planner_statuses",
        "planner_success_status",
        "action_effects",
        "runtime_data_actions_source_derived",
        "mirror_pairs",
        "dogfood",
        "forbidden_blanket_phrases",
        "selected_safety_pages",
        "stale_selected_dry_run_phrases",
    }
    assert type(data["schema_version"]) is int and data["schema_version"] == 1
    selectors = _string_map(data["cli_selectors"], name="cli_selectors")
    assert set(selectors) == {"validate", "plan", "legacy_simulation", "execute"}
    statuses = _unique_string_list(data["planner_statuses"], name="planner_statuses")
    assert isinstance(data["planner_success_status"], str)
    assert data["planner_success_status"] in statuses
    _string_map(data["action_effects"], name="action_effects")
    _unique_string_list(
        data["runtime_data_actions_source_derived"],
        name="runtime_data_actions_source_derived",
    )
    mirrors = _string_map(data["mirror_pairs"], name="mirror_pairs")
    assert len(mirrors.values()) == len(set(mirrors.values()))
    dogfood = data["dogfood"]
    assert isinstance(dogfood, dict) and dogfood
    assert all(isinstance(path, str) and path for path in dogfood)
    for path, tokens in dogfood.items():
        _unique_string_list(tokens, name=f"dogfood[{path}]")
    _unique_string_list(data["forbidden_blanket_phrases"], name="forbidden_blanket_phrases")
    _unique_string_list(data["selected_safety_pages"], name="selected_safety_pages")
    _unique_string_list(
        data["stale_selected_dry_run_phrases"], name="stale_selected_dry_run_phrases"
    )
    return data


def _odibi_execute_parameters() -> list[str]:
    if importlib.util.find_spec("fastmcp") is not None:
        from odibi_mcp.mcp_server import odibi_execute

        return list(inspect.signature(odibi_execute).parameters)
    assert REPO is not None
    source = (REPO / "odibi_mcp/mcp_server.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "odibi_execute"
    ]
    assert len(functions) == 1
    function = functions[0]
    assert len(function.args.defaults) == 1
    assert isinstance(function.args.defaults[0], ast.Constant)
    assert function.args.defaults[0].value is None
    return [argument.arg for argument in function.args.args]


def test_fixture_matches_live_safety_constants():
    data = _contract()
    assert data["planner_statuses"] == list(get_args(get_type_hints(PlanningResponse)["status"]))
    assert data["planner_success_status"] == "planned"
    assert {name: ACTION_EFFECTS[name].value for name in data["action_effects"]} == data[
        "action_effects"
    ]
    assert set(data["runtime_data_actions_source_derived"]) == set(RUNTIME_DATA_ACTIONS)

    planned = plan_pipeline_yaml(
        "project: demo\npipelines:\n"
        "  - pipeline: ingest\n"
        "    nodes:\n"
        "      - name: source\n"
        "        read: {}\n"
    )
    assert planned.status == "planned" and _planning_exit_code(planned) == 0
    invalid = plan_pipeline_yaml("pipelines: nope\n")
    assert invalid.status == "invalid" and _planning_exit_code(invalid) == 2
    unresolved = plan_pipeline_yaml(
        "project: demo\nconnections:\n"
        "  external:\n    type: secret-provider-canary\n"
        "pipelines:\n  - pipeline: ingest\n    nodes:\n"
        "      - name: source\n        read:\n"
        "          connection: external\n          path: canary.csv\n"
    )
    assert unresolved.status == "unresolved" and _planning_exit_code(unresolved) == 3


def test_live_cli_parser_accepts_all_documented_selectors(monkeypatch):
    cli_main = importlib.import_module("odibi.cli.main")
    cli_run = importlib.import_module("odibi.cli.run")

    captured = []
    monkeypatch.setattr(cli_main, "immutable_plan_command", lambda args: captured.append(args) or 0)
    monkeypatch.setattr(cli_main, "cmd_validate", lambda args: captured.append(args) or 0)
    monkeypatch.setattr(cli_run, "run_command", lambda args: captured.append(args) or 0)
    for argv in (
        ["odibi", "plan", "--stdin", "--format", "json"],
        ["odibi", "validate", "CONFIG", "--format", "json"],
        ["odibi", "run", "CONFIG", "--dry-run"],
        ["odibi", "run", "CONFIG"],
    ):
        monkeypatch.setattr(sys, "argv", argv)
        assert cli_main.main() == 0
    assert captured[0].stdin is True and captured[0].format == "json"
    assert captured[1].file == "CONFIG" and captured[1].format == "json"
    assert captured[2].config == "CONFIG" and captured[2].dry_run is True
    assert captured[3].config == "CONFIG" and captured[3].dry_run is False


def test_planning_authority_has_exact_five_rungs_and_selectors():
    root = REPO or corpus_root()
    text = (root / "docs/features/planning.md").read_text(encoding="utf-8")
    for rung in (
        "**1. Inspect**",
        "**2. Validate**",
        "**3. Plan**",
        "**4. Legacy simulation**",
        "**5. Execute**",
    ):
        assert text.count(rung) == 1
    for selector in _contract()["cli_selectors"].values():
        assert selector in text.replace("\\|", "|")
    assert "There is no command named `odibi inspect`" in text
    assert 'Only `status == "planned"`' in text
    assert "[MCP route guidance](../guides/mcp_guide.md#transport-routes)" in text


def test_selected_dry_run_links_resolve_to_planning_authority():
    if REPO is None:
        return
    expected = {
        "docs/ODIBI_DEEP_CONTEXT.md": {
            "features/planning.md#operation-safety-ladder": 2,
            "features/planning.md#legacy-runtime-simulation-is-different": 2,
        },
        "docs/reference/developer_cheatsheet.md": {
            "../features/planning.md#operation-safety-ladder": 1,
        },
        "docs/visuals/run_lifecycle.md": {
            "../features/planning.md#operation-safety-ladder": 1,
        },
    }
    for source_name, links in expected.items():
        source = REPO / source_name
        text = source.read_text(encoding="utf-8")
        for link, minimum_count in links.items():
            assert text.count(f"]({link})") >= minimum_count
            path, anchor = link.split("#", 1)
            target = (source.parent / path).resolve()
            assert target.is_relative_to(REPO.resolve()) and target.is_file()
            headings = target.read_text(encoding="utf-8")
            slugs = {
                re.sub(r"[^a-z0-9 -]", "", heading.casefold()).strip().replace(" ", "-")
                for heading in re.findall(r"^#{1,6}\s+(.+)$", headings, flags=re.MULTILINE)
            }
            assert anchor in slugs


def test_selected_canonical_mirrors_are_byte_identical_in_source_mode():
    if REPO is None:
        return
    for source, mirror in _contract()["mirror_pairs"].items():
        assert (REPO / source).read_bytes() == (REPO / mirror).read_bytes()


def test_agent_searches_authority_then_retrieves_bounded_answers():
    data = _contract()
    kb = OdibiKnowledge()
    all_text = ""
    for path, tokens in data["dogfood"].items():
        hits = kb.search_docs(tokens[0])
        assert any(hit["path"] == path for hit in hits)
        result = kb.get_doc(path)
        assert result["path"] == path
        text = result["content"]
        all_text += text
        for token in tokens:
            assert token in text
    for phrase in data["forbidden_blanket_phrases"]:
        assert phrase.casefold() not in all_text.casefold()


def test_selected_pages_reject_stale_safety_and_transport_spelling():
    if REPO is None:
        return
    data = _contract()
    selected = "\n".join(
        (REPO / path).read_text(encoding="utf-8") for path in data["selected_safety_pages"]
    ).casefold()
    for phrase in data["forbidden_blanket_phrases"]:
        assert phrase.casefold() not in selected
    for phrase in data["stale_selected_dry_run_phrases"]:
        assert phrase.casefold() not in selected
    assert "params_json" not in selected
    assert selected.count("odibi_execute(action, args_json=none)") == 2
    assert _odibi_execute_parameters() == ["action", "args_json"]


def test_installed_corpus_and_readme_metadata():
    if REPO is not None:
        return
    import odibi_mcp

    root = corpus_root().resolve()
    package_corpus = Path(odibi_mcp.__file__).resolve().parent / "_corpus"
    assert root == package_corpus.resolve()
    kb = OdibiKnowledge()
    for mirror in _contract()["mirror_pairs"].values():
        relative = mirror.removeprefix("odibi_mcp/_corpus/")
        assert (root / relative).resolve().is_relative_to(root)
        assert (root / relative).is_file()
        result = kb.get_doc(relative)
        assert result["path"] == relative and result["content"]
    gateway = kb.get_doc("docs/guides/mcp_guide.md")["content"]
    assert "odibi_execute(action, args_json=None)" in gateway
    assert "params_json" not in gateway
    metadata = importlib.metadata.metadata("odibi")
    description = metadata.get_payload()
    assert "Effects before you run" in description
    assert "odibi-quickstart-output/" in description
