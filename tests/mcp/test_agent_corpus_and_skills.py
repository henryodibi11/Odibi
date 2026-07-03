"""Guards for the agent-enablement layer: skills, docs corpus, live schema, examples.

These keep the MCP's agent-facing surface working and the packaged corpus in sync
with the repo's source-of-truth (docs/ + examples/ + .assistant/).
"""

from pathlib import Path

import pytest

from odibi_mcp.corpus import corpus_root
from odibi_mcp.knowledge import OdibiKnowledge

REPO = Path(__file__).resolve().parents[2]
KB = OdibiKnowledge()

KNOWN_SKILLS = {
    "odibi",
    "pipeline-yaml-authoring",
    "add-a-connection",
    "validation-workflow",
    "engine-parity",
    "databricks-notebook-protocol",
}


# --- skills ----------------------------------------------------------------


def test_list_skills_covers_inventory():
    names = {s["name"] for s in KB.list_skills()}
    assert KNOWN_SKILLS <= names, f"missing skills: {KNOWN_SKILLS - names}"
    # every skill has a non-empty description
    assert all(s["description"] for s in KB.list_skills())


def test_get_skill_returns_body_and_requires_resolve():
    names = {s["name"] for s in KB.list_skills()}
    for name in names:
        sk = KB.get_skill(name)
        assert sk.get("content"), f"{name} has no body"
        # any `requires:` must point at a skill that exists
        meta, _ = KB._parse_frontmatter(
            (corpus_root() / ".assistant" / "skills" / name / "SKILL.md").read_text(
                encoding="utf-8"
            )
        )
        req = meta.get("requires", "")
        for dep in req.strip("[]").split(","):
            dep = dep.strip()
            if dep:
                assert dep in names, f"{name} requires unknown skill {dep}"


def test_get_skill_unknown_lists_available():
    res = KB.get_skill("does-not-exist")
    assert "error" in res and "available" in res


# --- onboarding ------------------------------------------------------------


def test_onboard_payload():
    ob = KB.onboard()
    assert ob["instructions"], "onboard has no instructions"
    assert len(ob["skills"]) >= len(KNOWN_SKILLS)
    assert len(ob["workflow"]) >= 5


# --- live schema (always in sync with the models) --------------------------


@pytest.mark.parametrize(
    "section",
    [
        "project",
        "pipeline",
        "node",
        "read",
        "write",
        "transform",
        "validation",
        "gate",
        "quarantine",
        "privacy",
        "write_metadata",
        "lineage",
        "docs",
        "retry",
        "logging",
        "story",
        "system",
        "incremental",
        "alert",
        "performance",
        "simulation",
        "delete_detection",
    ],
)
def test_get_schema_generates_per_section(section):
    res = KB.get_schema(section)
    assert "schema" in res
    assert res["schema"].get("title")  # a real JSON Schema


def test_get_schema_default_returns_project_and_sections():
    res = KB.get_schema()
    assert "project" in res and "sections" in res
    assert "categories" in res
    assert "read" in res["sections"]
    assert "connections" in res["categories"]
    assert "api" in res["categories"]
    assert "project_sub" in res["categories"]


def test_get_schema_connections_returns_all_types():
    res = KB.get_schema("connections")
    assert res["section"] == "connections"
    assert "schemas" in res
    expected = {"local", "azure_blob", "delta", "unity_catalog", "sql_server", "http", "custom"}
    assert set(res["schemas"].keys()) == expected
    for name, schema in res["schemas"].items():
        assert schema.get("title"), f"connection {name} has no title"


def test_get_schema_api_returns_all_sub_models():
    res = KB.get_schema("api")
    assert res["section"] == "api"
    assert "schemas" in res
    expected = {"pagination", "response", "retry", "rate_limit", "options"}
    assert set(res["schemas"].keys()) == expected
    for name, schema in res["schemas"].items():
        assert schema.get("title"), f"api sub-model {name} has no title"


# --- docs + examples -------------------------------------------------------


def test_search_and_get_doc():
    hits = KB.search_docs("scd2")
    assert hits, "search_docs found nothing for scd2"
    doc = KB.get_doc(hits[0]["path"])
    assert "content" in doc and doc["content"]


def test_list_examples_nonempty():
    ex = KB.list_examples()
    assert len(ex) > 50  # the corpus ships the full example set


# --- corpus parity (catch a stale packaged snapshot) -----------------------


def test_packaged_corpus_in_sync_with_source():
    """The committed odibi_mcp/_corpus must not drift from the repo source.

    Skipped when the packaged snapshot isn't present (e.g. a sdist-only checkout).
    """
    packaged = REPO / "odibi_mcp" / "_corpus"
    if not packaged.is_dir():
        pytest.skip("packaged corpus not present")

    src_skills = {p.parent.name for p in (REPO / ".assistant" / "skills").glob("*/SKILL.md")}
    pkg_skills = {p.parent.name for p in (packaged / ".assistant" / "skills").glob("*/SKILL.md")}
    assert src_skills == pkg_skills, (
        f"corpus skills out of sync (run scripts/build_corpus.py): "
        f"src-only={src_skills - pkg_skills}, pkg-only={pkg_skills - src_skills}"
    )

    src_examples = sum(1 for _ in (REPO / "examples").rglob("*.y*ml"))
    pkg_examples = sum(1 for _ in (packaged / "examples").rglob("*.y*ml"))
    assert src_examples == pkg_examples, (
        f"corpus examples out of sync (run scripts/build_corpus.py): "
        f"src={src_examples} pkg={pkg_examples}"
    )
