"""Phase 0 safety net: every shipped YAML must match its declared kind.

This is the anti-rot guard for the YAML-hardening work. It loads *every* YAML we
ship (examples/, docs/, scaffold templates) through the SAME model `odibi run`
uses (`ProjectConfig`, after registering the standard transformer library) and
asserts each file behaves as its category says it should:

  * RUNNABLE (default)  -> must construct cleanly under ProjectConfig.
  * FRAGMENT            -> a `pipelines:`-only fragment meant to be imported;
                           must NOT construct as a full ProjectConfig.
  * SIM_SPEC            -> a standalone simulation spec (scope/entities/columns),
                           a different schema entirely; must NOT be a ProjectConfig.
  * NEEDS_ENV           -> runnable once its env vars are set; we set dummies.

A NEW yaml that is broken falls into the default RUNNABLE branch and fails here,
forcing the author to fix it or categorize it deliberately. That is the point:
our own examples can no longer silently rot, and "valid" tracks "runs".
"""

from pathlib import Path

import pytest
import yaml

from odibi.config import ProjectConfig
from odibi.transformers import register_standard_library
from odibi.utils.config_loader import load_yaml_with_env

register_standard_library()

REPO_ROOT = Path(__file__).resolve().parents[2]
SEARCH_DIRS = ("examples", "docs", "odibi/scaffold")


def _discover():
    files = []
    for base in SEARCH_DIRS:
        root = REPO_ROOT / base
        if not root.exists():
            continue
        files += list(root.rglob("*.yaml")) + list(root.rglob("*.yml"))
    rel = []
    for f in files:
        p = f.relative_to(REPO_ROOT).as_posix()
        if ".github" in p or "mcp_config" in p:
            continue
        rel.append(p)
    return sorted(set(rel))


ALL_YAML = _discover()

# ── Taxonomy ──────────────────────────────────────────────────────────────────
# Pipeline fragments: start at `pipelines:` with no project/story/system. Meant
# to be imported, not run standalone. (Tracked for Phase 5 — may be completed.)
FRAGMENTS = {
    "examples/phase1/customer_dimension.yaml",
    "examples/phase1/date_dimension.yaml",
    "examples/phase1/employee_scd2.yaml",
    "examples/phase1/fact_orders.yaml",
    "examples/phase1/monthly_sales_agg.yaml",
}

# Standalone simulation specs (scope/entities/columns at top level) — NOT pipeline
# configs. A different schema; ProjectConfig rightly rejects them.
SIM_SPECS = {
    "examples/chemical_engineering/cstr_flowsheet_complete.yaml",
    "examples/renewable_energy/solar_thermal_tracking.yaml",
}

# Runnable once env vars are provided. Map file -> {VAR: dummy_value}.
NEEDS_ENV = {
    "examples/improvement_target.odibi.yaml": {
        "BOUND_SOURCE_ROOT": "/tmp/bound",
        "ARTIFACTS_ROOT": "/tmp/artifacts",
    },
    "examples/walkthrough_test/bronze_production_orders.odibi.yaml": {
        "SAP_ERP_CONN": "Server=localhost;Database=erp;",
    },
}


def _raw(path):
    with open(REPO_ROOT / path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def test_yaml_inventory_is_nonempty():
    # Guard against a glob/path regression silently making this suite a no-op.
    assert len(ALL_YAML) >= 100, (
        f"expected to discover the shipped YAML corpus, got {len(ALL_YAML)}"
    )


@pytest.mark.parametrize("rel", ALL_YAML)
def test_shipped_yaml_matches_declared_kind(rel, monkeypatch):
    if rel in FRAGMENTS:
        raw = _raw(rel)
        assert "pipelines" in raw and "project" not in raw, (
            f"{rel} is listed as a FRAGMENT but doesn't look like one "
            f"(expected top-level `pipelines:` and no `project:`)."
        )
        with pytest.raises(Exception):
            ProjectConfig(**load_yaml_with_env(str(REPO_ROOT / rel), _defer_substitution=True))
        return

    if rel in SIM_SPECS:
        raw = _raw(rel)
        assert "columns" in raw and "project" not in raw, (
            f"{rel} is listed as a SIM_SPEC but doesn't look like one "
            f"(expected a top-level simulation spec, not a project config)."
        )
        return

    if rel in NEEDS_ENV:
        for var, val in NEEDS_ENV[rel].items():
            monkeypatch.setenv(var, val)

    cfg = load_yaml_with_env(str(REPO_ROOT / rel), _defer_substitution=True)
    try:
        ProjectConfig(**cfg)
    except Exception as e:  # pragma: no cover - failure path is the message
        pytest.fail(
            f"{rel} does not construct under the runtime model (ProjectConfig).\n"
            f"If this is intentional, add it to FRAGMENTS / SIM_SPECS / NEEDS_ENV in "
            f"this test. Otherwise fix the YAML.\n\n{type(e).__name__}: {str(e)[:600]}"
        )


def test_taxonomy_entries_exist():
    # Keep the manifest honest: every categorized path must still exist.
    for rel in sorted(FRAGMENTS | SIM_SPECS | set(NEEDS_ENV)):
        assert (REPO_ROOT / rel).exists(), f"taxonomy lists a missing file: {rel}"
