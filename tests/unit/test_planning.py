"""Adversarial tests for the immutable package planning boundary."""

from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, fields, is_dataclass
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import threading
from typing import Callable, ContextManager, Dict, List
import warnings

import pytest
import yaml

import odibi.planning as planning
from odibi.planning import (
    DEFAULT_PLANNING_LIMITS,
    DiagnosticSubject,
    LogicalEdge,
    LogicalNodePlan,
    LogicalPipelinePlan,
    LogicalProjectPlan,
    PlanningDiagnostic,
    PlanningLimits,
    PlanningResponse,
    plan_pipeline_bytes,
    plan_pipeline_yaml,
)


PLANNED_YAML = """\
project: demo
pipelines:
  - pipeline: ingest
    nodes:
      - name: source
        read: {}
      - name: sink
        depends_on: [source, source]
        write: {}
"""
INVALID_YAML = """\
project: demo
pipelines:
  - pipeline: ingest
    nodes:
      - name: sink
        depends_on: [missing]
        write: {}
"""
LIMIT_YAML = "x" * (DEFAULT_PLANNING_LIMITS.max_input_bytes + 1)
CANARIES = (
    "/physical/secret/source.csv",
    "https://provider.invalid/resource",
    "credential-canary-71db",
    "provider-canary-823c",
    "environment-canary-f10a",
    "exception-canary-e933",
    "/tmp/temp-canary-b6d2",
    "host-canary-a901",
)
UNRESOLVED_YAML = f"""\
project: demo
engine: spark
connections:
  external:
    type: {CANARIES[3]}
    token: {CANARIES[2]}
story:
  path: {CANARIES[0]}
imports:
  - {CANARIES[6]}
pipelines:
  - pipeline: ingest
    nodes:
      - name: source
        read:
          connection: external
          path: {CANARIES[0]}
      - name: transform
        depends_on: [source]
        transformer: custom_transform
        params:
          endpoint: {CANARIES[1]}
"""


def _limits(**overrides: int) -> PlanningLimits:
    """Return nominal limits with selected ceilings tightened."""
    values = {
        item.name: getattr(DEFAULT_PLANNING_LIMITS, item.name) for item in fields(PlanningLimits)
    }
    values.update(overrides)
    return PlanningLimits(**values)


def _failure_code(response: PlanningResponse) -> str:
    """Return the sole code from a fixed invalid response."""
    assert response.status == "invalid"
    assert response.plan is None
    assert len(response.diagnostics) == 1
    return response.diagnostics[0].code


def _pipeline_yaml(node_count: int, *, pipeline_name: str = "p") -> str:
    """Build a closed logical pipeline containing resolved read nodes."""
    nodes = "\n".join(f"      - name: n{index}\n        read: {{}}" for index in range(node_count))
    return f"pipelines:\n  - pipeline: {pipeline_name}\n    nodes:\n{nodes}\n"


def _state_snapshot() -> Dict[str, object]:
    """Capture process globals that one planner operation must preserve."""
    root = logging.getLogger()
    loggers = {
        name: (
            logger.level,
            logger.disabled,
            logger.propagate,
            tuple(id(handler) for handler in logger.handlers),
            tuple(id(item) for item in logger.filters),
        )
        for name, logger in logging.Logger.manager.loggerDict.items()
        if isinstance(logger, logging.Logger)
    }
    return {
        "cwd": os.getcwd(),
        "environment": dict(os.environ),
        "sys_path": tuple(sys.path),
        "modules": tuple(sorted((name, id(module)) for name, module in sys.modules.items())),
        "warnings": tuple(warnings.filters),
        "root_logger": (
            root.level,
            root.disabled,
            tuple(id(handler) for handler in root.handlers),
            tuple(id(item) for item in root.filters),
        ),
        "loggers": loggers,
        "threads": tuple((thread.ident, thread.name) for thread in threading.enumerate()),
    }


def test_exact_planned_schema_and_canonical_serialization() -> None:
    """A closed document returns only the exact source-ordered logical schema."""
    response = plan_pipeline_yaml(PLANNED_YAML)

    assert response == PlanningResponse(
        schema_version="1.0",
        status="planned",
        plan=LogicalProjectPlan(
            name="demo",
            pipelines=(
                LogicalPipelinePlan(
                    name="ingest",
                    nodes=(
                        LogicalNodePlan(
                            name="source",
                            kind="read",
                            depends_on=(),
                            resolution="resolved",
                        ),
                        LogicalNodePlan(
                            name="sink",
                            kind="write",
                            depends_on=("source",),
                            resolution="resolved",
                        ),
                    ),
                    edges=(LogicalEdge(source="source", target="sink"),),
                ),
            ),
        ),
        diagnostics=(),
        truncated=False,
    )
    expected = {
        "schema_version": "1.0",
        "status": "planned",
        "plan": {
            "name": "demo",
            "pipelines": [
                {
                    "name": "ingest",
                    "nodes": [
                        {
                            "name": "source",
                            "kind": "read",
                            "depends_on": [],
                            "resolution": "resolved",
                        },
                        {
                            "name": "sink",
                            "kind": "write",
                            "depends_on": ["source"],
                            "resolution": "resolved",
                        },
                    ],
                    "edges": [{"source": "source", "target": "sink"}],
                }
            ],
        },
        "diagnostics": [],
        "truncated": False,
    }
    assert response.to_dict() == expected
    assert response.to_json() == json.dumps(
        expected,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    assert all(plan_pipeline_yaml(PLANNED_YAML) == response for _ in range(100))


def test_public_dtos_are_frozen_slotted_dataclasses() -> None:
    """Every public transfer object is nominal, frozen, dataclass-backed, and slotted."""
    instances = (
        DEFAULT_PLANNING_LIMITS,
        DiagnosticSubject("document", None),
        PlanningDiagnostic(
            "SCHEMA_INVALID", "error", "schema", DiagnosticSubject("document", None), "x"
        ),
        LogicalEdge("a", "b"),
        LogicalNodePlan("a", "read", (), "resolved"),
        LogicalPipelinePlan("p", (), ()),
        LogicalProjectPlan(None, ()),
        PlanningResponse("1.0", "planned", LogicalProjectPlan(None, ()), (), False),
    )
    for instance in instances:
        assert is_dataclass(instance)
        assert not hasattr(instance, "__dict__")
        with pytest.raises(FrozenInstanceError):
            setattr(instance, fields(instance)[0].name, "mutated")


def test_unresolved_output_is_typed_bounded_and_non_echoing() -> None:
    """Excluded extension/provider/runtime meaning is unresolved without physical echoes."""
    response = plan_pipeline_yaml(UNRESOLVED_YAML)
    rendered = response.to_json()

    assert response.status == "unresolved"
    assert response.plan is not None
    assert {item.code for item in response.diagnostics} == {
        "UNRESOLVED_EXTENSION_DEPENDENCY",
        "UNRESOLVED_PROVIDER_DEPENDENCY",
        "UNRESOLVED_RUNTIME_DEPENDENCY",
    }
    assert all(item.severity == "error" for item in response.diagnostics)
    assert all(node.resolution == "unresolved" for node in response.plan.pipelines[0].nodes)
    assert response.plan.pipelines[0].nodes[1].kind == "transform"
    assert all(canary not in rendered for canary in CANARIES)
    assert not {
        "valid",
        "errors",
        "warnings",
        "mode",
        "execution_plan",
        "output",
    }.intersection(response.to_dict())


def test_inactive_runtime_fields_do_not_create_false_unresolved_meaning() -> None:
    """Null and empty excluded-facility fields carry no runtime meaning."""
    payload = PLANNED_YAML.replace(
        "project: demo\n",
        "project: demo\nengine: null\nconnections: {}\nimports: []\nstory: {}\n",
    )

    response = plan_pipeline_yaml(payload)

    assert response.status == "planned"
    assert response.diagnostics == ()
    assert response.plan is not None
    assert all(
        node.resolution == "resolved"
        for pipeline in response.plan.pipelines
        for node in pipeline.nodes
    )


@pytest.mark.parametrize("field", ("extension", "plugin", "custom", "pattern"))
@pytest.mark.parametrize("value", ("{}", "{name: custom_handler}"))
def test_unknown_extension_nodes_always_fail_closed_as_unresolved(field: str, value: str) -> None:
    """Unknown-kind extension forms never become resolved when their value is empty."""
    payload = (
        f"pipelines:\n  - pipeline: p\n    nodes:\n      - name: n\n        {field}: {value}\n"
    )

    response = plan_pipeline_yaml(payload)

    assert response.status == "unresolved"
    assert response.plan is not None
    node = response.plan.pipelines[0].nodes[0]
    assert node.kind == "unknown"
    assert node.resolution == "unresolved"
    assert {item.code for item in response.diagnostics} == {"UNRESOLVED_EXTENSION_DEPENDENCY"}


@pytest.mark.parametrize(
    ("payload", "code"),
    (
        ("project: [", "YAML_SYNTAX_INVALID"),
        ("---\na: 1\n---\nb: 2\n", "MULTIPLE_DOCUMENTS"),
        ("project: a\nproject: b\npipelines: []\n", "SCHEMA_INVALID"),
        ("project: !!python/object:builtins.object {}\n", "SCHEMA_INVALID"),
        (
            "base: &base {project: demo}\n<<: *base\npipelines: []\n",
            "SCHEMA_INVALID",
        ),
        ("recursive: &recursive [*recursive]\n", "STRUCTURE_LIMIT_EXCEEDED"),
        ("project: 2026-07-27\npipelines: []\n", "SCHEMA_INVALID"),
        ("project: .nan\npipelines: []\n", "SCHEMA_INVALID"),
        (
            "pipelines:\n- pipeline: p\n  nodes:\n  - name: n\n    arbitrary: true\n",
            "SCHEMA_INVALID",
        ),
        (INVALID_YAML, "REFERENCE_INVALID"),
        (
            "pipelines:\n- pipeline: p\n  nodes:\n"
            "  - name: a\n    depends_on: [b]\n    read: {}\n"
            "  - name: b\n    depends_on: [a]\n    read: {}\n",
            "DEPENDENCY_CYCLE",
        ),
    ),
)
def test_invalid_inputs_use_fixed_non_reflective_diagnostics(payload: str, code: str) -> None:
    """Parser/schema/reference/cycle failures never return raw input or exceptions."""
    response = plan_pipeline_yaml(payload + "\n# exception-canary-e933")
    assert _failure_code(response) == code
    assert "exception-canary-e933" not in response.to_json()


def test_bytes_entry_rejects_invalid_type_utf8_and_oversize_before_parse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bytes planning enforces type, N/N+1 length, and strict UTF-8 before YAML scan."""
    calls = 0

    def parse_spy(*args: object, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        raise AssertionError("parser should not run")

    monkeypatch.setattr(planning, "_load_one_bounded_document", parse_spy)
    assert _failure_code(plan_pipeline_bytes(bytearray(b"x"))) == "INPUT_LIMIT_EXCEEDED"
    assert _failure_code(plan_pipeline_bytes(b"\xff")) == "INVALID_UTF8"
    assert _failure_code(plan_pipeline_bytes(b"x" * 1_048_577)) == "INPUT_LIMIT_EXCEEDED"
    assert calls == 0


def test_input_byte_limit_accepts_n_and_rejects_n_plus_one_for_text_and_bytes() -> None:
    """Both APIs use the same exact default UTF-8 byte boundary."""
    suffix_size = DEFAULT_PLANNING_LIMITS.max_input_bytes - len(PLANNED_YAML.encode())
    at_limit = PLANNED_YAML + ("#" * suffix_size)
    assert len(at_limit.encode()) == DEFAULT_PLANNING_LIMITS.max_input_bytes

    assert plan_pipeline_yaml(at_limit).status == "planned"
    assert plan_pipeline_bytes(at_limit.encode()).status == "planned"
    assert _failure_code(plan_pipeline_yaml(at_limit + "x")) == "INPUT_LIMIT_EXCEEDED"
    assert _failure_code(plan_pipeline_bytes(at_limit.encode() + b"x")) == "INPUT_LIMIT_EXCEEDED"


def test_utf8_limit_counts_encoded_bytes_not_codepoints() -> None:
    """A tightened text limit applies to encoded UTF-8 width and rejects surrogates."""
    limits = _limits(max_input_bytes=4)
    assert _failure_code(plan_pipeline_yaml("ééx", limits=limits)) == "INPUT_LIMIT_EXCEEDED"
    assert _failure_code(plan_pipeline_yaml("\ud800", limits=limits)) == "INVALID_UTF8"


def test_depth_node_and_alias_limits_accept_n_and_reject_n_plus_one() -> None:
    """Pre-load structure counters enforce exact tightened ceilings."""
    depth_n = "pipelines:\n- pipeline: p\n  nodes:\n  - name: n\n    read: {}\n"
    depth_n_plus_one = (
        "pipelines:\n- pipeline: p\n  nodes:\n  - name: n\n    read:\n      nested: []\n"
    )
    assert plan_pipeline_yaml(depth_n, limits=_limits(max_yaml_depth=6)).status == "planned"
    assert (
        _failure_code(plan_pipeline_yaml(depth_n_plus_one, limits=_limits(max_yaml_depth=6)))
        == "STRUCTURE_LIMIT_EXCEEDED"
    )

    node_count = sum(
        isinstance(event, (yaml.MappingStartEvent, yaml.SequenceStartEvent, yaml.ScalarEvent))
        for event in yaml.parse(depth_n)
    )
    assert (
        plan_pipeline_yaml(depth_n, limits=_limits(max_yaml_nodes=node_count)).status == "planned"
    )
    assert (
        _failure_code(plan_pipeline_yaml(depth_n, limits=_limits(max_yaml_nodes=node_count - 1)))
        == "STRUCTURE_LIMIT_EXCEEDED"
    )

    one_alias = (
        "vars:\n  source: &source []\n  copies: [*source]\n"
        "pipelines:\n- pipeline: p\n  nodes:\n  - name: n\n    read: {}\n"
    )
    two_aliases = one_alias.replace("copies: [*source]", "copies: [*source, *source]")
    assert plan_pipeline_yaml(one_alias, limits=_limits(max_aliases=1)).status == "unresolved"
    assert (
        _failure_code(plan_pipeline_yaml(two_aliases, limits=_limits(max_aliases=1)))
        == "STRUCTURE_LIMIT_EXCEEDED"
    )


def test_plan_count_limits_accept_n_and_reject_n_plus_one() -> None:
    """Pipeline, per-pipeline node, total-node, and edge limits are exact."""
    one_pipeline = _pipeline_yaml(1)
    two_pipelines = one_pipeline + _pipeline_yaml(1, pipeline_name="q").replace(
        "pipelines:\n", "", 1
    )
    assert plan_pipeline_yaml(one_pipeline, limits=_limits(max_pipelines=1)).status == "planned"
    assert (
        _failure_code(plan_pipeline_yaml(two_pipelines, limits=_limits(max_pipelines=1)))
        == "PLAN_LIMIT_EXCEEDED"
    )

    assert (
        plan_pipeline_yaml(_pipeline_yaml(2), limits=_limits(max_nodes_per_pipeline=2)).status
        == "planned"
    )
    assert (
        _failure_code(
            plan_pipeline_yaml(_pipeline_yaml(3), limits=_limits(max_nodes_per_pipeline=2))
        )
        == "PLAN_LIMIT_EXCEEDED"
    )

    two_total = (
        "pipelines:\n"
        "- pipeline: p\n  nodes:\n  - name: a\n    read: {}\n"
        "- pipeline: q\n  nodes:\n  - name: b\n    read: {}\n"
    )
    three_total = two_total.replace(
        "- pipeline: q\n  nodes:\n", "- pipeline: q\n  nodes:\n  - name: c\n    read: {}\n"
    )
    assert plan_pipeline_yaml(two_total, limits=_limits(max_nodes_total=2)).status == "planned"
    assert (
        _failure_code(plan_pipeline_yaml(three_total, limits=_limits(max_nodes_total=2)))
        == "PLAN_LIMIT_EXCEEDED"
    )

    one_edge = (
        "pipelines:\n- pipeline: p\n  nodes:\n"
        "  - name: a\n    read: {}\n"
        "  - name: b\n    depends_on: [a]\n    write: {}\n"
    )
    two_edges = one_edge.replace("depends_on: [a]", "depends_on: [a, c]").replace(
        "  - name: b", "  - name: c\n    read: {}\n  - name: b"
    )
    assert plan_pipeline_yaml(one_edge, limits=_limits(max_edges_total=1)).status == "planned"
    assert (
        _failure_code(plan_pipeline_yaml(two_edges, limits=_limits(max_edges_total=1)))
        == "PLAN_LIMIT_EXCEEDED"
    )


def test_identifier_limit_normalization_and_rejections() -> None:
    """Identifiers normalize to NFC before duplicate/reference checks and reject paths."""
    one = _pipeline_yaml(1, pipeline_name="é").replace("n0", "n")
    assert plan_pipeline_yaml(one, limits=_limits(max_identifier_codepoints=1)).status == "planned"
    assert (
        _failure_code(
            plan_pipeline_yaml(
                _pipeline_yaml(1, pipeline_name="éx").replace("n0", "n"),
                limits=_limits(max_identifier_codepoints=1),
            )
        )
        == "SCHEMA_INVALID"
    )
    normalized_duplicate = (
        "pipelines:\n- pipeline: p\n  nodes:\n- name: é\n  read: {}\n- name: e\u0301\n  read: {}\n"
    )
    assert _failure_code(plan_pipeline_yaml(normalized_duplicate)) == "SCHEMA_INVALID"
    for identifier in ("../secret", "a\\b", "https:secret", "bad\x00name"):
        payload = _pipeline_yaml(1, pipeline_name=json.dumps(identifier))
        assert _failure_code(plan_pipeline_yaml(payload)) == "SCHEMA_INVALID"


def test_diagnostic_truncation_is_canonical_and_status_remains_unresolved() -> None:
    """All unresolved errors influence status before deterministic diagnostic retention."""
    nodes = "\n".join(
        f"  - name: n{index}\n    extension: {{name: hidden{index}}}" for index in range(257)
    )
    payload = f"pipelines:\n- pipeline: p\n  nodes:\n{nodes}\n"
    response = plan_pipeline_yaml(payload)

    assert response.status == "unresolved"
    assert response.plan is not None
    assert len(response.plan.pipelines[0].nodes) == 257
    assert len(response.diagnostics) == 256
    assert response.truncated is True
    names = [item.subject.name for item in response.diagnostics]
    assert names == sorted(names)


def test_response_limit_accepts_n_and_replaces_n_plus_one_deterministically() -> None:
    """Canonical response size is checked exactly and falls back without a partial plan."""
    response = plan_pipeline_yaml(PLANNED_YAML)
    encoded_size = len(response.to_json().encode())
    assert (
        plan_pipeline_yaml(PLANNED_YAML, limits=_limits(max_response_bytes=encoded_size))
        == response
    )

    limited = plan_pipeline_yaml(PLANNED_YAML, limits=_limits(max_response_bytes=encoded_size - 1))
    assert _failure_code(limited) == "RESPONSE_LIMIT_EXCEEDED"
    assert limited.truncated is True


def test_emitted_non_identifier_limit_fails_closed() -> None:
    """Tightening below a planner-owned literal produces only a fixed internal failure."""
    response = plan_pipeline_yaml(
        PLANNED_YAML,
        limits=_limits(max_emitted_string_codepoints=len("resolved") - 1),
    )

    assert _failure_code(response) == "INTERNAL_PLANNING_FAILURE"


@pytest.mark.parametrize(
    "field_name,bad_value",
    (
        ("max_input_bytes", 0),
        ("max_aliases", True),
        ("max_pipelines", DEFAULT_PLANNING_LIMITS.max_pipelines + 1),
    ),
)
def test_nominal_limits_only_tighten_defaults(field_name: str, bad_value: int) -> None:
    """Wrong, boolean, non-positive, raised, subclass, and structural authorities fail closed."""
    assert (
        _failure_code(plan_pipeline_yaml(PLANNED_YAML, limits=_limits(**{field_name: bad_value})))
        == "INVALID_LIMITS"
    )

    class LimitsSubclass(PlanningLimits):
        pass

    assert (
        _failure_code(plan_pipeline_yaml(PLANNED_YAML, limits=LimitsSubclass())) == "INVALID_LIMITS"
    )
    assert _failure_code(plan_pipeline_yaml(PLANNED_YAML, limits=object())) == "INVALID_LIMITS"


@pytest.mark.parametrize(
    "checkpoint",
    (
        "_validate_input",
        "_load_one_bounded_document",
        "_project_closed_logical_schema",
        "_finalize_response",
    ),
)
def test_injected_internal_failures_are_fixed_and_sanitized(
    monkeypatch: pytest.MonkeyPatch, checkpoint: str
) -> None:
    """Unexpected checkpoint failures return one non-reflective internal response."""

    def fail(*args: object, **kwargs: object) -> None:
        raise RuntimeError("exception-canary-e933 /physical/secret/source.csv")

    monkeypatch.setattr(planning, checkpoint, fail)
    response = plan_pipeline_yaml(PLANNED_YAML)
    assert _failure_code(response) == "INTERNAL_PLANNING_FAILURE"
    assert all(canary not in response.to_json() for canary in CANARIES)


def test_injected_serialization_failure_is_fixed_and_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tentative response serialization failure maps to the fixed serialization code."""

    def fail(*args: object, **kwargs: object) -> None:
        raise RuntimeError("exception-canary-e933")

    monkeypatch.setattr(planning, "_tuples_to_lists", fail)
    response = plan_pipeline_yaml(PLANNED_YAML)
    assert response.status == "invalid"
    assert response.diagnostics[0].code == "INTERNAL_SERIALIZATION_FAILURE"
    assert "exception-canary-e933" not in str(response)


def test_planner_call_triggers_no_effect_or_global_mutation(
    immutable_planning_tripwires: Callable[[], ContextManager[List[str]]],
) -> None:
    """Success and failure execute positively under deny-by-default effect tripwires."""
    with immutable_planning_tripwires() as attempts:
        before = _state_snapshot()
        success = plan_pipeline_yaml(PLANNED_YAML)
        failure = plan_pipeline_yaml(INVALID_YAML)
        after = _state_snapshot()

    assert success.status == "planned"
    assert _failure_code(failure) == "REFERENCE_INVALID"
    assert attempts == []
    assert after == before


def test_planner_module_has_only_allowed_static_imports() -> None:
    """Static import graph remains standard-library plus PyYAML with no Odibi runtime."""
    source = Path(planning.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".", 1)[0])
    assert imports <= {
        "__future__",
        "dataclasses",
        "json",
        "math",
        "re",
        "types",
        "typing",
        "unicodedata",
        "yaml",
    }


def test_package_import_is_runtime_free_and_lazy_exports_remain_compatible() -> None:
    """Fresh package/planner imports are light while explicit runtime exports still work."""
    script = r"""
import json
import sys
import odibi
root_modules = sorted(name for name in sys.modules if name.startswith("odibi"))
import odibi.planning
planning_modules = sorted(name for name in sys.modules if name.startswith("odibi"))
from odibi import Context, transform
from odibi.registry import FunctionRegistry
from odibi.validate.pipeline import validate_yaml
validation = validate_yaml(
    "pipelines:\n"
    "  - pipeline: p\n"
    "    nodes:\n"
    "      - name: n\n"
    "        transform:\n"
    "          steps:\n"
    "            - function: clean_text\n"
    "              params: {columns: [name]}\n"
)
print(json.dumps({
    "root": root_modules,
    "planning": planning_modules,
    "context": Context.__name__,
    "transform": transform.__name__,
    "validation": validation["valid"],
    "registered": len(FunctionRegistry.list_functions()),
}))
"""
    result = subprocess.run(
        [sys.executable, "-B", "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        text=True,
        capture_output=True,
        check=True,
    )
    observed = json.loads(result.stdout)
    assert observed["root"] == ["odibi"]
    assert set(observed["planning"]) == {"odibi", "odibi.planning"}
    assert observed["context"] == "Context"
    assert observed["transform"] == "transform"
    assert observed["validation"] is True
    assert observed["registered"] > 40


def test_mixed_barrier_concurrency_equals_serial_goldens_and_preserves_state() -> None:
    """Sixteen workers interleave 200 calls without shared state or output drift."""
    cases = (PLANNED_YAML, UNRESOLVED_YAML, INVALID_YAML, LIMIT_YAML)
    goldens = {case: plan_pipeline_yaml(case).to_json() for case in cases}
    barrier = threading.Barrier(16)

    def plan(index: int) -> str:
        barrier.wait()
        case = cases[index % len(cases)]
        return plan_pipeline_yaml(case).to_json()

    before = _state_snapshot()
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(plan, range(208)))
    after = _state_snapshot()

    assert results == [goldens[cases[index % len(cases)]] for index in range(208)]
    assert before == after
