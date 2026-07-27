"""Adversarial route tests for the dedicated immutable planning CLI."""

from __future__ import annotations

import argparse
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from odibi.cli import main
import odibi.cli.plan as cli_plan
from odibi.cli.plan import INTERNAL_SERIALIZATION_FAILURE_JSON, plan_command
from odibi.planning import (
    DEFAULT_PLANNING_LIMITS,
    DiagnosticSubject,
    PlanningDiagnostic,
    PlanningResponse,
    plan_pipeline_bytes,
)


PLANNED_YAML = b"""\
project: demo
pipelines:
  - pipeline: ingest
    nodes:
      - name: source
        read: {}
      - name: sink
        depends_on: [source]
        write: {}
"""
UNRESOLVED_YAML = b"""\
project: demo
connections:
  external:
    type: secret-provider-canary
pipelines:
  - pipeline: ingest
    nodes:
      - name: source
        read:
          connection: external
          path: /physical/path/canary.csv
"""
INVALID_YAML = b"project: ["


class BinaryStdin:
    """Minimal text-stream facade exposing caller-owned binary stdin."""

    def __init__(self, value: bytes) -> None:
        self.buffer = BytesIO(value)


class FailingStdout:
    """Output stream that proves write failures are not retried or reflected."""

    def __init__(self) -> None:
        self.calls = 0

    def write(self, value: str) -> int:
        self.calls += 1
        raise OSError("stdout-exception-canary")


def _invoke(monkeypatch: pytest.MonkeyPatch, payload: bytes) -> tuple[int, str, BinaryStdin]:
    """Invoke the direct command with isolated caller-owned in-memory streams."""
    stdin = BinaryStdin(payload)
    stdout = StringIO()
    original_stdin = sys.stdin
    original_stdout = sys.stdout
    try:
        # Assign directly so the adversarial import tripwire encloses only the
        # production adapter, not pytest's monkeypatch implementation.
        sys.stdin = stdin
        sys.stdout = stdout
        exit_code = plan_command(argparse.Namespace(stdin=True, format="json"))
    finally:
        sys.stdin = original_stdin
        sys.stdout = original_stdout
    return exit_code, stdout.getvalue(), stdin


@pytest.mark.parametrize(
    ("payload", "expected_exit", "expected_status"),
    (
        pytest.param(PLANNED_YAML, 0, "planned", id="planned"),
        pytest.param(INVALID_YAML, 2, "invalid", id="invalid-yaml"),
        pytest.param(UNRESOLVED_YAML, 3, "unresolved", id="unresolved"),
        pytest.param(b"\xff", 2, "invalid", id="invalid-utf8"),
        pytest.param(
            b"x" * (DEFAULT_PLANNING_LIMITS.max_input_bytes + 1),
            2,
            "invalid",
            id="input-limit-exceeded",
        ),
    ),
)
def test_direct_command_matches_package_response_and_preserves_streams(
    monkeypatch: pytest.MonkeyPatch,
    immutable_planning_tripwires,
    payload: bytes,
    expected_exit: int,
    expected_status: str,
) -> None:
    """All normal status/limit/UTF-8 paths emit the exact package response once."""
    with immutable_planning_tripwires() as attempts:
        exit_code, output, stdin = _invoke(monkeypatch, payload)

    expected = plan_pipeline_bytes(payload).to_dict()
    assert exit_code == expected_exit
    assert output == plan_pipeline_bytes(payload).to_json() + "\n"
    assert json.loads(output) == expected
    assert expected["status"] == expected_status
    assert stdin.buffer.closed is False
    assert sys.stdout.closed is False
    assert attempts == []


def test_direct_adapter_repeats_canonical_output_without_state_growth(
    monkeypatch: pytest.MonkeyPatch,
    immutable_planning_tripwires,
) -> None:
    """Twenty-five direct calls remain byte-identical under shared effect tripwires."""
    expected = plan_pipeline_bytes(PLANNED_YAML).to_json() + "\n"

    with immutable_planning_tripwires() as attempts:
        observed = [_invoke(monkeypatch, PLANNED_YAML)[:2] for _ in range(25)]

    assert observed == [(0, expected)] * 25
    assert attempts == []


def test_internal_response_exits_four_without_echo(monkeypatch: pytest.MonkeyPatch) -> None:
    """A fixed internal-category planner response uses the dedicated internal exit."""
    response = PlanningResponse(
        schema_version="1.0",
        status="invalid",
        plan=None,
        diagnostics=(
            PlanningDiagnostic(
                code="INTERNAL_PLANNING_FAILURE",
                severity="error",
                category="internal",
                subject=DiagnosticSubject("document", None),
                message="Immutable planning could not be completed.",
            ),
        ),
        truncated=False,
    )
    monkeypatch.setattr(cli_plan, "plan_pipeline_bytes", lambda raw: response)

    exit_code, output, _ = _invoke(monkeypatch, PLANNED_YAML)

    assert exit_code == 4
    assert output == response.to_json() + "\n"


def test_serialization_failure_uses_precompiled_constant_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Serialization failure never tries to construct or serialize a second DTO."""
    calls = 0

    def fail() -> str:
        nonlocal calls
        calls += 1
        raise RuntimeError("serialization-exception-canary")

    response = SimpleNamespace(status="planned", diagnostics=(), to_json=fail)
    monkeypatch.setattr(cli_plan, "plan_pipeline_bytes", lambda raw: response)

    exit_code, output, _ = _invoke(monkeypatch, PLANNED_YAML)

    assert exit_code == 4
    assert calls == 1
    assert output == INTERNAL_SERIALIZATION_FAILURE_JSON + "\n"
    assert "exception-canary" not in output


def test_stdout_failure_exits_four_without_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """A caller stream error is neither retried nor included in another response."""
    stdin = BinaryStdin(PLANNED_YAML)
    stdout = FailingStdout()
    monkeypatch.setattr(sys, "stdin", stdin)
    monkeypatch.setattr(sys, "stdout", stdout)

    assert plan_command(argparse.Namespace(stdin=True, format="json")) == 4
    assert stdout.calls == 1
    assert stdin.buffer.closed is False


@pytest.mark.parametrize(
    "argv",
    (
        ["odibi", "plan"],
        ["odibi", "plan", "--stdin"],
        ["odibi", "plan", "--format", "json"],
        ["odibi", "plan", "config.yaml", "--stdin", "--format", "json"],
        ["odibi", "plan", "--stdin", "--format", "yaml"],
        ["odibi", "plan", "--stdin", "--format", "json", "--env", "dev"],
    ),
)
def test_selector_rejects_implicit_or_runtime_inputs_with_argparse_exit_two(
    monkeypatch: pytest.MonkeyPatch, argv: list[str]
) -> None:
    """The selector accepts no path, environment, implicit stdin, or non-JSON format."""
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2


def test_main_selects_planner_before_runtime_command_imports(
    monkeypatch: pytest.MonkeyPatch,
    immutable_planning_tripwires,
) -> None:
    """The plan branch reaches the primitive without importing runtime command modules."""
    forbidden = (
        "odibi.cli.run",
        "odibi.pipeline",
        "odibi.config",
        "odibi.context",
        "odibi.registry",
        "odibi.connections",
        "odibi.catalog",
        "odibi.story",
        "odibi.utils.extensions",
    )
    before = {name for name in sys.modules if name.startswith(forbidden)}
    monkeypatch.setattr(sys, "argv", ["odibi", "plan", "--stdin", "--format", "json"])
    stdin = BinaryStdin(PLANNED_YAML)
    stdout = StringIO()
    monkeypatch.setattr(sys, "stdin", stdin)
    monkeypatch.setattr(sys, "stdout", stdout)

    with immutable_planning_tripwires() as attempts:
        assert main() == 0

    after = {name for name in sys.modules if name.startswith(forbidden)}
    assert after == before
    assert json.loads(stdout.getvalue()) == plan_pipeline_bytes(PLANNED_YAML).to_dict()
    assert attempts == []


def test_python_module_entry_is_runtime_free_and_canonical() -> None:
    """The external source command imports no forbidden runtime facilities."""
    script = (
        r"""
import builtins
from io import BytesIO, StringIO
import json
import runpy
import sys

forbidden = (
    "odibi.cli.run", "odibi.pipeline", "odibi.config", "odibi.context",
    "odibi.registry", "odibi.connections", "odibi.catalog", "odibi.story",
    "odibi.utils", "odibi_mcp", "pandas", "pyspark", "dotenv",
)
seen = []
original_import = builtins.__import__
def guarded_import(name, *args, **kwargs):
    if name.startswith(forbidden):
        seen.append(name)
    return original_import(name, *args, **kwargs)
builtins.__import__ = guarded_import
class Input:
    buffer = BytesIO("""
        + repr(PLANNED_YAML)
        + r""")
sys.argv = ["odibi", "plan", "--stdin", "--format", "json"]
sys.stdin = Input()
capture = StringIO()
sys.stdout = capture
try:
    runpy.run_module("odibi", run_name="__main__")
except SystemExit as error:
    assert error.code == 0
assert seen == [], seen
assert not any(name.startswith(forbidden) for name in sys.modules)
value = json.loads(capture.getvalue())
assert value["schema_version"] == "1.0" and value["status"] == "planned"
"""
    )
    result = subprocess.run(
        [sys.executable, "-B", "-c", script],
        cwd=Path(__file__).resolve().parents[3],
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


def test_external_python_module_emits_exact_one_line_without_writes(tmp_path: Path) -> None:
    """The explicit external route leaves a read-only ambient directory untouched."""
    ambient = tmp_path / "ambient"
    ambient.mkdir()
    ambient.chmod(0o555)
    before = tuple(ambient.iterdir())
    env = {
        **os.environ,
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": str(Path(__file__).resolve().parents[3]),
    }

    result = subprocess.run(
        [sys.executable, "-B", "-m", "odibi", "plan", "--stdin", "--format", "json"],
        cwd=ambient,
        env=env,
        input=PLANNED_YAML,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout == plan_pipeline_bytes(PLANNED_YAML).to_json().encode() + b"\n"
    assert result.stderr == b""
    assert tuple(ambient.iterdir()) == before


@pytest.mark.parametrize(
    ("payload", "expected_exit"),
    (
        pytest.param(PLANNED_YAML, 0, id="planned"),
        pytest.param(INVALID_YAML, 2, id="invalid-yaml"),
        pytest.param(UNRESOLVED_YAML, 3, id="unresolved"),
        pytest.param(
            b"x" * (DEFAULT_PLANNING_LIMITS.max_input_bytes + 1),
            2,
            id="input-limit-exceeded",
        ),
    ),
)
def test_installed_console_script_matches_package_in_read_only_ambient_root(
    tmp_path: Path, payload: bytes, expected_exit: int
) -> None:
    """The installed entry point preserves the exact planner matrix without source-path help."""
    console_script = Path(sys.executable).with_name("odibi")
    assert console_script.is_file(), "install the project before running the installed-route suite"
    ambient = tmp_path / "installed-ambient"
    ambient.mkdir()
    ambient.chmod(0o555)
    before = tuple(ambient.iterdir())
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("DATABRICKS_", "AZURE_", "AWS_", "GOOGLE_", "OTEL_"))
    }
    env.pop("PYTHONPATH", None)
    env.pop("PYTEST_CURRENT_TEST", None)
    env["PYTHONDONTWRITEBYTECODE"] = "1"

    result = subprocess.run(
        [str(console_script), "plan", "--stdin", "--format", "json"],
        cwd=ambient,
        env=env,
        input=payload,
        capture_output=True,
        check=False,
    )

    expected = plan_pipeline_bytes(payload)
    assert result.returncode == expected_exit
    assert result.stdout == expected.to_json().encode() + b"\n"
    assert result.stderr == b""
    assert tuple(ambient.iterdir()) == before


def test_external_route_ignores_project_extension_and_entry_point_canaries(tmp_path: Path) -> None:
    """Planning neither executes ambient project code nor enumerates installed entry points."""
    ambient = tmp_path / "ambient-canaries"
    ambient.mkdir()
    for name in ("transforms.py", "plugins.py", "entry_point_canary.py"):
        (ambient / name).write_text(
            "raise AssertionError('extension-entry-point-canary-e933')\n",
            encoding="utf-8",
        )
    before = {path.name: path.read_bytes() for path in ambient.iterdir()}
    script = (
        r"""
import importlib.metadata
from io import BytesIO, StringIO
import json
import sys
from odibi.cli.main import main

def forbidden_entry_points(*args, **kwargs):
    raise AssertionError("entry-point-enumeration-canary-e933")

def forbidden_load(*args, **kwargs):
    raise AssertionError("entry-point-load-canary-e933")

importlib.metadata.entry_points = forbidden_entry_points
importlib.metadata.EntryPoint.load = forbidden_load
class Input:
    buffer = BytesIO(%r)
sys.argv = ["odibi", "plan", "--stdin", "--format", "json"]
sys.stdin = Input()
capture = StringIO()
sys.stdout = capture
code = main()
assert code == 0
value = json.loads(capture.getvalue())
assert value["schema_version"] == "1.0" and value["status"] == "planned"
"""
        % PLANNED_YAML
    )
    result = subprocess.run(
        [sys.executable, "-B", "-c", script],
        cwd=ambient,
        env={
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(Path(__file__).resolve().parents[3]),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert {path.name: path.read_bytes() for path in ambient.iterdir()} == before
