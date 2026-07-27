"""Offline contract tests for path-aware validation and runtime loading convergence."""

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from odibi.config import load_config_from_file
from odibi.registry import FunctionRegistry
from odibi.validate import validate_config_file, validate_yaml


HEADER = """project: demo
connections:
  local:
    type: local
    base_path: ${ROOT_PATH}
story:
  connection: local
  path: stories
system:
  connection: local
  path: system
"""


def _project(tmp_path: Path, body: str = "pipelines: []\n") -> Path:
    path = tmp_path / "project.yaml"
    path.write_text(HEADER + body)
    return path


def test_injected_environment_recurses_without_mutating_process(tmp_path, monkeypatch):
    monkeypatch.setenv("ROOT_PATH", "process")
    before = dict(os.environ)
    child = tmp_path / "child.yaml"
    child.write_text("pipelines: []\nvars:\n  nested: ${ROOT_PATH}\n")
    project = _project(tmp_path, "imports: child.yaml\n")
    loaded = load_config_from_file(project, environment={"ROOT_PATH": "injected"})
    assert loaded.connections["local"].base_path == "injected"
    assert dict(os.environ) == before


def test_scalar_list_and_nested_imports_keep_source_anchor(tmp_path):
    nested = tmp_path / "nested"
    nested.mkdir()
    leaf = nested / "leaf.yaml"
    leaf.write_text(
        "pipelines:\n  - pipeline: imported\n    nodes:\n"
        "      - name: query\n        read:\n          connection: local\n"
        "          format: sql\n          sql_file: query.sql\n"
    )
    child = tmp_path / "child.yaml"
    child.write_text("imports: nested/leaf.yaml\n")
    scalar = _project(tmp_path, "imports: child.yaml\n")
    model = load_config_from_file(scalar, environment={"ROOT_PATH": str(tmp_path)})
    assert model.pipelines[0].nodes[0].source_yaml == str(leaf.resolve())
    scalar.write_text(HEADER + "imports: [child.yaml]\n")
    (tmp_path / ".env").write_text(f"ROOT_PATH={tmp_path}\n")
    assert validate_config_file(scalar)["valid"]


def test_dotenv_override_overlay_and_process_unchanged(tmp_path, monkeypatch):
    monkeypatch.setenv("ROOT_PATH", "process-secret")
    before = dict(os.environ)
    (tmp_path / ".env").write_text("ROOT_PATH=dotenv-value\n")
    project = _project(tmp_path)
    (tmp_path / "env.prod.yaml").write_text("project: prod_demo\n")
    result = validate_config_file(project, env="prod")
    assert result["valid"]
    assert (
        load_config_from_file(project, env="prod", environment={"ROOT_PATH": "x"}).project
        == "prod_demo"
    )
    assert dict(os.environ) == before


def test_validation_does_not_execute_project_python(tmp_path):
    (tmp_path / "transforms.py").write_text("raise AssertionError('must not execute')\n")
    project = _project(tmp_path)
    (tmp_path / ".env").write_text(f"ROOT_PATH={tmp_path}\n")
    assert validate_config_file(project)["valid"]


def test_content_import_requires_path_and_custom_transform_warns(tmp_path):
    assert validate_yaml("imports: child.yaml\n")["errors"][0]["code"] == "IMPORT_PATH_REQUIRED"
    body = """pipelines:
  - pipeline: custom
    nodes:
      - name: custom_node
        transformer: project_custom_transform
        params: {answer: 42}
"""
    project = _project(tmp_path, body)
    (tmp_path / ".env").write_text(f"ROOT_PATH={tmp_path}\n")
    result = validate_config_file(project)
    assert result["valid"]
    assert result["warnings"][0]["code"] == "TRANSFORMER_NOT_VERIFIED"


def test_structured_failures_are_redacted(tmp_path):
    missing = validate_config_file(tmp_path / "absent.yaml")
    assert missing["errors"][0]["code"] == "CONFIG_FILE_NOT_FOUND"
    project = _project(tmp_path)
    sentinel = "SECRET_SENTINEL_7f31"
    project.write_text(HEADER.replace("${ROOT_PATH}", sentinel + ": [") + "pipelines: []\n")
    malformed = validate_config_file(project)
    rendered = json.dumps(malformed)
    assert malformed["errors"][0]["code"] == "YAML_PARSE_ERROR"
    assert sentinel not in rendered


def test_substituted_parse_failure_is_absent_from_logs_and_result(tmp_path):
    sentinel = "SECRET_SENTINEL_parse_91af"
    project = _project(tmp_path)
    (tmp_path / ".env").write_text(f"ROOT_PATH={sentinel}: [\n")
    with patch("odibi.utils.config_loader.logger") as logger:
        result = validate_config_file(project)
    assert result["errors"][0]["code"] == "YAML_PARSE_ERROR"
    assert sentinel not in json.dumps(result)
    assert sentinel not in repr(logger.method_calls)


def test_missing_direct_and_nested_imports_are_import_errors(tmp_path):
    direct = _project(tmp_path, "imports: absent.yaml\n")
    (tmp_path / ".env").write_text(f"ROOT_PATH={tmp_path}\n")
    assert validate_config_file(direct)["errors"][0]["code"] == "IMPORT_LOAD_ERROR"

    child = tmp_path / "child.yaml"
    child.write_text("imports: nested-absent.yaml\n")
    direct.write_text(HEADER + "imports: child.yaml\n")
    assert validate_config_file(direct)["errors"][0]["code"] == "IMPORT_LOAD_ERROR"


def test_registered_project_transform_remains_unverified(tmp_path):
    def project_transform(context, current, answer):
        return current

    FunctionRegistry.register(project_transform, name="already_registered_project_transform")
    project = _project(
        tmp_path,
        """pipelines:
  - pipeline: custom
    nodes:
      - name: custom_node
        transformer: already_registered_project_transform
        params: {answer: 42}
""",
    )
    (tmp_path / ".env").write_text(f"ROOT_PATH={tmp_path}\n")
    result = validate_config_file(project)
    assert result["valid"]
    assert result["warnings"][0]["code"] == "TRANSFORMER_NOT_VERIFIED"


def test_project_override_of_shipped_name_remains_unverified(tmp_path, monkeypatch):
    import odibi.transformers as transformers

    monkeypatch.setattr(FunctionRegistry, "_functions", {})
    monkeypatch.setattr(FunctionRegistry, "_signatures", {})
    monkeypatch.setattr(FunctionRegistry, "_param_models", {})
    monkeypatch.setattr(transformers, "_standard_library_registered", False)

    def project_filter_rows(context, current, project_value):
        return current

    FunctionRegistry.register(project_filter_rows, name="filter_rows")
    project = _project(
        tmp_path,
        """pipelines:
  - pipeline: custom
    nodes:
      - name: custom_node
        transformer: filter_rows
        params: {project_value: 42}
""",
    )
    (tmp_path / ".env").write_text(f"ROOT_PATH={tmp_path}\n")
    result = validate_config_file(project)
    assert result["valid"]
    assert result["warnings"][0]["code"] == "TRANSFORMER_NOT_VERIFIED"
    assert FunctionRegistry.get_function("filter_rows") is project_filter_rows


def test_semantic_exception_is_bounded_and_redacted(tmp_path):
    sentinel = "SECRET_SENTINEL_semantic_48b2"
    project = _project(tmp_path)
    (tmp_path / ".env").write_text(f"ROOT_PATH={tmp_path}\n")
    with patch(
        "odibi.validate.pipeline._validate_loaded_project", side_effect=RuntimeError(sentinel)
    ):
        result = validate_config_file(project)
    assert result["errors"][0]["code"] == "VALIDATION_INTERNAL_ERROR"
    assert sentinel not in json.dumps(result)


def test_missing_environment_exposes_name_not_other_values(tmp_path):
    project = _project(tmp_path)
    result = validate_config_file(project)
    assert result["errors"][0]["code"] == "MISSING_ENVIRONMENT_VARIABLE"
    assert "ROOT_PATH" in json.dumps(result)


def test_runtime_uses_shared_authority_before_connections(tmp_path):
    from odibi.pipeline import PipelineManager

    project = _project(tmp_path)
    (tmp_path / ".env").write_text(f"ROOT_PATH={tmp_path}\n")
    with (
        patch.object(PipelineManager, "_build_connections", side_effect=RuntimeError("boundary")),
        pytest.raises(RuntimeError, match="boundary"),
    ):
        PipelineManager.from_yaml(str(project))
