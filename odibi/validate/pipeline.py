"""Pipeline YAML validation.

Single source of structural truth: project configs are validated by constructing
the *runtime* model (`ProjectConfig`) — the exact model `odibi run` builds — so
"valid" means "will run". Semantic checks (transformer registry, dependency graph,
wrong-key detection, pattern params) are layered on top. The CLI (`odibi validate`)
and the MCP `validate_yaml` tool both route here, so they can never diverge.
"""

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from dotenv import dotenv_values

from odibi.config import PipelineConfig, ProjectConfig, load_config_from_file
from odibi.patterns import _PATTERNS
from odibi.registry import FunctionRegistry


def _result(errors: List[Dict[str, Any]], warnings=None) -> Dict[str, Any]:
    warnings = warnings or []
    if errors:
        summary = f"{len(errors)} error(s), {len(warnings)} warning(s)"
    elif warnings:
        summary = f"Valid with {len(warnings)} warning(s)"
    else:
        summary = "Valid"
    return {"valid": not errors, "errors": errors, "warnings": warnings, "summary": summary}


def _build_validation_environment(config_path: Path) -> Dict[str, str]:
    """Build runtime-compatible root environment without process mutation."""
    environment = dict(os.environ)
    dotenv_path = config_path.parent / ".env"
    if dotenv_path.is_file():
        for name, value in dotenv_values(dotenv_path).items():
            if value is not None:
                environment[name] = value
    return environment


def _load_failure_result(exc: Exception, config_path: Path) -> Dict[str, Any]:
    """Convert expected loader failures to value-redacted public errors."""
    chain = []
    current: Optional[BaseException] = exc
    while current is not None and current not in chain:
        chain.append(current)
        current = current.__cause__ or current.__context__
    safe_text = "\n".join(str(item) for item in chain)
    error = {
        "field_path": "root",
        "source_path": str(config_path),
        "fix": "Check the configuration file and try again.",
    }
    missing_env = re.search(r"Missing environment variable: ([A-Za-z_][A-Za-z0-9_]*)", safe_text)
    recipe = re.search(r"recipe ['\"]?([A-Za-z_][A-Za-z0-9_-]*)", safe_text, re.IGNORECASE)
    yaml_error = next((item for item in chain if isinstance(item, yaml.YAMLError)), None)
    if isinstance(exc, FileNotFoundError) and not config_path.exists():
        error.update(code="CONFIG_FILE_NOT_FOUND", message="Configuration file was not found.")
    elif missing_env:
        error.update(
            code="MISSING_ENVIRONMENT_VARIABLE",
            field_path=missing_env.group(1),
            message=f"Required environment variable '{missing_env.group(1)}' is not set.",
            fix="Set the named variable in the process environment or sibling .env file.",
        )
    elif yaml_error is not None or "YAML parsing failed" in safe_text:
        error.update(code="YAML_PARSE_ERROR", message="Configuration contains invalid YAML syntax.")
        mark = getattr(yaml_error, "problem_mark", None)
        if mark is not None:
            error.update(line=mark.line + 1, column=mark.column + 1)
    elif "import" in safe_text.lower() and any(
        isinstance(item, FileNotFoundError) for item in chain
    ):
        error.update(
            code="IMPORT_LOAD_ERROR", message="An imported configuration file could not be loaded."
        )
    elif recipe or "recipe" in safe_text.lower():
        error.update(code="RECIPE_ERROR", field_path="recipes", message="Recipe expansion failed.")
        if recipe:
            error["recipe"] = recipe.group(1)
    elif "Configuration validation failed" in safe_text or any(
        callable(getattr(item, "errors", None)) for item in chain
    ):
        error.update(
            code="MODEL_VALIDATION_FAILED",
            message="Configuration does not match the project model.",
            fix="Check required fields and field types.",
        )
        location = re.search(r"(?:^|\n)\s*[^\n]+ - ([A-Za-z0-9_.]+):", safe_text)
        if location:
            error["field_path"] = location.group(1)
    else:
        error.update(code="VALIDATION_INTERNAL_ERROR", message="Validation failed unexpectedly.")
    return _result([error])


def _safe_semantic_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """Remove loaded configuration values from file-validation diagnostics."""
    messages = {
        "NO_NODES": "A pipeline has no nodes.",
        "INVALID_NODE_NAME": "A node name must use alphanumeric characters and underscores.",
        "WRONG_KEY_SOURCE": "A node uses the unsupported 'source' key.",
        "WRONG_KEY_SINK": "A node uses the unsupported 'sink' key.",
        "WRONG_KEY_INPUTS": "A node uses the legacy 'inputs' key.",
        "WRONG_KEY_OUTPUTS": "A node uses the legacy 'outputs' key.",
        "MISSING_DEPENDENCY": "A node dependency does not exist in the pipeline.",
        "PATTERN_REQUIRES": "A required pattern parameter is missing.",
        "TRANSFORMER_NOT_VERIFIED": "A transformer is not in the shipped registry; project transforms are not imported during safe validation.",
        "INVALID_TRANSFORMER_PARAMS": "Transformer parameters are invalid.",
        "PYDANTIC_VALIDATION_FAILED": "Configuration does not match the expected model.",
    }
    for kind in ("errors", "warnings"):
        for diagnostic in result.get(kind, []):
            diagnostic["message"] = messages.get(
                diagnostic.get("code"), "Configuration semantic validation failed."
            )
            diagnostic.pop("fix", None)
    return result


def _register_shipped_transformers_without_overwriting_project() -> None:
    """Register built-ins while preserving existing project-owned names."""
    project_functions = {
        name: function
        for name, function in FunctionRegistry._functions.items()
        if not getattr(function, "__module__", "").startswith("odibi.transformers")
    }
    project_signatures = {
        name: FunctionRegistry._signatures[name]
        for name in project_functions
        if name in FunctionRegistry._signatures
    }
    project_param_models = {
        name: FunctionRegistry._param_models[name]
        for name in project_functions
        if name in FunctionRegistry._param_models
    }

    from odibi.transformers import register_standard_library

    register_standard_library()
    for name, function in project_functions.items():
        FunctionRegistry._functions[name] = function
        if name in project_signatures:
            FunctionRegistry._signatures[name] = project_signatures[name]
        if name in project_param_models:
            FunctionRegistry._param_models[name] = project_param_models[name]
        else:
            FunctionRegistry._param_models.pop(name, None)


def _validate_loaded_project(project_config: ProjectConfig) -> Dict[str, Any]:
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    try:
        _register_shipped_transformers_without_overwriting_project()
    except Exception:
        pass
    for index, pipeline_config in enumerate(project_config.pipelines):
        _validate_pipeline_nodes(pipeline_config, errors, warnings, f"pipelines[{index}]", index)
    return _safe_semantic_result(_result(errors, warnings))


def validate_config_file(path: Union[str, Path], env: str = None) -> Dict[str, Any]:
    """Validate a file through the normalized, pre-runtime model authority."""
    config_path = Path(path)
    try:
        project_config = load_config_from_file(
            str(config_path), env=env, environment=_build_validation_environment(config_path)
        )
    except Exception as exc:
        return _load_failure_result(exc, config_path)
    try:
        return _validate_loaded_project(project_config)
    except Exception:
        return _result(
            [
                {
                    "code": "VALIDATION_INTERNAL_ERROR",
                    "field_path": "root",
                    "source_path": str(config_path),
                    "message": "Validation failed unexpectedly.",
                    "fix": "Check the configuration file and try again.",
                }
            ]
        )


def _pydantic_errors_to_structured(exc: Exception, location: str) -> List[Dict[str, Any]]:
    """Convert a Pydantic ValidationError (or any error) into our structured form.

    Field paths and friendly fixes make the runtime model's errors actionable for
    both humans and AI agents instead of raw Pydantic dumps.
    """
    structured: List[Dict[str, Any]] = []
    raw_errors = getattr(exc, "errors", None)
    if callable(raw_errors):
        try:
            for err in exc.errors():
                loc = ".".join(str(p) for p in err.get("loc", ()))
                field_path = f"{location}.{loc}" if loc else location
                etype = err.get("type", "")
                msg = err.get("msg", str(err))
                if etype == "missing":
                    fix = f"Add the required '{loc or 'field'}' block/field."
                elif etype.endswith("_type"):
                    fix = "Fix the value's type to match the field."
                elif etype == "extra_forbidden":
                    fix = "Remove the unknown key (or fix its spelling)."
                else:
                    fix = "Check this field against the schema."
                structured.append(
                    {
                        "code": "PYDANTIC_VALIDATION_FAILED",
                        "field_path": field_path,
                        "message": msg,
                        "fix": fix,
                    }
                )
        except Exception:  # pragma: no cover - defensive
            pass
    if not structured:
        structured.append(
            {
                "code": "PYDANTIC_VALIDATION_FAILED",
                "field_path": location,
                "message": str(exc),
                "fix": "Check required fields and data types",
            }
        )
    return structured


def format_validation_error(exc: Exception, header: str = "Configuration is invalid") -> str:
    """Render a Pydantic ValidationError as an actionable, URL-free message.

    Used by `odibi run` (and anywhere a raw Pydantic dump would otherwise reach the
    user) so config errors read as "field: problem -> fix" instead of a stack of
    errors.pydantic.dev links.
    """
    items = _pydantic_errors_to_structured(exc, "root")
    n = len(items)
    lines = [f"{header} ({n} error{'s' if n != 1 else ''}):"]
    for it in items:
        lines.append(f"  • {it['field_path']}: {it['message']}")
        if it.get("fix"):
            lines.append(f"      → {it['fix']}")
    return "\n".join(lines)


def validate_yaml(yaml_content: str) -> Dict[str, Any]:
    """Validate pipeline YAML configuration.

    Performs comprehensive validation:
    1. YAML syntax check
    2. Pydantic model validation
    3. Transformer parameter validation
    4. Pattern parameter validation
    5. DAG dependency validation
    6. Common mistake detection (source: vs read:, etc.)

    Args:
        yaml_content: YAML string to validate

    Returns:
        Dictionary with validation results:
        {
            "valid": bool,
            "errors": [{"code": str, "field_path": str, "message": str, "fix": str}],
            "warnings": [{"code": str, "message": str}],
            "summary": str
        }

    Example:
        >>> result = validate_yaml(yaml_string)
        >>> if not result["valid"]:
        ...     for error in result["errors"]:
        ...         print(f"{error['field_path']}: {error['message']}")
    """
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    try:
        _register_shipped_transformers_without_overwriting_project()
    except Exception:  # pragma: no cover - defensive; never block validation on this
        pass

    try:
        config = yaml.safe_load(yaml_content)
    except yaml.YAMLError:
        return {
            "valid": False,
            "errors": [
                {
                    "code": "YAML_PARSE_ERROR",
                    "field_path": "root",
                    "message": "Content contains invalid YAML syntax.",
                    "fix": "Fix YAML syntax errors",
                }
            ],
            "warnings": [],
            "summary": "YAML syntax error",
        }

    if not isinstance(config, dict):
        return {
            "valid": False,
            "errors": [
                {
                    "code": "INVALID_ROOT",
                    "field_path": "root",
                    "message": "Config must be a dictionary",
                    "fix": "Ensure YAML starts with key-value pairs",
                }
            ],
            "warnings": [],
            "summary": "Invalid config structure",
        }

    if config.get("imports"):
        return _result(
            [
                {
                    "code": "IMPORT_PATH_REQUIRED",
                    "field_path": "imports",
                    "message": "Imports require a source path.",
                    "fix": "Call validate_config_file() for file-based configuration.",
                }
            ]
        )

    # Attempt recipe resolution before validation
    from odibi.recipes import resolve_recipes

    try:
        config = resolve_recipes(config)
    except ValueError:
        errors.append(
            {
                "code": "RECIPE_ERROR",
                "field_path": "recipes",
                "message": "Recipe expansion failed.",
                "fix": "Check recipe names and required variables",
            }
        )

    is_project_config = "project" in config or "connections" in config
    is_pipeline_file = "pipelines" in config and "project" not in config

    if is_project_config:
        _validate_project_config(config, errors, warnings)
    elif is_pipeline_file:
        _validate_pipeline_file(config, errors, warnings)
    else:
        errors.append(
            {
                "code": "UNKNOWN_CONFIG_TYPE",
                "field_path": "root",
                "message": "Config must be a project (with 'project:' key) or pipeline file (with 'pipelines:' key)",
                "fix": "Add 'project:' for project.yaml or 'pipelines:' for pipeline file",
            }
        )

    if errors:
        summary = f"{len(errors)} error(s), {len(warnings)} warning(s)"
    elif warnings:
        summary = f"Valid with {len(warnings)} warning(s)"
    else:
        summary = "Valid"

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "summary": summary,
    }


def _validate_project_config(
    config: Dict[str, Any],
    errors: List[Dict[str, Any]],
    warnings: List[Dict[str, Any]],
) -> None:
    """Validate a project config by constructing the runtime model.

    Cheap structural pre-checks (with stable error codes callers/tests rely on)
    run first; if they pass, the full `ProjectConfig` is constructed — the exact
    model `odibi run` builds — so story/system requirements and connection-reference
    existence are enforced identically to runtime. Semantic node checks then run on
    the constructed model.
    """
    # Cheap pre-checks with stable codes (don't even attempt the model if the
    # top-level shape is obviously wrong — keeps messages crisp).
    has_missing_key = False
    required_keys = ["project", "connections"]
    for key in required_keys:
        if key not in config:
            has_missing_key = True
            errors.append(
                {
                    "code": "MISSING_KEY",
                    "field_path": "root",
                    "message": f"Missing required key: '{key}'",
                    "fix": f"Add '{key}:' to project.yaml",
                }
            )

    connections = config.get("connections", {})
    if "connections" in config and not isinstance(connections, dict):
        has_missing_key = True
        errors.append(
            {
                "code": "INVALID_CONNECTIONS",
                "field_path": "connections",
                "message": "'connections' must be a dictionary",
                "fix": "Format as 'connections: {name: {type: ...}}'",
            }
        )
    elif isinstance(connections, dict):
        for conn_name, conn_config in connections.items():
            if isinstance(conn_config, dict) and "type" not in conn_config:
                errors.append(
                    {
                        "code": "MISSING_CONNECTION_TYPE",
                        "field_path": f"connections.{conn_name}",
                        "message": f"Connection '{conn_name}' missing 'type'",
                        "fix": "Add 'type: local|azure_blob|sql_server|...'",
                    }
                )

    imports = config.get("imports", [])
    if imports and not isinstance(imports, list):
        errors.append(
            {
                "code": "INVALID_IMPORTS",
                "field_path": "imports",
                "message": "'imports' must be a list of file paths",
                "fix": "Format as 'imports: [path1.yaml, path2.yaml]'",
            }
        )

    # If the top-level shape is broken, the model can't construct meaningfully —
    # stop here with the crisp pre-check errors rather than a noisy Pydantic dump.
    if has_missing_key:
        return

    # Pipelines may be absent legitimately: a scaffold skeleton (no pipelines yet)
    # or an imports-based project (pipelines live in imported files we can't resolve
    # from a bare string). Inject an empty list so the rest of the structure still
    # validates, and warn only when there's truly nothing to run.
    config_for_model = config
    if "pipelines" not in config:
        if "imports" not in config:
            warnings.append(
                {
                    "code": "NO_PIPELINES",
                    "message": (
                        "Project defines no 'pipelines:' and no 'imports:'. Add pipelines "
                        "(or imports) before running — a pipeline-less project cannot run."
                    ),
                }
            )
        config_for_model = {**config, "pipelines": []}

    # Keystone: construct the runtime model. This is where validate stops being
    # more lenient than run — story/system are required, connection references are
    # checked, unknown keys (Phase 2) are rejected, all identically to `odibi run`.
    try:
        project_config = ProjectConfig(**config_for_model)
    except Exception as e:
        errors.extend(_pydantic_errors_to_structured(e, "root"))
        return

    # Semantic checks over the validated pipelines.
    for i, pipeline_config in enumerate(project_config.pipelines):
        _validate_pipeline_nodes(pipeline_config, errors, warnings, f"pipelines[{i}]", i)


def _validate_pipeline_file(
    config: Dict[str, Any],
    errors: List[Dict[str, Any]],
    warnings: List[Dict[str, Any]],
) -> None:
    """Validate an imported pipeline fragment (no project/story/system).

    A fragment is valid *as a fragment* but cannot be run directly — it must be
    imported by a project config. We surface that as a warning so a green result
    isn't mistaken for "this file will run".
    """
    warnings.append(
        {
            "code": "PIPELINE_FRAGMENT",
            "message": (
                "This file is a pipeline fragment (no 'project:'/'story:'/'system:'). "
                "It must be imported by a project config; it cannot be run directly."
            ),
        }
    )
    pipelines = config.get("pipelines")
    if pipelines is None:
        errors.append(
            {
                "code": "MISSING_PIPELINES_KEY",
                "field_path": "root",
                "message": "Imported pipeline files must have top-level 'pipelines:' key",
                "fix": "Add 'pipelines:' as the top-level key",
            }
        )
        return

    _validate_pipelines_list(pipelines, errors, warnings, "pipelines")


def _validate_pipelines_list(
    pipelines: Any,
    errors: List[Dict[str, Any]],
    warnings: List[Dict[str, Any]],
    location: str,
) -> None:
    """Validate list of pipeline definitions."""
    if not isinstance(pipelines, list):
        errors.append(
            {
                "code": "INVALID_PIPELINES",
                "field_path": location,
                "message": "'pipelines' must be a list",
                "fix": "Format as 'pipelines: [{pipeline: name, ...}]'",
            }
        )
        return

    for i, pipeline in enumerate(pipelines):
        if not isinstance(pipeline, dict):
            errors.append(
                {
                    "code": "INVALID_PIPELINE",
                    "field_path": f"{location}[{i}]",
                    "message": f"Pipeline at index {i} must be a dictionary",
                    "fix": "Each pipeline must be a YAML object with keys",
                }
            )
            continue

        pipeline_name = pipeline.get("pipeline") or pipeline.get("name")
        if not pipeline_name:
            errors.append(
                {
                    "code": "MISSING_PIPELINE_NAME",
                    "field_path": f"{location}[{i}]",
                    "message": f"Pipeline at index {i} missing 'pipeline:' or 'name:'",
                    "fix": "Add 'pipeline: <name>' or 'name: <name>'",
                }
            )
            pipeline_name = f"pipeline_{i}"

        try:
            pipeline_config = PipelineConfig(**pipeline)
        except Exception as e:
            errors.append(
                {
                    "code": "PYDANTIC_VALIDATION_FAILED",
                    "field_path": f"{location}[{i}]",
                    "message": str(e),
                    "fix": "Check required fields and data types",
                }
            )
            continue

        _validate_pipeline_nodes(pipeline_config, errors, warnings, f"{location}[{i}]", i)


def _validate_pipeline_nodes(
    pipeline_config: PipelineConfig,
    errors: List[Dict[str, Any]],
    warnings: List[Dict[str, Any]],
    location: str,
    pipeline_idx: int,
) -> None:
    """Validate nodes in a pipeline."""
    nodes = pipeline_config.nodes

    if not nodes:
        warnings.append(
            {
                "code": "NO_NODES",
                "message": f"Pipeline '{pipeline_config.pipeline}' has no nodes",
            }
        )
        return

    node_names = {node.name for node in nodes}

    for node_idx, node in enumerate(nodes):
        node_loc = f"{location}.nodes[{node_idx}]"

        _check_node_name(node.name, node_loc, errors)
        _check_wrong_keys(node, node_loc, errors, warnings)
        _check_dependencies(node, node_names, node_loc, errors)
        _validate_pattern_params(node, pipeline_idx, node_idx, errors)
        _validate_transformer_params(node, pipeline_idx, node_idx, errors, warnings)
        _validate_simulation_block(node, node_loc, errors)


def _validate_simulation_block(
    node: Any,
    location: str,
    errors: List[Dict[str, Any]],
) -> None:
    """Validate a node's simulation spec.

    Simulation lives under ``read.options.simulation`` as a raw dict, so it isn't
    checked when ProjectConfig is built. Construct SimulationConfig here so typos
    in the simulation/generator config (e.g. ``noise`` vs ``volatility``) are
    caught at validate time, not at run time.
    """
    read = getattr(node, "read", None)
    if read is None:
        return
    fmt = getattr(read, "format", None)
    if getattr(fmt, "value", fmt) != "simulation":
        return
    options = getattr(read, "options", None) or {}
    sim = options.get("simulation") if isinstance(options, dict) else None
    if not isinstance(sim, dict):
        return
    from odibi.config import SimulationConfig

    try:
        SimulationConfig(**sim)
    except Exception as e:
        errors.extend(_pydantic_errors_to_structured(e, f"{location}.read.options.simulation"))


def _check_node_name(name: str, location: str, errors: List[Dict[str, Any]]) -> None:
    """Check node name format."""
    from odibi.scaffold import sanitize_node_name

    sanitized = sanitize_node_name(name)
    if sanitized != name.lower():
        errors.append(
            {
                "code": "INVALID_NODE_NAME",
                "field_path": location,
                "message": f"Node name '{name}' must be alphanumeric + underscore only",
                "fix": f"Use '{sanitized}' instead",
            }
        )


def _check_wrong_keys(
    node: Any,
    location: str,
    errors: List[Dict[str, Any]],
    warnings: List[Dict[str, Any]],
) -> None:
    """Check for common wrong keys."""
    node_dict = node.model_dump() if hasattr(node, "model_dump") else {}

    wrong_keys = {
        "source": ("Use 'read:' instead of 'source:'", True),
        "sink": ("Use 'write:' instead of 'sink:'", True),
        "inputs": ("Use 'read:' instead of 'inputs:' for SQL/file sources", False),
        "outputs": ("Use 'write:' instead of 'outputs:'", False),
    }

    for wrong_key, (fix_msg, is_error) in wrong_keys.items():
        # Only flag a key the user actually *set* — model_dump() includes legacy
        # fields (e.g. `inputs`) at their None default, which must not false-warn.
        if node_dict.get(wrong_key):
            error_dict = {
                "code": f"WRONG_KEY_{wrong_key.upper()}",
                "field_path": location,
                "message": f"Node '{node.name}' uses '{wrong_key}:'. {fix_msg}",
                "fix": fix_msg,
            }
            if is_error:
                errors.append(error_dict)
            else:
                warnings.append({"code": error_dict["code"], "message": error_dict["message"]})


def _check_dependencies(
    node: Any,
    node_names: set,
    location: str,
    errors: List[Dict[str, Any]],
) -> None:
    """Check dependency existence."""
    for dep in node.depends_on:
        if dep not in node_names:
            errors.append(
                {
                    "code": "MISSING_DEPENDENCY",
                    "field_path": f"{location}.depends_on",
                    "message": f"Node '{node.name}' depends on '{dep}' which doesn't exist",
                    "fix": f"Add node '{dep}' or remove from depends_on",
                }
            )


def _validate_pattern_params(
    node: Any,
    pipeline_idx: int,
    node_idx: int,
    errors: List[Dict[str, Any]],
) -> None:
    """Validate pattern-specific parameters."""
    if not node.transformer or node.transformer not in _PATTERNS:
        return

    pattern_cls = _PATTERNS[node.transformer]
    required_params = getattr(pattern_cls, "required_params", [])

    for param_name in required_params:
        if param_name not in node.params:
            errors.append(
                {
                    "code": "PATTERN_REQUIRES",
                    "field_path": f"pipelines[{pipeline_idx}].nodes[{node_idx}].params.{param_name}",
                    "message": f"Pattern '{node.transformer}' requires parameter '{param_name}'",
                    "fix": f"Add '{param_name}' to params dict",
                }
            )


def _validate_transformer_params(
    node: Any,
    pipeline_idx: int,
    node_idx: int,
    errors: List[Dict[str, Any]],
    warnings: List[Dict[str, Any]],
) -> None:
    """Validate transformer parameters."""

    def is_shipped(name: str) -> bool:
        function = FunctionRegistry.get_function(name)
        module = getattr(function, "__module__", "") if function else ""
        return module == "odibi.transformers" or module.startswith("odibi.transformers.")

    if node.transform and node.transform.steps:
        for step_idx, step in enumerate(node.transform.steps):
            if hasattr(step, "function") and step.function:
                if not is_shipped(step.function):
                    warnings.append(
                        {
                            "code": "TRANSFORMER_NOT_VERIFIED",
                            "field_path": f"pipelines[{pipeline_idx}].nodes[{node_idx}].transform.steps[{step_idx}].function",
                            "message": f"Transformer '{step.function}' is not in the shipped registry; project transforms are not imported during safe validation.",
                        }
                    )
                else:
                    try:
                        FunctionRegistry.validate_params(step.function, step.params)
                    except ValueError as e:
                        errors.append(
                            {
                                "code": "INVALID_TRANSFORMER_PARAMS",
                                "field_path": f"pipelines[{pipeline_idx}].nodes[{node_idx}].transform.steps[{step_idx}].params",
                                "message": str(e),
                                "fix": f"Check required params for '{step.function}'",
                            }
                        )

    if node.transformer and node.transformer not in _PATTERNS:
        if not is_shipped(node.transformer):
            warnings.append(
                {
                    "code": "TRANSFORMER_NOT_VERIFIED",
                    "field_path": f"pipelines[{pipeline_idx}].nodes[{node_idx}].transformer",
                    "message": f"Transformer '{node.transformer}' is not in the shipped registry; project transforms are not imported during safe validation.",
                }
            )
        else:
            try:
                FunctionRegistry.validate_params(node.transformer, node.params)
            except ValueError as e:
                errors.append(
                    {
                        "code": "INVALID_TRANSFORMER_PARAMS",
                        "field_path": f"pipelines[{pipeline_idx}].nodes[{node_idx}].params",
                        "message": str(e),
                        "fix": f"Check required params for '{node.transformer}'",
                    }
                )
