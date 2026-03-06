"""Pipeline YAML validation."""

from typing import Any, Dict, List

import yaml

from odibi.config import PipelineConfig
from odibi.patterns import _PATTERNS
from odibi.registry import FunctionRegistry


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
        config = yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        return {
            "valid": False,
            "errors": [
                {
                    "code": "YAML_PARSE_ERROR",
                    "field_path": "root",
                    "message": str(e),
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
    """Validate project.yaml config."""
    required_keys = ["project", "connections"]
    for key in required_keys:
        if key not in config:
            errors.append(
                {
                    "code": "MISSING_KEY",
                    "field_path": "root",
                    "message": f"Missing required key: '{key}'",
                    "fix": f"Add '{key}:' to project.yaml",
                }
            )

    recommended_keys = ["story", "system"]
    for key in recommended_keys:
        if key not in config:
            warnings.append(
                {
                    "code": "MISSING_RECOMMENDED",
                    "message": f"Missing recommended key: '{key}'",
                }
            )

    connections = config.get("connections", {})
    if not isinstance(connections, dict):
        errors.append(
            {
                "code": "INVALID_CONNECTIONS",
                "field_path": "connections",
                "message": "'connections' must be a dictionary",
                "fix": "Format as 'connections: {name: {type: ...}}'",
            }
        )
    else:
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

    pipelines = config.get("pipelines", [])
    if pipelines:
        _validate_pipelines_list(pipelines, errors, warnings, "pipelines")


def _validate_pipeline_file(
    config: Dict[str, Any],
    errors: List[Dict[str, Any]],
    warnings: List[Dict[str, Any]],
) -> None:
    """Validate imported pipeline file."""
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
        _validate_transformer_params(node, pipeline_idx, node_idx, errors)


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
        if wrong_key in node_dict:
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
) -> None:
    """Validate transformer parameters."""
    if node.transform and node.transform.steps:
        for step_idx, step in enumerate(node.transform.steps):
            if hasattr(step, "function") and step.function:
                if not FunctionRegistry.has_function(step.function):
                    errors.append(
                        {
                            "code": "UNKNOWN_TRANSFORMER",
                            "field_path": f"pipelines[{pipeline_idx}].nodes[{node_idx}].transform.steps[{step_idx}].function",
                            "message": f"Transformer '{step.function}' not found",
                            "fix": "Use 'odibi list transformers' to see available transformers",
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
        if not FunctionRegistry.has_function(node.transformer):
            errors.append(
                {
                    "code": "UNKNOWN_TRANSFORMER",
                    "field_path": f"pipelines[{pipeline_idx}].nodes[{node_idx}].transformer",
                    "message": f"Transformer '{node.transformer}' not found",
                    "fix": "Use 'odibi list transformers' or 'odibi list patterns'",
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
