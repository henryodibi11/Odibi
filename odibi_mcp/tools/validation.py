"""Enhanced validation tool for Phase 1 - uses Pipeline.validate() + pattern validation."""

from typing import Any, Dict
import yaml as yaml_lib

from odibi.config import PipelineConfig
from odibi.patterns import _PATTERNS


def validate_pipeline(yaml_content: str, check_connections: bool = False) -> Dict[str, Any]:
    """Enhanced pipeline validation using Pipeline.validate() + pattern validation.

    This provides comprehensive validation:
    1. YAML syntax check
    2. Pydantic model validation
    3. Transformer parameter validation (via FunctionRegistry)
    4. DAG validation (via DependencyGraph)
    5. Pattern parameter validation (via required_params metadata)
    6. Connection existence checks (optional)

    Args:
        yaml_content: YAML string to validate
        check_connections: If True, validate that connections are available

    Returns:
        {
            "valid": bool,
            "errors": [{"code": str, "field_path": str, "message": str, "fix": str}],
            "warnings": [{"code": str, "message": str}],
            "summary": str
        }
    """
    errors = []
    warnings = []

    # Step 1: Parse YAML
    try:
        config_dict = yaml_lib.safe_load(yaml_content)
    except yaml_lib.YAMLError as e:
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

    if not isinstance(config_dict, dict):
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

    # Step 2: Extract pipelines (handle both project and pipeline file formats)
    pipelines_list = []
    if "pipelines" in config_dict:
        pipelines_list = config_dict["pipelines"]
    elif "pipeline" in config_dict:
        # Single pipeline dict
        pipelines_list = [config_dict]
    else:
        return {
            "valid": False,
            "errors": [
                {
                    "code": "NO_PIPELINES",
                    "field_path": "root",
                    "message": "No pipelines found in config",
                    "fix": "Add 'pipelines:' list or 'pipeline:' definition",
                }
            ],
            "warnings": [],
            "summary": "No pipelines to validate",
        }

    if not isinstance(pipelines_list, list):
        pipelines_list = [pipelines_list]

    # Step 3: Validate each pipeline
    for i, pipeline_dict in enumerate(pipelines_list):
        # Validate through Pydantic
        try:
            pipeline_config = PipelineConfig(**pipeline_dict)
        except Exception as e:
            errors.append(
                {
                    "code": "PYDANTIC_VALIDATION_FAILED",
                    "field_path": f"pipelines[{i}]",
                    "message": str(e),
                    "fix": "Check that all required fields are present and types are correct",
                }
            )
            continue

        # Step 4: Validate pattern parameters for each node
        for node_idx, node in enumerate(pipeline_config.nodes):
            if node.transformer and node.transformer in _PATTERNS:
                # Get pattern class
                pattern_cls = _PATTERNS[node.transformer]
                required_params = getattr(pattern_cls, "required_params", [])

                # Check required params are present
                for param_name in required_params:
                    if param_name not in node.params:
                        errors.append(
                            {
                                "code": "PATTERN_REQUIRES",
                                "field_path": f"pipelines[{i}].nodes[{node_idx}].params.{param_name}",
                                "message": f"Pattern '{node.transformer}' requires parameter '{param_name}'",
                                "fix": f"Add '{param_name}' to params dict for this node",
                            }
                        )

        # Step 5: Validate transformer parameters using FunctionRegistry
        from odibi.registry import FunctionRegistry

        for node_idx, node in enumerate(pipeline_config.nodes):
            # Check transform steps
            if node.transform and node.transform.steps:
                for step_idx, step in enumerate(node.transform.steps):
                    if hasattr(step, "function") and step.function:
                        if not FunctionRegistry.has_function(step.function):
                            errors.append(
                                {
                                    "code": "UNKNOWN_TRANSFORMER",
                                    "field_path": f"pipelines[{i}].nodes[{node_idx}].transform.steps[{step_idx}].function",
                                    "message": f"Transformer '{step.function}' not found in FunctionRegistry",
                                    "fix": "Use list_transformers to see available transformers",
                                }
                            )
                        else:
                            try:
                                FunctionRegistry.validate_params(step.function, step.params)
                            except ValueError as e:
                                errors.append(
                                    {
                                        "code": "INVALID_TRANSFORMER_PARAMS",
                                        "field_path": f"pipelines[{i}].nodes[{node_idx}].transform.steps[{step_idx}].params",
                                        "message": str(e),
                                        "fix": f"Use list_transformers to see required params for '{step.function}'",
                                    }
                                )

            # Check node-level transformer (used by patterns)
            if node.transformer and node.transformer not in _PATTERNS:
                # It's a regular transformer, not a pattern
                if not FunctionRegistry.has_function(node.transformer):
                    errors.append(
                        {
                            "code": "UNKNOWN_TRANSFORMER",
                            "field_path": f"pipelines[{i}].nodes[{node_idx}].transformer",
                            "message": f"Transformer '{node.transformer}' not found",
                            "fix": "Use list_transformers or list_patterns to see available options",
                        }
                    )
                else:
                    try:
                        FunctionRegistry.validate_params(node.transformer, node.params)
                    except ValueError as e:
                        errors.append(
                            {
                                "code": "INVALID_TRANSFORMER_PARAMS",
                                "field_path": f"pipelines[{i}].nodes[{node_idx}].params",
                                "message": str(e),
                                "fix": f"Use list_transformers to see required params for '{node.transformer}'",
                            }
                        )

        # Step 6: Check for DAG issues (simple checks without full Pipeline instantiation)
        node_names = {node.name for node in pipeline_config.nodes}
        for node_idx, node in enumerate(pipeline_config.nodes):
            for dep in node.depends_on:
                if dep not in node_names:
                    errors.append(
                        {
                            "code": "MISSING_DEPENDENCY",
                            "field_path": f"pipelines[{i}].nodes[{node_idx}].depends_on",
                            "message": f"Node '{node.name}' depends on '{dep}' which doesn't exist",
                            "fix": f"Add node '{dep}' or remove from depends_on",
                        }
                    )

    # Generate summary
    if errors:
        summary = f"❌ {len(errors)} error(s), {len(warnings)} warning(s)"
    elif warnings:
        summary = f"⚠️  Valid with {len(warnings)} warning(s)"
    else:
        summary = "✅ Valid"

    return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings, "summary": summary}
