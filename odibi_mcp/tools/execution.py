"""Validation-only and immutable logical planning helpers for MCP callers."""

from __future__ import annotations

from typing import Any, Dict, Literal, overload

import odibi.planning as immutable_planning


class ExecutionError(Exception):
    """Pipeline execution error retained for source compatibility."""

    pass


def _validate_legacy_max_rows(max_rows: int) -> None:
    """Validate the deprecated ignored row-bound compatibility parameter."""
    if type(max_rows) is not int:
        raise TypeError("max_rows must be an integer")
    if not 1 <= max_rows <= 1000:
        raise ValueError("max_rows must be between 1 and 1000")


def _validate_yaml_only(yaml_content: str) -> Dict[str, Any]:
    """Preserve the existing validation-only contract without claiming immutability."""
    import yaml

    from odibi.config import ProjectConfig

    try:
        parsed = yaml.safe_load(yaml_content)
    except yaml.YAMLError as error:
        return {
            "valid": False,
            "errors": [{"code": "YAML_SYNTAX_ERROR", "message": str(error)}],
            "warnings": [],
            "mode": "validate",
        }

    try:
        config = ProjectConfig(**parsed)
    except Exception as error:
        return {
            "valid": False,
            "errors": [
                {
                    "code": "VALIDATION_ERROR",
                    "message": f"Config validation failed: {str(error)}",
                }
            ],
            "warnings": [],
            "mode": "validate",
        }

    return {
        "valid": True,
        "errors": [],
        "warnings": [],
        "mode": "validate",
        "message": (
            f"YAML is valid. Pipeline '{config.pipelines[0].pipeline}' has "
            f"{len(config.pipelines[0].nodes)} nodes."
        ),
    }


@overload
def test_pipeline(
    yaml_content: str,
    *,
    mode: Literal["validate"],
    max_rows: int = 100,
) -> Dict[str, Any]: ...


@overload
def test_pipeline(
    yaml_content: str,
    *,
    mode: Literal["dry-run"] = "dry-run",
    max_rows: int = 100,
) -> dict[str, object]: ...


def test_pipeline(
    yaml_content: str,
    *,
    mode: Literal["validate", "dry-run"] = "dry-run",
    max_rows: int = 100,
) -> Dict[str, Any] | dict[str, object]:
    """Validate supplied YAML or return its shared immutable logical plan.

    ``max_rows`` remains accepted and bounded for one compatibility cycle but
    has no planning semantics and never appears in planning output.
    """
    if mode not in ("validate", "dry-run"):
        raise ValueError("mode must be one of: validate, dry-run")
    _validate_legacy_max_rows(max_rows)
    if mode == "validate":
        return _validate_yaml_only(yaml_content)
    return immutable_planning.plan_pipeline_yaml(yaml_content).to_dict()


def validate_yaml_runnable(yaml_content: str) -> dict[str, object]:
    """Return immutable planning schema 1.0 for the deprecated runnable alias."""
    return immutable_planning.plan_pipeline_yaml(yaml_content).to_dict()
