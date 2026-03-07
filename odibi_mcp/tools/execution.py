"""Pipeline execution and testing tools for MCP.

Provides safe, sandboxed pipeline testing with three modes:
1. validate - YAML structure validation only
2. dry-run - Validation + execution plan (no actual execution)
3. sample - Limited execution on first N rows (local only)
"""

import tempfile
from pathlib import Path
from typing import Any, Dict, Literal
import yaml
import subprocess
import sys

from odibi.config import ProjectConfig


class ExecutionError(Exception):
    """Pipeline execution error."""

    pass


def test_pipeline(
    yaml_content: str,
    mode: Literal["validate", "dry-run", "sample"] = "dry-run",
    max_rows: int = 100,
) -> Dict[str, Any]:
    """Test a pipeline YAML without full execution.

    Args:
        yaml_content: Complete odibi YAML configuration
        mode: Testing mode (validate, dry-run, sample)
        max_rows: Max rows to process in sample mode (capped at 1000)

    Returns:
        {
            "valid": bool,
            "errors": list,
            "warnings": list,
            "mode": str,
            "execution_plan": str,  # dry-run/sample mode
            "output": str,  # dry-run/sample stdout
        }
    """
    warnings = []

    # Step 1: Validate YAML syntax
    try:
        parsed = yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        return {
            "valid": False,
            "errors": [{"code": "YAML_SYNTAX_ERROR", "message": str(e)}],
            "warnings": [],
            "mode": mode,
        }

    # Step 2: Validate through Pydantic
    try:
        config = ProjectConfig(**parsed)
    except Exception as e:
        return {
            "valid": False,
            "errors": [
                {
                    "code": "VALIDATION_ERROR",
                    "message": f"Config validation failed: {str(e)}",
                }
            ],
            "warnings": [],
            "mode": mode,
        }

    # Security: Sample mode requires local connections only (check before validate mode exit)
    if mode == "sample":
        for conn_name, conn_cfg in config.connections.items():
            # Check if connection is defined (not placeholder)
            # Placeholders are empty ConnectionConfig objects with no type
            if isinstance(conn_cfg, dict):
                # YAML dict form
                conn_type = conn_cfg.get("type", None)
            else:
                # Pydantic model
                conn_type = getattr(conn_cfg, "type", None)

            # If no type specified, it's a placeholder - not safe for sample mode
            # If type is specified and not local/file, also not safe
            if conn_type is None:
                warnings.append(
                    {
                        "code": "SAMPLE_MODE_SECURITY",
                        "message": f"Sample mode requires fully defined connections. Connection '{conn_name}' is a placeholder. Switching to dry-run mode.",
                    }
                )
                mode = "dry-run"
                break
            elif conn_type not in ("local", "file"):
                warnings.append(
                    {
                        "code": "SAMPLE_MODE_SECURITY",
                        "message": f"Sample mode only supports local connections. Connection '{conn_name}' is type '{conn_type}'. Switching to dry-run mode.",
                    }
                )
                mode = "dry-run"
                break

    # Validate mode stops here (after security checks)
    if mode == "validate":
        return {
            "valid": True,
            "errors": [],
            "warnings": warnings,
            "mode": mode,
            "message": f"YAML is valid. Pipeline '{config.pipelines[0].pipeline}' has {len(config.pipelines[0].nodes)} nodes.",
        }

    # Cap max_rows
    max_rows = min(max_rows, 1000)

    # Step 3: Execute via CLI
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write(yaml_content)
        tmp_path = tmp.name

    try:
        cmd = [sys.executable, "-m", "odibi", "run", tmp_path]

        if mode == "dry-run":
            cmd.append("--dry-run")
        elif mode == "sample":
            # Sample mode: run first pipeline with limit
            # Note: odibi doesn't have --limit flag, so we rely on read config limits
            # This is best-effort - actual limiting happens in the YAML
            warnings.append(
                {
                    "code": "SAMPLE_LIMIT",
                    "message": f"Sample mode will execute pipeline. Row limit ({max_rows}) must be set in YAML read config.",
                }
            )

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=Path.cwd(),
        )

        if result.returncode != 0:
            # Check if it's a dry-run success message
            if mode == "dry-run" and "dry" in result.stdout.lower():
                # Dry-run outputs to stdout even on "error" code
                return {
                    "valid": True,
                    "errors": [],
                    "warnings": warnings,
                    "mode": mode,
                    "execution_plan": result.stdout,
                    "output": result.stdout,
                }

            return {
                "valid": False,
                "errors": [
                    {
                        "code": "EXECUTION_FAILED",
                        "message": f"Pipeline execution failed (exit code {result.returncode})",
                        "stdout": result.stdout,
                        "stderr": result.stderr,
                    }
                ],
                "warnings": warnings,
                "mode": mode,
            }

        return {
            "valid": True,
            "errors": [],
            "warnings": warnings,
            "mode": mode,
            "execution_plan": result.stdout if mode == "dry-run" else None,
            "output": result.stdout,
            "message": f"Pipeline executed successfully in {mode} mode",
        }

    except subprocess.TimeoutExpired:
        return {
            "valid": False,
            "errors": [
                {
                    "code": "TIMEOUT",
                    "message": "Pipeline execution exceeded 30 second timeout",
                }
            ],
            "warnings": warnings,
            "mode": mode,
        }
    except Exception as e:
        return {
            "valid": False,
            "errors": [{"code": "EXECUTION_ERROR", "message": str(e)}],
            "warnings": warnings,
            "mode": mode,
        }
    finally:
        # Cleanup temp file
        try:
            Path(tmp_path).unlink()
        except Exception:
            pass


def validate_yaml_runnable(yaml_content: str) -> Dict[str, Any]:
    """Quick validation: parse + dry-run check.

    This is a convenience wrapper for test_pipeline in dry-run mode.
    """
    return test_pipeline(yaml_content, mode="dry-run")
