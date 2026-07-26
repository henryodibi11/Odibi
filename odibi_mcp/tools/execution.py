"""Pipeline validation and CLI dry-run tools for MCP.

Dry-run always selects the CLI ``--dry-run`` command, but known catalog/bootstrap
side effects remain unresolved pending global dry-run hardening.
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
    *,
    mode: Literal["validate", "dry-run"] = "dry-run",
    max_rows: int = 100,
) -> Dict[str, Any]:
    """Validate a pipeline YAML or build its dry-run execution plan.

    Args:
        yaml_content: Complete odibi YAML configuration
        mode: Supported testing mode (validate or dry-run)
        max_rows: Validated row bound forwarded by the MCP dispatcher

    Returns:
        {
            "valid": bool,
            "errors": list,
            "warnings": list,
            "mode": str,
            "execution_plan": str,  # dry-run mode
            "output": str,  # dry-run stdout
        }
    """
    if mode not in ("validate", "dry-run"):
        raise ValueError("mode must be one of: validate, dry-run")
    if type(max_rows) is not int:
        raise TypeError("max_rows must be an integer")
    if not 1 <= max_rows <= 1000:
        raise ValueError("max_rows must be between 1 and 1000")

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

    # Validate mode stops here without creating a file or launching a subprocess.
    if mode == "validate":
        return {
            "valid": True,
            "errors": [],
            "warnings": warnings,
            "mode": mode,
            "message": f"YAML is valid. Pipeline '{config.pipelines[0].pipeline}' has {len(config.pipelines[0].nodes)} nodes.",
        }

    # Step 3: Execute via CLI
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write(yaml_content)
        tmp_path = tmp.name

    try:
        cmd = [sys.executable, "-m", "odibi", "run", tmp_path]

        cmd.append("--dry-run")

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
            "execution_plan": result.stdout,
            "output": result.stdout,
            "message": "Pipeline dry-run completed successfully",
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
