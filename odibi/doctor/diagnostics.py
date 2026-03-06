"""Diagnostic utilities for troubleshooting odibi environment issues."""

import os
import sys
from pathlib import Path
from typing import Any, Dict, List


def doctor() -> Dict[str, Any]:
    """Run diagnostics check on environment and configuration.

    Checks:
    - Python version
    - Installed packages (pyspark, pandas, etc.)
    - Environment variables
    - ODIBI_CONFIG if set
    - Connections if project loaded

    Returns:
        Dictionary with:
        - status: "healthy" | "warnings" | "errors"
        - python_version: str
        - packages: {name: version}
        - environment: {var: "set" | "not set"}
        - project_loaded: bool
        - connections: {name: "ok" | "error"}
        - issues: [{"severity": "error"|"warning", "message": str, "fix": str}]
        - suggestions: [str]
    """
    issues: List[Dict[str, str]] = []
    suggestions: List[str] = []
    packages: Dict[str, str] = {}
    environment: Dict[str, Any] = {}
    connections: Dict[str, str] = {}

    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    environment["python_version"] = python_version

    if sys.version_info < (3, 9):
        issues.append(
            {
                "severity": "error",
                "message": f"Python {python_version} is not supported",
                "fix": "Install Python 3.9 or higher",
            }
        )

    required_packages = ["pandas", "pydantic", "yaml"]
    optional_packages = ["pyspark", "polars", "duckdb", "azure.storage.blob", "pyodbc"]

    for pkg in required_packages:
        try:
            if pkg == "yaml":
                import yaml

                packages["pyyaml"] = getattr(yaml, "__version__", "unknown")
            else:
                mod = __import__(pkg)
                packages[pkg] = getattr(mod, "__version__", "unknown")
        except ImportError:
            issues.append(
                {
                    "severity": "error",
                    "message": f"Required package '{pkg}' not installed",
                    "fix": f"pip install {pkg}",
                }
            )
            packages[pkg] = "not installed"

    for pkg in optional_packages:
        try:
            mod = __import__(pkg.split(".")[0])
            packages[pkg.split(".")[0]] = getattr(mod, "__version__", "unknown")
        except ImportError:
            packages[pkg.split(".")[0]] = "not installed"

    odibi_config = os.environ.get("ODIBI_CONFIG", "")
    odibi_projects_dir = os.environ.get("ODIBI_PROJECTS_DIR", "")

    environment["ODIBI_CONFIG"] = odibi_config or "not set"
    environment["ODIBI_PROJECTS_DIR"] = odibi_projects_dir or "not set"
    environment["cwd"] = os.getcwd()

    project_loaded = False

    if not odibi_config:
        issues.append(
            {
                "severity": "warning",
                "message": "ODIBI_CONFIG environment variable is not set",
                "fix": "Set ODIBI_CONFIG to point to your exploration.yaml or project.yaml",
            }
        )
    elif not Path(odibi_config).exists():
        issues.append(
            {
                "severity": "error",
                "message": f"ODIBI_CONFIG points to non-existent file: {odibi_config}",
                "fix": "Check the path in your environment configuration",
            }
        )
    else:
        project_loaded = True
        try:
            from odibi.context import create_context

            ctx = create_context(Path(odibi_config))
            ctx.initialize_connections()
            for name, conn in ctx.connections.items():
                try:
                    conn_type = type(conn).__name__
                    connections[name] = f"ok ({conn_type})"
                except Exception as e:
                    connections[name] = f"error: {e}"
                    issues.append(
                        {
                            "severity": "error",
                            "message": f"Connection '{name}' failed to initialize: {e}",
                            "fix": "Check connection configuration in YAML file",
                        }
                    )
        except Exception as e:
            project_loaded = False
            issues.append(
                {
                    "severity": "error",
                    "message": f"Failed to load project context: {e}",
                    "fix": "Check YAML file syntax and configuration",
                }
            )

    if not odibi_projects_dir:
        suggestions.append("Set ODIBI_PROJECTS_DIR for auto-discovery of project files")
    elif not Path(odibi_projects_dir).exists():
        issues.append(
            {
                "severity": "warning",
                "message": f"ODIBI_PROJECTS_DIR points to non-existent folder: {odibi_projects_dir}",
                "fix": "Create the directory or update the environment variable",
            }
        )

    if issues:
        has_errors = any(i["severity"] == "error" for i in issues)
        status = "errors" if has_errors else "warnings"
    else:
        status = "healthy"

    return {
        "status": status,
        "python_version": python_version,
        "packages": packages,
        "environment": environment,
        "project_loaded": project_loaded,
        "connections": connections,
        "issues": issues,
        "suggestions": suggestions,
    }


def diagnose_path(path: str) -> Dict[str, Any]:
    """Diagnose a specific path - check if it exists, readable/writable, detect format.

    Args:
        path: Path to diagnose (relative or absolute)

    Returns:
        Dictionary with:
        - input_path: str
        - resolved_path: str
        - exists: bool
        - is_file: bool
        - is_directory: bool
        - readable: bool
        - writable: bool
        - format: str (for files)
        - size: int (for files)
        - contents: List[Dict] (for directories)
        - error: Optional[str]
    """
    result = {
        "input_path": path,
        "exists": False,
        "is_file": False,
        "is_directory": False,
        "readable": False,
        "writable": False,
        "contents": [],
        "error": None,
    }

    try:
        p = Path(path)

        if not p.is_absolute():
            cwd_path = Path.cwd() / path
            if cwd_path.exists():
                p = cwd_path

        result["resolved_path"] = str(p.absolute())
        result["exists"] = p.exists()

        if p.exists():
            result["is_file"] = p.is_file()
            result["is_directory"] = p.is_dir()
            result["readable"] = os.access(p, os.R_OK)
            result["writable"] = os.access(p, os.W_OK)

            if p.is_dir():
                contents = []
                for item in sorted(p.iterdir())[:50]:
                    item_info = {
                        "name": item.name,
                        "is_dir": item.is_dir(),
                    }
                    if item.is_file():
                        item_info["size"] = item.stat().st_size
                    contents.append(item_info)
                result["contents"] = contents
            elif p.is_file():
                result["size"] = p.stat().st_size
                result["suffix"] = p.suffix
                if p.suffix in [".csv", ".parquet", ".xlsx", ".json", ".yaml", ".yml"]:
                    result["format"] = p.suffix[1:]
                else:
                    result["format"] = "unknown"
    except Exception as e:
        result["error"] = str(e)

    return result
