"""Diagnostic utilities for troubleshooting odibi environment issues."""

import os
import sys
from importlib import metadata
from pathlib import Path
from typing import Any, Dict, List


def doctor() -> Dict[str, Any]:
    """Run diagnostics check on environment and configuration.

    Checks:
    - Python version
    - Installed packages (pyspark, pandas, etc.)
    - Environment variables
    - ODIBI_CONFIG if set
    - Actual interpreter/import provenance and Connect distribution conflicts

    This check never loads project contents, credentials, profiles or sessions.

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

    required_packages = ["pandas", "pydantic", "pyyaml"]
    optional_packages = [
        "pyspark",
        "delta-spark",
        "polars",
        "duckdb",
        "azure-storage-blob",
        "pyodbc",
        "databricks-connect",
        "databricks-sdk",
        "pyarrow",
        "deltalake",
        "odibi",
    ]

    for pkg in required_packages:
        try:
            packages[pkg] = metadata.version(pkg)
        except metadata.PackageNotFoundError:
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
            packages[pkg] = metadata.version(pkg)
        except metadata.PackageNotFoundError:
            packages[pkg] = "not installed"

    import odibi

    runtime = {
        "executable": sys.executable,
        "python_version": python_version,
        "odibi_import": odibi.__file__,
        "odibi_version": odibi.__version__,
        "odibi_distribution_version": packages["odibi"],
    }
    if packages["databricks-connect"] != "not installed":
        conflicts = [p for p in ("pyspark", "delta-spark") if packages[p] != "not installed"]
        if conflicts:
            issues.append(
                {
                    "severity": "error",
                    "message": "Databricks Connect conflicts with standalone "
                    + ", ".join(conflicts),
                    "fix": "Use a fresh runtime-matched environment with odibi and databricks-connect, without odibi[spark]/[all]/[dev]",
                }
            )
        line = ".".join(packages["databricks-connect"].split(".")[:2])
        expected_python = {"15.4": (3, 11), "16.4": (3, 12)}.get(line)
        if expected_python and sys.version_info[:2] != expected_python:
            issues.append(
                {
                    "severity": "error",
                    "message": f"Connect {line} requires Python {expected_python[0]}.{expected_python[1]}",
                    "fix": "Select the matching interpreter; see docs/setup_databricks_connect.md and the Databricks compatibility matrix",
                }
            )
        if packages["databricks-sdk"] == "not installed":
            issues.append(
                {
                    "severity": "error",
                    "message": "Databricks SDK is missing",
                    "fix": "Install the dependencies required by your matched databricks-connect release",
                }
            )
    else:
        suggestions.append(
            "For remote Spark, install databricks-connect matched to your compute/Python in a dedicated environment; see docs/setup_databricks_connect.md"
        )

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
        suggestions.append(
            "Project path exists; contents, credentials and connections were not loaded or tested"
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
        "runtime": runtime,
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
