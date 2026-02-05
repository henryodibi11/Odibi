# odibi_mcp/tools/execute.py
"""Execution tools for active AI agent behavior."""

import subprocess
import sys
import os
import tempfile
import logging
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Safety limits
MAX_CODE_LENGTH = 50000
MAX_OUTPUT_LENGTH = 100000
EXECUTION_TIMEOUT = 600  # 10 minutes - ADLS operations can be slow


@dataclass
class ExecutionResult:
    """Result of code/command execution."""

    success: bool
    stdout: str
    stderr: str
    return_code: int
    error: Optional[str] = None


@dataclass
class FindPathResult:
    """Result of path search."""

    pattern: str
    start_dir: str
    matches: List[str] = field(default_factory=list)
    error: Optional[str] = None


def run_python(code: str, timeout: int = EXECUTION_TIMEOUT) -> ExecutionResult:
    """
    Execute Python code and return the output.

    This enables the AI to:
    - Test code snippets before suggesting them
    - Analyze data by running quick scripts
    - Debug by inspecting runtime state
    - Self-heal by trying different approaches

    Args:
        code: Python code to execute
        timeout: Maximum execution time in seconds (default: 120)

    Returns:
        ExecutionResult with stdout, stderr, and success status

    Example:
        run_python('''
        import pandas as pd
        df = pd.read_csv("data.csv")
        print(df.head())
        print(df.dtypes)
        ''')
    """
    if len(code) > MAX_CODE_LENGTH:
        return ExecutionResult(
            success=False,
            stdout="",
            stderr="",
            return_code=-1,
            error=f"Code too long: {len(code)} chars (max: {MAX_CODE_LENGTH})",
        )

    # Write code to temp file
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False, encoding="utf-8"
        ) as f:
            f.write(code)
            temp_path = f.name

        # Execute with current Python interpreter
        env = os.environ.copy()
        # Ensure odibi is importable
        pythonpath = env.get("PYTHONPATH", "")
        odibi_root = str(Path(__file__).parent.parent.parent)
        if odibi_root not in pythonpath:
            env["PYTHONPATH"] = f"{odibi_root}{os.pathsep}{pythonpath}"

        # Use PYTHONUNBUFFERED to ensure output isn't stuck in buffers
        env["PYTHONUNBUFFERED"] = "1"

        result = subprocess.run(
            [sys.executable, "-u", temp_path],  # -u for unbuffered
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
            cwd=os.getcwd(),
        )

        stdout = result.stdout[:MAX_OUTPUT_LENGTH]
        stderr = result.stderr[:MAX_OUTPUT_LENGTH]

        if len(result.stdout) > MAX_OUTPUT_LENGTH:
            stdout += f"\n... (truncated, {len(result.stdout)} total chars)"
        if len(result.stderr) > MAX_OUTPUT_LENGTH:
            stderr += f"\n... (truncated, {len(result.stderr)} total chars)"

        return ExecutionResult(
            success=result.returncode == 0,
            stdout=stdout,
            stderr=stderr,
            return_code=result.returncode,
        )

    except subprocess.TimeoutExpired as e:
        # Try to capture any partial output
        partial_stdout = ""
        partial_stderr = ""
        if hasattr(e, "stdout") and e.stdout:
            partial_stdout = (
                e.stdout[:MAX_OUTPUT_LENGTH]
                if isinstance(e.stdout, str)
                else e.stdout.decode("utf-8", errors="replace")[:MAX_OUTPUT_LENGTH]
            )
        if hasattr(e, "stderr") and e.stderr:
            partial_stderr = (
                e.stderr[:MAX_OUTPUT_LENGTH]
                if isinstance(e.stderr, str)
                else e.stderr.decode("utf-8", errors="replace")[:MAX_OUTPUT_LENGTH]
            )

        return ExecutionResult(
            success=False,
            stdout=partial_stdout,
            stderr=partial_stderr,
            return_code=-1,
            error=f"Execution timed out after {timeout} seconds. Try increasing timeout or running manually: python -m odibi run <config>.yaml",
        )
    except Exception as e:
        return ExecutionResult(
            success=False,
            stdout="",
            stderr="",
            return_code=-1,
            error=str(e),
        )
    finally:
        # Cleanup temp file
        try:
            if "temp_path" in locals():
                os.unlink(temp_path)
        except Exception:
            pass


def run_odibi(args: str, timeout: int = EXECUTION_TIMEOUT) -> ExecutionResult:
    """
    Execute odibi CLI command and return output.

    This enables the AI to:
    - Run pipelines: run_odibi("run project.yaml")
    - Dry-run validation: run_odibi("run project.yaml --dry-run")
    - Check environment: run_odibi("doctor")
    - View execution history: run_odibi("story last")
    - List features: run_odibi("list transformers")

    Args:
        args: Arguments to pass to `python -m odibi <args>`
        timeout: Maximum execution time in seconds (default: 120)

    Returns:
        ExecutionResult with stdout, stderr, and success status

    Example:
        run_odibi("run projects/bronze.yaml --dry-run")
        run_odibi("list transformers")
        run_odibi("doctor")
    """
    try:
        env = os.environ.copy()
        pythonpath = env.get("PYTHONPATH", "")
        odibi_root = str(Path(__file__).parent.parent.parent)
        if odibi_root not in pythonpath:
            env["PYTHONPATH"] = f"{odibi_root}{os.pathsep}{pythonpath}"

        # Build command with unbuffered output
        cmd = [sys.executable, "-u", "-m", "odibi"] + args.split()

        # Use PYTHONUNBUFFERED to ensure output isn't stuck in buffers
        env["PYTHONUNBUFFERED"] = "1"

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
            cwd=os.getcwd(),
        )

        stdout = result.stdout[:MAX_OUTPUT_LENGTH]
        stderr = result.stderr[:MAX_OUTPUT_LENGTH]

        return ExecutionResult(
            success=result.returncode == 0,
            stdout=stdout,
            stderr=stderr,
            return_code=result.returncode,
        )

    except subprocess.TimeoutExpired as e:
        # Try to capture any partial output
        partial_stdout = ""
        partial_stderr = ""
        if hasattr(e, "stdout") and e.stdout:
            partial_stdout = (
                e.stdout[:MAX_OUTPUT_LENGTH]
                if isinstance(e.stdout, str)
                else e.stdout.decode("utf-8", errors="replace")[:MAX_OUTPUT_LENGTH]
            )
        if hasattr(e, "stderr") and e.stderr:
            partial_stderr = (
                e.stderr[:MAX_OUTPUT_LENGTH]
                if isinstance(e.stderr, str)
                else e.stderr.decode("utf-8", errors="replace")[:MAX_OUTPUT_LENGTH]
            )

        return ExecutionResult(
            success=False,
            stdout=partial_stdout,
            stderr=partial_stderr,
            return_code=-1,
            error=f"Command timed out after {timeout} seconds. Try: python -m odibi {args}",
        )
    except Exception as e:
        return ExecutionResult(
            success=False,
            stdout="",
            stderr="",
            return_code=-1,
            error=str(e),
        )


def find_path(
    pattern: str,
    start_dir: Optional[str] = None,
    max_results: int = 50,
) -> FindPathResult:
    """
    Search for files/directories matching a glob pattern.

    This enables the AI to:
    - Find files when path is unknown
    - Discover project structure
    - Locate config files, data files, etc.
    - Self-recover when given bad paths

    Args:
        pattern: Glob pattern (e.g., "*.yaml", "**/*.csv", "**/stories/**")
        start_dir: Directory to start search (default: cwd, or ODIBI_PROJECTS_DIR)
        max_results: Maximum number of results to return

    Returns:
        FindPathResult with list of matching paths

    Example:
        find_path("*.yaml")  # Find all YAML files in cwd
        find_path("**/*.csv", "D:/data")  # Find CSVs recursively
        find_path("**/stories/**/run_*.json")  # Find story files
    """
    try:
        # Determine start directory
        if start_dir:
            base = Path(start_dir)
        else:
            # Try ODIBI_PROJECTS_DIR first, then cwd
            projects_dir = os.environ.get("ODIBI_PROJECTS_DIR")
            if projects_dir and Path(projects_dir).exists():
                base = Path(projects_dir)
            else:
                base = Path.cwd()

        if not base.exists():
            return FindPathResult(
                pattern=pattern,
                start_dir=str(base),
                error=f"Start directory does not exist: {base}",
            )

        # Find matches
        matches = []
        for path in base.glob(pattern):
            matches.append(str(path))
            if len(matches) >= max_results:
                break

        return FindPathResult(
            pattern=pattern,
            start_dir=str(base),
            matches=matches,
        )

    except Exception as e:
        return FindPathResult(
            pattern=pattern,
            start_dir=str(start_dir or ""),
            error=str(e),
        )


def execute_pipeline(
    config_path: str,
    pipeline_name: Optional[str] = None,
    dry_run: bool = False,
) -> ExecutionResult:
    """
    Execute an odibi pipeline.

    Convenience wrapper around run_odibi for running pipelines.

    Args:
        config_path: Path to project YAML file
        pipeline_name: Specific pipeline to run (optional, runs all if not specified)
        dry_run: If True, validate without writing outputs

    Returns:
        ExecutionResult with execution output

    Example:
        execute_pipeline("projects/bronze.yaml", dry_run=True)
        execute_pipeline("projects/silver.yaml", "customer_dim")
    """
    args = f"run {config_path}"
    if pipeline_name:
        args += f" --pipeline {pipeline_name}"
    if dry_run:
        args += " --dry-run"

    return run_odibi(args)
