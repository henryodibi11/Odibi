"""Shell command execution tools for agents.

Run shell commands, pytest, ruff, and Odibi CLI.
"""

import os
import subprocess
import sys
from dataclasses import dataclass
from typing import Optional


@dataclass
class CommandResult:
    """Result of a shell command execution."""

    success: bool
    stdout: str
    stderr: str
    return_code: int
    command: str
    working_dir: str
    error: Optional[str] = None


def run_command(
    command: str,
    working_dir: Optional[str] = None,
    timeout: int = 60,
    shell: bool = True,
    env: Optional[dict[str, str]] = None,
) -> CommandResult:
    """Execute a shell command.

    Args:
        command: The command to execute.
        working_dir: Working directory for the command.
        timeout: Timeout in seconds.
        shell: Whether to run through shell.
        env: Additional environment variables.

    Returns:
        CommandResult with output and status.
    """
    cwd = working_dir or os.getcwd()

    full_env = os.environ.copy()
    if env:
        full_env.update(env)

    try:
        result = subprocess.run(
            command,
            shell=shell,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=full_env,
        )

        return CommandResult(
            success=result.returncode == 0,
            stdout=result.stdout,
            stderr=result.stderr,
            return_code=result.returncode,
            command=command,
            working_dir=cwd,
        )

    except subprocess.TimeoutExpired:
        return CommandResult(
            success=False,
            stdout="",
            stderr="",
            return_code=-1,
            command=command,
            working_dir=cwd,
            error=f"Command timed out after {timeout} seconds",
        )
    except Exception as e:
        return CommandResult(
            success=False,
            stdout="",
            stderr="",
            return_code=-1,
            command=command,
            working_dir=cwd,
            error=str(e),
        )


def run_pytest(
    test_path: Optional[str] = None,
    working_dir: Optional[str] = None,
    verbose: bool = True,
    capture: bool = False,
    markers: Optional[str] = None,
    extra_args: Optional[list[str]] = None,
) -> CommandResult:
    """Run pytest with specified options.

    Args:
        test_path: Specific test file or directory.
        working_dir: Working directory.
        verbose: Use verbose output.
        capture: Capture stdout/stderr (False for no capture).
        markers: Pytest marker expression.
        extra_args: Additional pytest arguments.

    Returns:
        CommandResult with test output.
    """
    cmd_parts = [sys.executable, "-m", "pytest"]

    if verbose:
        cmd_parts.append("-v")

    if not capture:
        cmd_parts.append("-s")

    if markers:
        cmd_parts.extend(["-m", markers])

    if extra_args:
        cmd_parts.extend(extra_args)

    if test_path:
        cmd_parts.append(test_path)

    command = " ".join(cmd_parts)
    return run_command(command, working_dir=working_dir, timeout=300)


def run_ruff(
    path: str = ".",
    working_dir: Optional[str] = None,
    fix: bool = False,
    check_only: bool = True,
) -> CommandResult:
    """Run ruff linter/formatter.

    Args:
        path: Path to lint.
        working_dir: Working directory.
        fix: Auto-fix issues.
        check_only: Only check, don't modify.

    Returns:
        CommandResult with ruff output.
    """
    cmd_parts = [sys.executable, "-m", "ruff", "check", path]

    if fix:
        cmd_parts.append("--fix")

    command = " ".join(cmd_parts)
    return run_command(command, working_dir=working_dir, timeout=60)


def run_odibi_pipeline(
    pipeline_path: str,
    working_dir: Optional[str] = None,
    dry_run: bool = True,
    engine: str = "pandas",
) -> CommandResult:
    """Execute an Odibi pipeline.

    Args:
        pipeline_path: Path to pipeline YAML file.
        working_dir: Working directory.
        dry_run: Validate without executing.
        engine: Engine to use (pandas, spark, polars).

    Returns:
        CommandResult with pipeline output.
    """
    cmd_parts = [
        sys.executable,
        "-m",
        "odibi",
        "run",
        pipeline_path,
        "--engine",
        engine,
    ]

    if dry_run:
        cmd_parts.append("--dry-run")

    command = " ".join(cmd_parts)
    return run_command(command, working_dir=working_dir, timeout=300)


def run_diagnostics(
    path: str = ".",
    working_dir: Optional[str] = None,
    include_ruff: bool = True,
    include_mypy: bool = False,
    include_pytest: bool = False,
) -> CommandResult:
    """Run code diagnostics (linting, type checking, tests).

    Args:
        path: Path to check.
        working_dir: Working directory.
        include_ruff: Run ruff linter.
        include_mypy: Run mypy type checker.
        include_pytest: Run pytest.

    Returns:
        CommandResult with combined diagnostics.
    """
    results = []
    all_success = True
    cwd = working_dir or os.getcwd()

    if include_ruff:
        ruff_result = run_ruff(path=path, working_dir=cwd)
        if not ruff_result.success:
            all_success = False
        results.append(("ruff", ruff_result))

    if include_mypy:
        mypy_cmd = f"{sys.executable} -m mypy {path} --ignore-missing-imports"
        mypy_result = run_command(mypy_cmd, working_dir=cwd, timeout=120)
        if not mypy_result.success:
            all_success = False
        results.append(("mypy", mypy_result))

    if include_pytest:
        pytest_result = run_pytest(test_path=path, working_dir=cwd)
        if not pytest_result.success:
            all_success = False
        results.append(("pytest", pytest_result))

    combined_stdout = []
    combined_stderr = []

    for name, result in results:
        combined_stdout.append(f"=== {name.upper()} ===")
        if result.stdout:
            combined_stdout.append(result.stdout)
        if result.stderr:
            combined_stderr.append(f"[{name}] {result.stderr}")

    return CommandResult(
        success=all_success,
        stdout="\n".join(combined_stdout),
        stderr="\n".join(combined_stderr),
        return_code=0 if all_success else 1,
        command=f"diagnostics({path})",
        working_dir=cwd,
    )


def run_typecheck(
    path: str = ".",
    working_dir: Optional[str] = None,
) -> CommandResult:
    """Run Python type checker (mypy).

    Args:
        path: Path to check.
        working_dir: Working directory.

    Returns:
        CommandResult with type checking output.
    """
    cmd = f"{sys.executable} -m mypy {path} --ignore-missing-imports --no-error-summary"
    return run_command(cmd, working_dir=working_dir, timeout=120)


def format_command_result(result: CommandResult) -> str:
    """Format command result for display in chat.

    Args:
        result: The command result to format.

    Returns:
        Markdown-formatted command output.
    """
    status = "✅" if result.success else "❌"

    output_parts = [
        f"{status} **Command:** `{result.command}`",
        f"**Exit code:** {result.return_code}",
    ]

    if result.error:
        output_parts.append(f"**Error:** {result.error}")

    if result.stdout:
        stdout_preview = result.stdout[:2000]
        if len(result.stdout) > 2000:
            stdout_preview += f"\n... ({len(result.stdout) - 2000} more characters)"
        output_parts.append(f"\n```\n{stdout_preview}\n```")

    if result.stderr and result.stderr.strip():
        stderr_preview = result.stderr[:1000]
        if len(result.stderr) > 1000:
            stderr_preview += f"\n... ({len(result.stderr) - 1000} more characters)"
        output_parts.append(f"\n**stderr:**\n```\n{stderr_preview}\n```")

    return "\n".join(output_parts)
