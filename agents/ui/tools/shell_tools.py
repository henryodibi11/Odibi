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
