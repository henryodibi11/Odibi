"""OdibiPipelineRunner: Runs odibi pipelines and captures results.

Executes odibi commands in a sandbox environment and captures structured
results for decision-making. All execution happens via subprocess to
isolate from the agent's Python environment.

Supports WSL execution for Spark compatibility on Windows.
"""

import logging
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Optional

from odibi.agents.improve.results import (
    CommandResult,
    ExecutionStatus,
    GoldenResult,
    LintResult,
    PipelineResult,
    TestResult,
    ValidationResult,
)

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 300  # 5 minutes
PYTEST_TIMEOUT = 600  # 10 minutes for tests
PIPELINE_TIMEOUT = 900  # 15 minutes for pipelines


def _is_windows() -> bool:
    """Check if running on Windows."""
    return os.name == "nt"


def _windows_to_wsl_path(windows_path: Path) -> str:
    """Convert Windows path to WSL path.

    Examples:
        D:\\odibi -> /mnt/d/odibi
        C:\\Users\\name -> /mnt/c/Users/name
    """
    path_str = str(windows_path.resolve())
    if len(path_str) >= 2 and path_str[1] == ":":
        drive = path_str[0].lower()
        rest = path_str[2:].replace("\\", "/")
        return f"/mnt/{drive}{rest}"
    return path_str.replace("\\", "/")


def check_wsl_available(distro: str = "Ubuntu") -> tuple[bool, str]:
    """Check if WSL and the specified distribution are available.

    Returns:
        Tuple of (is_available, message).
    """
    if not _is_windows():
        return False, "Not running on Windows - WSL not applicable"

    try:
        # Check if wsl.exe exists
        result = subprocess.run(
            ["wsl.exe", "--list", "--quiet"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            return False, f"WSL not available: {result.stderr}"

        # Check if the specific distro is installed
        distros = result.stdout.strip().split("\n")
        # Clean up distro names (may have null chars on Windows)
        distros = [d.replace("\x00", "").strip() for d in distros if d.strip()]

        if not any(distro.lower() in d.lower() for d in distros):
            available = ", ".join(distros) if distros else "none"
            return False, f"WSL distribution '{distro}' not found. Available: {available}"

        return True, f"WSL distribution '{distro}' is available"

    except FileNotFoundError:
        return False, "wsl.exe not found - WSL may not be installed"
    except subprocess.TimeoutExpired:
        return False, "WSL check timed out"
    except Exception as e:
        return False, f"WSL check failed: {e}"


def check_wsl_environment(
    distro: str = "Ubuntu",
    python_cmd: str = "python3",
    check_spark: bool = True,
    spark_home: str = "",
    java_home: str = "",
) -> tuple[bool, list[str]]:
    """Validate WSL environment for running Spark/Odibi.

    Returns:
        Tuple of (all_ok, list of messages).
    """
    messages = []

    # First check WSL is available
    wsl_ok, wsl_msg = check_wsl_available(distro)
    if not wsl_ok:
        return False, [wsl_msg]
    messages.append(f"✅ {wsl_msg}")

    def run_wsl_check(cmd: str) -> tuple[bool, str]:
        """Run a command in WSL and return success + output."""
        try:
            result = subprocess.run(
                ["wsl.exe", "-d", distro, "--", "bash", "-c", cmd],
                capture_output=True,
                text=True,
                timeout=30,
            )
            output = (result.stdout + result.stderr).strip()
            return result.returncode == 0, output
        except Exception as e:
            return False, str(e)

    # Check Python
    py_ok, py_out = run_wsl_check(f"which {python_cmd} && {python_cmd} --version")
    if py_ok:
        messages.append(f"✅ Python: {py_out.split()[-1] if py_out else 'found'}")
    else:
        messages.append(f"❌ Python '{python_cmd}' not found in WSL")
        return False, messages

    # Check pip/packages
    pip_ok, _ = run_wsl_check(f"{python_cmd} -c 'import pyspark' 2>/dev/null")
    if pip_ok:
        messages.append("✅ PySpark module available")
    else:
        messages.append("⚠️ PySpark module not found (may need: pip install pyspark)")

    if check_spark:
        # Check JAVA_HOME
        if java_home:
            java_check = f"[ -d '{java_home}' ] && echo 'exists'"
            java_ok, _ = run_wsl_check(java_check)
            if java_ok:
                messages.append(f"✅ JAVA_HOME exists: {java_home}")
            else:
                messages.append(f"⚠️ JAVA_HOME path not found: {java_home}")

        # Check java command
        java_cmd_ok, java_ver = run_wsl_check("java -version 2>&1 | head -1")
        if java_cmd_ok:
            messages.append(f"✅ Java: {java_ver[:50] if java_ver else 'found'}")
        else:
            messages.append("❌ Java not found in PATH")

        # Check SPARK_HOME
        if spark_home:
            spark_check = f"[ -d '{spark_home}' ] && echo 'exists'"
            spark_ok, _ = run_wsl_check(spark_check)
            if spark_ok:
                messages.append(f"✅ SPARK_HOME exists: {spark_home}")
            else:
                messages.append(f"⚠️ SPARK_HOME path not found: {spark_home}")

        # Check spark-submit
        spark_cmd_ok, spark_out = run_wsl_check(
            "which spark-submit 2>/dev/null || echo 'not in PATH'"
        )
        if spark_cmd_ok and "not in PATH" not in spark_out:
            messages.append(f"✅ spark-submit: {spark_out}")
        else:
            messages.append("⚠️ spark-submit not in PATH (may work via SPARK_HOME)")

    # Check pytest
    pytest_ok, _ = run_wsl_check(f"{python_cmd} -m pytest --version 2>/dev/null")
    if pytest_ok:
        messages.append("✅ pytest available")
    else:
        messages.append("⚠️ pytest not found")

    # Check ruff
    ruff_ok, _ = run_wsl_check(f"{python_cmd} -m ruff --version 2>/dev/null")
    if ruff_ok:
        messages.append("✅ ruff available")
    else:
        messages.append("⚠️ ruff not found")

    # Overall status
    errors = [m for m in messages if m.startswith("❌")]
    return len(errors) == 0, messages


class RunnerError(Exception):
    """Base exception for runner operations."""

    pass


class OdibiPipelineRunner:
    """Runs odibi pipelines in a sandbox and captures results.

    All commands are executed in the sandbox directory context using
    subprocess. This ensures isolation from the agent's Python environment.

    Supports WSL execution for Spark on Windows via use_wsl option.

    Attributes:
        sandbox_path: Path to the sandbox directory.
        python_cmd: Python command to use (default: "python").
        use_wsl: Whether to run commands through WSL.
        wsl_distro: WSL distribution name (default: "Ubuntu").
        wsl_env: Environment variables to set in WSL (for SPARK_HOME, etc.).
        wsl_shell_init: Shell commands to run before each command.
    """

    def __init__(
        self,
        sandbox_path: Path,
        python_cmd: str = "python",
        use_wsl: bool = False,
        wsl_distro: str = "Ubuntu",
        wsl_env: Optional[dict[str, str]] = None,
        wsl_shell_init: str = "",
    ) -> None:
        """Initialize the runner.

        Args:
            sandbox_path: Path to the sandbox directory.
            python_cmd: Python command to use.
            use_wsl: Run commands through WSL (for Spark on Windows).
            wsl_distro: WSL distribution to use.
            wsl_env: Environment variables to export in WSL (SPARK_HOME, JAVA_HOME, etc.).
            wsl_shell_init: Shell init commands (e.g., 'source ~/.bashrc').
        """
        self._sandbox_path = Path(sandbox_path)
        self._python_cmd = python_cmd
        self._use_wsl = use_wsl and _is_windows()
        self._wsl_distro = wsl_distro
        self._wsl_env = wsl_env or {}
        self._wsl_shell_init = wsl_shell_init

        if not self._sandbox_path.exists():
            raise RunnerError(f"Sandbox path does not exist: {sandbox_path}")

    @property
    def sandbox_path(self) -> Path:
        """Read-only access to sandbox path."""
        return self._sandbox_path

    def _run_command(
        self,
        args: list[str],
        timeout: int = DEFAULT_TIMEOUT,
        cwd: Optional[Path] = None,
    ) -> CommandResult:
        """Run a command and capture output.

        Supports WSL execution when use_wsl is enabled.

        Args:
            args: Command arguments.
            timeout: Timeout in seconds.
            cwd: Working directory (default: sandbox_path).

        Returns:
            CommandResult with output and status.
        """
        work_dir = cwd or self._sandbox_path

        # Build the actual command to run
        if self._use_wsl:
            # Convert paths to WSL format and wrap with wsl.exe
            wsl_cwd = _windows_to_wsl_path(work_dir)

            # Build environment exports for Spark
            env_exports = []
            for key, value in self._wsl_env.items():
                if value:  # Only export non-empty values
                    env_exports.append(f"export {key}='{value}'")
            env_str = " && ".join(env_exports) if env_exports else ""

            # Build shell init (e.g., source ~/.bashrc)
            init_str = self._wsl_shell_init.strip() if self._wsl_shell_init else ""

            # Join args into a single command string for bash
            inner_cmd = " ".join(args)

            # Combine: init -> env exports -> cd -> command
            parts = []
            if init_str:
                parts.append(init_str)
            if env_str:
                parts.append(env_str)
            parts.append(f"cd '{wsl_cwd}'")
            parts.append(inner_cmd)
            full_cmd = " && ".join(parts)

            # Run via wsl
            actual_args = [
                "wsl.exe",
                "-d",
                self._wsl_distro,
                "--",
                "bash",
                "-c",
                full_cmd,
            ]
            cmd_str = f"[WSL:{self._wsl_distro}] {inner_cmd}"
        else:
            actual_args = args
            cmd_str = " ".join(args)

        logger.debug(f"Running command: {cmd_str} in {work_dir}")

        start = time.time()
        timed_out = False
        stdout = ""
        stderr = ""
        exit_code = -1

        try:
            result = subprocess.run(
                actual_args,
                cwd=None if self._use_wsl else work_dir,  # WSL handles cwd via bash cd
                capture_output=True,
                text=True,
                timeout=timeout,
                encoding="utf-8",
                errors="replace",
            )
            stdout = result.stdout or ""
            stderr = result.stderr or ""
            exit_code = result.returncode

            # Check for common WSL errors
            if self._use_wsl:
                combined = stdout + stderr
                if "The Windows Subsystem for Linux has not been enabled" in combined:
                    stderr = "WSL is not enabled. Run 'wsl --install' in admin PowerShell."
                    exit_code = 1
                elif "cannot access" in combined and "/mnt/" in combined:
                    stderr = f"WSL cannot access path. Ensure drive is mounted in WSL.\n{stderr}"
                elif "command not found" in combined or "not found" in stderr:
                    # Add helpful hints for missing commands
                    if "spark" in combined.lower():
                        stderr += "\nHint: Ensure SPARK_HOME is set and spark/bin is in PATH"
                    if "java" in combined.lower():
                        stderr += "\nHint: Ensure JAVA_HOME is set and Java is installed"
                    if "python" in combined.lower():
                        stderr += f"\nHint: Ensure {self._python_cmd} is installed in WSL"

        except subprocess.TimeoutExpired as e:
            timed_out = True
            stdout = (
                e.stdout or ""
                if isinstance(e.stdout, str)
                else (e.stdout.decode("utf-8", errors="replace") if e.stdout else "")
            )
            stderr = (
                e.stderr or ""
                if isinstance(e.stderr, str)
                else (e.stderr.decode("utf-8", errors="replace") if e.stderr else "")
            )
            logger.warning(f"Command timed out after {timeout}s: {cmd_str}")
        except FileNotFoundError as e:
            stderr = f"Command not found: {e}. "
            if self._use_wsl:
                stderr += "Ensure wsl.exe is available and WSL is installed."
            exit_code = 127
            logger.error(f"Command not found: {cmd_str}: {e}")
        except PermissionError as e:
            stderr = f"Permission denied: {e}"
            exit_code = 126
            logger.error(f"Permission denied: {cmd_str}: {e}")
        except Exception as e:
            stderr = f"Unexpected error: {e}"
            exit_code = 1
            logger.error(f"Command failed: {cmd_str}: {e}")

        duration = time.time() - start

        return CommandResult(
            command=cmd_str,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            duration=duration,
            timed_out=timed_out,
        )

    def run_pipeline(
        self,
        config_path: str,
        timeout: int = PIPELINE_TIMEOUT,
    ) -> PipelineResult:
        """Run an odibi pipeline.

        Args:
            config_path: Path to the pipeline config file (relative to sandbox).
            timeout: Timeout in seconds.

        Returns:
            PipelineResult with execution details.
        """
        args = [
            self._python_cmd,
            "-m",
            "odibi",
            "run",
            config_path,
        ]

        result = self._run_command(args, timeout=timeout)

        # Parse output for pipeline info
        pipeline_name = Path(config_path).stem
        status = ExecutionStatus.SUCCESS if result.success else ExecutionStatus.FAILED

        if result.timed_out:
            status = ExecutionStatus.TIMEOUT

        # Try to extract node counts from output
        total_nodes = 0
        completed_nodes = 0
        failed_nodes = 0
        skipped_nodes = 0

        # Look for patterns like "3/5 nodes completed"
        completed_match = re.search(r"(\d+)/(\d+)\s+nodes?\s+completed", result.stdout, re.I)
        if completed_match:
            completed_nodes = int(completed_match.group(1))
            total_nodes = int(completed_match.group(2))

        # Look for failed/skipped counts
        failed_match = re.search(r"(\d+)\s+failed", result.stdout, re.I)
        if failed_match:
            failed_nodes = int(failed_match.group(1))

        skipped_match = re.search(r"(\d+)\s+skipped", result.stdout, re.I)
        if skipped_match:
            skipped_nodes = int(skipped_match.group(1))

        return PipelineResult(
            config_path=config_path,
            pipeline_name=pipeline_name,
            status=status,
            duration=result.duration,
            total_nodes=total_nodes,
            completed_nodes=completed_nodes,
            failed_nodes=failed_nodes,
            skipped_nodes=skipped_nodes,
            exit_code=result.exit_code,
            output=result.stdout + result.stderr,
            error_message=result.stderr if not result.success else None,
        )

    def run_tests(
        self,
        test_path: str = "tests/",
        timeout: int = PYTEST_TIMEOUT,
        verbose: bool = True,
        extra_args: Optional[list[str]] = None,
    ) -> TestResult:
        """Run pytest tests.

        Args:
            test_path: Path to tests directory or file.
            timeout: Timeout in seconds.
            verbose: Whether to use verbose output.
            extra_args: Additional pytest arguments (e.g., --ignore=...).

        Returns:
            TestResult with test counts and output.
        """
        args = [
            self._python_cmd,
            "-m",
            "pytest",
            test_path,
        ]

        if verbose:
            args.append("-v")

        # Add extra arguments like --ignore
        if extra_args:
            args.extend(extra_args)

        result = self._run_command(args, timeout=timeout)

        # Parse pytest output
        passed = 0
        failed = 0
        skipped = 0
        errors = 0
        failed_tests = []

        # Look for summary line like "5 passed, 2 failed, 1 skipped"
        summary_match = re.search(
            r"(\d+)\s+passed(?:.*?(\d+)\s+failed)?(?:.*?(\d+)\s+skipped)?(?:.*?(\d+)\s+error)?",
            result.stdout,
            re.I,
        )
        if summary_match:
            passed = int(summary_match.group(1)) if summary_match.group(1) else 0
            failed = int(summary_match.group(2)) if summary_match.group(2) else 0
            skipped = int(summary_match.group(3)) if summary_match.group(3) else 0
            errors = int(summary_match.group(4)) if summary_match.group(4) else 0

        # Extract failed test names only if there are actual failures
        # pytest outputs: "tests/test_foo.py::test_bar FAILED [100%]"
        # Old regex captured "[100%]" - now we capture the test ID before FAILED
        if failed > 0 or errors > 0 or result.exit_code != 0:
            failed_tests = re.findall(r"(\S+::\S+)\s+FAILED\b", result.stdout)
            # Fallback: try simpler pattern if no :: in output
            if not failed_tests:
                failed_tests = re.findall(r"(\S+\.py\S*)\s+FAILED\b", result.stdout)

        return TestResult(
            passed=passed,
            failed=failed,
            skipped=skipped,
            errors=errors,
            duration=result.duration,
            exit_code=result.exit_code,
            output=result.stdout + result.stderr,
            failed_tests=failed_tests,
        )

    def run_lint(
        self,
        path: str = "odibi/",
        timeout: int = DEFAULT_TIMEOUT,
    ) -> LintResult:
        """Run ruff linter.

        Args:
            path: Path to lint (default: odibi/ package).
            timeout: Timeout in seconds.

        Returns:
            LintResult with error counts and output.
        """
        args = [
            self._python_cmd,
            "-m",
            "ruff",
            "check",
            path,
        ]

        result = self._run_command(args, timeout=timeout)

        # Parse ruff output
        error_count = 0
        warning_count = 0
        fixable_count = 0
        errors = []

        # Look for "Found X errors" pattern
        found_match = re.search(r"Found\s+(\d+)\s+error", result.stdout, re.I)
        if found_match:
            error_count = int(found_match.group(1))

        # Look for fixable count
        fixable_match = re.search(r"(\d+)\s+fixable", result.stdout, re.I)
        if fixable_match:
            fixable_count = int(fixable_match.group(1))

        # Extract individual errors (first line of each error)
        error_lines = re.findall(r"^[A-Z]\d+.*$", result.stdout, re.MULTILINE)
        errors = error_lines[:20]  # Limit to first 20

        return LintResult(
            error_count=error_count,
            warning_count=warning_count,
            fixable_count=fixable_count,
            exit_code=result.exit_code,
            output=result.stdout + result.stderr,
            duration=result.duration,
            errors=errors,
        )

    def validate_config(
        self,
        config_path: str,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> ValidationResult:
        """Run odibi validate on a config file.

        Args:
            config_path: Path to config file.
            timeout: Timeout in seconds.

        Returns:
            ValidationResult with validation status.
        """
        args = [
            self._python_cmd,
            "-m",
            "odibi",
            "validate",
            config_path,
        ]

        result = self._run_command(args, timeout=timeout)

        valid = result.success
        errors = []
        warnings = []

        # Parse validation output
        if "[OK]" in result.stdout:
            valid = True
        elif "[X]" in result.stdout or "failed" in result.stdout.lower():
            valid = False

        # Extract errors
        error_lines = re.findall(r"\[!\].*$|Error:.*$", result.stdout, re.MULTILINE | re.I)
        errors = error_lines

        # Extract warnings
        warning_lines = re.findall(r"\[\?\].*$|Warning:.*$", result.stdout, re.MULTILINE | re.I)
        warnings = warning_lines

        return ValidationResult(
            valid=valid,
            config_path=config_path,
            errors=errors,
            warnings=warnings,
            exit_code=result.exit_code,
            output=result.stdout + result.stderr,
            duration=result.duration,
        )

    def run_golden_project(
        self,
        config_path: str,
        timeout: int = PIPELINE_TIMEOUT,
    ) -> GoldenResult:
        """Run a golden/learning harness config.

        A "golden" project is a known-good pipeline configuration that
        should always pass. Running it detects regressions.

        Args:
            config_path: Path to the golden config.
            timeout: Timeout in seconds.

        Returns:
            GoldenResult with pass/fail status.
        """
        config_name = Path(config_path).stem

        try:
            # First validate the config
            validation = self.validate_config(config_path, timeout=60)
            if not validation.valid:
                return GoldenResult(
                    config_name=config_name,
                    config_path=config_path,
                    passed=False,
                    error_message=f"Validation failed: {validation.errors}",
                    duration=validation.duration,
                )

            # Then run the pipeline
            pipeline_result = self.run_pipeline(config_path, timeout=timeout)

            return GoldenResult(
                config_name=config_name,
                config_path=config_path,
                pipeline_result=pipeline_result,
                passed=pipeline_result.success,
                error_message=pipeline_result.error_message,
                duration=pipeline_result.duration,
            )

        except Exception as e:
            return GoldenResult(
                config_name=config_name,
                config_path=config_path,
                passed=False,
                error_message=str(e),
            )

    def run_golden_projects(
        self,
        harness_dir: str = ".odibi/learning_harness",
        timeout: int = PIPELINE_TIMEOUT,
    ) -> list[GoldenResult]:
        """Run all golden/learning harness configs.

        Args:
            harness_dir: Directory containing harness configs.
            timeout: Timeout per config.

        Returns:
            List of GoldenResult for each config.
        """
        harness_path = self._sandbox_path / harness_dir
        results = []

        if not harness_path.exists():
            logger.warning(f"Learning harness directory not found: {harness_path}")
            return results

        # Find all .yaml files in harness directory
        for config_file in harness_path.glob("*.yaml"):
            if config_file.name.startswith("_"):
                continue  # Skip private/template files

            relative_path = str(config_file.relative_to(self._sandbox_path))
            result = self.run_golden_project(relative_path, timeout=timeout)
            results.append(result)

        return results

    def get_modified_files(self) -> list[str]:
        """Get list of files modified in the sandbox.

        Uses git diff if available, otherwise returns empty list.

        Returns:
            List of modified file paths relative to sandbox.
        """
        # This is a placeholder - in a real implementation you'd compare
        # against the original master copy or use git
        return []

    def get_diff(self, against_path: Optional[Path] = None) -> str:
        """Get diff of changes in sandbox.

        Args:
            against_path: Path to compare against (e.g., master copy).

        Returns:
            Diff string.
        """
        # Placeholder - would use difflib or git diff
        return ""
