"""Execution gateway for the Odibi AI Agent Suite.

All code execution goes through this single boundary via run_task.sh in WSL.
Agents MUST NOT execute commands directly; they generate task definitions
that the runner executes.

ENFORCEMENT (Phase 0):
- run_task() requires caller to provide agent permissions
- PermissionDeniedError raised if agent lacks can_execute_tasks

ENFORCEMENT (Phase 7.D):
- Source binding: Pipelines may ONLY read from selected SourcePools
- Source pools are mounted read-only
- Execution MAY NOT access arbitrary filesystem paths or network resources
- All source usage is logged and attributable
"""

import logging
import os
import queue
import shlex
import subprocess
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .agent_base import AgentPermissions, AgentRole
    from .source_binding import ExecutionSourceContext, SourceBindingGuard

from .enforcement import PermissionDeniedError, PermissionEnforcer


StreamOutputCallback = Callable[[str, bool], None]


@dataclass
class ExecutionResult:
    """Result from executing a task via run_task.sh."""

    stdout: str
    stderr: str
    exit_code: int
    command: str
    success: bool

    @property
    def output(self) -> str:
        """Combined stdout and stderr output."""
        parts = []
        if self.stdout:
            parts.append(self.stdout)
        if self.stderr:
            parts.append(f"[stderr]: {self.stderr}")
        return "\n".join(parts)

    def to_evidence(
        self,
        started_at: str,
        finished_at: str,
        truncation_limit: int = 10000,
    ) -> "ExecutionEvidence":
        """Convert to ExecutionEvidence for structured reporting.

        Args:
            started_at: ISO timestamp when execution started.
            finished_at: ISO timestamp when execution finished.
            truncation_limit: Maximum characters for stdout/stderr.

        Returns:
            ExecutionEvidence with truncated outputs and computed hash.
        """
        from .evidence import ExecutionEvidence

        return ExecutionEvidence.from_execution_result(
            self, started_at, finished_at, truncation_limit
        )


if TYPE_CHECKING:
    from .evidence import ExecutionEvidence


@dataclass
class TaskDefinition:
    """A task to be executed by run_task.sh.

    Agents generate these; the ExecutionGateway runs them.
    """

    task_type: str
    args: list[str]
    working_dir: Optional[str] = None
    timeout_seconds: int = 300
    description: str = ""

    def to_args(self) -> list[str]:
        """Convert to command-line arguments for run_task.sh."""
        result = [self.task_type, *self.args]
        if self.working_dir:
            result = ["--workdir", self.working_dir, *result]
        if self.timeout_seconds != 300:
            result = ["--timeout", str(self.timeout_seconds), *result]
        return result


class ExecutionGateway:
    """Single boundary for all code execution, via run_task.sh in WSL.

    This gateway ensures:
    - All execution happens in WSL (Ubuntu)
    - Spark runs in local mode
    - Python uses virtual environments
    - No Windows-native commands
    - No mouse/keyboard control
    - No global software installation

    Phase 7.D ENFORCEMENT:
    - Source binding: When source_context is set, pipelines may ONLY
      read from selected SourcePools
    - Source pools are mounted read-only
    - Execution MAY NOT access arbitrary paths or network resources
    - All source usage is logged and attributable

    Example:
        ```python
        gateway = ExecutionGateway(odibi_root="d:/odibi")

        # Bind sources (Phase 7.D)
        gateway.bind_sources(source_context)

        # Run a pipeline (will enforce source bounds)
        result = gateway.run_task(TaskDefinition(
            task_type="pipeline",
            args=["run", "my_pipeline.yaml"],
            description="Run the main pipeline",
        ))

        # Validate config
        result = gateway.run_task(TaskDefinition(
            task_type="validate",
            args=["config.yaml"],
        ))

        # Run tests
        result = gateway.run_task(TaskDefinition(
            task_type="pytest",
            args=["tests/", "-v"],
        ))
        ```
    """

    VALID_TASK_TYPES = frozenset(
        [
            "pipeline",
            "validate",
            "pytest",
            "ruff",
            "black",
            "mypy",
            "odibi",
            "spark-submit",
            "python",
            "env-check",
        ]
    )

    def __init__(self, odibi_root: str, wsl_distro: Optional[str] = None):
        """Initialize the execution gateway.

        Args:
            odibi_root: Path to Odibi repository root.
            wsl_distro: Optional WSL distribution name. If None, uses default.
        """
        self.odibi_root = odibi_root
        self.wsl_distro = wsl_distro
        self._script_path = self._get_wsl_script_path()

        # Phase 7.D: Source binding
        self._source_context: Optional["ExecutionSourceContext"] = None
        self._source_guard: Optional["SourceBindingGuard"] = None
        self._source_enforcement_enabled: bool = False

        # Phase 9.D: Bound source root for pipeline environment
        self._bound_source_root: Optional[str] = None
        self._artifacts_root: str = os.path.join(odibi_root, ".odibi", "artifacts")

    def _get_wsl_script_path(self) -> str:
        """Convert Windows path to WSL path for run_task.sh."""
        return self._to_wsl_path(f"{self.odibi_root}/scripts/run_task.sh")

    def _to_wsl_path(self, windows_path: str) -> str:
        """Convert a Windows path to WSL path format.

        Args:
            windows_path: Path in Windows format (e.g., D:/odibi/file.yaml)

        Returns:
            Path in WSL format (e.g., /mnt/d/odibi/file.yaml)
        """
        path = windows_path.replace("\\", "/")
        while "//" in path:
            path = path.replace("//", "/")
        if len(path) > 1 and path[1] == ":":
            drive = path[0].lower()
            wsl_path = f"/mnt/{drive}{path[2:]}"
        else:
            wsl_path = path
        return wsl_path

    def _build_wsl_command(
        self, task: TaskDefinition, env_vars: Optional[Dict[str, str]] = None
    ) -> list[str]:
        """Build the WSL command to execute.

        Args:
            task: The task definition to run.
            env_vars: Optional dict of environment variables to export in WSL.
                      These are explicitly passed since Windows env vars don't
                      automatically propagate to WSL.

        Returns:
            Command list suitable for subprocess.run()
        """
        cmd = ["wsl"]
        if self.wsl_distro:
            cmd.extend(["-d", self.wsl_distro])

        # Build env export prefix - ALWAYS export required env vars
        # Use stdbuf -oL -eL to force line buffering for BOTH stdout and stderr
        # This is critical for WSL→Windows pipe buffering issues
        args_str = " ".join(shlex.quote(a) for a in task.to_args())

        # Export key environment variables in WSL shell
        # Only export Odibi-specific vars to avoid shell injection
        safe_var_names = (
            "BOUND_SOURCE_ROOT",
            "ARTIFACTS_ROOT",
            "ODIBI_ROOT",
            "PYTHONUNBUFFERED",
        )
        safe_vars = {k: v for k, v in (env_vars or {}).items() if k in safe_var_names}

        # Build bash command with env exports and stdbuf
        script_with_args = f"stdbuf -oL -eL {self._script_path} {args_str}"
        if safe_vars:
            export_stmts = " && ".join(f'export {k}="{v}"' for k, v in safe_vars.items())
            bash_script = f"{export_stmts} && {script_with_args}"
        else:
            bash_script = script_with_args

        cmd.extend(["bash", "-c", bash_script])
        return cmd

    # ============================================
    # Phase 7.D: Source Binding
    # ============================================

    def bind_sources(self, context: "ExecutionSourceContext") -> None:
        """Bind source context for execution enforcement.

        Once bound, all pipeline executions will be validated against
        the source context. Pipelines referencing paths outside the
        bound pools will fail.

        Phase 9.D: Also sets BOUND_SOURCE_ROOT for pipeline environment.

        Args:
            context: The execution source context with mounted pools

        Note:
            This is idempotent - calling multiple times with same context
            has no effect. Calling with different context replaces binding.
        """
        from .source_binding import SourceBindingGuard

        self._source_context = context
        self._source_guard = SourceBindingGuard(context)
        self._source_enforcement_enabled = True

        # Phase 9.D: Set bound source root for environment injection
        self._bound_source_root = context.source_cache_root

    def unbind_sources(self) -> None:
        """Remove source binding (for testing only).

        WARNING: This disables source enforcement. Should only be used
        in tests or when source binding is not applicable.
        """
        self._source_context = None
        self._source_guard = None
        self._source_enforcement_enabled = False
        self._bound_source_root = None

    def get_source_context(self) -> Optional["ExecutionSourceContext"]:
        """Get the current source context.

        Returns:
            The bound ExecutionSourceContext or None
        """
        return self._source_context

    def is_source_bound(self) -> bool:
        """Check if sources are bound for enforcement.

        Returns:
            True if source enforcement is active
        """
        return self._source_enforcement_enabled and self._source_context is not None

    def validate_source_path(self, path: str) -> Optional[str]:
        """Validate that a path is within bound source pools.

        Args:
            path: Absolute path to validate

        Returns:
            The pool_id if valid, None if outside pools or unbound

        Note:
            Does not raise - use enforce_source_path for strict checking.
        """
        if not self.is_source_bound():
            return None
        return self._source_context.validate_path(path)

    def enforce_source_path(self, path: str, access_type: str = "read") -> str:
        """Enforce that a path is within bound source pools.

        Args:
            path: Absolute path to validate
            access_type: Type of access ("read", "list", "stat")

        Returns:
            The pool_id containing this path

        Raises:
            SourceViolationError: If path is outside all pools
            RuntimeError: If sources are not bound
        """
        if not self.is_source_bound():
            raise RuntimeError(
                "Source enforcement called but no sources are bound. "
                "Call bind_sources() first or disable enforcement."
            )
        return self._source_context.enforce_path(path, access_type)

    def get_source_violations(self) -> List:
        """Get all source violations detected during execution.

        Returns:
            List of SourceViolationError instances
        """
        if self._source_guard:
            return self._source_guard.get_violations()
        return []

    def has_source_violations(self) -> bool:
        """Check if any source violations were detected.

        Returns:
            True if violations exist
        """
        return self._source_guard.has_violations() if self._source_guard else False

    # ============================================
    # Phase 9.D: Environment Injection
    # ============================================

    def get_bound_source_root(self) -> Optional[str]:
        """Get the bound source root path.

        Returns:
            Absolute path to bound source root or None if not bound
        """
        return self._bound_source_root

    def get_artifacts_root(self) -> str:
        """Get the artifacts root path.

        Returns:
            Absolute path to artifacts directory
        """
        return self._artifacts_root

    def set_bound_source_root(self, path: str) -> None:
        """Explicitly set the bound source root path.

        Phase 9.D: Called by SourceBinder to set the cycle-specific
        binding root path.

        Args:
            path: Absolute path to the bound source root
        """
        self._bound_source_root = path

    def _build_execution_environment(self) -> Dict[str, str]:
        """Build environment variables for pipeline execution.

        Phase 9.D: Injects BOUND_SOURCE_ROOT and ARTIFACTS_ROOT
        into the execution environment.

        BOUND_SOURCE_ROOT is set to:
        1. The explicitly bound source root (from bind_sources), or
        2. The default source_cache path (.odibi/source_cache/) as fallback

        This ensures pipelines can always reference ${BOUND_SOURCE_ROOT}
        even when no explicit source binding has occurred.

        Returns:
            Dict of environment variables to pass to subprocess
        """
        # Start with current environment
        env = os.environ.copy()

        # Determine bound source root: explicit binding or default fallback
        source_root = self._bound_source_root
        if not source_root:
            # Fallback to default source_cache path for unbound execution
            source_root = os.path.join(self.odibi_root, ".odibi", "source_cache")

        # Convert to WSL path for cross-platform compatibility
        wsl_path = self._to_wsl_path(source_root)
        env["BOUND_SOURCE_ROOT"] = wsl_path
        # Also provide Windows path for native execution
        env["BOUND_SOURCE_ROOT_WIN"] = source_root

        # Inject artifacts root
        wsl_artifacts = self._to_wsl_path(self._artifacts_root)
        env["ARTIFACTS_ROOT"] = wsl_artifacts
        env["ARTIFACTS_ROOT_WIN"] = self._artifacts_root

        # Inject odibi root for reference
        env["ODIBI_ROOT"] = self._to_wsl_path(self.odibi_root)
        env["ODIBI_ROOT_WIN"] = self.odibi_root

        # Force unbuffered output for real-time streaming
        env["PYTHONUNBUFFERED"] = "1"

        return env

    def validate_task(self, task: TaskDefinition) -> Optional[str]:
        """Validate a task before execution.

        Args:
            task: The task to validate.

        Returns:
            Error message if invalid, None if valid.
        """
        if task.task_type not in self.VALID_TASK_TYPES:
            valid = sorted(self.VALID_TASK_TYPES)
            return f"Invalid task type: {task.task_type}. Valid types: {valid}"

        if task.timeout_seconds < 1 or task.timeout_seconds > 3600:
            return f"Timeout must be between 1 and 3600 seconds, got {task.timeout_seconds}"

        return None

    def run_task(
        self,
        task: TaskDefinition,
        agent_permissions: Optional["AgentPermissions"] = None,
        agent_role: Optional["AgentRole"] = None,
    ) -> ExecutionResult:
        """Execute a task via run_task.sh in WSL.

        Phase 9.D: Now injects BOUND_SOURCE_ROOT and ARTIFACTS_ROOT
        into the execution environment.

        Args:
            task: The task definition to execute.
            agent_permissions: Permissions of the calling agent (REQUIRED for enforcement).
            agent_role: Role of the calling agent (REQUIRED for enforcement).

        Returns:
            ExecutionResult with stdout, stderr, and exit code.

        Raises:
            PermissionDeniedError: If agent lacks can_execute_tasks permission.
            ValueError: If the task is invalid.
        """
        if agent_permissions is not None and agent_role is not None:
            PermissionEnforcer.check_execute_permission(agent_permissions, agent_role)
        elif agent_permissions is None and agent_role is None:
            raise PermissionDeniedError(
                "ExecutionGateway.run_task() requires agent_permissions and agent_role. "
                "This is a Phase 0 enforcement requirement."
            )
        else:
            raise PermissionDeniedError(
                "Both agent_permissions and agent_role must be provided together."
            )

        error = self.validate_task(task)
        if error:
            return ExecutionResult(
                stdout="",
                stderr=error,
                exit_code=-1,
                command=str(task.to_args()),
                success=False,
            )

        # Phase 9.D: Build environment with bound source root
        exec_env = self._build_execution_environment()

        # Pass env vars to WSL command (Windows env doesn't propagate to WSL)
        cmd = self._build_wsl_command(task, env_vars=exec_env)
        cmd_str = " ".join(cmd)

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=task.timeout_seconds,
                cwd=self.odibi_root,
                env=exec_env,
            )
            return ExecutionResult(
                stdout=proc.stdout,
                stderr=proc.stderr,
                exit_code=proc.returncode,
                command=cmd_str,
                success=proc.returncode == 0,
            )
        except subprocess.TimeoutExpired:
            return ExecutionResult(
                stdout="",
                stderr=f"Task timed out after {task.timeout_seconds} seconds",
                exit_code=-2,
                command=cmd_str,
                success=False,
            )
        except FileNotFoundError:
            return ExecutionResult(
                stdout="",
                stderr="WSL or run_task.sh not found. Ensure WSL is installed.",
                exit_code=-3,
                command=cmd_str,
                success=False,
            )
        except Exception as e:
            return ExecutionResult(
                stdout="",
                stderr=str(e),
                exit_code=-4,
                command=cmd_str,
                success=False,
            )

    def run_simple(
        self,
        task_type: str,
        *args: str,
        timeout: int = 300,
        agent_permissions: Optional["AgentPermissions"] = None,
        agent_role: Optional["AgentRole"] = None,
    ) -> ExecutionResult:
        """Convenience method for simple task execution.

        Args:
            task_type: The type of task to run.
            *args: Arguments for the task.
            timeout: Timeout in seconds.
            agent_permissions: Permissions of the calling agent (REQUIRED).
            agent_role: Role of the calling agent (REQUIRED).

        Returns:
            ExecutionResult.
        """
        task = TaskDefinition(
            task_type=task_type,
            args=list(args),
            timeout_seconds=timeout,
        )
        return self.run_task(task, agent_permissions, agent_role)

    def check_environment(
        self,
        agent_permissions: Optional["AgentPermissions"] = None,
        agent_role: Optional["AgentRole"] = None,
    ) -> ExecutionResult:
        """Run environment validation checks.

        Args:
            agent_permissions: Permissions of the calling agent (REQUIRED).
            agent_role: Role of the calling agent (REQUIRED).

        Returns:
            ExecutionResult with environment status.
        """
        return self.run_simple(
            "env-check", agent_permissions=agent_permissions, agent_role=agent_role
        )

    def run_pipeline(
        self,
        config_path: str,
        dry_run: bool = False,
        agent_permissions: Optional["AgentPermissions"] = None,
        agent_role: Optional["AgentRole"] = None,
    ) -> ExecutionResult:
        """Run an Odibi pipeline.

        Args:
            config_path: Path to the pipeline YAML config (Windows or WSL format).
            dry_run: If True, validate without executing.
            agent_permissions: Permissions of the calling agent (REQUIRED).
            agent_role: Role of the calling agent (REQUIRED).

        Returns:
            ExecutionResult.
        """
        wsl_config_path = self._to_wsl_path(config_path)
        args = ["run", wsl_config_path]
        if dry_run:
            args.append("--dry-run")
        return self.run_simple(
            "pipeline", *args, agent_permissions=agent_permissions, agent_role=agent_role
        )

    def validate_config(
        self,
        config_path: str,
        agent_permissions: Optional["AgentPermissions"] = None,
        agent_role: Optional["AgentRole"] = None,
    ) -> ExecutionResult:
        """Validate an Odibi configuration file.

        Args:
            config_path: Path to the config file (Windows or WSL format).
            agent_permissions: Permissions of the calling agent (REQUIRED).
            agent_role: Role of the calling agent (REQUIRED).

        Returns:
            ExecutionResult with validation results.
        """
        wsl_config_path = self._to_wsl_path(config_path)
        return self.run_simple(
            "validate",
            wsl_config_path,
            agent_permissions=agent_permissions,
            agent_role=agent_role,
        )

    def run_tests(
        self,
        test_path: str = "tests/",
        verbose: bool = True,
        agent_permissions: Optional["AgentPermissions"] = None,
        agent_role: Optional["AgentRole"] = None,
    ) -> ExecutionResult:
        """Run pytest tests.

        Args:
            test_path: Path to tests directory or file.
            verbose: If True, use verbose output.
            agent_permissions: Permissions of the calling agent (REQUIRED).
            agent_role: Role of the calling agent (REQUIRED).

        Returns:
            ExecutionResult.
        """
        args = [test_path]
        if verbose:
            args.append("-v")
        return self.run_simple(
            "pytest",
            *args,
            timeout=600,
            agent_permissions=agent_permissions,
            agent_role=agent_role,
        )

    def run_linter(
        self,
        path: str = ".",
        agent_permissions: Optional["AgentPermissions"] = None,
        agent_role: Optional["AgentRole"] = None,
    ) -> ExecutionResult:
        """Run ruff linter.

        Args:
            path: Path to lint.
            agent_permissions: Permissions of the calling agent (REQUIRED).
            agent_role: Role of the calling agent (REQUIRED).

        Returns:
            ExecutionResult.
        """
        return self.run_simple(
            "ruff",
            "check",
            path,
            agent_permissions=agent_permissions,
            agent_role=agent_role,
        )

    def run_formatter(
        self,
        path: str = ".",
        check_only: bool = True,
        agent_permissions: Optional["AgentPermissions"] = None,
        agent_role: Optional["AgentRole"] = None,
    ) -> ExecutionResult:
        """Run black formatter.

        Args:
            path: Path to format.
            check_only: If True, only check without modifying.
            agent_permissions: Permissions of the calling agent (REQUIRED).
            agent_role: Role of the calling agent (REQUIRED).

        Returns:
            ExecutionResult.
        """
        args = [path]
        if check_only:
            args.append("--check")
        return self.run_simple(
            "black", *args, agent_permissions=agent_permissions, agent_role=agent_role
        )

    def run_task_streaming(
        self,
        task: TaskDefinition,
        agent_permissions: Optional["AgentPermissions"] = None,
        agent_role: Optional["AgentRole"] = None,
        on_output: Optional[StreamOutputCallback] = None,
    ) -> ExecutionResult:
        """Execute a task with real-time output streaming.

        Unlike run_task, this method streams stdout/stderr line-by-line
        to the provided callback, enabling real-time UI updates.

        Args:
            task: The task definition to execute.
            agent_permissions: Permissions of the calling agent (REQUIRED).
            agent_role: Role of the calling agent (REQUIRED).
            on_output: Callback invoked for each line of output.
                       Signature: (line: str, is_stderr: bool) -> None

        Returns:
            ExecutionResult with complete stdout, stderr, and exit code.
        """
        if agent_permissions is not None and agent_role is not None:
            PermissionEnforcer.check_execute_permission(agent_permissions, agent_role)
        elif agent_permissions is None and agent_role is None:
            raise PermissionDeniedError(
                "ExecutionGateway.run_task_streaming() requires agent_permissions and agent_role."
            )
        else:
            raise PermissionDeniedError(
                "Both agent_permissions and agent_role must be provided together."
            )

        error = self.validate_task(task)
        if error:
            return ExecutionResult(
                stdout="",
                stderr=error,
                exit_code=-1,
                command=str(task.to_args()),
                success=False,
            )

        exec_env = self._build_execution_environment()
        cmd = self._build_wsl_command(task, env_vars=exec_env)
        cmd_str = " ".join(cmd)

        stdout_lines: list[str] = []
        stderr_lines: list[str] = []

        logger.debug("Starting WSL command: %s", cmd_str)

        # Decouple callback execution from stream reading to prevent backpressure
        # If on_output is slow (UI updates, network), it won't block the subprocess
        output_queue: "queue.Queue[Optional[Tuple[str, bool]]]" = queue.Queue()
        callback_error: List[Exception] = []

        def callback_worker():
            """Process output lines in a separate thread to avoid blocking subprocess."""
            while True:
                try:
                    item = output_queue.get(timeout=1.0)
                    if item is None:  # Sentinel to stop
                        break
                    line, is_stderr = item
                    if on_output:
                        try:
                            on_output(line, is_stderr)
                        except Exception as e:
                            callback_error.append(e)
                            logger.warning("Callback error (continuing): %s", e)
                except queue.Empty:
                    continue
                except Exception:
                    break

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                text=True,
                encoding="utf-8",
                errors="replace",
                cwd=self.odibi_root,
                env=exec_env,
                bufsize=1,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
            )

            # Start callback worker if we have a callback
            worker_thread = None
            if on_output:
                worker_thread = threading.Thread(target=callback_worker, daemon=True)
                worker_thread.start()

            def read_stream(stream, is_stderr: bool, lines_list: list):
                """Read from stream and queue lines for callback (non-blocking)."""
                try:
                    for line in iter(stream.readline, ""):
                        lines_list.append(line)
                        if on_output:
                            output_queue.put((line, is_stderr))
                except Exception as e:
                    logger.warning("Stream read error: %s", e)
                finally:
                    try:
                        stream.close()
                    except Exception:
                        pass

            stdout_thread = threading.Thread(
                target=read_stream,
                args=(proc.stdout, False, stdout_lines),
                daemon=True,
            )
            stderr_thread = threading.Thread(
                target=read_stream,
                args=(proc.stderr, True, stderr_lines),
                daemon=True,
            )

            stdout_thread.start()
            stderr_thread.start()

            try:
                logger.debug("Waiting for process (timeout=%s)", task.timeout_seconds)
                proc.wait(timeout=task.timeout_seconds)
                logger.debug("Process exited with code %s", proc.returncode)
            except subprocess.TimeoutExpired:
                logger.warning("Process timeout after %s seconds, killing", task.timeout_seconds)
                proc.kill()
                stdout_thread.join(timeout=1)
                stderr_thread.join(timeout=1)
                if worker_thread:
                    output_queue.put(None)
                    worker_thread.join(timeout=1)
                timeout_msg = f"\nTask timed out after {task.timeout_seconds} seconds"
                return ExecutionResult(
                    stdout="".join(stdout_lines),
                    stderr="".join(stderr_lines) + timeout_msg,
                    exit_code=-2,
                    command=cmd_str,
                    success=False,
                )

            # Wait for stream readers to finish
            stdout_thread.join(timeout=5)
            stderr_thread.join(timeout=5)

            # Signal callback worker to stop and wait for it
            if worker_thread:
                output_queue.put(None)
                worker_thread.join(timeout=2)

            return ExecutionResult(
                stdout="".join(stdout_lines),
                stderr="".join(stderr_lines),
                exit_code=proc.returncode,
                command=cmd_str,
                success=proc.returncode == 0,
            )

        except FileNotFoundError:
            return ExecutionResult(
                stdout="",
                stderr="WSL or run_task.sh not found. Ensure WSL is installed.",
                exit_code=-3,
                command=cmd_str,
                success=False,
            )
        except Exception as e:
            logger.exception("Unexpected error in run_task_streaming")
            return ExecutionResult(
                stdout="".join(stdout_lines),
                stderr=f"{chr(10).join(stderr_lines)}\nInternal error: {e}",
                exit_code=-4,
                command=cmd_str,
                success=False,
            )
