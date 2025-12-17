"""Tests for ExperimentRunner.

Phase E2: Basic Experiment Execution

These tests verify that:
1. Experiments run successfully in sandboxes
2. stdout/stderr are captured correctly
3. Non-zero exit codes are handled
4. Timeout enforcement fails hard
5. Execution outside sandbox paths is rejected
"""

import sys
from pathlib import Path

import pytest

from odibi.agents.explorer.clone_manager import RepoCloneManager
from odibi.agents.explorer.experiment_runner import (
    CommandResult,
    ExperimentResult,
    ExperimentRunner,
    ExperimentSpec,
    ExperimentStatus,
    ExperimentTimeoutError,
    SandboxNotFoundError,
    SandboxPathViolation,
)


class TestExperimentRunnerInit:
    """Tests for ExperimentRunner initialization."""

    def test_init_with_clone_manager(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        runner = ExperimentRunner(manager)

        assert runner.clone_manager is manager
        assert runner.get_results() == []


class TestSuccessfulExecution:
    """Tests for successful command execution in sandbox."""

    @pytest.fixture
    def runner_with_sandbox(self, tmp_path: Path):
        """Create a runner with an active sandbox."""
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        source_repo = tmp_path / "source"
        source_repo.mkdir()
        (source_repo / "file.txt").write_text("hello world")

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        sandbox_info = manager.create_sandbox(source_repo)
        runner = ExperimentRunner(manager)

        return runner, sandbox_info

    def test_simple_command_succeeds(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        spec = ExperimentSpec(
            name="test_echo",
            description="Test echo command",
            commands=["echo hello"],
            timeout_seconds=30,
        )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert result.status == ExperimentStatus.COMPLETED
        assert result.exit_code == 0
        assert "hello" in result.stdout

    def test_returns_experiment_result(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        spec = ExperimentSpec(
            name="test_spec",
            description="Test description",
            commands=["echo test"],
            timeout_seconds=30,
        )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert isinstance(result, ExperimentResult)
        assert result.experiment_id.startswith("exp_")
        assert result.sandbox_id == sandbox_info.sandbox_id
        assert result.started_at is not None
        assert result.ended_at is not None

    def test_result_stored_in_runner(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        spec = ExperimentSpec(
            name="stored_test",
            description="Test storage",
            commands=["echo stored"],
            timeout_seconds=30,
        )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert result in runner.get_results()
        assert runner.get_result(result.experiment_id) == result

    def test_multiple_commands_run_sequentially(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        spec = ExperimentSpec(
            name="multi_command",
            description="Multiple commands",
            commands=["echo first", "echo second", "echo third"],
            timeout_seconds=30,
        )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert result.status == ExperimentStatus.COMPLETED
        assert "first" in result.stdout
        assert "second" in result.stdout
        assert "third" in result.stdout

    def test_command_runs_in_sandbox_directory(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        if sys.platform == "win32":
            spec = ExperimentSpec(
                name="pwd_test",
                description="Check working directory",
                commands=["cd"],
                timeout_seconds=30,
            )
        else:
            spec = ExperimentSpec(
                name="pwd_test",
                description="Check working directory",
                commands=["pwd"],
                timeout_seconds=30,
            )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert result.status == ExperimentStatus.COMPLETED
        sandbox_path_str = str(sandbox_info.sandbox_path).lower().replace("/", "\\")
        stdout_normalized = result.stdout.strip().lower().replace("/", "\\")
        assert sandbox_path_str in stdout_normalized or stdout_normalized in sandbox_path_str


class TestStdoutStderrCapture:
    """Tests for stdout/stderr capture."""

    @pytest.fixture
    def runner_with_sandbox(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        source_repo = tmp_path / "source"
        source_repo.mkdir()

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        sandbox_info = manager.create_sandbox(source_repo)
        runner = ExperimentRunner(manager)

        return runner, sandbox_info

    def test_captures_stdout(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        spec = ExperimentSpec(
            name="stdout_test",
            description="Test stdout",
            commands=["echo stdout_content"],
            timeout_seconds=30,
        )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert "stdout_content" in result.stdout

    def test_captures_stderr(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        if sys.platform == "win32":
            spec = ExperimentSpec(
                name="stderr_test",
                description="Test stderr",
                commands=["echo stderr_content 1>&2"],
                timeout_seconds=30,
            )
        else:
            spec = ExperimentSpec(
                name="stderr_test",
                description="Test stderr",
                commands=["echo stderr_content >&2"],
                timeout_seconds=30,
            )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert "stderr_content" in result.stderr

    def test_captures_both_stdout_and_stderr(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        if sys.platform == "win32":
            spec = ExperimentSpec(
                name="both_test",
                description="Test both streams",
                commands=["echo out_msg && echo err_msg 1>&2"],
                timeout_seconds=30,
            )
        else:
            spec = ExperimentSpec(
                name="both_test",
                description="Test both streams",
                commands=["echo out_msg && echo err_msg >&2"],
                timeout_seconds=30,
            )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert "out_msg" in result.stdout
        assert "err_msg" in result.stderr

    def test_captures_multiline_output(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        if sys.platform == "win32":
            spec = ExperimentSpec(
                name="multiline_test",
                description="Test multiline",
                commands=["echo line1 && echo line2 && echo line3"],
                timeout_seconds=30,
            )
        else:
            spec = ExperimentSpec(
                name="multiline_test",
                description="Test multiline",
                commands=["echo line1; echo line2; echo line3"],
                timeout_seconds=30,
            )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert "line1" in result.stdout
        assert "line2" in result.stdout
        assert "line3" in result.stdout


class TestNonZeroExitCodes:
    """Tests for non-zero exit code handling."""

    @pytest.fixture
    def runner_with_sandbox(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        source_repo = tmp_path / "source"
        source_repo.mkdir()

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        sandbox_info = manager.create_sandbox(source_repo)
        runner = ExperimentRunner(manager)

        return runner, sandbox_info

    def test_captures_nonzero_exit_code(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        if sys.platform == "win32":
            spec = ExperimentSpec(
                name="exit_code_test",
                description="Test exit code",
                commands=["exit 42"],
                timeout_seconds=30,
            )
        else:
            spec = ExperimentSpec(
                name="exit_code_test",
                description="Test exit code",
                commands=["exit 42"],
                timeout_seconds=30,
            )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert result.status == ExperimentStatus.FAILED
        assert result.exit_code == 42

    def test_stops_on_first_failure(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        if sys.platform == "win32":
            spec = ExperimentSpec(
                name="stop_on_fail",
                description="Test stop on failure",
                commands=["echo before", "exit 1", "echo after"],
                timeout_seconds=30,
            )
        else:
            spec = ExperimentSpec(
                name="stop_on_fail",
                description="Test stop on failure",
                commands=["echo before", "exit 1", "echo after"],
                timeout_seconds=30,
            )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert result.status == ExperimentStatus.FAILED
        assert "before" in result.stdout
        assert "after" not in result.stdout

    def test_failed_command_status(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        spec = ExperimentSpec(
            name="fail_status",
            description="Test failure status",
            commands=["nonexistent_command_xyz123"],
            timeout_seconds=30,
        )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert result.status == ExperimentStatus.FAILED
        assert result.exit_code != 0


class TestTimeoutEnforcement:
    """Tests for timeout enforcement (hard failure)."""

    @pytest.fixture
    def runner_with_sandbox(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        source_repo = tmp_path / "source"
        source_repo.mkdir()

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        sandbox_info = manager.create_sandbox(source_repo)
        runner = ExperimentRunner(manager)

        return runner, sandbox_info

    def test_timeout_raises_error(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        if sys.platform == "win32":
            spec = ExperimentSpec(
                name="timeout_test",
                description="Test timeout",
                commands=["ping -n 10 127.0.0.1"],
                timeout_seconds=1,
            )
        else:
            spec = ExperimentSpec(
                name="timeout_test",
                description="Test timeout",
                commands=["sleep 10"],
                timeout_seconds=1,
            )

        with pytest.raises(ExperimentTimeoutError) as exc_info:
            runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert exc_info.value.timeout_seconds == 1
        assert "EXPERIMENT TIMEOUT" in str(exc_info.value)

    def test_timeout_is_hard_failure(self, runner_with_sandbox):
        """Timeout must raise exception, not just set status."""
        runner, sandbox_info = runner_with_sandbox

        if sys.platform == "win32":
            spec = ExperimentSpec(
                name="hard_timeout",
                description="Test hard timeout",
                commands=["ping -n 10 127.0.0.1"],
                timeout_seconds=1,
            )
        else:
            spec = ExperimentSpec(
                name="hard_timeout",
                description="Test hard timeout",
                commands=["sleep 10"],
                timeout_seconds=1,
            )

        raised = False
        try:
            runner.run_experiment(spec, sandbox_info.sandbox_id)
        except ExperimentTimeoutError:
            raised = True

        assert raised, "Timeout must raise ExperimentTimeoutError, not return normally"

    def test_timeout_includes_experiment_id(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        if sys.platform == "win32":
            spec = ExperimentSpec(
                name="timeout_id",
                description="Test timeout ID",
                commands=["ping -n 10 127.0.0.1"],
                timeout_seconds=1,
            )
        else:
            spec = ExperimentSpec(
                name="timeout_id",
                description="Test timeout ID",
                commands=["sleep 10"],
                timeout_seconds=1,
            )

        with pytest.raises(ExperimentTimeoutError) as exc_info:
            runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert exc_info.value.experiment_id.startswith("exp_")


class TestSandboxValidation:
    """Tests for sandbox path validation before execution."""

    def test_rejects_unknown_sandbox_id(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        runner = ExperimentRunner(manager)

        spec = ExperimentSpec(
            name="unknown_sandbox",
            description="Test unknown sandbox",
            commands=["echo test"],
            timeout_seconds=30,
        )

        with pytest.raises(SandboxNotFoundError) as exc_info:
            runner.run_experiment(spec, "nonexistent_id")

        assert "nonexistent_id" in str(exc_info.value)

    def test_rejects_destroyed_sandbox(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        source_repo = tmp_path / "source"
        source_repo.mkdir()

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        sandbox_info = manager.create_sandbox(source_repo)
        sandbox_id = sandbox_info.sandbox_id

        manager.destroy_sandbox(sandbox_id)

        runner = ExperimentRunner(manager)

        spec = ExperimentSpec(
            name="destroyed_sandbox",
            description="Test destroyed sandbox",
            commands=["echo test"],
            timeout_seconds=30,
        )

        with pytest.raises(SandboxNotFoundError):
            runner.run_experiment(spec, sandbox_id)

    def test_rejects_manually_deleted_sandbox_dir(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        source_repo = tmp_path / "source"
        source_repo.mkdir()

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        sandbox_info = manager.create_sandbox(source_repo)

        import shutil

        shutil.rmtree(sandbox_info.sandbox_path)

        runner = ExperimentRunner(manager)

        spec = ExperimentSpec(
            name="deleted_dir",
            description="Test deleted directory",
            commands=["echo test"],
            timeout_seconds=30,
        )

        with pytest.raises(SandboxPathViolation) as exc_info:
            runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert "does not exist" in str(exc_info.value)


class TestExperimentMetadata:
    """Tests for experiment metadata handling."""

    @pytest.fixture
    def runner_with_sandbox(self, tmp_path: Path):
        sandbox_root = tmp_path / "sandboxes"
        trusted_repo = tmp_path / "odibi"
        trusted_repo.mkdir()

        source_repo = tmp_path / "source"
        source_repo.mkdir()

        manager = RepoCloneManager(sandbox_root, trusted_repo)
        sandbox_info = manager.create_sandbox(source_repo)
        runner = ExperimentRunner(manager)

        return runner, sandbox_info

    def test_result_includes_spec_metadata(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        spec = ExperimentSpec(
            name="meta_test",
            description="Test metadata capture",
            commands=["echo meta"],
            timeout_seconds=30,
        )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)

        metadata_dict = dict(result.metadata)
        assert metadata_dict.get("spec_name") == "meta_test"
        assert metadata_dict.get("spec_description") == "Test metadata capture"

    def test_result_to_dict(self, runner_with_sandbox):
        runner, sandbox_info = runner_with_sandbox

        spec = ExperimentSpec(
            name="dict_test",
            description="Test dict conversion",
            commands=["echo dict"],
            timeout_seconds=30,
        )

        result = runner.run_experiment(spec, sandbox_info.sandbox_id)
        result_dict = result.to_dict()

        assert result_dict["experiment_id"] == result.experiment_id
        assert result_dict["sandbox_id"] == sandbox_info.sandbox_id
        assert result_dict["status"] == "completed"
        assert "dict" in result_dict["stdout"]


class TestCommandResult:
    """Tests for CommandResult dataclass."""

    def test_command_result_fields(self):
        result = CommandResult(
            command="echo test",
            stdout="test output",
            stderr="error output",
            exit_code=0,
            timed_out=False,
        )

        assert result.command == "echo test"
        assert result.stdout == "test output"
        assert result.stderr == "error output"
        assert result.exit_code == 0
        assert result.timed_out is False

    def test_command_result_timeout_flag(self):
        result = CommandResult(
            command="sleep 100",
            stdout="",
            stderr="",
            exit_code=-1,
            timed_out=True,
        )

        assert result.timed_out is True
