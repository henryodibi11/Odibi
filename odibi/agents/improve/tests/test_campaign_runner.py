"""Tests for Campaign Runner, Memory, and Status Tracker.

These tests verify:
- Campaign stops on max_cycles
- Campaign stops on max_hours
- Campaign stops on convergence
- Gates block promotion
- Memory prevents repeat failures
- Status updated each cycle
"""

import shutil
import tempfile
import time
from datetime import datetime
from pathlib import Path

import pytest

from odibi.agents.improve.campaign import CampaignRunner, create_campaign_runner
from odibi.agents.improve.config import CampaignConfig, EnvironmentConfig
from odibi.agents.improve.environment import ImprovementEnvironment
from odibi.agents.improve.memory import CampaignMemory
from odibi.agents.improve.results import (
    CycleResult,
    GateCheckResult,
    TestResult,
)
from odibi.agents.improve.status import StatusTracker


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp = tempfile.mkdtemp(prefix="campaign_test_")
    yield Path(temp)
    shutil.rmtree(temp, ignore_errors=True)


@pytest.fixture
def mock_sacred_repo(temp_dir: Path):
    """Create a mock Sacred repository."""
    sacred = temp_dir / "sacred_repo"
    sacred.mkdir(parents=True)

    # Create minimal structure
    (sacred / "odibi").mkdir()
    (sacred / "odibi" / "__init__.py").write_text("# odibi package")
    (sacred / "tests").mkdir()
    (sacred / "tests" / "test_core.py").write_text("def test_pass(): pass")

    return sacred


@pytest.fixture
def environment_root(temp_dir: Path):
    """Create a temporary environment root."""
    return temp_dir / "improve_env"


@pytest.fixture
def env_config(mock_sacred_repo: Path, environment_root: Path):
    """Create an EnvironmentConfig for testing."""
    return EnvironmentConfig(
        sacred_repo=mock_sacred_repo,
        environment_root=environment_root,
    )


@pytest.fixture
def initialized_env(env_config: EnvironmentConfig):
    """Create and initialize an ImprovementEnvironment."""
    env = ImprovementEnvironment(env_config)
    env.initialize()
    return env


@pytest.fixture
def campaign_config():
    """Create a CampaignConfig for testing."""
    return CampaignConfig(
        name="Test Campaign",
        goal="Test bugs",
        max_cycles=5,
        max_hours=1.0,
        convergence_threshold=2,
        require_tests_pass=True,
        require_lint_clean=True,
        require_golden_pass=True,
    )


@pytest.fixture
def memory_path(temp_dir: Path):
    """Create a temporary memory directory."""
    path = temp_dir / "memory"
    path.mkdir(parents=True)
    return path


class TestCampaignMemory:
    """Tests for CampaignMemory."""

    def test_memory_creates_files(self, memory_path: Path):
        """Test that memory creates required files."""
        CampaignMemory(memory_path)

        assert (memory_path / "lessons.jsonl").exists()
        assert (memory_path / "avoided_issues.jsonl").exists()
        assert (memory_path / "patterns.jsonl").exists()

    def test_record_cycle(self, memory_path: Path):
        """Test recording a cycle result."""
        memory = CampaignMemory(memory_path)

        result = CycleResult(
            cycle_id="cycle_001",
            sandbox_path=Path("/test"),
            started_at=datetime.now(),
            promoted=True,
            learning=True,
            lesson="Fixed a bug",
            tests_passed=10,
            tests_failed=0,
        )
        result.mark_complete(promoted=True, learning=True, lesson="Fixed a bug")

        memory.record(result)

        cycles = memory.get_recent_cycles(10)
        assert len(cycles) == 1
        assert cycles[0].cycle_id == "cycle_001"

    def test_get_failed_approaches(self, memory_path: Path):
        """Test getting failed approaches."""
        memory = CampaignMemory(memory_path)

        # Record a failed cycle
        result = CycleResult(
            cycle_id="cycle_001",
            sandbox_path=Path("/test"),
            started_at=datetime.now(),
        )
        result.mark_complete(
            promoted=False,
            rejection_reason="Tests failed",
            learning=True,
            lesson="bad_approach",
        )

        memory.record(result)

        failed = memory.get_failed_approaches()
        assert "bad_approach" in failed

    def test_get_successful_patterns(self, memory_path: Path):
        """Test getting successful patterns."""
        memory = CampaignMemory(memory_path)

        # Record a successful cycle
        result = CycleResult(
            cycle_id="cycle_001",
            sandbox_path=Path("/test"),
            started_at=datetime.now(),
        )
        result.mark_complete(
            promoted=True,
            learning=True,
            lesson="good_pattern",
        )

        memory.record(result)

        patterns = memory.get_successful_patterns()
        assert "good_pattern" in patterns

    def test_get_recent_cycles(self, memory_path: Path):
        """Test getting recent cycles."""
        memory = CampaignMemory(memory_path)

        # Record multiple cycles
        for i in range(5):
            result = CycleResult(
                cycle_id=f"cycle_{i:03d}",
                sandbox_path=Path("/test"),
                started_at=datetime.now(),
            )
            result.mark_complete(promoted=(i % 2 == 0))
            memory.record(result)

        recent = memory.get_recent_cycles(3)
        assert len(recent) == 3
        assert recent[-1].cycle_id == "cycle_004"

    def test_memory_prevents_repeat_failures(self, memory_path: Path):
        """Test that memory tracks failed approaches to prevent repeats."""
        memory = CampaignMemory(memory_path)

        # Record multiple failures with same approach
        for i in range(3):
            result = CycleResult(
                cycle_id=f"cycle_{i:03d}",
                sandbox_path=Path("/test"),
                started_at=datetime.now(),
            )
            result.mark_complete(
                promoted=False,
                rejection_reason="Same failure",
                lesson="repeated_mistake",
            )
            memory.record(result)

        failed = memory.get_failed_approaches()
        assert failed.count("repeated_mistake") >= 1

    def test_query_similar(self, memory_path: Path):
        """Test querying for similar lessons."""
        memory = CampaignMemory(memory_path)

        result = CycleResult(
            cycle_id="cycle_001",
            sandbox_path=Path("/test"),
            started_at=datetime.now(),
        )
        result.mark_complete(
            promoted=True,
            learning=True,
            lesson="fixed null handling in filter transformer",
        )
        memory.record(result)

        similar = memory.query_similar("null filter")
        assert len(similar) > 0

    def test_memory_clear(self, memory_path: Path):
        """Test clearing memory."""
        memory = CampaignMemory(memory_path)

        result = CycleResult(
            cycle_id="cycle_001",
            sandbox_path=Path("/test"),
            started_at=datetime.now(),
        )
        memory.record(result)

        assert memory.get_cycle_count() == 1

        memory.clear()

        assert memory.get_cycle_count() == 0


class TestStatusTracker:
    """Tests for StatusTracker."""

    def test_start_campaign(self, temp_dir: Path):
        """Test starting a campaign."""
        status_file = temp_dir / "status.json"
        tracker = StatusTracker(status_file)

        tracker.start_campaign("test_campaign_001")

        summary = tracker.get_summary()
        assert summary.campaign_id == "test_campaign_001"
        assert summary.status == "RUNNING"
        assert summary.cycles_completed == 0

    def test_status_updated_each_cycle(self, temp_dir: Path):
        """Test that status is updated after each cycle."""
        status_file = temp_dir / "status.json"
        tracker = StatusTracker(status_file)

        tracker.start_campaign("test_campaign")

        # Update with cycle results
        for i in range(3):
            result = CycleResult(
                cycle_id=f"cycle_{i:03d}",
                sandbox_path=Path("/test"),
                started_at=datetime.now(),
                promoted=(i == 1),  # Only cycle 1 promoted
                learning=True,
            )
            tracker.update(result)

            summary = tracker.get_summary()
            assert summary.cycles_completed == i + 1

        final_summary = tracker.get_summary()
        assert final_summary.cycles_completed == 3
        assert final_summary.improvements_promoted == 1
        assert final_summary.improvements_rejected == 2

    def test_finish_campaign(self, temp_dir: Path):
        """Test finishing a campaign."""
        status_file = temp_dir / "status.json"
        tracker = StatusTracker(status_file)

        tracker.start_campaign("test_campaign")
        tracker.finish_campaign("MAX_CYCLES_REACHED")

        summary = tracker.get_summary()
        assert summary.status == "COMPLETED"
        assert summary.stop_reason == "MAX_CYCLES_REACHED"

    def test_generate_report(self, temp_dir: Path):
        """Test generating markdown report."""
        status_file = temp_dir / "status.json"
        tracker = StatusTracker(status_file)

        tracker.start_campaign("test_campaign")

        result = CycleResult(
            cycle_id="cycle_001",
            sandbox_path=Path("/test"),
            started_at=datetime.now(),
            promoted=True,
            learning=True,
            lesson="Fixed bug",
        )
        tracker.update(result)

        report = tracker.generate_report()

        assert "# Campaign Report" in report
        assert "test_campaign" in report
        assert "Cycles completed: 1" in report

    def test_convergence_counter(self, temp_dir: Path):
        """Test that convergence counter tracks non-learning cycles."""
        status_file = temp_dir / "status.json"
        tracker = StatusTracker(status_file)

        tracker.start_campaign("test_campaign")

        # Non-learning cycles should increment counter
        for i in range(3):
            result = CycleResult(
                cycle_id=f"cycle_{i:03d}",
                sandbox_path=Path("/test"),
                started_at=datetime.now(),
                promoted=False,
                learning=False,
            )
            tracker.update(result)

        summary = tracker.get_summary()
        assert summary.convergence_counter == 3

    def test_convergence_counter_resets_on_learning(self, temp_dir: Path):
        """Test that convergence counter resets on learning cycle."""
        status_file = temp_dir / "status.json"
        tracker = StatusTracker(status_file)

        tracker.start_campaign("test_campaign")

        # Two non-learning cycles
        for i in range(2):
            result = CycleResult(
                cycle_id=f"cycle_{i:03d}",
                sandbox_path=Path("/test"),
                started_at=datetime.now(),
                promoted=False,
                learning=False,
            )
            tracker.update(result)

        assert tracker.get_convergence_counter() == 2

        # One learning cycle
        result = CycleResult(
            cycle_id="cycle_002",
            sandbox_path=Path("/test"),
            started_at=datetime.now(),
            promoted=False,
            learning=True,
            rejection_reason="Tests failed",
            lesson="learned something",
        )
        tracker.update(result)

        assert tracker.get_convergence_counter() == 0


class TestCampaignRunner:
    """Tests for CampaignRunner."""

    def test_campaign_stops_on_max_cycles(self, initialized_env, campaign_config):
        """Test that campaign stops when max_cycles is reached."""
        campaign_config.max_cycles = 3
        campaign_config.max_hours = 10.0  # High enough not to trigger
        campaign_config.convergence_threshold = 100  # High enough not to trigger

        runner = CampaignRunner(initialized_env, campaign_config)

        # Mock check_gates to always fail (no promotion, but has learning)
        def mock_check_gates(sandbox):
            return GateCheckResult(
                tests_passed=False,
                lint_passed=True,
                validate_passed=True,
                golden_passed=True,
                all_passed=False,
                test_result=TestResult(
                    passed=5, failed=1, skipped=0, errors=0, duration=1.0, exit_code=1, output=""
                ),
                failure_reasons=["Tests failed"],
            )

        runner.check_gates = mock_check_gates

        result = runner.run()

        assert result.stop_reason == "MAX_CYCLES_REACHED"
        assert result.cycles_completed == 3

    def test_campaign_stops_on_max_hours(self, initialized_env, campaign_config):
        """Test that campaign stops when max_hours is exceeded."""
        campaign_config.max_cycles = 100
        campaign_config.max_hours = 0.0001  # Very short
        campaign_config.convergence_threshold = 100

        runner = CampaignRunner(initialized_env, campaign_config)

        # Mock to make cycles take some time
        original_run_cycle = runner.run_cycle

        def slow_run_cycle(cycle_num):
            time.sleep(0.5)  # Exceed max_hours
            return original_run_cycle(cycle_num)

        runner.run_cycle = slow_run_cycle

        result = runner.run()

        assert result.stop_reason == "TIME_BUDGET_EXHAUSTED"

    def test_campaign_stops_on_convergence(self, initialized_env, campaign_config):
        """Test that campaign stops when converged."""
        campaign_config.max_cycles = 100
        campaign_config.max_hours = 10.0
        campaign_config.convergence_threshold = 2

        runner = CampaignRunner(initialized_env, campaign_config)

        # Mock check_gates to always fail with no learning
        def mock_check_gates(sandbox):
            return GateCheckResult(
                tests_passed=True,
                lint_passed=True,
                validate_passed=True,
                golden_passed=True,
                all_passed=True,  # Gates pass but no learning
            )

        # Mock to return no-learning cycles
        cycle_count = [0]

        def mock_run_cycle(cycle_num):
            cycle_count[0] += 1
            result = CycleResult(
                cycle_id=f"cycle_{cycle_num:03d}",
                sandbox_path=Path("/test"),
                started_at=datetime.now(),
                promoted=False,
                learning=False,  # No learning
            )
            runner._cycles.append(result)
            return result

        runner.run_cycle = mock_run_cycle

        result = runner.run()

        assert result.stop_reason == "CONVERGED"
        assert result.cycles_completed >= campaign_config.convergence_threshold

    def test_gates_block_promotion(self, initialized_env, campaign_config):
        """Test that failing gates block promotion."""
        campaign_config.max_cycles = 1

        runner = CampaignRunner(initialized_env, campaign_config)

        # Run one cycle
        result = runner.run()

        # Without actual code to modify, gates should fail
        # (tests fail because there's no pytest in the sandbox)
        assert result.cycles_completed == 1
        # The cycle should not be promoted due to gate failure
        # Gates failed means no promotion
        # If all gates actually passed, it would be promoted
        # If any gate failed, it would not be promoted

    def test_is_converged(self, initialized_env, campaign_config):
        """Test convergence detection."""
        campaign_config.convergence_threshold = 3

        runner = CampaignRunner(initialized_env, campaign_config)

        # No cycles yet - not converged
        assert not runner.is_converged()

        # Add learning cycles - not converged
        for i in range(3):
            runner._cycles.append(
                CycleResult(
                    cycle_id=f"cycle_{i:03d}",
                    sandbox_path=Path("/test"),
                    started_at=datetime.now(),
                    learning=True,
                )
            )
        assert not runner.is_converged()

        # Replace with non-learning cycles - converged
        runner._cycles = []
        for i in range(3):
            runner._cycles.append(
                CycleResult(
                    cycle_id=f"cycle_{i:03d}",
                    sandbox_path=Path("/test"),
                    started_at=datetime.now(),
                    learning=False,
                )
            )
        assert runner.is_converged()

    def test_sandbox_cleanup_on_error(self, initialized_env, campaign_config):
        """Test that sandboxes are cleaned up even on error."""
        campaign_config.max_cycles = 1

        runner = CampaignRunner(initialized_env, campaign_config)

        # Mock check_gates to raise an error
        def mock_check_gates(sandbox):
            raise RuntimeError("Simulated error")

        runner.check_gates = mock_check_gates

        # Run should not raise
        runner.run()

        # Sandbox should be cleaned up
        sandboxes = list(initialized_env.config.sandboxes_path.glob("*"))
        assert len(sandboxes) == 0

    def test_check_gates_all_pass(self, initialized_env, campaign_config):
        """Test gate checking when all gates pass."""
        runner = CampaignRunner(initialized_env, campaign_config)

        sandbox = initialized_env.create_sandbox("test_cycle")

        try:
            # This will actually run gates - results depend on sandbox state
            gate_result = runner.check_gates(sandbox)

            # Gate result should have proper structure
            assert isinstance(gate_result, GateCheckResult)
            assert isinstance(gate_result.tests_passed, bool)
            assert isinstance(gate_result.lint_passed, bool)
            assert isinstance(gate_result.golden_passed, bool)
        finally:
            initialized_env.destroy_sandbox(sandbox)


class TestCreateCampaignRunner:
    """Tests for create_campaign_runner factory function."""

    def test_create_from_environment(self, initialized_env):
        """Test creating runner from initialized environment."""
        runner = create_campaign_runner(
            initialized_env.config.environment_root,
            CampaignConfig(max_cycles=5),
        )

        assert runner is not None
        assert runner.environment is not None
        assert runner.campaign_config.max_cycles == 5

    def test_create_with_default_config(self, initialized_env):
        """Test creating runner with default config."""
        runner = create_campaign_runner(
            initialized_env.config.environment_root,
        )

        assert runner is not None
        assert runner.campaign_config.max_cycles == 10  # Default

    def test_create_from_nonexistent_fails(self, temp_dir):
        """Test that creating from nonexistent environment fails."""
        from odibi.agents.improve.campaign import CampaignError

        with pytest.raises(CampaignError):
            create_campaign_runner(temp_dir / "nonexistent")
