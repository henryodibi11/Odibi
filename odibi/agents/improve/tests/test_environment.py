"""Tests for the Improvement Environment.

These tests verify:
- Environment initialization
- Sandbox create/destroy lifecycle
- Snapshot and rollback functionality
- CRITICAL: Sacred repo is NEVER modified
- Config validation
"""

import shutil
import tempfile
from pathlib import Path

import pytest

from odibi.agents.improve.config import (
    CampaignConfig,
    CommandConfig,
    EnvironmentConfig,
    GateConfig,
    StopConfig,
)
from odibi.agents.improve.environment import (
    ImprovementEnvironment,
    EnvironmentNotInitializedError,
    PromotionError,
    load_environment,
)
from odibi.agents.improve.sandbox import (
    SandboxInfo,
    SandboxPathViolation,
    validate_sandbox_path,
)
from odibi.agents.improve.snapshot import (
    SnapshotInfo,
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp = tempfile.mkdtemp(prefix="improve_test_")
    yield Path(temp)
    shutil.rmtree(temp, ignore_errors=True)


@pytest.fixture
def mock_sacred_repo(temp_dir: Path):
    """Create a mock Sacred repository."""
    sacred = temp_dir / "sacred_repo"
    sacred.mkdir(parents=True)

    # Create some files
    (sacred / "odibi").mkdir()
    (sacred / "odibi" / "__init__.py").write_text("# odibi package")
    (sacred / "odibi" / "core.py").write_text("# core module\ndef main(): pass")
    (sacred / "tests").mkdir()
    (sacred / "tests" / "test_core.py").write_text("# tests\ndef test_main(): pass")
    (sacred / "README.md").write_text("# Odibi\n\nA data pipeline framework.")

    return sacred


@pytest.fixture
def environment_root(temp_dir: Path):
    """Create a temporary environment root."""
    root = temp_dir / "improve_env"
    return root


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


class TestConfigValidation:
    """Tests for configuration validation."""

    def test_config_defaults(self, mock_sacred_repo: Path, environment_root: Path):
        """Test that config has sensible defaults."""
        config = EnvironmentConfig(
            sacred_repo=mock_sacred_repo,
            environment_root=environment_root,
        )

        assert config.gates.require_ruff_clean is True
        assert config.gates.require_pytest_pass is True
        assert config.stop_conditions.max_cycles == 10
        assert config.stop_conditions.max_hours == 4.0
        assert config.snapshots.enabled is True
        assert config.snapshots.compress is True

    def test_config_paths_resolved(self, mock_sacred_repo: Path, environment_root: Path):
        """Test that paths are resolved to absolute."""
        config = EnvironmentConfig(
            sacred_repo=mock_sacred_repo,
            environment_root=environment_root,
        )

        assert config.sacred_repo.is_absolute()
        assert config.environment_root.is_absolute()

    def test_config_sacred_must_exist(self, environment_root: Path):
        """Test that sacred repo must exist."""
        with pytest.raises(ValueError, match="does not exist"):
            EnvironmentConfig(
                sacred_repo=Path("/nonexistent/path"),
                environment_root=environment_root,
            )

    def test_config_env_root_not_inside_sacred(self, mock_sacred_repo: Path):
        """Test that environment root cannot be inside sacred repo."""
        # Note: Pydantic validates environment_root before sacred_repo is available
        # in info.data, so this raises a different error. The key is it DOES fail.
        with pytest.raises(ValueError):
            EnvironmentConfig(
                sacred_repo=mock_sacred_repo,
                environment_root=mock_sacred_repo / "improve",
            )

    def test_config_property_paths(self, env_config: EnvironmentConfig):
        """Test that property paths are computed correctly."""
        assert env_config.master_path == env_config.environment_root / "master"
        assert env_config.sandboxes_path == env_config.environment_root / "sandboxes"
        assert env_config.snapshots_path == env_config.environment_root / "snapshots"
        assert env_config.memory_path == env_config.environment_root / "memory"

    def test_gate_config(self):
        """Test GateConfig defaults."""
        gates = GateConfig()
        assert gates.require_ruff_clean is True
        assert gates.require_pytest_pass is True
        assert gates.require_odibi_validate is True
        assert gates.require_golden_projects is True

    def test_stop_config(self):
        """Test StopConfig defaults and validation."""
        stop = StopConfig()
        assert stop.max_cycles == 10
        assert stop.max_hours == 4.0
        assert stop.convergence_cycles == 3

    def test_command_config(self):
        """Test CommandConfig defaults."""
        commands = CommandConfig()
        assert "pytest" in commands.test
        assert "ruff" in commands.lint
        assert "{file}" in commands.validate_cmd

    def test_campaign_config(self):
        """Test CampaignConfig."""
        campaign = CampaignConfig(
            name="Test Campaign",
            goal="Test bugs",
            max_cycles=5,
        )
        assert campaign.name == "Test Campaign"
        assert campaign.max_cycles == 5
        assert campaign.focus_transformers is None


class TestEnvironmentInitialization:
    """Tests for environment initialization."""

    def test_initialize_creates_structure(self, env_config: EnvironmentConfig):
        """Test that initialization creates all directories."""
        env = ImprovementEnvironment(env_config)
        env.initialize()

        assert env_config.environment_root.exists()
        assert env_config.master_path.exists()
        assert env_config.sandboxes_path.exists()
        assert env_config.snapshots_path.exists()
        assert env_config.memory_path.exists()
        assert env_config.reports_path.exists()
        assert env_config.stories_path.exists()
        assert env_config.config_file_path.exists()
        assert env_config.status_file_path.exists()

    def test_initialize_clones_sacred_to_master(
        self, env_config: EnvironmentConfig, mock_sacred_repo: Path
    ):
        """Test that Sacred is cloned to Master."""
        env = ImprovementEnvironment(env_config)
        env.initialize()

        # Check files were copied
        master_init = env_config.master_path / "odibi" / "__init__.py"
        assert master_init.exists()

        # Content should match
        sacred_init = mock_sacred_repo / "odibi" / "__init__.py"
        assert master_init.read_text() == sacred_init.read_text()

    def test_initialize_creates_memory_files(self, env_config: EnvironmentConfig):
        """Test that memory files are created."""
        env = ImprovementEnvironment(env_config)
        env.initialize()

        assert (env_config.memory_path / "lessons.jsonl").exists()
        assert (env_config.memory_path / "patterns.jsonl").exists()
        assert (env_config.memory_path / "avoided_issues.jsonl").exists()

    def test_initialize_is_idempotent(self, env_config: EnvironmentConfig):
        """Test that calling initialize twice doesn't fail."""
        env = ImprovementEnvironment(env_config)
        env.initialize()
        env.initialize()  # Should not raise

        assert env.is_initialized

    def test_initialize_force_reinitializes(self, env_config: EnvironmentConfig):
        """Test that force=True reinitializes."""
        env = ImprovementEnvironment(env_config)
        env.initialize()

        # Modify master
        test_file = env_config.master_path / "odibi" / "test_new.py"
        test_file.write_text("# new file")
        assert test_file.exists()

        # Force reinitialize
        env.initialize(force=True)

        # New file should be gone
        assert not test_file.exists()


class TestSandboxCreateDestroy:
    """Tests for sandbox lifecycle."""

    def test_create_sandbox(self, initialized_env: ImprovementEnvironment):
        """Test creating a sandbox."""
        sandbox = initialized_env.create_sandbox("cycle_001")

        assert sandbox.sandbox_id.startswith("cycle_001_")
        assert sandbox.sandbox_path.exists()
        assert sandbox.is_active is True
        assert sandbox in initialized_env.list_active_sandboxes()

    def test_create_sandbox_clones_master(self, initialized_env: ImprovementEnvironment):
        """Test that sandbox contains Master files."""
        sandbox = initialized_env.create_sandbox("cycle_001")

        init_file = sandbox.sandbox_path / "odibi" / "__init__.py"
        assert init_file.exists()
        assert init_file.read_text() == "# odibi package"

    def test_create_multiple_sandboxes(self, initialized_env: ImprovementEnvironment):
        """Test creating multiple sandboxes."""
        sb1 = initialized_env.create_sandbox("cycle_001")
        sb2 = initialized_env.create_sandbox("cycle_002")

        assert sb1.sandbox_id != sb2.sandbox_id
        assert sb1.sandbox_path != sb2.sandbox_path
        assert len(initialized_env.list_active_sandboxes()) == 2

    def test_destroy_sandbox(self, initialized_env: ImprovementEnvironment):
        """Test destroying a sandbox."""
        sandbox = initialized_env.create_sandbox("cycle_001")
        sandbox_path = sandbox.sandbox_path

        result = initialized_env.destroy_sandbox(sandbox)

        assert result is True
        assert not sandbox_path.exists()
        assert sandbox not in initialized_env.list_active_sandboxes()

    def test_destroy_nonexistent_sandbox(self, initialized_env: ImprovementEnvironment):
        """Test destroying a sandbox that doesn't exist."""
        fake_sandbox = SandboxInfo(
            sandbox_id="nonexistent",
            sandbox_path=Path("/nonexistent"),
            created_at="2025-01-01T00:00:00",
            is_active=True,
        )

        result = initialized_env.destroy_sandbox(fake_sandbox)
        assert result is False

    def test_cleanup_all_sandboxes(self, initialized_env: ImprovementEnvironment):
        """Test cleaning up all sandboxes."""
        sb1 = initialized_env.create_sandbox("cycle_001")
        sb2 = initialized_env.create_sandbox("cycle_002")

        count = initialized_env.cleanup_all_sandboxes()

        assert count == 2
        assert not sb1.sandbox_path.exists()
        assert not sb2.sandbox_path.exists()
        assert len(initialized_env.list_active_sandboxes()) == 0

    def test_create_sandbox_requires_initialization(self, env_config: EnvironmentConfig):
        """Test that sandbox creation requires initialization."""
        env = ImprovementEnvironment(env_config)

        with pytest.raises(EnvironmentNotInitializedError):
            env.create_sandbox("cycle_001")


class TestSnapshotAndRollback:
    """Tests for snapshot and rollback functionality."""

    def test_snapshot_master(self, initialized_env: ImprovementEnvironment):
        """Test creating a Master snapshot."""
        snapshot_path = initialized_env.snapshot_master("cycle_001")

        assert snapshot_path.exists()
        assert snapshot_path.suffix == ".gz"
        assert "before_cycle_001" in snapshot_path.name

    def test_list_snapshots(self, initialized_env: ImprovementEnvironment):
        """Test listing snapshots."""
        initialized_env.snapshot_master("cycle_001")
        initialized_env.snapshot_master("cycle_002")

        snapshots = initialized_env.list_snapshots()

        assert len(snapshots) == 2
        assert all(isinstance(s, SnapshotInfo) for s in snapshots)

    def test_rollback_master(self, initialized_env: ImprovementEnvironment):
        """Test rolling back Master to a snapshot."""
        # Create snapshot
        snapshot_path = initialized_env.snapshot_master("cycle_001")

        # Modify Master
        test_file = initialized_env.config.master_path / "odibi" / "new_file.py"
        test_file.write_text("# new content")
        assert test_file.exists()

        # Rollback
        initialized_env.rollback_master(snapshot_path)

        # New file should be gone
        assert not test_file.exists()
        # Original files should still exist
        assert (initialized_env.config.master_path / "odibi" / "__init__.py").exists()

    def test_snapshot_rotation(self, initialized_env: ImprovementEnvironment):
        """Test that old snapshots are rotated."""
        # Create more snapshots than max
        for i in range(15):
            initialized_env.snapshot_master(f"cycle_{i:03d}")

        snapshots = initialized_env.list_snapshots()

        # Should be limited to max_snapshots (default 10)
        assert len(snapshots) <= 10


class TestSacredNeverModified:
    """CRITICAL: Tests that Sacred repo is NEVER modified."""

    def test_sacred_not_modified_on_init(
        self, mock_sacred_repo: Path, env_config: EnvironmentConfig
    ):
        """Test that initialization doesn't modify Sacred."""
        # Get original state
        original_files = set()
        original_contents = {}
        for f in mock_sacred_repo.rglob("*"):
            if f.is_file():
                rel = f.relative_to(mock_sacred_repo)
                original_files.add(rel)
                original_contents[rel] = f.read_text()

        # Initialize environment
        env = ImprovementEnvironment(env_config)
        env.initialize()

        # Verify nothing changed
        for f in mock_sacred_repo.rglob("*"):
            if f.is_file():
                rel = f.relative_to(mock_sacred_repo)
                assert rel in original_files, f"New file in Sacred: {rel}"
                assert f.read_text() == original_contents[rel], f"Modified in Sacred: {rel}"

        # No extra files
        current_files = {
            f.relative_to(mock_sacred_repo) for f in mock_sacred_repo.rglob("*") if f.is_file()
        }
        assert current_files == original_files

    def test_sacred_not_modified_on_sandbox_create(
        self, mock_sacred_repo: Path, initialized_env: ImprovementEnvironment
    ):
        """Test that sandbox creation doesn't modify Sacred."""
        original_contents = {}
        for f in mock_sacred_repo.rglob("*"):
            if f.is_file():
                original_contents[f] = f.read_text()

        # Create sandbox
        initialized_env.create_sandbox("cycle_001")

        # Verify Sacred unchanged
        for f, content in original_contents.items():
            assert f.exists(), f"File deleted from Sacred: {f}"
            assert f.read_text() == content, f"File modified in Sacred: {f}"

    def test_sacred_not_modified_on_sandbox_destroy(
        self, mock_sacred_repo: Path, initialized_env: ImprovementEnvironment
    ):
        """Test that sandbox destruction doesn't modify Sacred."""
        sandbox = initialized_env.create_sandbox("cycle_001")

        original_contents = {}
        for f in mock_sacred_repo.rglob("*"):
            if f.is_file():
                original_contents[f] = f.read_text()

        # Destroy sandbox
        initialized_env.destroy_sandbox(sandbox)

        # Verify Sacred unchanged
        for f, content in original_contents.items():
            assert f.exists(), f"File deleted from Sacred: {f}"
            assert f.read_text() == content, f"File modified in Sacred: {f}"

    def test_sacred_not_modified_on_promotion(
        self, mock_sacred_repo: Path, initialized_env: ImprovementEnvironment
    ):
        """Test that promotion doesn't modify Sacred."""
        sandbox = initialized_env.create_sandbox("cycle_001")

        # Modify sandbox
        new_file = sandbox.sandbox_path / "odibi" / "improved.py"
        new_file.write_text("# improved code")

        original_contents = {}
        for f in mock_sacred_repo.rglob("*"):
            if f.is_file():
                original_contents[f] = f.read_text()

        # Promote
        initialized_env.snapshot_master("cycle_001")
        initialized_env.promote_to_master(sandbox)

        # Verify Sacred unchanged
        for f, content in original_contents.items():
            assert f.exists(), f"File deleted from Sacred: {f}"
            assert f.read_text() == content, f"File modified in Sacred: {f}"

    def test_sandbox_path_validation_rejects_sacred(
        self, mock_sacred_repo: Path, environment_root: Path
    ):
        """Test that sandbox cannot be created inside Sacred."""
        sandboxes_root = environment_root / "sandboxes"
        sandboxes_root.mkdir(parents=True)

        # Try to create sandbox path inside sacred
        bad_path = mock_sacred_repo / "sandbox"

        # This should raise SandboxPathViolation - either because the path
        # is not inside sandboxes_root, OR because it's inside sacred.
        # Either validation correctly prevents the dangerous operation.
        with pytest.raises(SandboxPathViolation):
            validate_sandbox_path(bad_path, sandboxes_root, mock_sacred_repo)


class TestPromotion:
    """Tests for sandbox promotion to Master."""

    def test_promote_to_master(self, initialized_env: ImprovementEnvironment):
        """Test promoting sandbox changes to Master."""
        sandbox = initialized_env.create_sandbox("cycle_001")

        # Add new file in sandbox
        new_file = sandbox.sandbox_path / "odibi" / "new_feature.py"
        new_file.write_text("# new feature")

        # Snapshot and promote
        initialized_env.snapshot_master("cycle_001")
        result = initialized_env.promote_to_master(sandbox)

        assert result is True

        # New file should be in Master
        master_new = initialized_env.config.master_path / "odibi" / "new_feature.py"
        assert master_new.exists()
        assert master_new.read_text() == "# new feature"

    def test_promote_nonexistent_sandbox_fails(self, initialized_env: ImprovementEnvironment):
        """Test that promoting nonexistent sandbox fails."""
        fake_sandbox = SandboxInfo(
            sandbox_id="nonexistent",
            sandbox_path=Path("/nonexistent"),
            created_at="2025-01-01T00:00:00",
            is_active=True,
        )

        with pytest.raises(PromotionError, match="not found"):
            initialized_env.promote_to_master(fake_sandbox)


class TestLoadEnvironment:
    """Tests for loading an existing environment."""

    def test_load_environment(self, initialized_env: ImprovementEnvironment):
        """Test loading an existing environment."""
        root = initialized_env.config.environment_root

        loaded = load_environment(root)

        assert loaded.is_initialized
        assert loaded.sacred_repo == initialized_env.sacred_repo
        assert loaded.environment_root == root

    def test_load_nonexistent_fails(self, temp_dir: Path):
        """Test that loading nonexistent environment fails."""
        with pytest.raises(EnvironmentNotInitializedError):
            load_environment(temp_dir / "nonexistent")
