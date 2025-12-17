"""ImprovementEnvironment: Manages the Sacred → Master → Sandbox hierarchy.

This is the core class for the improvement infrastructure. It provides:
- Environment initialization (create dirs, clone Sacred → Master)
- Sandbox lifecycle management (create, destroy)
- Snapshot/rollback for Master
- Change promotion from sandbox to Master

CRITICAL INVARIANT: The Sacred repository is NEVER modified.
"""

import json
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml

from odibi.agents.improve.config import EnvironmentConfig
from odibi.agents.improve.sandbox import (
    SandboxInfo,
    create_sandbox_clone,
    destroy_sandbox,
    generate_sandbox_id,
    validate_sandbox_path,
)
from odibi.agents.improve.snapshot import (
    SnapshotInfo,
    create_snapshot,
    list_snapshots,
    restore_snapshot,
    rotate_snapshots,
)

logger = logging.getLogger(__name__)


class EnvironmentError(Exception):
    """Base exception for environment operations."""

    pass


class EnvironmentNotInitializedError(EnvironmentError):
    """Raised when environment is not initialized."""

    pass


class PromotionError(EnvironmentError):
    """Raised when promotion to Master fails."""

    def __init__(self, message: str, cause: Optional[Exception] = None):
        self.cause = cause
        super().__init__(message)


class ImprovementEnvironment:
    """Manages the Sacred → Master → Sandbox hierarchy.

    This class is the primary interface for the improvement infrastructure.
    It enforces the trust hierarchy and provides safe operations for:
    - Initializing the environment structure
    - Creating and destroying sandboxes
    - Snapshotting and rolling back Master
    - Promoting changes from sandbox to Master

    Attributes:
        config: The environment configuration.
        sacred_repo: Path to the Sacred source (NEVER touched).
        environment_root: Root directory for master/sandboxes/snapshots.
    """

    def __init__(self, config: EnvironmentConfig) -> None:
        """Initialize the improvement environment.

        Args:
            config: The environment configuration.
        """
        self._config = config
        self._active_sandboxes: dict[str, SandboxInfo] = {}
        self._initialized = False

    @property
    def config(self) -> EnvironmentConfig:
        """Read-only access to configuration."""
        return self._config

    @property
    def sacred_repo(self) -> Path:
        """Read-only access to Sacred repository path."""
        return self._config.sacred_repo

    @property
    def environment_root(self) -> Path:
        """Read-only access to environment root path."""
        return self._config.environment_root

    @property
    def is_initialized(self) -> bool:
        """Check if environment has been initialized."""
        return self._initialized and self._config.master_path.exists()

    def initialize(self, force: bool = False) -> None:
        """Create environment structure and clone Sacred → Master.

        Creates the following structure:
        - environment_root/master/odibi/ (clone of Sacred)
        - environment_root/sandboxes/
        - environment_root/snapshots/
        - environment_root/memory/
        - environment_root/reports/
        - environment_root/stories/
        - environment_root/config.yaml
        - environment_root/status.json

        Args:
            force: If True, reinitialize even if already exists.

        Raises:
            EnvironmentError: If initialization fails.
        """
        if self.is_initialized and not force:
            logger.info("Environment already initialized")
            self._initialized = True
            return

        logger.info(f"Initializing improvement environment at {self.environment_root}")

        # Create directory structure
        directories = [
            self._config.environment_root,
            self._config.master_path.parent,  # master/
            self._config.sandboxes_path,
            self._config.snapshots_path,
            self._config.memory_path,
            self._config.reports_path,
            self._config.stories_path,
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created directory: {directory}")

        # Clone Sacred → Master (if not exists or force)
        if not self._config.master_path.exists() or force:
            if self._config.master_path.exists():
                logger.info(f"Removing existing Master at {self._config.master_path}")
                shutil.rmtree(self._config.master_path)

            logger.info("Cloning Sacred → Master")
            logger.info(f"  Source: {self.sacred_repo}")
            logger.info(f"  Target: {self._config.master_path}")

            try:
                shutil.copytree(
                    self.sacred_repo,
                    self._config.master_path,
                    symlinks=False,
                    ignore=shutil.ignore_patterns(
                        "__pycache__",
                        "*.pyc",
                        ".git",
                        "*.egg-info",
                        ".pytest_cache",
                        ".mypy_cache",
                        ".ruff_cache",
                        ".venv",
                        "venv",
                        ".odibi",  # Large cache data
                        "stories",  # Execution traces
                        "data",  # Test data
                        ".coverage",
                        "coverage.xml",
                    ),
                )
            except Exception as e:
                raise EnvironmentError(f"Failed to clone Sacred → Master: {e}")

        # Write config.yaml
        self._write_config_file()

        # Write initial status.json
        self._write_status_file()

        # Create memory files
        self._initialize_memory_files()

        self._initialized = True
        logger.info("Environment initialization complete")

    def _write_config_file(self) -> None:
        """Write config.yaml to environment root."""
        config_data = {
            "environment": {
                "version": self._config.version,
                "created_at": datetime.now().isoformat(),
            },
            "source": {
                "sacred_repo": str(self.sacred_repo),
                "auto_reset_master": False,
            },
            "gates": {
                "require_ruff_clean": self._config.gates.require_ruff_clean,
                "require_pytest_pass": self._config.gates.require_pytest_pass,
                "require_odibi_validate": self._config.gates.require_odibi_validate,
                "require_golden_projects": self._config.gates.require_golden_projects,
            },
            "stop_conditions": {
                "max_cycles": self._config.stop_conditions.max_cycles,
                "max_hours": self._config.stop_conditions.max_hours,
                "convergence_cycles": self._config.stop_conditions.convergence_cycles,
            },
            "commands": {
                "test": self._config.commands.test,
                "lint": self._config.commands.lint,
                "validate": self._config.commands.validate_cmd,
            },
            "snapshots": {
                "enabled": self._config.snapshots.enabled,
                "max_snapshots": self._config.snapshots.max_snapshots,
                "compress": self._config.snapshots.compress,
            },
        }

        with open(self._config.config_file_path, "w", encoding="utf-8") as f:
            yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)

        logger.debug(f"Wrote config to {self._config.config_file_path}")

    def _write_status_file(
        self,
        status: str = "INITIALIZED",
        current_cycle: int = 0,
        cycles_completed: int = 0,
        improvements_promoted: int = 0,
    ) -> None:
        """Write status.json to environment root."""
        status_data = {
            "status": status,
            "initialized_at": datetime.now().isoformat(),
            "current_cycle": current_cycle,
            "cycles_completed": cycles_completed,
            "improvements_promoted": improvements_promoted,
            "improvements_rejected": 0,
            "master_changes": 0,
            "last_promotion": None,
            "convergence_counter": 0,
            "issues_avoided": [],
            "elapsed_hours": 0.0,
            "active_sandboxes": list(self._active_sandboxes.keys()),
        }

        with open(self._config.status_file_path, "w", encoding="utf-8") as f:
            json.dump(status_data, f, indent=2)

        logger.debug(f"Wrote status to {self._config.status_file_path}")

    def _initialize_memory_files(self) -> None:
        """Create empty memory files."""
        memory_files = [
            self._config.memory_path / "lessons.jsonl",
            self._config.memory_path / "patterns.jsonl",
            self._config.memory_path / "avoided_issues.jsonl",
        ]

        for memory_file in memory_files:
            if not memory_file.exists():
                memory_file.touch()
                logger.debug(f"Created memory file: {memory_file}")

    def create_sandbox(self, cycle_id: str) -> SandboxInfo:
        """Create a new sandbox cloned from Master.

        Args:
            cycle_id: Identifier for the cycle (e.g., "cycle_001").

        Returns:
            SandboxInfo describing the created sandbox.

        Raises:
            EnvironmentNotInitializedError: If environment not initialized.
            SandboxCreationError: If sandbox creation fails.
        """
        if not self.is_initialized:
            raise EnvironmentNotInitializedError(
                "Environment must be initialized before creating sandboxes"
            )

        sandbox_id = generate_sandbox_id(cycle_id)
        sandbox_path = self._config.sandboxes_path / sandbox_id

        # Validate sandbox path is safe
        validate_sandbox_path(
            sandbox_path,
            self._config.sandboxes_path,
            self.sacred_repo,
        )

        # Clone Master → Sandbox
        create_sandbox_clone(self._config.master_path, sandbox_path)

        # Create SandboxInfo
        info = SandboxInfo(
            sandbox_id=sandbox_id,
            sandbox_path=sandbox_path,
            created_at=datetime.now().isoformat(),
            is_active=True,
        )

        self._active_sandboxes[sandbox_id] = info
        logger.info(f"Created sandbox {sandbox_id} from Master")

        return info

    def destroy_sandbox(self, sandbox: SandboxInfo) -> bool:
        """Destroy a sandbox and clean up.

        Args:
            sandbox: The sandbox to destroy.

        Returns:
            True if sandbox was destroyed, False if not found.
        """
        if sandbox.sandbox_id not in self._active_sandboxes:
            logger.warning(f"Sandbox {sandbox.sandbox_id} not found in active list")
            # Try to destroy anyway if path exists
            if sandbox.sandbox_path.exists():
                destroy_sandbox(sandbox.sandbox_path, self._config.sandboxes_path)
                return True
            return False

        result = destroy_sandbox(sandbox.sandbox_path, self._config.sandboxes_path)
        del self._active_sandboxes[sandbox.sandbox_id]

        return result

    def get_sandbox(self, sandbox_id: str) -> Optional[SandboxInfo]:
        """Get info about an active sandbox.

        Args:
            sandbox_id: ID of the sandbox.

        Returns:
            SandboxInfo if found, None otherwise.
        """
        return self._active_sandboxes.get(sandbox_id)

    def list_active_sandboxes(self) -> list[SandboxInfo]:
        """List all active sandboxes.

        Returns:
            List of active SandboxInfo.
        """
        return list(self._active_sandboxes.values())

    def snapshot_master(self, cycle_id: str) -> Path:
        """Create a snapshot of Master before promotion.

        Args:
            cycle_id: Identifier for the cycle (used in snapshot name).

        Returns:
            Path to the created snapshot.

        Raises:
            EnvironmentNotInitializedError: If not initialized.
        """
        if not self.is_initialized:
            raise EnvironmentNotInitializedError(
                "Environment must be initialized before creating snapshots"
            )

        snapshot_id = f"before_{cycle_id}"
        snapshot_info = create_snapshot(
            source_path=self._config.master_path,
            snapshots_dir=self._config.snapshots_path,
            snapshot_id=snapshot_id,
            compress=self._config.snapshots.compress,
        )

        # Rotate old snapshots
        if self._config.snapshots.max_snapshots > 0:
            rotate_snapshots(
                self._config.snapshots_path,
                self._config.snapshots.max_snapshots,
            )

        logger.info(f"Created Master snapshot: {snapshot_info.snapshot_path}")
        return snapshot_info.snapshot_path

    def rollback_master(self, snapshot_path: Path) -> None:
        """Rollback Master to a previous snapshot.

        WARNING: This REPLACES the current Master with the snapshot contents.

        Args:
            snapshot_path: Path to the snapshot to restore.

        Raises:
            SnapshotRestoreError: If restoration fails.
        """
        if not self.is_initialized:
            raise EnvironmentNotInitializedError("Environment must be initialized before rollback")

        compressed = snapshot_path.suffix == ".gz"
        restore_snapshot(
            snapshot_path=snapshot_path,
            target_path=self._config.master_path,
            compressed=compressed,
        )

        logger.info(f"Rolled back Master from snapshot: {snapshot_path}")

    def list_snapshots(self) -> list[SnapshotInfo]:
        """List all available snapshots.

        Returns:
            List of SnapshotInfo, sorted by creation time.
        """
        return list_snapshots(self._config.snapshots_path)

    def promote_to_master(self, sandbox: SandboxInfo, diff: str = "") -> bool:
        """Promote sandbox changes to Master.

        This replaces Master with the sandbox contents.
        A snapshot should be taken before calling this.

        Args:
            sandbox: The sandbox to promote.
            diff: Optional diff string for logging.

        Returns:
            True if promotion succeeded.

        Raises:
            PromotionError: If promotion fails.
        """
        if not self.is_initialized:
            raise EnvironmentNotInitializedError("Environment must be initialized before promotion")

        if sandbox.sandbox_id not in self._active_sandboxes:
            raise PromotionError(f"Sandbox {sandbox.sandbox_id} not found in active list")

        if not sandbox.sandbox_path.exists():
            raise PromotionError(f"Sandbox path does not exist: {sandbox.sandbox_path}")

        try:
            # Remove current Master
            if self._config.master_path.exists():
                shutil.rmtree(self._config.master_path)

            # Copy sandbox to Master
            shutil.copytree(
                sandbox.sandbox_path,
                self._config.master_path,
                symlinks=False,
                ignore=shutil.ignore_patterns(
                    "__pycache__",
                    "*.pyc",
                    ".pytest_cache",
                    ".mypy_cache",
                    ".ruff_cache",
                ),
            )

            logger.info(f"Promoted sandbox {sandbox.sandbox_id} to Master")
            if diff:
                logger.debug(f"Diff:\n{diff[:500]}...")  # Log first 500 chars

            return True

        except Exception as e:
            raise PromotionError(f"Failed to promote sandbox to Master: {e}", cause=e)

    def cleanup_all_sandboxes(self) -> int:
        """Destroy all active sandboxes.

        Returns:
            Number of sandboxes cleaned up.
        """
        sandbox_ids = list(self._active_sandboxes.keys())
        count = 0

        for sandbox_id in sandbox_ids:
            try:
                sandbox = self._active_sandboxes[sandbox_id]
                if self.destroy_sandbox(sandbox):
                    count += 1
            except Exception as e:
                logger.warning(f"Failed to cleanup sandbox {sandbox_id}: {e}")

        return count


def load_environment(environment_root: Path) -> ImprovementEnvironment:
    """Load an existing environment from its root directory.

    Args:
        environment_root: Path to the environment root.

    Returns:
        ImprovementEnvironment loaded from config.

    Raises:
        EnvironmentNotInitializedError: If environment doesn't exist.
    """
    config_path = environment_root / "config.yaml"

    if not config_path.exists():
        raise EnvironmentNotInitializedError(f"Environment config not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    config = EnvironmentConfig(
        sacred_repo=Path(data["source"]["sacred_repo"]),
        environment_root=environment_root,
        version=data["environment"]["version"],
    )

    env = ImprovementEnvironment(config)
    env._initialized = True

    return env
