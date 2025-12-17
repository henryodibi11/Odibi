"""Configuration models for the Improvement Environment.

Provides Pydantic models for environment, gate, stop, command, and snapshot
configurations. All paths are configurable for portability across machines.
"""

from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class GateConfig(BaseModel):
    """Gate requirements for promoting changes to Master.

    All gates must pass before changes can be promoted.
    """

    require_ruff_clean: bool = Field(
        default=True,
        description="Require ruff linting to pass with no errors",
    )
    require_pytest_pass: bool = Field(
        default=True,
        description="Require all pytest tests to pass",
    )
    require_odibi_validate: bool = Field(
        default=True,
        description="Require odibi validate to pass on modified configs",
    )
    require_golden_projects: bool = Field(
        default=True,
        description="Require all learning harness configs to pass",
    )


class StopConfig(BaseModel):
    """Stop conditions for improvement campaigns.

    Campaign stops when ANY condition is met.
    """

    max_cycles: int = Field(
        default=10,
        ge=1,
        description="Maximum number of improvement cycles",
    )
    max_hours: float = Field(
        default=4.0,
        gt=0,
        description="Maximum hours to run before stopping",
    )
    convergence_cycles: int = Field(
        default=3,
        ge=1,
        description="Stop if N consecutive cycles learn nothing new",
    )


class CommandConfig(BaseModel):
    """Command templates for running tests, lint, and validation.

    Commands are run in the sandbox directory context.
    """

    test: str = Field(
        default="python -m pytest tests/ -v",
        description="Command to run tests",
    )
    lint: str = Field(
        default="python -m ruff check odibi/",
        description="Command to run linter",
    )
    validate_cmd: str = Field(
        default="python -m odibi validate {file}",
        description="Command to validate a config file ({file} is replaced)",
    )
    use_wsl: bool = Field(
        default=False,
        description="Run commands through WSL (required for Spark on Windows)",
    )
    wsl_distro: str = Field(
        default="Ubuntu",
        description="WSL distribution to use (e.g., Ubuntu, Debian)",
    )
    wsl_python: str = Field(
        default="python3",
        description="Python command in WSL environment",
    )


class SnapshotConfig(BaseModel):
    """Configuration for Master snapshots."""

    enabled: bool = Field(
        default=True,
        description="Whether to create snapshots before promotions",
    )
    max_snapshots: int = Field(
        default=10,
        ge=1,
        description="Maximum number of snapshots to keep",
    )
    compress: bool = Field(
        default=True,
        description="Whether to compress snapshots (.tar.gz)",
    )


class EnvironmentConfig(BaseModel):
    """Complete configuration for the Improvement Environment.

    Attributes:
        sacred_repo: Path to the source repository (NEVER modified).
        environment_root: Path where master/sandboxes/snapshots are created.
        gates: Gate requirements for promotion.
        stop_conditions: When to stop a campaign.
        commands: Command templates for test/lint/validate.
        snapshots: Snapshot configuration.
        version: Configuration schema version.
    """

    sacred_repo: Path = Field(
        description="Path to the Sacred source repository (NEVER touched)",
    )
    environment_root: Path = Field(
        description="Root directory for master, sandboxes, snapshots",
    )
    gates: GateConfig = Field(default_factory=GateConfig)
    stop_conditions: StopConfig = Field(default_factory=StopConfig)
    commands: CommandConfig = Field(default_factory=CommandConfig)
    snapshots: SnapshotConfig = Field(default_factory=SnapshotConfig)
    version: str = Field(
        default="1.0.0",
        description="Configuration schema version",
    )

    @field_validator("sacred_repo", "environment_root", mode="before")
    @classmethod
    def convert_to_path(cls, v: str | Path) -> Path:
        """Convert string paths to Path objects."""
        return Path(v) if isinstance(v, str) else v

    @field_validator("sacred_repo")
    @classmethod
    def validate_sacred_exists(cls, v: Path) -> Path:
        """Validate that sacred repo path exists."""
        resolved = v.resolve()
        if not resolved.exists():
            raise ValueError(f"Sacred repository does not exist: {resolved}")
        if not resolved.is_dir():
            raise ValueError(f"Sacred repository must be a directory: {resolved}")
        return resolved

    @field_validator("environment_root")
    @classmethod
    def validate_environment_root(cls, v: Path) -> Path:
        """Resolve environment root path."""
        return v.resolve()

    @model_validator(mode="after")
    def validate_paths_not_overlapping(self) -> "EnvironmentConfig":
        """Validate that sacred_repo and environment_root don't overlap."""
        sacred_resolved = self.sacred_repo.resolve()
        env_resolved = self.environment_root.resolve()

        # Environment root must not be inside sacred repo
        try:
            env_resolved.relative_to(sacred_resolved)
            raise ValueError(
                f"Environment root cannot be inside sacred repo: "
                f"{env_resolved} is inside {sacred_resolved}"
            )
        except ValueError as e:
            if "cannot be inside" in str(e):
                raise
            # Path.relative_to raises ValueError when paths are unrelated - expected

        # Sacred repo must not be inside environment root
        try:
            sacred_resolved.relative_to(env_resolved)
            raise ValueError(
                f"Sacred repo cannot be inside environment root: "
                f"{sacred_resolved} is inside {env_resolved}"
            )
        except ValueError as e:
            if "cannot be inside" in str(e):
                raise
            # Path.relative_to raises ValueError when paths are unrelated - expected

        return self

    @property
    def master_path(self) -> Path:
        """Path to the Master clone (full repo, not just odibi package)."""
        return self.environment_root / "master"

    @property
    def sandboxes_path(self) -> Path:
        """Path to the sandboxes directory."""
        return self.environment_root / "sandboxes"

    @property
    def snapshots_path(self) -> Path:
        """Path to the snapshots directory."""
        return self.environment_root / "snapshots"

    @property
    def memory_path(self) -> Path:
        """Path to the memory directory."""
        return self.environment_root / "memory"

    @property
    def reports_path(self) -> Path:
        """Path to the reports directory."""
        return self.environment_root / "reports"

    @property
    def stories_path(self) -> Path:
        """Path to the stories directory."""
        return self.environment_root / "stories"

    @property
    def config_file_path(self) -> Path:
        """Path to the config.yaml file."""
        return self.environment_root / "config.yaml"

    @property
    def status_file_path(self) -> Path:
        """Path to the status.json file."""
        return self.environment_root / "status.json"


class CampaignConfig(BaseModel):
    """Configuration for an improvement campaign run.

    Extends environment config with campaign-specific settings.
    """

    name: str = Field(
        default="Improvement Campaign",
        description="Human-readable campaign name",
    )
    goal: str = Field(
        default="Find and fix bugs",
        description="Campaign goal description",
    )
    max_cycles: int = Field(
        default=10,
        ge=1,
        description="Maximum cycles for this campaign",
    )
    max_hours: float = Field(
        default=4.0,
        gt=0,
        description="Maximum hours for this campaign",
    )
    convergence_threshold: int = Field(
        default=3,
        ge=1,
        description="Cycles without learning before stopping",
    )
    require_tests_pass: bool = Field(
        default=True,
        description="Gate: require all tests to pass",
    )
    require_lint_clean: bool = Field(
        default=True,
        description="Gate: require lint to pass",
    )
    require_golden_pass: bool = Field(
        default=True,
        description="Gate: require golden projects to pass",
    )
    focus_transformers: Optional[list[str]] = Field(
        default=None,
        description="Limit to specific transformers (optional)",
    )
    focus_patterns: Optional[list[str]] = Field(
        default=None,
        description="Limit to specific patterns (optional)",
    )
    test_path: str = Field(
        default="tests/",
        description="Path to test directory or file",
    )
    test_ignore_patterns: list[str] = Field(
        default_factory=lambda: [
            "tests/test_streaming.py",
            "tests/test_streaming_display.py",
        ],
        description="Test files/directories to ignore (agents tests, etc.)",
    )
    lint_path: str = Field(
        default="odibi/",
        description="Path to lint (should be odibi package only)",
    )

    enable_llm_improvements: bool = Field(
        default=True,
        description="Enable LLM-powered code improvements",
    )
    max_improvement_attempts_per_cycle: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Maximum LLM fix attempts per cycle",
    )
    llm_model: str = Field(
        default="gpt-4o-mini",
        description="LLM model name (gpt-4o-mini, gpt-4o, etc.)",
    )
    llm_endpoint: Optional[str] = Field(
        default=None,
        description="LLM API endpoint (uses env vars if None)",
    )
    llm_api_key: Optional[str] = Field(
        default=None,
        description="LLM API key (uses env vars if None)",
    )

    use_wsl: bool = Field(
        default=True,
        description="Run pipeline commands through WSL (for Spark on Windows)",
    )
    wsl_distro: str = Field(
        default="Ubuntu-20.04",
        description="WSL distribution to use",
    )
    wsl_python: str = Field(
        default="python3.9",
        description="Python command in WSL",
    )
    wsl_shell_init: str = Field(
        default="",
        description=(
            "Shell commands to run before each WSL command (e.g., "
            "'source ~/.bashrc' or 'export SPARK_HOME=/opt/spark')"
        ),
    )
    wsl_env: dict[str, str] = Field(
        default_factory=lambda: {
            "SPARK_HOME": "/opt/spark",
            "JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-amd64",
            "PYSPARK_PYTHON": "python3",
        },
        description="Environment variables to set in WSL for Spark",
    )
