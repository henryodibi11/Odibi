"""Configuration management for the Odibi AI Assistant.

Loads and saves settings from/to .odibi/agent_config.yaml.
Supports any OpenAI-compatible LLM provider.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml


@dataclass
class LLMConfig:
    """LLM provider configuration.

    Works with any OpenAI-compatible API:
    - OpenAI (https://api.openai.com/v1)
    - Azure OpenAI (https://your-resource.openai.azure.com)
    - Ollama (http://localhost:11434/v1)
    - LM Studio (http://localhost:1234/v1)
    - vLLM, LocalAI, Together, Anyscale, etc.
    """

    endpoint: str = ""
    model: str = "gpt-4o"
    api_key: str = ""
    api_type: str = "openai"
    api_version: str = "2024-12-01-preview"

    def __post_init__(self):
        if not self.endpoint:
            self.endpoint = os.getenv(
                "LLM_ENDPOINT",
                os.getenv(
                    "OPENAI_API_BASE",
                    os.getenv("AZURE_OPENAI_ENDPOINT", "https://api.openai.com/v1"),
                ),
            )
        if not self.api_key:
            self.api_key = os.getenv(
                "LLM_API_KEY",
                os.getenv("OPENAI_API_KEY", os.getenv("AZURE_OPENAI_API_KEY", "")),
            )
        if not self.model:
            self.model = os.getenv(
                "LLM_MODEL",
                os.getenv("OPENAI_MODEL", os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")),
            )
        if "azure" in self.endpoint.lower() and self.api_type == "openai":
            self.api_type = "azure"


@dataclass
class MemoryConfig:
    """Memory backend configuration."""

    backend_type: str = "local"
    connection_name: Optional[str] = None
    path_prefix: str = "agent/memories"
    table_path: str = "system.agent_memories"


@dataclass
class ProjectConfig:
    """Project configuration.

    Supports multiple paths:
    - working_project: Primary folder for work, config storage, and indexing (required)
    - reference_repos: Optional list of additional codebases for grep/read access
    """

    working_project: str = ""
    reference_repos: list[str] = field(default_factory=list)
    project_yaml_path: Optional[str] = None

    @property
    def reference_repo(self) -> str:
        """Backward compatibility: return first reference repo or empty string."""
        return self.reference_repos[0] if self.reference_repos else ""

    @reference_repo.setter
    def reference_repo(self, value: str) -> None:
        """Backward compatibility: set as first reference repo."""
        if value:
            if not self.reference_repos:
                self.reference_repos = [value]
            else:
                self.reference_repos[0] = value

    @property
    def project_root(self) -> str:
        """The active working project path."""
        return self.working_project

    @project_root.setter
    def project_root(self, value: str) -> None:
        """Set the working project path."""
        self.working_project = value

    def __post_init__(self):
        if not self.project_yaml_path and self.working_project:
            candidate = Path(self.working_project) / "project.yaml"
            if candidate.exists():
                self.project_yaml_path = str(candidate)


@dataclass
class AgentUIConfig:
    """Complete UI configuration."""

    llm: LLMConfig = field(default_factory=LLMConfig)
    memory: MemoryConfig = field(default_factory=MemoryConfig)
    project: ProjectConfig = field(default_factory=ProjectConfig)
    default_agent: str = "auto"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for YAML serialization."""
        return {
            "llm": {
                "endpoint": self.llm.endpoint,
                "model": self.llm.model,
                "api_key": self.llm.api_key,
                "api_type": self.llm.api_type,
                "api_version": self.llm.api_version,
            },
            "memory": {
                "backend_type": self.memory.backend_type,
                "connection_name": self.memory.connection_name,
                "path_prefix": self.memory.path_prefix,
                "table_path": self.memory.table_path,
            },
            "project": {
                "working_project": self.project.working_project,
                "reference_repos": self.project.reference_repos,
                "project_yaml_path": self.project.project_yaml_path,
            },
            "default_agent": self.default_agent,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AgentUIConfig":
        """Create from dictionary."""
        llm_data = data.get("llm", {})
        memory_data = data.get("memory", {})
        project_data = data.get("project", {})

        model = llm_data.get("model") or llm_data.get("deployment_name", "gpt-4o")

        return cls(
            llm=LLMConfig(
                endpoint=llm_data.get("endpoint", ""),
                model=model,
                api_key=llm_data.get("api_key", ""),
                api_type=llm_data.get("api_type", "openai"),
                api_version=llm_data.get("api_version", "2024-12-01-preview"),
            ),
            memory=MemoryConfig(
                backend_type=memory_data.get("backend_type", "local"),
                connection_name=memory_data.get("connection_name"),
                path_prefix=memory_data.get("path_prefix", "agent/memories"),
                table_path=memory_data.get("table_path", "system.agent_memories"),
            ),
            project=ProjectConfig(
                working_project=project_data.get("working_project", ""),
                reference_repos=project_data.get("reference_repos")
                or ([project_data["reference_repo"]] if project_data.get("reference_repo") else []),
                project_yaml_path=project_data.get("project_yaml_path"),
            ),
            default_agent=data.get("default_agent", "auto"),
        )


def get_config_path(working_project: str) -> Path:
    """Get the path to the agent config file."""
    return Path(working_project) / ".odibi" / "agent_config.yaml"


def load_config(working_project: str = "") -> AgentUIConfig:
    """Load configuration from YAML file."""
    if not working_project:
        return AgentUIConfig()

    config_path = get_config_path(working_project)

    if not config_path.exists():
        return AgentUIConfig(project=ProjectConfig(working_project=working_project))

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        config = AgentUIConfig.from_dict(data)
        config.project.working_project = working_project
        return config
    except Exception:
        return AgentUIConfig(project=ProjectConfig(working_project=working_project))


def save_config(config: AgentUIConfig) -> bool:
    """Save configuration to YAML file."""
    if not config.project.working_project:
        return False

    config_path = get_config_path(config.project.working_project).resolve()
    config_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        data = config.to_dict()
        # Don't persist API key to disk for security - user must re-enter each session
        # But keep the key in memory for the current session
        data["llm"]["api_key"] = ""

        with open(config_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
        return True
    except Exception:
        return False


def load_odibi_connections(project_yaml_path: Optional[str] = None) -> list[str]:
    """Load connection names from Odibi project.yaml."""
    if not project_yaml_path:
        return []

    path = Path(project_yaml_path)
    if not path.exists():
        return []

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        connections = data.get("connections", {})
        if isinstance(connections, dict):
            return list(connections.keys())
        return []
    except Exception:
        return []


class ConnectionError(Exception):
    """Error when loading a connection."""

    pass


def get_odibi_connection(
    project_yaml_path: str,
    connection_name: str,
) -> Optional[Any]:
    """Get an Odibi connection object by name.

    Args:
        project_yaml_path: Path to project.yaml/odibi.yaml
        connection_name: Name of the connection to retrieve

    Returns:
        Connection object or None if not found

    Raises:
        ConnectionError: If connection exists but fails to load (with details)
    """
    if not project_yaml_path:
        raise ConnectionError("No project.yaml path provided")

    # Check if file exists (handle both local and Databricks paths)
    try:
        import os

        if not os.path.exists(project_yaml_path):
            raise ConnectionError(f"project.yaml not found at: {project_yaml_path}")
    except Exception as e:
        raise ConnectionError(f"Cannot access path '{project_yaml_path}': {e}") from e

    try:
        from odibi.config import load_config_from_file

        config = load_config_from_file(project_yaml_path)
    except Exception as e:
        # Provide more detailed error for YAML parsing failures
        error_detail = str(e)
        try:
            # Try to read and show the first few lines for debugging
            with open(project_yaml_path, "r", encoding="utf-8") as f:
                preview = f.read(500)
            error_detail = f"{e}\n\nFile preview (first 500 chars):\n{preview}"
        except Exception:
            pass
        raise ConnectionError(f"Failed to parse '{project_yaml_path}': {error_detail}") from e

    if connection_name not in config.connections:
        available = list(config.connections.keys())
        raise ConnectionError(
            f"Connection '{connection_name}' not in config. Available: {available}"
        )

    try:
        from odibi.context import EngineContext

        conn_config = config.connections[connection_name]
        ctx = EngineContext.create(conn_config)
        return ctx.connection
    except Exception as e:
        raise ConnectionError(f"Failed to create connection '{connection_name}': {e}") from e
