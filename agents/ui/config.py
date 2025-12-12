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
    api_version: str = "2024-02-15-preview"

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

    Supports two paths:
    - working_project: Primary folder for work, config storage, and indexing (required)
    - reference_repo: Optional secondary codebase for grep/read access
    """

    working_project: str = ""
    reference_repo: str = ""
    project_yaml_path: Optional[str] = None

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
                "reference_repo": self.project.reference_repo,
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
                api_version=llm_data.get("api_version", "2024-02-15-preview"),
            ),
            memory=MemoryConfig(
                backend_type=memory_data.get("backend_type", "local"),
                connection_name=memory_data.get("connection_name"),
                path_prefix=memory_data.get("path_prefix", "agent/memories"),
                table_path=memory_data.get("table_path", "system.agent_memories"),
            ),
            project=ProjectConfig(
                working_project=project_data.get("working_project", ""),
                reference_repo=project_data.get("reference_repo", ""),
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
        if data["llm"].get("api_key"):
            data["llm"]["api_key"] = "***"

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
