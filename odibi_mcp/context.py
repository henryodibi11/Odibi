# odibi_mcp/context.py
"""MCP Project Context - loads odibi.yaml and provides access to connections, stories, catalog."""

import os
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Literal
from dataclasses import dataclass, field

import yaml

logger = logging.getLogger(__name__)

# Mode type for context
ContextMode = Literal["full", "exploration"]


@dataclass
class MCPProjectContext:
    """
    Holds the loaded project configuration and initialized connections.

    This is the bridge between MCP tools and the odibi framework.
    """

    project_name: str
    config_path: Path
    config: Dict[str, Any]
    connections: Dict[str, Any] = field(default_factory=dict)
    story_connection: Optional[str] = None
    story_path: Optional[str] = None
    catalog_connection: Optional[str] = None
    mode: ContextMode = "full"  # "full" or "exploration"
    _initialized: bool = False

    @classmethod
    def from_mcp_config(cls, mcp_config_path: str) -> "MCPProjectContext":
        """Load project context from MCP config file."""
        mcp_config_path = Path(mcp_config_path)

        if not mcp_config_path.exists():
            raise FileNotFoundError(f"MCP config not found: {mcp_config_path}")

        with open(mcp_config_path) as f:
            mcp_config = yaml.safe_load(f)

        project_config = mcp_config.get("project", {})
        config_path = project_config.get("config_path", "./odibi.yaml")

        # Resolve relative to MCP config location
        if not Path(config_path).is_absolute():
            config_path = mcp_config_path.parent / config_path

        return cls.from_odibi_yaml(str(config_path))

    @classmethod
    def from_odibi_yaml(cls, odibi_yaml_path: str) -> "MCPProjectContext":
        """Load project context from odibi.yaml."""
        config_path = Path(odibi_yaml_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Odibi config not found: {config_path}")

        with open(config_path) as f:
            config = yaml.safe_load(f)

        project_name = config.get("project", "unknown")

        # Extract story configuration
        story_config = config.get("story", {})
        story_connection = story_config.get("connection")
        story_path = story_config.get("path", "stories")

        # Extract catalog/system configuration
        system_config = config.get("system", {})
        catalog_connection = system_config.get("connection")

        ctx = cls(
            project_name=project_name,
            config_path=config_path,
            config=config,
            story_connection=story_connection,
            story_path=story_path,
            catalog_connection=catalog_connection,
        )

        return ctx

    @classmethod
    def from_exploration_config(cls, config_path: str) -> "MCPProjectContext":
        """
        Load project context in exploration mode - connections only.

        This is a lightweight mode for data discovery without requiring
        full pipeline, story, or system configuration.

        Example mcp_config.yaml:
        ```yaml
        project: my_exploration  # optional
        connections:
          my_sql:
            type: azure_sql
            connection_string: ${SQL_CONN}
          local:
            type: local
            path: ./data
        ```
        """
        path = Path(config_path)

        if not path.exists():
            raise FileNotFoundError(f"Exploration config not found: {path}")

        with open(path) as f:
            config = yaml.safe_load(f)

        # Validate minimal requirements
        if "connections" not in config:
            raise ValueError("Exploration config must have 'connections' section")

        project_name = config.get("project", "exploration")

        ctx = cls(
            project_name=project_name,
            config_path=path,
            config=config,
            story_connection=None,
            story_path=None,
            catalog_connection=None,
            mode="exploration",
        )

        logger.info(f"Loaded exploration mode config: {path}")
        return ctx

    def is_exploration_mode(self) -> bool:
        """Check if running in exploration mode (connections only)."""
        return self.mode == "exploration"

    def initialize_connections(self) -> None:
        """Initialize all connections from config."""
        if self._initialized:
            return

        from odibi.connections.factory import register_builtins
        from odibi.plugins import get_connection_factory, load_plugins

        # Register built-in connection factories
        register_builtins()
        load_plugins()

        connections_config = self.config.get("connections", {})

        for name, conn_config in connections_config.items():
            try:
                # Resolve env vars in connection config
                resolved_config = self._resolve_env_vars(conn_config)
                conn_type = resolved_config.get("type", "local")

                factory = get_connection_factory(conn_type)
                if factory:
                    conn = factory(name, resolved_config)
                    self.connections[name] = conn
                    logger.info(f"Initialized connection: {name}")
                else:
                    logger.warning(f"Unknown connection type: {conn_type}")
            except Exception as e:
                logger.warning(f"Failed to initialize connection {name}: {e}")

        self._initialized = True

    def _resolve_env_vars(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve ${ENV_VAR} placeholders in config values."""
        resolved = {}
        for key, value in config.items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                env_var = value[2:-1]
                resolved[key] = os.environ.get(env_var, value)
            elif isinstance(value, dict):
                resolved[key] = self._resolve_env_vars(value)
            else:
                resolved[key] = value
        return resolved

    def get_connection(self, name: str) -> Any:
        """Get an initialized connection by name."""
        if not self._initialized:
            self.initialize_connections()

        if name not in self.connections:
            raise KeyError(f"Connection not found: {name}")

        return self.connections[name]

    def get_story_base_path(self) -> Optional[Path]:
        """Get the base path for story files."""
        if not self.story_connection or not self.story_path:
            return None

        try:
            conn = self.get_connection(self.story_connection)
            return Path(conn.get_path(self.story_path))
        except Exception as e:
            logger.warning(f"Could not resolve story path: {e}")
            return None

    def get_pipelines(self) -> list:
        """Get list of pipeline configs (including from imports)."""
        pipelines = list(self.config.get("pipelines", []))

        # Also load from imports
        for import_path in self.config.get("imports", []):
            try:
                import_full_path = self.config_path.parent / import_path
                if import_full_path.exists():
                    with open(import_full_path) as f:
                        imported = yaml.safe_load(f)
                    if imported:
                        # Handle nested pipelines key (pipelines: [...])
                        if "pipelines" in imported and isinstance(imported["pipelines"], list):
                            for p in imported["pipelines"]:
                                p["_source"] = str(import_full_path)
                                pipelines.append(p)
                        else:
                            # The imported file is the pipeline config itself
                            imported["_source"] = str(import_full_path)
                            pipelines.append(imported)
            except Exception as e:
                logger.warning(f"Failed to load imported pipeline {import_path}: {e}")

        return pipelines

    def get_pipeline(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a specific pipeline config by name."""
        for pipeline in self.get_pipelines():
            # Match by 'pipeline' key (e.g., pipeline: bronze) or 'name' key
            pipeline_name = pipeline.get("pipeline", pipeline.get("name"))
            if pipeline_name == name:
                return pipeline
        return None


# Global context - initialized on server startup
_project_context: Optional[MCPProjectContext] = None


def get_project_context() -> Optional[MCPProjectContext]:
    """Get the global project context."""
    return _project_context


def set_project_context(ctx: MCPProjectContext) -> None:
    """Set the global project context."""
    global _project_context
    _project_context = ctx


def _is_full_project_config(config: Dict[str, Any]) -> bool:
    """Check if config has full project structure (pipelines, story, system)."""
    return all(key in config for key in ["pipelines", "story", "system"])


def _is_exploration_config(config: Dict[str, Any]) -> bool:
    """Check if config is exploration mode (connections only, no pipelines)."""
    return "connections" in config and "pipelines" not in config


def _load_config_auto(config_path: str) -> MCPProjectContext:
    """
    Auto-detect config type and load appropriately.

    - If config has pipelines/story/system -> full mode
    - If config has only connections -> exploration mode
    """
    path = Path(config_path)

    with open(path) as f:
        config = yaml.safe_load(f)

    if _is_full_project_config(config):
        logger.info(f"Detected full project config: {path}")
        return MCPProjectContext.from_odibi_yaml(str(path))
    elif _is_exploration_config(config):
        logger.info(f"Detected exploration config: {path}")
        return MCPProjectContext.from_exploration_config(str(path))
    else:
        # Has pipelines but missing story/system - try as full config
        # (will fail with helpful Pydantic error if incomplete)
        logger.info(f"Loading as project config: {path}")
        return MCPProjectContext.from_odibi_yaml(str(path))


def initialize_from_env() -> Optional[MCPProjectContext]:
    """Initialize project context from MCP_CONFIG or ODIBI_CONFIG env var."""
    global _project_context

    mcp_config = os.environ.get("MCP_CONFIG")
    odibi_config = os.environ.get("ODIBI_CONFIG")

    if mcp_config and Path(mcp_config).exists():
        logger.info(f"Loading MCP config from: {mcp_config}")
        _project_context = MCPProjectContext.from_mcp_config(mcp_config)
        _project_context.initialize_connections()
        return _project_context

    if odibi_config and Path(odibi_config).exists():
        logger.info(f"Loading config from: {odibi_config}")
        _project_context = _load_config_auto(odibi_config)
        _project_context.initialize_connections()
        return _project_context

    # Try default locations - check for exploration config first
    for default_path in ["./odibi.yaml", "./mcp_config.yaml", "./exploration.yaml"]:
        if Path(default_path).exists():
            logger.info(f"Loading config from default: {default_path}")
            _project_context = _load_config_auto(default_path)
            _project_context.initialize_connections()
            return _project_context

    logger.warning("No project config found. MCP tools will return empty results.")
    return None
