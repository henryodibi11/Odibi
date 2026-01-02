"""
Semantic Layer Runner
=====================

Orchestrates semantic layer execution including:
- Loading semantic configuration from ProjectConfig
- Executing views against SQL Server
- Generating semantic layer stories
- Generating combined lineage

Usage:
    runner = SemanticLayerRunner(project_config)
    result = runner.run()  # Uses connection from semantic config
"""

from typing import Any, Callable, Dict, Optional

from odibi.config import ProjectConfig
from odibi.semantics.metrics import SemanticLayerConfig, parse_semantic_config
from odibi.semantics.story import SemanticStoryGenerator, SemanticStoryMetadata
from odibi.story.lineage import LineageGenerator, LineageResult
from odibi.utils.logging_context import get_logging_context


class SemanticConfig:
    """Extended semantic config with connection info."""

    def __init__(self, config_dict: Dict[str, Any]):
        self.connection: Optional[str] = config_dict.get("connection")
        self.sql_output_path: Optional[str] = config_dict.get("sql_output_path")
        self.layer_config = parse_semantic_config(config_dict)


class SemanticLayerRunner:
    """
    Run semantic layer operations with story generation.

    Orchestrates the full semantic layer execution:
    1. Parse semantic config from project
    2. Execute views against SQL Server
    3. Generate semantic story (HTML + JSON)
    4. Optionally generate combined lineage

    Example:
        ```python
        runner = SemanticLayerRunner(project_config)
        result = runner.run(
            execute_sql=sql_conn.execute,
            save_sql_to="gold/views/",
            write_file=adls_write,
        )
        ```
    """

    def __init__(
        self,
        project_config: ProjectConfig,
        name: Optional[str] = None,
    ):
        """
        Initialize runner with project configuration.

        Args:
            project_config: ProjectConfig with semantic section
            name: Optional name for the semantic layer run
        """
        self.project_config = project_config
        self.name = name or f"{project_config.project}_semantic"

        self._semantic_ext: Optional[SemanticConfig] = None
        self._story_generator: Optional[SemanticStoryGenerator] = None
        self._last_metadata: Optional[SemanticStoryMetadata] = None

    @property
    def semantic_ext(self) -> SemanticConfig:
        """Get extended semantic configuration with connection info."""
        if self._semantic_ext is None:
            self._semantic_ext = self._parse_semantic_config()
        return self._semantic_ext

    @property
    def semantic_config(self) -> SemanticLayerConfig:
        """Get parsed semantic layer configuration."""
        return self.semantic_ext.layer_config

    @property
    def connection_name(self) -> Optional[str]:
        """Get the SQL Server connection name for views."""
        return self.semantic_ext.connection

    @property
    def sql_output_path(self) -> Optional[str]:
        """Get the path for saving SQL files."""
        return self.semantic_ext.sql_output_path

    def _parse_semantic_config(self) -> SemanticConfig:
        """Parse semantic config from project config."""
        ctx = get_logging_context()

        semantic_dict = self.project_config.semantic
        if not semantic_dict:
            ctx.warning("No semantic configuration found in project config")
            return SemanticConfig({})

        ctx.debug(
            "Parsing semantic config",
            keys=list(semantic_dict.keys()),
            connection=semantic_dict.get("connection"),
            metrics_count=len(semantic_dict.get("metrics", [])),
            views_count=len(semantic_dict.get("views", [])),
        )

        return SemanticConfig(semantic_dict)

    def run(
        self,
        execute_sql: Optional[Callable[[str], None]] = None,
        save_sql_to: Optional[str] = None,
        write_file: Optional[Callable[[str, str], None]] = None,
        generate_story: Optional[bool] = None,
        generate_lineage: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Execute the semantic layer.

        Args:
            execute_sql: Callable that executes SQL. If not provided, uses the
                connection specified in semantic.connection config.
            save_sql_to: Path to save SQL files. If not provided, uses
                semantic.sql_output_path from config.
            write_file: Optional callable to write files (for remote storage)
            generate_story: Whether to generate execution story
            generate_lineage: Whether to generate combined lineage

        Returns:
            Dict with execution results including:
            - views_created: List of created view names
            - views_failed: List of failed view names
            - duration: Total execution time
            - story_paths: Dict with json/html paths if story generated
            - lineage_paths: Dict with json/html paths if lineage generated
        """
        ctx = get_logging_context()

        if execute_sql is None:
            execute_sql = self._get_execute_sql_from_connection()

        if save_sql_to is None:
            save_sql_to = self.sql_output_path

        # Auto-create write_file using story connection if sql_output_path is set
        if write_file is None and save_sql_to:
            write_file = self._get_write_file_from_story_connection()
            if write_file:
                ctx.info("Using story connection for SQL file output", path=save_sql_to)

        # Read defaults from story config if not explicitly provided
        if generate_story is None:
            generate_story = self.project_config.story.auto_generate
        if generate_lineage is None:
            generate_lineage = self.project_config.story.generate_lineage

        ctx.info(
            "Starting semantic layer execution",
            name=self.name,
            connection=self.connection_name,
            views_count=len(self.semantic_config.views),
        )

        result = {
            "views_created": [],
            "views_failed": [],
            "duration": 0.0,
            "story_paths": None,
            "lineage_paths": None,
            "connection": self.connection_name,
        }

        if not self.semantic_config.views:
            ctx.warning("No views defined in semantic config")
            return result

        stories_path = self.project_config.story.path
        storage_options = self._get_storage_options()

        self._story_generator = SemanticStoryGenerator(
            config=self.semantic_config,
            name=self.name,
            output_path=stories_path,
            storage_options=storage_options,
        )

        metadata = self._story_generator.execute_with_story(
            execute_sql=execute_sql,
            save_sql_to=save_sql_to,
            write_file=write_file,
        )
        self._last_metadata = metadata

        result["views_created"] = [v.view_name for v in metadata.views if v.status == "success"]
        result["views_failed"] = [v.view_name for v in metadata.views if v.status == "failed"]
        result["duration"] = metadata.duration

        if generate_story:
            story_paths = self._story_generator.save_story(write_file=write_file)
            result["story_paths"] = story_paths
            ctx.info("Semantic story saved", paths=story_paths)

        if generate_lineage:
            lineage_result = self._generate_lineage(write_file)
            if lineage_result:
                result["lineage_paths"] = {
                    "json": lineage_result.json_path,
                    "html": lineage_result.html_path,
                }

        ctx.info(
            "Semantic layer execution complete",
            views_created=len(result["views_created"]),
            views_failed=len(result["views_failed"]),
            duration=result["duration"],
        )

        return result

    def _get_execute_sql_from_connection(self) -> Callable[[str], None]:
        """Get an execute_sql callable from the configured connection."""
        ctx = get_logging_context()

        if not self.connection_name:
            raise ValueError(
                "No execute_sql provided and no connection specified in semantic config. "
                "Either pass execute_sql to run() or add 'connection: your_sql_conn' to semantic config."
            )

        conn_config = self.project_config.connections.get(self.connection_name)
        if not conn_config:
            available = ", ".join(self.project_config.connections.keys())
            raise ValueError(
                f"Semantic connection '{self.connection_name}' not found. Available: {available}"
            )

        ctx.info(
            "Creating SQL executor from connection",
            connection=self.connection_name,
            type=str(conn_config.type),
        )

        from odibi.connections.azure_sql import AzureSQL

        server = getattr(conn_config, "host", None) or getattr(conn_config, "server", None)
        database = getattr(conn_config, "database", None)
        port = getattr(conn_config, "port", 1433)

        if not server or not database:
            raise ValueError(
                f"Connection '{self.connection_name}' missing required 'host' or 'database'. "
                f"Available fields: {list(conn_config.model_fields_set) if hasattr(conn_config, 'model_fields_set') else 'unknown'}"
            )

        auth_mode = "aad_msi"
        username = None
        password = None

        if hasattr(conn_config, "auth") and conn_config.auth:
            auth = conn_config.auth
            mode = getattr(auth, "mode", None)
            if mode:
                auth_mode = mode.value if hasattr(mode, "value") else str(mode)
            username = getattr(auth, "username", None)
            password = getattr(auth, "password", None)
        else:
            username = getattr(conn_config, "username", None)
            password = getattr(conn_config, "password", None)
            if username and password:
                auth_mode = "sql_login"

        sql_conn = AzureSQL(
            server=server,
            database=database,
            port=port,
            auth_mode=auth_mode,
            username=username,
            password=password,
        )

        return sql_conn.execute

    def _generate_lineage(
        self,
        write_file: Optional[Callable[[str, str], None]] = None,
    ) -> Optional[LineageResult]:
        """Generate combined lineage from all stories."""
        ctx = get_logging_context()

        stories_path = self.project_config.story.path
        storage_options = self._get_storage_options()
        blob_base_url = self._get_blob_base_url()

        try:
            lineage_gen = LineageGenerator(
                stories_path=stories_path,
                storage_options=storage_options,
                blob_base_url=blob_base_url,
            )
            result = lineage_gen.generate()
            lineage_gen.save(result, write_file=write_file)
            return result
        except Exception as e:
            ctx.warning(f"Failed to generate lineage: {e}")
            return None

    def _get_blob_base_url(self) -> Optional[str]:
        """
        Construct the HTTPS blob URL base for story links.

        Returns URL like: https://account.blob.core.windows.net/container/stories
        """
        story_conn_name = self.project_config.story.connection
        story_conn = self.project_config.connections.get(story_conn_name)

        if not story_conn:
            return None

        account_name = getattr(story_conn, "account_name", None)
        container = getattr(story_conn, "container", None)
        stories_path = self.project_config.story.path

        if account_name and container:
            base = f"https://{account_name}.blob.core.windows.net/{container}"
            if stories_path:
                base = f"{base}/{stories_path.strip('/')}"
            return base

        return None

    def _get_storage_options(self) -> Dict[str, Any]:
        """
        Get storage options from story connection for fsspec/adlfs.

        Handles all Azure auth modes:
        - account_key / direct_key: Returns account_key for fsspec
        - sas: Returns sas_token for fsspec
        - connection_string: Returns connection_string for fsspec
        - aad_msi / managed_identity: Returns empty dict (uses default Azure credential)
        - key_vault: Would need to fetch secret (not implemented here)
        """
        story_conn_name = self.project_config.story.connection
        story_conn = self.project_config.connections.get(story_conn_name)

        if not story_conn:
            return {}

        # Check for direct credentials on connection
        if hasattr(story_conn, "credentials") and story_conn.credentials:
            return dict(story_conn.credentials)
        if hasattr(story_conn, "account_key") and story_conn.account_key:
            return {"account_key": story_conn.account_key}
        if hasattr(story_conn, "sas_token") and story_conn.sas_token:
            return {"sas_token": story_conn.sas_token}

        # Check nested auth structure
        if hasattr(story_conn, "auth") and story_conn.auth:
            auth = story_conn.auth

            # Helper to get value from auth (handles both dict and Pydantic model)
            def get_auth_value(key: str):
                if isinstance(auth, dict):
                    return auth.get(key)
                return getattr(auth, key, None)

            auth_mode = get_auth_value("mode")
            if auth_mode:
                mode_value = auth_mode.value if hasattr(auth_mode, "value") else str(auth_mode)
            else:
                mode_value = None

            # account_key or direct_key mode
            account_key = get_auth_value("account_key")
            if account_key:
                return {"account_key": account_key}

            # SAS token mode
            sas_token = get_auth_value("sas_token")
            if sas_token:
                return {"sas_token": sas_token}

            # Connection string mode
            connection_string = get_auth_value("connection_string")
            if connection_string:
                return {"connection_string": connection_string}

            # MSI / managed identity - uses DefaultAzureCredential, no explicit creds needed
            if mode_value in ("aad_msi", "managed_identity"):
                # Return account_name for adlfs to use with DefaultAzureCredential
                account_name = getattr(story_conn, "account_name", None)
                if account_name:
                    return {"account_name": account_name}
                return {}

            # Key Vault mode - would need to fetch from Key Vault
            if mode_value == "key_vault":
                ctx = get_logging_context()
                ctx.warning(
                    "Key Vault auth for SQL output not yet implemented. "
                    "Consider using direct_key or aad_msi for story connection."
                )
                return {}

        return {}

    def _get_write_file_from_story_connection(self) -> Optional[Callable[[str, str], None]]:
        """
        Create a write_file callback using the story connection.

        Returns a callable that writes files to the story connection's storage,
        or None if no valid connection is available.
        """
        ctx = get_logging_context()
        storage_options = self._get_storage_options()

        story_conn_name = self.project_config.story.connection
        story_conn = self.project_config.connections.get(story_conn_name)

        if not story_conn:
            ctx.debug("No story connection found", connection=story_conn_name)
            return None

        conn_type = getattr(story_conn, "type", None)
        if conn_type is None:
            ctx.debug("Story connection has no type")
            return None

        conn_type_value = conn_type.value if hasattr(conn_type, "value") else str(conn_type)

        if conn_type_value == "local":
            base_path = getattr(story_conn, "base_path", "./data")

            def write_file_local(path: str, content: str) -> None:
                import os

                full_path = os.path.join(base_path, path)
                os.makedirs(os.path.dirname(full_path), exist_ok=True)
                ctx.debug("Writing SQL file locally", path=full_path)
                with open(full_path, "w") as f:
                    f.write(content)

            return write_file_local

        elif conn_type_value in ("azure_blob", "delta"):
            if not storage_options:
                ctx.debug("No storage options available for Azure write_file")
                return None

            account_name = getattr(story_conn, "account_name", None)
            container = getattr(story_conn, "container", None)

            if not account_name or not container:
                ctx.debug("Azure connection missing account_name or container")
                return None

            def write_file_azure(path: str, content: str) -> None:
                import fsspec

                if path.startswith(("abfs://", "az://")):
                    full_path = path
                else:
                    full_path = f"abfs://{container}@{account_name}.dfs.core.windows.net/{path}"

                # adlfs needs account_name along with credentials
                fs_options = {"account_name": account_name, **storage_options}
                fs = fsspec.filesystem("abfs", **fs_options)
                ctx.debug("Writing SQL file via Azure", path=full_path)
                with fs.open(full_path, "w") as f:
                    f.write(content)

            return write_file_azure

        else:
            ctx.debug("Unsupported connection type for write_file", type=conn_type_value)
            return None

    @property
    def metadata(self) -> Optional[SemanticStoryMetadata]:
        """Get the last execution metadata."""
        return self._last_metadata


def run_semantic_layer(
    project_config: ProjectConfig,
    execute_sql: Callable[[str], None],
    save_sql_to: Optional[str] = None,
    write_file: Optional[Callable[[str, str], None]] = None,
    generate_story: bool = True,
    generate_lineage: bool = False,
) -> Dict[str, Any]:
    """
    Convenience function to run semantic layer from project config.

    Args:
        project_config: ProjectConfig with semantic section
        execute_sql: Callable that executes SQL against the database
        save_sql_to: Optional path to save SQL files
        write_file: Optional callable to write files
        generate_story: Whether to generate execution story
        generate_lineage: Whether to generate combined lineage

    Returns:
        Dict with execution results
    """
    runner = SemanticLayerRunner(project_config)
    return runner.run(
        execute_sql=execute_sql,
        save_sql_to=save_sql_to,
        write_file=write_file,
        generate_story=generate_story,
        generate_lineage=generate_lineage,
    )
