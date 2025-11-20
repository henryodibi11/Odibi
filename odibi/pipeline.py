"""Pipeline executor and orchestration."""

from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
import time
from pathlib import Path

from odibi.config import PipelineConfig, ProjectConfig, RetryConfig
from odibi.context import create_context
from odibi.graph import DependencyGraph
from odibi.node import Node, NodeResult
from odibi.engine import PandasEngine
from odibi.exceptions import DependencyError
from odibi.story import StoryGenerator
from odibi.connections import LocalConnection
from odibi.utils import load_yaml_with_env
from odibi.utils.logging import logger, configure_logging
from odibi.state import StateManager


@dataclass
class PipelineResults:
    """Results from pipeline execution."""

    pipeline_name: str
    completed: List[str] = field(default_factory=list)
    failed: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    node_results: Dict[str, NodeResult] = field(default_factory=dict)
    duration: float = 0.0
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    story_path: Optional[str] = None

    def get_node_result(self, name: str) -> Optional[NodeResult]:
        """Get result for specific node.

        Args:
            name: Node name

        Returns:
            NodeResult if available, None otherwise
        """
        return self.node_results.get(name)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "pipeline_name": self.pipeline_name,
            "completed": self.completed,
            "failed": self.failed,
            "skipped": self.skipped,
            "duration": self.duration,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "node_count": len(self.node_results),
        }


class Pipeline:
    """Pipeline executor and orchestrator."""

    def __init__(
        self,
        pipeline_config: PipelineConfig,
        engine: str = "pandas",
        connections: Optional[Dict[str, Any]] = None,
        generate_story: bool = True,
        story_config: Optional[Dict[str, Any]] = None,
        retry_config: Optional[RetryConfig] = None,
    ):
        """Initialize pipeline.

        Args:
            pipeline_config: Pipeline configuration
            engine: Engine type ('pandas' or 'spark')
            connections: Available connections
            generate_story: Whether to generate execution stories
            story_config: Story generator configuration
            retry_config: Retry configuration
        """
        self.config = pipeline_config
        self.engine_type = engine
        self.connections = connections or {}
        self.generate_story = generate_story
        self.retry_config = retry_config

        # Initialize story generator
        story_config = story_config or {}
        self.story_generator = StoryGenerator(
            pipeline_name=pipeline_config.pipeline,
            max_sample_rows=story_config.get("max_sample_rows", 10),
            output_path=story_config.get("output_path", "stories/"),
        )

        # Initialize engine
        if engine == "pandas":
            self.engine = PandasEngine()
        else:
            raise ValueError(f"Unsupported engine: {engine}")

        # Initialize context
        self.context = create_context(engine)

        # Build dependency graph
        self.graph = DependencyGraph(pipeline_config.nodes)

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "PipelineManager":
        """Create PipelineManager from YAML file (recommended).

        This method now returns a PipelineManager that can run all or specific pipelines.

        Args:
            yaml_path: Path to YAML configuration file

        Returns:
            PipelineManager instance (use .run() to execute)

        Example:
            >>> from odibi.pipeline import Pipeline
            >>> manager = Pipeline.from_yaml("config.yaml")
            >>> results = manager.run()  # Run all pipelines
            >>> results = manager.run('bronze_to_silver')  # Run specific pipeline

        Note:
            For direct access to PipelineManager class:
            >>> from odibi.pipeline import PipelineManager
            >>> manager = PipelineManager.from_yaml("config.yaml")
        """
        # Delegate to PipelineManager
        return PipelineManager.from_yaml(yaml_path)

    def run(
        self, parallel: bool = False, dry_run: bool = False, resume_from_failure: bool = False
    ) -> PipelineResults:
        """Execute the pipeline.

        Args:
            parallel: Whether to use parallel execution (not implemented yet)
            dry_run: Whether to simulate execution without running operations
            resume_from_failure: Whether to skip successfully completed nodes from last run

        Returns:
            PipelineResults with execution details
        """
        start_time = time.time()
        start_timestamp = datetime.now().isoformat()

        results = PipelineResults(pipeline_name=self.config.pipeline, start_time=start_timestamp)
        state_manager = StateManager() if resume_from_failure else None

        # Get execution order
        execution_order = self.graph.topological_sort()

        # Execute nodes in order
        for node_name in execution_order:
            # Skip if dependencies failed
            node_config = self.graph.nodes[node_name]
            deps_failed = any(dep in results.failed for dep in node_config.depends_on)

            if deps_failed:
                results.skipped.append(node_name)
                continue

            # Check for resume capability
            should_skip = False
            if resume_from_failure and state_manager:
                last_status = state_manager.get_last_run_status(self.config.pipeline, node_name)
                if last_status is True:
                    # Node succeeded last time. Can we restore state?
                    # We can only skip if we can restore the output context (if it was written)
                    # or if it produced no output context (e.g. pure side effect, though rare in ETL)

                    # If node has write, we can try to read it back to restore context
                    if node_config.write:
                        try:
                            # Create a temporary node instance to perform restore
                            temp_node = Node(
                                config=node_config,
                                context=self.context,
                                engine=self.engine,
                                connections=self.connections,
                            )
                            if temp_node.restore():
                                should_skip = True
                                logger.info(
                                    f"Resuming: Skipped successful node '{node_name}' (restored from storage)"
                                )
                        except Exception as e:
                            logger.warning(
                                f"Resuming: Could not restore node '{node_name}', re-running. Error: {e}"
                            )
                    else:
                        # Pure transform without write.
                        # We cannot skip this unless we know for sure no downstream node needs it.
                        # For safety in this phase, we re-run it.
                        # Future improvement: Check if any pending node depends on this.
                        logger.info(
                            f"Resuming: Re-running node '{node_name}' (in-memory transform cannot be restored)"
                        )

            if should_skip:
                results.completed.append(node_name)
                # Add a dummy result for the skipped node
                results.node_results[node_name] = NodeResult(
                    node_name=node_name,
                    success=True,
                    duration=0.0,
                    metadata={"skipped": True, "reason": "resume_from_failure"},
                )
                continue

            # Execute node
            node = Node(
                config=node_config,
                context=self.context,
                engine=self.engine,
                connections=self.connections,
                config_file=None,  # TODO: track config file
                dry_run=dry_run,
                retry_config=self.retry_config,
            )

            node_result = node.execute()
            results.node_results[node_name] = node_result

            if node_result.success:
                results.completed.append(node_name)
            else:
                results.failed.append(node_name)

        # Calculate duration
        results.duration = time.time() - start_time
        results.end_time = datetime.now().isoformat()

        # Save state if running normally (not dry run)
        if not dry_run:
            StateManager().save_pipeline_run(self.config.pipeline, results)

        # Generate story
        if self.generate_story:
            story_path = self.story_generator.generate(
                node_results=results.node_results,
                completed=results.completed,
                failed=results.failed,
                skipped=results.skipped,
                duration=results.duration,
                start_time=results.start_time,
                end_time=results.end_time,
                context=self.context,
            )
            results.story_path = story_path

        return results

    def run_node(self, node_name: str, mock_data: Optional[Dict[str, Any]] = None) -> NodeResult:
        """Execute a single node (for testing/debugging).

        Args:
            node_name: Name of node to execute
            mock_data: Optional mock data to register in context

        Returns:
            NodeResult
        """
        if node_name not in self.graph.nodes:
            raise ValueError(f"Node '{node_name}' not found in pipeline")

        # Register mock data if provided
        if mock_data:
            for name, data in mock_data.items():
                self.context.register(name, data)

        # Execute the node
        node_config = self.graph.nodes[node_name]
        node = Node(
            config=node_config,
            context=self.context,
            engine=self.engine,
            connections=self.connections,
        )

        return node.execute()

    def validate(self) -> Dict[str, Any]:
        """Validate pipeline without executing.

        Returns:
            Validation results
        """
        validation = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "node_count": len(self.graph.nodes),
            "execution_order": [],
        }

        try:
            # Test topological sort
            execution_order = self.graph.topological_sort()
            validation["execution_order"] = execution_order

        except DependencyError as e:
            validation["valid"] = False
            validation["errors"].append(str(e))

        # Check for missing connections
        for node in self.config.nodes:
            if node.read and node.read.connection not in self.connections:
                validation["warnings"].append(
                    f"Node '{node.name}': connection '{node.read.connection}' not configured"
                )
            if node.write and node.write.connection not in self.connections:
                validation["warnings"].append(
                    f"Node '{node.name}': connection '{node.write.connection}' not configured"
                )

        return validation

    def get_execution_layers(self) -> List[List[str]]:
        """Get nodes grouped by execution layers.

        Returns:
            List of layers, where each layer is a list of node names
        """
        return self.graph.get_execution_layers()

    def visualize(self) -> str:
        """Get text visualization of pipeline.

        Returns:
            String representation of pipeline graph
        """
        return self.graph.visualize()


class PipelineManager:
    """Manages multiple pipelines from a YAML configuration."""

    def __init__(
        self,
        project_config: ProjectConfig,
        connections: Dict[str, Any],
    ):
        """Initialize pipeline manager.

        Args:
            project_config: Validated project configuration
            connections: Connection objects (already instantiated)
        """
        self.project_config = project_config
        self.connections = connections
        self._pipelines: Dict[str, Pipeline] = {}

        # Configure logging
        configure_logging(
            structured=project_config.logging.structured, level=project_config.logging.level.value
        )

        # Get story configuration
        story_config = self._get_story_config()

        # Create all pipeline instances from project_config.pipelines
        for pipeline_config in project_config.pipelines:
            pipeline_name = pipeline_config.pipeline

            self._pipelines[pipeline_name] = Pipeline(
                pipeline_config=pipeline_config,
                engine=project_config.engine,
                connections=connections,
                story_config=story_config,
                retry_config=project_config.retry,
            )

    def _get_story_config(self) -> Dict[str, Any]:
        """Build story config from project_config.story.

        Resolves story output path using connection.

        Returns:
            Dictionary for StoryGenerator initialization
        """
        story_cfg = self.project_config.story

        # Resolve story path using connection
        story_conn = self.connections[story_cfg.connection]
        output_path = story_conn.get_path(story_cfg.path)

        return {
            "auto_generate": story_cfg.auto_generate,
            "max_sample_rows": story_cfg.max_sample_rows,
            "output_path": output_path,
        }

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "PipelineManager":
        """Create PipelineManager from YAML file.

        Args:
            yaml_path: Path to YAML configuration file

        Returns:
            PipelineManager instance ready to run pipelines

        Example:
            >>> manager = PipelineManager.from_yaml("config.yaml")
            >>> results = manager.run()  # Run all pipelines
        """
        # Load YAML with environment variable substitution
        yaml_path_obj = Path(yaml_path)
        # Path check is now handled inside load_yaml_with_env but we can keep/remove it.
        # load_yaml_with_env takes a str, so passing str(yaml_path_obj) is safer.

        try:
            config = load_yaml_with_env(str(yaml_path_obj))
        except FileNotFoundError:
            # Re-raise to maintain backward compatibility with existing error handling if needed
            # or just let it bubble up. The original raised FileNotFoundError manually.
            raise FileNotFoundError(f"YAML file not found: {yaml_path}")

        # Parse and validate entire YAML as ProjectConfig (single source of truth)
        project_config = ProjectConfig(**config)

        # Build connections from project_config.connections
        connections = cls._build_connections(project_config.connections)

        return cls(
            project_config=project_config,
            connections=connections,
        )

    @staticmethod
    def _build_connections(conn_configs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Convert connection configs to connection objects.

        Args:
            conn_configs: Connection configurations from ProjectConfig

        Returns:
            Dictionary of connection name -> connection object

        Raises:
            ValueError: If connection type is not supported
        """
        connections = {}
        for conn_name, conn_config in conn_configs.items():
            conn_type = conn_config.get("type", "local")

            if conn_type == "local":
                connections[conn_name] = LocalConnection(
                    base_path=conn_config.get("base_path", "./data")
                )
            elif conn_type == "azure_adls" or conn_type == "azure_blob":
                # Import here to avoid dependency issues
                try:
                    from odibi.connections.azure_adls import AzureADLS
                except ImportError:
                    raise ImportError(
                        "Azure ADLS support requires 'pip install odibi[azure]'. "
                        "See README.md for installation instructions."
                    )

                # Handle config discrepancies (config.py vs flat structure)
                # 1. Account name
                account = conn_config.get("account_name") or conn_config.get("account")
                if not account:
                    raise ValueError(f"Connection '{conn_name}' missing 'account_name'")

                # 2. Auth dictionary (preferred) vs flat structure (legacy)
                auth_config = conn_config.get("auth", {})

                # Extract auth details from auth dict OR top-level
                key_vault_name = auth_config.get("key_vault_name") or conn_config.get(
                    "key_vault_name"
                )
                secret_name = auth_config.get("secret_name") or conn_config.get("secret_name")
                account_key = auth_config.get("account_key") or conn_config.get("account_key")
                tenant_id = auth_config.get("tenant_id") or conn_config.get("tenant_id")
                client_id = auth_config.get("client_id") or conn_config.get("client_id")
                client_secret = auth_config.get("client_secret") or conn_config.get("client_secret")

                # Default auth mode if not specified
                auth_mode = conn_config.get("auth_mode", "key_vault")

                connections[conn_name] = AzureADLS(
                    account=account,
                    container=conn_config["container"],
                    path_prefix=conn_config.get("path_prefix", ""),
                    auth_mode=auth_mode,
                    key_vault_name=key_vault_name,
                    secret_name=secret_name,
                    account_key=account_key,
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=client_secret,
                    validate=conn_config.get("validate", True),
                )
            elif conn_type == "azure_sql" or conn_type == "sql_server":
                # Import AzureSQL
                try:
                    from odibi.connections.azure_sql import AzureSQL
                except ImportError:
                    raise ImportError(
                        "Azure SQL support requires 'pip install odibi[azure]'. "
                        "See README.md for installation instructions."
                    )

                # Extract config
                auth_config = conn_config.get("auth", {})
                username = auth_config.get("username") or conn_config.get("username")
                password = auth_config.get("password") or conn_config.get("password")

                connections[conn_name] = AzureSQL(
                    server=conn_config["server"],
                    database=conn_config["database"],
                    driver=conn_config.get("driver", "ODBC Driver 18 for SQL Server"),
                    username=username,
                    password=password,
                    auth_mode=conn_config.get("auth_mode", "aad_msi"),
                    port=conn_config.get("port", 1433),
                    timeout=conn_config.get("timeout", 30),
                )
            else:
                raise ValueError(
                    f"Unsupported connection type: {conn_type}. "
                    f"Supported types: local, azure_adls, azure_sql. "
                    f"See docs for connection setup."
                )

        return connections

    def run(
        self,
        pipelines: Optional[Union[str, List[str]]] = None,
        dry_run: bool = False,
        resume_from_failure: bool = False,
    ) -> Union[PipelineResults, Dict[str, PipelineResults]]:
        """Run one, multiple, or all pipelines.

        Args:
            pipelines: Pipeline name(s) to run.
            dry_run: Whether to simulate execution.
            resume_from_failure: Whether to skip successfully completed nodes from last run.

        Returns:
            PipelineResults or Dict of results
        """
        # Determine which pipelines to run
        if pipelines is None:
            # Run all pipelines in order
            pipeline_names = list(self._pipelines.keys())
        elif isinstance(pipelines, str):
            # Single pipeline
            pipeline_names = [pipelines]
        else:
            # List of pipelines
            pipeline_names = pipelines

        # Validate pipeline names
        for name in pipeline_names:
            if name not in self._pipelines:
                available = ", ".join(self._pipelines.keys())
                raise ValueError(f"Pipeline '{name}' not found. Available pipelines: {available}")

        # Run pipelines
        results = {}
        for name in pipeline_names:
            logger.info(f"Running pipeline: {name}")
            if dry_run:
                logger.info("Mode: DRY RUN (Simulation)")
            if resume_from_failure:
                logger.info("Mode: RESUME FROM FAILURE")

            results[name] = self._pipelines[name].run(
                dry_run=dry_run, resume_from_failure=resume_from_failure
            )

            # Print summary
            result = results[name]
            status = "SUCCESS" if not result.failed else "FAILED"
            if result.failed:
                logger.error(f"{status} - {name}", duration=f"{result.duration:.2f}s")
            else:
                logger.info(f"{status} - {name}", duration=f"{result.duration:.2f}s")

            logger.info(f"Completed: {len(result.completed)} nodes")
            if result.failed:
                logger.info(f"Failed: {len(result.failed)} nodes")

            if result.story_path:
                logger.info(f"Story: {result.story_path}")

        # Return single result if only one pipeline, otherwise dict
        if len(pipeline_names) == 1:
            return results[pipeline_names[0]]
        else:
            return results

    def list_pipelines(self) -> List[str]:
        """Get list of available pipeline names.

        Returns:
            List of pipeline names
        """
        return list(self._pipelines.keys())

    def get_pipeline(self, name: str) -> Pipeline:
        """Get a specific pipeline instance.

        Args:
            name: Pipeline name

        Returns:
            Pipeline instance

        Raises:
            ValueError: If pipeline not found
        """
        if name not in self._pipelines:
            available = ", ".join(self._pipelines.keys())
            raise ValueError(f"Pipeline '{name}' not found. Available: {available}")
        return self._pipelines[name]
