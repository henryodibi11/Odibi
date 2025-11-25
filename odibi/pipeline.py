"""Pipeline executor and orchestration."""

import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import odibi.transformers  # noqa: F401 # Register built-in transformers
from odibi.config import AlertConfig, ErrorStrategy, PipelineConfig, ProjectConfig, RetryConfig
from odibi.connections import LocalConnection
from odibi.context import create_context
from odibi.engine.registry import get_engine_class
from odibi.exceptions import DependencyError
from odibi.graph import DependencyGraph
from odibi.node import Node, NodeResult
from odibi.plugins import get_connection_factory, load_plugins
from odibi.state import StateManager
from odibi.story import StoryGenerator
from odibi.utils import load_yaml_with_env
from odibi.utils.alerting import send_alert
from odibi.utils.logging import configure_logging, logger


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
        alerts: Optional[List[AlertConfig]] = None,
        performance_config: Optional[Any] = None,
    ):
        """Initialize pipeline.

        Args:
            pipeline_config: Pipeline configuration
            engine: Engine type ('pandas' or 'spark')
            connections: Available connections
            generate_story: Whether to generate execution stories
            story_config: Story generator configuration
            retry_config: Retry configuration
            alerts: Alert configurations
            performance_config: Performance tuning configuration
        """
        self.config = pipeline_config
        self.project_config = None  # Set by PipelineManager if available
        self.engine_type = engine
        self.connections = connections or {}
        self.generate_story = generate_story
        self.retry_config = retry_config
        self.alerts = alerts or []
        self.performance_config = performance_config

        # Initialize story generator
        story_config = story_config or {}

        self.story_generator = StoryGenerator(
            pipeline_name=pipeline_config.pipeline,
            max_sample_rows=story_config.get("max_sample_rows", 10),
            output_path=story_config.get("output_path", "stories/"),
            storage_options=story_config.get("storage_options", {}),
        )

        # Initialize engine
        engine_config = {}
        if performance_config:
            engine_config["performance"] = (
                performance_config.dict()
                if hasattr(performance_config, "dict")
                else performance_config
            )

        try:
            EngineClass = get_engine_class(engine)
        except ValueError as e:
            # Handle Spark special case message
            if engine == "spark":
                raise ImportError(
                    "Spark engine not available. "
                    "Install with 'pip install odibi[spark]' or ensure pyspark is installed."
                )
            raise e

        if engine == "spark":
            # SparkEngine can take existing session if needed, but here we let it create/get one
            # We might need to pass connections to it for ADLS auth config
            self.engine = EngineClass(connections=connections, config=engine_config)
        else:
            self.engine = EngineClass(config=engine_config)

        # Initialize context
        spark_session = getattr(self.engine, "spark", None)
        self.context = create_context(engine, spark_session=spark_session)

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
        self,
        parallel: bool = False,
        dry_run: bool = False,
        resume_from_failure: bool = False,
        max_workers: int = 4,
    ) -> PipelineResults:
        """Execute the pipeline.

        Args:
            parallel: Whether to use parallel execution
            dry_run: Whether to simulate execution without running operations
            resume_from_failure: Whether to skip successfully completed nodes from last run
            max_workers: Maximum number of parallel threads (default: 4)

        Returns:
            PipelineResults with execution details
        """
        start_time = time.time()
        start_timestamp = datetime.now().isoformat()

        results = PipelineResults(pipeline_name=self.config.pipeline, start_time=start_timestamp)

        # Alert: on_start
        self._send_alerts("on_start", results)

        state_manager = StateManager() if resume_from_failure else None

        # Define node processing function (inner function to capture self/context)
        def process_node(node_name: str) -> NodeResult:
            # Check dependencies (thread-safe check on results object? No, need synchronization if strictly parallel)
            # But since we execute by layers, dependencies (previous layers) are guaranteed to be done.
            # So we only need to check if any dependency FAILED.

            node_config = self.graph.nodes[node_name]
            deps_failed = any(dep in results.failed for dep in node_config.depends_on)

            if deps_failed:
                return NodeResult(
                    node_name=node_name,
                    success=False,
                    duration=0.0,
                    metadata={"skipped": True, "reason": "dependency_failed"},
                )

            # Check for resume capability
            if resume_from_failure and state_manager:
                last_status = state_manager.get_last_run_status(self.config.pipeline, node_name)
                if last_status is True:
                    # Try to restore
                    if node_config.write:
                        try:
                            temp_node = Node(
                                config=node_config,
                                context=self.context,
                                engine=self.engine,
                                connections=self.connections,
                            )
                            if temp_node.restore():
                                logger.info(f"Resuming: Skipped node '{node_name}' (restored)")
                                return NodeResult(
                                    node_name=node_name,
                                    success=True,
                                    duration=0.0,
                                    metadata={"skipped": True, "reason": "resume_from_failure"},
                                )
                        except Exception as e:
                            logger.warning(f"Resuming: Could not restore '{node_name}': {e}")
                    else:
                        # Cannot restore pure transform safely without more logic
                        logger.info(f"Resuming: Re-running '{node_name}' (in-memory)")

            # Execute node
            try:
                node = Node(
                    config=node_config,
                    context=self.context,
                    engine=self.engine,
                    connections=self.connections,
                    dry_run=dry_run,
                    retry_config=self.retry_config,
                )
                return node.execute()
            except Exception as e:
                logger.error(f"Node '{node_name}' failed: {e}")
                return NodeResult(node_name=node_name, success=False, duration=0.0, error=str(e))

        if parallel:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            # Get execution layers (nodes in a layer are independent)
            layers = self.graph.get_execution_layers()

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for layer in layers:
                    # Submit all nodes in layer
                    future_to_node = {
                        executor.submit(process_node, node_name): node_name for node_name in layer
                    }

                    # Wait for layer to complete
                    layer_failed = False
                    for future in as_completed(future_to_node):
                        node_name = future_to_node[future]
                        try:
                            result = future.result()
                            results.node_results[node_name] = result

                            if result.success:
                                # specific check for skipped result
                                if result.metadata.get("skipped"):
                                    if result.metadata.get("reason") == "dependency_failed":
                                        results.skipped.append(node_name)
                                        # Treat dependency failure as failure for downstream?
                                        # Logic says if node skipped due to deps, it didn't "fail" execution,
                                        # but downstream will also skip.
                                        # Wait, process_node logic returns success=False for dep failure above?
                                        # Let's fix process_node return values.
                                    else:
                                        results.completed.append(node_name)
                                else:
                                    results.completed.append(node_name)
                            else:
                                if result.metadata.get("skipped"):
                                    results.skipped.append(node_name)
                                    # Add to failed so downstream skips?
                                    results.failed.append(node_name)
                                else:
                                    results.failed.append(node_name)
                                    layer_failed = True

                                    # Check Fail Fast
                                    node_config = self.graph.nodes[node_name]
                                    if node_config.on_error == ErrorStrategy.FAIL_FAST:
                                        logger.error(
                                            f"FAIL_FAST: Node '{node_name}' failed. Stopping pipeline."
                                        )
                                        executor.shutdown(cancel_futures=True, wait=False)
                                        break

                        except Exception as exc:
                            logger.error(f"Node '{node_name}' generated exception: {exc}")
                            results.failed.append(node_name)
                            layer_failed = True

                            # Check Fail Fast
                            node_config = self.graph.nodes[node_name]
                            if node_config.on_error == ErrorStrategy.FAIL_FAST:
                                logger.error(
                                    f"FAIL_FAST: Node '{node_name}' failed. Stopping pipeline."
                                )
                                executor.shutdown(cancel_futures=True, wait=False)
                                break

                    # If any node in layer failed, we continue to next layer (unless fail_fast triggered break)
                    if layer_failed:
                        # Check if any failure was fail_fast (if we broke loop above)
                        # We need to check if we should stop completely
                        # Actually, if we broke the inner loop, we are here.
                        # We need to break outer loop too if fail_fast happened.
                        # Let's check results for fail_fast trigger?
                        # Or just check if executor is shutdown? Hard to check.
                        # Let's check the failed nodes' config.
                        for failed_node in results.failed:
                            if self.graph.nodes[failed_node].on_error == ErrorStrategy.FAIL_FAST:
                                return results

        else:
            # Serial execution (existing logic refactored to use process_node for consistency?)
            # Or keep original robust logic?
            # Let's keep original linear execution but use layers to be consistent,
            # or use topological sort which is safer for serial.
            execution_order = self.graph.topological_sort()
            for node_name in execution_order:
                result = process_node(node_name)
                results.node_results[node_name] = result

                if result.success:
                    if (
                        result.metadata.get("skipped")
                        and result.metadata.get("reason") == "dependency_failed"
                    ):
                        results.skipped.append(node_name)
                        results.failed.append(node_name)  # Mark as failed so downstream skips
                    else:
                        results.completed.append(node_name)
                else:
                    if result.metadata.get("skipped"):
                        results.skipped.append(node_name)
                        results.failed.append(node_name)
                    else:
                        results.failed.append(node_name)

                        # Check Fail Fast
                        node_config = self.graph.nodes[node_name]
                        if node_config.on_error == ErrorStrategy.FAIL_FAST:
                            logger.error(
                                f"FAIL_FAST: Node '{node_name}' failed. Stopping pipeline."
                            )
                            break

        # Calculate duration
        results.duration = time.time() - start_time
        results.end_time = datetime.now().isoformat()

        # Save state if running normally (not dry run)
        if not dry_run:
            StateManager().save_pipeline_run(self.config.pipeline, results)

        # Generate story
        if self.generate_story:
            # Convert config to JSON-compatible dict (resolves Enums to strings)
            if hasattr(self.config, "model_dump"):
                config_dump = self.config.model_dump(mode="json")
            else:
                config_dump = self.config.dict()

            # Merge project-level config if available
            if self.project_config:
                project_dump = (
                    self.project_config.model_dump(mode="json")
                    if hasattr(self.project_config, "model_dump")
                    else self.project_config.dict()
                )
                # Merge relevant fields
                for field in ["project", "plant", "asset", "business_unit", "layer"]:
                    if field in project_dump and project_dump[field]:
                        config_dump[field] = project_dump[field]

            story_path = self.story_generator.generate(
                node_results=results.node_results,
                completed=results.completed,
                failed=results.failed,
                skipped=results.skipped,
                duration=results.duration,
                start_time=results.start_time,
                end_time=results.end_time,
                context=self.context,
                config=config_dump,
            )
            results.story_path = story_path

        # Alert: on_success / on_failure
        if results.failed:
            self._send_alerts("on_failure", results)
        else:
            self._send_alerts("on_success", results)

        return results

    def _send_alerts(self, event: str, results: PipelineResults) -> None:
        """Send alerts for a specific event.

        Args:
            event: Event name (on_start, on_success, on_failure)
            results: Pipeline results
        """
        for alert_config in self.alerts:
            if event in alert_config.on_events:
                status = "FAILED" if results.failed else "SUCCESS"
                if event == "on_start":
                    status = "STARTED"

                context = {
                    "pipeline": self.config.pipeline,
                    "status": status,
                    "duration": results.duration,
                    "timestamp": datetime.now().isoformat(),
                    "project_config": self.project_config,
                }

                msg = f"Pipeline '{self.config.pipeline}' {status}"
                if results.failed:
                    msg += f". Failed nodes: {', '.join(results.failed)}"

                send_alert(alert_config, msg, context)

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
                generate_story=story_config.get("auto_generate", True),
                story_config=story_config,
                retry_config=project_config.retry,
                alerts=project_config.alerts,
                performance_config=project_config.performance,
            )
            # Inject project config into pipeline for richer context
            self._pipelines[pipeline_name].project_config = project_config

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

        # Get storage options (e.g., credentials) from connection if available
        storage_options = {}
        if hasattr(story_conn, "pandas_storage_options"):
            storage_options = story_conn.pandas_storage_options()

        return {
            "auto_generate": story_cfg.auto_generate,
            "max_sample_rows": story_cfg.max_sample_rows,
            "output_path": output_path,
            "storage_options": storage_options,
        }

    @classmethod
    def from_yaml(cls, yaml_path: str, env: str = None) -> "PipelineManager":
        """Create PipelineManager from YAML file.

        Args:
            yaml_path: Path to YAML configuration file
            env: Environment name to apply overrides (e.g. 'prod')

        Returns:
            PipelineManager instance ready to run pipelines

        Example:
            >>> manager = PipelineManager.from_yaml("config.yaml", env="prod")
            >>> results = manager.run()  # Run all pipelines
        """
        # Load YAML with environment variable substitution
        yaml_path_obj = Path(yaml_path)
        config_dir = yaml_path_obj.parent.absolute()

        # --- Auto-Discovery of Custom Transforms ---
        # Check for 'transforms.py' in config directory and CWD
        import importlib.util
        import os
        import sys

        def load_transforms_module(path):
            if os.path.exists(path):
                try:
                    spec = importlib.util.spec_from_file_location("transforms_autodiscovered", path)
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        sys.modules["transforms_autodiscovered"] = module
                        spec.loader.exec_module(module)
                        logger.info(f"Auto-loaded transforms from: {path}")
                except Exception as e:
                    logger.warning(f"Failed to auto-load transforms from {path}: {e}")

        # 1. Config Directory
        load_transforms_module(os.path.join(config_dir, "transforms.py"))

        # 2. Current Working Directory (if different)
        cwd = os.getcwd()
        if os.path.abspath(cwd) != str(config_dir):
            load_transforms_module(os.path.join(cwd, "transforms.py"))

        # Path check is now handled inside load_yaml_with_env but we can keep/remove it.
        # load_yaml_with_env takes a str, so passing str(yaml_path_obj) is safer.

        try:
            config = load_yaml_with_env(str(yaml_path_obj), env=env)
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

        # Load plugins
        load_plugins()

        # Build initial connection objects (without prefetching secrets)
        for conn_name, conn_config in conn_configs.items():
            conn_type = conn_config.get("type", "local")

            # Check plugins
            factory = get_connection_factory(conn_type)
            if factory:
                connections[conn_name] = factory(conn_name, conn_config)
                continue

            if conn_type == "local":
                connections[conn_name] = LocalConnection(
                    base_path=conn_config.get("base_path", "./data")
                )
            elif conn_type == "http":
                from odibi.connections.http import HttpConnection

                connections[conn_name] = HttpConnection(
                    base_url=conn_config.get("base_url", ""),
                    headers=conn_config.get("headers"),
                    auth=conn_config.get("auth"),
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
                sas_token = auth_config.get("sas_token") or conn_config.get("sas_token")
                tenant_id = auth_config.get("tenant_id") or conn_config.get("tenant_id")
                client_id = auth_config.get("client_id") or conn_config.get("client_id")
                client_secret = auth_config.get("client_secret") or conn_config.get("client_secret")

                # Default auth mode if not specified
                auth_mode = conn_config.get("auth_mode", "key_vault")

                # Auto-detect auth_mode if not specified but sas_token is present
                if "auth_mode" not in conn_config:
                    if sas_token:
                        auth_mode = "sas_token"
                    elif key_vault_name and secret_name:
                        auth_mode = "key_vault"
                    elif account_key:
                        auth_mode = "direct_key"
                    elif tenant_id and client_id and client_secret:
                        auth_mode = "service_principal"
                    else:
                        # Default to managed_identity if no specific credentials provided
                        # This supports the "Default (Recommended)" option in the template
                        auth_mode = "managed_identity"

                # Check validation_mode (eager vs lazy default)
                validation_mode = conn_config.get("validation_mode", "lazy")
                validate = conn_config.get("validate")

                if validate is None:
                    # If not explicitly set, derive from validation_mode
                    validate = True if validation_mode == "eager" else False

                # Register secrets for redaction
                if account_key:
                    logger.register_secret(account_key)
                if sas_token:
                    logger.register_secret(sas_token)
                if client_secret:
                    logger.register_secret(client_secret)

                connections[conn_name] = AzureADLS(
                    account=account,
                    container=conn_config["container"],
                    path_prefix=conn_config.get("path_prefix", ""),
                    auth_mode=auth_mode,
                    key_vault_name=key_vault_name,
                    secret_name=secret_name,
                    account_key=account_key,
                    sas_token=sas_token,
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=client_secret,
                    validate=validate,
                )
            elif conn_type == "delta":
                # Check if we have support
                try:
                    # For local delta, we need 'deltalake' (pandas) or 'delta-spark' (spark)
                    # But connection object itself might just be a holder for path/catalog config
                    # Since we don't have a dedicated DeltaConnection class yet in the core,
                    # we can use a generic config holder or simple dict if the engine handles it.
                    # However, to be clean, let's assume we treat it as "local" with special validation
                    # if it's path-based, or just pass through if catalog-based.

                    # Actually, better to use LocalConnection for path-based delta if no specific class
                    if "path" in conn_config:
                        connections[conn_name] = LocalConnection(
                            base_path=conn_config.get("path") or conn_config.get("base_path")
                        )
                    else:
                        # Catalog based (Spark only)
                        # We can create a dummy connection object that just holds the catalog/schema info
                        # so the Engine can read it.
                        from odibi.connections.base import BaseConnection

                        class DeltaCatalogConnection(BaseConnection):
                            def __init__(self, catalog, schema):
                                self.catalog = catalog
                                self.schema = schema

                            def get_path(self, table):
                                return f"{self.catalog}.{self.schema}.{table}"

                            def validate(self):
                                pass

                            def pandas_storage_options(self):
                                return {}

                        connections[conn_name] = DeltaCatalogConnection(
                            catalog=conn_config.get("catalog"),
                            schema=conn_config.get("schema") or "default",
                        )

                except Exception as e:
                    raise ValueError(f"Failed to initialize Delta connection '{conn_name}': {e}")

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
                # Support 'host' (new standard) or 'server' (legacy)
                server = conn_config.get("host") or conn_config.get("server")
                if not server:
                    raise ValueError(f"Connection '{conn_name}' missing 'host' or 'server'")

                auth_config = conn_config.get("auth", {})
                username = auth_config.get("username") or conn_config.get("username")
                password = auth_config.get("password") or conn_config.get("password")
                key_vault_name = auth_config.get("key_vault_name") or conn_config.get(
                    "key_vault_name"
                )
                secret_name = auth_config.get("secret_name") or conn_config.get("secret_name")

                # Auto-detect auth_mode for SQL if not specified
                auth_mode = conn_config.get("auth_mode")
                if not auth_mode:
                    if username and password:
                        auth_mode = "sql"
                    elif key_vault_name and secret_name and username:
                        auth_mode = "key_vault"
                    else:
                        auth_mode = "aad_msi"

                # Register secret
                if password:
                    logger.register_secret(password)

                connections[conn_name] = AzureSQL(
                    server=server,
                    database=conn_config["database"],
                    driver=conn_config.get("driver", "ODBC Driver 18 for SQL Server"),
                    username=username,
                    password=password,
                    auth_mode=auth_mode,
                    key_vault_name=key_vault_name,
                    secret_name=secret_name,
                    port=conn_config.get("port", 1433),
                    timeout=conn_config.get("timeout", 30),
                )
            else:
                raise ValueError(
                    f"Unsupported connection type: {conn_type}. "
                    f"Supported types: local, azure_adls, azure_sql. "
                    f"See docs for connection setup."
                )

        # Parallel Key Vault fetch (Optimization)
        # We run this after all connections are instantiated to batch the requests
        try:
            from odibi.utils import configure_connections_parallel

            connections, errors = configure_connections_parallel(connections, verbose=False)
            if errors:
                for error in errors:
                    logger.warning(error)
        except ImportError:
            # Optional utility, skip if not available or fails
            pass

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
