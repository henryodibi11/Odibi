"""Pipeline executor and orchestration."""

import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from odibi.config import AlertConfig, ErrorStrategy, PipelineConfig, ProjectConfig, RetryConfig
from odibi.context import create_context
from odibi.engine.registry import get_engine_class
from odibi.exceptions import DependencyError
from odibi.graph import DependencyGraph
from odibi.lineage import OpenLineageAdapter
from odibi.node import Node, NodeResult
from odibi.plugins import get_connection_factory, load_plugins
from odibi.registry import FunctionRegistry
from odibi.state import StateManager, create_state_backend
from odibi.story import StoryGenerator
from odibi.transformers import register_standard_library
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
        catalog_manager: Optional[Any] = None,
        lineage_adapter: Optional[Any] = None,
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
            catalog_manager: System Catalog Manager (Phase 1)
            lineage_adapter: OpenLineage Adapter
        """
        self.config = pipeline_config
        self.project_config = None  # Set by PipelineManager if available
        self.engine_type = engine
        self.connections = connections or {}
        self.generate_story = generate_story
        self.retry_config = retry_config
        self.alerts = alerts or []
        self.performance_config = performance_config
        self.catalog_manager = catalog_manager
        self.lineage = lineage_adapter

        # Initialize story generator
        story_config = story_config or {}

        self.story_generator = StoryGenerator(
            pipeline_name=pipeline_config.pipeline,
            max_sample_rows=story_config.get("max_sample_rows", 10),
            output_path=story_config.get("output_path", "stories/"),
            storage_options=story_config.get("storage_options", {}),
            catalog_manager=catalog_manager,
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
        on_error: Optional[str] = None,
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

        # Lineage: Start
        parent_run_id = None
        if self.lineage:
            parent_run_id = self.lineage.emit_pipeline_start(self.config)

        # Drift Detection (Governance)
        if self.catalog_manager:
            try:
                import hashlib
                import json

                # Calculate Local Hash
                if hasattr(self.config, "model_dump"):
                    dump = self.config.model_dump(mode="json")
                else:
                    dump = self.config.dict()
                dump_str = json.dumps(dump, sort_keys=True)
                local_hash = hashlib.md5(dump_str.encode("utf-8")).hexdigest()

                # Get Remote Hash
                remote_hash = self.catalog_manager.get_pipeline_hash(self.config.pipeline)

                if remote_hash and remote_hash != local_hash:
                    logger.warning(
                        f"⚠️ DRIFT DETECTED: Local pipeline definition differs from Catalog.\n"
                        f"   Local Hash: {local_hash[:8]}\n"
                        f"   Catalog Hash: {remote_hash[:8]}\n"
                        f"   Advice: Deploy changes using 'odibi deploy' before running in production."
                    )
                elif not remote_hash:
                    logger.info("ℹ️ Pipeline not found in Catalog (Running un-deployed code)")
            except Exception as e:
                logger.debug(f"Drift detection check failed: {e}")

        state_manager = None
        if resume_from_failure:
            if self.project_config:
                try:
                    backend = create_state_backend(
                        config=self.project_config,
                        project_root=".",
                        spark_session=getattr(self.engine, "spark", None),
                    )
                    state_manager = StateManager(backend=backend)
                except Exception as e:
                    logger.warning(f"Could not initialize StateManager: {e}")
            else:
                logger.warning("Resume capability unavailable: Project configuration missing.")

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
                last_info = state_manager.get_last_run_info(self.config.pipeline, node_name)

                # logger.info(f"DEBUG: last_info for {node_name}: {last_info}")

                can_resume = False
                resume_reason = ""

                if last_info and last_info.get("success"):
                    # Check 1: Version Hash (Code/Config Change)
                    last_hash = last_info.get("metadata", {}).get("version_hash")
                    # current_hash = self.graph.nodes[node_name].get_version_hash()

                    # Note: We need to access Node object to get hash, but we only have NodeConfig here.
                    # Wait, graph.nodes[node_name] IS NodeConfig.
                    # NodeConfig doesn't have get_version_hash method (it's on Node class).
                    # We can instantiate a temporary Node or move the hash logic to a utility/config method.
                    # Node.get_version_hash uses self.config.
                    # Let's use a static helper or re-instantiate.
                    # Since get_version_hash is simple, we can just compute it here.

                    # Calculate hash from config
                    import hashlib
                    import json

                    node_cfg = self.graph.nodes[node_name]
                    dump = (
                        node_cfg.model_dump(
                            mode="json", exclude={"description", "tags", "log_level"}
                        )
                        if hasattr(node_cfg, "model_dump")
                        else node_cfg.dict(exclude={"description", "tags", "log_level"})
                    )
                    dump_str = json.dumps(dump, sort_keys=True)
                    current_hash = hashlib.md5(dump_str.encode("utf-8")).hexdigest()

                    if last_hash == current_hash:
                        # Check 2: Cascading Invalidation
                        # If any dependency was NOT skipped (meaning it ran), we must run.
                        deps_ran = False
                        for dep in node_config.depends_on:
                            # If dep is in completed but NOT in skipped list -> It executed this run
                            if dep in results.completed and dep not in results.skipped:
                                deps_ran = True
                                break

                        if not deps_ran:
                            can_resume = True
                            resume_reason = "Previously succeeded and restored from storage"
                        else:
                            resume_reason = "Upstream dependency executed"
                    else:
                        resume_reason = f"Configuration changed (Hash mismatch: {str(last_hash)[:7]}... != {str(current_hash)[:7]}...)"
                else:
                    resume_reason = "No successful previous run found"

                if can_resume:
                    # Try to restore
                    if node_config.write:
                        try:
                            temp_node = Node(
                                config=node_config,
                                context=self.context,
                                engine=self.engine,
                                connections=self.connections,
                            )
                            # If using dry run or mock engine in tests, restore might always succeed or fail based on implementation
                            # In the failing test case, the mock engine's read is mocked but restore() calls engine.read
                            # Let's ensure we pass the result properly.
                            if temp_node.restore():
                                logger.info(f"Skipping '{node_name}': {resume_reason}")
                                result = NodeResult(
                                    node_name=node_name,
                                    success=True,
                                    duration=0.0,
                                    metadata={
                                        "skipped": True,
                                        "reason": "resume_from_failure",
                                        "version_hash": current_hash,
                                    },
                                )
                                # IMPORTANT: Set rows_processed/schema from restored if possible?
                                # For now just returning metadata is key.
                                return result
                            else:
                                logger.info(f"Re-running '{node_name}': Restore failed")
                        except Exception as e:
                            logger.warning(f"Resuming: Could not restore '{node_name}': {e}")
                    else:
                        # Cannot restore pure transform safely without more logic
                        logger.info(
                            f"Re-running '{node_name}': In-memory transform (cannot be restored)"
                        )
                else:
                    logger.info(f"Re-running '{node_name}': {resume_reason}")

            # Lineage: Node Start
            node_run_id = None
            if self.lineage and parent_run_id:
                node_run_id = self.lineage.emit_node_start(node_config, parent_run_id)

            # Execute node
            result = None
            try:
                node = Node(
                    config=node_config,
                    context=self.context,
                    engine=self.engine,
                    connections=self.connections,
                    dry_run=dry_run,
                    retry_config=self.retry_config,
                    catalog_manager=self.catalog_manager,
                )
                result = node.execute()
            except Exception as e:
                logger.error(f"Node '{node_name}' failed: {e}")
                result = NodeResult(node_name=node_name, success=False, duration=0.0, error=str(e))

            # Lineage: Node Complete
            if self.lineage and node_run_id:
                self.lineage.emit_node_complete(node_config, result, node_run_id)

            return result

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

                                    # Determine Error Strategy: CLI override > Node Config
                                    strategy = (
                                        ErrorStrategy(on_error)
                                        if on_error
                                        else node_config.on_error
                                    )

                                    if strategy == ErrorStrategy.FAIL_FAST:
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

                        # Determine Error Strategy: CLI override > Node Config
                        strategy = ErrorStrategy(on_error) if on_error else node_config.on_error

                        if strategy == ErrorStrategy.FAIL_FAST:
                            logger.error(
                                f"FAIL_FAST: Node '{node_name}' failed. Stopping pipeline."
                            )
                            break

        # Calculate duration
        results.duration = time.time() - start_time
        results.end_time = datetime.now().isoformat()

        # Save state if running normally (not dry run)
        if not dry_run:
            if not state_manager and self.project_config:
                try:
                    backend = create_state_backend(
                        config=self.project_config,
                        project_root=".",
                        spark_session=getattr(self.engine, "spark", None),
                    )
                    state_manager = StateManager(backend=backend)
                except Exception as e:
                    logger.warning(f"Could not initialize StateManager for saving run: {e}")

            if state_manager:
                state_manager.save_pipeline_run(self.config.pipeline, results)

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

            # Phase 1: Optimize Catalog on Success
            if self.catalog_manager:
                self.catalog_manager.optimize()

        # Lineage: Complete
        if self.lineage:
            self.lineage.emit_pipeline_complete(self.config, results)

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
                    "event_type": event,
                }

                # Enrich with story summary (row counts, story URL)
                if event != "on_start" and self.generate_story:
                    story_summary = self.story_generator.get_alert_summary()
                    context.update(story_summary)

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

            # Validate transformer params against registered models
            for node_name, node in self.graph.nodes.items():
                # 1. Top-level transformer
                if node.transformer:
                    try:
                        FunctionRegistry.validate_params(node.transformer, node.params)
                    except ValueError as e:
                        validation["errors"].append(f"Node '{node_name}' transformer error: {e}")
                        validation["valid"] = False

                # 2. Transform steps
                if node.transform and node.transform.steps:
                    for i, step in enumerate(node.transform.steps):
                        # Handle string steps (SQL)
                        if isinstance(step, str):
                            continue

                        # Handle TransformStep objects
                        if hasattr(step, "function") and step.function:
                            try:
                                FunctionRegistry.validate_params(step.function, step.params)
                            except ValueError as e:
                                validation["errors"].append(
                                    f"Node '{node_name}' step {i + 1} error: {e}"
                                )
                                validation["valid"] = False

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
        self.catalog_manager = None
        self.lineage_adapter = None

        # Configure logging
        configure_logging(
            structured=project_config.logging.structured, level=project_config.logging.level.value
        )

        # Initialize Lineage Adapter
        self.lineage_adapter = OpenLineageAdapter(project_config.lineage)

        # Initialize CatalogManager if configured (Phase 1)
        if project_config.system:
            from odibi.catalog import CatalogManager

            spark = None
            engine_instance = None

            if project_config.engine == "spark":
                # We need to instantiate an engine to get the session
                try:
                    from odibi.engine.spark_engine import SparkEngine

                    # We need connections for Spark to configure ADLS
                    temp_engine = SparkEngine(connections=connections, config={})
                    spark = temp_engine.spark
                except Exception as e:
                    logger.warning(f"Failed to initialize Spark for System Catalog: {e}")

            sys_conn = connections.get(project_config.system.connection)
            if sys_conn:
                base_path = sys_conn.get_path(project_config.system.path)

                # If running locally (no spark), initialize a local engine for the catalog
                if not spark:
                    try:
                        from odibi.engine.pandas_engine import PandasEngine

                        engine_instance = PandasEngine(config={})
                    except Exception as e:
                        logger.warning(f"Failed to initialize PandasEngine for System Catalog: {e}")

                if spark or engine_instance:
                    self.catalog_manager = CatalogManager(
                        spark=spark,
                        config=project_config.system,
                        base_path=base_path,
                        engine=engine_instance,
                    )
                    self.catalog_manager.bootstrap()
            else:
                logger.warning(f"System connection '{project_config.system.connection}' not found.")

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
                catalog_manager=self.catalog_manager,
                lineage_adapter=self.lineage_adapter,
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
        # 1. Bootstrap Registry (Standard Library)
        register_standard_library()

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
        from odibi.connections.factory import register_builtins

        connections = {}

        # 1. Register built-in connection types
        register_builtins()

        # 2. Load plugins (external)
        load_plugins()

        # 3. Build connection objects
        for conn_name, conn_config in conn_configs.items():
            # Convert Pydantic model to dict if needed
            if hasattr(conn_config, "model_dump"):
                conn_config = conn_config.model_dump()
            elif hasattr(conn_config, "dict"):
                conn_config = conn_config.dict()

            conn_type = conn_config.get("type", "local")

            factory = get_connection_factory(conn_type)
            if factory:
                try:
                    connections[conn_name] = factory(conn_name, conn_config)
                except Exception as e:
                    # Enhance error with context
                    raise ValueError(
                        f"Failed to create connection '{conn_name}' (type={conn_type}): {e}"
                    ) from e
            else:
                raise ValueError(
                    f"Unsupported connection type: {conn_type}. "
                    f"Supported types: local, azure_adls, azure_sql, delta, etc. "
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
        parallel: bool = False,
        max_workers: int = 4,
        on_error: Optional[str] = None,
    ) -> Union[PipelineResults, Dict[str, PipelineResults]]:
        """Run one, multiple, or all pipelines.

        Args:
            pipelines: Pipeline name(s) to run.
            dry_run: Whether to simulate execution.
            resume_from_failure: Whether to skip successfully completed nodes from last run.
            parallel: Whether to run nodes in parallel.
            max_workers: Maximum number of worker threads for parallel execution.
            on_error: Override error handling strategy (fail_fast, fail_later, ignore).

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
            # ...
            results[name] = self._pipelines[name].run(
                dry_run=dry_run,
                resume_from_failure=resume_from_failure,
                parallel=parallel,
                max_workers=max_workers,
                on_error=on_error,
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

    def deploy(self, pipelines: Optional[Union[str, List[str]]] = None) -> bool:
        """Deploy pipeline definitions to the System Catalog.

        This registers pipeline and node configurations in the catalog,
        enabling drift detection and governance features.

        Args:
            pipelines: Optional pipeline name(s) to deploy. If None, deploys all.

        Returns:
            True if deployment succeeded, False otherwise.

        Example:
            >>> manager = PipelineManager.from_yaml("odibi.yaml")
            >>> manager.deploy()  # Deploy all pipelines
            >>> manager.deploy("sales_daily")  # Deploy specific pipeline
        """
        if not self.catalog_manager:
            logger.warning("System Catalog not configured. Cannot deploy.")
            return False

        # Determine which pipelines to deploy
        if pipelines is None:
            to_deploy = self.project_config.pipelines
        elif isinstance(pipelines, str):
            to_deploy = [p for p in self.project_config.pipelines if p.pipeline == pipelines]
        else:
            to_deploy = [p for p in self.project_config.pipelines if p.pipeline in pipelines]

        if not to_deploy:
            logger.warning("No matching pipelines found to deploy.")
            return False

        try:
            self.catalog_manager.bootstrap()

            for pipeline_config in to_deploy:
                logger.info(f"Deploying pipeline: {pipeline_config.pipeline}")
                self.catalog_manager.register_pipeline(pipeline_config, self.project_config)

                for node in pipeline_config.nodes:
                    self.catalog_manager.register_node(pipeline_config.pipeline, node)

            logger.info(f"Deployed {len(to_deploy)} pipeline(s) to System Catalog.")
            return True

        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            return False
