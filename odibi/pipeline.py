"""Pipeline executor and orchestration."""

import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    import pandas as pd

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
from odibi.utils.logging_context import (
    create_logging_context,
    set_logging_context,
)
from odibi.utils.progress import NodeStatus, PipelineProgress


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

        # Create logging context for this pipeline
        self._ctx = create_logging_context(
            pipeline_id=pipeline_config.pipeline,
            engine=engine,
        )

        self._ctx.info(
            f"Initializing pipeline: {pipeline_config.pipeline}",
            engine=engine,
            node_count=len(pipeline_config.nodes),
            connections=list(self.connections.keys()) if self.connections else [],
        )

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

        self._ctx.debug(f"Engine initialized: {engine}")

        # Initialize context
        spark_session = getattr(self.engine, "spark", None)
        self.context = create_context(engine, spark_session=spark_session)

        # Build dependency graph
        self.graph = DependencyGraph(pipeline_config.nodes)

        # Log graph structure
        layers = self.graph.get_execution_layers()
        edge_count = sum(len(n.depends_on) for n in pipeline_config.nodes)
        self._ctx.log_graph_operation(
            operation="build",
            node_count=len(pipeline_config.nodes),
            edge_count=edge_count,
            layer_count=len(layers),
        )

    def __enter__(self) -> "Pipeline":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - cleanup connections."""
        self._cleanup_connections()

    def _cleanup_connections(self) -> None:
        """Clean up all connection resources."""
        if not self.connections:
            return

        for name, conn in self.connections.items():
            if hasattr(conn, "close"):
                try:
                    conn.close()
                    self._ctx.debug(f"Closed connection: {name}")
                except Exception as e:
                    self._ctx.warning(f"Failed to close connection {name}: {e}", exc_info=True)

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
        tag: Optional[str] = None,
        node: Optional[str] = None,
        console: bool = False,
    ) -> PipelineResults:
        """Execute the pipeline.

        Args:
            parallel: Whether to use parallel execution
            dry_run: Whether to simulate execution without running operations
            resume_from_failure: Whether to skip successfully completed nodes from last run
            max_workers: Maximum number of parallel threads (default: 4)
            on_error: Override error handling strategy
            tag: Filter nodes by tag (only nodes with this tag will run)
            node: Run only the specific node by name
            console: Whether to show rich console output with progress

        Returns:
            PipelineResults with execution details
        """
        start_time = time.time()
        start_timestamp = datetime.now().isoformat()

        results = PipelineResults(pipeline_name=self.config.pipeline, start_time=start_timestamp)

        # Set global logging context for this pipeline run
        set_logging_context(self._ctx)

        # Get execution plan info for logging
        layers = self.graph.get_execution_layers()
        execution_order = self.graph.topological_sort()

        # Apply node filters (--tag, --node)
        filtered_nodes = set(execution_order)
        if tag:
            filtered_nodes = {name for name in filtered_nodes if tag in self.graph.nodes[name].tags}
            self._ctx.info(f"Filtering by tag '{tag}': {len(filtered_nodes)} nodes match")
        if node:
            if node in self.graph.nodes:
                filtered_nodes = {node}
                self._ctx.info(f"Running single node: {node}")
            else:
                available = ", ".join(self.graph.nodes.keys())
                raise ValueError(f"Node '{node}' not found. Available: {available}")

        # Update execution order to only include filtered nodes
        execution_order = [n for n in execution_order if n in filtered_nodes]
        layers = [[n for n in layer if n in filtered_nodes] for layer in layers]
        layers = [layer for layer in layers if layer]  # Remove empty layers

        self._ctx.info(
            f"Starting pipeline: {self.config.pipeline}",
            mode="parallel" if parallel else "serial",
            dry_run=dry_run,
            resume_from_failure=resume_from_failure,
            node_count=len(self.graph.nodes),
            layer_count=len(layers),
            max_workers=max_workers if parallel else 1,
        )

        if parallel:
            self._ctx.debug(
                f"Parallel execution plan: {len(layers)} layers",
                layers=[list(layer) for layer in layers],
            )
        else:
            self._ctx.debug(
                f"Serial execution order: {len(execution_order)} nodes",
                order=execution_order,
            )

        # Initialize progress tracker for console output
        progress: Optional[PipelineProgress] = None
        if console:
            progress = PipelineProgress(
                pipeline_name=self.config.pipeline,
                node_names=execution_order,
                engine=self.engine_type,
            )
            progress.start()

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
                    self._ctx.warning(
                        "DRIFT DETECTED: Local pipeline differs from Catalog",
                        local_hash=local_hash[:8],
                        catalog_hash=remote_hash[:8],
                        suggestion="Deploy changes using 'odibi deploy' before production",
                    )
                elif not remote_hash:
                    self._ctx.info(
                        "Pipeline not found in Catalog (Running un-deployed code)",
                        catalog_status="not_deployed",
                    )
                else:
                    self._ctx.debug(
                        "Drift check passed",
                        hash=local_hash[:8],
                    )
            except Exception as e:
                self._ctx.debug(f"Drift detection check failed: {e}")

        state_manager = None
        if resume_from_failure:
            self._ctx.info("Resume from failure enabled - checking previous run state")
            if self.project_config:
                try:
                    backend = create_state_backend(
                        config=self.project_config,
                        project_root=".",
                        spark_session=getattr(self.engine, "spark", None),
                    )
                    state_manager = StateManager(backend=backend)
                    self._ctx.debug("StateManager initialized for resume capability")
                except Exception as e:
                    self._ctx.warning(
                        f"Could not initialize StateManager: {e}",
                        suggestion="Check state backend configuration",
                    )
            else:
                self._ctx.warning(
                    "Resume capability unavailable: Project configuration missing",
                    suggestion="Ensure project config is set for resume support",
                )

        # Define node processing function (inner function to capture self/context)
        def process_node(node_name: str) -> NodeResult:
            node_ctx = self._ctx.with_context(node_id=node_name)

            node_config = self.graph.nodes[node_name]
            deps_failed_list = [dep for dep in node_config.depends_on if dep in results.failed]
            deps_failed = len(deps_failed_list) > 0

            if deps_failed:
                node_ctx.warning(
                    "Skipping node due to dependency failure",
                    skipped=True,
                    failed_dependencies=deps_failed_list,
                    suggestion="Fix upstream node failures first",
                )
                return NodeResult(
                    node_name=node_name,
                    success=False,
                    duration=0.0,
                    metadata={"skipped": True, "reason": "dependency_failed"},
                )

            # Check for resume capability
            if resume_from_failure and state_manager:
                last_info = state_manager.get_last_run_info(self.config.pipeline, node_name)

                can_resume = False
                resume_reason = ""

                if last_info and last_info.get("success"):
                    last_hash = last_info.get("metadata", {}).get("version_hash")

                    from odibi.utils.hashing import calculate_node_hash

                    node_cfg = self.graph.nodes[node_name]
                    current_hash = calculate_node_hash(node_cfg)

                    if last_hash == current_hash:
                        deps_ran = False
                        for dep in node_config.depends_on:
                            if dep in results.completed and dep not in results.skipped:
                                deps_ran = True
                                break

                        if not deps_ran:
                            can_resume = True
                            resume_reason = "Previously succeeded and restored from storage"
                        else:
                            resume_reason = "Upstream dependency executed"
                    else:
                        resume_reason = (
                            f"Configuration changed (Hash: {str(last_hash)[:7]}... "
                            f"!= {str(current_hash)[:7]}...)"
                        )
                else:
                    resume_reason = "No successful previous run found"

                if can_resume:
                    if node_config.write:
                        try:
                            temp_node = Node(
                                config=node_config,
                                context=self.context,
                                engine=self.engine,
                                connections=self.connections,
                                performance_config=self.performance_config,
                                pipeline_name=self.config.pipeline,
                            )
                            if temp_node.restore():
                                node_ctx.info(
                                    "Skipping node (restored from previous run)",
                                    skipped=True,
                                    reason="resume_from_failure",
                                    version_hash=current_hash[:8],
                                )
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
                                return result
                            else:
                                node_ctx.debug(
                                    "Re-running node: Restore failed",
                                    reason="restore_failed",
                                )
                        except Exception as e:
                            node_ctx.warning(
                                f"Could not restore node: {e}",
                                reason="restore_error",
                            )
                    else:
                        node_ctx.debug(
                            "Re-running node: In-memory transform (cannot be restored)",
                            reason="no_write_config",
                        )
                else:
                    node_ctx.debug(f"Re-running node: {resume_reason}")

            # Lineage: Node Start
            node_run_id = None
            if self.lineage and parent_run_id:
                node_run_id = self.lineage.emit_node_start(node_config, parent_run_id)

            # Execute node with operation context
            result = None
            node_start = time.time()
            node_ctx.debug(
                "Executing node",
                transformer=node_config.transformer,
                has_read=bool(node_config.read),
                has_write=bool(node_config.write),
            )

            try:
                node = Node(
                    config=node_config,
                    context=self.context,
                    engine=self.engine,
                    connections=self.connections,
                    dry_run=dry_run,
                    retry_config=self.retry_config,
                    catalog_manager=self.catalog_manager,
                    performance_config=self.performance_config,
                    pipeline_name=self.config.pipeline,
                )
                result = node.execute()

                node_duration = time.time() - node_start
                if result.success:
                    node_ctx.info(
                        "Node completed successfully",
                        duration_ms=round(node_duration * 1000, 2),
                        rows_processed=result.rows_processed,
                    )
                else:
                    node_ctx.error(
                        "Node execution failed",
                        duration_ms=round(node_duration * 1000, 2),
                        error=result.error,
                    )

            except Exception as e:
                node_duration = time.time() - node_start
                node_ctx.error(
                    f"Node raised exception: {e}",
                    duration_ms=round(node_duration * 1000, 2),
                    error_type=type(e).__name__,
                    suggestion="Check node configuration and input data",
                )
                result = NodeResult(node_name=node_name, success=False, duration=0.0, error=str(e))

            # Lineage: Node Complete
            if self.lineage and node_run_id:
                self.lineage.emit_node_complete(node_config, result, node_run_id)

            return result

        if parallel:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            layers = self.graph.get_execution_layers()
            self._ctx.info(
                f"Starting parallel execution with {max_workers} workers",
                total_layers=len(layers),
                max_workers=max_workers,
            )

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for layer_idx, layer in enumerate(layers):
                    layer_start = time.time()
                    self._ctx.debug(
                        f"Executing layer {layer_idx + 1}/{len(layers)}",
                        layer_index=layer_idx,
                        nodes_in_layer=list(layer),
                        node_count=len(layer),
                    )

                    future_to_node = {
                        executor.submit(process_node, node_name): node_name for node_name in layer
                    }

                    layer_failed = False
                    for future in as_completed(future_to_node):
                        node_name = future_to_node[future]
                        try:
                            result = future.result()
                            results.node_results[node_name] = result

                            if result.success:
                                if result.metadata.get("skipped"):
                                    if result.metadata.get("reason") == "dependency_failed":
                                        results.skipped.append(node_name)
                                        results.failed.append(node_name)
                                        if progress:
                                            progress.update_node(
                                                node_name,
                                                NodeStatus.SKIPPED,
                                                result.duration,
                                                result.rows_processed,
                                            )
                                    else:
                                        results.completed.append(node_name)
                                        if progress:
                                            progress.update_node(
                                                node_name,
                                                NodeStatus.SKIPPED,
                                                result.duration,
                                                result.rows_processed,
                                            )
                                else:
                                    results.completed.append(node_name)
                                    if progress:
                                        progress.update_node(
                                            node_name,
                                            NodeStatus.SUCCESS,
                                            result.duration,
                                            result.rows_processed,
                                            result.metadata.get("phase_timings_ms"),
                                        )
                            else:
                                if result.metadata.get("skipped"):
                                    results.skipped.append(node_name)
                                    results.failed.append(node_name)
                                    if progress:
                                        progress.update_node(
                                            node_name,
                                            NodeStatus.SKIPPED,
                                            result.duration,
                                            result.rows_processed,
                                        )
                                else:
                                    results.failed.append(node_name)
                                    layer_failed = True
                                    if progress:
                                        progress.update_node(
                                            node_name,
                                            NodeStatus.FAILED,
                                            result.duration,
                                            result.rows_processed,
                                        )

                                    node_config = self.graph.nodes[node_name]
                                    strategy = (
                                        ErrorStrategy(on_error)
                                        if on_error
                                        else node_config.on_error
                                    )

                                    if strategy == ErrorStrategy.FAIL_FAST:
                                        self._ctx.error(
                                            "FAIL_FAST triggered: Stopping pipeline",
                                            failed_node=node_name,
                                            error=result.error,
                                            remaining_nodes=len(future_to_node) - 1,
                                        )
                                        executor.shutdown(cancel_futures=True, wait=False)
                                        break

                        except Exception as exc:
                            self._ctx.error(
                                "Node generated exception",
                                node=node_name,
                                error=str(exc),
                                error_type=type(exc).__name__,
                            )
                            results.failed.append(node_name)
                            layer_failed = True
                            if progress:
                                progress.update_node(node_name, NodeStatus.FAILED)

                            node_config = self.graph.nodes[node_name]
                            strategy = ErrorStrategy(on_error) if on_error else node_config.on_error
                            if strategy == ErrorStrategy.FAIL_FAST:
                                self._ctx.error(
                                    "FAIL_FAST triggered: Stopping pipeline",
                                    failed_node=node_name,
                                )
                                executor.shutdown(cancel_futures=True, wait=False)
                                break

                    layer_duration = time.time() - layer_start
                    self._ctx.debug(
                        f"Layer {layer_idx + 1} completed",
                        layer_index=layer_idx,
                        duration_ms=round(layer_duration * 1000, 2),
                        layer_failed=layer_failed,
                    )

                    if layer_failed:
                        for failed_node in results.failed:
                            if self.graph.nodes[failed_node].on_error == ErrorStrategy.FAIL_FAST:
                                return results

        else:
            self._ctx.info("Starting serial execution")
            execution_order = self.graph.topological_sort()
            for idx, node_name in enumerate(execution_order):
                self._ctx.debug(
                    f"Executing node {idx + 1}/{len(execution_order)}",
                    node=node_name,
                    order=idx + 1,
                    total=len(execution_order),
                )

                result = process_node(node_name)
                results.node_results[node_name] = result

                if result.success:
                    if (
                        result.metadata.get("skipped")
                        and result.metadata.get("reason") == "dependency_failed"
                    ):
                        results.skipped.append(node_name)
                        results.failed.append(node_name)
                        if progress:
                            progress.update_node(
                                node_name,
                                NodeStatus.SKIPPED,
                                result.duration,
                                result.rows_processed,
                            )
                    else:
                        results.completed.append(node_name)
                        if progress:
                            status = (
                                NodeStatus.SKIPPED
                                if result.metadata.get("skipped")
                                else NodeStatus.SUCCESS
                            )
                            progress.update_node(
                                node_name,
                                status,
                                result.duration,
                                result.rows_processed,
                            )
                else:
                    if result.metadata.get("skipped"):
                        results.skipped.append(node_name)
                        results.failed.append(node_name)
                        if progress:
                            progress.update_node(
                                node_name,
                                NodeStatus.SKIPPED,
                                result.duration,
                                result.rows_processed,
                            )
                    else:
                        results.failed.append(node_name)
                        if progress:
                            progress.update_node(
                                node_name,
                                NodeStatus.FAILED,
                                result.duration,
                                result.rows_processed,
                            )

                        node_config = self.graph.nodes[node_name]
                        strategy = ErrorStrategy(on_error) if on_error else node_config.on_error

                        if strategy == ErrorStrategy.FAIL_FAST:
                            self._ctx.error(
                                "FAIL_FAST triggered: Stopping pipeline",
                                failed_node=node_name,
                                error=result.error,
                                remaining_nodes=len(execution_order) - idx - 1,
                            )
                            break

        # Calculate duration
        results.duration = time.time() - start_time
        results.end_time = datetime.now().isoformat()

        # Batch write run records to catalog (much faster than per-node writes)
        if self.catalog_manager:
            run_records = []
            for node_result in results.node_results.values():
                if node_result.metadata and "_run_record" in node_result.metadata:
                    run_records.append(node_result.metadata.pop("_run_record"))
            if run_records:
                self.catalog_manager.log_runs_batch(run_records)
                self._ctx.debug(
                    f"Batch logged {len(run_records)} run records",
                    record_count=len(run_records),
                )

            # Batch write output metadata for cross-pipeline dependencies
            output_records = []
            for node_result in results.node_results.values():
                if node_result.metadata and "_output_record" in node_result.metadata:
                    output_records.append(node_result.metadata.pop("_output_record"))
            if output_records:
                try:
                    self.catalog_manager.register_outputs_batch(output_records)
                    self._ctx.debug(
                        f"Batch registered {len(output_records)} output(s)",
                        output_count=len(output_records),
                    )
                except Exception as e:
                    self._ctx.warning(
                        f"Failed to register outputs (non-fatal): {e}",
                        error_type=type(e).__name__,
                    )

        # Finish progress display
        if progress:
            progress.finish(
                completed=len(results.completed),
                failed=len(results.failed),
                skipped=len(results.skipped),
                duration=results.duration,
            )
            # Print phase timing breakdown for performance analysis
            progress.print_phase_timing_report(pipeline_duration_s=results.duration)

        # Log pipeline completion summary
        status = "SUCCESS" if not results.failed else "FAILED"
        self._ctx.info(
            f"Pipeline {status}: {self.config.pipeline}",
            status=status,
            duration_s=round(results.duration, 2),
            completed=len(results.completed),
            failed=len(results.failed),
            skipped=len(results.skipped),
            total_nodes=len(self.graph.nodes),
        )

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
                    self._ctx.warning(
                        f"Could not initialize StateManager for saving run: {e}",
                        suggestion="Check state backend configuration",
                    )

            if state_manager:
                state_manager.save_pipeline_run(self.config.pipeline, results)
                self._ctx.debug("Pipeline run state saved")

        # Generate story
        if self.generate_story:
            if hasattr(self.config, "model_dump"):
                config_dump = self.config.model_dump(mode="json")
            else:
                config_dump = self.config.dict()

            if self.project_config:
                project_dump = (
                    self.project_config.model_dump(mode="json")
                    if hasattr(self.project_config, "model_dump")
                    else self.project_config.dict()
                )
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
            self._ctx.info("Story generated", story_path=story_path)

        # Alert: on_success / on_failure
        if results.failed:
            self._send_alerts("on_failure", results)
        else:
            self._send_alerts("on_success", results)

            if self.catalog_manager:
                self.catalog_manager.optimize()
                self._ctx.debug("Catalog optimized")

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
            performance_config=self.performance_config,
            pipeline_name=self.config.pipeline,
        )

        return node.execute()

    def validate(self) -> Dict[str, Any]:
        """Validate pipeline without executing.

        Returns:
            Validation results
        """
        self._ctx.info("Validating pipeline configuration")

        validation = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "node_count": len(self.graph.nodes),
            "execution_order": [],
        }

        try:
            execution_order = self.graph.topological_sort()
            validation["execution_order"] = execution_order
            self._ctx.debug(
                "Dependency graph validated",
                execution_order=execution_order,
            )

            for node_name, node in self.graph.nodes.items():
                if node.transformer:
                    try:
                        FunctionRegistry.validate_params(node.transformer, node.params)
                    except ValueError as e:
                        validation["errors"].append(f"Node '{node_name}' transformer error: {e}")
                        validation["valid"] = False
                        self._ctx.log_validation_result(
                            passed=False,
                            rule_name=f"transformer_params:{node_name}",
                            failures=[str(e)],
                        )

                if node.transform and node.transform.steps:
                    for i, step in enumerate(node.transform.steps):
                        if isinstance(step, str):
                            continue

                        if hasattr(step, "function") and step.function:
                            try:
                                FunctionRegistry.validate_params(step.function, step.params)
                            except ValueError as e:
                                validation["errors"].append(
                                    f"Node '{node_name}' step {i + 1} error: {e}"
                                )
                                validation["valid"] = False
                                self._ctx.log_validation_result(
                                    passed=False,
                                    rule_name=f"step_params:{node_name}:step_{i + 1}",
                                    failures=[str(e)],
                                )

        except DependencyError as e:
            validation["valid"] = False
            validation["errors"].append(str(e))
            self._ctx.error(
                "Dependency graph validation failed",
                error=str(e),
            )

        for node in self.config.nodes:
            if node.read and node.read.connection not in self.connections:
                validation["warnings"].append(
                    f"Node '{node.name}': connection '{node.read.connection}' not configured"
                )
            if node.write and node.write.connection not in self.connections:
                validation["warnings"].append(
                    f"Node '{node.name}': connection '{node.write.connection}' not configured"
                )

        self._ctx.info(
            f"Validation {'passed' if validation['valid'] else 'failed'}",
            valid=validation["valid"],
            errors=len(validation["errors"]),
            warnings=len(validation["warnings"]),
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

        # Create manager-level logging context
        self._ctx = create_logging_context(engine=project_config.engine)

        self._ctx.info(
            "Initializing PipelineManager",
            project=project_config.project,
            engine=project_config.engine,
            pipeline_count=len(project_config.pipelines),
            connection_count=len(connections),
        )

        # Initialize Lineage Adapter
        self.lineage_adapter = OpenLineageAdapter(project_config.lineage)

        # Initialize CatalogManager if configured
        if project_config.system:
            from odibi.catalog import CatalogManager

            spark = None
            engine_instance = None

            if project_config.engine == "spark":
                try:
                    from odibi.engine.spark_engine import SparkEngine

                    temp_engine = SparkEngine(connections=connections, config={})
                    spark = temp_engine.spark
                    self._ctx.debug("Spark session initialized for System Catalog")
                except Exception as e:
                    self._ctx.warning(
                        f"Failed to initialize Spark for System Catalog: {e}",
                        suggestion="Check Spark configuration",
                    )

            sys_conn = connections.get(project_config.system.connection)
            if sys_conn:
                base_path = sys_conn.get_path(project_config.system.path)

                if not spark:
                    try:
                        from odibi.engine.pandas_engine import PandasEngine

                        engine_instance = PandasEngine(config={})
                        self._ctx.debug("PandasEngine initialized for System Catalog")
                    except Exception as e:
                        self._ctx.warning(
                            f"Failed to initialize PandasEngine for System Catalog: {e}"
                        )

                if spark or engine_instance:
                    self.catalog_manager = CatalogManager(
                        spark=spark,
                        config=project_config.system,
                        base_path=base_path,
                        engine=engine_instance,
                    )
                    self.catalog_manager.bootstrap()
                    self._ctx.info("System Catalog initialized", path=base_path)
            else:
                self._ctx.warning(
                    f"System connection '{project_config.system.connection}' not found",
                    suggestion="Configure the system connection in your config",
                )

        # Get story configuration
        story_config = self._get_story_config()

        # Create all pipeline instances
        self._ctx.debug(
            "Creating pipeline instances",
            pipelines=[p.pipeline for p in project_config.pipelines],
        )
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
            self._pipelines[pipeline_name].project_config = project_config

        self._ctx.info(
            "PipelineManager ready",
            pipelines=list(self._pipelines.keys()),
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
        logger.info(f"Loading configuration from: {yaml_path}")

        register_standard_library()

        yaml_path_obj = Path(yaml_path)
        config_dir = yaml_path_obj.parent.absolute()

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

        load_transforms_module(os.path.join(config_dir, "transforms.py"))

        cwd = os.getcwd()
        if os.path.abspath(cwd) != str(config_dir):
            load_transforms_module(os.path.join(cwd, "transforms.py"))

        try:
            config = load_yaml_with_env(str(yaml_path_obj), env=env)
            logger.debug("Configuration loaded successfully")
        except FileNotFoundError:
            logger.error(f"YAML file not found: {yaml_path}")
            raise FileNotFoundError(f"YAML file not found: {yaml_path}")

        project_config = ProjectConfig(**config)
        logger.debug(
            "Project config validated",
            project=project_config.project,
            pipelines=len(project_config.pipelines),
        )

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

        logger.debug(f"Building {len(conn_configs)} connections")

        connections = {}

        register_builtins()
        load_plugins()

        for conn_name, conn_config in conn_configs.items():
            if hasattr(conn_config, "model_dump"):
                conn_config = conn_config.model_dump()
            elif hasattr(conn_config, "dict"):
                conn_config = conn_config.dict()

            conn_type = conn_config.get("type", "local")

            factory = get_connection_factory(conn_type)
            if factory:
                try:
                    connections[conn_name] = factory(conn_name, conn_config)
                    logger.debug(
                        f"Connection created: {conn_name}",
                        type=conn_type,
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to create connection '{conn_name}'",
                        type=conn_type,
                        error=str(e),
                    )
                    raise ValueError(
                        f"Failed to create connection '{conn_name}' (type={conn_type}): {e}"
                    ) from e
            else:
                logger.error(
                    f"Unsupported connection type: {conn_type}",
                    connection=conn_name,
                    suggestion="Check supported connection types in docs",
                )
                raise ValueError(
                    f"Unsupported connection type: {conn_type}. "
                    f"Supported types: local, azure_adls, azure_sql, delta, etc. "
                    f"See docs for connection setup."
                )

        try:
            from odibi.utils import configure_connections_parallel

            connections, errors = configure_connections_parallel(connections, verbose=False)
            if errors:
                for error in errors:
                    logger.warning(error)
        except ImportError:
            pass

        logger.info(f"Built {len(connections)} connections successfully")

        return connections

    def run(
        self,
        pipelines: Optional[Union[str, List[str]]] = None,
        dry_run: bool = False,
        resume_from_failure: bool = False,
        parallel: bool = False,
        max_workers: int = 4,
        on_error: Optional[str] = None,
        tag: Optional[str] = None,
        node: Optional[str] = None,
        console: bool = False,
    ) -> Union[PipelineResults, Dict[str, PipelineResults]]:
        """Run one, multiple, or all pipelines.

        Args:
            pipelines: Pipeline name(s) to run.
            dry_run: Whether to simulate execution.
            resume_from_failure: Whether to skip successfully completed nodes from last run.
            parallel: Whether to run nodes in parallel.
            max_workers: Maximum number of worker threads for parallel execution.
            on_error: Override error handling strategy (fail_fast, fail_later, ignore).
            tag: Filter nodes by tag (only nodes with this tag will run).
            node: Run only the specific node by name.
            console: Whether to show rich console output with progress.

        Returns:
            PipelineResults or Dict of results
        """
        if pipelines is None:
            pipeline_names = list(self._pipelines.keys())
        elif isinstance(pipelines, str):
            pipeline_names = [pipelines]
        else:
            pipeline_names = pipelines

        for name in pipeline_names:
            if name not in self._pipelines:
                available = ", ".join(self._pipelines.keys())
                self._ctx.error(
                    f"Pipeline not found: {name}",
                    available=list(self._pipelines.keys()),
                )
                raise ValueError(f"Pipeline '{name}' not found. Available pipelines: {available}")

        # Phase 2: Auto-register pipelines and nodes before execution
        if self.catalog_manager:
            self._auto_register_pipelines(pipeline_names)

        self._ctx.info(
            f"Running {len(pipeline_names)} pipeline(s)",
            pipelines=pipeline_names,
            dry_run=dry_run,
            parallel=parallel,
        )

        results = {}
        for idx, name in enumerate(pipeline_names):
            self._ctx.info(
                f"Executing pipeline {idx + 1}/{len(pipeline_names)}: {name}",
                pipeline=name,
                order=idx + 1,
            )

            results[name] = self._pipelines[name].run(
                dry_run=dry_run,
                resume_from_failure=resume_from_failure,
                parallel=parallel,
                max_workers=max_workers,
                on_error=on_error,
                tag=tag,
                node=node,
                console=console,
            )

            result = results[name]
            status = "SUCCESS" if not result.failed else "FAILED"
            self._ctx.info(
                f"Pipeline {status}: {name}",
                status=status,
                duration_s=round(result.duration, 2),
                completed=len(result.completed),
                failed=len(result.failed),
            )

            # Invalidate catalog cache so next pipeline sees updated outputs
            if self.catalog_manager:
                self.catalog_manager.invalidate_cache()

            if result.story_path:
                self._ctx.debug(f"Story generated: {result.story_path}")

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
            self._ctx.warning(
                "System Catalog not configured. Cannot deploy.",
                suggestion="Configure system catalog in your YAML config",
            )
            return False

        if pipelines is None:
            to_deploy = self.project_config.pipelines
        elif isinstance(pipelines, str):
            to_deploy = [p for p in self.project_config.pipelines if p.pipeline == pipelines]
        else:
            to_deploy = [p for p in self.project_config.pipelines if p.pipeline in pipelines]

        if not to_deploy:
            self._ctx.warning("No matching pipelines found to deploy.")
            return False

        self._ctx.info(
            f"Deploying {len(to_deploy)} pipeline(s) to System Catalog",
            pipelines=[p.pipeline for p in to_deploy],
        )

        try:
            self.catalog_manager.bootstrap()

            for pipeline_config in to_deploy:
                self._ctx.debug(
                    f"Deploying pipeline: {pipeline_config.pipeline}",
                    node_count=len(pipeline_config.nodes),
                )
                self.catalog_manager.register_pipeline(pipeline_config, self.project_config)

                for node in pipeline_config.nodes:
                    self.catalog_manager.register_node(pipeline_config.pipeline, node)

            self._ctx.info(
                f"Deployment complete: {len(to_deploy)} pipeline(s)",
                deployed=[p.pipeline for p in to_deploy],
            )
            return True

        except Exception as e:
            self._ctx.error(
                f"Deployment failed: {e}",
                error_type=type(e).__name__,
                suggestion="Check catalog configuration and permissions",
            )
            return False

    def _auto_register_pipelines(self, pipeline_names: List[str]) -> None:
        """Auto-register pipelines and nodes before execution.

        This ensures meta_pipelines and meta_nodes are populated automatically
        when running pipelines, without requiring explicit deploy() calls.

        Uses "check-before-write" pattern with batch writes for performance:
        - Reads existing hashes in one read
        - Compares version_hash to skip unchanged records
        - Batch writes only changed/new records

        Args:
            pipeline_names: List of pipeline names to register
        """
        if not self.catalog_manager:
            return

        try:
            import hashlib
            import json

            existing_pipelines = self.catalog_manager.get_all_registered_pipelines()
            existing_nodes = self.catalog_manager.get_all_registered_nodes(pipeline_names)

            pipeline_records = []
            node_records = []

            for name in pipeline_names:
                pipeline = self._pipelines[name]
                config = pipeline.config

                if hasattr(config, "model_dump"):
                    dump = config.model_dump(mode="json")
                else:
                    dump = config.dict()
                dump_str = json.dumps(dump, sort_keys=True)
                pipeline_hash = hashlib.md5(dump_str.encode("utf-8")).hexdigest()

                if existing_pipelines.get(name) != pipeline_hash:
                    all_tags = set()
                    for node in config.nodes:
                        if node.tags:
                            all_tags.update(node.tags)

                    pipeline_records.append(
                        {
                            "pipeline_name": name,
                            "version_hash": pipeline_hash,
                            "description": config.description or "",
                            "layer": config.layer or "",
                            "schedule": "",
                            "tags_json": json.dumps(list(all_tags)),
                        }
                    )

                pipeline_existing_nodes = existing_nodes.get(name, {})
                for node in config.nodes:
                    if hasattr(node, "model_dump"):
                        node_dump = node.model_dump(
                            mode="json", exclude={"description", "tags", "log_level"}
                        )
                    else:
                        node_dump = node.dict(exclude={"description", "tags", "log_level"})
                    node_dump_str = json.dumps(node_dump, sort_keys=True)
                    node_hash = hashlib.md5(node_dump_str.encode("utf-8")).hexdigest()

                    if pipeline_existing_nodes.get(node.name) != node_hash:
                        node_type = "transform"
                        if node.read:
                            node_type = "read"
                        if node.write:
                            node_type = "write"

                        node_records.append(
                            {
                                "pipeline_name": name,
                                "node_name": node.name,
                                "version_hash": node_hash,
                                "type": node_type,
                                "config_json": json.dumps(node_dump),
                            }
                        )

            if pipeline_records:
                self.catalog_manager.register_pipelines_batch(pipeline_records)
                self._ctx.debug(
                    f"Batch registered {len(pipeline_records)} changed pipeline(s)",
                    pipelines=[r["pipeline_name"] for r in pipeline_records],
                )
            else:
                self._ctx.debug("All pipelines unchanged - skipping registration")

            if node_records:
                self.catalog_manager.register_nodes_batch(node_records)
                self._ctx.debug(
                    f"Batch registered {len(node_records)} changed node(s)",
                    nodes=[r["node_name"] for r in node_records],
                )
            else:
                self._ctx.debug("All nodes unchanged - skipping registration")

        except Exception as e:
            self._ctx.warning(
                f"Auto-registration failed (non-fatal): {e}",
                error_type=type(e).__name__,
            )

    # -------------------------------------------------------------------------
    # Phase 5: List/Query Methods
    # -------------------------------------------------------------------------

    def list_registered_pipelines(self) -> "pd.DataFrame":
        """List all registered pipelines from the system catalog.

        Returns:
            DataFrame with pipeline metadata from meta_pipelines
        """
        import pandas as pd

        if not self.catalog_manager:
            self._ctx.warning("Catalog manager not configured")
            return pd.DataFrame()

        try:
            df = self.catalog_manager._read_local_table(
                self.catalog_manager.tables["meta_pipelines"]
            )
            return df
        except Exception as e:
            self._ctx.warning(f"Failed to list pipelines: {e}")
            return pd.DataFrame()

    def list_registered_nodes(self, pipeline: Optional[str] = None) -> "pd.DataFrame":
        """List nodes from the system catalog.

        Args:
            pipeline: Optional pipeline name to filter by

        Returns:
            DataFrame with node metadata from meta_nodes
        """
        import pandas as pd

        if not self.catalog_manager:
            self._ctx.warning("Catalog manager not configured")
            return pd.DataFrame()

        try:
            df = self.catalog_manager._read_local_table(self.catalog_manager.tables["meta_nodes"])
            if not df.empty and pipeline:
                df = df[df["pipeline_name"] == pipeline]
            return df
        except Exception as e:
            self._ctx.warning(f"Failed to list nodes: {e}")
            return pd.DataFrame()

    def list_runs(
        self,
        pipeline: Optional[str] = None,
        node: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 10,
    ) -> "pd.DataFrame":
        """List recent runs with optional filters.

        Args:
            pipeline: Optional pipeline name to filter by
            node: Optional node name to filter by
            status: Optional status to filter by (SUCCESS, FAILURE)
            limit: Maximum number of runs to return

        Returns:
            DataFrame with run history from meta_runs
        """
        import pandas as pd

        if not self.catalog_manager:
            self._ctx.warning("Catalog manager not configured")
            return pd.DataFrame()

        try:
            df = self.catalog_manager._read_local_table(self.catalog_manager.tables["meta_runs"])
            if df.empty:
                return df

            if pipeline:
                df = df[df["pipeline_name"] == pipeline]
            if node:
                df = df[df["node_name"] == node]
            if status:
                df = df[df["status"] == status]

            if "timestamp" in df.columns:
                df = df.sort_values("timestamp", ascending=False)

            return df.head(limit)
        except Exception as e:
            self._ctx.warning(f"Failed to list runs: {e}")
            return pd.DataFrame()

    def list_tables(self) -> "pd.DataFrame":
        """List registered assets from meta_tables.

        Returns:
            DataFrame with table/asset metadata
        """
        import pandas as pd

        if not self.catalog_manager:
            self._ctx.warning("Catalog manager not configured")
            return pd.DataFrame()

        try:
            df = self.catalog_manager._read_local_table(self.catalog_manager.tables["meta_tables"])
            return df
        except Exception as e:
            self._ctx.warning(f"Failed to list tables: {e}")
            return pd.DataFrame()

    # -------------------------------------------------------------------------
    # Phase 5.2: State Methods
    # -------------------------------------------------------------------------

    def get_state(self, key: str) -> Optional[Dict[str, Any]]:
        """Get a specific state entry (HWM, content hash, etc.).

        Args:
            key: The state key to look up

        Returns:
            Dictionary with state data or None if not found
        """

        if not self.catalog_manager:
            return None

        try:
            df = self.catalog_manager._read_table(self.catalog_manager.tables["meta_state"])
            if df.empty or "key" not in df.columns:
                return None

            row = df[df["key"] == key]
            if row.empty:
                return None

            return row.iloc[0].to_dict()
        except Exception:
            return None

    def get_all_state(self, prefix: Optional[str] = None) -> "pd.DataFrame":
        """Get all state entries, optionally filtered by key prefix.

        Args:
            prefix: Optional key prefix to filter by

        Returns:
            DataFrame with state entries
        """
        import pandas as pd

        if not self.catalog_manager:
            return pd.DataFrame()

        try:
            df = self.catalog_manager._read_table(self.catalog_manager.tables["meta_state"])
            if not df.empty and prefix and "key" in df.columns:
                df = df[df["key"].str.startswith(prefix)]
            return df
        except Exception as e:
            self._ctx.warning(f"Failed to get state: {e}")
            return pd.DataFrame()

    def clear_state(self, key: str) -> bool:
        """Remove a state entry.

        Args:
            key: The state key to remove

        Returns:
            True if deleted, False otherwise
        """
        if not self.catalog_manager:
            return False

        try:
            return self.catalog_manager.clear_state_key(key)
        except Exception as e:
            self._ctx.warning(f"Failed to clear state: {e}")
            return False

    # -------------------------------------------------------------------------
    # Phase 5.3-5.4: Schema/Lineage and Stats Methods
    # -------------------------------------------------------------------------

    def get_schema_history(
        self,
        table: str,
        limit: int = 5,
    ) -> "pd.DataFrame":
        """Get schema version history for a table.

        Args:
            table: Table identifier (supports smart path resolution)
            limit: Maximum number of versions to return

        Returns:
            DataFrame with schema history
        """
        import pandas as pd

        if not self.catalog_manager:
            return pd.DataFrame()

        try:
            resolved_path = self._resolve_table_path(table)
            history = self.catalog_manager.get_schema_history(resolved_path, limit)
            return pd.DataFrame(history)
        except Exception as e:
            self._ctx.warning(f"Failed to get schema history: {e}")
            return pd.DataFrame()

    def get_lineage(
        self,
        table: str,
        direction: str = "both",
    ) -> "pd.DataFrame":
        """Get lineage for a table.

        Args:
            table: Table identifier (supports smart path resolution)
            direction: "upstream", "downstream", or "both"

        Returns:
            DataFrame with lineage relationships
        """
        import pandas as pd

        if not self.catalog_manager:
            return pd.DataFrame()

        try:
            resolved_path = self._resolve_table_path(table)

            results = []
            if direction in ("upstream", "both"):
                upstream = self.catalog_manager.get_upstream(resolved_path)
                for r in upstream:
                    r["direction"] = "upstream"
                results.extend(upstream)

            if direction in ("downstream", "both"):
                downstream = self.catalog_manager.get_downstream(resolved_path)
                for r in downstream:
                    r["direction"] = "downstream"
                results.extend(downstream)

            return pd.DataFrame(results)
        except Exception as e:
            self._ctx.warning(f"Failed to get lineage: {e}")
            return pd.DataFrame()

    def get_pipeline_status(self, pipeline: str) -> Dict[str, Any]:
        """Get last run status, duration, timestamp for a pipeline.

        Args:
            pipeline: Pipeline name

        Returns:
            Dict with status info
        """
        if not self.catalog_manager:
            return {}

        try:
            runs = self.list_runs(pipeline=pipeline, limit=1)
            if runs.empty:
                return {"status": "never_run", "pipeline": pipeline}

            last_run = runs.iloc[0].to_dict()
            return {
                "pipeline": pipeline,
                "last_status": last_run.get("status"),
                "last_run_at": last_run.get("timestamp"),
                "last_duration_ms": last_run.get("duration_ms"),
                "last_node": last_run.get("node_name"),
            }
        except Exception as e:
            self._ctx.warning(f"Failed to get pipeline status: {e}")
            return {}

    def get_node_stats(self, node: str, days: int = 7) -> Dict[str, Any]:
        """Get average duration, row counts, success rate over period.

        Args:
            node: Node name
            days: Number of days to look back

        Returns:
            Dict with node statistics
        """
        import pandas as pd

        if not self.catalog_manager:
            return {}

        try:
            avg_duration = self.catalog_manager.get_average_duration(node, days)

            df = self.catalog_manager._read_local_table(self.catalog_manager.tables["meta_runs"])
            if df.empty:
                return {"node": node, "runs": 0}

            if "timestamp" in df.columns:
                cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)
                if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                if df["timestamp"].dt.tz is None:
                    df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
                df = df[df["timestamp"] >= cutoff]

            node_runs = df[df["node_name"] == node]
            if node_runs.empty:
                return {"node": node, "runs": 0}

            total = len(node_runs)
            success = len(node_runs[node_runs["status"] == "SUCCESS"])
            avg_rows = node_runs["rows_processed"].mean() if "rows_processed" in node_runs else None

            return {
                "node": node,
                "runs": total,
                "success_rate": success / total if total > 0 else 0,
                "avg_duration_s": avg_duration,
                "avg_rows": avg_rows,
                "period_days": days,
            }
        except Exception as e:
            self._ctx.warning(f"Failed to get node stats: {e}")
            return {}

    # -------------------------------------------------------------------------
    # Phase 6: Smart Path Resolution
    # -------------------------------------------------------------------------

    def _resolve_table_path(self, identifier: str) -> str:
        """Resolve a user-friendly identifier to a full table path.

        Accepts:
        - Relative path: "bronze/OEE/vw_OSMPerformanceOEE"
        - Registered table: "test.vw_OSMPerformanceOEE"
        - Node name: "opsvisdata_vw_OSMPerformanceOEE"
        - Full path: "abfss://..." (used as-is)

        Args:
            identifier: User-friendly table identifier

        Returns:
            Full table path
        """
        if self._is_full_path(identifier):
            return identifier

        if self.catalog_manager:
            resolved = self._lookup_in_catalog(identifier)
            if resolved:
                return resolved

        for pipeline in self._pipelines.values():
            for node in pipeline.config.nodes:
                if node.name == identifier and node.write:
                    conn = self.connections.get(node.write.connection)
                    if conn:
                        return conn.get_path(node.write.path or node.write.table)

        sys_conn_name = (
            self.project_config.system.connection if self.project_config.system else None
        )
        if sys_conn_name:
            sys_conn = self.connections.get(sys_conn_name)
            if sys_conn:
                return sys_conn.get_path(identifier)

        return identifier

    def _is_full_path(self, identifier: str) -> bool:
        """Check if identifier is already a full path."""
        full_path_prefixes = ("abfss://", "s3://", "gs://", "hdfs://", "/", "C:", "D:")
        return identifier.startswith(full_path_prefixes)

    def _lookup_in_catalog(self, identifier: str) -> Optional[str]:
        """Look up identifier in meta_tables catalog."""
        if not self.catalog_manager:
            return None

        try:
            df = self.catalog_manager._read_local_table(self.catalog_manager.tables["meta_tables"])
            if df.empty or "table_name" not in df.columns:
                return None

            match = df[df["table_name"] == identifier]
            if not match.empty and "path" in match.columns:
                return match.iloc[0]["path"]

            if "." in identifier:
                parts = identifier.split(".", 1)
                if len(parts) == 2:
                    match = df[df["table_name"] == parts[1]]
                    if not match.empty and "path" in match.columns:
                        return match.iloc[0]["path"]

        except Exception:
            pass

        return None
