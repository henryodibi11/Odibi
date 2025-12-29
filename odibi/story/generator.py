"""Story generator for pipeline execution documentation."""

import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from odibi.node import NodeResult
from odibi.story.metadata import DeltaWriteInfo, NodeExecutionMetadata, PipelineStoryMetadata
from odibi.story.renderers import HTMLStoryRenderer, JSONStoryRenderer
from odibi.utils.logging_context import get_logging_context


# Custom class to force block style for multiline strings
class MultilineString(str):
    """String subclass to force YAML block scalar style."""

    pass


def multiline_presenter(dumper, data):
    """YAML representer for MultilineString."""
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


yaml.add_representer(MultilineString, multiline_presenter)


class StoryGenerator:
    """Generates markdown documentation of pipeline execution."""

    def __init__(
        self,
        pipeline_name: str,
        max_sample_rows: int = 10,
        output_path: str = "stories/",
        retention_days: int = 30,
        retention_count: int = 100,
        storage_options: Optional[Dict[str, Any]] = None,
        catalog_manager: Optional[Any] = None,
    ):
        """Initialize story generator.

        Args:
            pipeline_name: Name of the pipeline
            max_sample_rows: Maximum rows to show in samples
            output_path: Directory for story output
            retention_days: Days to keep stories
            retention_count: Max number of stories to keep
            storage_options: Credentials for remote storage (e.g. ADLS)
            catalog_manager: System Catalog Manager for historical context
        """
        self.pipeline_name = pipeline_name
        self.max_sample_rows = max_sample_rows
        self.output_path_str = output_path  # Store original string
        self.is_remote = "://" in output_path
        self.storage_options = storage_options or {}
        self.catalog_manager = catalog_manager

        # Track last generated story for alert enrichment
        self._last_story_path: Optional[str] = None
        self._last_metadata: Optional[PipelineStoryMetadata] = None

        if not self.is_remote:
            self.output_path = Path(output_path)
            self.output_path.mkdir(parents=True, exist_ok=True)
        else:
            self.output_path = None  # Handle remote paths differently

        self.retention_days = retention_days
        self.retention_count = retention_count

        ctx = get_logging_context()
        ctx.debug(
            "StoryGenerator initialized",
            pipeline=pipeline_name,
            output_path=output_path,
            is_remote=self.is_remote,
            retention_days=retention_days,
            retention_count=retention_count,
        )

    def generate(
        self,
        node_results: Dict[str, NodeResult],
        completed: List[str],
        failed: List[str],
        skipped: List[str],
        duration: float,
        start_time: str,
        end_time: str,
        context: Any = None,
        config: Optional[Dict[str, Any]] = None,
        graph_data: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Generate story HTML and JSON.

        Args:
            node_results: Dictionary of node name -> NodeResult
            completed: List of completed node names
            failed: List of failed node names
            skipped: List of skipped node names
            duration: Total pipeline duration
            start_time: ISO timestamp of start
            end_time: ISO timestamp of end
            context: Optional context to access intermediate DataFrames
            config: Optional pipeline configuration snapshot
            graph_data: Optional graph data dict with nodes/edges for DAG visualization

        Returns:
            Path to generated HTML story file
        """
        ctx = get_logging_context()
        ctx.debug(
            "Generating story",
            pipeline=self.pipeline_name,
            node_count=len(node_results),
            completed=len(completed),
            failed=len(failed),
            skipped=len(skipped),
        )

        # 1. Build metadata object
        metadata = PipelineStoryMetadata(
            pipeline_name=self.pipeline_name,
            pipeline_layer=config.get("layer") if config else None,
            started_at=start_time,
            completed_at=end_time,
            duration=duration,
            total_nodes=len(completed) + len(failed) + len(skipped),
            completed_nodes=len(completed),
            failed_nodes=len(failed),
            skipped_nodes=len(skipped),
            project=config.get("project") if config else None,
            plant=config.get("plant") if config else None,
            asset=config.get("asset") if config else None,
            business_unit=config.get("business_unit") if config else None,
        )

        # Add Git Info
        # git_info = self._get_git_info()
        # We can't easily add arbitrary fields to dataclass without changing it,
        # but we can rely on the fact that it's just metadata.
        # For now, let's skip adding git info to the core model or extend it later.

        # Process all nodes in order
        all_nodes = completed + failed + skipped

        # If we have config, try to follow config order instead of list order
        if config and "nodes" in config:
            config_order = [n["name"] for n in config["nodes"]]
            # Sort all_nodes based on index in config_order
            all_nodes.sort(key=lambda x: config_order.index(x) if x in config_order else 999)

        for node_name in all_nodes:
            if node_name in node_results:
                result = node_results[node_name]
                node_meta = self._convert_result_to_metadata(result, node_name)

                # Status overrides (result object has success bool, but we have lists)
                if node_name in failed:
                    node_meta.status = "failed"
                elif node_name in skipped:
                    node_meta.status = "skipped"
                else:
                    node_meta.status = "success"

                metadata.nodes.append(node_meta)
            else:
                # Skipped node without result
                metadata.nodes.append(
                    NodeExecutionMetadata(
                        node_name=node_name, operation="skipped", status="skipped", duration=0.0
                    )
                )

            # Enrich with Historical Context (if available)
            current_node = metadata.nodes[-1]
            if self.catalog_manager:
                try:
                    avg_rows = self.catalog_manager.get_average_volume(node_name)
                    avg_duration = self.catalog_manager.get_average_duration(node_name)

                    current_node.historical_avg_rows = avg_rows
                    current_node.historical_avg_duration = avg_duration

                    # Compute anomalies (Phase 1 - Triage)
                    self._compute_anomalies(current_node)
                except Exception as e:
                    ctx = get_logging_context()
                    ctx.debug(
                        "Failed to fetch historical metrics for node",
                        node_name=node_name,
                        error=str(e),
                    )

        # 2. Build graph data for interactive DAG (Phase 2)
        metadata.graph_data = self._build_graph_data(metadata, graph_data, config)

        # 3. Compare with last successful run (Phase 3)
        self._compare_with_last_success(metadata)

        # 4. Add git info (Phase 3)
        metadata.git_info = self._get_git_info()

        # 5. Render outputs
        timestamp_obj = datetime.now()
        date_str = timestamp_obj.strftime("%Y-%m-%d")
        time_str = timestamp_obj.strftime("%H-%M-%S")

        # Create structured path: {pipeline_name}/{date}/
        relative_folder = f"{self.pipeline_name}/{date_str}"

        if self.is_remote:
            base_path = f"{self.output_path_str.rstrip('/')}/{relative_folder}"
        else:
            base_path = self.output_path / relative_folder
            base_path.mkdir(parents=True, exist_ok=True)

        base_filename = f"run_{time_str}"

        # Prepare renderers
        html_renderer = HTMLStoryRenderer()
        json_renderer = JSONStoryRenderer()

        # Paths
        if self.is_remote:
            html_path = f"{base_path}/{base_filename}.html"
            json_path = f"{base_path}/{base_filename}.json"
        else:
            html_path = str(base_path / f"{base_filename}.html")
            json_path = str(base_path / f"{base_filename}.json")

        # Render HTML
        html_content = html_renderer.render(metadata)

        # Render JSON
        json_content = json_renderer.render(metadata)

        # Write files
        try:
            if self.is_remote:
                self._write_remote(html_path, html_content)
                self._write_remote(json_path, json_content)
            else:
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(html_content)
                with open(json_path, "w", encoding="utf-8") as f:
                    f.write(json_content)

            ctx.debug(
                "Story files written",
                html_path=html_path,
                html_size=len(html_content),
                json_path=json_path,
                json_size=len(json_content),
            )
        except Exception as e:
            ctx.error(
                "Failed to write story files",
                error=str(e),
                html_path=html_path,
                json_path=json_path,
            )
            raise

        # Store for alert enrichment
        self._last_story_path = html_path
        self._last_metadata = metadata

        # Cleanup and generate index
        self.cleanup()
        self._generate_pipeline_index()

        ctx.info(
            "Story generated",
            path=html_path,
            nodes=len(metadata.nodes),
            success_rate=metadata.get_success_rate(),
        )

        return html_path

    def get_alert_summary(self) -> Dict[str, Any]:
        """Get a summary of the last generated story for alerts.

        Returns:
            Dictionary with metrics suitable for alert payloads
        """
        if not self._last_metadata:
            return {}

        summary = self._last_metadata.get_alert_summary()
        summary["story_path"] = self._last_story_path
        return summary

    def _get_duration_history(self, node_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get duration history for a node across recent runs.

        Args:
            node_name: The node name to get history for
            limit: Maximum number of runs to include

        Returns:
            List of {"run_id": "...", "duration": 1.5, "started_at": "..."} dicts
        """
        import json

        ctx = get_logging_context()

        if self.is_remote:
            ctx.debug("Duration history not yet supported for remote storage")
            return []

        if self.output_path is None:
            return []

        pipeline_dir = self.output_path / self.pipeline_name
        if not pipeline_dir.exists():
            return []

        json_files = sorted(
            pipeline_dir.glob("**/*.json"),
            key=lambda p: str(p),
            reverse=True,
        )

        history = []
        for json_path in json_files[: limit + 1]:  # +1 to skip current run if it exists
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                for node_data in data.get("nodes", []):
                    if node_data.get("node_name") == node_name:
                        history.append(
                            {
                                "run_id": data.get("run_id", "unknown"),
                                "duration": node_data.get("duration", 0),
                                "started_at": data.get("started_at", ""),
                            }
                        )
                        break
            except Exception as e:
                ctx.debug(f"Failed to load run for duration history: {json_path}, error: {e}")
                continue

        return history[:limit]

    def _find_last_successful_run(self) -> Optional[Dict[str, Any]]:
        """Find the most recent successful run's JSON data.

        Returns:
            Dictionary of the last successful run metadata, or None
        """
        import json

        ctx = get_logging_context()

        if self.is_remote:
            ctx.debug("Cross-run comparison not yet supported for remote storage")
            return None

        if self.output_path is None:
            return None

        pipeline_dir = self.output_path / self.pipeline_name
        if not pipeline_dir.exists():
            return None

        # Find all JSON files, sorted by path (date/time order) descending
        json_files = sorted(
            pipeline_dir.glob("**/*.json"),
            key=lambda p: str(p),
            reverse=True,
        )

        # Find the most recent successful run
        for json_path in json_files:
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # Check if this run was successful (no failed nodes)
                if data.get("failed_nodes", 0) == 0:
                    ctx.debug(
                        "Found last successful run",
                        path=str(json_path),
                        run_id=data.get("run_id"),
                    )
                    return data
            except Exception as e:
                ctx.debug(f"Failed to load story JSON: {json_path}, error: {e}")
                continue

        return None

    def _compare_with_last_success(self, metadata: PipelineStoryMetadata) -> None:
        """Compare current run with last successful run and populate change_summary."""
        ctx = get_logging_context()

        # Collect duration history for all nodes (before comparison)
        for node in metadata.nodes:
            history = self._get_duration_history(node.node_name, limit=10)
            if history:
                node.duration_history = history

        last_success = self._find_last_successful_run()
        if not last_success:
            ctx.debug("No previous successful run found for comparison")
            return

        metadata.compared_to_run_id = last_success.get("run_id")

        # Build lookup for previous run's nodes
        prev_nodes = {n["node_name"]: n for n in last_success.get("nodes", [])}

        # Track changes
        sql_changed = []
        schema_changed = []
        rows_changed = []
        newly_failing = []
        duration_changed = []

        for node in metadata.nodes:
            prev = prev_nodes.get(node.node_name)
            if not prev:
                # New node, not in previous run
                continue

            changes = []

            # Compare SQL hash
            if node.sql_hash and prev.get("sql_hash"):
                if node.sql_hash != prev["sql_hash"]:
                    changes.append("sql")
                    sql_changed.append(node.node_name)
                    node.previous_sql_hash = prev["sql_hash"]

            # Compare schema (output)
            curr_schema = set(node.schema_out or [])
            prev_schema = set(prev.get("schema_out") or [])
            if curr_schema != prev_schema:
                changes.append("schema")
                schema_changed.append(node.node_name)

            # Compare row counts (significant change = >20%)
            if node.rows_out is not None and prev.get("rows_out") is not None:
                prev_rows = prev["rows_out"]
                if prev_rows > 0:
                    pct_change = abs(node.rows_out - prev_rows) / prev_rows
                    if pct_change > 0.2:
                        changes.append("rows")
                        rows_changed.append(node.node_name)
                        node.previous_rows_out = prev_rows

            # Compare duration (significant change = 2x slower)
            if node.duration and prev.get("duration"):
                prev_dur = prev["duration"]
                if prev_dur > 0 and node.duration >= prev_dur * 2:
                    changes.append("duration")
                    duration_changed.append(node.node_name)
                    node.previous_duration = prev_dur

            # Check if newly failing
            if node.status == "failed" and prev.get("status") == "success":
                newly_failing.append(node.node_name)

            # Capture previous config snapshot for diff viewer
            if prev.get("config_snapshot"):
                node.previous_config_snapshot = prev["config_snapshot"]

            if changes:
                node.changed_from_last_success = True
                node.changes_detected = changes

        # Build summary
        metadata.change_summary = {
            "has_changes": bool(sql_changed or schema_changed or rows_changed or newly_failing),
            "sql_changed_count": len(sql_changed),
            "sql_changed_nodes": sql_changed,
            "schema_changed_count": len(schema_changed),
            "schema_changed_nodes": schema_changed,
            "rows_changed_count": len(rows_changed),
            "rows_changed_nodes": rows_changed,
            "duration_changed_count": len(duration_changed),
            "duration_changed_nodes": duration_changed,
            "newly_failing_count": len(newly_failing),
            "newly_failing_nodes": newly_failing,
            "compared_to_run_id": metadata.compared_to_run_id,
        }

        ctx.debug(
            "Cross-run comparison complete",
            compared_to=metadata.compared_to_run_id,
            sql_changed=len(sql_changed),
            schema_changed=len(schema_changed),
            newly_failing=len(newly_failing),
        )

    def _build_graph_data(
        self,
        metadata: PipelineStoryMetadata,
        graph_data: Optional[Dict[str, Any]],
        config: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build enriched graph data for interactive DAG visualization.

        Combines static graph structure with runtime execution metadata.
        """
        ctx = get_logging_context()

        # Build node lookup for runtime data
        node_lookup = {n.node_name: n for n in metadata.nodes}

        # Debug: Log which path we're taking
        path_taken = (
            "graph_data"
            if graph_data
            else ("config" if config and "nodes" in config else "fallback")
        )
        ctx.debug(
            "Building graph data",
            path=path_taken,
            has_graph_data=bool(graph_data),
            has_config=bool(config),
            config_has_nodes=bool(config and "nodes" in config),
            metadata_node_count=len(metadata.nodes),
        )

        # Start with provided graph_data or build from config
        if graph_data:
            nodes = graph_data.get("nodes", [])
            edges = graph_data.get("edges", [])
        elif config and "nodes" in config:
            nodes = []
            edges = []
            for node_cfg in config["nodes"]:
                nodes.append(
                    {
                        "id": node_cfg["name"],
                        "label": node_cfg["name"],
                        "type": node_cfg.get("type", "transform"),
                    }
                )
                # Check depends_on for intra-pipeline dependencies
                for dep in node_cfg.get("depends_on", []):
                    edges.append({"source": dep, "target": node_cfg["name"]})

                # Check inputs block for cross-pipeline dependencies
                inputs = node_cfg.get("inputs", {})
                if inputs:
                    for input_name, input_val in inputs.items():
                        if isinstance(input_val, str) and input_val.startswith("$"):
                            ref = input_val[1:]
                            if "." in ref:
                                pipeline_name, node_ref = ref.split(".", 1)
                                edges.append(
                                    {
                                        "source": node_ref,
                                        "target": node_cfg["name"],
                                        "source_pipeline": pipeline_name,
                                    }
                                )
                            else:
                                edges.append({"source": ref, "target": node_cfg["name"]})
        else:
            # Fallback: build from metadata nodes
            nodes = [{"id": n.node_name, "label": n.node_name} for n in metadata.nodes]
            edges = []
            for n in metadata.nodes:
                # Debug: Log config_snapshot contents for each node
                ctx.debug(
                    "Fallback path: checking node config_snapshot",
                    node_name=n.node_name,
                    has_config_snapshot=bool(n.config_snapshot),
                    config_snapshot_keys=(
                        list(n.config_snapshot.keys()) if n.config_snapshot else []
                    ),
                    has_inputs=bool(n.config_snapshot and n.config_snapshot.get("inputs")),
                    inputs_value=n.config_snapshot.get("inputs") if n.config_snapshot else None,
                    has_depends_on=bool(n.config_snapshot and n.config_snapshot.get("depends_on")),
                )

                # Check depends_on for intra-pipeline dependencies
                if n.config_snapshot and n.config_snapshot.get("depends_on"):
                    for dep in n.config_snapshot["depends_on"]:
                        edges.append({"source": dep, "target": n.node_name})

                # Check inputs block for cross-pipeline dependencies
                if n.config_snapshot and n.config_snapshot.get("inputs"):
                    for input_name, input_val in n.config_snapshot["inputs"].items():
                        ctx.debug(
                            "Processing input reference",
                            node_name=n.node_name,
                            input_name=input_name,
                            input_val=input_val,
                            is_string=isinstance(input_val, str),
                            starts_with_dollar=isinstance(input_val, str)
                            and input_val.startswith("$"),
                        )
                        # Handle $pipeline.node reference format
                        if isinstance(input_val, str) and input_val.startswith("$"):
                            # Format: $pipeline_name.node_name
                            ref = input_val[1:]  # Remove $
                            if "." in ref:
                                pipeline_name, node_ref = ref.split(".", 1)
                                edges.append(
                                    {
                                        "source": node_ref,
                                        "target": n.node_name,
                                        "source_pipeline": pipeline_name,
                                    }
                                )
                                ctx.debug(
                                    "Added cross-pipeline edge",
                                    source=node_ref,
                                    target=n.node_name,
                                    source_pipeline=pipeline_name,
                                )
                            else:
                                edges.append({"source": ref, "target": n.node_name})
                                ctx.debug(
                                    "Added same-pipeline edge from inputs",
                                    source=ref,
                                    target=n.node_name,
                                )

        # Collect all node IDs that exist in the current pipeline
        existing_node_ids = {node["id"] for node in nodes}

        # Find cross-pipeline dependencies (edge sources that don't exist as nodes)
        # Build a map of node_ref -> pipeline_name for labeling
        external_node_pipelines = {}
        cross_pipeline_deps = set()
        for edge in edges:
            if edge["source"] not in existing_node_ids:
                cross_pipeline_deps.add(edge["source"])
                # Track the pipeline name if available
                if "source_pipeline" in edge:
                    external_node_pipelines[edge["source"]] = edge["source_pipeline"]

        # Debug: Log summary before adding external nodes
        ctx.debug(
            "Graph data summary",
            total_nodes=len(nodes),
            total_edges=len(edges),
            existing_node_ids=list(existing_node_ids),
            edge_sources=[e["source"] for e in edges],
            cross_pipeline_deps=list(cross_pipeline_deps),
        )

        # Add placeholder nodes for cross-pipeline dependencies
        for dep_id in cross_pipeline_deps:
            pipeline_name = external_node_pipelines.get(dep_id)
            label = f"{pipeline_name}.{dep_id}" if pipeline_name else dep_id
            ctx.debug(
                "Adding external node for cross-pipeline dependency",
                dep_id=dep_id,
                pipeline_name=pipeline_name,
                label=label,
            )
            nodes.append(
                {
                    "id": dep_id,
                    "label": label,
                    "type": "external",
                    "source_pipeline": pipeline_name,
                }
            )

        # Build dependency lookup: node_id -> list of source nodes (with pipeline info)
        node_dependencies = {}
        for edge in edges:
            target = edge["target"]
            source = edge["source"]
            source_pipeline = edge.get("source_pipeline")
            dep_label = f"{source_pipeline}.{source}" if source_pipeline else source

            if target not in node_dependencies:
                node_dependencies[target] = []
            node_dependencies[target].append(dep_label)

        # Enrich nodes with runtime execution data
        enriched_nodes = []
        for node in nodes:
            node_id = node["id"]
            runtime = node_lookup.get(node_id)
            is_external = node.get("type") == "external"

            enriched = {
                "id": node_id,
                "label": node.get("label", node_id),
                "type": node.get("type", "transform"),
                "status": runtime.status if runtime else ("external" if is_external else "unknown"),
                "duration": runtime.duration if runtime else 0,
                "rows_out": runtime.rows_out if runtime else None,
                "is_anomaly": runtime.is_anomaly if runtime else False,
                "is_slow": runtime.is_slow if runtime else False,
                "has_row_anomaly": runtime.has_row_anomaly if runtime else False,
                "error_message": runtime.error_message if runtime else None,
                "validation_count": len(runtime.validation_warnings) if runtime else 0,
                "is_external": is_external,
                "source_pipeline": node.get("source_pipeline"),
                "dependencies": node_dependencies.get(node_id, []),
            }
            enriched_nodes.append(enriched)

        return {
            "nodes": enriched_nodes,
            "edges": edges,
        }

    def _compute_anomalies(self, node: NodeExecutionMetadata) -> None:
        """Compute anomaly flags for a node based on historical data.

        Anomaly rules:
        - is_slow: node duration is 3x or more than historical avg
        - has_row_anomaly: rows_out deviates ±50% from historical avg
        """
        anomaly_reasons = []

        # Check for slow execution (3x threshold)
        if node.historical_avg_duration and node.historical_avg_duration > 0:
            if node.duration >= node.historical_avg_duration * 3:
                node.is_slow = True
                ratio = node.duration / node.historical_avg_duration
                avg_dur = node.historical_avg_duration
                anomaly_reasons.append(
                    f"Slow: {node.duration:.2f}s vs avg {avg_dur:.2f}s ({ratio:.1f}x)"
                )

        # Check for row count anomaly (±50% threshold)
        if node.historical_avg_rows and node.historical_avg_rows > 0 and node.rows_out is not None:
            pct_change = abs(node.rows_out - node.historical_avg_rows) / node.historical_avg_rows
            if pct_change >= 0.5:
                node.has_row_anomaly = True
                direction = "+" if node.rows_out > node.historical_avg_rows else "-"
                avg_rows = node.historical_avg_rows
                pct_str = f"{pct_change * 100:.0f}"
                anomaly_reasons.append(
                    f"Rows: {node.rows_out:,} vs avg {avg_rows:,.0f} ({direction}{pct_str}%)"
                )

        if anomaly_reasons:
            node.is_anomaly = True
            node.anomaly_reasons = anomaly_reasons

    def _convert_result_to_metadata(
        self, result: NodeResult, node_name: str
    ) -> NodeExecutionMetadata:
        """Convert NodeResult to NodeExecutionMetadata."""
        meta = result.metadata or {}

        # Extract Delta Info
        delta_info = None
        if "delta_info" in meta:
            d = meta["delta_info"]
            # Check if it's already an object or dict
            if isinstance(d, DeltaWriteInfo):
                delta_info = d
            else:
                # It might be a dict if coming from loose dict
                pass

        node_meta = NodeExecutionMetadata(
            node_name=node_name,
            operation="transform",  # Generic default
            status="success" if result.success else "failed",
            duration=result.duration,
            rows_out=result.rows_processed,
            schema_out=result.result_schema,
            # From metadata dict
            rows_in=None,  # Calculated if we had input info
            sample_in=meta.get("sample_data_in"),
            executed_sql=meta.get("executed_sql", []),
            sql_hash=meta.get("sql_hash"),
            transformation_stack=meta.get("transformation_stack", []),
            config_snapshot=meta.get("config_snapshot"),
            delta_info=delta_info,
            data_diff=meta.get("data_diff"),
            environment=meta.get("environment"),
            source_files=meta.get("source_files", []),
            null_profile=meta.get("null_profile"),
            schema_in=meta.get("schema_in"),
            sample_data=meta.get("sample_data"),
            columns_added=meta.get("columns_added", []),
            columns_removed=meta.get("columns_removed", []),
            error_message=str(result.error) if result.error else None,
            error_type=type(result.error).__name__ if result.error else None,
            error_traceback=meta.get("error_traceback"),
            error_traceback_cleaned=meta.get("error_traceback_cleaned"),
            validation_warnings=meta.get("validation_warnings", []),
            execution_steps=meta.get("steps", []),
            failed_rows_samples=meta.get("failed_rows_samples", {}),
            failed_rows_counts=meta.get("failed_rows_counts", {}),
            failed_rows_truncated=meta.get("failed_rows_truncated", False),
            truncated_validations=meta.get("truncated_validations", []),
            retry_history=meta.get("retry_history", []),
        )

        # Calculate derived metrics
        node_meta.calculate_row_change()  # Needs rows_in
        # schema changes are already in metadata from Node logic

        return node_meta

    def _write_remote(self, path: str, content: str) -> None:
        """Write content to remote path using fsspec."""
        ctx = get_logging_context()
        try:
            import fsspec

            # Use provided storage options (credentials)
            with fsspec.open(path, "w", encoding="utf-8", **self.storage_options) as f:
                f.write(content)
            ctx.debug("Remote file written", path=path, size=len(content))
        except ImportError:
            # Fallback for environments without fsspec (e.g., minimal Spark)
            # Try dbutils if on Databricks
            try:
                from pyspark.dbutils import DBUtils
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()
                dbutils = DBUtils(spark)
                # dbutils.fs.put expects string
                dbutils.fs.put(path, content, True)
                ctx.debug("Remote file written via dbutils", path=path, size=len(content))
            except Exception as e:
                ctx.error(
                    "Failed to write remote story",
                    path=path,
                    error=str(e),
                )
                raise RuntimeError(
                    f"Could not write story to {path}. Install 'fsspec' or 'adlfs'."
                ) from e

    def _clean_config_for_dump(self, config: Any) -> Any:
        """Clean configuration for YAML dumping.

        Handles multiline strings to force block style.
        """
        if isinstance(config, dict):
            return {k: self._clean_config_for_dump(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._clean_config_for_dump(v) for v in config]
        elif isinstance(config, str) and "\n" in config:
            # Use custom class to force block style
            # Strip trailing spaces from lines to allow block style
            cleaned = config.replace(" \n", "\n").strip()
            return MultilineString(cleaned)
        return config

    def _get_git_info(self) -> Dict[str, str]:
        """Get current git commit and branch."""
        try:
            # Run git commands silently
            commit = (
                subprocess.check_output(
                    ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL
                )
                .decode("utf-8")
                .strip()
            )

            branch = (
                subprocess.check_output(
                    ["git", "rev-parse", "--abbrev-ref", "HEAD"], stderr=subprocess.DEVNULL
                )
                .decode("utf-8")
                .strip()
            )

            return {"commit": commit, "branch": branch}
        except Exception:
            return {"commit": "unknown", "branch": "unknown"}

    def cleanup(self) -> None:
        """Remove old stories based on retention policy."""
        ctx = get_logging_context()

        if self.is_remote:
            self._cleanup_remote()
            return

        if self.output_path is None:
            return

        try:
            # 1. Clean new nested structure: {pipeline}/{date}/run_*.html
            pipeline_dir = self.output_path / self.pipeline_name
            if pipeline_dir.exists():
                # Find all files recursively
                stories = sorted(
                    pipeline_dir.glob("**/*.html"),
                    key=lambda p: str(p),  # Sort by path (date/time)
                    reverse=True,
                )
                json_stories = sorted(
                    pipeline_dir.glob("**/*.json"),
                    key=lambda p: str(p),
                    reverse=True,
                )

                self._apply_retention(stories, json_stories)

                # Clean empty date directories
                for date_dir in pipeline_dir.iterdir():
                    if date_dir.is_dir() and not any(date_dir.iterdir()):
                        try:
                            date_dir.rmdir()
                        except Exception:
                            pass

            # 2. Clean legacy flat structure: {pipeline}_*.html in root
            legacy_stories = sorted(
                self.output_path.glob(f"{self.pipeline_name}_*.html"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            # Only clean legacy if we have them
            if legacy_stories:
                # We don't want to count legacy + new against the same limit technically,
                # but for simplicity let's just clean legacy based on their own existence
                self._apply_retention(legacy_stories, [])

            ctx.debug(
                "Retention policy applied",
                pipeline=self.pipeline_name,
                retention_days=self.retention_days,
                retention_count=self.retention_count,
            )

        except Exception as e:
            ctx.warning("Story cleanup failed", error=str(e))

    def _apply_retention(self, stories: List[Path], json_stories: List[Path]) -> None:
        """Apply count and time retention policies."""
        from datetime import timedelta

        # 1. Count retention
        if self.retention_count is not None and len(stories) > self.retention_count:
            to_delete = stories[self.retention_count :]
            for path in to_delete:
                path.unlink(missing_ok=True)

        if self.retention_count is not None and len(json_stories) > self.retention_count:
            to_delete = json_stories[self.retention_count :]
            for path in to_delete:
                path.unlink(missing_ok=True)

        # 2. Time retention
        now = datetime.now()
        if self.retention_days is None:
            return
        cutoff = now - timedelta(days=self.retention_days)

        # Check remaining files
        # For nested files, we could parse date from folder name, but mtime is safer fallback
        retention_count = self.retention_count or 100
        remaining = stories[:retention_count] + json_stories[:retention_count]

        for path in remaining:
            if path.exists():
                # Try to infer date from path first (faster/more accurate than mtime)
                # Path format: .../{date}/run_{time}.html
                try:
                    # Try to parse parent folder as date
                    file_date = datetime.strptime(path.parent.name, "%Y-%m-%d")
                    if file_date < cutoff.replace(hour=0, minute=0, second=0, microsecond=0):
                        path.unlink(missing_ok=True)
                        continue
                except ValueError:
                    pass

                # Fallback to mtime
                mtime = datetime.fromtimestamp(path.stat().st_mtime)
                if mtime < cutoff:
                    path.unlink(missing_ok=True)

    def _cleanup_remote(self) -> None:
        """Clean up old stories from remote storage using fsspec."""
        ctx = get_logging_context()

        try:
            import fsspec
            from datetime import timedelta

            # Build the pipeline stories path
            pipeline_path = f"{self.output_path_str.rstrip('/')}/{self.pipeline_name}"

            # Get filesystem from the path
            fs, path_prefix = fsspec.core.url_to_fs(pipeline_path, **self.storage_options)

            # Check if path exists
            if not fs.exists(path_prefix):
                ctx.debug("Remote story path does not exist yet", path=pipeline_path)
                return

            # List all files recursively
            all_files = []
            try:
                for root, dirs, files in fs.walk(path_prefix):
                    for f in files:
                        if f.endswith((".html", ".json")):
                            full_path = f"{root}/{f}" if root else f
                            all_files.append(full_path)
            except Exception as e:
                ctx.debug(f"Could not walk remote path: {e}")
                return

            if not all_files:
                return

            # Sort by path (which includes date folders) - newest first
            all_files.sort(reverse=True)

            # Separate html and json
            html_files = [f for f in all_files if f.endswith(".html")]
            json_files = [f for f in all_files if f.endswith(".json")]

            deleted_count = 0

            # Apply count retention
            if self.retention_count is not None:
                if len(html_files) > self.retention_count:
                    for f in html_files[self.retention_count :]:
                        try:
                            fs.rm(f)
                            deleted_count += 1
                        except Exception:
                            pass

                if len(json_files) > self.retention_count:
                    for f in json_files[self.retention_count :]:
                        try:
                            fs.rm(f)
                            deleted_count += 1
                        except Exception:
                            pass

            # Apply time retention
            if self.retention_days is not None:
                cutoff = datetime.now() - timedelta(days=self.retention_days)
                cutoff_str = cutoff.strftime("%Y-%m-%d")

                # Check remaining files
                retention_count = self.retention_count or 100
                remaining = html_files[:retention_count] + json_files[:retention_count]

                for f in remaining:
                    # Try to parse date from path (format: .../YYYY-MM-DD/run_*.html)
                    try:
                        parts = f.replace("\\", "/").split("/")
                        for part in parts:
                            if len(part) == 10 and part[4] == "-" and part[7] == "-":
                                if part < cutoff_str:
                                    try:
                                        fs.rm(f)
                                        deleted_count += 1
                                    except Exception:
                                        pass
                                break
                    except Exception:
                        pass

            # Clean empty date directories
            try:
                for item in fs.ls(path_prefix, detail=False):
                    if fs.isdir(item):
                        contents = fs.ls(item, detail=False)
                        if not contents:
                            fs.rmdir(item)
            except Exception:
                pass

            if deleted_count > 0:
                ctx.debug(
                    "Remote story cleanup completed",
                    deleted=deleted_count,
                    pipeline=self.pipeline_name,
                )

        except ImportError:
            ctx.debug("fsspec not available for remote cleanup")
        except Exception as e:
            ctx.warning(f"Remote story cleanup failed: {e}")

    def _generate_pipeline_index(self) -> None:
        """Generate an index.html with a table of recent runs (Phase 3)."""
        import json

        ctx = get_logging_context()

        if self.is_remote:
            ctx.debug("Pipeline index not yet supported for remote storage")
            return

        if self.output_path is None:
            return

        pipeline_dir = self.output_path / self.pipeline_name
        if not pipeline_dir.exists():
            return

        # Find all JSON files
        json_files = sorted(
            pipeline_dir.glob("**/*.json"),
            key=lambda p: str(p),
            reverse=True,
        )

        if not json_files:
            return

        # Load metadata from each run
        runs = []
        for json_path in json_files[:50]:  # Limit to 50 most recent
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                html_path = json_path.with_suffix(".html")
                relative_html = html_path.relative_to(pipeline_dir)

                runs.append(
                    {
                        "run_id": data.get("run_id", "unknown"),
                        "started_at": data.get("started_at", ""),
                        "duration": data.get("duration", 0),
                        "total_nodes": data.get("total_nodes", 0),
                        "completed_nodes": data.get("completed_nodes", 0),
                        "failed_nodes": data.get("failed_nodes", 0),
                        "success_rate": data.get("success_rate", 0),
                        "html_path": str(relative_html).replace("\\", "/"),
                        "status": "failed" if data.get("failed_nodes", 0) > 0 else "success",
                    }
                )
            except Exception as e:
                ctx.debug(f"Failed to load run metadata: {json_path}, error: {e}")
                continue

        if not runs:
            return

        # Generate index HTML
        index_html = self._render_index_html(runs)
        index_path = pipeline_dir / "index.html"

        try:
            with open(index_path, "w", encoding="utf-8") as f:
                f.write(index_html)
            ctx.debug("Pipeline index generated", path=str(index_path), runs=len(runs))
        except Exception as e:
            ctx.warning(f"Failed to write pipeline index: {e}")

    def _render_index_html(self, runs: List[Dict[str, Any]]) -> str:
        """Render the pipeline history index HTML."""
        rows_html = ""
        for run in runs:
            status_class = "success" if run["status"] == "success" else "failed"
            status_icon = "✓" if run["status"] == "success" else "✗"
            rows_html += f"""
            <tr class="{status_class}">
                <td><a href="{run["html_path"]}">{run["run_id"]}</a></td>
                <td>{run["started_at"]}</td>
                <td>{run["duration"]:.2f}s</td>
                <td>{run["total_nodes"]}</td>
                <td class="status-cell {status_class}">{status_icon} {run["completed_nodes"]}/{run["total_nodes"]}</td>
                <td>{run["success_rate"]:.1f}%</td>
            </tr>
            """

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pipeline History: {self.pipeline_name}</title>
    <style>
        :root {{
            --primary-color: #0066cc;
            --success-color: #28a745;
            --error-color: #dc3545;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: #f4f7f9;
            margin: 0;
            padding: 20px;
        }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        h1 {{ color: var(--primary-color); margin-bottom: 20px; }}
        table {{
            width: 100%;
            background: #fff;
            border-collapse: collapse;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        th, td {{ padding: 12px 16px; text-align: left; border-bottom: 1px solid #e1e4e8; }}
        th {{ background: #f8f9fa; font-weight: 600; }}
        tr:hover {{ background: #f8f9fa; }}
        a {{ color: var(--primary-color); text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .status-cell.success {{ color: var(--success-color); font-weight: 600; }}
        .status-cell.failed {{ color: var(--error-color); font-weight: 600; }}
        tr.failed {{ background: #fff5f5; }}
    </style>
</head>
<body>
<div class="container">
    <h1>📊 Pipeline History: {self.pipeline_name}</h1>
    <p style="color: #666; margin-bottom: 20px;">Showing {len(runs)} most recent runs</p>
    <table>
        <thead>
            <tr>
                <th>Run ID</th>
                <th>Started</th>
                <th>Duration</th>
                <th>Nodes</th>
                <th>Status</th>
                <th>Success Rate</th>
            </tr>
        </thead>
        <tbody>
            {rows_html}
        </tbody>
    </table>
</div>
</body>
</html>
"""

    # Legacy methods removed as they are now handled by renderers
    # _generate_node_section, _sample_to_markdown, _dataframe_to_markdown
