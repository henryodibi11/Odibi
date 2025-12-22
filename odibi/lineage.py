import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

try:
    from openlineage.client import OpenLineageClient
    from openlineage.client.facet import (
        DocumentationJobFacet,
        ErrorMessageRunFacet,
        NominalTimeRunFacet,
        ParentRunFacet,
        ProcessingEngineRunFacet,
        SchemaDatasetFacet,
        SchemaField,
        SourceCodeJobFacet,
    )
    from openlineage.client.run import (
        InputDataset,
        Job,
        OutputDataset,
        Run,
        RunEvent,
        RunState,
    )

    HAS_OPENLINEAGE = True
except ImportError:
    HAS_OPENLINEAGE = False
    InputDataset = Any
    OutputDataset = Any
    RunEvent = Any  # Also needed for type hints? no, I didn't use it in signature

from odibi.config import LineageConfig, NodeConfig, PipelineConfig
from odibi.node import NodeResult

logger = logging.getLogger(__name__)


class OpenLineageAdapter:
    """Adapter for OpenLineage integration."""

    def __init__(self, config: Optional[LineageConfig] = None):
        self.enabled = HAS_OPENLINEAGE and config is not None
        if not HAS_OPENLINEAGE:
            logger.debug("OpenLineage not installed. Skipping lineage.")
            return

        if not config:
            self.enabled = False
            return

        url = config.url or os.getenv("OPENLINEAGE_URL")
        api_key = config.api_key or os.getenv("OPENLINEAGE_API_KEY")

        if not url:
            self.enabled = False
            return

        try:
            self.client = OpenLineageClient(url=url, api_key=api_key)
            self.namespace = config.namespace
            self.pipeline_run_id = None
            self.pipeline_name = None
        except Exception as e:
            logger.warning(f"Failed to initialize OpenLineage client: {e}", exc_info=True)
            self.enabled = False

    def emit_pipeline_start(self, pipeline_config: PipelineConfig) -> str:
        """Emit pipeline start event (Parent Run)."""
        if not self.enabled:
            return str(uuid.uuid4())

        try:
            self.pipeline_run_id = str(uuid.uuid4())
            self.pipeline_name = pipeline_config.pipeline

            event_time = datetime.now(timezone.utc).isoformat()

            run = Run(
                runId=self.pipeline_run_id,
                facets={
                    "nominalTime": NominalTimeRunFacet(
                        nominalStartTime=event_time, nominalEndTime=None
                    ),
                    "processing_engine": ProcessingEngineRunFacet(
                        version=__import__("odibi").__version__,
                        name="Odibi",
                        openlineageAdapterVersion=__import__("odibi").__version__,
                    ),
                },
            )

            job = Job(
                namespace=self.namespace,
                name=pipeline_config.pipeline,
                facets={
                    "documentation": DocumentationJobFacet(
                        description=pipeline_config.description or "Odibi Pipeline"
                    )
                },
            )

            event = RunEvent(
                eventType=RunState.START,
                eventTime=event_time,
                run=run,
                job=job,
                inputs=[],
                outputs=[],
                producer="https://github.com/henryodibi11/Odibi",
            )

            self.client.emit(event)
            return self.pipeline_run_id

        except Exception as e:
            logger.warning(f"Failed to emit OpenLineage pipeline start: {e}", exc_info=True)
            return str(uuid.uuid4())

    def emit_pipeline_complete(self, pipeline_config: PipelineConfig, results: Any):
        """Emit pipeline completion event."""
        if not self.enabled or not self.pipeline_run_id:
            return

        try:
            event_time = datetime.now(timezone.utc).isoformat()

            # Determine success based on results
            success = not results.failed
            event_type = RunState.COMPLETE if success else RunState.FAIL

            run_facets = {}
            if not success:
                run_facets["errorMessage"] = ErrorMessageRunFacet(
                    message=f"Pipeline failed with nodes: {results.failed}",
                    programmingLanguage="python",
                )

            run = Run(runId=self.pipeline_run_id, facets=run_facets)

            job = Job(namespace=self.namespace, name=pipeline_config.pipeline)

            event = RunEvent(
                eventType=event_type,
                eventTime=event_time,
                run=run,
                job=job,
                inputs=[],
                outputs=[],
                producer="https://github.com/henryodibi11/Odibi",
            )

            self.client.emit(event)

        except Exception as e:
            logger.warning(f"Failed to emit OpenLineage pipeline complete: {e}", exc_info=True)

    def emit_node_start(self, config: NodeConfig, parent_run_id: str):
        """Emit node start event."""
        if not self.enabled:
            return str(uuid.uuid4())

        try:
            run_id = str(uuid.uuid4())
            event_time = datetime.now(timezone.utc).isoformat()

            # Resolve Inputs
            inputs = []
            if config.read:
                # We need connection obj to resolve path?
                # Without access to instantiated connections here, we do best effort with names
                # Ideally we pass connections to adapter, but adapter is initialized once.
                # We can accept connections as arg? Or just use string names for now.
                # Let's use string logic for now.
                ds = self._create_dataset_from_config(config.read, is_input=True)
                if ds:
                    inputs.append(ds)
            elif config.depends_on:
                # Dependency inputs? Not external datasets usually, but internal.
                # OpenLineage tracks DATASETS. Internal DFs are ephemeral.
                pass

            run_facets = {
                "parent": ParentRunFacet(
                    run={"runId": parent_run_id},
                    job={
                        "namespace": self.namespace,
                        "name": self.pipeline_name or "unknown_pipeline",
                    },
                )
            }

            job_facets = {
                "sourceCode": SourceCodeJobFacet(
                    language="python",
                    source_code=(
                        str(config.model_dump_json())
                        if hasattr(config, "model_dump_json")
                        else str(config.model_dump())
                    ),
                )
            }

            if config.description:
                job_facets["documentation"] = DocumentationJobFacet(description=config.description)

            run = Run(runId=run_id, facets=run_facets)

            job = Job(
                namespace=self.namespace,
                name=f"{self.pipeline_name}.{config.name}",
                facets=job_facets,
            )

            event = RunEvent(
                eventType=RunState.START,
                eventTime=event_time,
                run=run,
                job=job,
                inputs=inputs,
                outputs=[],
                producer="https://github.com/henryodibi11/Odibi",
            )

            self.client.emit(event)
            return run_id

        except Exception as e:
            logger.warning(f"Failed to emit OpenLineage node start: {e}")
            return str(uuid.uuid4())

    def emit_node_complete(self, config: NodeConfig, result: NodeResult, run_id: str):
        """Emit node completion event."""
        if not self.enabled or not run_id:
            return

        try:
            event_time = datetime.now(timezone.utc).isoformat()
            event_type = RunState.COMPLETE if result.success else RunState.FAIL

            outputs = []
            if config.write:
                ds = self._create_dataset_from_config(
                    config.write, is_input=False, schema=result.result_schema
                )
                if ds:
                    outputs.append(ds)

            run_facets = {}
            if not result.success and result.error:
                run_facets["errorMessage"] = ErrorMessageRunFacet(
                    message=str(result.error), programmingLanguage="python"
                )

            run = Run(runId=run_id, facets=run_facets)

            job = Job(namespace=self.namespace, name=f"{self.pipeline_name}.{config.name}")

            event = RunEvent(
                eventType=event_type,
                eventTime=event_time,
                run=run,
                job=job,
                inputs=[],
                outputs=outputs,
                producer="https://github.com/henryodibi11/Odibi",
            )

            self.client.emit(event)

        except Exception as e:
            logger.warning(f"Failed to emit OpenLineage node complete: {e}")

    def _create_dataset_from_config(
        self, config_op: Any, is_input: bool, schema: Any = None
    ) -> Optional[Union[InputDataset, OutputDataset]]:
        """Create OpenLineage Dataset from Read/Write config."""
        # Best effort naming
        try:
            conn_name = config_op.connection
            name = config_op.path or config_op.table or "unknown"

            # Namespace strategy: connection name usually maps to a storage account/container
            namespace = f"{self.namespace}.{conn_name}"

            facets = {}
            if schema:
                fields = []
                # schema is usually a dict {col: type}
                if isinstance(schema, dict):
                    for col, dtype in schema.items():
                        fields.append(SchemaField(name=col, type=str(dtype)))

                if fields:
                    facets["schema"] = SchemaDatasetFacet(fields=fields)

            if is_input:
                return InputDataset(namespace=namespace, name=name, facets=facets)
            else:
                return OutputDataset(namespace=namespace, name=name, facets=facets)
        except Exception:
            return None


class LineageTracker:
    """Track cross-pipeline lineage relationships.

    This class provides table-level lineage tracking across pipelines,
    storing relationships in the System Catalog for later querying.

    Example:
        ```python
        tracker = LineageTracker(catalog)
        tracker.record_lineage(
            read_config=node.read,
            write_config=node.write,
            pipeline="silver_pipeline",
            node="process_customers",
            run_id="run-123",
            connections=connections
        )
        ```
    """

    def __init__(self, catalog: Optional[Any] = None):
        """Initialize LineageTracker.

        Args:
            catalog: CatalogManager instance for persistence
        """
        self.catalog = catalog

    def record_lineage(
        self,
        read_config: Optional[Any],
        write_config: Optional[Any],
        pipeline: str,
        node: str,
        run_id: str,
        connections: Dict[str, Any],
    ) -> None:
        """Record lineage from node's read/write config.

        Args:
            read_config: ReadConfig from the node
            write_config: WriteConfig from the node
            pipeline: Pipeline name
            node: Node name
            run_id: Execution run ID
            connections: Dictionary of connection configurations
        """
        if not self.catalog or not write_config:
            return

        target_table = self._resolve_table_path(write_config, connections)
        if not target_table:
            return

        if read_config:
            source_table = self._resolve_table_path(read_config, connections)
            if source_table:
                self.catalog.record_lineage(
                    source_table=source_table,
                    target_table=target_table,
                    target_pipeline=pipeline,
                    target_node=node,
                    run_id=run_id,
                )

    def record_dependency_lineage(
        self,
        depends_on: List[str],
        write_config: Optional[Any],
        pipeline: str,
        node: str,
        run_id: str,
        node_outputs: Dict[str, str],
        connections: Dict[str, Any],
    ) -> None:
        """Record lineage from node dependencies.

        Args:
            depends_on: List of dependency node names
            write_config: WriteConfig from the node
            pipeline: Pipeline name
            node: Node name
            run_id: Execution run ID
            node_outputs: Map of node names to their output table paths
            connections: Dictionary of connection configurations
        """
        if not self.catalog or not write_config:
            return

        target_table = self._resolve_table_path(write_config, connections)
        if not target_table:
            return

        for dep_node in depends_on:
            source_table = node_outputs.get(dep_node)
            if source_table:
                self.catalog.record_lineage(
                    source_table=source_table,
                    target_table=target_table,
                    source_pipeline=pipeline,
                    source_node=dep_node,
                    target_pipeline=pipeline,
                    target_node=node,
                    run_id=run_id,
                )

    def _resolve_table_path(
        self,
        config: Any,
        connections: Dict[str, Any],
    ) -> Optional[str]:
        """Resolve full table path from read/write config.

        Args:
            config: ReadConfig or WriteConfig
            connections: Dictionary of connection configurations

        Returns:
            Full table path (e.g., "connection/path" or "catalog.schema.table")
        """
        try:
            conn_name = config.connection
            path = getattr(config, "path", None)
            table = getattr(config, "table", None)

            if table:
                conn = connections.get(conn_name)
                if conn and hasattr(conn, "schema_name"):
                    catalog = getattr(conn, "catalog", "")
                    schema = conn.schema_name
                    return f"{catalog}.{schema}.{table}" if catalog else f"{schema}.{table}"
                return f"{conn_name}.{table}"

            if path:
                return f"{conn_name}/{path}"

            return None
        except Exception:
            return None

    def get_upstream(self, table_path: str, depth: int = 3) -> List[Dict]:
        """Get all upstream sources for a table.

        Args:
            table_path: Table to trace upstream from
            depth: Maximum depth to traverse (default: 3)

        Returns:
            List of upstream lineage records with depth information
        """
        if not self.catalog:
            return []
        return self.catalog.get_upstream(table_path, depth)

    def get_downstream(self, table_path: str, depth: int = 3) -> List[Dict]:
        """Get all downstream consumers of a table.

        Args:
            table_path: Table to trace downstream from
            depth: Maximum depth to traverse (default: 3)

        Returns:
            List of downstream lineage records with depth information
        """
        if not self.catalog:
            return []
        return self.catalog.get_downstream(table_path, depth)

    def get_impact_analysis(self, table_path: str, depth: int = 3) -> Dict[str, Any]:
        """Perform impact analysis for a table.

        Args:
            table_path: Table to analyze impact for
            depth: Maximum depth to traverse (default: 3)

        Returns:
            Dict containing:
            - affected_tables: list of downstream tables
            - affected_pipelines: list of affected pipelines
            - total_depth: maximum depth reached
        """
        downstream = self.get_downstream(table_path, depth)

        affected_tables = set()
        affected_pipelines = set()
        max_depth = 0

        for record in downstream:
            target = record.get("target_table")
            if target:
                affected_tables.add(target)
            pipeline = record.get("target_pipeline")
            if pipeline:
                affected_pipelines.add(pipeline)
            record_depth = record.get("depth", 0)
            if record_depth > max_depth:
                max_depth = record_depth

        return {
            "table": table_path,
            "affected_tables": list(affected_tables),
            "affected_pipelines": list(affected_pipelines),
            "total_depth": max_depth,
            "downstream_count": len(downstream),
        }
