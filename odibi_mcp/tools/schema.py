# odibi_mcp/tools/schema.py
"""Schema-related MCP tools - wired to real pipeline outputs."""

import logging
from dataclasses import dataclass, field
from typing import List, Optional
from pathlib import Path

from odibi_mcp.contracts.schema import SchemaResponse, ColumnSpec
from odibi_mcp.contracts.resources import ResourceRef
from odibi_mcp.context import get_project_context

logger = logging.getLogger(__name__)


@dataclass
class ColumnDiff:
    """Difference for a single column."""

    column: str
    status: str  # "added", "removed", "type_changed", "nullable_changed"
    source_type: Optional[str] = None
    target_type: Optional[str] = None
    source_nullable: Optional[bool] = None
    target_nullable: Optional[bool] = None


@dataclass
class SchemaDiffResponse:
    """Response for compare_schemas."""

    source_name: str
    target_name: str
    is_compatible: bool
    added_columns: List[str] = field(default_factory=list)
    removed_columns: List[str] = field(default_factory=list)
    type_changes: List[ColumnDiff] = field(default_factory=list)
    nullable_changes: List[ColumnDiff] = field(default_factory=list)
    common_columns: List[str] = field(default_factory=list)
    summary: str = ""


def compare_schemas(
    source_connection: str,
    source_path: str,
    target_connection: str,
    target_path: str,
    source_sheet: Optional[str] = None,
    target_sheet: Optional[str] = None,
) -> SchemaDiffResponse:
    """
    Compare schemas between two data sources.

    Useful for validating source-to-target compatibility before building pipelines.
    Returns differences in columns, types, and nullability.

    Args:
        source_connection: Connection name for source
        source_path: Path to source file/table
        target_connection: Connection name for target
        target_path: Path to target file/table
        source_sheet: Optional sheet name for Excel source
        target_sheet: Optional sheet name for Excel target
    """
    from odibi_mcp.tools.discovery import infer_schema

    # Get source schema
    source_schema = infer_schema(
        connection=source_connection,
        path=source_path,
        sheet=source_sheet,
    )

    # Get target schema
    target_schema = infer_schema(
        connection=target_connection,
        path=target_path,
        sheet=target_sheet,
    )

    # Build column maps
    source_cols = {col.name: col for col in source_schema.columns}
    target_cols = {col.name: col for col in target_schema.columns}

    source_names = set(source_cols.keys())
    target_names = set(target_cols.keys())

    # Calculate differences
    added = sorted(target_names - source_names)
    removed = sorted(source_names - target_names)
    common = sorted(source_names & target_names)

    type_changes = []
    nullable_changes = []

    for col_name in common:
        src_col = source_cols[col_name]
        tgt_col = target_cols[col_name]

        # Normalize types for comparison
        src_type = src_col.dtype.lower().replace("int64", "int").replace("float64", "float")
        tgt_type = tgt_col.dtype.lower().replace("int64", "int").replace("float64", "float")

        if src_type != tgt_type:
            type_changes.append(
                ColumnDiff(
                    column=col_name,
                    status="type_changed",
                    source_type=src_col.dtype,
                    target_type=tgt_col.dtype,
                )
            )

        if src_col.nullable != tgt_col.nullable:
            nullable_changes.append(
                ColumnDiff(
                    column=col_name,
                    status="nullable_changed",
                    source_nullable=src_col.nullable,
                    target_nullable=tgt_col.nullable,
                )
            )

    # Determine compatibility
    is_compatible = len(removed) == 0 and len(type_changes) == 0

    # Build summary
    summary_parts = []
    if is_compatible:
        summary_parts.append("Schemas are compatible.")
    else:
        summary_parts.append("Schemas have breaking differences.")

    if added:
        summary_parts.append(f"{len(added)} columns added in target.")
    if removed:
        summary_parts.append(f"{len(removed)} columns missing in target (BREAKING).")
    if type_changes:
        summary_parts.append(f"{len(type_changes)} type mismatches (BREAKING).")
    if nullable_changes:
        summary_parts.append(f"{len(nullable_changes)} nullability changes.")
    if not any([added, removed, type_changes, nullable_changes]):
        summary_parts.append("Schemas are identical.")

    return SchemaDiffResponse(
        source_name=f"{source_connection}/{source_path}",
        target_name=f"{target_connection}/{target_path}",
        is_compatible=is_compatible,
        added_columns=added,
        removed_columns=removed,
        type_changes=type_changes,
        nullable_changes=nullable_changes,
        common_columns=common,
        summary=" ".join(summary_parts),
    )


@dataclass
class OutputInfo:
    """Information about a pipeline output."""

    name: str
    node: str
    schema: Optional[SchemaResponse]
    resource: ResourceRef


@dataclass
class ListOutputsResponse:
    """List of pipeline outputs."""

    pipeline: str
    outputs: List[OutputInfo]


def output_schema(
    pipeline: str,
    output_name: str,
) -> SchemaResponse:
    """
    Get schema for a pipeline output.

    Reads the actual output file to infer schema.
    Supports both 'outputs' (multi-output) and 'write' (single output) patterns.
    """
    ctx = get_project_context()
    if not ctx:
        return SchemaResponse(columns=[], row_count=None, partition_columns=[])

    pipeline_config = ctx.get_pipeline(pipeline)
    if not pipeline_config:
        return SchemaResponse(columns=[], row_count=None, partition_columns=[])

    # Find the output in pipeline nodes
    for node in pipeline_config.get("nodes", []):
        node_name = node.get("name", "unknown")

        # Collect all outputs to check
        output_configs = []

        # Check 'outputs' dict (multi-output pattern)
        for out_name, out_config in node.get("outputs", {}).items():
            if isinstance(out_config, dict):
                output_configs.append((out_name, out_config))

        # Check 'write' block (single output pattern)
        write_config = node.get("write")
        if isinstance(write_config, dict):
            output_configs.append((node_name, write_config))

        for out_name, out_config in output_configs:
            if out_name == output_name or f"{node_name}.{out_name}" == output_name:
                conn_name = out_config.get("connection")
                out_path = out_config.get("path")

                if not conn_name or not out_path:
                    continue

                try:
                    conn = ctx.get_connection(conn_name)
                    full_path = Path(conn.get_path(out_path))

                    if not full_path.exists():
                        continue

                    import pandas as pd

                    suffix = full_path.suffix.lower()

                    if suffix == ".csv":
                        df = pd.read_csv(full_path, nrows=100)
                    elif suffix == ".parquet":
                        df = pd.read_parquet(full_path)
                    elif suffix == ".json":
                        df = pd.read_json(full_path, lines=True, nrows=100)
                    elif full_path.is_dir():
                        # Delta or partitioned
                        parquet_files = list(full_path.glob("*.parquet"))
                        if parquet_files:
                            df = pd.read_parquet(parquet_files[0])
                        else:
                            continue
                    else:
                        continue

                    columns = []
                    for col in df.columns:
                        columns.append(
                            ColumnSpec(
                                name=col,
                                dtype=str(df[col].dtype),
                                nullable=df[col].isnull().any(),
                            )
                        )

                    return SchemaResponse(
                        columns=columns,
                        row_count=len(df),
                        partition_columns=[],
                    )

                except Exception as e:
                    logger.warning(f"Could not read schema for {output_name}: {e}")

    return SchemaResponse(columns=[], row_count=None, partition_columns=[])


def list_outputs(
    pipeline: str,
) -> ListOutputsResponse:
    """
    List all outputs for a pipeline.

    Extracts outputs from pipeline configuration.
    Supports both 'outputs' (multi-output) and 'write' (single output) patterns.
    """
    ctx = get_project_context()
    if not ctx:
        return ListOutputsResponse(pipeline=pipeline, outputs=[])

    pipeline_config = ctx.get_pipeline(pipeline)
    if not pipeline_config:
        return ListOutputsResponse(pipeline=pipeline, outputs=[])

    outputs = []

    for node in pipeline_config.get("nodes", []):
        node_name = node.get("name", "unknown")

        # Collect outputs from both 'outputs' dict and 'write' block
        output_configs = []

        # Check for 'outputs' pattern (multi-output nodes)
        node_outputs = node.get("outputs", {})
        for out_name, out_config in node_outputs.items():
            if isinstance(out_config, dict):
                output_configs.append((out_name, out_config))

        # Check for 'write' pattern (single output, standard odibi pattern)
        write_config = node.get("write")
        if isinstance(write_config, dict):
            output_configs.append((node_name, write_config))

        # Process all outputs
        for out_name, out_config in output_configs:
            conn_name = out_config.get("connection", "unknown")
            out_path = out_config.get("path", "")
            out_format = out_config.get("format", "unknown")

            # Try to get schema
            schema = None
            try:
                if conn_name and out_path:
                    conn = ctx.get_connection(conn_name)
                    full_path = Path(conn.get_path(out_path))

                    if full_path.exists():
                        import pandas as pd

                        suffix = full_path.suffix.lower()
                        if suffix == ".csv":
                            df = pd.read_csv(full_path, nrows=10)
                        elif suffix == ".parquet":
                            df = pd.read_parquet(full_path).head(10)
                        elif full_path.is_dir():
                            parquet_files = list(full_path.glob("*.parquet"))
                            if parquet_files:
                                df = pd.read_parquet(parquet_files[0]).head(10)
                            else:
                                df = None
                        else:
                            df = None

                        if df is not None:
                            columns = [
                                ColumnSpec(name=col, dtype=str(df[col].dtype), nullable=True)
                                for col in df.columns
                            ]
                            schema = SchemaResponse(
                                columns=columns, row_count=None, partition_columns=[]
                            )
            except Exception:
                pass

            outputs.append(
                OutputInfo(
                    name=out_name,
                    node=node_name,
                    schema=schema,
                    resource=ResourceRef(
                        kind=out_format,
                        logical_name=out_path,
                        connection=conn_name,
                    ),
                )
            )

    return ListOutputsResponse(pipeline=pipeline, outputs=outputs)
