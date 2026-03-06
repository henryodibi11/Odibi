"""Phase 3: Smart Chaining & Auto-Suggestion Tools.

Wires discovery tools → construction tools for 2-call workflows.
"""

from typing import Any, Dict, List
from pathlib import Path


def suggest_pipeline(profile: Dict[str, Any]) -> Dict[str, Any]:
    """Auto-suggest pattern based on profiled data characteristics.

    Analyzes profile_source results and recommends the best pattern + params.

    Args:
        profile: Output from profile_source (ProfileSourceResponse as dict)

    Returns:
        {
            "suggested_pattern": str,
            "confidence": float,
            "reason": str,
            "ready_for": {
                "apply_pattern_template": {...}
            }
        }
    """
    # Extract key characteristics
    candidate_keys = profile.get("candidate_keys", [])
    profile.get("candidate_watermarks", [])
    row_count_estimate = profile.get("row_count_estimate", 0)
    schema = profile.get("schema", {})
    columns = schema.get("columns", [])

    # Decision logic
    pattern = None
    confidence = 0.0
    reason = ""

    # Check for high-cardinality key columns
    key_columns = []
    for col in columns:
        if isinstance(col, dict):
            col_name = col.get("name", "")
            cardinality = col.get("cardinality", "")

            if cardinality == "unique" or col_name in candidate_keys:
                key_columns.append(col_name)

    # Pattern selection heuristics
    if len(key_columns) > 0:
        # Has keys - could be dimension or fact

        # Check for high volume
        if row_count_estimate and row_count_estimate > 100000:
            # High volume = likely fact table
            pattern = "fact"
            confidence = 0.8
            reason = f"High volume ({row_count_estimate:,} rows) with keys {key_columns} suggests transactional fact data"
        else:
            # Lower volume = likely dimension
            pattern = "dimension"
            confidence = 0.75
            reason = f"Moderate volume with keys {key_columns} suggests dimension/reference data"

    # Check for timestamp columns (incremental candidates)
    timestamp_cols = [
        col.get("name")
        for col in columns
        if isinstance(col, dict)
        and (
            "date" in col.get("dtype", "").lower()
            or "time" in col.get("dtype", "").lower()
            or col.get("name", "").lower() in ["created_at", "updated_at", "modified_date"]
        )
    ]

    if timestamp_cols and not pattern:
        pattern = "fact"
        confidence = 0.7
        reason = f"Timestamp columns {timestamp_cols} suggest event/transaction data (fact pattern)"

    # Fallback
    if not pattern:
        pattern = "dimension"
        confidence = 0.5
        reason = "Default to dimension pattern (reference data)"

    # Build ready_for params
    source_conn = profile.get("connection", "source")
    source_path = profile.get("path", "unknown")
    source_format = profile.get("format", "csv")

    # Determine if it's a table or file
    if "." in source_path and not source_path.endswith((".csv", ".parquet", ".json")):
        # Looks like schema.table
        source_table = source_path
        source_format = "sql"
        use_path = False
    else:
        source_table = None
        use_path = True

    ready_params = {
        "pattern": pattern,
        "pipeline_name": f"{pattern}_{Path(source_path).stem}",
        "source_connection": source_conn,
        "target_connection": "local",  # Default
        "target_path": f"gold/{Path(source_path).stem}",
        "source_format": source_format,
        "layer": "gold",
    }

    if use_path:
        ready_params["source_table"] = source_path
    else:
        ready_params["source_table"] = source_table

    # Add pattern-specific params
    if pattern == "dimension" and key_columns:
        ready_params["natural_key"] = key_columns[0]
        ready_params["surrogate_key"] = f"{key_columns[0]}_sk"
    elif pattern in ("fact", "scd2") and key_columns:
        ready_params["keys"] = key_columns[:3]  # Top 3 key candidates

    if pattern == "scd2" and len(columns) > 3:
        # Suggest tracking columns (exclude keys and timestamps)
        track_cols = [
            col.get("name")
            for col in columns
            if isinstance(col, dict)
            and col.get("name") not in key_columns
            and "date" not in col.get("dtype", "").lower()
        ][:5]  # Top 5

        if track_cols:
            ready_params["tracked_columns"] = track_cols

    return {
        "suggested_pattern": pattern,
        "confidence": confidence,
        "reason": reason,
        "key_columns_found": key_columns,
        "timestamp_columns_found": timestamp_cols,
        "ready_for": {"apply_pattern_template": ready_params},
        "next_step": "Call apply_pattern_template with ready_for params (or adjust if needed)",
    }


def create_ingestion_pipeline(
    pipeline_name: str,
    source_connection: str,
    target_connection: str,
    tables: List[Dict[str, Any]],
    layer: str = "bronze",
    target_format: str = "delta",
    mode: str = "append_once",
) -> Dict[str, Any]:
    """Create a multi-table bulk ingestion pipeline.

    Generates a pipeline with one node per table for parallel ingestion.

    Args:
        pipeline_name: Pipeline name
        source_connection: Source connection (SQL database)
        target_connection: Target connection (data lake)
        tables: List of table specs [{"schema": "dbo", "table": "Orders", "keys": ["id"]}]
        layer: Pipeline layer (default: bronze)
        target_format: Output format (default: delta)
        mode: Write mode (default: append_once for idempotent ingestion)

    Returns:
        {
            "yaml": str,
            "valid": bool,
            "node_count": int,
            "tables_ingested": list
        }
    """
    from odibi_mcp.tools.builder import (
        create_pipeline as create_session,
        add_node,
        configure_read,
        configure_write,
        render_pipeline_yaml,
        discard_pipeline,
    )

    errors = []
    warnings = []

    if not tables:
        return {
            "yaml": "",
            "valid": False,
            "errors": [{"code": "NO_TABLES", "message": "No tables provided"}],
            "warnings": [],
        }

    # Create builder session
    session = create_session(pipeline_name, layer)

    if "error" in session:
        return {"yaml": "", "valid": False, "errors": [session["error"]], "warnings": []}

    sid = session["session_id"]
    nodes_created = []

    try:
        # Add a node for each table
        for i, table_spec in enumerate(tables):
            schema = table_spec.get("schema", "dbo")
            table = table_spec.get("table")
            keys = table_spec.get("keys", [])
            where = table_spec.get("where")

            if not table:
                warnings.append(f"Table spec {i} missing 'table' field, skipping")
                continue

            # Sanitize node name
            node_name = f"{schema}_{table}".lower().replace(".", "_").replace("-", "_")

            # Add node
            result = add_node(sid, node_name)
            if "error" in result:
                errors.append(result["error"])
                continue

            # Configure read
            read_table = f"{schema}.{table}"
            read_params = {
                "session_id": sid,
                "node_name": node_name,
                "connection": source_connection,
                "format": "sql",
                "table": read_table,
            }

            if where:
                read_params["query"] = f"SELECT * FROM {read_table} WHERE {where}"
                del read_params["table"]

            result = configure_read(**read_params)
            if "error" in result:
                errors.append(result["error"])
                continue

            # Configure write
            target_path = f"{layer}/{schema}_{table}".lower()
            write_params = {
                "session_id": sid,
                "node_name": node_name,
                "connection": target_connection,
                "format": target_format,
                "path": target_path,
                "mode": mode,
            }

            if mode in ("upsert", "append_once", "merge") and keys:
                write_params["keys"] = keys

            result = configure_write(**write_params)
            if "error" in result:
                errors.append(result["error"])
                continue

            nodes_created.append(node_name)

        if not nodes_created:
            discard_pipeline(sid)
            return {
                "yaml": "",
                "valid": False,
                "errors": errors or [{"code": "NO_NODES", "message": "No nodes created"}],
                "warnings": warnings,
            }

        # Render pipeline
        result = render_pipeline_yaml(sid)

        if not result.get("valid"):
            return {
                "yaml": result.get("yaml", ""),
                "valid": False,
                "errors": result.get("errors", []) + errors,
                "warnings": warnings + result.get("warnings", []),
            }

        return {
            "yaml": result["yaml"],
            "valid": True,
            "node_count": len(nodes_created),
            "tables_ingested": nodes_created,
            "errors": errors,
            "warnings": warnings,
            "next_step": "Save YAML and run with: python -m odibi run <file>.yaml",
        }

    except Exception as e:
        # Clean up session on error
        discard_pipeline(sid)
        return {
            "yaml": "",
            "valid": False,
            "errors": errors + [{"code": "BUILD_FAILED", "message": str(e)}],
            "warnings": warnings,
        }
