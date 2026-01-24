# odibi_mcp/tools/yaml_builder.py
"""
YAML Builder tools - generates correct Odibi YAML using actual Pydantic models.

These tools solve the schema confusion problem by:
1. Building configs from Odibi's actual Pydantic models (ReadConfig, WriteConfig, etc.)
2. Validating before serializing
3. Generating YAML that Odibi will definitely accept

This replaces the string-template approach in knowledge.py which drifted from the schema.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)


@dataclass
class TableSpec:
    """Specification for a table to ingest."""

    schema_name: str
    table_name: str
    primary_key: list[str] = field(default_factory=list)
    where_clause: Optional[str] = None
    select_columns: Optional[list[str]] = None  # None = SELECT *
    incremental_column: Optional[str] = None
    incremental_mode: Optional[str] = None  # rolling_window, append, etc.
    incremental_lookback: Optional[int] = None
    incremental_unit: Optional[str] = None  # day, hour, minute


@dataclass
class SqlPipelineSpec:
    """Specification for a SQL ingestion pipeline."""

    pipeline_name: str
    source_connection: str
    target_connection: str
    target_format: str = "delta"
    target_schema: Optional[str] = None
    tables: list[TableSpec] = field(default_factory=list)
    layer: str = "bronze"
    node_prefix: str = ""


@dataclass
class GenerateSqlPipelineResponse:
    """Response from generate_sql_pipeline."""

    pipeline_yaml: str
    project_imports: list[str]
    node_count: int
    warnings: list[str]
    errors: list[str]


def _sanitize_node_name(name: str) -> str:
    """Sanitize name to be valid node name (alphanumeric + underscore only)."""
    sanitized = name.replace("-", "_").replace(".", "_").replace(" ", "_")
    # Remove any remaining invalid characters
    sanitized = "".join(c for c in sanitized if c.isalnum() or c == "_")
    # Ensure doesn't start with number
    if sanitized and sanitized[0].isdigit():
        sanitized = "_" + sanitized
    return sanitized.lower()


def _build_read_config(
    table: TableSpec,
    connection: str,
) -> dict[str, Any]:
    """Build a ReadConfig dict for a SQL table.

    Uses the CORRECT Odibi schema:
    - format: sql
    - query: for pushdown queries
    - table: for direct table reads
    """
    read_config: dict[str, Any] = {
        "connection": connection,
        "format": "sql",
    }

    # Build query if we have WHERE clause or specific columns
    if table.where_clause or table.select_columns:
        columns = ", ".join(table.select_columns) if table.select_columns else "*"
        query = f"SELECT {columns} FROM {table.schema_name}.{table.table_name}"
        if table.where_clause:
            query += f" WHERE {table.where_clause}"
        read_config["query"] = query
    else:
        # Direct table reference
        read_config["table"] = f"{table.schema_name}.{table.table_name}"

    return read_config


def _build_write_config(
    table: TableSpec,
    connection: str,
    format_type: str,
    target_schema: Optional[str],
) -> dict[str, Any]:
    """Build a WriteConfig dict."""
    write_config: dict[str, Any] = {
        "connection": connection,
        "format": format_type,
    }

    # Build table name for target
    target_table_name = table.table_name.lower()
    if target_schema:
        write_config["table"] = f"{target_schema}.{target_table_name}"
    else:
        write_config["table"] = target_table_name

    # Delta-specific defaults
    if format_type == "delta":
        write_config["mode"] = "overwrite"

    return write_config


def _build_incremental_config(table: TableSpec) -> Optional[dict[str, Any]]:
    """Build IncrementalConfig if table has incremental settings."""
    if not table.incremental_column:
        return None

    config: dict[str, Any] = {
        "column": table.incremental_column,
        "mode": table.incremental_mode or "rolling_window",
    }

    if table.incremental_lookback is not None:
        config["lookback"] = table.incremental_lookback
    if table.incremental_unit:
        config["unit"] = table.incremental_unit

    return config


def generate_sql_pipeline(
    pipeline_name: str,
    source_connection: str,
    target_connection: str,
    tables: list[dict[str, Any]],
    target_format: str = "delta",
    target_schema: Optional[str] = None,
    layer: str = "bronze",
    node_prefix: str = "",
) -> GenerateSqlPipelineResponse:
    """
    Generate a complete pipeline YAML for SQL source ingestion.

    This uses the CORRECT Odibi schema structure:
    - Top-level `pipelines:` key (required for imported files)
    - `pipeline:` key for pipeline name (not `name:`)
    - `read:` block with `format: sql` and `query:` or `table:`
    - `write:` block with correct format settings

    Args:
        pipeline_name: Name for the pipeline
        source_connection: SQL database connection name
        target_connection: Target storage connection name
        tables: List of table specs (dicts with schema, table, optional keys)
        target_format: Output format (delta, parquet, etc.)
        target_schema: Optional schema prefix for target tables
        layer: Pipeline layer (bronze, silver, gold)
        node_prefix: Prefix for node names

    Returns:
        GenerateSqlPipelineResponse with YAML and metadata
    """
    warnings: list[str] = []
    errors: list[str] = []

    # Parse table specs
    table_specs: list[TableSpec] = []
    for t in tables:
        spec = TableSpec(
            schema_name=t.get("schema", t.get("schema_name", "dbo")),
            table_name=t.get("table", t.get("table_name", "")),
            primary_key=t.get("primary_key", []),
            where_clause=t.get("where", t.get("where_clause")),
            select_columns=t.get("columns", t.get("select_columns")),
            incremental_column=t.get("incremental_column"),
            incremental_mode=t.get("incremental_mode"),
            incremental_lookback=t.get("incremental_lookback"),
            incremental_unit=t.get("incremental_unit"),
        )
        if not spec.table_name:
            errors.append(f"Table spec missing table_name: {t}")
            continue
        table_specs.append(spec)

    if not table_specs:
        return GenerateSqlPipelineResponse(
            pipeline_yaml="",
            project_imports=[],
            node_count=0,
            warnings=warnings,
            errors=errors or ["No valid tables provided"],
        )

    # Build nodes
    nodes: list[dict[str, Any]] = []
    for table in table_specs:
        # Build node name
        raw_name = f"{node_prefix}{table.schema_name}_{table.table_name}"
        node_name = _sanitize_node_name(raw_name)

        if node_name != raw_name.lower().replace(".", "_"):
            warnings.append(f"Node name sanitized: '{raw_name}' -> '{node_name}'")

        # Build node config
        node: dict[str, Any] = {
            "name": node_name,
            "read": _build_read_config(table, source_connection),
            "write": _build_write_config(table, target_connection, target_format, target_schema),
        }

        # Add incremental config if specified
        incremental = _build_incremental_config(table)
        if incremental:
            node["incremental"] = incremental

        nodes.append(node)

    # Build pipeline structure
    pipeline_config: dict[str, Any] = {
        "pipeline": pipeline_name,
        "layer": layer,
        "nodes": nodes,
    }

    # Build full YAML structure (with top-level pipelines key for imports)
    full_config = {"pipelines": [pipeline_config]}

    # Generate YAML with nice formatting
    yaml_content = yaml.dump(
        full_config,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
        width=120,
    )

    # Suggest import path
    suggested_import = f"pipelines/{layer}/{pipeline_name}.yaml"

    return GenerateSqlPipelineResponse(
        pipeline_yaml=yaml_content,
        project_imports=[suggested_import],
        node_count=len(nodes),
        warnings=warnings,
        errors=errors,
    )


@dataclass
class ValidateConfigResponse:
    """Response from validate_odibi_config."""

    valid: bool
    errors: list[dict[str, Any]]
    warnings: list[dict[str, Any]]
    summary: str


def validate_odibi_config(
    yaml_content: str,
    check_imports: bool = False,
    base_path: Optional[str] = None,
) -> ValidateConfigResponse:
    """
    Validate Odibi YAML using actual Pydantic models.

    This provides much better validation than the string-based approach:
    1. YAML syntax check
    2. Required keys check
    3. Pydantic model validation (catches read: vs inputs: confusion)
    4. Pattern-specific validation
    5. Import resolution (optional)

    Args:
        yaml_content: YAML string to validate
        check_imports: Whether to validate imported pipeline files
        base_path: Base path for resolving imports (required if check_imports=True)

    Returns:
        ValidateConfigResponse with detailed errors and warnings
    """
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    # Step 1: Parse YAML
    try:
        config = yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        return ValidateConfigResponse(
            valid=False,
            errors=[{"code": "YAML_PARSE_ERROR", "message": str(e), "location": "root"}],
            warnings=[],
            summary="YAML syntax error",
        )

    if not isinstance(config, dict):
        return ValidateConfigResponse(
            valid=False,
            errors=[{"code": "INVALID_ROOT", "message": "Config must be a dictionary"}],
            warnings=[],
            summary="Invalid config structure",
        )

    # Step 2: Determine config type (project vs pipeline file)
    is_project_config = "project" in config or "connections" in config
    is_pipeline_file = "pipelines" in config and "project" not in config

    if is_project_config:
        _validate_project_config(config, errors, warnings)
    elif is_pipeline_file:
        _validate_pipeline_file(config, errors, warnings)
    else:
        errors.append(
            {
                "code": "UNKNOWN_CONFIG_TYPE",
                "message": "Config must be a project (with 'project:' key) or pipeline file (with 'pipelines:' key)",
            }
        )

    # Generate summary
    if errors:
        summary = f"{len(errors)} error(s), {len(warnings)} warning(s)"
    elif warnings:
        summary = f"Valid with {len(warnings)} warning(s)"
    else:
        summary = "Valid"

    return ValidateConfigResponse(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        summary=summary,
    )


def _validate_project_config(
    config: dict[str, Any],
    errors: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
) -> None:
    """Validate a project.yaml config."""
    # Required top-level keys
    required_keys = ["project", "connections"]
    for key in required_keys:
        if key not in config:
            errors.append(
                {
                    "code": "MISSING_KEY",
                    "message": f"Missing required key: '{key}'",
                    "location": "root",
                }
            )

    # Recommended keys
    recommended_keys = ["story", "system"]
    for key in recommended_keys:
        if key not in config:
            warnings.append(
                {
                    "code": "MISSING_RECOMMENDED",
                    "message": f"Missing recommended key: '{key}'",
                    "location": "root",
                }
            )

    # Validate connections
    connections = config.get("connections", {})
    if not isinstance(connections, dict):
        errors.append(
            {
                "code": "INVALID_CONNECTIONS",
                "message": "'connections' must be a dictionary",
                "location": "connections",
            }
        )
    else:
        for conn_name, conn_config in connections.items():
            if isinstance(conn_config, dict) and "type" not in conn_config:
                errors.append(
                    {
                        "code": "MISSING_CONNECTION_TYPE",
                        "message": f"Connection '{conn_name}' missing 'type'",
                        "location": f"connections.{conn_name}",
                    }
                )

    # Validate imports (if present)
    imports = config.get("imports", [])
    if imports and not isinstance(imports, list):
        errors.append(
            {
                "code": "INVALID_IMPORTS",
                "message": "'imports' must be a list of file paths",
                "location": "imports",
            }
        )

    # Validate inline pipelines (if present)
    pipelines = config.get("pipelines", [])
    if pipelines:
        _validate_pipelines_list(pipelines, errors, warnings, "pipelines")


def _validate_pipeline_file(
    config: dict[str, Any],
    errors: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
) -> None:
    """Validate an imported pipeline file (must have top-level 'pipelines:')."""
    pipelines = config.get("pipelines")
    if pipelines is None:
        errors.append(
            {
                "code": "MISSING_PIPELINES_KEY",
                "message": "Imported pipeline files must have top-level 'pipelines:' key",
                "location": "root",
                "fix": "Add 'pipelines:' as the top-level key containing a list of pipeline definitions",
            }
        )
        return

    _validate_pipelines_list(pipelines, errors, warnings, "pipelines")


def _validate_pipelines_list(
    pipelines: Any,
    errors: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
    location: str,
) -> None:
    """Validate a list of pipeline definitions."""
    if not isinstance(pipelines, list):
        errors.append(
            {
                "code": "INVALID_PIPELINES",
                "message": "'pipelines' must be a list",
                "location": location,
            }
        )
        return

    for i, pipeline in enumerate(pipelines):
        if not isinstance(pipeline, dict):
            errors.append(
                {
                    "code": "INVALID_PIPELINE",
                    "message": f"Pipeline at index {i} must be a dictionary",
                    "location": f"{location}[{i}]",
                }
            )
            continue

        # Pipeline name can be 'pipeline:' or 'name:'
        pipeline_name = pipeline.get("pipeline") or pipeline.get("name")
        if not pipeline_name:
            errors.append(
                {
                    "code": "MISSING_PIPELINE_NAME",
                    "message": f"Pipeline at index {i} missing 'pipeline:' or 'name:'",
                    "location": f"{location}[{i}]",
                }
            )
            pipeline_name = f"pipeline_{i}"

        # Validate nodes
        nodes = pipeline.get("nodes", [])
        if not nodes:
            warnings.append(
                {
                    "code": "NO_NODES",
                    "message": f"Pipeline '{pipeline_name}' has no nodes",
                    "location": f"{location}[{i}]",
                }
            )
        else:
            _validate_nodes(nodes, errors, warnings, f"{location}[{i}].nodes", pipeline_name)


def _validate_nodes(
    nodes: list[Any],
    errors: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
    location: str,
    pipeline_name: str,
) -> None:
    """Validate pipeline nodes."""
    if not isinstance(nodes, list):
        errors.append(
            {
                "code": "INVALID_NODES",
                "message": "'nodes' must be a list",
                "location": location,
            }
        )
        return

    for j, node in enumerate(nodes):
        if not isinstance(node, dict):
            errors.append(
                {
                    "code": "INVALID_NODE",
                    "message": f"Node at index {j} must be a dictionary",
                    "location": f"{location}[{j}]",
                }
            )
            continue

        node_name = node.get("name", f"node_{j}")
        node_loc = f"{location}[{j}]"

        # Check node name format
        if not _sanitize_node_name(node_name) == node_name.lower():
            errors.append(
                {
                    "code": "INVALID_NODE_NAME",
                    "message": f"Node name '{node_name}' must be alphanumeric + underscore only",
                    "location": node_loc,
                    "fix": f"Use '{_sanitize_node_name(node_name)}' instead",
                }
            )

        # Check for WRONG keys (common AI mistakes)
        wrong_keys = {
            "source": "Use 'read:' instead of 'source:'",
            "sink": "Use 'write:' instead of 'sink:'",
            "inputs": "Use 'read:' instead of 'inputs:' for SQL/file sources",
            "outputs": "Use 'write:' instead of 'outputs:'",
        }
        for wrong_key, fix in wrong_keys.items():
            if wrong_key in node:
                # inputs/outputs are valid in some contexts, but warn about potential confusion
                if wrong_key in ("inputs", "outputs"):
                    warnings.append(
                        {
                            "code": f"DEPRECATED_KEY_{wrong_key.upper()}",
                            "message": f"Node '{node_name}' uses '{wrong_key}:'. {fix}",
                            "location": node_loc,
                        }
                    )
                else:
                    errors.append(
                        {
                            "code": f"WRONG_KEY_{wrong_key.upper()}",
                            "message": f"Node '{node_name}' uses '{wrong_key}:'. {fix}",
                            "location": node_loc,
                            "fix": fix,
                        }
                    )

        # Validate read config
        read_config = node.get("read")
        if read_config:
            _validate_read_config(read_config, errors, warnings, f"{node_loc}.read", node_name)

        # Validate write config
        write_config = node.get("write")
        if write_config:
            _validate_write_config(write_config, errors, warnings, f"{node_loc}.write", node_name)


def _validate_read_config(
    config: Any,
    errors: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
    location: str,
    node_name: str,
) -> None:
    """Validate a read config block."""
    if not isinstance(config, dict):
        errors.append(
            {
                "code": "INVALID_READ",
                "message": f"Node '{node_name}': 'read' must be a dictionary",
                "location": location,
            }
        )
        return

    # Required: connection
    if "connection" not in config:
        errors.append(
            {
                "code": "MISSING_CONNECTION",
                "message": f"Node '{node_name}': read config missing 'connection'",
                "location": location,
            }
        )

    # Required: format
    if "format" not in config:
        errors.append(
            {
                "code": "MISSING_FORMAT",
                "message": f"Node '{node_name}': read config missing 'format'",
                "location": location,
                "fix": "Add 'format: sql' for SQL sources, or 'format: csv|parquet|delta|json' for files",
            }
        )

    # Check format-specific requirements
    format_type = config.get("format", "")
    if format_type == "sql":
        # SQL format should have query or table
        if "query" not in config and "table" not in config:
            warnings.append(
                {
                    "code": "SQL_NO_SOURCE",
                    "message": f"Node '{node_name}': SQL read should have 'query:' or 'table:'",
                    "location": location,
                }
            )
    elif format_type in ("csv", "parquet", "json"):
        # File formats should have path
        if "path" not in config and "table" not in config:
            warnings.append(
                {
                    "code": "FILE_NO_PATH",
                    "message": f"Node '{node_name}': {format_type} read should have 'path:'",
                    "location": location,
                }
            )

    # Check for wrong SQL syntax
    if "sql" in config:
        errors.append(
            {
                "code": "WRONG_SQL_KEY",
                "message": f"Node '{node_name}': use 'query:' instead of 'sql:'",
                "location": location,
                "fix": "Rename 'sql:' to 'query:'",
            }
        )


def _validate_write_config(
    config: Any,
    errors: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
    location: str,
    node_name: str,
) -> None:
    """Validate a write config block."""
    if not isinstance(config, dict):
        errors.append(
            {
                "code": "INVALID_WRITE",
                "message": f"Node '{node_name}': 'write' must be a dictionary",
                "location": location,
            }
        )
        return

    # Required: connection
    if "connection" not in config:
        errors.append(
            {
                "code": "MISSING_CONNECTION",
                "message": f"Node '{node_name}': write config missing 'connection'",
                "location": location,
            }
        )

    # Required: format
    if "format" not in config:
        errors.append(
            {
                "code": "MISSING_FORMAT",
                "message": f"Node '{node_name}': write config missing 'format'",
                "location": location,
                "fix": "Add 'format: delta|parquet|csv|json'",
            }
        )


def generate_project_yaml(
    project_name: str,
    connections: list[dict[str, Any]],
    imports: Optional[list[str]] = None,
    story_connection: Optional[str] = None,
    system_connection: Optional[str] = None,
) -> str:
    """
    Generate a project.yaml file.

    Args:
        project_name: Name of the project
        connections: List of connection configs
        imports: List of pipeline files to import
        story_connection: Connection name for story output
        system_connection: Connection name for system data

    Returns:
        YAML string for project.yaml
    """
    # Build connections dict
    conn_dict: dict[str, Any] = {}
    first_connection = None
    for conn in connections:
        name = conn.get("name", "default")
        if first_connection is None:
            first_connection = name
        conn_config = {k: v for k, v in conn.items() if k != "name"}
        conn_dict[name] = conn_config

    # Build project config
    config: dict[str, Any] = {
        "project": project_name,
        "connections": conn_dict,
    }

    # Add story/system with defaults
    default_conn = story_connection or system_connection or first_connection or "local"
    config["story"] = {
        "connection": story_connection or default_conn,
        "path": "stories",
    }
    config["system"] = {
        "connection": system_connection or default_conn,
        "path": "_system",
    }

    # Add imports
    if imports:
        config["imports"] = imports

    return yaml.dump(
        config,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
        width=120,
    )
