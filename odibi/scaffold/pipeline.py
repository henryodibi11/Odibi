"""Pipeline YAML scaffolding."""

from typing import Any, Dict, List, Optional

import yaml


def sanitize_node_name(name: str) -> str:
    """Sanitize name to be valid node name (alphanumeric + underscore only).

    Args:
        name: Raw node name

    Returns:
        Sanitized node name in lowercase

    Example:
        >>> sanitize_node_name("my-table.name")
        'my_table_name'
        >>> sanitize_node_name("123_table")
        '_123_table'
    """
    sanitized = name.replace("-", "_").replace(".", "_").replace(" ", "_")
    sanitized = "".join(c for c in sanitized if c.isalnum() or c == "_")
    if sanitized and sanitized[0].isdigit():
        sanitized = "_" + sanitized
    return sanitized.lower()


def generate_sql_pipeline(
    pipeline_name: str,
    source_connection: str,
    target_connection: str,
    tables: List[Dict[str, Any]],
    target_format: str = "delta",
    target_schema: Optional[str] = None,
    layer: str = "bronze",
    node_prefix: str = "",
) -> str:
    """Generate a SQL ingestion pipeline YAML.

    Args:
        pipeline_name: Name for the pipeline
        source_connection: SQL database connection name
        target_connection: Target storage connection name
        tables: List of table specs with keys:
            - schema (or schema_name): Source schema
            - table (or table_name): Table name
            - primary_key (optional): List of PK columns
            - where (or where_clause) (optional): WHERE clause
            - columns (or select_columns) (optional): List of columns (None = SELECT *)
            - incremental_column (optional): Column for incremental loading
            - incremental_mode (optional): Mode (rolling_window, append, etc.)
            - incremental_lookback (optional): Lookback value
            - incremental_unit (optional): Time unit (day, hour, minute)
        target_format: Output format (delta, parquet, etc.)
        target_schema: Optional schema prefix for target tables
        layer: Pipeline layer (bronze, silver, gold)
        node_prefix: Prefix for node names

    Returns:
        YAML string for pipeline file

    Example:
        >>> tables = [
        ...     {"schema": "dbo", "table": "customers", "primary_key": ["id"]},
        ...     {"schema": "dbo", "table": "orders"}
        ... ]
        >>> yaml = generate_sql_pipeline("ingest", "sqldb", "lake", tables)
    """
    nodes: List[Dict[str, Any]] = []

    for table in tables:
        schema_name = table.get("schema", table.get("schema_name", "dbo"))
        table_name = table.get("table", table.get("table_name"))

        if not table_name:
            continue

        raw_name = f"{node_prefix}{schema_name}_{table_name}"
        node_name = sanitize_node_name(raw_name)

        read_config: Dict[str, Any] = {
            "connection": source_connection,
            "format": "sql",
        }

        where_clause = table.get("where", table.get("where_clause"))
        select_columns = table.get("columns", table.get("select_columns"))

        if where_clause or select_columns:
            columns = ", ".join(select_columns) if select_columns else "*"
            query = f"SELECT {columns} FROM {schema_name}.{table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            read_config["query"] = query
        else:
            read_config["table"] = f"{schema_name}.{table_name}"

        write_config: Dict[str, Any] = {
            "connection": target_connection,
            "format": target_format,
        }

        target_table_name = table_name.lower()
        if target_schema:
            write_config["table"] = f"{target_schema}.{target_table_name}"
        else:
            write_config["table"] = target_table_name

        if target_format == "delta":
            write_config["mode"] = "overwrite"

        node: Dict[str, Any] = {
            "name": node_name,
            "read": read_config,
            "write": write_config,
        }

        incremental_column = table.get("incremental_column")
        if incremental_column:
            incremental_config: Dict[str, Any] = {
                "column": incremental_column,
                "mode": table.get("incremental_mode", "rolling_window"),
            }
            if "incremental_lookback" in table:
                incremental_config["lookback"] = table["incremental_lookback"]
            if "incremental_unit" in table:
                incremental_config["unit"] = table["incremental_unit"]
            node["incremental"] = incremental_config

        nodes.append(node)

    pipeline_config: Dict[str, Any] = {
        "pipeline": pipeline_name,
        "layer": layer,
        "nodes": nodes,
    }

    full_config = {"pipelines": [pipeline_config]}

    return yaml.dump(
        full_config,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
        width=120,
    )
