"""Phase 1 MCP Construction Tools - Typed Pipeline Building.

This module provides typed, Pydantic-backed tools for building valid Odibi pipeline configurations.
Agents call typed functions; YAML is only generated at the end after full validation.

Design Principles:
1. Pydantic models are the source of truth
2. Agents select from lists, never invent
3. Fail fast with actionable errors
4. Round-trip validation (re-parse generated YAML)
5. Progressive disclosure (list → describe → apply)
6. Cheap model friendly (enum constraints, no prose)
"""

from typing import Any, Dict, List, Optional
import sys
from pathlib import Path

# Ensure odibi is importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from odibi.registry import FunctionRegistry
from odibi.transformers import register_standard_library
from odibi.patterns import _PATTERNS
from odibi.config import (
    PipelineConfig,
    WriteMode,
)

# Ensure transformers are registered
register_standard_library()


def list_transformers(
    category: Optional[str] = None, search: Optional[str] = None
) -> Dict[str, Any]:
    """List all registered transformers with parameter schemas.

    This tool exposes the FunctionRegistry so agents discover available transformers
    instead of hallucinating function names.

    Args:
        category: Optional filter (e.g., 'scd', 'merge', 'column', 'filter', 'aggregate')
        search: Optional search term to filter by name or description

    Returns:
        {
            "transformers": [
                {
                    "name": str,
                    "description": str,
                    "category": str,
                    "parameters": {
                        "param_name": {
                            "type": str,
                            "required": bool,
                            "default": Any,
                            "description": str
                        }
                    },
                    "example_yaml": str
                }
            ],
            "count": int,
            "categories": {"category_name": int}
        }
    """
    functions = FunctionRegistry.list_functions()
    result = []
    category_counts = {}

    for name in sorted(functions):
        info = FunctionRegistry.get_function_info(name)
        param_model = FunctionRegistry.get_param_model(name)

        # Derive category from module path
        func = FunctionRegistry.get(name)
        module = getattr(func, "__module__", "")
        cat = module.split(".")[-1] if "transformers" in module else "custom"

        # Track category counts
        category_counts[cat] = category_counts.get(cat, 0) + 1

        # Apply filters
        if category and category != "all" and cat != category:
            continue

        if (
            search
            and search.lower() not in name.lower()
            and search.lower() not in info.get("docstring", "").lower()
        ):
            continue

        # Build parameter schema from Pydantic model if available
        params_schema = {}
        if param_model:
            for field_name, field_info in param_model.model_fields.items():
                type_str = str(field_info.annotation)
                # Clean up type string (remove <class '...'> wrapper)
                if "'" in type_str:
                    type_str = type_str.split("'")[1] if "'" in type_str else type_str

                params_schema[field_name] = {
                    "type": type_str,
                    "required": field_info.is_required(),
                    "default": str(field_info.default)
                    if not field_info.is_required() and field_info.default is not None
                    else None,
                    "description": field_info.description or "",
                }

        # Generate example YAML
        example_params = []
        for param_name, param_info in params_schema.items():
            if param_info["required"]:
                example_value = "[your_value]"
                if "list" in param_info["type"].lower():
                    example_value = "[col1, col2]"
                elif "bool" in param_info["type"].lower():
                    example_value = "true"
                example_params.append(f"    {param_name}: {example_value}")

        example_yaml = f"- function: {name}"
        if example_params:
            example_yaml += "\n  params:\n" + "\n".join(example_params)

        result.append(
            {
                "name": name,
                "description": info.get("docstring", "")[:200],  # Truncate for readability
                "category": cat,
                "parameters": params_schema,
                "example_yaml": example_yaml,
            }
        )

    return {"transformers": result, "count": len(result), "categories": category_counts}


def list_patterns() -> Dict[str, Any]:
    """List the 6 warehouse patterns with their required parameters.

    Returns pattern metadata so agents know exactly what each pattern needs.

    Returns:
        {
            "patterns": [
                {
                    "name": str,
                    "description": str,
                    "use_when": str,
                    "required_params": [str],
                    "optional_params": [str],
                    "example_call": {
                        "tool": "apply_pattern_template",
                        "params": {...}
                    }
                }
            ]
        }
    """
    result = []

    for name, cls in _PATTERNS.items():
        # Extract docstring
        description = (cls.__doc__ or "").strip().split("\n")[0]  # First line

        # Build example call
        example_params = {
            "pattern": name,
            "pipeline_name": f"{name}_pipeline",
            "source_connection": "my_connection",
            "target_connection": "local",
            "target_path": f"gold/{name}_output",
        }

        # Add pattern-specific example params
        if name == "dimension":
            example_params.update(
                {
                    "source_table": "dbo.DimCustomer",
                    "keys": ["customer_id"],
                }
            )
        elif name == "scd2":
            example_params.update(
                {
                    "source_table": "dbo.Employee",
                    "keys": ["employee_id"],
                    "tracked_columns": ["name", "email", "department"],
                }
            )
        elif name == "fact":
            example_params.update(
                {
                    "source_table": "dbo.FactOrders",
                    "keys": ["order_id"],
                }
            )
        elif name == "date_dimension":
            example_params.update(
                {
                    "start_date": "2020-01-01",
                    "end_date": "2030-12-31",
                }
            )
        elif name == "aggregation":
            example_params.update(
                {
                    "source_table": "silver.fact_orders",
                    "grain": ["customer_id", "month"],
                    "measures": ["SUM(total_amount) as total_revenue"],
                }
            )

        result.append(
            {
                "name": name,
                "description": description,
                "use_when": getattr(cls, "use_when", "") or f"Building {name} tables",
                "required_params": getattr(cls, "required_params", []),
                "optional_params": getattr(cls, "optional_params", []),
                "example_call": {"tool": "apply_pattern_template", "params": example_params},
            }
        )

    return {"patterns": result, "count": len(result)}


def apply_pattern_template(
    pattern: str,
    pipeline_name: str,
    source_connection: str,
    target_connection: str,
    target_path: str,
    source_table: Optional[str] = None,
    source_query: Optional[str] = None,
    source_format: str = "sql",
    target_format: str = "delta",
    layer: str = "gold",
    keys: Optional[List[str]] = None,
    tracked_columns: Optional[List[str]] = None,
    # Date dimension specific
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    # Aggregation specific
    grain: Optional[List[str]] = None,
    measures: Optional[List[str]] = None,
    # SCD2 specific (uses keys + tracked_columns above)
    # Dimension specific
    natural_key: Optional[str] = None,
    surrogate_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a complete pipeline YAML from a pattern and typed parameters.

    This is the core Phase 1 tool: one call generates a validated pipeline.

    Args:
        pattern: Pattern name (dimension, fact, scd2, merge, aggregation, date_dimension)
        pipeline_name: Name for the pipeline (alphanumeric + underscore)
        source_connection: Connection name for reading data
        target_connection: Connection name for writing data
        target_path: Output path or table name
        source_table: Table name (e.g., 'dbo.DimCustomer')
        source_query: Optional SQL query (overrides source_table)
        source_format: Source format (sql, csv, parquet, json, delta)
        target_format: Output format (delta, parquet, csv, json)
        layer: Pipeline layer (bronze, silver, gold)
        keys: Business key columns
        tracked_columns: Columns to track for changes (SCD2)
        start_date: Start date for date dimension (YYYY-MM-DD)
        end_date: End date for date dimension (YYYY-MM-DD)
        grain: Aggregation grain columns
        measures: Aggregation measure expressions
        natural_key: Natural key for dimension pattern
        surrogate_key: Surrogate key column name for dimension pattern

    Returns:
        {
            "yaml": str,  # The generated pipeline YAML
            "valid": bool,
            "errors": [...],
            "warnings": [...],
        }
    """
    errors = []
    warnings = []

    # Validate pattern exists
    if pattern not in _PATTERNS:
        return {
            "yaml": "",
            "valid": False,
            "errors": [
                {
                    "code": "UNKNOWN_PATTERN",
                    "message": f"Pattern '{pattern}' not found",
                    "allowed_values": list(_PATTERNS.keys()),
                    "fix": f"Use one of: {', '.join(_PATTERNS.keys())}",
                }
            ],
            "warnings": [],
        }

    # Validate pipeline name (alphanumeric + underscore)
    import re

    if not re.match(r"^[a-zA-Z0-9_]+$", pipeline_name):
        errors.append(
            {
                "code": "INVALID_NAME",
                "field_path": "pipeline_name",
                "message": f"Pipeline name '{pipeline_name}' is invalid",
                "fix": "Use only alphanumeric characters and underscores",
            }
        )

    # Build node params based on pattern
    node_params = {}

    if pattern == "dimension":
        if not natural_key or not surrogate_key:
            errors.append(
                {
                    "code": "PATTERN_REQUIRES",
                    "field_path": "params",
                    "message": "Dimension pattern requires 'natural_key' and 'surrogate_key'",
                    "fix": "Provide natural_key and surrogate_key parameters",
                }
            )
        else:
            node_params = {
                "natural_key": natural_key,
                "surrogate_key": surrogate_key,
                "target": target_path,
            }
            if tracked_columns:
                node_params["track_cols"] = tracked_columns

    elif pattern == "scd2":
        if not keys:
            errors.append(
                {
                    "code": "PATTERN_REQUIRES",
                    "field_path": "keys",
                    "message": "SCD2 pattern requires 'keys'",
                    "fix": "Provide keys parameter (business key columns)",
                }
            )
        if not tracked_columns:
            errors.append(
                {
                    "code": "PATTERN_REQUIRES",
                    "field_path": "tracked_columns",
                    "message": "SCD2 pattern requires 'tracked_columns'",
                    "fix": "Provide tracked_columns parameter",
                }
            )
        if keys and tracked_columns:
            node_params = {
                "keys": keys,
                "target": target_path,
                "tracked_columns": tracked_columns,
            }

    elif pattern == "fact":
        if keys:
            node_params["keys"] = keys

    elif pattern == "date_dimension":
        if not start_date or not end_date:
            errors.append(
                {
                    "code": "PATTERN_REQUIRES",
                    "field_path": "params",
                    "message": "Date dimension pattern requires 'start_date' and 'end_date'",
                    "fix": "Provide start_date and end_date in YYYY-MM-DD format",
                }
            )
        else:
            node_params = {
                "start_date": start_date,
                "end_date": end_date,
            }

    elif pattern == "aggregation":
        if not grain or not measures:
            errors.append(
                {
                    "code": "PATTERN_REQUIRES",
                    "field_path": "params",
                    "message": "Aggregation pattern requires 'grain' and 'measures'",
                    "fix": "Provide grain (list of group-by columns) and measures (list of aggregation expressions)",
                }
            )
        else:
            node_params = {
                "grain": grain,
                "measures": measures,
            }

    # Return early if validation failed
    if errors:
        return {"yaml": "", "valid": False, "errors": errors, "warnings": warnings}

    # Build Pydantic models
    try:
        # Build read config
        read_dict = {
            "connection": source_connection,
            "format": source_format,
        }

        if source_query:
            read_dict["query"] = source_query
        elif source_table:
            if source_format == "sql":
                read_dict["table"] = source_table
            else:
                read_dict["path"] = source_table
        else:
            # Date dimension doesn't need a source
            if pattern != "date_dimension":
                errors.append(
                    {
                        "code": "MISSING_SOURCE",
                        "message": "Either source_table or source_query is required",
                        "fix": "Provide source_table or source_query parameter",
                    }
                )
                return {"yaml": "", "valid": False, "errors": errors, "warnings": warnings}

        # Build write config
        write_mode = WriteMode.OVERWRITE
        write_options = {}

        # Pattern-specific write modes
        if pattern == "dimension":
            write_mode = WriteMode.OVERWRITE
        elif pattern == "fact":
            write_mode = WriteMode.APPEND
            if keys:
                # Use upsert mode with keys
                write_mode = WriteMode.UPSERT
                write_options["keys"] = keys
        elif pattern == "scd2":
            write_mode = WriteMode.APPEND
        elif pattern == "merge":
            write_mode = WriteMode.MERGE
        elif pattern == "aggregation":
            write_mode = WriteMode.OVERWRITE
        elif pattern == "date_dimension":
            write_mode = WriteMode.OVERWRITE

        write_dict = {
            "connection": target_connection,
            "format": target_format,
            "path": target_path,
            "mode": write_mode,
        }

        if write_options:
            write_dict["options"] = write_options

        # Build node config
        node_dict = {
            "name": f"{pattern}_node",
        }

        if pattern != "date_dimension":
            node_dict["read"] = read_dict

        node_dict["write"] = write_dict

        # Add pattern-specific config
        if node_params:
            node_dict["transformer"] = pattern
            node_dict["params"] = node_params

        # Build pipeline config
        pipeline_dict = {"pipeline": pipeline_name, "layer": layer, "nodes": [node_dict]}

        # Validate through Pydantic
        pipeline_config = PipelineConfig(**pipeline_dict)

        # Serialize to YAML
        import yaml

        yaml_content = yaml.safe_dump(
            {"pipelines": [pipeline_config.model_dump(mode="json", exclude_none=True)]},
            default_flow_style=False,
            sort_keys=False,
        )

        # Round-trip validation: re-parse the YAML
        try:
            reparsed = yaml.safe_load(yaml_content)
            PipelineConfig(**reparsed["pipelines"][0])
        except Exception as e:
            errors.append(
                {
                    "code": "ROUND_TRIP_FAILED",
                    "message": f"Generated YAML failed round-trip validation: {str(e)}",
                    "fix": "This is a bug in the tool - please report it",
                }
            )
            return {"yaml": "", "valid": False, "errors": errors, "warnings": warnings}

        return {
            "yaml": yaml_content,
            "valid": True,
            "errors": [],
            "warnings": warnings,
            "next_step": "Add this to your project.yaml file under the 'pipelines:' section",
        }

    except Exception as e:
        errors.append(
            {
                "code": "BUILD_FAILED",
                "message": f"Failed to build pipeline config: {str(e)}",
                "fix": "Check that all required parameters are provided correctly",
            }
        )
        return {"yaml": "", "valid": False, "errors": errors, "warnings": warnings}
