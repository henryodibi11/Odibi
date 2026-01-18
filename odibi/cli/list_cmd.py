"""CLI commands for listing available transformers, patterns, and connections."""

from __future__ import annotations

import inspect
import json
from typing import Any, Dict, List, Optional

from odibi.connections.factory import register_builtins
from odibi.patterns import _PATTERNS
from odibi.plugins import _CONNECTION_FACTORIES
from odibi.registry import FunctionRegistry
from odibi.transformers import register_standard_library


def _ensure_initialized():
    """Ensure transformers and connections are registered."""
    if not FunctionRegistry.list_functions():
        register_standard_library()
    if not _CONNECTION_FACTORIES:
        register_builtins()


def list_transformers_command(args) -> int:
    """List all registered transformers."""
    _ensure_initialized()

    transformers = FunctionRegistry.list_functions()
    transformers.sort()

    if args.format == "json":
        result = []
        for name in transformers:
            info = FunctionRegistry.get_function_info(name)
            params = []
            for pname, pinfo in info.get("parameters", {}).items():
                if pname == "current":
                    continue
                params.append(
                    {
                        "name": pname,
                        "required": pinfo.get("required", False),
                        "default": _serialize_default(pinfo.get("default")),
                    }
                )
            result.append(
                {
                    "name": name,
                    "parameters": params,
                    "docstring": _get_first_line(info.get("docstring")),
                }
            )
        print(json.dumps(result, indent=2))
    else:
        print(f"Available Transformers ({len(transformers)}):")
        print("=" * 60)
        for name in transformers:
            info = FunctionRegistry.get_function_info(name)
            doc = _get_first_line(info.get("docstring")) or "No description"
            print(f"  {name:<30} {doc}")

    return 0


def list_patterns_command(args) -> int:
    """List all registered patterns."""
    patterns = list(_PATTERNS.keys())
    patterns.sort()

    if args.format == "json":
        result = []
        for name in patterns:
            pattern_cls = _PATTERNS[name]
            doc = inspect.getdoc(pattern_cls) or ""
            params = _extract_pattern_params(pattern_cls)
            result.append(
                {
                    "name": name,
                    "class": pattern_cls.__name__,
                    "docstring": _get_first_line(doc),
                    "parameters": params,
                }
            )
        print(json.dumps(result, indent=2))
    else:
        print(f"Available Patterns ({len(patterns)}):")
        print("=" * 60)
        for name in patterns:
            pattern_cls = _PATTERNS[name]
            doc = _get_first_line(inspect.getdoc(pattern_cls)) or "No description"
            print(f"  {name:<20} {doc}")

    return 0


def list_connections_command(args) -> int:
    """List all registered connection types."""
    _ensure_initialized()

    connections = list(_CONNECTION_FACTORIES.keys())
    connections.sort()

    connection_docs = {
        "local": "Local filesystem (CSV, Parquet, JSON, etc.)",
        "http": "HTTP/REST API endpoints",
        "azure_blob": "Azure Blob Storage",
        "azure_adls": "Azure Data Lake Storage Gen2",
        "delta": "Delta Lake tables",
        "sql_server": "Microsoft SQL Server",
        "azure_sql": "Azure SQL Database",
    }

    if args.format == "json":
        result = []
        for name in connections:
            result.append(
                {
                    "name": name,
                    "description": connection_docs.get(name, "No description"),
                }
            )
        print(json.dumps(result, indent=2))
    else:
        print(f"Available Connection Types ({len(connections)}):")
        print("=" * 60)
        for name in connections:
            doc = connection_docs.get(name, "No description")
            print(f"  {name:<20} {doc}")

    return 0


def explain_command(args) -> int:
    """Explain a transformer, pattern, or connection."""
    _ensure_initialized()

    name = args.name
    found = False

    # Check transformers
    if FunctionRegistry.has_function(name):
        found = True
        info = FunctionRegistry.get_function_info(name)
        print(f"Transformer: {name}")
        print("=" * 60)
        print()
        if info.get("docstring"):
            print(info["docstring"])
            print()
        print("Parameters:")
        for pname, pinfo in info.get("parameters", {}).items():
            if pname == "current":
                continue
            required = "required" if pinfo.get("required") else "optional"
            default = pinfo.get("default")
            default_str = f" (default: {default})" if default is not None else ""
            print(f"  - {pname}: {required}{default_str}")
        print()
        print("Example YAML:")
        print("  transforms:")
        print(f"    - function: {name}")
        print("      params:")
        for pname, pinfo in info.get("parameters", {}).items():
            if pname == "current":
                continue
            if pinfo.get("required"):
                print(f"        {pname}: <value>")

    # Check patterns
    if name in _PATTERNS:
        if found:
            print("\n" + "-" * 60 + "\n")
        found = True
        pattern_cls = _PATTERNS[name]
        doc = inspect.getdoc(pattern_cls) or "No documentation"
        print(f"Pattern: {name}")
        print("=" * 60)
        print()
        print(doc)
        print()
        print("Example YAML:")
        print("  pattern:")
        print(f"    type: {name}")
        print("    params:")
        params = _extract_pattern_params(pattern_cls)
        for param in params:
            if param.get("required"):
                print(f"      {param['name']}: <value>")

    # Check connections
    connection_docs = {
        "local": {
            "description": "Local filesystem connection for reading/writing files.",
            "params": ["path (required)", "format (csv, parquet, json, etc.)"],
        },
        "http": {
            "description": "HTTP/REST API connection for fetching data.",
            "params": ["base_url (required)", "headers (optional)", "auth (optional)"],
        },
        "azure_blob": {
            "description": "Azure Blob Storage connection.",
            "params": ["account_name (required)", "container (required)", "credential (required)"],
        },
        "azure_adls": {
            "description": "Azure Data Lake Storage Gen2 connection.",
            "params": ["account_name (required)", "container (required)", "credential (required)"],
        },
        "delta": {
            "description": "Delta Lake connection for reading/writing Delta tables.",
            "params": ["path (required)"],
        },
        "sql_server": {
            "description": "Microsoft SQL Server connection.",
            "params": [
                "server (required)",
                "database (required)",
                "auth_mode (sql/windows/managed_identity)",
            ],
        },
        "azure_sql": {
            "description": "Azure SQL Database connection (same as sql_server).",
            "params": [
                "server (required)",
                "database (required)",
                "auth_mode (sql/managed_identity)",
            ],
        },
    }

    if name in _CONNECTION_FACTORIES:
        if found:
            print("\n" + "-" * 60 + "\n")
        found = True
        conn_info = connection_docs.get(name, {"description": "No documentation", "params": []})
        print(f"Connection: {name}")
        print("=" * 60)
        print()
        print(conn_info["description"])
        print()
        print("Parameters:")
        for p in conn_info.get("params", []):
            print(f"  - {p}")
        print()
        print("Example YAML:")
        print("  connections:")
        print("    my_connection:")
        print(f"      type: {name}")
        if name == "local":
            print("      path: ./data")
        elif name in ("azure_blob", "azure_adls"):
            print("      account_name: myaccount")
            print("      container: mycontainer")
        elif name in ("sql_server", "azure_sql"):
            print("      server: myserver.database.windows.net")
            print("      database: mydb")

    if not found:
        print(f"Unknown: '{name}'")
        print()
        print("Try one of:")
        print(f"  Transformers: {', '.join(FunctionRegistry.list_functions()[:5])}...")
        print(f"  Patterns: {', '.join(_PATTERNS.keys())}")
        print(f"  Connections: {', '.join(_CONNECTION_FACTORIES.keys())}")
        return 1

    return 0


def _get_first_line(text: Optional[str]) -> Optional[str]:
    """Get first line of docstring."""
    if not text:
        return None
    lines = text.strip().split("\n")
    return lines[0].strip() if lines else None


def _serialize_default(value: Any) -> Any:
    """Serialize default value for JSON output."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, dict)):
        return value
    return str(value)


def _extract_pattern_params(pattern_cls) -> List[Dict[str, Any]]:
    """Extract parameter info from pattern class docstring."""
    doc = inspect.getdoc(pattern_cls) or ""
    params = []

    # Parse Configuration Options section from docstring
    in_config = False
    for line in doc.split("\n"):
        line = line.strip()
        if "Configuration Options" in line or "params dict" in line:
            in_config = True
            continue
        if in_config:
            if line.startswith("- **") and "**" in line[4:]:
                # Parse "- **name** (type): description"
                end = line.index("**", 4)
                name = line[4:end]
                required = "required" in line.lower()
                params.append({"name": name, "required": required})
            elif line and not line.startswith("-") and not line.startswith("*"):
                # End of params section
                if params:
                    break

    # Fallback: common params for known patterns
    if not params:
        if pattern_cls.__name__ == "DimensionPattern":
            params = [
                {"name": "natural_key", "required": True},
                {"name": "surrogate_key", "required": True},
                {"name": "scd_type", "required": False},
                {"name": "track_cols", "required": False},
            ]
        elif pattern_cls.__name__ == "FactPattern":
            params = [
                {"name": "grain", "required": True},
                {"name": "dimensions", "required": False},
                {"name": "measures", "required": False},
            ]
        elif pattern_cls.__name__ == "SCD2Pattern":
            params = [
                {"name": "natural_key", "required": True},
                {"name": "track_cols", "required": True},
                {"name": "target", "required": True},
            ]
        elif pattern_cls.__name__ == "MergePattern":
            params = [
                {"name": "merge_key", "required": True},
                {"name": "target", "required": True},
            ]
        elif pattern_cls.__name__ == "AggregationPattern":
            params = [
                {"name": "group_by", "required": True},
                {"name": "aggregations", "required": True},
            ]
        elif pattern_cls.__name__ == "DateDimensionPattern":
            params = [
                {"name": "start_date", "required": True},
                {"name": "end_date", "required": True},
            ]

    return params


def add_list_parser(subparsers):
    """Add list subcommand parsers."""
    # odibi list
    list_parser = subparsers.add_parser(
        "list", help="List available transformers, patterns, or connections"
    )
    list_subparsers = list_parser.add_subparsers(dest="list_command")

    # odibi list transformers
    trans_parser = list_subparsers.add_parser(
        "transformers", help="List all registered transformers"
    )
    trans_parser.add_argument(
        "--format",
        "-f",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table)",
    )

    # odibi list patterns
    pattern_parser = list_subparsers.add_parser("patterns", help="List all registered patterns")
    pattern_parser.add_argument(
        "--format",
        "-f",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table)",
    )

    # odibi list connections
    conn_parser = list_subparsers.add_parser(
        "connections", help="List all registered connection types"
    )
    conn_parser.add_argument(
        "--format",
        "-f",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table)",
    )


def add_explain_parser(subparsers):
    """Add explain subcommand parser."""
    explain_parser = subparsers.add_parser(
        "explain", help="Explain a transformer, pattern, or connection"
    )
    explain_parser.add_argument(
        "name", help="Name of the transformer, pattern, or connection to explain"
    )


def list_command(args) -> int:
    """Handle list subcommands."""
    if args.list_command == "transformers":
        return list_transformers_command(args)
    elif args.list_command == "patterns":
        return list_patterns_command(args)
    elif args.list_command == "connections":
        return list_connections_command(args)
    else:
        print("Usage: odibi list <transformers|patterns|connections>")
        return 1
