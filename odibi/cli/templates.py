"""CLI commands for template generation and introspection.

Commands:
    odibi templates list              List all available template types
    odibi templates show <name>       Show YAML template for a type
    odibi templates transformer <name> Show transformer documentation
    odibi templates schema            Generate JSON schema for VS Code
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from argparse import Namespace


def templates_list_command(args: Namespace) -> int:
    """List all available template types."""
    from odibi.tools.templates import list_templates

    templates = list_templates()

    if args.format == "json":
        print(json.dumps(templates, indent=2))
        return 0

    if args.category:
        category = args.category.lower()
        if category not in templates:
            print(f"Unknown category: '{category}'")
            print(f"Available: {', '.join(templates.keys())}")
            return 1
        print(f"Available {category} templates:")
        print("=" * 40)
        for name in sorted(templates[category]):
            print(f"  {name}")
        return 0

    print("Available Templates")
    print("=" * 40)
    for category, names in templates.items():
        print(f"\n{category.title()}:")
        for name in sorted(names):
            print(f"  {name}")

    print("\nUsage: odibi templates show <name>")
    return 0


def templates_show_command(args: Namespace) -> int:
    """Show YAML template for a type."""
    from odibi.tools.templates import show_template

    try:
        template = show_template(
            args.name,
            show_optional=not args.required_only,
            show_comments=not args.no_comments,
        )
        print(template)
        return 0
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def templates_transformer_command(args: Namespace) -> int:
    """Show transformer documentation."""
    from odibi.tools.templates import show_transformer

    try:
        doc = show_transformer(args.name, show_example=not args.no_example)
        print(doc)
        return 0
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def templates_schema_command(args: Namespace) -> int:
    """Generate JSON schema for VS Code autocomplete."""
    from odibi.tools.templates import generate_json_schema

    output_path = args.output or "odibi.schema.json"

    try:
        schema = generate_json_schema(
            output_path=output_path,
            include_transformers=not args.no_transformers,
        )
        print(f"JSON Schema written to: {output_path}")
        if args.verbose:
            print(f"Definitions: {len(schema.get('$defs', {}))}")
        return 0
    except Exception as e:
        print(f"Error generating schema: {e}", file=sys.stderr)
        return 1


def add_templates_parser(subparsers: argparse._SubParsersAction) -> None:
    """Add templates subcommand parsers."""
    templates_parser = subparsers.add_parser(
        "templates",
        help="Generate YAML templates and JSON schemas from Pydantic models",
        description=(
            "Generate configuration templates and documentation directly from "
            "the source Pydantic models. Templates are always in sync with the code."
        ),
    )
    templates_subparsers = templates_parser.add_subparsers(dest="templates_command")

    list_parser = templates_subparsers.add_parser(
        "list",
        help="List all available template types",
    )
    list_parser.add_argument(
        "--category",
        "-c",
        choices=["connections", "patterns", "configs"],
        help="Filter by category",
    )
    list_parser.add_argument(
        "--format",
        "-f",
        choices=["table", "json"],
        default="table",
        help="Output format",
    )

    show_parser = templates_subparsers.add_parser(
        "show",
        help="Show YAML template for a connection, pattern, or config type",
    )
    show_parser.add_argument(
        "name",
        help="Template name (e.g., azure_blob, dimension, node)",
    )
    show_parser.add_argument(
        "--required-only",
        "-r",
        action="store_true",
        help="Only show required fields",
    )
    show_parser.add_argument(
        "--no-comments",
        action="store_true",
        help="Omit description comments",
    )

    transformer_parser = templates_subparsers.add_parser(
        "transformer",
        help="Show documentation for a transformer",
    )
    transformer_parser.add_argument(
        "name",
        help="Transformer name (e.g., select, filter, scd2)",
    )
    transformer_parser.add_argument(
        "--no-example",
        action="store_true",
        help="Omit YAML example",
    )

    schema_parser = templates_subparsers.add_parser(
        "schema",
        help="Generate JSON schema for VS Code autocomplete",
    )
    schema_parser.add_argument(
        "--output",
        "-o",
        help="Output file path (default: odibi.schema.json)",
    )
    schema_parser.add_argument(
        "--no-transformers",
        action="store_true",
        help="Exclude transformer definitions from schema",
    )
    schema_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show additional output",
    )


def templates_command(args: Namespace) -> int:
    """Handle templates subcommands."""
    if args.templates_command == "list":
        return templates_list_command(args)
    elif args.templates_command == "show":
        return templates_show_command(args)
    elif args.templates_command == "transformer":
        return templates_transformer_command(args)
    elif args.templates_command == "schema":
        return templates_schema_command(args)
    else:
        print("Usage: odibi templates <list|show|transformer|schema>")
        print("\nExamples:")
        print("  odibi templates list")
        print("  odibi templates show azure_blob")
        print("  odibi templates transformer scd2")
        print("  odibi templates schema --output odibi.schema.json")
        return 1
