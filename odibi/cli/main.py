"""Command-line interface for odibi."""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

try:
    from rich.console import Console
    from rich.table import Table

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


def format_output(data: Any, format_type: str = "auto") -> str:
    """Format output for display."""
    if format_type == "json":
        return json.dumps(data, indent=2)
    return str(data)


def print_table(data: Dict[str, str], title: str = "") -> None:
    """Print a key-value table."""
    if RICH_AVAILABLE:
        console = Console()
        table = Table(title=title, show_header=True, header_style="bold magenta")
        table.add_column("Key", style="cyan")
        table.add_column("Value", style="green")
        for key, value in data.items():
            table.add_row(key, str(value))
        console.print(table)
    else:
        if title:
            print(f"\n{title}")
            print("=" * len(title))
        for key, value in data.items():
            print(f"{key:30s}: {value}")


def cmd_discover(args: argparse.Namespace) -> int:
    """Run discovery on a connection."""
    from odibi.pipeline import PipelineManager

    try:
        pm = PipelineManager.from_yaml(Path(args.config))
        result = pm.discover(
            connection_name=args.connection,
            dataset=args.dataset,
            include_profile=args.profile,
            include_schema=args.schema,
        )

        if args.format == "json":
            print(json.dumps(result, indent=2))
        else:
            print(f"\nDiscovery results for connection '{args.connection}':")
            if args.dataset:
                print(f"Dataset: {args.dataset}")
            print(f"\nFound {len(result.get('tables', []))} tables")
            print(format_output(result, args.format))

        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_scaffold_project(args: argparse.Namespace) -> int:
    """Generate project scaffold."""
    from odibi.pipeline import PipelineManager

    try:
        pm = PipelineManager()
        yaml_content = pm.scaffold_project(project_name=args.name)

        output_file = Path(f"{args.name}.yaml")
        if output_file.exists() and not args.force:
            print(
                f"Error: {output_file} already exists. Use --force to overwrite.", file=sys.stderr
            )
            return 1

        output_file.write_text(yaml_content)
        print(f"Generated project scaffold: {output_file}")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_scaffold_sql_pipeline(args: argparse.Namespace) -> int:
    """Generate SQL ingestion pipeline scaffold."""
    from odibi.pipeline import PipelineManager

    try:
        pm = PipelineManager.from_yaml(Path(args.config))
        tables = args.tables.split(",") if args.tables else []

        yaml_content = pm.scaffold_sql_pipeline(
            pipeline_name=args.name,
            source_connection=args.source,
            target_connection=args.target,
            tables=tables,
        )

        output_file = Path(f"{args.name}_pipeline.yaml")
        if output_file.exists() and not args.force:
            print(
                f"Error: {output_file} already exists. Use --force to overwrite.", file=sys.stderr
            )
            return 1

        output_file.write_text(yaml_content)
        print(f"Generated SQL pipeline scaffold: {output_file}")
        print(f"Tables: {', '.join(tables)}")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate YAML file."""
    from odibi.pipeline import PipelineManager

    try:
        yaml_file = Path(args.file)
        if not yaml_file.exists():
            print(f"Error: File not found: {yaml_file}", file=sys.stderr)
            return 1

        yaml_content = yaml_file.read_text()
        pm = PipelineManager()
        result = pm.validate_yaml(yaml_content)

        if args.format == "json":
            print(json.dumps(result, indent=2))
        else:
            if result.get("valid"):
                print(f"✓ {yaml_file} is valid")
                return 0
            else:
                print(f"✗ {yaml_file} has errors:")
                for error in result.get("errors", []):
                    print(f"  - {error.get('field_path', 'unknown')}: {error.get('message', '')}")
                return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_doctor(args: argparse.Namespace) -> int:
    """Run environment diagnostics."""
    from odibi.doctor import doctor

    try:
        result = doctor()

        if args.format == "json":
            print(json.dumps(result, indent=2))
            return 0

        print("\n=== Odibi Environment Diagnostics ===\n")
        print(f"Status: {result['status'].upper()}")
        print(f"Python: {result['python_version']}")

        print("\n--- Packages ---")
        print_table(result.get("packages", {}))

        print("\n--- Environment ---")
        env_vars = result.get("environment", {})
        print_table(env_vars, "Environment Variables")

        if result.get("project_loaded"):
            print("\n✓ Project loaded successfully")
            if result.get("connections"):
                print("\n--- Connections ---")
                print_table(result["connections"], "Connection Status")
        else:
            print("\n✗ No project loaded")

        if result.get("issues"):
            print("\n--- Issues ---")
            for issue in result["issues"]:
                severity = issue["severity"].upper()
                print(f"[{severity}] {issue['message']}")
                print(f"  Fix: {issue['fix']}")

        if result.get("suggestions"):
            print("\n--- Suggestions ---")
            for suggestion in result["suggestions"]:
                print(f"  - {suggestion}")

        return 0 if result["status"] == "healthy" else 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_doctor_path(args: argparse.Namespace) -> int:
    """Diagnose a specific path."""
    from odibi.doctor import diagnose_path

    try:
        result = diagnose_path(args.path)

        if args.format == "json":
            print(json.dumps(result, indent=2))
            return 0

        print(f"\nPath: {result['input_path']}")
        print(f"Resolved: {result.get('resolved_path', 'N/A')}")
        print(f"Exists: {result['exists']}")

        if result.get("error"):
            print(f"Error: {result['error']}")
            return 1

        if result["exists"]:
            print(f"Type: {'Directory' if result['is_directory'] else 'File'}")
            print(f"Readable: {result.get('readable', False)}")
            print(f"Writable: {result.get('writable', False)}")

            if result["is_file"]:
                print(f"Size: {result.get('size', 0)} bytes")
                print(f"Format: {result.get('format', 'unknown')}")

            if result["is_directory"] and result.get("contents"):
                print(f"\nContents ({len(result['contents'])} items):")
                for item in result["contents"][:20]:
                    item_type = "DIR" if item["is_dir"] else "FILE"
                    size = f" ({item.get('size', 0)} bytes)" if "size" in item else ""
                    print(f"  [{item_type}] {item['name']}{size}")

        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="odibi", description="Odibi - Declarative Data Engineering Framework"
    )
    parser.add_argument("--version", action="version", version="odibi 2.18.0")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    discover_parser = subparsers.add_parser("discover", help="Discover data sources")
    discover_parser.add_argument("connection", help="Connection name")
    discover_parser.add_argument("dataset", nargs="?", help="Dataset/database name")
    discover_parser.add_argument("--config", default="exploration.yaml", help="Config file")
    discover_parser.add_argument("--profile", action="store_true", help="Include profiling")
    discover_parser.add_argument("--schema", action="store_true", help="Include schema")
    discover_parser.add_argument("--format", choices=["auto", "json"], default="auto")

    scaffold_parsers = subparsers.add_parser("scaffold", help="Generate scaffolds")
    scaffold_sub = scaffold_parsers.add_subparsers(dest="scaffold_type")

    project_parser = scaffold_sub.add_parser("project", help="Generate project scaffold")
    project_parser.add_argument("name", help="Project name")
    project_parser.add_argument("--force", action="store_true", help="Overwrite existing")

    sql_parser = scaffold_sub.add_parser("sql-pipeline", help="Generate SQL pipeline scaffold")
    sql_parser.add_argument("name", help="Pipeline name")
    sql_parser.add_argument("--config", default="exploration.yaml", help="Config file")
    sql_parser.add_argument("--source", required=True, help="Source connection name")
    sql_parser.add_argument("--target", required=True, help="Target connection name")
    sql_parser.add_argument("--tables", required=True, help="Comma-separated table list")
    sql_parser.add_argument("--force", action="store_true", help="Overwrite existing")

    validate_parser = subparsers.add_parser("validate", help="Validate YAML file")
    validate_parser.add_argument("file", help="YAML file to validate")
    validate_parser.add_argument("--format", choices=["auto", "json"], default="auto")

    doctor_parser = subparsers.add_parser("doctor", help="Run environment diagnostics")
    doctor_parser.add_argument("--format", choices=["auto", "json"], default="auto")

    doctor_path_parser = subparsers.add_parser("doctor-path", help="Diagnose a path")
    doctor_path_parser.add_argument("path", help="Path to diagnose")
    doctor_path_parser.add_argument("--format", choices=["auto", "json"], default="auto")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == "discover":
        return cmd_discover(args)
    elif args.command == "scaffold":
        if args.scaffold_type == "project":
            return cmd_scaffold_project(args)
        elif args.scaffold_type == "sql-pipeline":
            return cmd_scaffold_sql_pipeline(args)
        else:
            scaffold_parsers.print_help()
            return 1
    elif args.command == "validate":
        return cmd_validate(args)
    elif args.command == "doctor":
        return cmd_doctor(args)
    elif args.command == "doctor-path":
        return cmd_doctor_path(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
