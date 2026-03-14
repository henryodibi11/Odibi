"""Command-line interface for odibi."""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

from odibi import __version__

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
    from odibi.validate.pipeline import validate_yaml

    try:
        yaml_file = Path(args.file)
        if not yaml_file.exists():
            print(f"Error: File not found: {yaml_file}", file=sys.stderr)
            return 1

        yaml_content = yaml_file.read_text()
        result = validate_yaml(yaml_content)

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
    parser.add_argument("--version", action="version", version=f"odibi {__version__}")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # odibi run
    run_parser = subparsers.add_parser("run", help="Execute pipeline")
    run_parser.add_argument("config", help="Path to YAML config file")
    run_parser.add_argument(
        "--env", default=None, help="Environment to apply overrides (e.g., dev, qat, prod)"
    )
    run_parser.add_argument(
        "--dry-run", action="store_true", help="Simulate execution without running operations"
    )
    run_parser.add_argument(
        "--resume", action="store_true", help="Resume from last failure (skip successful nodes)"
    )
    run_parser.add_argument(
        "--parallel", action="store_true", help="Run independent nodes in parallel"
    )
    run_parser.add_argument(
        "--workers",
        type=int,
        help="Number of worker threads for parallel execution (default: 4)",
    )
    run_parser.add_argument(
        "--on-error",
        choices=["fail_fast", "fail_later", "ignore"],
        help="Override error handling strategy",
    )
    run_parser.add_argument(
        "--tag",
        help="Filter nodes by tag (e.g., --tag daily)",
    )
    run_parser.add_argument(
        "--pipeline",
        dest="pipeline_name",
        help="Run specific pipeline by name",
    )
    run_parser.add_argument(
        "--node",
        dest="node_name",
        help="Run specific node by name",
    )

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

    # odibi init / init-pipeline / create / generate-project
    from odibi.cli.init_pipeline import add_init_parser

    add_init_parser(subparsers)

    # odibi story (generate, diff, list, last, show)
    from odibi.cli.story import add_story_parser

    add_story_parser(subparsers)

    # odibi catalog (runs, pipelines, nodes, state, tables, metrics, patterns, stats, sync)
    from odibi.cli.catalog import add_catalog_parser

    add_catalog_parser(subparsers)

    # odibi lineage (upstream, downstream, impact)
    from odibi.cli.lineage import add_lineage_parser

    add_lineage_parser(subparsers)

    # odibi schema (history, diff)
    from odibi.cli.schema import add_schema_parser

    add_schema_parser(subparsers)

    # odibi secrets (init, validate)
    from odibi.cli.secrets import add_secrets_parser

    add_secrets_parser(subparsers)

    # odibi system (sync, rebuild-summaries, optimize, cleanup)
    from odibi.cli.system import add_system_parser

    add_system_parser(subparsers)

    # odibi templates (list, show, transformer, schema)
    from odibi.cli.templates import add_templates_parser

    add_templates_parser(subparsers)

    # odibi list (transformers, patterns, connections) & odibi explain
    from odibi.cli.list_cmd import add_explain_parser, add_list_parser

    add_list_parser(subparsers)
    add_explain_parser(subparsers)

    # odibi export (--target airflow|dagster)
    from odibi.cli.export import add_export_parser

    add_export_parser(subparsers)

    # odibi ui
    from odibi.cli.ui import add_ui_parser

    add_ui_parser(subparsers)

    # odibi graph
    graph_parser = subparsers.add_parser("graph", help="Visualize pipeline dependency graph")
    graph_parser.add_argument("config", help="Path to YAML config file")
    graph_parser.add_argument("--pipeline", help="Pipeline name (default: first pipeline)")
    graph_parser.add_argument("--env", help="Environment overrides")
    graph_parser.add_argument(
        "--format", choices=["ascii", "dot", "mermaid"], default="ascii", help="Output format"
    )
    graph_parser.add_argument("--verbose", action="store_true", help="Show full tracebacks")

    # odibi deploy
    deploy_parser = subparsers.add_parser(
        "deploy", help="Deploy pipeline definitions to System Catalog"
    )
    deploy_parser.add_argument("config", help="Path to YAML config file")
    deploy_parser.add_argument("--env", help="Environment overrides")

    # odibi test
    test_parser = subparsers.add_parser("test", help="Run Odibi unit tests")
    test_parser.add_argument("path", nargs="?", default=".", help="Test file or directory")
    test_parser.add_argument("--snapshot", action="store_true", help="Update snapshot baselines")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == "run":
        from odibi.cli.run import run_command

        return run_command(args)
    elif args.command == "discover":
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
    elif args.command in ("init-pipeline", "create", "init", "generate-project"):
        from odibi.cli.init_pipeline import init_pipeline_command

        return init_pipeline_command(args)
    elif args.command == "story":
        from odibi.cli.story import story_command

        return story_command(args)
    elif args.command == "catalog":
        from odibi.cli.catalog import catalog_command

        return catalog_command(args)
    elif args.command == "lineage":
        from odibi.cli.lineage import lineage_command

        return lineage_command(args)
    elif args.command == "schema":
        from odibi.cli.schema import schema_command

        return schema_command(args)
    elif args.command == "secrets":
        from odibi.cli.secrets import secrets_command

        return secrets_command(args)
    elif args.command == "system":
        from odibi.cli.system import system_command

        return system_command(args)
    elif args.command == "templates":
        from odibi.cli.templates import templates_command

        return templates_command(args)
    elif args.command == "list":
        from odibi.cli.list_cmd import list_command

        return list_command(args)
    elif args.command == "explain":
        from odibi.cli.list_cmd import explain_command

        return explain_command(args)
    elif args.command == "export":
        from odibi.cli.export import export_command

        return export_command(args)
    elif args.command == "ui":
        from odibi.cli.ui import ui_command

        return ui_command(args)
    elif args.command == "graph":
        from odibi.cli.graph import graph_command

        return graph_command(args)
    elif args.command == "deploy":
        from odibi.cli.deploy import deploy_command

        return deploy_command(args)
    elif args.command == "test":
        from odibi.cli.test import test_command

        return test_command(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
