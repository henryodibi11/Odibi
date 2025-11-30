"""CLI commands for schema tracking."""

import json
from datetime import datetime
from typing import Optional

from odibi.config import load_config_from_file


def add_schema_parser(subparsers) -> None:
    """Add schema-related subcommands to the CLI."""
    schema_parser = subparsers.add_parser("schema", help="Schema version tracking commands")
    schema_subparsers = schema_parser.add_subparsers(dest="schema_command")

    # odibi schema history <table>
    history_parser = schema_subparsers.add_parser("history", help="Show schema version history")
    history_parser.add_argument("table", help="Table path (e.g., silver/customers)")
    history_parser.add_argument("--config", help="Path to YAML config file")
    history_parser.add_argument(
        "--limit", type=int, default=10, help="Maximum versions to show (default: 10)"
    )
    history_parser.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table)",
    )

    # odibi schema diff <table> --from <v1> --to <v2>
    diff_parser = schema_subparsers.add_parser("diff", help="Compare two schema versions")
    diff_parser.add_argument("table", help="Table path (e.g., silver/customers)")
    diff_parser.add_argument("--config", help="Path to YAML config file")
    diff_parser.add_argument("--from-version", type=int, help="Source version number")
    diff_parser.add_argument("--to-version", type=int, help="Target version number")


def schema_command(args) -> int:
    """Execute schema commands."""
    if not hasattr(args, "schema_command") or not args.schema_command:
        print("Usage: odibi schema <command> [options]")
        print("Commands: history, diff")
        return 1

    if args.schema_command == "history":
        return _schema_history(args)
    elif args.schema_command == "diff":
        return _schema_diff(args)
    else:
        print(f"Unknown schema command: {args.schema_command}")
        return 1


def _get_catalog_manager(config_path: Optional[str]):
    """Get CatalogManager instance from config."""
    if not config_path:
        print("Error: --config is required")
        return None

    try:
        project_config = load_config_from_file(config_path)

        from odibi.catalog import CatalogManager
        from odibi.engine import get_engine

        engine = get_engine(project_config.engine)
        system_conn = project_config.connections.get(project_config.system.connection)

        if hasattr(system_conn, "base_path"):
            base_path = f"{system_conn.base_path.rstrip('/')}/{project_config.system.path}"
        else:
            base_path = project_config.system.path

        catalog = CatalogManager(
            spark=None,
            config=project_config.system,
            base_path=base_path,
            engine=engine,
        )

        return catalog

    except FileNotFoundError:
        print(f"Error: Config file not found: {config_path}")
        return None
    except Exception as e:
        print(f"Error loading config: {e}")
        return None


def _schema_history(args) -> int:
    """Show schema version history for a table."""
    catalog = _get_catalog_manager(args.config)
    if not catalog:
        return 1

    history = catalog.get_schema_history(args.table, limit=args.limit)

    if not history:
        print(f"No schema history found for: {args.table}")
        return 0

    if args.format == "json":
        print(json.dumps(history, indent=2, default=str))
        return 0

    # Table format output
    print(f"\nSchema History: {args.table}")
    print("=" * 80)
    print(f"{'Version':<10} {'Captured At':<22} {'Changes'}")
    print("-" * 80)

    for record in history:
        version = f"v{record.get('schema_version', '?')}"
        captured_at = record.get("captured_at", "")
        if isinstance(captured_at, datetime):
            captured_at = captured_at.strftime("%Y-%m-%d %H:%M:%S")

        added = record.get("columns_added") or []
        removed = record.get("columns_removed") or []
        changed = record.get("columns_type_changed") or []

        changes = []
        if added:
            changes.append(f"+{', '.join(added[:3])}" + ("..." if len(added) > 3 else ""))
        if removed:
            changes.append(f"-{', '.join(removed[:3])}" + ("..." if len(removed) > 3 else ""))
        if changed:
            changes.append(f"~{', '.join(changed[:3])}" + ("..." if len(changed) > 3 else ""))

        if not changes:
            if record.get("schema_version") == 1:
                columns = json.loads(record.get("columns", "{}"))
                changes.append(f"Initial schema ({len(columns)} columns)")
            else:
                changes.append("(no changes detected)")

        print(f"{version:<10} {captured_at:<22} {'; '.join(changes)}")

    print()
    return 0


def _schema_diff(args) -> int:
    """Compare two schema versions."""
    catalog = _get_catalog_manager(args.config)
    if not catalog:
        return 1

    history = catalog.get_schema_history(args.table, limit=100)

    if not history:
        print(f"No schema history found for: {args.table}")
        return 1

    # Find the versions
    from_version = args.from_version
    to_version = args.to_version

    # Default to comparing latest two versions
    if from_version is None and to_version is None:
        if len(history) < 2:
            print("Need at least 2 versions to compare")
            return 1
        to_version = history[0].get("schema_version")
        from_version = history[1].get("schema_version")

    from_record = None
    to_record = None

    for record in history:
        if record.get("schema_version") == from_version:
            from_record = record
        if record.get("schema_version") == to_version:
            to_record = record

    if not from_record:
        print(f"Version v{from_version} not found")
        return 1
    if not to_record:
        print(f"Version v{to_version} not found")
        return 1

    from_cols = json.loads(from_record.get("columns", "{}"))
    to_cols = json.loads(to_record.get("columns", "{}"))

    print(f"\nSchema Diff: {args.table}")
    print(f"From v{from_version} → v{to_version}")
    print("=" * 60)

    all_cols = sorted(set(from_cols.keys()) | set(to_cols.keys()))

    for col in all_cols:
        in_from = col in from_cols
        in_to = col in to_cols

        if in_from and in_to:
            if from_cols[col] == to_cols[col]:
                print(f"  {col:<30} {to_cols[col]:<20} (unchanged)")
            else:
                print(f"~ {col:<30} {from_cols[col]} → {to_cols[col]}")
        elif in_to and not in_from:
            print(f"+ {col:<30} {to_cols[col]:<20} (added in v{to_version})")
        elif in_from and not in_to:
            print(f"- {col:<30} {from_cols[col]:<20} (removed in v{to_version})")

    print()
    return 0
