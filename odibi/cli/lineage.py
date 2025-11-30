"""CLI commands for cross-pipeline lineage tracking."""

import json
from typing import Dict, List, Optional

from odibi.config import load_config_from_file


def add_lineage_parser(subparsers) -> None:
    """Add lineage-related subcommands to the CLI."""
    lineage_parser = subparsers.add_parser("lineage", help="Cross-pipeline lineage commands")
    lineage_subparsers = lineage_parser.add_subparsers(dest="lineage_command")

    # odibi lineage upstream <table>
    upstream_parser = lineage_subparsers.add_parser(
        "upstream", help="Trace upstream sources of a table"
    )
    upstream_parser.add_argument("table", help="Table path (e.g., gold/customer_360)")
    upstream_parser.add_argument("--config", help="Path to YAML config file")
    upstream_parser.add_argument(
        "--depth", type=int, default=3, help="Maximum depth to traverse (default: 3)"
    )
    upstream_parser.add_argument(
        "--format",
        choices=["tree", "json"],
        default="tree",
        help="Output format (default: tree)",
    )

    # odibi lineage downstream <table>
    downstream_parser = lineage_subparsers.add_parser(
        "downstream", help="Trace downstream consumers of a table"
    )
    downstream_parser.add_argument("table", help="Table path (e.g., bronze/customers_raw)")
    downstream_parser.add_argument("--config", help="Path to YAML config file")
    downstream_parser.add_argument(
        "--depth", type=int, default=3, help="Maximum depth to traverse (default: 3)"
    )
    downstream_parser.add_argument(
        "--format",
        choices=["tree", "json"],
        default="tree",
        help="Output format (default: tree)",
    )

    # odibi lineage impact <table>
    impact_parser = lineage_subparsers.add_parser(
        "impact", help="Impact analysis for schema changes"
    )
    impact_parser.add_argument("table", help="Table path to analyze impact for")
    impact_parser.add_argument("--config", help="Path to YAML config file")
    impact_parser.add_argument(
        "--depth", type=int, default=3, help="Maximum depth to traverse (default: 3)"
    )


def lineage_command(args) -> int:
    """Execute lineage commands."""
    if not hasattr(args, "lineage_command") or not args.lineage_command:
        print("Usage: odibi lineage <command> [options]")
        print("Commands: upstream, downstream, impact")
        return 1

    if args.lineage_command == "upstream":
        return _lineage_upstream(args)
    elif args.lineage_command == "downstream":
        return _lineage_downstream(args)
    elif args.lineage_command == "impact":
        return _lineage_impact(args)
    else:
        print(f"Unknown lineage command: {args.lineage_command}")
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


def _build_tree(records: List[Dict], root: str, direction: str = "upstream") -> Dict:
    """Build a tree structure from lineage records."""
    tree = {"name": root, "children": []}

    by_depth = {}
    for record in records:
        depth = record.get("depth", 0)
        if depth not in by_depth:
            by_depth[depth] = []
        by_depth[depth].append(record)

    if direction == "upstream":
        depth_0_records = by_depth.get(0, [])
        for record in depth_0_records:
            source = record.get("source_table")
            node_info = ""
            if record.get("source_pipeline") and record.get("source_node"):
                node_info = f" ({record['source_pipeline']}.{record['source_node']})"
            child = {"name": f"{source}{node_info}", "children": []}
            tree["children"].append(child)
    else:
        depth_0_records = by_depth.get(0, [])
        for record in depth_0_records:
            target = record.get("target_table")
            node_info = ""
            if record.get("target_pipeline") and record.get("target_node"):
                node_info = f" ({record['target_pipeline']}.{record['target_node']})"
            child = {"name": f"{target}{node_info}", "children": []}
            tree["children"].append(child)

    return tree


def _print_tree(node: Dict, prefix: str = "", is_last: bool = True, depth: int = 0) -> None:
    """Print a tree structure in ASCII format."""
    connector = "└── " if is_last else "├── "
    if depth == 0:
        print(node["name"])
    else:
        print(f"{prefix}{connector}{node['name']}")

    children = node.get("children", [])
    child_prefix = prefix + ("    " if is_last else "│   ")
    for i, child in enumerate(children):
        is_child_last = i == len(children) - 1
        _print_tree(child, child_prefix, is_child_last, depth + 1)


def _lineage_upstream(args) -> int:
    """Trace upstream lineage for a table."""
    catalog = _get_catalog_manager(args.config)
    if not catalog:
        return 1

    upstream = catalog.get_upstream(args.table, depth=args.depth)

    if not upstream:
        print(f"No upstream lineage found for: {args.table}")
        return 0

    if args.format == "json":
        print(json.dumps(upstream, indent=2, default=str))
        return 0

    print(f"\nUpstream Lineage: {args.table}")
    print("=" * 60)

    tree = _build_tree(upstream, args.table, direction="upstream")
    _print_tree(tree)

    print()
    return 0


def _lineage_downstream(args) -> int:
    """Trace downstream lineage for a table."""
    catalog = _get_catalog_manager(args.config)
    if not catalog:
        return 1

    downstream = catalog.get_downstream(args.table, depth=args.depth)

    if not downstream:
        print(f"No downstream lineage found for: {args.table}")
        return 0

    if args.format == "json":
        print(json.dumps(downstream, indent=2, default=str))
        return 0

    print(f"\nDownstream Lineage: {args.table}")
    print("=" * 60)

    tree = _build_tree(downstream, args.table, direction="downstream")
    _print_tree(tree)

    print()
    return 0


def _lineage_impact(args) -> int:
    """Perform impact analysis for a table."""
    catalog = _get_catalog_manager(args.config)
    if not catalog:
        return 1

    downstream = catalog.get_downstream(args.table, depth=args.depth)

    if not downstream:
        print(f"No downstream dependencies found for: {args.table}")
        return 0

    affected_tables = set()
    affected_pipelines = set()

    for record in downstream:
        target = record.get("target_table")
        if target:
            affected_tables.add(target)
        pipeline = record.get("target_pipeline")
        if pipeline:
            affected_pipelines.add(pipeline)

    print(f"\n⚠️  Impact Analysis: {args.table}")
    print("=" * 60)
    print(f"\nChanges to {args.table} would affect:")
    print()

    if affected_tables:
        print("  Affected Tables:")
        for table in sorted(affected_tables):
            pipeline_info = ""
            for record in downstream:
                if record.get("target_table") == table:
                    if record.get("target_pipeline"):
                        pipeline_info = f" (pipeline: {record['target_pipeline']})"
                    break
            print(f"    - {table}{pipeline_info}")

    print()
    print("  Summary:")
    print(
        f"    Total: {len(affected_tables)} downstream table(s) in {len(affected_pipelines)} pipeline(s)"
    )
    print()

    return 0
