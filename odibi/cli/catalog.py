"""Catalog CLI command for querying the System Catalog."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from odibi.pipeline import PipelineManager
from odibi.utils.extensions import load_extensions
from odibi.utils.logging import logger


def add_catalog_parser(subparsers):
    """Add catalog subcommand parser."""
    catalog_parser = subparsers.add_parser(
        "catalog",
        help="Query System Catalog metadata",
        description="Query and explore the System Catalog (runs, pipelines, nodes, state, etc.)",
    )

    catalog_subparsers = catalog_parser.add_subparsers(
        dest="catalog_command", help="Catalog commands"
    )

    # odibi catalog runs
    runs_parser = catalog_subparsers.add_parser("runs", help="List execution runs from meta_runs")
    runs_parser.add_argument("config", help="Path to YAML config file")
    runs_parser.add_argument("--pipeline", "-p", help="Filter by pipeline name")
    runs_parser.add_argument("--node", "-n", help="Filter by node name")
    runs_parser.add_argument(
        "--status", "-s", choices=["SUCCESS", "FAILED", "RUNNING"], help="Filter by status"
    )
    runs_parser.add_argument(
        "--days", "-d", type=int, default=7, help="Show runs from last N days (default: 7)"
    )
    runs_parser.add_argument(
        "--limit", "-l", type=int, default=20, help="Maximum number of runs to show (default: 20)"
    )
    runs_parser.add_argument(
        "--format",
        "-f",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table)",
    )

    # odibi catalog pipelines
    pipelines_parser = catalog_subparsers.add_parser(
        "pipelines", help="List registered pipelines from meta_pipelines"
    )
    pipelines_parser.add_argument("config", help="Path to YAML config file")
    pipelines_parser.add_argument(
        "--format",
        "-f",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table)",
    )

    # odibi catalog nodes
    nodes_parser = catalog_subparsers.add_parser(
        "nodes", help="List registered nodes from meta_nodes"
    )
    nodes_parser.add_argument("config", help="Path to YAML config file")
    nodes_parser.add_argument("--pipeline", "-p", help="Filter by pipeline name")
    nodes_parser.add_argument(
        "--format",
        "-f",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table)",
    )

    # odibi catalog state
    state_parser = catalog_subparsers.add_parser("state", help="List HWM state from meta_state")
    state_parser.add_argument("config", help="Path to YAML config file")
    state_parser.add_argument("--pipeline", "-p", help="Filter by pipeline name")
    state_parser.add_argument(
        "--format",
        "-f",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table)",
    )

    # odibi catalog tables
    tables_parser = catalog_subparsers.add_parser(
        "tables", help="List registered assets from meta_tables"
    )
    tables_parser.add_argument("config", help="Path to YAML config file")
    tables_parser.add_argument("--project", help="Filter by project name")
    tables_parser.add_argument(
        "--format",
        "-f",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table)",
    )

    # odibi catalog metrics
    metrics_parser = catalog_subparsers.add_parser(
        "metrics", help="List metrics definitions from meta_metrics"
    )
    metrics_parser.add_argument("config", help="Path to YAML config file")
    metrics_parser.add_argument(
        "--format",
        "-f",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table)",
    )

    # odibi catalog patterns
    patterns_parser = catalog_subparsers.add_parser(
        "patterns", help="List pattern compliance from meta_patterns"
    )
    patterns_parser.add_argument("config", help="Path to YAML config file")
    patterns_parser.add_argument(
        "--format",
        "-f",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table)",
    )

    # odibi catalog stats
    stats_parser = catalog_subparsers.add_parser("stats", help="Show execution statistics")
    stats_parser.add_argument("config", help="Path to YAML config file")
    stats_parser.add_argument("--pipeline", "-p", help="Filter by pipeline name")
    stats_parser.add_argument(
        "--days", "-d", type=int, default=7, help="Statistics over last N days (default: 7)"
    )

    # odibi catalog sync
    sync_parser = catalog_subparsers.add_parser(
        "sync",
        help="Sync catalog to secondary destination",
        description=(
            "Manually sync system catalog tables to the configured sync_to destination. "
            "Use this to sync on-demand or when automatic sync is set to 'manual'."
        ),
    )
    sync_parser.add_argument("config", help="Path to YAML config file")
    sync_parser.add_argument(
        "--tables",
        "-t",
        help="Comma-separated list of tables to sync (default: high-priority tables)",
    )
    sync_parser.add_argument(
        "--mode",
        "-m",
        choices=["incremental", "full"],
        help="Override sync mode (default: use config value)",
    )
    sync_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be synced without actually syncing",
    )

    # odibi catalog sync-status
    sync_status_parser = catalog_subparsers.add_parser(
        "sync-status",
        help="Show sync status for configured destinations",
    )
    sync_status_parser.add_argument("config", help="Path to YAML config file")

    # odibi catalog sync-purge
    sync_purge_parser = catalog_subparsers.add_parser(
        "sync-purge",
        help="Purge old records from SQL Server sync tables",
        description="Delete records older than N days from SQL Server sync tables.",
    )
    sync_purge_parser.add_argument("config", help="Path to YAML config file")
    sync_purge_parser.add_argument(
        "--days",
        "-d",
        type=int,
        default=90,
        help="Delete records older than N days (default: 90)",
    )
    sync_purge_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be purged without actually deleting",
    )

    return catalog_parser


def catalog_command(args):
    """Execute catalog command."""
    if not hasattr(args, "catalog_command") or args.catalog_command is None:
        print("Usage: odibi catalog <command>")
        print("\nAvailable commands:")
        print("  runs        List execution runs")
        print("  pipelines   List registered pipelines")
        print("  nodes       List registered nodes")
        print("  state       List HWM state checkpoints")
        print("  tables      List registered assets")
        print("  metrics     List metrics definitions")
        print("  patterns    List pattern compliance")
        print("  stats       Show execution statistics")
        print("  sync        Sync catalog to secondary destination")
        print("  sync-status Show sync status")
        print("  sync-purge  Purge old records from SQL Server sync tables")
        return 1

    command_map = {
        "runs": _runs_command,
        "pipelines": _pipelines_command,
        "nodes": _nodes_command,
        "state": _state_command,
        "tables": _tables_command,
        "metrics": _metrics_command,
        "patterns": _patterns_command,
        "stats": _stats_command,
        "sync": _sync_command,
        "sync-status": _sync_status_command,
        "sync-purge": _sync_purge_command,
    }

    handler = command_map.get(args.catalog_command)
    if handler:
        return handler(args)
    else:
        print(f"Unknown catalog command: {args.catalog_command}")
        return 1


def _get_catalog_manager(args):
    """Load config and return catalog manager."""
    try:
        config_path = Path(args.config).resolve()

        load_extensions(config_path.parent)
        if config_path.parent.parent != config_path.parent:
            load_extensions(config_path.parent.parent)
        if config_path.parent != Path.cwd():
            load_extensions(Path.cwd())

        manager = PipelineManager.from_yaml(args.config)

        if not manager.catalog_manager:
            logger.error("System Catalog not configured. Add 'system' section to config.")
            return None

        return manager.catalog_manager

    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return None


def _format_table(headers: list, rows: list, max_width: int = 40) -> str:
    """Format data as ASCII table."""
    if not rows:
        return "No data found."

    def truncate(val, width):
        s = str(val) if val is not None else ""
        if len(s) > width:
            return s[: width - 3] + "..."
        return s

    col_widths = []
    for i, header in enumerate(headers):
        max_col = len(header)
        for row in rows:
            if i < len(row):
                max_col = max(max_col, min(len(str(row[i] or "")), max_width))
        col_widths.append(min(max_col, max_width))

    header_line = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    separator = "-+-".join("-" * w for w in col_widths)

    lines = [header_line, separator]
    for row in rows:
        row_line = " | ".join(
            truncate(row[i] if i < len(row) else "", col_widths[i]).ljust(col_widths[i])
            for i in range(len(headers))
        )
        lines.append(row_line)

    return "\n".join(lines)


def _format_output(headers: list, rows: list, output_format: str) -> str:
    """Format output as table or JSON."""
    if output_format == "json":
        data = [dict(zip(headers, row)) for row in rows]
        return json.dumps(data, indent=2, default=str)
    else:
        return _format_table(headers, rows)


def _runs_command(args) -> int:
    """List execution runs."""
    catalog = _get_catalog_manager(args)
    if not catalog:
        return 1

    try:
        df = catalog._read_local_table(catalog.tables["meta_runs"])

        if df.empty:
            print("No runs found in catalog.")
            return 0

        cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)

        if "timestamp" in df.columns:
            import pandas as pd

            if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
                df["timestamp"] = pd.to_datetime(df["timestamp"])

            if df["timestamp"].dt.tz is None:
                df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")

            df = df[df["timestamp"] >= cutoff]

        if args.pipeline and "pipeline_name" in df.columns:
            df = df[df["pipeline_name"] == args.pipeline]

        if args.node and "node_name" in df.columns:
            df = df[df["node_name"] == args.node]

        if args.status and "status" in df.columns:
            df = df[df["status"] == args.status]

        if "timestamp" in df.columns:
            df = df.sort_values("timestamp", ascending=False)

        df = df.head(args.limit)

        headers = [
            "run_id",
            "pipeline_name",
            "node_name",
            "status",
            "rows",
            "duration_ms",
            "timestamp",
        ]
        available_cols = [c for c in headers if c in df.columns]

        if "rows" in available_cols:
            available_cols[available_cols.index("rows")] = "rows_processed"
            headers[headers.index("rows")] = "rows_processed"

        rows = df[available_cols].values.tolist() if available_cols else []

        print(_format_output(headers, rows, args.format))
        print(f"\nShowing {len(rows)} runs from the last {args.days} days.")
        return 0

    except Exception as e:
        logger.error(f"Failed to query runs: {e}")
        return 1


def _pipelines_command(args) -> int:
    """List registered pipelines."""
    catalog = _get_catalog_manager(args)
    if not catalog:
        return 1

    try:
        df = catalog._read_local_table(catalog.tables["meta_pipelines"])

        if df.empty:
            print("No pipelines registered in catalog. Run 'odibi deploy' first.")
            return 0

        headers = ["pipeline_name", "layer", "description", "version_hash", "updated_at"]
        available_cols = [c for c in headers if c in df.columns]

        if "updated_at" in df.columns:
            df = df.sort_values("updated_at", ascending=False)

        rows = df[available_cols].values.tolist() if available_cols else []

        print(_format_output(headers, rows, args.format))
        print(f"\n{len(rows)} pipeline(s) registered.")
        return 0

    except Exception as e:
        logger.error(f"Failed to query pipelines: {e}")
        return 1


def _nodes_command(args) -> int:
    """List registered nodes."""
    catalog = _get_catalog_manager(args)
    if not catalog:
        return 1

    try:
        df = catalog._read_local_table(catalog.tables["meta_nodes"])

        if df.empty:
            print("No nodes registered in catalog. Run 'odibi deploy' first.")
            return 0

        if args.pipeline and "pipeline_name" in df.columns:
            df = df[df["pipeline_name"] == args.pipeline]

        headers = ["pipeline_name", "node_name", "type", "version_hash", "updated_at"]
        available_cols = [c for c in headers if c in df.columns]

        if "updated_at" in df.columns:
            df = df.sort_values("updated_at", ascending=False)

        rows = df[available_cols].values.tolist() if available_cols else []

        print(_format_output(headers, rows, args.format))
        print(f"\n{len(rows)} node(s) registered.")
        return 0

    except Exception as e:
        logger.error(f"Failed to query nodes: {e}")
        return 1


def _state_command(args) -> int:
    """List HWM state checkpoints."""
    catalog = _get_catalog_manager(args)
    if not catalog:
        return 1

    try:
        df = catalog._read_local_table(catalog.tables["meta_state"])

        if df.empty:
            print("No state checkpoints found in catalog.")
            return 0

        if args.pipeline and "key" in df.columns:
            df = df[df["key"].str.contains(args.pipeline, na=False)]

        headers = ["key", "value", "updated_at"]
        available_cols = [c for c in headers if c in df.columns]

        rows = df[available_cols].values.tolist() if available_cols else []

        print(_format_output(headers, rows, args.format))
        print(f"\n{len(rows)} state checkpoint(s) found.")
        return 0

    except Exception as e:
        logger.error(f"Failed to query state: {e}")
        return 1


def _tables_command(args) -> int:
    """List registered assets."""
    catalog = _get_catalog_manager(args)
    if not catalog:
        return 1

    try:
        df = catalog._read_local_table(catalog.tables["meta_tables"])

        if df.empty:
            print("No assets registered in catalog.")
            return 0

        if args.project and "project_name" in df.columns:
            df = df[df["project_name"] == args.project]

        headers = ["project_name", "table_name", "path", "format", "pattern_type", "updated_at"]
        available_cols = [c for c in headers if c in df.columns]

        rows = df[available_cols].values.tolist() if available_cols else []

        print(_format_output(headers, rows, args.format))
        print(f"\n{len(rows)} asset(s) registered.")
        return 0

    except Exception as e:
        logger.error(f"Failed to query tables: {e}")
        return 1


def _metrics_command(args) -> int:
    """List metrics definitions."""
    catalog = _get_catalog_manager(args)
    if not catalog:
        return 1

    try:
        df = catalog._read_local_table(catalog.tables["meta_metrics"])

        if df.empty:
            print("No metrics defined in catalog.")
            return 0

        headers = ["metric_name", "source_table", "definition_sql", "dimensions"]
        available_cols = [c for c in headers if c in df.columns]

        rows = df[available_cols].values.tolist() if available_cols else []

        print(_format_output(headers, rows, args.format))
        print(f"\n{len(rows)} metric(s) defined.")
        return 0

    except Exception as e:
        logger.error(f"Failed to query metrics: {e}")
        return 1


def _patterns_command(args) -> int:
    """List pattern compliance."""
    catalog = _get_catalog_manager(args)
    if not catalog:
        return 1

    try:
        df = catalog._read_local_table(catalog.tables["meta_patterns"])

        if df.empty:
            print("No pattern data in catalog.")
            return 0

        headers = ["table_name", "pattern_type", "compliance_score", "configuration"]
        available_cols = [c for c in headers if c in df.columns]

        rows = df[available_cols].values.tolist() if available_cols else []

        print(_format_output(headers, rows, args.format))
        print(f"\n{len(rows)} pattern record(s) found.")
        return 0

    except Exception as e:
        logger.error(f"Failed to query patterns: {e}")
        return 1


def _stats_command(args) -> int:
    """Show execution statistics."""
    catalog = _get_catalog_manager(args)
    if not catalog:
        return 1

    try:
        import pandas as pd

        df = catalog._read_local_table(catalog.tables["meta_runs"])

        if df.empty:
            print("No runs found in catalog for statistics.")
            return 0

        cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)

        if "timestamp" in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
                df["timestamp"] = pd.to_datetime(df["timestamp"])

            if df["timestamp"].dt.tz is None:
                df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")

            df = df[df["timestamp"] >= cutoff]

        if args.pipeline and "pipeline_name" in df.columns:
            df = df[df["pipeline_name"] == args.pipeline]

        if df.empty:
            print(f"No runs in the last {args.days} days.")
            return 0

        print(f"=== Execution Statistics (Last {args.days} Days) ===\n")

        total_runs = len(df)
        success_runs = len(df[df["status"] == "SUCCESS"]) if "status" in df.columns else 0
        failed_runs = len(df[df["status"] == "FAILED"]) if "status" in df.columns else 0
        success_rate = (success_runs / total_runs * 100) if total_runs > 0 else 0

        print(f"Total Runs:     {total_runs}")
        print(f"Successful:     {success_runs}")
        print(f"Failed:         {failed_runs}")
        print(f"Success Rate:   {success_rate:.1f}%")

        if "rows_processed" in df.columns:
            total_rows = df["rows_processed"].sum()
            avg_rows = df["rows_processed"].mean()
            print(f"\nTotal Rows:     {int(total_rows):,}")
            print(f"Avg Rows/Run:   {int(avg_rows):,}")

        if "duration_ms" in df.columns:
            avg_duration_ms = df["duration_ms"].mean()
            total_duration_ms = df["duration_ms"].sum()
            print(f"\nAvg Duration:   {avg_duration_ms / 1000:.2f}s")
            print(f"Total Runtime:  {total_duration_ms / 1000:.2f}s")

        if "pipeline_name" in df.columns:
            print("\n--- Runs by Pipeline ---")
            pipeline_counts = df["pipeline_name"].value_counts()
            for pipeline, count in pipeline_counts.items():
                print(f"  {pipeline}: {count}")

        if "node_name" in df.columns and "status" in df.columns:
            failed_nodes = df[df["status"] == "FAILED"]["node_name"].value_counts()
            if not failed_nodes.empty:
                print("\n--- Most Failed Nodes ---")
                for node, count in failed_nodes.head(5).items():
                    print(f"  {node}: {count} failures")

        return 0

    except Exception as e:
        logger.error(f"Failed to compute statistics: {e}")
        return 1


def _sync_command(args):
    """Execute catalog sync command."""
    from odibi.catalog_sync import CatalogSyncer, ALL_SYNC_TABLES, DEFAULT_SYNC_TABLES
    from odibi.config import load_config_from_file

    try:
        config_path = Path(args.config).resolve()
        project_config = load_config_from_file(str(config_path))

        if not project_config.system:
            print("Error: No 'system' configuration found in config file.")
            return 1

        if not project_config.system.sync_to:
            print("Error: No 'sync_to' configured in system section.")
            print("\nTo enable sync, add sync_to to your config:")
            print("  system:")
            print("    connection: your_blob_connection")
            print("    sync_to:")
            print("      connection: your_sql_server")
            print("      schema_name: odibi_system")
            return 1

        sync_config = project_config.system.sync_to

        # Parse tables argument
        tables_to_sync = None
        if args.tables:
            tables_to_sync = [t.strip() for t in args.tables.split(",")]
            invalid = [t for t in tables_to_sync if t not in ALL_SYNC_TABLES]
            if invalid:
                print(f"Error: Unknown tables: {invalid}")
                print(f"Available tables: {ALL_SYNC_TABLES}")
                return 1

        # Override mode if specified
        if args.mode:
            sync_config = sync_config.model_copy(update={"mode": args.mode})

        # Dry run
        if args.dry_run:
            tables = tables_to_sync or sync_config.tables or DEFAULT_SYNC_TABLES
            print("=== Dry Run - Would sync the following ===\n")
            print(f"Target: {sync_config.connection}")
            print(f"Mode: {sync_config.mode}")
            print(f"Tables: {', '.join(tables)}")
            return 0

        # Get catalog manager
        catalog = _get_catalog_manager(args)
        if not catalog:
            print("Error: Could not initialize catalog manager.")
            return 1

        # Get target connection
        load_extensions(config_path.parent)
        pm = PipelineManager(str(config_path))
        target_conn = pm.connections.get(sync_config.connection)
        if not target_conn:
            print(f"Error: Target connection '{sync_config.connection}' not found.")
            return 1

        # Create syncer and run
        syncer = CatalogSyncer(
            source_catalog=catalog,
            sync_config=sync_config,
            target_connection=target_conn,
            spark=getattr(pm, "spark", None),
            environment=project_config.system.environment,
        )

        print(f"Syncing catalog to {sync_config.connection}...")
        results = syncer.sync(tables=tables_to_sync)

        # Print results
        print("\n=== Sync Results ===\n")
        total_rows = 0
        for table, result in results.items():
            status = "✓" if result.get("success") else "✗"
            rows = result.get("rows", 0)
            total_rows += rows
            error = result.get("error", "")
            if error:
                print(f"  {status} {table}: {error}")
            else:
                print(f"  {status} {table}: {rows:,} rows")

        success_count = sum(1 for r in results.values() if r.get("success"))
        print(f"\nSummary: {success_count}/{len(results)} tables synced, {total_rows:,} total rows")

        return 0 if success_count == len(results) else 1

    except Exception as e:
        logger.error(f"Sync failed: {e}")
        return 1


def _sync_status_command(args):
    """Show sync status for configured destinations."""
    from odibi.config import load_config_from_file

    try:
        config_path = Path(args.config).resolve()
        project_config = load_config_from_file(str(config_path))

        if not project_config.system:
            print("Error: No 'system' configuration found.")
            return 1

        print("=== Catalog Sync Status ===\n")
        print(f"Primary: {project_config.system.connection}")
        print(f"Path: {project_config.system.path}")

        if not project_config.system.sync_to:
            print("\nNo sync_to configured.")
            return 0

        sync_config = project_config.system.sync_to
        print(f"\nSync Target: {sync_config.connection}")
        print(f"Mode: {sync_config.mode}")
        print(f"Trigger: {sync_config.on}")
        print(f"Async: {sync_config.async_sync}")

        if sync_config.tables:
            print(f"Tables: {', '.join(sync_config.tables)}")
        else:
            print("Tables: (default high-priority tables)")

        # Try to get last sync timestamps
        catalog = _get_catalog_manager(args)
        if catalog:
            print("\n--- Last Sync Timestamps ---")
            from odibi.catalog_sync import DEFAULT_SYNC_TABLES

            for table in sync_config.tables or DEFAULT_SYNC_TABLES:
                key = f"sync_to:{sync_config.connection}:{table}:last_timestamp"
                try:
                    value = catalog.get_state(key)
                    if value:
                        print(f"  {table}: {value}")
                    else:
                        print(f"  {table}: never synced")
                except Exception:
                    print(f"  {table}: unknown")

        return 0

    except Exception as e:
        logger.error(f"Failed to get sync status: {e}")
        return 1


def _sync_purge_command(args):
    """Purge old records from SQL Server sync tables."""
    from odibi.catalog_sync import CatalogSyncer
    from odibi.config import load_config_from_file

    try:
        config_path = Path(args.config).resolve()
        project_config = load_config_from_file(str(config_path))

        if not project_config.system or not project_config.system.sync_to:
            print("Error: No 'sync_to' configured in system section.")
            return 1

        sync_config = project_config.system.sync_to

        # Get catalog and target connection
        catalog = _get_catalog_manager(args)
        if not catalog:
            print("Error: Could not initialize catalog manager.")
            return 1

        load_extensions(config_path.parent)
        pm = PipelineManager(str(config_path))
        target_conn = pm.connections.get(sync_config.connection)
        if not target_conn:
            print(f"Error: Target connection '{sync_config.connection}' not found.")
            return 1

        syncer = CatalogSyncer(
            source_catalog=catalog,
            sync_config=sync_config,
            target_connection=target_conn,
            environment=project_config.system.environment,
        )

        if syncer.target_type != "sql_server":
            print(f"Error: Purge only supported for SQL Server targets, got: {syncer.target_type}")
            return 1

        if args.dry_run:
            print(f"=== Dry Run - Would purge records older than {args.days} days ===\n")
            print(f"Target: {sync_config.connection}")
            print(f"Schema: {sync_config.schema_name or 'odibi_system'}")
            print("Tables: meta_runs, meta_pipeline_runs, meta_node_runs, meta_failures")
            return 0

        print(f"Purging records older than {args.days} days from {sync_config.connection}...")
        results = syncer.purge_sql_tables(days=args.days)

        print("\n=== Purge Results ===\n")
        for table, result in results.items():
            status = "✓" if result.get("success") else "✗"
            if result.get("success"):
                print(f"  {status} {table}: purged (keeping last {args.days} days)")
            else:
                print(f"  {status} {table}: {result.get('error', 'unknown error')}")

        success_count = sum(1 for r in results.values() if r.get("success"))
        print(f"\nPurged {success_count}/{len(results)} tables")

        return 0 if success_count == len(results) else 1

    except Exception as e:
        logger.error(f"Purge failed: {e}")
        return 1
