"""
Diagnostics CLI Commands
========================

Commands for analyzing pipeline drift and data changes.
"""


from odibi.diagnostics.manager import HistoryManager
from odibi.diagnostics.diff import diff_runs
from odibi.diagnostics.delta import get_delta_diff


def diagnostics_command(args):
    """
    Handle diagnostics subcommands.
    """
    if args.diag_command == "run-diff":
        return run_diff_command(args)
    elif args.diag_command == "delta-diff":
        return delta_diff_command(args)
    else:
        print(f"Unknown diagnostics command: {args.diag_command}")
        return 1


def run_diff_command(args):
    """
    Compare two pipeline runs.

    Usage:
        odibi run-diff <pipeline_name> [run_id_a] [run_id_b]
    """
    manager = HistoryManager(history_path=args.history_dir)

    # Resolve runs
    run_a = None
    run_b = None

    runs = manager.list_runs(args.pipeline_name)
    if not runs:
        print(f"❌ No history found for pipeline '{args.pipeline_name}' in {args.history_dir}")
        return 1

    if args.run_id_b:
        # Explicit comparison: run_diff pipeline A B
        run_a = manager.get_run_by_id(args.pipeline_name, args.run_id_a)
        run_b = manager.get_run_by_id(args.pipeline_name, args.run_id_b)
    elif args.run_id_a:
        # Compare specific run vs its predecessor
        run_b = manager.get_run_by_id(args.pipeline_name, args.run_id_a)
        if run_b:
            run_a = manager.get_previous_run(args.pipeline_name, args.run_id_a)
    else:
        # Default: Compare latest vs previous
        if len(runs) < 2:
            print("⚠️  Not enough runs to compare (need at least 2)")
            return 1
        run_b = manager.load_run(runs[0]["path"])  # Latest
        run_a = manager.load_run(runs[1]["path"])  # Previous

    if not run_a or not run_b:
        print("❌ Could not resolve runs for comparison.")
        return 1

    print(f"\n🔍 Comparing Runs for '{args.pipeline_name}'")
    print(f"  🅰️  Baseline: {run_a.run_id} ({run_a.started_at})")
    print(f"  🅱️  Current:  {run_b.run_id} ({run_b.started_at})")
    print("-" * 60)

    result = diff_runs(run_a, run_b)

    # 1. Drift Source (Logic/Config Change)
    if result.drift_source_nodes:
        print("\n🚨 Drift Detected (Logic/Config Change):")
        for node in result.drift_source_nodes:
            diff = result.node_diffs[node]
            reasons = []
            if diff.sql_changed:
                reasons.append("SQL")
            if diff.config_changed:
                reasons.append("Config")
            if diff.transformation_changed:
                reasons.append("Transform Stack")

            print(f"  - {node}: {', '.join(reasons)}")

            if diff.sql_changed:
                print("    (SQL Hash mismatch)")
    else:
        print("\n✅ No Logic Drift Detected")

    # 2. Data Impact
    if result.impacted_downstream_nodes or any(
        d.rows_diff != 0 for d in result.node_diffs.values()
    ):
        print("\n📊 Data Impact:")
        for node, diff in result.node_diffs.items():
            if diff.rows_diff != 0 or diff.schema_change:
                icon = "⚠️" if diff.schema_change else "ℹ️"
                print(f"  {icon} {node}:")
                if diff.rows_diff != 0:
                    print(f"     Rows: {diff.rows_out_a} -> {diff.rows_out_b} ({diff.rows_diff:+})")
                if diff.schema_change:
                    print(
                        f"     Schema: +{len(diff.columns_added)} / -{len(diff.columns_removed)} cols"
                    )

    # 3. Topology
    if result.nodes_added:
        print(f"\n➕ Nodes Added: {', '.join(result.nodes_added)}")
    if result.nodes_removed:
        print(f"\n➖ Nodes Removed: {', '.join(result.nodes_removed)}")

    return 0


def delta_diff_command(args):
    """
    Directly compare Delta table versions.

    Usage:
        odibi delta-diff <path> [v1] [v2] [--deep]
    """
    try:
        # Determine versions
        # If not provided, we need to find them.
        # For now, require versions or assume latest-1 vs latest logic inside get_delta_diff?
        # Actually get_delta_diff takes v_a, v_b.
        # We need to query current version to know what "latest" is if not provided.

        from deltalake import DeltaTable

        dt = DeltaTable(args.path)
        latest_ver = dt.version()

        v_b = int(args.v2) if args.v2 is not None else latest_ver
        v_a = int(args.v1) if args.v1 is not None else (v_b - 1)

        if v_a < 0:
            print("❌ Version cannot be negative")
            return 1

        print(f"🔍 Delta Diff: {args.path}")
        print(f"   v{v_a} -> v{v_b}")
        if args.deep:
            print("   (Deep mode: Reading data for exact row diffs)")

        # Run Diff
        # Note: CLI uses Pandas backend (deltalake) by default as it's lighter than spinning up Spark
        diff = get_delta_diff(args.path, v_a, v_b, spark=None, deep=args.deep)

        print("\nRESULTS")
        print("-------")
        print(f"Rows Change: {diff.rows_change:+}")
        print(f"Files Change: {diff.files_change:+}")
        print(f"Size Change: {diff.size_change_bytes:+} bytes")

        if diff.schema_added:
            print(f"\n➕ Schema Added: {', '.join(diff.schema_added)}")
        if diff.schema_removed:
            print(f"\n➖ Schema Removed: {', '.join(diff.schema_removed)}")

        if diff.operations_between:
            print(f"\nOperations: {', '.join(set(diff.operations_between))}")

        if args.deep:
            if diff.sample_added:
                print(f"\nSample Added ({len(diff.sample_added)}):")
                print(diff.sample_added)
            if diff.sample_removed:
                print(f"\nSample Removed ({len(diff.sample_removed)}):")
                print(diff.sample_removed)

        return 0

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


def add_diagnostics_parser(subparsers):
    """Add diagnostics subcommands."""
    diag_parser = subparsers.add_parser(
        "diagnostics", aliases=["diag"], help="Pipeline diagnostics and drift analysis"
    )
    diag_subparsers = diag_parser.add_subparsers(dest="diag_command", help="Diagnostics commands")

    # run-diff
    rd_parser = diag_subparsers.add_parser("run-diff", help="Compare two pipeline runs")
    rd_parser.add_argument("pipeline_name", help="Name of the pipeline")
    rd_parser.add_argument("run_id_a", nargs="?", help="Baseline run ID (optional)")
    rd_parser.add_argument("run_id_b", nargs="?", help="Comparison run ID (optional)")
    rd_parser.add_argument("--history-dir", default="stories/", help="Path to stories directory")

    # delta-diff
    dd_parser = diag_subparsers.add_parser("delta-diff", help="Compare Delta table versions")
    dd_parser.add_argument("path", help="Path to Delta table")
    dd_parser.add_argument("v1", nargs="?", help="Baseline version")
    dd_parser.add_argument("v2", nargs="?", help="Comparison version")
    dd_parser.add_argument(
        "--deep", action="store_true", help="Perform expensive row-level comparison"
    )

    return diag_parser
