"""CLI entry point for the Improvement Environment.

Usage:
    python -m agents.improve init --source D:/odibi/odibi --root D:/improve_odibi
    python -m agents.improve status --root D:/improve_odibi
    python -m agents.improve run --root D:/improve_odibi --max-cycles 10 --max-hours 4
    python -m agents.improve report --root D:/improve_odibi
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from odibi.agents.improve.campaign import create_campaign_runner
from odibi.agents.improve.config import CampaignConfig, EnvironmentConfig
from odibi.agents.improve.environment import (
    ImprovementEnvironment,
    EnvironmentNotInitializedError,
    load_environment,
)
from odibi.agents.improve.status import StatusTracker


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for CLI."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def cmd_init(args: argparse.Namespace) -> int:
    """Initialize a new improvement environment."""
    source = Path(args.source).resolve()
    root = Path(args.root).resolve()

    print("Initializing improvement environment...")
    print(f"  Sacred source: {source}")
    print(f"  Environment root: {root}")

    if not source.exists():
        print(f"ERROR: Source repository does not exist: {source}")
        return 1

    if not source.is_dir():
        print(f"ERROR: Source must be a directory: {source}")
        return 1

    try:
        config = EnvironmentConfig(
            sacred_repo=source,
            environment_root=root,
        )
    except ValueError as e:
        print(f"ERROR: Invalid configuration: {e}")
        return 1

    env = ImprovementEnvironment(config)

    try:
        env.initialize(force=args.force)
    except Exception as e:
        print(f"ERROR: Initialization failed: {e}")
        return 1

    print()
    print("Environment initialized successfully!")
    print()
    print("Directory structure created:")
    print(f"  {root}/")
    print("    master/           - Blessed working copy (full repo)")
    print("    sandboxes/        - Disposable experiment clones")
    print("    snapshots/        - Master rollback points")
    print("    memory/           - Persistent lessons")
    print("    reports/          - Cycle reports")
    print("    stories/          - Execution traces")
    print("    config.yaml       - Environment configuration")
    print("    status.json       - Current state")
    print()
    print("Next steps:")
    print(f"  1. Verify Master clone: dir {root}\\master\\odibi")
    print(f"  2. Check config: type {root}\\config.yaml")
    print(f"  3. Run status: python -m agents.improve status --root {root}")

    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Show status of an improvement environment."""
    root = Path(args.root).resolve()

    if not root.exists():
        print(f"ERROR: Environment root does not exist: {root}")
        return 1

    try:
        env = load_environment(root)
    except EnvironmentNotInitializedError as e:
        print(f"ERROR: {e}")
        return 1

    status_file = root / "status.json"

    print("Improvement Environment Status")
    print("==============================")
    print(f"Root: {root}")
    print(f"Sacred: {env.sacred_repo}")
    print(f"Initialized: {env.is_initialized}")
    print()

    if status_file.exists():
        with open(status_file, "r", encoding="utf-8") as f:
            status = json.load(f)

        print(f"Status: {status.get('status', 'UNKNOWN')}")
        print(f"Cycles completed: {status.get('cycles_completed', 0)}")
        print(f"Improvements promoted: {status.get('improvements_promoted', 0)}")
        print(f"Active sandboxes: {len(status.get('active_sandboxes', []))}")
    else:
        print("No status file found")

    print()

    # List snapshots
    snapshots = env.list_snapshots()
    print(f"Snapshots: {len(snapshots)}")
    for snap in snapshots[-5:]:  # Show last 5
        print(f"  - {snap.snapshot_id} ({snap.size_bytes:,} bytes)")

    return 0


def cmd_list_sandboxes(args: argparse.Namespace) -> int:
    """List active sandboxes."""
    root = Path(args.root).resolve()

    try:
        env = load_environment(root)
    except EnvironmentNotInitializedError as e:
        print(f"ERROR: {e}")
        return 1

    sandboxes = env.list_active_sandboxes()

    if not sandboxes:
        print("No active sandboxes")
        return 0

    print(f"Active Sandboxes ({len(sandboxes)}):")
    for sb in sandboxes:
        print(f"  {sb.sandbox_id}")
        print(f"    Path: {sb.sandbox_path}")
        print(f"    Created: {sb.created_at}")
        print()

    return 0


def cmd_cleanup(args: argparse.Namespace) -> int:
    """Cleanup all active sandboxes."""
    root = Path(args.root).resolve()

    try:
        env = load_environment(root)
    except EnvironmentNotInitializedError as e:
        print(f"ERROR: {e}")
        return 1

    count = env.cleanup_all_sandboxes()
    print(f"Cleaned up {count} sandboxes")

    return 0


def cmd_run(args: argparse.Namespace) -> int:
    """Run an improvement campaign."""
    root = Path(args.root).resolve()

    if not root.exists():
        print(f"ERROR: Environment root does not exist: {root}")
        return 1

    enable_llm = not getattr(args, "no_llm", False)
    llm_model = getattr(args, "llm_model", "gpt-4o-mini")
    max_attempts = getattr(args, "max_attempts", 3)

    print("Starting improvement campaign...")
    print(f"  Environment: {root}")
    print(f"  Max cycles: {args.max_cycles}")
    print(f"  Max hours: {args.max_hours}")
    print(f"  Convergence threshold: {args.convergence}")
    print(f"  LLM improvements: {'enabled' if enable_llm else 'disabled'}")
    if enable_llm:
        print(f"  LLM model: {llm_model}")
        print(f"  Max attempts per cycle: {max_attempts}")
    print()

    campaign_config = CampaignConfig(
        name=args.name or "CLI Campaign",
        goal=args.goal or "Find and fix bugs",
        max_cycles=args.max_cycles,
        max_hours=args.max_hours,
        convergence_threshold=args.convergence,
        require_tests_pass=not args.skip_tests,
        require_lint_clean=not args.skip_lint,
        require_golden_pass=not args.skip_golden,
        enable_llm_improvements=enable_llm,
        llm_model=llm_model,
        max_improvement_attempts_per_cycle=max_attempts,
    )

    try:
        runner = create_campaign_runner(root, campaign_config)
    except Exception as e:
        print(f"ERROR: Failed to create campaign runner: {e}")
        return 1

    try:
        result = runner.run()
    except KeyboardInterrupt:
        print("\nCampaign interrupted by user")
        return 130
    except Exception as e:
        print(f"ERROR: Campaign failed: {e}")
        return 1

    print()
    print("Campaign Complete")
    print("=================")
    print(f"Campaign ID: {result.campaign_id}")
    print(f"Status: {result.status}")
    print(f"Stop reason: {result.stop_reason}")
    print(f"Cycles completed: {result.cycles_completed}")
    print(f"Improvements promoted: {result.improvements_promoted}")
    print(f"Improvements rejected: {result.improvements_rejected}")
    print(f"Elapsed: {result.elapsed_hours:.2f} hours")

    return 0 if result.success else 1


def cmd_report(args: argparse.Namespace) -> int:
    """Generate or display campaign report."""
    root = Path(args.root).resolve()

    if not root.exists():
        print(f"ERROR: Environment root does not exist: {root}")
        return 1

    status_file = root / "status.json"
    reports_dir = root / "reports"

    tracker = StatusTracker(status_file, reports_dir)

    if args.output:
        output_path = Path(args.output)
        report = tracker.generate_report()
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"Report saved to: {output_path}")
    else:
        report = tracker.generate_report()
        print(report)

    return 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        prog="agents.improve",
        description="Odibi Agent Improvement Environment CLI",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # init command
    init_parser = subparsers.add_parser(
        "init",
        help="Initialize a new improvement environment",
    )
    init_parser.add_argument(
        "--source",
        type=str,
        required=True,
        help="Path to Sacred source repository (NEVER touched)",
    )
    init_parser.add_argument(
        "--root",
        type=str,
        required=True,
        help="Path for environment root directory",
    )
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Force reinitialization if already exists",
    )
    init_parser.set_defaults(func=cmd_init)

    # status command
    status_parser = subparsers.add_parser(
        "status",
        help="Show environment status",
    )
    status_parser.add_argument(
        "--root",
        type=str,
        required=True,
        help="Path to environment root directory",
    )
    status_parser.set_defaults(func=cmd_status)

    # list-sandboxes command
    list_parser = subparsers.add_parser(
        "list-sandboxes",
        help="List active sandboxes",
    )
    list_parser.add_argument(
        "--root",
        type=str,
        required=True,
        help="Path to environment root directory",
    )
    list_parser.set_defaults(func=cmd_list_sandboxes)

    # cleanup command
    cleanup_parser = subparsers.add_parser(
        "cleanup",
        help="Cleanup all active sandboxes",
    )
    cleanup_parser.add_argument(
        "--root",
        type=str,
        required=True,
        help="Path to environment root directory",
    )
    cleanup_parser.set_defaults(func=cmd_cleanup)

    # run command
    run_parser = subparsers.add_parser(
        "run",
        help="Run an improvement campaign",
    )
    run_parser.add_argument(
        "--root",
        type=str,
        required=True,
        help="Path to environment root directory",
    )
    run_parser.add_argument(
        "--max-cycles",
        type=int,
        default=10,
        help="Maximum number of improvement cycles (default: 10)",
    )
    run_parser.add_argument(
        "--max-hours",
        type=float,
        default=4.0,
        help="Maximum hours to run (default: 4.0)",
    )
    run_parser.add_argument(
        "--convergence",
        type=int,
        default=3,
        help="Stop after N cycles without learning (default: 3)",
    )
    run_parser.add_argument(
        "--name",
        type=str,
        help="Campaign name",
    )
    run_parser.add_argument(
        "--goal",
        type=str,
        help="Campaign goal description",
    )
    run_parser.add_argument(
        "--skip-tests",
        action="store_true",
        help="Skip test gate",
    )
    run_parser.add_argument(
        "--skip-lint",
        action="store_true",
        help="Skip lint gate",
    )
    run_parser.add_argument(
        "--skip-golden",
        action="store_true",
        help="Skip golden projects gate",
    )
    run_parser.add_argument(
        "--llm-model",
        type=str,
        default="gpt-4o-mini",
        help="LLM model for improvements (default: gpt-4o-mini)",
    )
    run_parser.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help="Max LLM fix attempts per cycle (default: 3)",
    )
    run_parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Disable LLM improvements",
    )
    run_parser.set_defaults(func=cmd_run)

    # report command
    report_parser = subparsers.add_parser(
        "report",
        help="Generate or display campaign report",
    )
    report_parser.add_argument(
        "--root",
        type=str,
        required=True,
        help="Path to environment root directory",
    )
    report_parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Output file path (prints to stdout if not specified)",
    )
    report_parser.set_defaults(func=cmd_report)

    args = parser.parse_args()
    setup_logging(args.verbose)

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
