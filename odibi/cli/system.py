"""System CLI command for managing system catalog operations."""

from pathlib import Path

from odibi.pipeline import PipelineManager
from odibi.state import create_state_backend, create_sync_source_backend, sync_system_data
from odibi.utils.extensions import load_extensions
from odibi.utils.logging import logger


def add_system_parser(subparsers):
    """Add system subcommand parser."""
    system_parser = subparsers.add_parser(
        "system",
        help="Manage System Catalog operations",
        description="Commands for syncing and managing system catalog data",
    )

    system_subparsers = system_parser.add_subparsers(dest="system_command", help="System commands")

    # odibi system sync
    sync_parser = system_subparsers.add_parser(
        "sync",
        help="Sync system data from source to target backend",
    )
    sync_parser.add_argument("config", help="Path to YAML config file")
    sync_parser.add_argument(
        "--env", default=None, help="Environment to apply overrides (e.g., dev, qat, prod)"
    )
    sync_parser.add_argument(
        "--tables",
        nargs="+",
        choices=["runs", "state"],
        default=None,
        help="Tables to sync (default: all)",
    )
    sync_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be synced without making changes",
    )

    return system_parser


def system_command(args):
    """Execute system command."""
    if not hasattr(args, "system_command") or args.system_command is None:
        print("Usage: odibi system <command>")
        print("\nAvailable commands:")
        print("  sync       Sync system data from source to target backend")
        return 1

    command_map = {
        "sync": _sync_command,
    }

    handler = command_map.get(args.system_command)
    if handler:
        return handler(args)
    else:
        print(f"Unknown system command: {args.system_command}")
        return 1


def _sync_command(args) -> int:
    """Sync system data from source to target."""
    try:
        config_path = Path(args.config).resolve()

        load_extensions(config_path.parent)
        if config_path.parent.parent != config_path.parent:
            load_extensions(config_path.parent.parent)
        if config_path.parent != Path.cwd():
            load_extensions(Path.cwd())

        manager = PipelineManager.from_yaml(args.config, environment=getattr(args, "env", None))
        project_config = manager.config

        if not project_config.system:
            logger.error("System Catalog not configured. Add 'system' section to config.")
            return 1

        if not project_config.system.sync_from:
            logger.error(
                "No sync_from configured in system config. "
                "Add 'sync_from' section with connection and path."
            )
            return 1

        # Create source backend
        sync_from = project_config.system.sync_from
        source_backend = create_sync_source_backend(
            sync_from_config=sync_from,
            connections=project_config.connections,
            project_root=str(config_path.parent),
        )

        # Create target backend
        target_backend = create_state_backend(
            config=project_config,
            project_root=str(config_path.parent),
        )

        source_conn = sync_from.connection
        target_conn = project_config.system.connection
        tables = args.tables or ["runs", "state"]

        if args.dry_run:
            print("[DRY RUN] Would sync system data:")
            print(f"  Source: {source_conn}")
            print(f"  Target: {target_conn}")
            print(f"  Tables: {', '.join(tables)}")
            return 0

        print(f"Syncing system data from '{source_conn}' to '{target_conn}'...")

        result = sync_system_data(
            source_backend=source_backend,
            target_backend=target_backend,
            tables=tables,
        )

        print("\nSync complete!")
        print(f"  Runs synced:  {result['runs']}")
        print(f"  State synced: {result['state']}")

        return 0

    except Exception as e:
        logger.error(f"Sync failed: {e}")
        return 1
