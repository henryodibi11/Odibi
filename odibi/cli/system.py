"""System CLI command for managing system catalog operations."""

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

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

    # odibi system rebuild-summaries
    rebuild_parser = system_subparsers.add_parser(
        "rebuild-summaries",
        help="Recompute derived tables from fact tables",
    )
    rebuild_parser.add_argument("config", help="Path to YAML config file")
    rebuild_parser.add_argument(
        "--env", default=None, help="Environment to apply overrides (e.g., dev, qat, prod)"
    )
    rebuild_parser.add_argument(
        "--pipeline",
        default=None,
        help="Specific pipeline to rebuild (mutually exclusive with --all)",
    )
    rebuild_parser.add_argument(
        "--all",
        dest="rebuild_all",
        action="store_true",
        help="Rebuild all pipelines",
    )
    rebuild_parser.add_argument(
        "--since",
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    rebuild_parser.add_argument(
        "--max-age-minutes",
        type=int,
        default=None,
        help="Max age for stale CLAIMED entries (default: DerivedUpdater.MAX_CLAIM_AGE_MINUTES)",
    )

    # odibi system cleanup
    cleanup_parser = system_subparsers.add_parser(
        "cleanup",
        help="Delete records older than retention period",
    )
    cleanup_parser.add_argument("config", help="Path to YAML config file")
    cleanup_parser.add_argument(
        "--env", default=None, help="Environment to apply overrides (e.g., dev, qat, prod)"
    )
    cleanup_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without making changes",
    )

    return system_parser


def system_command(args):
    """Execute system command."""
    if not hasattr(args, "system_command") or args.system_command is None:
        print("Usage: odibi system <command>")
        print("\nAvailable commands:")
        print("  sync              Sync system data from source to target backend")
        print("  rebuild-summaries Recompute derived tables from fact tables")
        print("  cleanup           Delete records older than retention period")
        return 1

    command_map = {
        "sync": _sync_command,
        "rebuild-summaries": _rebuild_summaries_command,
        "cleanup": _cleanup_command,
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


def _rebuild_summaries_command(args) -> int:
    """Recompute derived tables from fact tables."""
    from odibi.derived_updater import (
        MAX_CLAIM_AGE_MINUTES,
        DerivedUpdater,
    )

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

        pipeline_name: Optional[str] = args.pipeline
        rebuild_all: bool = args.rebuild_all

        if not pipeline_name and not rebuild_all:
            logger.error("Must specify either --pipeline or --all")
            return 1

        if pipeline_name and rebuild_all:
            logger.error("--pipeline and --all are mutually exclusive")
            return 1

        try:
            since_date = datetime.strptime(args.since, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Invalid date format: {args.since}. Use YYYY-MM-DD.")
            return 1

        max_age_minutes = args.max_age_minutes or MAX_CLAIM_AGE_MINUTES

        catalog = manager.get_catalog()
        if catalog is None:
            logger.error("CatalogManager not available")
            return 1

        updater = DerivedUpdater(catalog)

        run_ids = catalog.get_run_ids(pipeline_name=pipeline_name, since=since_date)

        if not run_ids:
            print(f"No runs found since {since_date}")
            if pipeline_name:
                print(f"  Pipeline filter: {pipeline_name}")
            return 0

        print(f"Found {len(run_ids)} runs to rebuild since {since_date}")
        if pipeline_name:
            print(f"  Pipeline filter: {pipeline_name}")

        rebuilt_count = 0
        skipped_count = 0
        failed_count = 0

        for run_id in run_ids:
            pipeline_run = catalog.get_pipeline_run(run_id)
            if pipeline_run is None:
                logger.warning(f"Run {run_id} not found, skipping")
                skipped_count += 1
                continue

            run_pipeline_name = pipeline_run.get("pipeline_name", "unknown")
            freshness_sla = pipeline_run.get("freshness_sla")

            for dt in ["meta_daily_stats", "meta_pipeline_health", "meta_sla_status"]:
                if dt == "meta_sla_status" and not freshness_sla:
                    continue

                token = updater.reclaim_for_rebuild(dt, run_id, max_age_minutes)
                if token is None:
                    skipped_count += 1
                    continue

                try:
                    if dt == "meta_daily_stats":
                        updater.update_daily_stats(run_id, pipeline_run)
                    elif dt == "meta_pipeline_health":
                        updater.update_pipeline_health(pipeline_run)
                    elif dt == "meta_sla_status":
                        owner = pipeline_run.get("owner")
                        freshness_anchor = pipeline_run.get("freshness_anchor", "run_completion")
                        updater.update_sla_status(
                            run_pipeline_name, owner, freshness_sla, freshness_anchor
                        )
                    updater.mark_applied(dt, run_id, token)
                    rebuilt_count += 1
                    print(f"  Rebuilt {dt} for {run_id}")
                except Exception as e:
                    updater.mark_failed(dt, run_id, token, str(e))
                    failed_count += 1
                    logger.warning(f"  Failed {dt} for {run_id}: {e}")

        print("\nRebuild complete!")
        print(f"  Rebuilt: {rebuilt_count}")
        print(f"  Skipped (already applied or not reclaimable): {skipped_count}")
        print(f"  Failed:  {failed_count}")

        return 0 if failed_count == 0 else 1

    except Exception as e:
        logger.error(f"Rebuild failed: {e}")
        raise


def _cleanup_command(args) -> int:
    """Delete records older than retention period."""
    from odibi.config import RetentionConfig

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

        catalog = manager.get_catalog()
        if catalog is None:
            logger.error("CatalogManager not available")
            return 1

        retention = project_config.system.retention_days or RetentionConfig()

        today = date.today()
        cutoffs = {
            "meta_daily_stats": today - timedelta(days=retention.daily_stats),
            "meta_failures": today - timedelta(days=retention.failures),
            "meta_observability_errors": today - timedelta(days=retention.observability_errors),
        }

        print("Cleanup with retention periods:")
        print(
            f"  meta_daily_stats:         {retention.daily_stats} days (cutoff: {cutoffs['meta_daily_stats']})"
        )
        print(
            f"  meta_failures:            {retention.failures} days (cutoff: {cutoffs['meta_failures']})"
        )
        print(
            f"  meta_observability_errors: {retention.observability_errors} days (cutoff: {cutoffs['meta_observability_errors']})"
        )

        if args.dry_run:
            print("\n[DRY RUN] Would delete records older than cutoffs")
            counts = _count_records_to_delete(catalog, cutoffs)
            for table, count in counts.items():
                print(f"  {table}: {count} records")
            return 0

        counts = _delete_old_records(catalog, cutoffs)
        print("\nCleanup complete!")
        for table, count in counts.items():
            print(f"  {table}: {count} records deleted")

        return 0

    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        raise


def _count_records_to_delete(catalog, cutoffs: dict) -> dict:
    """Count records that would be deleted."""
    counts = {}

    if catalog.is_spark_mode:
        counts = _count_records_spark(catalog, cutoffs)
    elif catalog.is_pandas_mode:
        counts = _count_records_pandas(catalog, cutoffs)
    elif catalog.is_sql_server_mode:
        counts = _count_records_sql_server(catalog, cutoffs)
    else:
        logger.warning("No backend available for counting records")

    return counts


def _count_records_spark(catalog, cutoffs: dict) -> dict:
    """Spark: Count records to delete."""
    from pyspark.sql import functions as F

    counts = {}
    for table, cutoff in cutoffs.items():
        try:
            df = catalog.spark.read.format("delta").load(catalog.tables[table])
            count = df.filter(F.col("date") < F.lit(str(cutoff))).count()
            counts[table] = count
        except Exception as e:
            logger.warning(f"Failed to count {table}: {e}")
            counts[table] = 0
    return counts


def _count_records_pandas(catalog, cutoffs: dict) -> dict:
    """Pandas/delta-rs: Count records to delete."""
    try:
        from deltalake import DeltaTable
    except ImportError:
        logger.warning("deltalake library not available")
        return {t: 0 for t in cutoffs}

    counts = {}
    storage_opts = catalog._get_storage_options()

    for table, cutoff in cutoffs.items():
        try:
            dt = DeltaTable(catalog.tables[table], storage_options=storage_opts or None)
            df = dt.to_pandas()
            import pandas as pd

            df["date"] = pd.to_datetime(df["date"]).dt.date
            count = len(df[df["date"] < cutoff])
            counts[table] = count
        except Exception as e:
            logger.warning(f"Failed to count {table}: {e}")
            counts[table] = 0
    return counts


def _count_records_sql_server(catalog, cutoffs: dict) -> dict:
    """SQL Server: Count records to delete."""
    counts = {}
    schema_name = getattr(catalog.config, "schema_name", None) or "odibi_system"

    for table, cutoff in cutoffs.items():
        try:
            sql = f"SELECT COUNT(*) FROM [{schema_name}].[{table}] WHERE date < :cutoff"
            result = catalog.connection.execute(sql, {"cutoff": cutoff})
            rows = list(result) if result else []
            counts[table] = rows[0][0] if rows else 0
        except Exception as e:
            logger.warning(f"Failed to count {table}: {e}")
            counts[table] = 0
    return counts


def _delete_old_records(catalog, cutoffs: dict) -> dict:
    """Delete records older than cutoffs."""
    if catalog.is_spark_mode:
        return _delete_records_spark(catalog, cutoffs)
    elif catalog.is_pandas_mode:
        return _delete_records_pandas(catalog, cutoffs)
    elif catalog.is_sql_server_mode:
        return _delete_records_sql_server(catalog, cutoffs)
    else:
        logger.warning("No backend available for deleting records")
        return {t: 0 for t in cutoffs}


def _delete_records_spark(catalog, cutoffs: dict) -> dict:
    """Spark: Delete old records using Delta DELETE."""

    counts = {}
    for table, cutoff in cutoffs.items():
        try:
            df = catalog.spark.read.format("delta").load(catalog.tables[table])
            before_count = df.count()

            delete_sql = f"""
                DELETE FROM delta.`{catalog.tables[table]}`
                WHERE date < '{cutoff}'
            """
            catalog.spark.sql(delete_sql)

            df = catalog.spark.read.format("delta").load(catalog.tables[table])
            after_count = df.count()
            counts[table] = before_count - after_count
        except Exception as e:
            logger.warning(f"Failed to delete from {table}: {e}")
            counts[table] = 0
    return counts


def _delete_records_pandas(catalog, cutoffs: dict) -> dict:
    """Pandas/delta-rs: Delete old records.

    Note: delta-rs predicate deletes require certain conditions.
    If not supported, raise NotImplementedError.
    """
    try:
        from deltalake import DeltaTable
    except ImportError:
        raise NotImplementedError("deltalake library not available for cleanup")

    storage_opts = catalog._get_storage_options()
    counts = {}

    for table, cutoff in cutoffs.items():
        try:
            dt = DeltaTable(catalog.tables[table], storage_options=storage_opts or None)

            df_before = dt.to_pandas()
            before_count = len(df_before)

            predicate = f"date < '{cutoff}'"
            try:
                dt.delete(predicate)
            except Exception as delete_err:
                if (
                    "not supported" in str(delete_err).lower()
                    or "predicate" in str(delete_err).lower()
                ):
                    raise NotImplementedError(
                        f"Predicate delete not supported for {table} in Pandas/delta-rs mode. "
                        f"Error: {delete_err}"
                    )
                raise

            dt = DeltaTable(catalog.tables[table], storage_options=storage_opts or None)
            df_after = dt.to_pandas()
            after_count = len(df_after)

            counts[table] = before_count - after_count
        except NotImplementedError:
            raise
        except Exception as e:
            logger.warning(f"Failed to delete from {table}: {e}")
            counts[table] = 0

    return counts


def _delete_records_sql_server(catalog, cutoffs: dict) -> dict:
    """SQL Server: Delete old records using DELETE statement."""
    counts = {}
    schema_name = getattr(catalog.config, "schema_name", None) or "odibi_system"

    for table, cutoff in cutoffs.items():
        try:
            count_sql = f"SELECT COUNT(*) FROM [{schema_name}].[{table}] WHERE date < :cutoff"
            result = catalog.connection.execute(count_sql, {"cutoff": cutoff})
            rows = list(result) if result else []
            count = rows[0][0] if rows else 0

            delete_sql = f"DELETE FROM [{schema_name}].[{table}] WHERE date < :cutoff"
            catalog.connection.execute(delete_sql, {"cutoff": cutoff})

            counts[table] = count
        except Exception as e:
            logger.warning(f"Failed to delete from {table}: {e}")
            counts[table] = 0
    return counts
