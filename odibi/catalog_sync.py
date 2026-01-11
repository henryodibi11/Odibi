"""
Catalog Sync - Syncs system catalog data to secondary destinations.

Enables replication of Delta-based system tables to SQL Server (for dashboards/queries)
or another blob storage (for cross-region backup).
"""

import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from odibi.config import SyncToConfig
from odibi.utils import get_logging_context

logger = logging.getLogger(__name__)


# Default high-priority tables to sync if not specified
DEFAULT_SYNC_TABLES = [
    "meta_runs",
    "meta_pipeline_runs",
    "meta_node_runs",
    "meta_tables",
    "meta_failures",
]

# All available tables that can be synced
ALL_SYNC_TABLES = [
    "meta_tables",
    "meta_runs",
    "meta_patterns",
    "meta_metrics",
    "meta_state",
    "meta_pipelines",
    "meta_nodes",
    "meta_schemas",
    "meta_lineage",
    "meta_outputs",
    "meta_pipeline_runs",
    "meta_node_runs",
    "meta_failures",
    "meta_observability_errors",
    "meta_derived_applied_runs",
    "meta_daily_stats",
    "meta_pipeline_health",
    "meta_sla_status",
]

# SQL Server DDL templates for each table
SQL_SERVER_DDL = {
    "meta_runs": """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'meta_runs' AND schema_id = SCHEMA_ID('{schema}'))
        BEGIN
            CREATE TABLE [{schema}].[meta_runs] (
                run_id NVARCHAR(100),
                pipeline_name NVARCHAR(255),
                node_name NVARCHAR(255),
                status NVARCHAR(50),
                rows_processed BIGINT,
                duration_ms BIGINT,
                metrics_json NVARCHAR(MAX),
                environment NVARCHAR(50),
                timestamp DATETIME2,
                date DATE,
                _synced_at DATETIME2 DEFAULT GETUTCDATE()
            );
            CREATE INDEX IX_meta_runs_pipeline_date ON [{schema}].[meta_runs] (pipeline_name, date);
        END
    """,
    "meta_pipeline_runs": """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'meta_pipeline_runs' AND schema_id = SCHEMA_ID('{schema}'))
        BEGIN
            CREATE TABLE [{schema}].[meta_pipeline_runs] (
                run_id NVARCHAR(100) PRIMARY KEY,
                pipeline_name NVARCHAR(255),
                status NVARCHAR(50),
                start_time DATETIME2,
                end_time DATETIME2,
                duration_ms BIGINT,
                nodes_total INT,
                nodes_success INT,
                nodes_failed INT,
                nodes_skipped INT,
                total_rows BIGINT,
                environment NVARCHAR(50),
                config_hash NVARCHAR(100),
                trigger NVARCHAR(50),
                date DATE,
                _synced_at DATETIME2 DEFAULT GETUTCDATE()
            );
            CREATE INDEX IX_meta_pipeline_runs_date ON [{schema}].[meta_pipeline_runs] (pipeline_name, date);
        END
    """,
    "meta_node_runs": """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'meta_node_runs' AND schema_id = SCHEMA_ID('{schema}'))
        BEGIN
            CREATE TABLE [{schema}].[meta_node_runs] (
                run_id NVARCHAR(100),
                pipeline_run_id NVARCHAR(100),
                pipeline_name NVARCHAR(255),
                node_name NVARCHAR(255),
                status NVARCHAR(50),
                start_time DATETIME2,
                end_time DATETIME2,
                duration_ms BIGINT,
                rows_read BIGINT,
                rows_written BIGINT,
                pattern NVARCHAR(100),
                error_message NVARCHAR(MAX),
                environment NVARCHAR(50),
                date DATE,
                _synced_at DATETIME2 DEFAULT GETUTCDATE()
            );
            CREATE INDEX IX_meta_node_runs_pipeline ON [{schema}].[meta_node_runs] (pipeline_name, node_name, date);
        END
    """,
    "meta_tables": """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'meta_tables' AND schema_id = SCHEMA_ID('{schema}'))
        BEGIN
            CREATE TABLE [{schema}].[meta_tables] (
                table_name NVARCHAR(500) PRIMARY KEY,
                connection NVARCHAR(255),
                path NVARCHAR(1000),
                format NVARCHAR(50),
                pipeline NVARCHAR(255),
                node NVARCHAR(255),
                layer NVARCHAR(50),
                row_count BIGINT,
                size_bytes BIGINT,
                last_updated DATETIME2,
                schema_json NVARCHAR(MAX),
                environment NVARCHAR(50),
                _synced_at DATETIME2 DEFAULT GETUTCDATE()
            );
        END
    """,
    "meta_failures": """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'meta_failures' AND schema_id = SCHEMA_ID('{schema}'))
        BEGIN
            CREATE TABLE [{schema}].[meta_failures] (
                failure_id NVARCHAR(100) PRIMARY KEY,
                run_id NVARCHAR(100),
                pipeline_name NVARCHAR(255),
                node_name NVARCHAR(255),
                error_type NVARCHAR(100),
                error_message NVARCHAR(MAX),
                stack_trace NVARCHAR(MAX),
                timestamp DATETIME2,
                environment NVARCHAR(50),
                date DATE,
                _synced_at DATETIME2 DEFAULT GETUTCDATE()
            );
            CREATE INDEX IX_meta_failures_date ON [{schema}].[meta_failures] (pipeline_name, date);
        END
    """,
}


class CatalogSyncer:
    """
    Syncs system catalog data from Delta tables to a secondary destination.

    Supports:
    - Delta → SQL Server (for dashboards/queries)
    - Delta → Delta (for cross-region replication)
    """

    def __init__(
        self,
        source_catalog: Any,  # CatalogManager
        sync_config: SyncToConfig,
        target_connection: Any,
        spark: Optional[Any] = None,
        environment: Optional[str] = None,
    ):
        """
        Initialize the catalog syncer.

        Args:
            source_catalog: CatalogManager instance (source of truth)
            sync_config: SyncToConfig with sync settings
            target_connection: Target connection object
            spark: SparkSession (optional, for Spark-based sync)
            environment: Environment tag
        """
        self.source = source_catalog
        self.config = sync_config
        self.target = target_connection
        self.spark = spark
        self.environment = environment
        self._ctx = get_logging_context()

        # Determine target type
        self.target_type = self._get_target_type()

    def _get_target_type(self) -> str:
        """Determine target connection type."""
        # Check various ways connections expose their type
        conn_type = None
        if hasattr(self.target, "connection_type"):
            conn_type = self.target.connection_type
        elif hasattr(self.target, "type"):
            conn_type = self.target.type
        else:
            # Check class name as fallback
            class_name = self.target.__class__.__name__.lower()
            if "sql" in class_name:
                conn_type = "sql_server"
            elif "adls" in class_name or "azure" in class_name or "blob" in class_name:
                conn_type = "azure_adls"
            elif "local" in class_name:
                conn_type = "local"

        if conn_type is None:
            conn_type = "unknown"

        # Normalize to sql_server or delta
        if conn_type in ("sql_server", "azure_sql", "AzureSQL"):
            return "sql_server"
        elif conn_type in ("azure_blob", "azure_adls", "adls", "local", "s3", "gcs"):
            return "delta"
        else:
            return "delta"  # Default to delta

    def get_tables_to_sync(self) -> List[str]:
        """Get list of tables to sync."""
        if self.config.tables:
            # Validate requested tables exist
            valid_tables = [t for t in self.config.tables if t in ALL_SYNC_TABLES]
            if len(valid_tables) != len(self.config.tables):
                invalid = set(self.config.tables) - set(valid_tables)
                self._ctx.warning(f"Unknown tables requested for sync: {invalid}")
            return valid_tables
        return DEFAULT_SYNC_TABLES

    def sync(self, tables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Sync catalog tables to target.

        Args:
            tables: Optional override of tables to sync

        Returns:
            Dict with sync results per table
        """
        tables_to_sync = tables or self.get_tables_to_sync()
        results = {}

        self._ctx.info(
            f"Starting catalog sync to {self.config.connection}",
            tables=tables_to_sync,
            mode=self.config.mode,
            target_type=self.target_type,
        )

        for table in tables_to_sync:
            try:
                if self.target_type == "sql_server":
                    result = self._sync_to_sql_server(table)
                else:
                    result = self._sync_to_delta(table)
                results[table] = result
            except Exception as e:
                self._ctx.warning(f"Failed to sync table {table}: {e}")
                results[table] = {"success": False, "error": str(e), "rows": 0}

        # Update last sync timestamp
        self._update_sync_state(results)

        success_count = sum(1 for r in results.values() if r.get("success"))
        total_rows = sum(r.get("rows", 0) for r in results.values())

        self._ctx.info(
            f"Catalog sync completed: {success_count}/{len(tables_to_sync)} tables",
            total_rows=total_rows,
        )

        return results

    def sync_async(self, tables: Optional[List[str]] = None) -> None:
        """Fire and forget sync - runs in background thread."""
        thread = threading.Thread(target=self.sync, args=(tables,), daemon=True)
        thread.start()

    def _sync_to_sql_server(self, table: str) -> Dict[str, Any]:
        """Sync a single table to SQL Server."""
        schema = self.config.schema_name or "odibi_system"

        # Ensure schema exists
        self._ensure_sql_schema(schema)

        # Ensure table exists
        self._ensure_sql_table(table, schema)

        # Read source data
        source_path = self.source.tables.get(table)
        if not source_path:
            return {"success": False, "error": f"Table {table} not found in source", "rows": 0}

        try:
            df = self._read_source_table(source_path)
            if df is None or (hasattr(df, "empty") and df.empty):
                return {"success": True, "rows": 0, "message": "No data to sync"}

            # Apply date filter for incremental mode
            if self.config.mode == "incremental":
                df = self._apply_incremental_filter(df, table)

            # Get row count before conversion
            row_count = len(df) if hasattr(df, "__len__") else df.count()
            if row_count == 0:
                return {"success": True, "rows": 0, "message": "No new data"}

            # Inject environment if not present or NULL in source
            if self.environment and "environment" in df.columns:
                df["environment"] = df["environment"].fillna(self.environment)
            elif self.environment:
                df["environment"] = self.environment

            # Convert to records and insert
            if self.config.mode == "full":
                # Truncate and reload
                self.target.execute(f"TRUNCATE TABLE [{schema}].[{table}]")

            records = self._df_to_records(df)
            self._insert_to_sql_server(table, schema, records)

            return {"success": True, "rows": row_count}

        except Exception as e:
            logger.exception(f"Error syncing {table} to SQL Server")
            return {"success": False, "error": str(e), "rows": 0}

    def _sync_to_delta(self, table: str) -> Dict[str, Any]:
        """Sync a single table to another Delta location."""
        source_path = self.source.tables.get(table)

        # Build target path - ensure it's absolute
        sync_path = self.config.path or "_odibi_system"
        if hasattr(self.target, "get_path"):
            base_path = self.target.get_path(sync_path)
        elif hasattr(self.target, "uri"):
            base_path = self.target.uri(sync_path)
        else:
            # Fallback - this shouldn't happen for blob connections
            base_path = sync_path

        target_path = f"{base_path}/{table}"

        # Validate path is absolute (abfss://, s3://, etc.)
        if not target_path.startswith(("abfss://", "s3://", "gs://", "az://", "/")):
            return {
                "success": False,
                "error": f"Target path is not absolute: {target_path}. Check sync_to connection.",
                "rows": 0,
            }

        if not source_path:
            return {"success": False, "error": f"Table {table} not found in source", "rows": 0}

        try:
            if self.spark:
                # Spark-based Delta sync
                df = self.spark.read.format("delta").load(source_path)

                if self.config.mode == "incremental":
                    df = self._apply_incremental_filter_spark(df, table)

                row_count = df.count()
                if row_count == 0:
                    return {"success": True, "rows": 0, "message": "No new data"}

                write_mode = "overwrite" if self.config.mode == "full" else "append"
                df.write.format("delta").mode(write_mode).save(target_path)

                return {"success": True, "rows": row_count}
            else:
                # Engine-based sync (Pandas/Polars)
                df = self._read_source_table(source_path)
                if df is None or df.empty:
                    return {"success": True, "rows": 0, "message": "No data"}

                if self.config.mode == "incremental":
                    df = self._apply_incremental_filter(df, table)

                row_count = len(df)
                if row_count == 0:
                    return {"success": True, "rows": 0, "message": "No new data"}

                # Write to target
                self.source.engine.write(
                    df,
                    connection=self.target,
                    format="delta",
                    path=target_path,
                    mode="overwrite" if self.config.mode == "full" else "append",
                )

                return {"success": True, "rows": row_count}

        except Exception as e:
            logger.exception(f"Error syncing {table} to Delta")
            return {"success": False, "error": str(e), "rows": 0}

    def _read_source_table(self, path: str) -> Any:
        """Read a table from source catalog."""
        if self.spark:
            try:
                return self.spark.read.format("delta").load(path).toPandas()
            except Exception:
                pass

        if self.source.engine:
            return self.source._read_local_table(path)

        return None

    def _apply_incremental_filter(self, df: Any, table: str) -> Any:
        """Filter DataFrame to only include new records for incremental sync."""
        # Get last sync timestamp
        last_sync = self._get_last_sync_timestamp(table)

        if last_sync and "timestamp" in df.columns:
            df = df[df["timestamp"] > last_sync]
        elif self.config.sync_last_days:
            cutoff = datetime.now(timezone.utc) - timedelta(days=self.config.sync_last_days)
            if "timestamp" in df.columns:
                df = df[df["timestamp"] > cutoff]
            elif "date" in df.columns:
                df = df[df["date"] > cutoff.date()]

        return df

    def _apply_incremental_filter_spark(self, df: Any, table: str) -> Any:
        """Filter Spark DataFrame for incremental sync."""
        from pyspark.sql.functions import col

        last_sync = self._get_last_sync_timestamp(table)

        if last_sync and "timestamp" in df.columns:
            df = df.filter(col("timestamp") > last_sync)
        elif self.config.sync_last_days:
            cutoff = datetime.now(timezone.utc) - timedelta(days=self.config.sync_last_days)
            if "timestamp" in df.columns:
                df = df.filter(col("timestamp") > cutoff)
            elif "date" in df.columns:
                df = df.filter(col("date") > cutoff.date())

        return df

    def _get_last_sync_timestamp(self, table: str) -> Optional[datetime]:
        """Get last successful sync timestamp for a table."""
        try:
            key = f"sync_to:{self.config.connection}:{table}:last_timestamp"
            value = self.source.get_state(key)
            if value:
                return datetime.fromisoformat(value)
        except Exception:
            pass
        return None

    def _update_sync_state(self, results: Dict[str, Any]) -> None:
        """Update sync state with last sync timestamps."""
        now = datetime.now(timezone.utc).isoformat()
        for table, result in results.items():
            if result.get("success"):
                key = f"sync_to:{self.config.connection}:{table}:last_timestamp"
                try:
                    self.source.set_state(key, now)
                except Exception as e:
                    logger.debug(f"Failed to update sync state for {table}: {e}")

    def _ensure_sql_schema(self, schema: str) -> None:
        """Create SQL Server schema if it doesn't exist."""
        try:
            ddl = f"""
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')
            BEGIN
                EXEC('CREATE SCHEMA [{schema}]')
            END
            """
            self.target.execute(ddl)
        except Exception as e:
            logger.debug(f"Schema creation note: {e}")

    def _ensure_sql_table(self, table: str, schema: str) -> None:
        """Create SQL Server table if it doesn't exist."""
        ddl_template = SQL_SERVER_DDL.get(table)
        if ddl_template:
            try:
                ddl = ddl_template.format(schema=schema)
                self.target.execute(ddl)
            except Exception as e:
                logger.debug(f"Table creation note for {table}: {e}")

    def _df_to_records(self, df: Any) -> List[Dict[str, Any]]:
        """Convert DataFrame to list of records."""
        if hasattr(df, "to_dict"):
            return df.to_dict("records")
        elif hasattr(df, "to_dicts"):
            return df.to_dicts()
        return []

    def _insert_to_sql_server(self, table: str, schema: str, records: List[Dict[str, Any]]) -> None:
        """Insert records to SQL Server table."""
        if not records:
            return

        # Get column names from first record
        columns = list(records[0].keys())
        placeholders = ", ".join([f":{col}" for col in columns])
        column_list = ", ".join([f"[{col}]" for col in columns])

        sql = f"INSERT INTO [{schema}].[{table}] ({column_list}) VALUES ({placeholders})"

        # Batch insert
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            for record in batch:
                # Convert values to SQL-safe format
                safe_record = {}
                for k, v in record.items():
                    if isinstance(v, (dict, list)):
                        safe_record[k] = json.dumps(v, default=str)
                    elif isinstance(v, datetime):
                        safe_record[k] = v.isoformat()
                    else:
                        safe_record[k] = v
                try:
                    self.target.execute(sql, safe_record)
                except Exception as e:
                    logger.debug(f"Insert error for record: {e}")

    def purge_sql_tables(self, days: int = 90) -> Dict[str, Any]:
        """
        Purge old records from SQL Server sync tables.

        Args:
            days: Delete records older than this many days (default: 90)

        Returns:
            Dict with purge results per table
        """
        if self.target_type != "sql_server":
            return {"error": "Purge only supported for SQL Server targets"}

        schema = self.config.schema_name or "odibi_system"
        results = {}

        # Tables with date columns for purging
        purgeable_tables = {
            "meta_runs": "date",
            "meta_pipeline_runs": "date",
            "meta_node_runs": "date",
            "meta_failures": "date",
        }

        tables_to_purge = self.config.tables or list(purgeable_tables.keys())

        for table in tables_to_purge:
            date_col = purgeable_tables.get(table)
            if not date_col:
                results[table] = {"success": False, "error": "No date column for purge"}
                continue

            try:
                sql = f"""
                DELETE FROM [{schema}].[{table}]
                WHERE [{date_col}] < DATEADD(day, -{days}, GETDATE())
                """
                self.target.execute(sql)
                results[table] = {"success": True, "days_retained": days}
                self._ctx.info(f"Purged {table} (records older than {days} days)")
            except Exception as e:
                results[table] = {"success": False, "error": str(e)}
                self._ctx.warning(f"Failed to purge {table}: {e}")

        return results
