"""
DerivedUpdater: Claim lifecycle for Leverage Summary Tables.

Provides exactly-once semantics for derived table updates via a guard table
(meta_derived_applied_runs). Supports Spark, Pandas/delta-rs, and SQL Server.

Invariants:
- try_claim, mark_applied, mark_failed are FAIL-FAST (do NOT swallow exceptions)
- log_observability_error is raw append only, swallows exceptions, NEVER uses guard
"""

import logging
import os
import random
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

if TYPE_CHECKING:
    from odibi.catalog import CatalogManager

logger = logging.getLogger(__name__)

# Suppress noisy delta-rs transaction conflict warnings (handled by retry)
if "RUST_LOG" not in os.environ:
    os.environ["RUST_LOG"] = "deltalake_core::kernel::transaction=error"

# Try to import deltalake, but don't fail yet (it might be a Spark run)
try:
    import pandas as pd
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake
except ImportError:
    DeltaTable = None
    write_deltalake = None
    pd = None
    pa = None

# Valid derived tables for apply_derived_update
VALID_DERIVED_TABLES = {"meta_daily_stats", "meta_pipeline_health", "meta_sla_status"}

# Default stale claim threshold (minutes) for rebuild reclaim eligibility
MAX_CLAIM_AGE_MINUTES = 60


def sql_escape(s: Optional[str]) -> str:
    """Escape single quotes for SQL interpolation."""
    if s is None:
        return ""
    return str(s).replace("'", "''")


def _sql_nullable_float(v: Optional[float]) -> str:
    """Format optional float for SQL (NULL or value)."""
    return "NULL" if v is None else str(float(v))


def _sql_nullable_int(v: Optional[int]) -> str:
    """Format optional int for SQL (NULL or value)."""
    return "NULL" if v is None else str(int(v))


def parse_duration_to_minutes(duration: str) -> int:
    """Parse duration string (e.g., '6h', '30m', '1d') to minutes."""
    if not duration:
        return 0
    match = re.match(r"^(\d+)([mhdw])$", duration.lower().strip())
    if not match:
        return 0
    value = int(match.group(1))
    unit = match.group(2)
    if unit == "m":
        return value
    elif unit == "h":
        return value * 60
    elif unit == "d":
        return value * 60 * 24
    elif unit == "w":
        return value * 60 * 24 * 7
    return 0


def _retry_delta_operation(func, max_retries: int = 5, base_delay: float = 1.0):
    """Retry a Delta operation with exponential backoff on concurrency conflicts.

    Only logs debug during retries. Raises after all retries fail.
    """
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            error_str = str(e)
            is_concurrent = any(
                msg in error_str
                for msg in [
                    "ConcurrentAppendException",
                    "ConcurrentDeleteReadException",
                    "ConcurrentDeleteDeleteException",
                    "DELTA_CONCURRENT",
                    "concurrent",
                    "conflict",
                ]
            )
            if not is_concurrent or attempt >= max_retries:
                raise
            delay = base_delay * (2**attempt) + random.uniform(0, 1.0)
            logger.debug(
                f"Delta concurrent write (attempt {attempt + 1}/{max_retries + 1}), "
                f"retrying in {delay:.2f}s..."
            )
            time.sleep(delay)


class DerivedUpdater:
    """
    Manages claim lifecycle for derived table updates.

    Provides idempotency guard via meta_derived_applied_runs table.
    Dispatches to Spark, Pandas/delta-rs, or SQL Server based on CatalogManager mode.
    """

    def __init__(self, catalog: "CatalogManager"):
        """
        Initialize DerivedUpdater.

        Args:
            catalog: CatalogManager instance for engine detection and table paths.
        """
        self.catalog = catalog
        self._guard_path = catalog.tables["meta_derived_applied_runs"]
        self._errors_path = catalog.tables["meta_observability_errors"]

    def try_claim(self, derived_table: str, run_id: str) -> Optional[str]:
        """
        Attempt to claim a derived table update for a run.

        Returns claim_token if successful, None if already claimed/applied/failed.
        FAIL-FAST: raises on errors (caller will wrap).

        Args:
            derived_table: Name of derived table (e.g., 'meta_daily_stats')
            run_id: Pipeline run ID

        Returns:
            claim_token (UUID string) if claim succeeded, None if already claimed
        """
        if self.catalog.is_spark_mode:
            return self._try_claim_spark(derived_table, run_id)
        elif self.catalog.is_pandas_mode:
            return self._try_claim_pandas(derived_table, run_id)
        elif self.catalog.is_sql_server_mode:
            return self._try_claim_sql_server(derived_table, run_id)
        else:
            raise RuntimeError("No supported backend available for DerivedUpdater")

    def mark_applied(self, derived_table: str, run_id: str, claim_token: str) -> None:
        """
        Mark a claimed update as successfully applied.

        FAIL-FAST: raises on errors (caller will wrap).

        Args:
            derived_table: Name of derived table
            run_id: Pipeline run ID
            claim_token: Token returned by try_claim (enforces ownership)
        """
        if self.catalog.is_spark_mode:
            self._mark_applied_spark(derived_table, run_id, claim_token)
        elif self.catalog.is_pandas_mode:
            self._mark_applied_pandas(derived_table, run_id, claim_token)
        elif self.catalog.is_sql_server_mode:
            self._mark_applied_sql_server(derived_table, run_id, claim_token)
        else:
            raise RuntimeError("No supported backend available for DerivedUpdater")

    def mark_failed(
        self, derived_table: str, run_id: str, claim_token: str, error_message: str
    ) -> None:
        """
        Mark a claimed update as failed.

        FAIL-FAST: raises on errors (caller will wrap).

        Args:
            derived_table: Name of derived table
            run_id: Pipeline run ID
            claim_token: Token returned by try_claim (enforces ownership)
            error_message: Error description (truncated to 500 chars)
        """
        error_msg = error_message[:500] if error_message else None
        if self.catalog.is_spark_mode:
            self._mark_failed_spark(derived_table, run_id, claim_token, error_msg)
        elif self.catalog.is_pandas_mode:
            self._mark_failed_pandas(derived_table, run_id, claim_token, error_msg)
        elif self.catalog.is_sql_server_mode:
            self._mark_failed_sql_server(derived_table, run_id, claim_token, error_msg)
        else:
            raise RuntimeError("No supported backend available for DerivedUpdater")

    def reclaim_for_rebuild(
        self,
        derived_table: str,
        run_id: str,
        max_age_minutes: int = MAX_CLAIM_AGE_MINUTES,
    ) -> Optional[str]:
        """
        Attempt to reclaim a derived table update for rebuild.

        Guard semantics:
        - APPLIED: return None (never reclaim - terminal state)
        - FAILED: CAS UPDATE to CLAIMED with new token, return token if ownership verified
        - CLAIMED older than max_age_minutes: CAS UPDATE to CLAIMED with new token, return token
        - No row exists: fall back to try_claim (insert-only)

        Uses CAS (compare-and-swap) UPDATE semantics, NEVER delete+insert.
        FAIL-FAST: raises on errors (caller must wrap).

        Args:
            derived_table: Name of derived table (e.g., 'meta_daily_stats')
            run_id: Pipeline run ID
            max_age_minutes: Maximum age in minutes for stale CLAIMED entries

        Returns:
            claim_token (UUID string) if reclaim succeeded, None if APPLIED or not reclaimable
        """
        if self.catalog.is_spark_mode:
            return self._reclaim_for_rebuild_spark(derived_table, run_id, max_age_minutes)
        elif self.catalog.is_pandas_mode:
            return self._reclaim_for_rebuild_pandas(derived_table, run_id, max_age_minutes)
        elif self.catalog.is_sql_server_mode:
            return self._reclaim_for_rebuild_sql_server(derived_table, run_id, max_age_minutes)
        else:
            raise RuntimeError("No supported backend available for DerivedUpdater")

    def log_observability_error(
        self,
        component: str,
        error_message: str,
        run_id: Optional[str] = None,
        pipeline_name: Optional[str] = None,
    ) -> None:
        """
        Log an observability system error.

        Raw append only, swallows ALL exceptions, NEVER uses guard or calls
        other derived updater methods (no recursion risk).

        Args:
            component: Component that failed (e.g., 'derived_updates', 'billing_query')
            error_message: Error description (truncated to 500 chars)
            run_id: Optional pipeline run ID
            pipeline_name: Optional pipeline name
        """
        try:
            error_msg = error_message[:500] if error_message else ""
            error_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc)

            if self.catalog.is_spark_mode:
                self._log_error_spark(error_id, run_id, pipeline_name, component, error_msg, now)
            elif self.catalog.is_pandas_mode:
                self._log_error_pandas(error_id, run_id, pipeline_name, component, error_msg, now)
            elif self.catalog.is_sql_server_mode:
                self._log_error_sql_server(
                    error_id, run_id, pipeline_name, component, error_msg, now
                )
            # If no backend available, silently skip (observability never fails)
        except Exception:
            # Swallow ALL exceptions - observability errors must never propagate
            pass

    # =========================================================================
    # SPARK IMPLEMENTATIONS
    # =========================================================================

    def _try_claim_spark(self, derived_table: str, run_id: str) -> Optional[str]:
        """Spark: MERGE insert-only + token verify."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType, StructField, StructType, TimestampType

        spark = self.catalog.spark
        claim_token = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        schema = StructType(
            [
                StructField("derived_table", StringType(), False),
                StructField("run_id", StringType(), False),
                StructField("claim_token", StringType(), False),
                StructField("status", StringType(), False),
                StructField("claimed_at", TimestampType(), False),
                StructField("applied_at", TimestampType(), True),
                StructField("error_message", StringType(), True),
            ]
        )

        new_row = spark.createDataFrame(
            [(derived_table, run_id, claim_token, "CLAIMED", now, None, None)], schema
        )
        new_row.createOrReplaceTempView("_odibi_claim_source")

        merge_sql = f"""
            MERGE INTO delta.`{self._guard_path}` AS target
            USING _odibi_claim_source AS source
            ON target.derived_table = source.derived_table AND target.run_id = source.run_id
            WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(merge_sql)
        spark.catalog.dropTempView("_odibi_claim_source")

        # Verify our token won
        verify_df = (
            spark.read.format("delta")
            .load(self._guard_path)
            .filter(
                (F.col("derived_table") == derived_table)
                & (F.col("run_id") == run_id)
                & (F.col("claim_token") == claim_token)
            )
        )
        if verify_df.count() > 0:
            logger.debug(f"Claimed {derived_table}/{run_id} with token {claim_token}")
            return claim_token
        else:
            logger.debug(f"Failed to claim {derived_table}/{run_id} - already claimed")
            return None

    def _mark_applied_spark(self, derived_table: str, run_id: str, claim_token: str) -> None:
        """Spark: UPDATE by key + token."""
        spark = self.catalog.spark
        now = datetime.now(timezone.utc)

        update_sql = f"""
            UPDATE delta.`{self._guard_path}`
            SET status = 'APPLIED', applied_at = timestamp'{now.isoformat()}'
            WHERE derived_table = '{derived_table}'
              AND run_id = '{run_id}'
              AND claim_token = '{claim_token}'
              AND status = 'CLAIMED'
        """
        spark.sql(update_sql)
        logger.debug(f"Marked {derived_table}/{run_id} as APPLIED")

    def _mark_failed_spark(
        self, derived_table: str, run_id: str, claim_token: str, error_message: Optional[str]
    ) -> None:
        """Spark: UPDATE by key + token."""
        spark = self.catalog.spark
        now = datetime.now(timezone.utc)
        escaped_msg = (error_message or "").replace("'", "''")

        update_sql = f"""
            UPDATE delta.`{self._guard_path}`
            SET status = 'FAILED', applied_at = timestamp'{now.isoformat()}', error_message = '{escaped_msg}'
            WHERE derived_table = '{derived_table}'
              AND run_id = '{run_id}'
              AND claim_token = '{claim_token}'
              AND status = 'CLAIMED'
        """
        spark.sql(update_sql)
        logger.debug(f"Marked {derived_table}/{run_id} as FAILED")

    def _reclaim_for_rebuild_spark(
        self, derived_table: str, run_id: str, max_age_minutes: int
    ) -> Optional[str]:
        """Spark: CAS UPDATE for reclaim, fall back to try_claim if no row."""
        from pyspark.sql import functions as F

        spark = self.catalog.spark
        claim_token = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        stale_cutoff = now - timedelta(minutes=max_age_minutes)

        existing = (
            spark.read.format("delta")
            .load(self._guard_path)
            .filter((F.col("derived_table") == derived_table) & (F.col("run_id") == run_id))
        )

        if existing.count() == 0:
            return self.try_claim(derived_table, run_id)

        row = existing.collect()[0]
        status = row["status"]

        if status == "APPLIED":
            logger.debug(f"Cannot reclaim {derived_table}/{run_id} - APPLIED is terminal")
            return None

        if status == "FAILED":
            update_sql = f"""
                UPDATE delta.`{self._guard_path}`
                SET claim_token = '{claim_token}',
                    status = 'CLAIMED',
                    claimed_at = timestamp'{now.isoformat()}',
                    applied_at = NULL,
                    error_message = NULL
                WHERE derived_table = '{derived_table}'
                  AND run_id = '{run_id}'
                  AND status = 'FAILED'
            """
            spark.sql(update_sql)
        elif status == "CLAIMED":
            claimed_at = row["claimed_at"]
            if claimed_at is None or claimed_at >= stale_cutoff:
                logger.debug(f"Cannot reclaim {derived_table}/{run_id} - CLAIMED is not stale")
                return None
            update_sql = f"""
                UPDATE delta.`{self._guard_path}`
                SET claim_token = '{claim_token}',
                    claimed_at = timestamp'{now.isoformat()}',
                    applied_at = NULL,
                    error_message = NULL
                WHERE derived_table = '{derived_table}'
                  AND run_id = '{run_id}'
                  AND status = 'CLAIMED'
                  AND claimed_at < timestamp'{stale_cutoff.isoformat()}'
            """
            spark.sql(update_sql)
        else:
            logger.debug(f"Unknown status {status} for {derived_table}/{run_id}")
            return None

        verify_df = (
            spark.read.format("delta")
            .load(self._guard_path)
            .filter(
                (F.col("derived_table") == derived_table)
                & (F.col("run_id") == run_id)
                & (F.col("claim_token") == claim_token)
            )
        )
        if verify_df.count() > 0:
            logger.debug(f"Reclaimed {derived_table}/{run_id} with token {claim_token}")
            return claim_token
        else:
            logger.debug(f"Failed to reclaim {derived_table}/{run_id} - CAS failed")
            return None

    def _log_error_spark(
        self,
        error_id: str,
        run_id: Optional[str],
        pipeline_name: Optional[str],
        component: str,
        error_message: str,
        timestamp: datetime,
    ) -> None:
        """Spark: append to meta_observability_errors."""
        from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

        spark = self.catalog.spark
        schema = StructType(
            [
                StructField("error_id", StringType(), False),
                StructField("run_id", StringType(), True),
                StructField("pipeline_name", StringType(), True),
                StructField("component", StringType(), False),
                StructField("error_message", StringType(), True),
                StructField("timestamp", TimestampType(), False),
                StructField("date", DateType(), False),
            ]
        )
        row = spark.createDataFrame(
            [
                (
                    error_id,
                    run_id,
                    pipeline_name,
                    component,
                    error_message,
                    timestamp,
                    timestamp.date(),
                )
            ],
            schema,
        )
        row.write.format("delta").mode("append").save(self._errors_path)

    # =========================================================================
    # PANDAS/DELTA-RS IMPLEMENTATIONS
    # =========================================================================

    def _try_claim_pandas(self, derived_table: str, run_id: str) -> Optional[str]:
        """Pandas/delta-rs: append claim + reread verify."""
        if not DeltaTable or not pd or not pa or not write_deltalake:
            raise ImportError("deltalake library required for pandas mode claim lifecycle")

        claim_token = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        storage_opts = self.catalog._get_storage_options()

        # Read existing guard table
        try:
            dt = DeltaTable(self._guard_path, storage_options=storage_opts or None)
            existing_df = dt.to_pandas()
        except Exception:
            existing_df = pd.DataFrame(
                columns=[
                    "derived_table",
                    "run_id",
                    "claim_token",
                    "status",
                    "claimed_at",
                    "applied_at",
                    "error_message",
                ]
            )

        # Check if already claimed
        mask = (existing_df["derived_table"] == derived_table) & (existing_df["run_id"] == run_id)
        if mask.any():
            logger.debug(f"Already claimed: {derived_table}/{run_id}")
            return None

        # Append new claim row with explicit Arrow schema for type compatibility
        new_row = pa.table(
            {
                "derived_table": [derived_table],
                "run_id": [run_id],
                "claim_token": [claim_token],
                "status": ["CLAIMED"],
                "claimed_at": pa.array([now], type=pa.timestamp("us", tz="UTC")),
                "applied_at": pa.array([None], type=pa.timestamp("us", tz="UTC")),
                "error_message": pa.array([None], type=pa.string()),
            }
        )

        def _do_append():
            write_deltalake(
                self._guard_path,
                new_row,
                mode="append",
                storage_options=storage_opts or None,
            )

        _retry_delta_operation(_do_append)

        # Re-read and verify our token won (handle race condition)
        try:
            dt = DeltaTable(self._guard_path, storage_options=storage_opts or None)
            verify_df = dt.to_pandas()
            mask = (
                (verify_df["derived_table"] == derived_table)
                & (verify_df["run_id"] == run_id)
                & (verify_df["claim_token"] == claim_token)
            )
            if mask.any():
                logger.debug(f"Claimed {derived_table}/{run_id} with token {claim_token}")
                return claim_token
            else:
                logger.debug(f"Lost race for {derived_table}/{run_id}")
                return None
        except Exception:
            # If we can't verify, assume claim failed
            return None

    def _mark_applied_pandas(self, derived_table: str, run_id: str, claim_token: str) -> None:
        """Pandas/delta-rs: read entire table, modify row, overwrite."""
        self._update_guard_row_pandas(
            derived_table, run_id, claim_token, new_status="APPLIED", error_message=None
        )

    def _mark_failed_pandas(
        self, derived_table: str, run_id: str, claim_token: str, error_message: Optional[str]
    ) -> None:
        """Pandas/delta-rs: read entire table, modify row, overwrite."""
        self._update_guard_row_pandas(
            derived_table, run_id, claim_token, new_status="FAILED", error_message=error_message
        )

    def _update_guard_row_pandas(
        self,
        derived_table: str,
        run_id: str,
        claim_token: str,
        new_status: str,
        error_message: Optional[str],
    ) -> None:
        """Helper: read-modify-write for guard table updates in pandas mode."""
        if not DeltaTable or not pd or not pa or not write_deltalake:
            raise ImportError("deltalake library required for pandas mode claim lifecycle")

        storage_opts = self.catalog._get_storage_options()
        now = datetime.now(timezone.utc)

        dt = DeltaTable(self._guard_path, storage_options=storage_opts or None)
        df = dt.to_pandas()

        mask = (
            (df["derived_table"] == derived_table)
            & (df["run_id"] == run_id)
            & (df["claim_token"] == claim_token)
            & (df["status"] == "CLAIMED")
        )
        if not mask.any():
            raise ValueError(
                f"Cannot update {derived_table}/{run_id}: not found with token {claim_token} in CLAIMED status"
            )

        df.loc[mask, "status"] = new_status
        df.loc[mask, "applied_at"] = now
        if error_message is not None:
            df.loc[mask, "error_message"] = error_message

        def _do_overwrite():
            write_deltalake(
                self._guard_path,
                df,
                mode="overwrite",
                schema_mode="overwrite",
                storage_options=storage_opts or None,
            )

        _retry_delta_operation(_do_overwrite)
        logger.debug(f"Marked {derived_table}/{run_id} as {new_status}")

    def _reclaim_for_rebuild_pandas(
        self, derived_table: str, run_id: str, max_age_minutes: int
    ) -> Optional[str]:
        """Pandas/delta-rs: read-modify-write CAS for reclaim, fall back to try_claim if no row."""
        if not DeltaTable or not pd or not pa or not write_deltalake:
            raise ImportError("deltalake library required for pandas mode claim lifecycle")

        claim_token = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        stale_cutoff = now - timedelta(minutes=max_age_minutes)
        storage_opts = self.catalog._get_storage_options()

        try:
            dt = DeltaTable(self._guard_path, storage_options=storage_opts or None)
            df = dt.to_pandas()
        except Exception:
            return self.try_claim(derived_table, run_id)

        mask = (df["derived_table"] == derived_table) & (df["run_id"] == run_id)
        if not mask.any():
            return self.try_claim(derived_table, run_id)

        row = df[mask].iloc[0]
        status = row["status"]

        if status == "APPLIED":
            logger.debug(f"Cannot reclaim {derived_table}/{run_id} - APPLIED is terminal")
            return None

        if status == "FAILED":
            df.loc[mask, "claim_token"] = claim_token
            df.loc[mask, "status"] = "CLAIMED"
            df.loc[mask, "claimed_at"] = now
            df.loc[mask, "applied_at"] = pd.NaT
            df.loc[mask, "error_message"] = None
        elif status == "CLAIMED":
            claimed_at = row["claimed_at"]
            if pd.isna(claimed_at):
                logger.debug(f"Cannot reclaim {derived_table}/{run_id} - claimed_at is null")
                return None
            if hasattr(claimed_at, "to_pydatetime"):
                claimed_at = claimed_at.to_pydatetime()
            if claimed_at.tzinfo is None:
                claimed_at = claimed_at.replace(tzinfo=timezone.utc)
            if claimed_at >= stale_cutoff:
                logger.debug(f"Cannot reclaim {derived_table}/{run_id} - CLAIMED is not stale")
                return None
            df.loc[mask, "claim_token"] = claim_token
            df.loc[mask, "claimed_at"] = now
            df.loc[mask, "applied_at"] = pd.NaT
            df.loc[mask, "error_message"] = None
        else:
            logger.debug(f"Unknown status {status} for {derived_table}/{run_id}")
            return None

        def _do_overwrite():
            write_deltalake(
                self._guard_path,
                df,
                mode="overwrite",
                schema_mode="overwrite",
                storage_options=storage_opts or None,
            )

        _retry_delta_operation(_do_overwrite)

        try:
            dt = DeltaTable(self._guard_path, storage_options=storage_opts or None)
            verify_df = dt.to_pandas()
            verify_mask = (
                (verify_df["derived_table"] == derived_table)
                & (verify_df["run_id"] == run_id)
                & (verify_df["claim_token"] == claim_token)
            )
            if verify_mask.any():
                logger.debug(f"Reclaimed {derived_table}/{run_id} with token {claim_token}")
                return claim_token
            else:
                logger.debug(f"Failed to reclaim {derived_table}/{run_id} - CAS failed")
                return None
        except Exception:
            return None

    def _log_error_pandas(
        self,
        error_id: str,
        run_id: Optional[str],
        pipeline_name: Optional[str],
        component: str,
        error_message: str,
        timestamp: datetime,
    ) -> None:
        """Pandas/delta-rs: append to meta_observability_errors."""
        if not pa or not write_deltalake:
            return  # Silently skip if library not available

        storage_opts = self.catalog._get_storage_options()
        # Use PyArrow table with explicit schema for type compatibility
        row = pa.table(
            {
                "error_id": [error_id],
                "run_id": pa.array([run_id], type=pa.string()),
                "pipeline_name": pa.array([pipeline_name], type=pa.string()),
                "component": [component],
                "error_message": pa.array([error_message], type=pa.string()),
                "timestamp": pa.array([timestamp], type=pa.timestamp("us", tz="UTC")),
                "date": pa.array([timestamp.date()], type=pa.date32()),
            }
        )

        def _do_append():
            write_deltalake(
                self._errors_path,
                row,
                mode="append",
                storage_options=storage_opts or None,
            )

        _retry_delta_operation(_do_append)

    # =========================================================================
    # SQL SERVER IMPLEMENTATIONS
    # =========================================================================

    def _get_sql_server_connection(self) -> Any:
        """Get SQL Server connection from catalog."""
        if not self.catalog.connection:
            raise RuntimeError("SQL Server mode requires connection in CatalogManager")
        return self.catalog.connection

    def _get_sql_server_schema(self) -> str:
        """Get SQL Server schema name."""
        # Use system config schema_name if available, else default
        if hasattr(self.catalog.config, "schema_name") and self.catalog.config.schema_name:
            return self.catalog.config.schema_name
        return "odibi_system"

    def _sql_server_table_exists(self, table_name: str) -> bool:
        """Check if a SQL Server table exists in the schema."""
        conn = self._get_sql_server_connection()
        schema = self._get_sql_server_schema()
        sql = f"""
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = '{table_name}' AND s.name = '{schema}'
        """
        try:
            result = conn.execute(sql)
            return bool(result)
        except Exception:
            return False

    def _try_claim_sql_server(self, derived_table: str, run_id: str) -> Optional[str]:
        """SQL Server: MERGE insert-only + token verify."""
        if not self._sql_server_table_exists("meta_derived_applied_runs"):
            raise NotImplementedError(
                "meta_derived_applied_runs table does not exist in SQL Server. "
                "Add DDL in the SQL Server backend phase before using claim lifecycle."
            )

        conn = self._get_sql_server_connection()
        schema = self._get_sql_server_schema()
        claim_token = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        merge_sql = f"""
            MERGE [{schema}].[meta_derived_applied_runs] AS target
            USING (SELECT :derived_table AS derived_table, :run_id AS run_id) AS source
            ON target.derived_table = source.derived_table AND target.run_id = source.run_id
            WHEN NOT MATCHED THEN
                INSERT (derived_table, run_id, claim_token, status, claimed_at, applied_at, error_message)
                VALUES (:derived_table, :run_id, :claim_token, 'CLAIMED', :now, NULL, NULL);
        """
        conn.execute(
            merge_sql,
            {
                "derived_table": derived_table,
                "run_id": run_id,
                "claim_token": claim_token,
                "now": now,
            },
        )

        # Verify our token won
        verify_sql = f"""
            SELECT claim_token FROM [{schema}].[meta_derived_applied_runs]
            WHERE derived_table = :derived_table AND run_id = :run_id
        """
        result = conn.execute(verify_sql, {"derived_table": derived_table, "run_id": run_id})
        if result and result[0][0] == claim_token:
            logger.debug(f"Claimed {derived_table}/{run_id} with token {claim_token}")
            return claim_token
        else:
            logger.debug(f"Failed to claim {derived_table}/{run_id} - already claimed")
            return None

    def _mark_applied_sql_server(self, derived_table: str, run_id: str, claim_token: str) -> None:
        """SQL Server: UPDATE by key + token."""
        if not self._sql_server_table_exists("meta_derived_applied_runs"):
            raise NotImplementedError(
                "meta_derived_applied_runs table does not exist in SQL Server. "
                "Add DDL in the SQL Server backend phase."
            )

        conn = self._get_sql_server_connection()
        schema = self._get_sql_server_schema()
        now = datetime.now(timezone.utc)

        update_sql = f"""
            UPDATE [{schema}].[meta_derived_applied_runs]
            SET status = 'APPLIED', applied_at = :now
            WHERE derived_table = :derived_table
              AND run_id = :run_id
              AND claim_token = :claim_token
              AND status = 'CLAIMED'
        """
        conn.execute(
            update_sql,
            {
                "derived_table": derived_table,
                "run_id": run_id,
                "claim_token": claim_token,
                "now": now,
            },
        )
        logger.debug(f"Marked {derived_table}/{run_id} as APPLIED")

    def _mark_failed_sql_server(
        self, derived_table: str, run_id: str, claim_token: str, error_message: Optional[str]
    ) -> None:
        """SQL Server: UPDATE by key + token."""
        if not self._sql_server_table_exists("meta_derived_applied_runs"):
            raise NotImplementedError(
                "meta_derived_applied_runs table does not exist in SQL Server. "
                "Add DDL in the SQL Server backend phase."
            )

        conn = self._get_sql_server_connection()
        schema = self._get_sql_server_schema()
        now = datetime.now(timezone.utc)

        update_sql = f"""
            UPDATE [{schema}].[meta_derived_applied_runs]
            SET status = 'FAILED', applied_at = :now, error_message = :error_message
            WHERE derived_table = :derived_table
              AND run_id = :run_id
              AND claim_token = :claim_token
              AND status = 'CLAIMED'
        """
        conn.execute(
            update_sql,
            {
                "derived_table": derived_table,
                "run_id": run_id,
                "claim_token": claim_token,
                "now": now,
                "error_message": error_message,
            },
        )
        logger.debug(f"Marked {derived_table}/{run_id} as FAILED")

    def _reclaim_for_rebuild_sql_server(
        self, derived_table: str, run_id: str, max_age_minutes: int
    ) -> Optional[str]:
        """SQL Server: CAS UPDATE for reclaim, fall back to try_claim if no row."""
        if not self._sql_server_table_exists("meta_derived_applied_runs"):
            raise NotImplementedError(
                "meta_derived_applied_runs table does not exist in SQL Server. "
                "Add DDL in the SQL Server backend phase before using reclaim."
            )

        conn = self._get_sql_server_connection()
        schema = self._get_sql_server_schema()
        claim_token = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        select_sql = f"""
            SELECT status, claimed_at FROM [{schema}].[meta_derived_applied_runs]
            WHERE derived_table = :derived_table AND run_id = :run_id
        """
        result = conn.execute(select_sql, {"derived_table": derived_table, "run_id": run_id})
        rows = list(result) if result else []

        if not rows:
            return self.try_claim(derived_table, run_id)

        status = rows[0][0]
        # claimed_at check is done in SQL WHERE clause via DATEADD

        if status == "APPLIED":
            logger.debug(f"Cannot reclaim {derived_table}/{run_id} - APPLIED is terminal")
            return None

        if status == "FAILED":
            update_sql = f"""
                UPDATE [{schema}].[meta_derived_applied_runs]
                SET claim_token = :claim_token,
                    status = 'CLAIMED',
                    claimed_at = :now,
                    applied_at = NULL,
                    error_message = NULL
                WHERE derived_table = :derived_table
                  AND run_id = :run_id
                  AND status = 'FAILED'
            """
            conn.execute(
                update_sql,
                {
                    "derived_table": derived_table,
                    "run_id": run_id,
                    "claim_token": claim_token,
                    "now": now,
                },
            )
        elif status == "CLAIMED":
            update_sql = f"""
                UPDATE [{schema}].[meta_derived_applied_runs]
                SET claim_token = :claim_token,
                    claimed_at = :now,
                    applied_at = NULL,
                    error_message = NULL
                WHERE derived_table = :derived_table
                  AND run_id = :run_id
                  AND status = 'CLAIMED'
                  AND claimed_at < DATEADD(MINUTE, -:max_age_minutes, GETUTCDATE())
            """
            conn.execute(
                update_sql,
                {
                    "derived_table": derived_table,
                    "run_id": run_id,
                    "claim_token": claim_token,
                    "now": now,
                    "max_age_minutes": max_age_minutes,
                },
            )
        else:
            logger.debug(f"Unknown status {status} for {derived_table}/{run_id}")
            return None

        verify_sql = f"""
            SELECT claim_token FROM [{schema}].[meta_derived_applied_runs]
            WHERE derived_table = :derived_table AND run_id = :run_id
        """
        result = conn.execute(verify_sql, {"derived_table": derived_table, "run_id": run_id})
        rows = list(result) if result else []
        if rows and rows[0][0] == claim_token:
            logger.debug(f"Reclaimed {derived_table}/{run_id} with token {claim_token}")
            return claim_token
        else:
            logger.debug(f"Failed to reclaim {derived_table}/{run_id} - CAS failed")
            return None

    def _log_error_sql_server(
        self,
        error_id: str,
        run_id: Optional[str],
        pipeline_name: Optional[str],
        component: str,
        error_message: str,
        timestamp: datetime,
    ) -> None:
        """SQL Server: append to meta_observability_errors."""
        if not self._sql_server_table_exists("meta_observability_errors"):
            # Silently skip - observability errors must not fail
            return

        conn = self._get_sql_server_connection()
        schema = self._get_sql_server_schema()

        insert_sql = f"""
            INSERT INTO [{schema}].[meta_observability_errors]
            (error_id, run_id, pipeline_name, component, error_message, timestamp, date)
            VALUES (:error_id, :run_id, :pipeline_name, :component, :error_message, :timestamp, :date)
        """
        conn.execute(
            insert_sql,
            {
                "error_id": error_id,
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "component": component,
                "error_message": error_message,
                "timestamp": timestamp,
                "date": timestamp.date(),
            },
        )

    # =========================================================================
    # DERIVED UPDATE METHODS (Phase 3)
    # =========================================================================

    def apply_derived_update(
        self, derived_table: str, run_id: str, update_fn: Callable[[], None]
    ) -> None:
        """
        Fail-safe wrapper for derived table updates using claim lifecycle.

        This is the ONLY entrypoint for derived updates. Never raises.

        Args:
            derived_table: Name of derived table (must be in VALID_DERIVED_TABLES)
            run_id: Pipeline run ID
            update_fn: Callable that performs the actual update
        """
        if derived_table not in VALID_DERIVED_TABLES:
            self.log_observability_error(
                "derived_update",
                f"Invalid derived_table: {derived_table}",
                run_id=run_id,
            )
            return

        try:
            token = self.try_claim(derived_table, run_id)
            if token is None:
                logger.debug(f"Skipping {derived_table}/{run_id} - already processed")
                return

            try:
                update_fn()
                self.mark_applied(derived_table, run_id, token)
            except Exception as e:
                self.mark_failed(derived_table, run_id, token, str(e))
                self.log_observability_error(
                    "derived_update",
                    f"{derived_table}: {e}",
                    run_id=run_id,
                )
        except Exception as e:
            self.log_observability_error(
                "derived_update",
                f"Claim lifecycle failed for {derived_table}: {e}",
                run_id=run_id,
            )

    def update_daily_stats(self, run_id: str, pipeline_run: Dict[str, Any]) -> None:
        """
        MERGE incremental deltas to meta_daily_stats.

        Dispatches by engine. FAIL-FAST (caller wraps via apply_derived_update).

        Args:
            run_id: Pipeline run ID
            pipeline_run: Dict with pipeline_name, run_end_at, duration_ms, status,
                          rows_processed, estimated_cost_usd, actual_cost_usd
        """
        if self.catalog.is_spark_mode:
            self._update_daily_stats_spark(run_id, pipeline_run)
        elif self.catalog.is_pandas_mode:
            self._update_daily_stats_pandas(run_id, pipeline_run)
        elif self.catalog.is_sql_server_mode:
            self._update_daily_stats_sql_server(run_id, pipeline_run)
        else:
            raise RuntimeError("No supported backend available for update_daily_stats")

    def update_pipeline_health(self, pipeline_run: Dict[str, Any]) -> None:
        """
        MERGE lifetime deltas + bounded 30d scan for window metrics to meta_pipeline_health.

        Dispatches by engine. FAIL-FAST (caller wraps via apply_derived_update).

        Args:
            pipeline_run: Dict with pipeline_name, owner, layer, status, duration_ms,
                          rows_processed, run_end_at
        """
        if self.catalog.is_spark_mode:
            self._update_pipeline_health_spark(pipeline_run)
        elif self.catalog.is_pandas_mode:
            self._update_pipeline_health_pandas(pipeline_run)
        elif self.catalog.is_sql_server_mode:
            self._update_pipeline_health_sql_server(pipeline_run)
        else:
            raise RuntimeError("No supported backend available for update_pipeline_health")

    def update_sla_status(
        self,
        pipeline_name: str,
        owner: Optional[str],
        freshness_sla: Optional[str],
        freshness_anchor: str,
    ) -> None:
        """
        MERGE freshness compliance to meta_sla_status.

        Dispatches by engine. FAIL-FAST (caller wraps via apply_derived_update).

        Args:
            pipeline_name: Pipeline name
            owner: Pipeline owner (nullable)
            freshness_sla: SLA string like '6h', '1d' (nullable - no-op if None)
            freshness_anchor: Anchor type ('run_completion', etc.)
        """
        if not freshness_sla:
            return

        if self.catalog.is_spark_mode:
            self._update_sla_status_spark(pipeline_name, owner, freshness_sla, freshness_anchor)
        elif self.catalog.is_pandas_mode:
            self._update_sla_status_pandas(pipeline_name, owner, freshness_sla, freshness_anchor)
        elif self.catalog.is_sql_server_mode:
            self._update_sla_status_sql_server(
                pipeline_name, owner, freshness_sla, freshness_anchor
            )
        else:
            raise RuntimeError("No supported backend available for update_sla_status")

    # =========================================================================
    # SPARK: DERIVED UPDATERS
    # =========================================================================

    def _update_daily_stats_spark(self, run_id: str, pipeline_run: Dict[str, Any]) -> None:
        """Spark: MERGE incremental deltas to meta_daily_stats."""
        spark = self.catalog.spark
        daily_stats_path = self.catalog.tables["meta_daily_stats"]

        pipeline_name = pipeline_run["pipeline_name"]
        run_end_at = pipeline_run["run_end_at"]
        run_date_iso = (
            run_end_at.date().isoformat() if hasattr(run_end_at, "date") else str(run_end_at)[:10]
        )
        duration_ms = int(pipeline_run["duration_ms"])
        status = pipeline_run["status"]
        rows_processed = int(pipeline_run.get("rows_processed") or 0)

        if duration_ms < 0:
            raise ValueError(f"Invalid duration_ms: {duration_ms}")
        if rows_processed < 0:
            raise ValueError(f"Invalid rows_processed: {rows_processed}")

        estimated_cost = pipeline_run.get("estimated_cost_usd")
        actual_cost = pipeline_run.get("actual_cost_usd")

        runs_delta = 1
        successes_delta = 1 if status == "SUCCESS" else 0
        failures_delta = 1 if status == "FAILURE" else 0

        if actual_cost is not None:
            cost_source = "databricks_billing"
        elif estimated_cost is not None:
            cost_source = "configured_rate"
        else:
            cost_source = "none"

        merge_sql = f"""
            MERGE INTO delta.`{daily_stats_path}` AS target
            USING (
                SELECT
                    '{sql_escape(pipeline_name)}' AS pipeline_name,
                    DATE '{run_date_iso}' AS date,
                    CAST({runs_delta} AS BIGINT) AS runs_delta,
                    CAST({successes_delta} AS BIGINT) AS successes_delta,
                    CAST({failures_delta} AS BIGINT) AS failures_delta,
                    CAST({rows_processed} AS BIGINT) AS rows_delta,
                    CAST({duration_ms} AS BIGINT) AS duration_delta,
                    CAST({_sql_nullable_float(estimated_cost)} AS DOUBLE) AS estimated_cost_delta,
                    CAST({_sql_nullable_float(actual_cost)} AS DOUBLE) AS actual_cost_delta,
                    '{sql_escape(cost_source)}' AS cost_source_new
            ) AS source
            ON target.pipeline_name = source.pipeline_name AND target.date = source.date
            WHEN MATCHED THEN UPDATE SET
                runs = COALESCE(target.runs, 0) + source.runs_delta,
                successes = COALESCE(target.successes, 0) + source.successes_delta,
                failures = COALESCE(target.failures, 0) + source.failures_delta,
                total_rows = COALESCE(target.total_rows, 0) + source.rows_delta,
                total_duration_ms = COALESCE(target.total_duration_ms, 0) + source.duration_delta,
                estimated_cost_usd = COALESCE(target.estimated_cost_usd, 0D) + COALESCE(source.estimated_cost_delta, 0D),
                actual_cost_usd = COALESCE(target.actual_cost_usd, 0D) + COALESCE(source.actual_cost_delta, 0D),
                cost_source = CASE
                    WHEN target.cost_source = source.cost_source_new THEN target.cost_source
                    WHEN target.cost_source = 'none' THEN source.cost_source_new
                    WHEN source.cost_source_new = 'none' THEN target.cost_source
                    ELSE 'mixed'
                END,
                cost_is_actual = CASE WHEN (target.actual_cost_usd IS NOT NULL) OR (source.actual_cost_delta IS NOT NULL) THEN 1 ELSE 0 END
            WHEN NOT MATCHED THEN INSERT (
                date, pipeline_name, runs, successes, failures, total_rows, total_duration_ms,
                estimated_cost_usd, actual_cost_usd, cost_source, cost_is_actual
            ) VALUES (
                source.date, source.pipeline_name, source.runs_delta, source.successes_delta,
                source.failures_delta, source.rows_delta, source.duration_delta,
                source.estimated_cost_delta, source.actual_cost_delta, source.cost_source_new,
                CASE WHEN source.actual_cost_delta IS NOT NULL THEN 1 ELSE 0 END
            )
        """
        spark.sql(merge_sql)
        logger.debug(f"Updated daily_stats for {pipeline_name}/{run_date_iso}")

    def _update_pipeline_health_spark(self, pipeline_run: Dict[str, Any]) -> None:
        """Spark: MERGE lifetime deltas + bounded 30d scan for window metrics."""
        spark = self.catalog.spark
        health_path = self.catalog.tables["meta_pipeline_health"]
        runs_path = self.catalog.tables["meta_pipeline_runs"]

        pipeline_name = pipeline_run["pipeline_name"]
        owner = pipeline_run.get("owner")
        layer = pipeline_run.get("layer")
        status = pipeline_run["status"]
        duration_ms = int(pipeline_run["duration_ms"])

        if duration_ms < 0:
            raise ValueError(f"Invalid duration_ms: {duration_ms}")

        runs_delta = 1
        success_delta = 1 if status == "SUCCESS" else 0
        fail_delta = 1 if status == "FAILURE" else 0

        owner_sql = f"'{sql_escape(owner)}'" if owner else "NULL"
        layer_sql = f"'{sql_escape(layer)}'" if layer else "NULL"

        merge_sql = f"""
            MERGE INTO delta.`{health_path}` AS target
            USING (
                SELECT
                    '{sql_escape(pipeline_name)}' AS pipeline_name,
                    {owner_sql} AS owner,
                    {layer_sql} AS layer,
                    CAST({runs_delta} AS BIGINT) AS runs_delta,
                    CAST({success_delta} AS BIGINT) AS success_delta,
                    CAST({fail_delta} AS BIGINT) AS fail_delta,
                    current_timestamp() AS last_run_at,
                    CASE WHEN {success_delta}=1 THEN current_timestamp() ELSE NULL END AS last_success_at,
                    CASE WHEN {fail_delta}=1 THEN current_timestamp() ELSE NULL END AS last_failure_at,
                    current_timestamp() AS updated_at
            ) AS source
            ON target.pipeline_name = source.pipeline_name
            WHEN MATCHED THEN UPDATE SET
                owner = source.owner,
                layer = source.layer,
                total_runs = COALESCE(target.total_runs, 0) + source.runs_delta,
                total_successes = COALESCE(target.total_successes, 0) + source.success_delta,
                total_failures = COALESCE(target.total_failures, 0) + source.fail_delta,
                last_run_at = source.last_run_at,
                last_success_at = COALESCE(source.last_success_at, target.last_success_at),
                last_failure_at = COALESCE(source.last_failure_at, target.last_failure_at),
                updated_at = source.updated_at
            WHEN NOT MATCHED THEN INSERT (
                pipeline_name, owner, layer,
                total_runs, total_successes, total_failures,
                success_rate_7d, success_rate_30d, avg_duration_ms_7d, total_rows_30d, estimated_cost_30d,
                last_success_at, last_failure_at, last_run_at, updated_at
            ) VALUES (
                source.pipeline_name, source.owner, source.layer,
                source.runs_delta, source.success_delta, source.fail_delta,
                NULL, NULL, NULL, NULL, NULL,
                source.last_success_at, source.last_failure_at, source.last_run_at, source.updated_at
            )
        """
        spark.sql(merge_sql)

        window_sql = f"""
            WITH base AS (
                SELECT run_end_at, status, duration_ms, COALESCE(rows_processed, 0) AS rows_processed
                FROM delta.`{runs_path}`
                WHERE pipeline_name = '{sql_escape(pipeline_name)}'
                  AND run_end_at >= current_timestamp() - INTERVAL 30 DAYS
            ),
            w7 AS (
                SELECT
                    COUNT(*) AS runs_7d,
                    SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS succ_7d,
                    AVG(duration_ms) AS avg_dur_7d
                FROM base WHERE run_end_at >= current_timestamp() - INTERVAL 7 DAYS
            ),
            w30 AS (
                SELECT
                    COUNT(*) AS runs_30d,
                    SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS succ_30d,
                    SUM(rows_processed) AS rows_30d
                FROM base
            )
            SELECT
                CASE WHEN w7.runs_7d=0 THEN NULL ELSE CAST(w7.succ_7d AS DOUBLE) / w7.runs_7d END AS success_rate_7d,
                CASE WHEN w30.runs_30d=0 THEN NULL ELSE CAST(w30.succ_30d AS DOUBLE) / w30.runs_30d END AS success_rate_30d,
                w7.avg_dur_7d AS avg_duration_ms_7d,
                w30.rows_30d AS total_rows_30d
            FROM w7 CROSS JOIN w30
        """
        win = spark.sql(window_sql).first()

        if win:
            sr7 = _sql_nullable_float(win.success_rate_7d)
            sr30 = _sql_nullable_float(win.success_rate_30d)
            avg_dur = _sql_nullable_float(win.avg_duration_ms_7d)
            rows_30d = _sql_nullable_int(win.total_rows_30d)

            update_sql = f"""
                UPDATE delta.`{health_path}`
                SET success_rate_7d = {sr7},
                    success_rate_30d = {sr30},
                    avg_duration_ms_7d = {avg_dur},
                    total_rows_30d = {rows_30d},
                    updated_at = current_timestamp()
                WHERE pipeline_name = '{sql_escape(pipeline_name)}'
            """
            spark.sql(update_sql)

        logger.debug(f"Updated pipeline_health for {pipeline_name}")

    def _update_sla_status_spark(
        self,
        pipeline_name: str,
        owner: Optional[str],
        freshness_sla: str,
        freshness_anchor: str,
    ) -> None:
        """Spark: fully SQL-derived MERGE with unix_timestamp minutes diff."""
        spark = self.catalog.spark
        sla_path = self.catalog.tables["meta_sla_status"]
        runs_path = self.catalog.tables["meta_pipeline_runs"]

        sla_minutes = parse_duration_to_minutes(freshness_sla)
        if sla_minutes <= 0:
            raise ValueError(f"Invalid freshness_sla: {freshness_sla}")

        owner_sql = f"'{sql_escape(owner)}'" if owner else "NULL"

        merge_sql = f"""
            MERGE INTO delta.`{sla_path}` AS target
            USING (
                WITH success_stats AS (
                    SELECT
                        MAX(run_end_at) AS last_success_at,
                        CAST((unix_timestamp(current_timestamp()) - unix_timestamp(MAX(run_end_at))) / 60 AS BIGINT) AS minutes_since
                    FROM delta.`{runs_path}`
                    WHERE pipeline_name = '{sql_escape(pipeline_name)}' AND status = 'SUCCESS'
                )
                SELECT
                    '{sql_escape(pipeline_name)}' AS pipeline_name,
                    {owner_sql} AS owner,
                    '{sql_escape(freshness_sla)}' AS freshness_sla,
                    '{sql_escape(freshness_anchor)}' AS freshness_anchor,
                    CAST({sla_minutes} AS BIGINT) AS freshness_sla_minutes,
                    s.last_success_at,
                    s.minutes_since AS minutes_since_success,
                    CASE
                        WHEN s.last_success_at IS NULL THEN 0
                        WHEN s.minutes_since <= {sla_minutes} THEN 1
                        ELSE 0
                    END AS sla_met,
                    CAST(
                        CASE
                            WHEN s.last_success_at IS NULL THEN NULL
                            WHEN s.minutes_since <= {sla_minutes} THEN NULL
                            ELSE ROUND((s.minutes_since - {sla_minutes}) / 60.0, 2)
                        END
                    AS DOUBLE) AS hours_overdue,
                    current_timestamp() AS updated_at
                FROM success_stats s
            ) AS source
            ON target.pipeline_name = source.pipeline_name
            WHEN MATCHED THEN UPDATE SET
                owner = source.owner,
                freshness_sla = source.freshness_sla,
                freshness_anchor = source.freshness_anchor,
                freshness_sla_minutes = source.freshness_sla_minutes,
                last_success_at = source.last_success_at,
                minutes_since_success = source.minutes_since_success,
                sla_met = source.sla_met,
                hours_overdue = source.hours_overdue,
                updated_at = source.updated_at
            WHEN NOT MATCHED THEN INSERT (
                pipeline_name, owner, freshness_sla, freshness_anchor, freshness_sla_minutes,
                last_success_at, minutes_since_success, sla_met, hours_overdue, updated_at
            ) VALUES (
                source.pipeline_name, source.owner, source.freshness_sla, source.freshness_anchor,
                source.freshness_sla_minutes, source.last_success_at, source.minutes_since_success,
                source.sla_met, source.hours_overdue, source.updated_at
            )
        """
        spark.sql(merge_sql)
        logger.debug(f"Updated sla_status for {pipeline_name}")

    # =========================================================================
    # PANDAS/DELTA-RS: DERIVED UPDATERS
    # =========================================================================

    def _update_daily_stats_pandas(self, run_id: str, pipeline_run: Dict[str, Any]) -> None:
        """Pandas/delta-rs: groupby + overwrite partition/day."""
        if not DeltaTable or not pd or not write_deltalake:
            raise ImportError("deltalake library required for pandas mode")

        storage_opts = self.catalog._get_storage_options()
        daily_stats_path = self.catalog.tables["meta_daily_stats"]
        runs_path = self.catalog.tables["meta_pipeline_runs"]

        pipeline_name = pipeline_run["pipeline_name"]
        run_end_at = pipeline_run["run_end_at"]
        run_date = (
            run_end_at.date()
            if hasattr(run_end_at, "date")
            else datetime.fromisoformat(str(run_end_at)[:10]).date()
        )

        try:
            dt = DeltaTable(daily_stats_path, storage_options=storage_opts or None)
            existing = dt.to_pandas()
        except Exception:
            existing = pd.DataFrame()

        try:
            runs_dt = DeltaTable(runs_path, storage_options=storage_opts or None)
            runs_df = runs_dt.to_pandas()
        except Exception:
            runs_df = pd.DataFrame()

        if runs_df.empty:
            return

        runs_df["run_date"] = pd.to_datetime(runs_df["run_end_at"]).dt.date
        day_runs = runs_df[
            (runs_df["pipeline_name"] == pipeline_name) & (runs_df["run_date"] == run_date)
        ]

        if day_runs.empty:
            return

        estimated_cost_sum = (
            day_runs["estimated_cost_usd"].fillna(0).sum()
            if "estimated_cost_usd" in day_runs.columns
            else None
        )
        actual_cost_sum = (
            day_runs["actual_cost_usd"].fillna(0).sum()
            if "actual_cost_usd" in day_runs.columns
            else None
        )

        has_actual = actual_cost_sum is not None and actual_cost_sum > 0
        has_estimated = estimated_cost_sum is not None and estimated_cost_sum > 0
        if has_actual:
            cost_source = "databricks_billing"
        elif has_estimated:
            cost_source = "configured_rate"
        else:
            cost_source = "none"

        new_row = pd.DataFrame(
            [
                {
                    "date": run_date,
                    "pipeline_name": pipeline_name,
                    "runs": int(len(day_runs)),
                    "successes": int((day_runs["status"] == "SUCCESS").sum()),
                    "failures": int((day_runs["status"] == "FAILURE").sum()),
                    "total_rows": int(day_runs["rows_processed"].fillna(0).sum()),
                    "total_duration_ms": int(day_runs["duration_ms"].sum()),
                    "estimated_cost_usd": float(estimated_cost_sum) if estimated_cost_sum else None,
                    "actual_cost_usd": float(actual_cost_sum) if actual_cost_sum else None,
                    "cost_source": cost_source,
                    "cost_is_actual": 1 if has_actual else 0,
                }
            ]
        )

        if not existing.empty:
            existing["date"] = pd.to_datetime(existing["date"]).dt.date
            existing = existing[
                ~((existing["date"] == run_date) & (existing["pipeline_name"] == pipeline_name))
            ]

        result = pd.concat([existing, new_row], ignore_index=True)

        def _do_overwrite():
            write_deltalake(
                daily_stats_path, result, mode="overwrite", storage_options=storage_opts or None
            )

        _retry_delta_operation(_do_overwrite)
        logger.debug(f"Updated daily_stats (pandas) for {pipeline_name}/{run_date}")

    def _update_pipeline_health_pandas(self, pipeline_run: Dict[str, Any]) -> None:
        """Pandas/delta-rs: filter last 30d + overwrite."""
        if not DeltaTable or not pd or not write_deltalake:
            raise ImportError("deltalake library required for pandas mode")

        storage_opts = self.catalog._get_storage_options()
        health_path = self.catalog.tables["meta_pipeline_health"]
        runs_path = self.catalog.tables["meta_pipeline_runs"]

        pipeline_name = pipeline_run["pipeline_name"]
        owner = pipeline_run.get("owner")
        layer = pipeline_run.get("layer")
        status = pipeline_run["status"]
        now = datetime.now(timezone.utc)

        try:
            health_dt = DeltaTable(health_path, storage_options=storage_opts or None)
            existing = health_dt.to_pandas()
        except Exception:
            existing = pd.DataFrame()

        try:
            runs_dt = DeltaTable(runs_path, storage_options=storage_opts or None)
            runs_df = runs_dt.to_pandas()
        except Exception:
            runs_df = pd.DataFrame()

        current_row = (
            existing[existing["pipeline_name"] == pipeline_name]
            if not existing.empty
            else pd.DataFrame()
        )

        if current_row.empty:
            total_runs = 1
            total_successes = 1 if status == "SUCCESS" else 0
            total_failures = 1 if status == "FAILURE" else 0
            last_success_at = now if status == "SUCCESS" else None
            last_failure_at = now if status == "FAILURE" else None
        else:
            row = current_row.iloc[0]
            total_runs = int(row.get("total_runs", 0) or 0) + 1
            total_successes = int(row.get("total_successes", 0) or 0) + (
                1 if status == "SUCCESS" else 0
            )
            total_failures = int(row.get("total_failures", 0) or 0) + (
                1 if status == "FAILURE" else 0
            )
            last_success_at = now if status == "SUCCESS" else row.get("last_success_at")
            last_failure_at = now if status == "FAILURE" else row.get("last_failure_at")

        success_rate_7d = None
        success_rate_30d = None
        avg_duration_ms_7d = None
        total_rows_30d = None

        if not runs_df.empty:
            runs_df["run_end_at"] = pd.to_datetime(runs_df["run_end_at"], utc=True)
            cutoff_30d = now - timedelta(days=30)
            cutoff_7d = now - timedelta(days=7)

            pipe_runs = runs_df[runs_df["pipeline_name"] == pipeline_name]
            runs_30d = pipe_runs[pipe_runs["run_end_at"] >= cutoff_30d]
            runs_7d = pipe_runs[pipe_runs["run_end_at"] >= cutoff_7d]

            if len(runs_7d) > 0:
                success_rate_7d = float((runs_7d["status"] == "SUCCESS").sum() / len(runs_7d))
                avg_duration_ms_7d = float(runs_7d["duration_ms"].mean())

            if len(runs_30d) > 0:
                success_rate_30d = float((runs_30d["status"] == "SUCCESS").sum() / len(runs_30d))
                total_rows_30d = int(runs_30d["rows_processed"].fillna(0).sum())

        new_row = pd.DataFrame(
            [
                {
                    "pipeline_name": pipeline_name,
                    "owner": owner,
                    "layer": layer,
                    "total_runs": total_runs,
                    "total_successes": total_successes,
                    "total_failures": total_failures,
                    "success_rate_7d": success_rate_7d,
                    "success_rate_30d": success_rate_30d,
                    "avg_duration_ms_7d": avg_duration_ms_7d,
                    "total_rows_30d": total_rows_30d,
                    "estimated_cost_30d": None,
                    "last_success_at": last_success_at,
                    "last_failure_at": last_failure_at,
                    "last_run_at": now,
                    "updated_at": now,
                }
            ]
        )

        if not existing.empty:
            existing = existing[existing["pipeline_name"] != pipeline_name]

        result = pd.concat([existing, new_row], ignore_index=True)

        def _do_overwrite():
            write_deltalake(
                health_path, result, mode="overwrite", storage_options=storage_opts or None
            )

        _retry_delta_operation(_do_overwrite)
        logger.debug(f"Updated pipeline_health (pandas) for {pipeline_name}")

    def _update_sla_status_pandas(
        self,
        pipeline_name: str,
        owner: Optional[str],
        freshness_sla: str,
        freshness_anchor: str,
    ) -> None:
        """Pandas/delta-rs: Python datetime arithmetic."""
        if not DeltaTable or not pd or not write_deltalake:
            raise ImportError("deltalake library required for pandas mode")

        sla_minutes = parse_duration_to_minutes(freshness_sla)
        if sla_minutes <= 0:
            raise ValueError(f"Invalid freshness_sla: {freshness_sla}")

        storage_opts = self.catalog._get_storage_options()
        sla_path = self.catalog.tables["meta_sla_status"]
        runs_path = self.catalog.tables["meta_pipeline_runs"]
        now = datetime.now(timezone.utc)

        try:
            sla_dt = DeltaTable(sla_path, storage_options=storage_opts or None)
            existing = sla_dt.to_pandas()
        except Exception:
            existing = pd.DataFrame()

        try:
            runs_dt = DeltaTable(runs_path, storage_options=storage_opts or None)
            runs_df = runs_dt.to_pandas()
        except Exception:
            runs_df = pd.DataFrame()

        last_success_at = None
        minutes_since_success = None
        sla_met = 0
        hours_overdue = None

        if not runs_df.empty:
            runs_df["run_end_at"] = pd.to_datetime(runs_df["run_end_at"], utc=True)
            success_runs = runs_df[
                (runs_df["pipeline_name"] == pipeline_name) & (runs_df["status"] == "SUCCESS")
            ]
            if not success_runs.empty:
                last_success_at = success_runs["run_end_at"].max()
                if pd.notna(last_success_at):
                    delta = now - last_success_at.to_pydatetime()
                    minutes_since_success = int(delta.total_seconds() / 60)
                    if minutes_since_success <= sla_minutes:
                        sla_met = 1
                        hours_overdue = None
                    else:
                        sla_met = 0
                        hours_overdue = round((minutes_since_success - sla_minutes) / 60.0, 2)

        new_row = pd.DataFrame(
            [
                {
                    "pipeline_name": pipeline_name,
                    "owner": owner,
                    "freshness_sla": freshness_sla,
                    "freshness_anchor": freshness_anchor,
                    "freshness_sla_minutes": sla_minutes,
                    "last_success_at": last_success_at,
                    "minutes_since_success": minutes_since_success,
                    "sla_met": sla_met,
                    "hours_overdue": hours_overdue,
                    "updated_at": now,
                }
            ]
        )

        if not existing.empty:
            existing = existing[existing["pipeline_name"] != pipeline_name]

        result = pd.concat([existing, new_row], ignore_index=True)

        def _do_overwrite():
            write_deltalake(
                sla_path, result, mode="overwrite", storage_options=storage_opts or None
            )

        _retry_delta_operation(_do_overwrite)
        logger.debug(f"Updated sla_status (pandas) for {pipeline_name}")

    # =========================================================================
    # SQL SERVER: DERIVED UPDATERS
    # =========================================================================

    def _update_daily_stats_sql_server(self, run_id: str, pipeline_run: Dict[str, Any]) -> None:
        """SQL Server: MERGE incremental deltas to meta_daily_stats."""
        if not self._sql_server_table_exists("meta_daily_stats"):
            raise NotImplementedError(
                "meta_daily_stats table does not exist in SQL Server. "
                "SQL Server backend for observability tables not yet implemented."
            )

        conn = self._get_sql_server_connection()
        schema = self._get_sql_server_schema()

        pipeline_name = pipeline_run["pipeline_name"]
        run_end_at = pipeline_run["run_end_at"]
        run_date = (
            run_end_at.date()
            if hasattr(run_end_at, "date")
            else datetime.fromisoformat(str(run_end_at)[:10]).date()
        )
        duration_ms = int(pipeline_run["duration_ms"])
        status = pipeline_run["status"]
        rows_processed = int(pipeline_run.get("rows_processed") or 0)

        if duration_ms < 0:
            raise ValueError(f"Invalid duration_ms: {duration_ms}")

        estimated_cost = pipeline_run.get("estimated_cost_usd")
        actual_cost = pipeline_run.get("actual_cost_usd")

        runs_delta = 1
        successes_delta = 1 if status == "SUCCESS" else 0
        failures_delta = 1 if status == "FAILURE" else 0

        if actual_cost is not None:
            cost_source = "databricks_billing"
        elif estimated_cost is not None:
            cost_source = "configured_rate"
        else:
            cost_source = "none"

        merge_sql = f"""
            MERGE [{schema}].[meta_daily_stats] AS target
            USING (
                SELECT
                    :pipeline_name AS pipeline_name,
                    :run_date AS date,
                    :runs_delta AS runs_delta,
                    :successes_delta AS successes_delta,
                    :failures_delta AS failures_delta,
                    :rows_delta AS rows_delta,
                    :duration_delta AS duration_delta,
                    :estimated_cost_delta AS estimated_cost_delta,
                    :actual_cost_delta AS actual_cost_delta,
                    :cost_source_new AS cost_source_new
            ) AS source
            ON target.pipeline_name = source.pipeline_name AND target.date = source.date
            WHEN MATCHED THEN UPDATE SET
                runs = COALESCE(target.runs, 0) + source.runs_delta,
                successes = COALESCE(target.successes, 0) + source.successes_delta,
                failures = COALESCE(target.failures, 0) + source.failures_delta,
                total_rows = COALESCE(target.total_rows, 0) + source.rows_delta,
                total_duration_ms = COALESCE(target.total_duration_ms, 0) + source.duration_delta,
                estimated_cost_usd = COALESCE(target.estimated_cost_usd, 0) + COALESCE(source.estimated_cost_delta, 0),
                actual_cost_usd = COALESCE(target.actual_cost_usd, 0) + COALESCE(source.actual_cost_delta, 0),
                cost_source = CASE
                    WHEN target.cost_source = source.cost_source_new THEN target.cost_source
                    WHEN target.cost_source = 'none' THEN source.cost_source_new
                    WHEN source.cost_source_new = 'none' THEN target.cost_source
                    ELSE 'mixed'
                END,
                cost_is_actual = CASE WHEN (target.actual_cost_usd IS NOT NULL) OR (source.actual_cost_delta IS NOT NULL) THEN 1 ELSE 0 END
            WHEN NOT MATCHED THEN INSERT (
                date, pipeline_name, runs, successes, failures, total_rows, total_duration_ms,
                estimated_cost_usd, actual_cost_usd, cost_source, cost_is_actual
            ) VALUES (
                source.date, source.pipeline_name, source.runs_delta, source.successes_delta,
                source.failures_delta, source.rows_delta, source.duration_delta,
                source.estimated_cost_delta, source.actual_cost_delta, source.cost_source_new,
                CASE WHEN source.actual_cost_delta IS NOT NULL THEN 1 ELSE 0 END
            );
        """
        conn.execute(
            merge_sql,
            {
                "pipeline_name": pipeline_name,
                "run_date": run_date,
                "runs_delta": runs_delta,
                "successes_delta": successes_delta,
                "failures_delta": failures_delta,
                "rows_delta": rows_processed,
                "duration_delta": duration_ms,
                "estimated_cost_delta": estimated_cost,
                "actual_cost_delta": actual_cost,
                "cost_source_new": cost_source,
            },
        )
        logger.debug(f"Updated daily_stats (sql_server) for {pipeline_name}/{run_date}")

    def _update_pipeline_health_sql_server(self, pipeline_run: Dict[str, Any]) -> None:
        """SQL Server: MERGE + CTE for window metrics."""
        if not self._sql_server_table_exists("meta_pipeline_health"):
            raise NotImplementedError(
                "meta_pipeline_health table does not exist in SQL Server. "
                "SQL Server backend for observability tables not yet implemented."
            )

        conn = self._get_sql_server_connection()
        schema = self._get_sql_server_schema()

        pipeline_name = pipeline_run["pipeline_name"]
        owner = pipeline_run.get("owner")
        layer = pipeline_run.get("layer")
        status = pipeline_run["status"]
        duration_ms = int(pipeline_run["duration_ms"])
        now = datetime.now(timezone.utc)

        if duration_ms < 0:
            raise ValueError(f"Invalid duration_ms: {duration_ms}")

        runs_delta = 1
        success_delta = 1 if status == "SUCCESS" else 0
        fail_delta = 1 if status == "FAILURE" else 0

        merge_sql = f"""
            MERGE [{schema}].[meta_pipeline_health] AS target
            USING (
                SELECT
                    :pipeline_name AS pipeline_name,
                    :owner AS owner,
                    :layer AS layer,
                    :runs_delta AS runs_delta,
                    :success_delta AS success_delta,
                    :fail_delta AS fail_delta,
                    :now AS last_run_at,
                    CASE WHEN :success_delta = 1 THEN :now ELSE NULL END AS last_success_at,
                    CASE WHEN :fail_delta = 1 THEN :now ELSE NULL END AS last_failure_at,
                    :now AS updated_at
            ) AS source
            ON target.pipeline_name = source.pipeline_name
            WHEN MATCHED THEN UPDATE SET
                owner = source.owner,
                layer = source.layer,
                total_runs = COALESCE(target.total_runs, 0) + source.runs_delta,
                total_successes = COALESCE(target.total_successes, 0) + source.success_delta,
                total_failures = COALESCE(target.total_failures, 0) + source.fail_delta,
                last_run_at = source.last_run_at,
                last_success_at = COALESCE(source.last_success_at, target.last_success_at),
                last_failure_at = COALESCE(source.last_failure_at, target.last_failure_at),
                updated_at = source.updated_at
            WHEN NOT MATCHED THEN INSERT (
                pipeline_name, owner, layer,
                total_runs, total_successes, total_failures,
                success_rate_7d, success_rate_30d, avg_duration_ms_7d, total_rows_30d, estimated_cost_30d,
                last_success_at, last_failure_at, last_run_at, updated_at
            ) VALUES (
                source.pipeline_name, source.owner, source.layer,
                source.runs_delta, source.success_delta, source.fail_delta,
                NULL, NULL, NULL, NULL, NULL,
                source.last_success_at, source.last_failure_at, source.last_run_at, source.updated_at
            );
        """
        conn.execute(
            merge_sql,
            {
                "pipeline_name": pipeline_name,
                "owner": owner,
                "layer": layer,
                "runs_delta": runs_delta,
                "success_delta": success_delta,
                "fail_delta": fail_delta,
                "now": now,
            },
        )

        if self._sql_server_table_exists("meta_pipeline_runs"):
            window_sql = f"""
                WITH base AS (
                    SELECT run_end_at, status, duration_ms, COALESCE(rows_processed, 0) AS rows_processed
                    FROM [{schema}].[meta_pipeline_runs]
                    WHERE pipeline_name = :pipeline_name
                      AND run_end_at >= DATEADD(DAY, -30, GETUTCDATE())
                ),
                w7 AS (
                    SELECT
                        COUNT(*) AS runs_7d,
                        SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS succ_7d,
                        AVG(CAST(duration_ms AS FLOAT)) AS avg_dur_7d
                    FROM base WHERE run_end_at >= DATEADD(DAY, -7, GETUTCDATE())
                ),
                w30 AS (
                    SELECT
                        COUNT(*) AS runs_30d,
                        SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS succ_30d,
                        SUM(rows_processed) AS rows_30d
                    FROM base
                )
                SELECT
                    CASE WHEN w7.runs_7d=0 THEN NULL ELSE CAST(w7.succ_7d AS FLOAT) / w7.runs_7d END AS success_rate_7d,
                    CASE WHEN w30.runs_30d=0 THEN NULL ELSE CAST(w30.succ_30d AS FLOAT) / w30.runs_30d END AS success_rate_30d,
                    w7.avg_dur_7d AS avg_duration_ms_7d,
                    w30.rows_30d AS total_rows_30d
                FROM w7 CROSS JOIN w30
            """
            result = conn.execute(window_sql, {"pipeline_name": pipeline_name})
            if result:
                row = result[0]
                update_sql = f"""
                    UPDATE [{schema}].[meta_pipeline_health]
                    SET success_rate_7d = :sr7,
                        success_rate_30d = :sr30,
                        avg_duration_ms_7d = :avg_dur,
                        total_rows_30d = :rows_30d,
                        updated_at = GETUTCDATE()
                    WHERE pipeline_name = :pipeline_name
                """
                conn.execute(
                    update_sql,
                    {
                        "pipeline_name": pipeline_name,
                        "sr7": row[0],
                        "sr30": row[1],
                        "avg_dur": row[2],
                        "rows_30d": row[3],
                    },
                )

        logger.debug(f"Updated pipeline_health (sql_server) for {pipeline_name}")

    def _update_sla_status_sql_server(
        self,
        pipeline_name: str,
        owner: Optional[str],
        freshness_sla: str,
        freshness_anchor: str,
    ) -> None:
        """SQL Server: DATEDIFF(MINUTE, last_success_at, GETUTCDATE())."""
        if not self._sql_server_table_exists("meta_sla_status"):
            raise NotImplementedError(
                "meta_sla_status table does not exist in SQL Server. "
                "SQL Server backend for observability tables not yet implemented."
            )

        sla_minutes = parse_duration_to_minutes(freshness_sla)
        if sla_minutes <= 0:
            raise ValueError(f"Invalid freshness_sla: {freshness_sla}")

        conn = self._get_sql_server_connection()
        schema = self._get_sql_server_schema()

        merge_sql = f"""
            MERGE [{schema}].[meta_sla_status] AS target
            USING (
                SELECT
                    :pipeline_name AS pipeline_name,
                    :owner AS owner,
                    :freshness_sla AS freshness_sla,
                    :freshness_anchor AS freshness_anchor,
                    :sla_minutes AS freshness_sla_minutes,
                    (SELECT MAX(run_end_at) FROM [{schema}].[meta_pipeline_runs]
                     WHERE pipeline_name = :pipeline_name AND status = 'SUCCESS') AS last_success_at
            ) AS source
            ON target.pipeline_name = source.pipeline_name
            WHEN MATCHED THEN UPDATE SET
                owner = source.owner,
                freshness_sla = source.freshness_sla,
                freshness_anchor = source.freshness_anchor,
                freshness_sla_minutes = source.freshness_sla_minutes,
                last_success_at = source.last_success_at,
                minutes_since_success = CASE
                    WHEN source.last_success_at IS NULL THEN NULL
                    ELSE DATEDIFF(MINUTE, source.last_success_at, GETUTCDATE())
                END,
                sla_met = CASE
                    WHEN source.last_success_at IS NULL THEN 0
                    WHEN DATEDIFF(MINUTE, source.last_success_at, GETUTCDATE()) <= :sla_minutes THEN 1
                    ELSE 0
                END,
                hours_overdue = CASE
                    WHEN source.last_success_at IS NULL THEN NULL
                    WHEN DATEDIFF(MINUTE, source.last_success_at, GETUTCDATE()) <= :sla_minutes THEN NULL
                    ELSE ROUND((DATEDIFF(MINUTE, source.last_success_at, GETUTCDATE()) - :sla_minutes) / 60.0, 2)
                END,
                updated_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (
                pipeline_name, owner, freshness_sla, freshness_anchor, freshness_sla_minutes,
                last_success_at, minutes_since_success, sla_met, hours_overdue, updated_at
            ) VALUES (
                source.pipeline_name, source.owner, source.freshness_sla, source.freshness_anchor,
                source.freshness_sla_minutes, source.last_success_at,
                CASE
                    WHEN source.last_success_at IS NULL THEN NULL
                    ELSE DATEDIFF(MINUTE, source.last_success_at, GETUTCDATE())
                END,
                CASE
                    WHEN source.last_success_at IS NULL THEN 0
                    WHEN DATEDIFF(MINUTE, source.last_success_at, GETUTCDATE()) <= :sla_minutes THEN 1
                    ELSE 0
                END,
                CASE
                    WHEN source.last_success_at IS NULL THEN NULL
                    WHEN DATEDIFF(MINUTE, source.last_success_at, GETUTCDATE()) <= :sla_minutes THEN NULL
                    ELSE ROUND((DATEDIFF(MINUTE, source.last_success_at, GETUTCDATE()) - :sla_minutes) / 60.0, 2)
                END,
                GETUTCDATE()
            );
        """
        conn.execute(
            merge_sql,
            {
                "pipeline_name": pipeline_name,
                "owner": owner,
                "freshness_sla": freshness_sla,
                "freshness_anchor": freshness_anchor,
                "sla_minutes": sla_minutes,
            },
        )
        logger.debug(f"Updated sla_status (sql_server) for {pipeline_name}")
