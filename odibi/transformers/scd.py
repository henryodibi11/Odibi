from __future__ import annotations

import os
import time
from typing import Any, List, Optional

from pydantic import BaseModel, Field, model_validator

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.utils.logging_context import get_logging_context


class SCD2Params(BaseModel):
    """
    Parameters for SCD Type 2 (Slowly Changing Dimensions) transformer.

    ### 🕰️ The "Time Machine" Pattern

    **Business Problem:**
    "I need to know what the customer's address was *last month*, not just where they live now."

    **The Solution:**
    SCD Type 2 tracks the full history of changes. Each record has an "effective window" (start/end dates) and a flag indicating if it is the current version.

    **Recipe 1: Using table name**
    ```yaml
    transformer: "scd2"
    params:
      target: "silver.dim_customers"   # Registered table name
      keys: ["customer_id"]
      track_cols: ["address", "tier"]
      effective_time_col: "txn_date"
    ```

    **Recipe 2: Using connection + path (ADLS)**
    ```yaml
    transformer: "scd2"
    params:
      connection: adls_prod            # Connection name
      path: OEE/silver/dim_customers   # Relative path
      keys: ["customer_id"]
      track_cols: ["address", "tier"]
      effective_time_col: "txn_date"
    ```

    **How it works:**
    1. **Match**: Finds existing records using `keys`.
    2. **Compare**: Checks `track_cols` to see if data changed.
    3. **Close**: If changed, updates the old record's `end_time_col` to the new `effective_time_col`.
    4. **Insert**: Adds a new record with `start_time_col` (renamed from `effective_time_col`)
       as the version start, open-ended `end_time_col`, and `is_current = true`.

    The ``effective_time_col`` value is copied into a new ``start_time_col`` column
    (default: ``valid_from``) in the target, giving each version a complete time window:
    ``[valid_from, valid_to)``. The original source column is preserved.

    **Note:** SCD2 is self-contained — it writes directly to the target table on all
    engines. No separate ``write:`` block is needed in your pipeline YAML.

    On Spark with Delta targets, uses an optimized Delta MERGE by default
    (``use_delta_merge: true``). Set ``use_delta_merge: false`` to use the legacy
    full-overwrite approach (still self-contained, just slower for large tables).

    On Pandas, writes directly to the target file (parquet or CSV).
    """

    target: Optional[str] = Field(
        None,
        description="Target table name or full path (use this OR connection+path)",
    )
    connection: Optional[str] = Field(
        None,
        description="Connection name to resolve path (use with 'path' param)",
    )
    path: Optional[str] = Field(
        None,
        description="Relative path within connection (e.g., 'OEE/silver/dim_customers')",
    )
    keys: List[str] = Field(..., description="Natural keys to identify unique entities")
    track_cols: List[str] = Field(..., description="Columns to monitor for changes")
    effective_time_col: str = Field(
        ...,
        description="Source column indicating when the change occurred.",
    )
    start_time_col: str = Field(
        default="valid_from",
        description="Name of the start timestamp column in the target. "
        "The effective_time_col value is copied to this column.",
    )
    end_time_col: str = Field(default="valid_to", description="Name of the end timestamp column")
    current_flag_col: str = Field(
        default="is_current", description="Name of the current record flag column"
    )
    delete_col: Optional[str] = Field(
        default=None, description="Column indicating soft deletion (boolean)"
    )
    use_delta_merge: bool = Field(
        default=True,
        description="Use Delta Lake MERGE for Spark engine (faster for large tables). "
        "Falls back to full overwrite if target is not Delta format.",
    )
    register_table: Optional[str] = Field(
        default=None,
        description="Register as Unity Catalog/metastore table after write "
        "(e.g., 'silver.dim_customers'). Spark only.",
    )

    @model_validator(mode="after")
    def check_target_or_connection(self):
        """Ensure either target or connection+path is provided."""
        if not self.target and not (self.connection and self.path):
            raise ValueError("SCD2: provide either 'target' OR both 'connection' and 'path'.")
        if self.target and (self.connection or self.path):
            raise ValueError("SCD2: use 'target' OR 'connection'+'path', not both.")
        return self


def scd2(context: EngineContext, params: SCD2Params, current: Any = None) -> EngineContext:
    """
    Implements SCD Type 2 Logic.

    SCD2 is self-contained: it writes directly to the target table on all
    engines and code paths. No separate write: block is needed.

    On Spark with use_delta_merge=True (default), uses an optimized Delta
    MERGE that only touches changed rows. The legacy path reads the full
    target, computes the union, and overwrites. Both write directly.

    On Pandas, writes directly to the target file (parquet or CSV).
    """
    ctx = get_logging_context()
    start_time = time.time()

    # Resolve target path from connection if provided
    target = params.target

    if params.connection and params.path:
        # Resolve path via connection
        connection = None
        if hasattr(context, "engine") and hasattr(context.engine, "connections"):
            connections = context.engine.connections
            if connections and params.connection in connections:
                connection = connections[params.connection]

        if connection is None:
            raise ValueError(
                f"SCD2: connection '{params.connection}' not found. "
                "Ensure the connection is defined in your project config."
            )

        if hasattr(connection, "get_path"):
            target = connection.get_path(params.path)
            ctx.debug(
                "Resolved SCD2 target path via connection",
                connection=params.connection,
                relative_path=params.path,
                resolved_path=target,
            )
        else:
            raise ValueError(
                f"SCD2: connection '{params.connection}' (type: {type(connection).__name__}) "
                f"does not support path resolution. Expected a connection with 'get_path' method. "
                f"Connection type must be 'local', 'adls', or similar file-based connection."
            )

    ctx.debug(
        "SCD2 starting",
        target=target,
        keys=params.keys,
        track_cols=params.track_cols,
    )

    source_df = context.df if current is None else current

    rows_before = None
    try:
        rows_before = source_df.shape[0] if hasattr(source_df, "shape") else None
        if rows_before is None and hasattr(source_df, "count"):
            rows_before = source_df.count()
    except Exception as e:
        ctx.debug(f"Could not get row count: {type(e).__name__}")

    ctx.debug(
        "SCD2 source loaded",
        source_rows=rows_before,
    )

    # Create a modified params with resolved target for internal functions
    resolved_params = params.model_copy(update={"target": target})

    if context.engine_type == EngineType.SPARK:
        result = _scd2_spark(context, source_df, resolved_params)
    elif context.engine_type == EngineType.PANDAS:
        result = _scd2_pandas(context, source_df, resolved_params)
    else:
        ctx.error("SCD2 failed: unsupported engine", engine_type=str(context.engine_type))
        raise ValueError(
            f"SCD2 transformer does not support engine type '{context.engine_type}'. "
            f"Supported engines: SPARK, PANDAS. "
            f"Check your engine configuration or use a different transformer."
        )

    # Register table in metastore if requested (Spark only)
    if params.register_table and context.engine_type == EngineType.SPARK:
        try:
            spark = context.spark
            if spark:
                ctx.debug(
                    "Registering table in metastore",
                    table_name=params.register_table,
                    location=target,
                )
                spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {params.register_table} "
                    f"USING DELTA LOCATION '{target}'"
                )
                ctx.info(
                    "Table registered successfully",
                    table_name=params.register_table,
                    location=target,
                )
        except Exception as e:
            ctx.warning(
                f"Failed to register table: {e}",
                table_name=params.register_table,
                error=str(e),
            )

    rows_after = None
    try:
        rows_after = result.df.shape[0] if hasattr(result.df, "shape") else None
        if rows_after is None and hasattr(result.df, "count"):
            rows_after = result.df.count()
    except Exception as e:
        ctx.debug(f"Could not get row count: {type(e).__name__}")

    elapsed_ms = (time.time() - start_time) * 1000
    ctx.debug(
        "SCD2 completed",
        target=target,
        source_rows=rows_before,
        result_rows=rows_after,
        elapsed_ms=round(elapsed_ms, 2),
    )

    return result


def _get_timestamp_type(df, reference_cols: List[str]) -> str:
    """Determine timestamp cast type from existing DataFrame columns.

    Checks reference columns for timezone-aware timestamp types and returns
    the matching cast string. Falls back to 'timestamp' (naive) if no
    tz-aware columns are found.

    Args:
        df: Spark DataFrame to inspect
        reference_cols: Column names to check for timestamp type

    Returns:
        Cast type string (e.g., 'timestamp' or 'timestamp_ntz')
    """
    if df is None:
        return "timestamp"
    from pyspark.sql.types import TimestampNTZType, TimestampType

    for col_name in reference_cols:
        if col_name in df.columns:
            col_type = df.schema[col_name].dataType
            if isinstance(col_type, TimestampNTZType):
                return "timestamp_ntz"
            if isinstance(col_type, TimestampType):
                return "timestamp"
    return "timestamp"


def _scd2_spark(context: EngineContext, source_df: Any, params: SCD2Params) -> EngineContext:
    """
    Internal helper for SCD2 logic on Spark engine.

    Compares source and target DataFrames, detects changes, closes old records, and inserts new versions
    according to SCD Type 2 rules. Handles both table and Delta path targets.

    All code paths write directly to the target (self-contained).
    When use_delta_merge is enabled, attempts the optimized Delta MERGE path first.
    Falls back to the legacy full-overwrite approach if Delta MERGE is unavailable.
    """
    from pyspark.sql import functions as F

    spark = context.spark

    # Define Columns
    eff_col = params.effective_time_col
    start_col = params.start_time_col
    end_col = params.end_time_col
    flag_col = params.current_flag_col

    # Validate effective_time_col exists in source
    source_cols = source_df.columns
    if eff_col not in source_cols:
        raise ValueError(
            f"SCD2: effective_time_col '{eff_col}' not found in source DataFrame. "
            f"Available columns: {source_cols}"
        )

    # Try optimized Delta MERGE path (avoids reading entire target)
    if params.use_delta_merge:
        try:
            result = _scd2_spark_delta_merge(context, source_df, params)
            if result is not None:
                return result
        except Exception as e:
            get_logging_context().warning(
                f"Delta MERGE path unavailable, using full overwrite: {e}",
                target=params.target,
            )

    # Legacy path: read entire target, join in memory, return full history
    # 1. Check if target exists
    target_df = None
    try:
        # Try reading as table first
        target_df = spark.table(params.target)
    except Exception as e:
        logger = get_logging_context()
        logger.debug(
            f"Target table '{params.target}' not found as registered table: {type(e).__name__}: {e}"
        )
        try:
            # Try reading as Delta path
            target_df = spark.read.format("delta").load(params.target)
        except Exception as e2:
            logger.debug(
                f"Target '{params.target}' not found as Delta path - assuming first run: {type(e2).__name__}: {e2}"
            )
            # Target doesn't exist yet - First Run
            pass

    # Prepare Source: Add SCD metadata columns
    # New records start as Current
    ts_type = _get_timestamp_type(source_df, [eff_col, start_col])
    new_records = source_df.withColumn(end_col, F.lit(None).cast(ts_type)).withColumn(
        flag_col, F.lit(True)
    )

    if target_df is None:
        # First Run: write directly to target
        if eff_col in new_records.columns and eff_col != start_col:
            new_records = new_records.withColumn(start_col, new_records[eff_col])

        is_path = (
            "/" in params.target
            or "\\" in params.target
            or ":" in params.target
            or params.target.startswith(".")
        )
        writer = new_records.write.format("delta").mode("overwrite")
        if is_path:
            writer.save(params.target)
        else:
            writer.saveAsTable(params.target)

        get_logging_context().info(
            "SCD2 legacy first run: wrote initial data directly to target",
            target=params.target,
        )

        return context.with_df(new_records)

    # 2. Logic: Compare Source vs Target (Current Records Only)
    # We only compare against currently open records in target
    # Handle optional filtering if flag col doesn't exist in target yet (migration?)
    if flag_col in target_df.columns:
        current_target = target_df.filter(F.col(flag_col) == F.lit(True))
    else:
        current_target = target_df

    # Rename target cols to avoid collision in join
    t_prefix = "__target_"
    renamed_target = current_target
    for c in current_target.columns:
        renamed_target = renamed_target.withColumnRenamed(c, f"{t_prefix}{c}")

    # Preserve effective_time_col with a unique name before join to avoid resolution issues
    # This ensures we can always reference it regardless of target schema
    # Use source_df[col] syntax to bind column reference directly to this DataFrame
    # (F.col() can get confused during lazy evaluation with complex join plans)
    eff_col_preserved = "__src_eff_time"
    source_with_eff = source_df.withColumn(eff_col_preserved, source_df[eff_col])

    # Alias source_df to ensure column references are unambiguous after join
    # Use backticks to handle column names with spaces or special characters
    source_aliased = source_with_eff.alias("__source")
    join_cond = [F.col(f"`__source`.`{k}`") == F.col(f"`{t_prefix}{k}`") for k in params.keys]

    joined = source_aliased.join(renamed_target, join_cond, "left")

    # Determine Status: Changed if track columns differ
    # Use explicit __source alias for source columns to avoid ambiguity
    # Use backticks to handle column names with spaces or special characters
    change_conds = []
    for col in params.track_cols:
        s_col = F.col(f"`__source`.`{col}`")
        t_col = F.col(f"`{t_prefix}{col}`")
        # Null-safe equality check: NOT (source <=> target)
        # Use ~ operator instead of F.not_() which doesn't exist in PySpark
        change_conds.append(~s_col.eqNullSafe(t_col))

    if change_conds:
        from functools import reduce

        is_changed = reduce(lambda a, b: a | b, change_conds)
    else:
        is_changed = F.lit(False)

    # A) Rows to Insert (New Keys OR Changed Keys)
    # Filter: TargetKey IS NULL OR is_changed
    # Select source columns using the __source alias with backticks for special chars
    rows_to_insert = joined.filter(
        F.col(f"`{t_prefix}{params.keys[0]}`").isNull() | is_changed
    ).select([F.col(f"`__source`.`{c}`").alias(c) for c in source_df.columns])

    # Add metadata to inserts (Start=eff_col, End=Null, Current=True)
    rows_to_insert = rows_to_insert.withColumn(end_col, F.lit(None).cast(ts_type)).withColumn(
        flag_col, F.lit(True)
    )

    # Copy effective_time_col to start_time_col (e.g., txn_date → valid_from)
    if eff_col in rows_to_insert.columns and eff_col != start_col:
        rows_to_insert = rows_to_insert.withColumn(start_col, rows_to_insert[eff_col])

    # B) Close Old Records
    # We need to update target_df.
    # Strategy:
    # 1. Identify keys that CHANGED (from joined result)
    # Also carry over the NEW effective date from source to use as END date
    # Use backticks to handle column names with spaces or special characters
    changed_keys_with_date = joined.filter(is_changed).select(
        *[F.col(f"`__source`.`{k}`").alias(k) for k in params.keys],
        F.col(f"`__source`.`{eff_col_preserved}`").alias("__new_end_date"),
    )

    # 2. Join Target with Changed Keys to apply updates
    # We rejoin target_df with changed_keys_with_date
    # Update logic: If match found AND is_current, set end_date = __new_end_date, flag = False

    target_updated = target_df.alias("tgt").join(
        changed_keys_with_date.alias("chg"), on=params.keys, how="left"
    )

    # Apply conditional logic
    # If chg.__new_end_date IS NOT NULL AND tgt.is_current == True:
    #    end_col = chg.__new_end_date
    #    flag_col = False
    # Else:
    #    Keep original

    # Use backticks for column references to handle special characters
    final_target = target_updated.select(
        *[
            (
                F.when(
                    (F.col("`__new_end_date`").isNotNull())
                    & (F.col(f"`tgt`.`{flag_col}`") == F.lit(True)),
                    F.col("`__new_end_date`"),
                )
                .otherwise(F.col(f"`tgt`.`{end_col}`"))
                .alias(end_col)
                if c == end_col
                else (
                    F.when(
                        (F.col("`__new_end_date`").isNotNull())
                        & (F.col(f"`tgt`.`{flag_col}`") == F.lit(True)),
                        F.lit(False),
                    )
                    .otherwise(F.col(f"`tgt`.`{c}`"))
                    .alias(c)
                    if c == flag_col
                    else F.col(f"`tgt`.`{c}`")
                )
            )
            for c in target_df.columns
        ]
    )

    # 3. Union: Updated History + New Inserts
    # Copy effective_time_col to start_time_col in final_target if needed
    if eff_col in final_target.columns and start_col not in final_target.columns:
        final_target = final_target.withColumn(start_col, final_target[eff_col])

    # UnionByName handles column order differences
    final_df = final_target.unionByName(rows_to_insert)

    # Write directly to target (self-contained, no external write: block needed)
    is_path = (
        "/" in params.target
        or "\\" in params.target
        or ":" in params.target
        or params.target.startswith(".")
    )
    writer = final_df.write.format("delta").mode("overwrite")
    if is_path:
        writer.save(params.target)
    else:
        writer.saveAsTable(params.target)

    get_logging_context().info(
        "SCD2 legacy overwrite: wrote full history directly to target",
        target=params.target,
    )

    return context.with_df(final_df)


def _scd2_spark_delta_merge(context: EngineContext, source_df, params: SCD2Params):
    """
    Optimized SCD2 using Delta Lake MERGE.

    Instead of reading the entire target, joining in memory, and overwriting,
    this uses Delta MERGE to only update affected rows. Much faster for large
    tables with small change sets.

    Returns None if target is not a Delta table (caller should fall back to
    the legacy full-overwrite path).
    """
    from pyspark.sql import functions as F

    try:
        from delta.tables import DeltaTable
    except ImportError:
        return None

    spark = context.spark
    target = params.target
    keys = params.keys
    eff_col = params.effective_time_col
    start_col = params.start_time_col
    end_col = params.end_time_col
    flag_col = params.current_flag_col
    ctx = get_logging_context()
    ts_type = _get_timestamp_type(source_df, [eff_col, start_col])

    # Check if target is a Delta table
    is_delta = False
    delta_table = None
    try:
        delta_table = DeltaTable.forName(spark, target)
        is_delta = True
    except Exception:
        try:
            if DeltaTable.isDeltaTable(spark, target):
                delta_table = DeltaTable.forPath(spark, target)
                is_delta = True
        except Exception:
            pass

    if not is_delta:
        # Target doesn't exist yet or is not Delta — check for first run
        target_exists = False
        try:
            spark.table(target)
            target_exists = True
        except Exception:
            try:
                spark.read.format("delta").load(target)
                target_exists = True
            except Exception:
                pass

        if not target_exists:
            # First run: write directly to target (same pattern as merge transformer)
            new_records = source_df.withColumn(end_col, F.lit(None).cast(ts_type)).withColumn(
                flag_col, F.lit(True)
            )
            if eff_col in new_records.columns and eff_col != start_col:
                new_records = new_records.withColumn(start_col, new_records[eff_col])

            is_path = "/" in target or "\\" in target or ":" in target or target.startswith(".")
            writer = new_records.write.format("delta").mode("overwrite")
            if is_path:
                writer.save(target)
            else:
                writer.saveAsTable(target)

            ctx.info(
                "SCD2 first run: wrote initial data directly to target",
                target=target,
            )

            return context.with_df(new_records)

        # Target exists but not Delta — can't use MERGE
        return None

    ctx.info(
        "Using Delta MERGE for SCD2 (optimized path)",
        target=target,
        keys=keys,
        track_cols=params.track_cols,
    )

    # Prepare merge source: rename eff_col to start_col, add SCD metadata
    # Also keep eff_col temporarily as __eff_col for the update_set reference
    merge_source = (
        source_df.withColumn(start_col, F.col(f"`{eff_col}`"))
        .withColumn(end_col, F.lit(None).cast(ts_type))
        .withColumn(flag_col, F.lit(True))
    )

    # Build MERGE condition: match on keys AND only current records in target
    key_conditions = [f"target.`{k}` = source.`{k}`" for k in keys]
    match_condition = " AND ".join(key_conditions) + f" AND target.`{flag_col}` = true"

    # Build change detection condition for whenMatchedUpdate
    change_conds = [f"NOT (target.`{col}` <=> source.`{col}`)" for col in params.track_cols]
    change_condition = " OR ".join(change_conds) if change_conds else "true"

    # Partial update: only close the old record (use eff_col from source for end date)
    update_set = {
        f"`{end_col}`": f"source.`{eff_col}`",
        f"`{flag_col}`": "false",
    }

    # Build explicit insert map: include start_col, exclude original eff_col
    insert_cols = [c for c in merge_source.columns if c != eff_col]
    insert_values = {f"`{col}`": f"source.`{col}`" for col in insert_cols}

    # Execute MERGE — closes old versions and inserts brand-new keys
    merger = (
        delta_table.alias("target")
        .merge(merge_source.alias("source"), match_condition)
        .whenMatchedUpdate(condition=change_condition, set=update_set)
        .whenNotMatchedInsert(values=insert_values)
    )
    merger.execute()

    # The MERGE above only *closes* changed rows — it does NOT insert the
    # new version for keys that already existed.  `whenNotMatchedInsert` only
    # fires for keys absent from the target, so changed rows need a separate
    # append.  We detect them by reading the (now-closed) target rows and
    # finding keys whose current version was just closed (is_current=false,
    # valid_to set to the source effective time).
    #
    # Simpler approach: re-read current target, left-anti-join the source to
    # find keys that have NO current row, then append those from merge_source.

    try:
        # Re-read target after MERGE
        try:
            updated_target = DeltaTable.forName(spark, target).toDF()
        except Exception:
            updated_target = DeltaTable.forPath(spark, target).toDF()

        # Keys in source that do NOT have an is_current=true row in target
        current_keys = updated_target.filter(F.col(f"`{flag_col}`") == F.lit(True)).select(
            [F.col(f"`{k}`") for k in keys]
        )

        # Anti-join: source keys missing from current target = changed rows
        # whose new version was never inserted
        changed_new_versions = merge_source.join(current_keys, on=keys, how="left_anti")

        if changed_new_versions.count() > 0:
            # Select only the columns that exist in the target schema
            target_columns = updated_target.columns
            write_cols = [c for c in changed_new_versions.columns if c in target_columns]
            changed_new_versions = changed_new_versions.select(
                [F.col(f"`{c}`") for c in write_cols]
            )

            is_path = "/" in target or "\\" in target or ":" in target or target.startswith(".")
            if is_path:
                changed_new_versions.write.format("delta").mode("append").save(target)
            else:
                changed_new_versions.write.format("delta").mode("append").saveAsTable(target)

            ctx.info(
                "Appended new versions for changed keys",
                target=target,
                appended_rows=changed_new_versions.count(),
            )
    except Exception as e:
        ctx.warning(
            f"Failed to append new versions for changed keys: {e}",
            target=target,
        )

    # Return the source records with start_col (MERGE already wrote to target)
    result_df = merge_source

    ctx.info("Delta MERGE SCD2 completed", target=target)

    return context.with_df(result_df)


def _scd2_pandas(context: EngineContext, source_df, params: SCD2Params) -> EngineContext:
    """
    Internal helper for SCD2 logic on Pandas engine.

    Handles SCD Type 2 logic using either DuckDB (if available and parquet target) or pure Pandas.
    Compares source and target, detects changes, closes old records, and inserts new versions.
    """
    import logging

    import pandas as pd

    logger = logging.getLogger(__name__)

    # Try using DuckDB
    try:
        import duckdb

        HAS_DUCKDB = True
    except ImportError:
        HAS_DUCKDB = False

    # 1. Load Target
    path = params.target

    # Resolve path if context has engine (EngineContext)
    if hasattr(context, "engine") and context.engine:
        # Try to resolve 'connection.path'
        if "." in path:
            parts = path.split(".", 1)
            conn_name = parts[0]
            rel_path = parts[1]
            if conn_name in context.engine.connections:
                try:
                    path = context.engine.connections[conn_name].get_path(rel_path)
                except Exception as e:
                    get_logging_context().debug(
                        f"Could not resolve connection path: {type(e).__name__}"
                    )

    # Define Cols
    keys = params.keys
    eff_col = params.effective_time_col
    start_col = params.start_time_col
    end_col = params.end_time_col
    flag_col = params.current_flag_col
    track = params.track_cols

    # --- DUCKDB IMPLEMENTATION ---
    if HAS_DUCKDB and str(path).endswith(".parquet") and os.path.exists(path):
        try:
            con = duckdb.connect(database=":memory:")
            con.register("source_df", source_df)

            # Helper to build condition string
            # DuckDB supports IS DISTINCT FROM
            change_cond_parts = []
            for col in track:
                change_cond_parts.append(f"s.{col} IS DISTINCT FROM t.{col}")
            change_cond = " OR ".join(change_cond_parts)

            join_cond = " AND ".join([f"s.{k} = t.{k}" for k in keys])

            src_cols = [c for c in source_df.columns if c not in [end_col, flag_col]]
            # Keep original columns and copy eff_col to start_col
            cols_select = ", ".join([f"s.{c}" for c in src_cols])
            if eff_col != start_col:
                cols_select += f", s.{eff_col} as {start_col}"
            null_check = " AND ".join([f"t.{k} IS NULL" for k in keys])

            sql_new_inserts = f"""
                SELECT {cols_select}, NULL::TIMESTAMP as {end_col}, True as {flag_col}
                FROM source_df s
                LEFT JOIN (SELECT * FROM read_parquet('{path}') WHERE {flag_col} = True) t
                ON {join_cond}
                WHERE {null_check}
            """

            sql_changed_inserts = f"""
                SELECT {cols_select}, NULL::TIMESTAMP as {end_col}, True as {flag_col}
                FROM source_df s
                JOIN (SELECT * FROM read_parquet('{path}') WHERE {flag_col} = True) t
                ON {join_cond}
                WHERE ({change_cond})
            """

            sql_closed_records = f"""
                SELECT
                    t.* EXCLUDE ({end_col}, {flag_col}),
                    s.{eff_col}::TIMESTAMP as {end_col},
                    False as {flag_col}
                FROM read_parquet('{path}') t
                JOIN source_df s ON {join_cond}
                WHERE t.{flag_col} = True AND ({change_cond})
            """

            sql_unchanged = f"""
                SELECT * FROM read_parquet('{path}') t
                WHERE NOT (
                    t.{flag_col} = True AND EXISTS (
                        SELECT 1 FROM source_df s
                        WHERE {join_cond} AND ({change_cond})
                    )
                )
            """

            final_query = f"""
                {sql_new_inserts}
                UNION ALL
                {sql_changed_inserts}
                UNION ALL
                {sql_closed_records}
                UNION ALL
                {sql_unchanged}
            """

            temp_path = str(path) + ".tmp.parquet"
            con.execute(f"COPY ({final_query}) TO '{temp_path}' (FORMAT PARQUET)")
            con.close()

            if os.path.exists(temp_path):
                if os.path.exists(path):
                    os.remove(path)
                os.rename(temp_path, path)

            return context.with_df(source_df)

        except Exception as e:
            logger.warning(f"DuckDB SCD2 failed, falling back to Pandas: {e}")
            pass

    # --- PANDAS FALLBACK ---
    target_df = pd.DataFrame()

    # Try loading if exists
    if os.path.exists(path):
        try:
            # Naive format detection or try/except
            if str(path).endswith(".parquet") or os.path.isdir(path):  # Parquet often directory
                target_df = pd.read_parquet(path)
            elif str(path).endswith(".csv"):
                target_df = pd.read_csv(path)
        except Exception as e:
            get_logging_context().debug(f"Could not read target file: {type(e).__name__}")

    # Prepare Source: rename eff_col to start_col, add SCD metadata
    source_df = source_df.copy()
    source_df[start_col] = source_df[eff_col]
    source_df[end_col] = pd.NaT
    source_df[flag_col] = True

    if target_df.empty:
        # First run: write directly to target
        if str(path).endswith(".parquet") or os.path.isdir(path) or not os.path.exists(path):
            source_df.to_parquet(path, index=False)
        elif str(path).endswith(".csv"):
            source_df.to_csv(path, index=False)

        get_logging_context().info(
            "SCD2 Pandas first run: wrote initial data directly to target",
            target=path,
        )

        return context.with_df(source_df)

    # Ensure types match for merge
    # (Skipping complex type alignment for brevity, relying on Pandas)

    # 2. Logic
    # Identify Current Records in Target
    if flag_col in target_df.columns:
        # Filter for current
        current_target = target_df[target_df[flag_col] == True].copy()  # noqa: E712
    else:
        current_target = target_df.copy()

    # Merge Source and Current Target to detect changes
    merged = pd.merge(
        source_df, current_target, on=keys, how="left", suffixes=("", "_tgt"), indicator=True
    )

    # A) New Records (Left Only) -> Insert as is
    new_inserts = merged[merged["_merge"] == "left_only"][source_df.columns].copy()

    # B) Potential Updates (Both)
    updates = merged[merged["_merge"] == "both"].copy()

    # Detect Changes
    def has_changed(row: Any) -> bool:
        for col in track:
            s = row.get(col)
            t = row.get(col + "_tgt")
            # Handle NaNs
            if pd.isna(s) and pd.isna(t):
                continue
            if s != t:
                return True
        return False

    updates["_changed"] = updates.apply(has_changed, axis=1)

    changed_records = updates[updates["_changed"] == True].copy()  # noqa: E712

    # Inserts for changed records (New Version)
    changed_inserts = changed_records[source_df.columns].copy()

    all_inserts = pd.concat([new_inserts, changed_inserts], ignore_index=True)

    # C) Close Old Records
    # We need to update rows in TARGET_DF
    # Update: end_date = source.eff_date, current = False

    final_target = target_df.copy()

    if not changed_records.empty:
        # Create a lookup for closing dates: Key -> New Effective Date
        # We use set_index on keys to facilitate mapping
        # Note: This assumes keys are unique in current_target (valid for SCD2)

        # Prepare DataFrame of keys to close + new end date
        keys_to_close = changed_records[keys + [start_col]].rename(columns={start_col: "__new_end"})
        keys_to_close = keys_to_close.sort_values("__new_end").drop_duplicates(
            subset=keys, keep="last"
        )

        # Merge original target with closing info
        # We use left merge to preserve all target rows
        final_target = final_target.merge(keys_to_close, on=keys, how="left")

        # Identify rows to update:
        # 1. Match found (__new_end is not null)
        # 2. Is currently active
        mask = (final_target["__new_end"].notna()) & (final_target[flag_col] == True)  # noqa: E712

        # Apply updates
        final_target.loc[mask, end_col] = final_target.loc[mask, "__new_end"]
        final_target.loc[mask, flag_col] = False

        # Cleanup
        final_target = final_target.drop(columns=["__new_end"])

    # 3. Combine
    result = pd.concat([final_target, all_inserts], ignore_index=True)

    # Write directly to target (self-contained, no external write: block needed)
    if str(path).endswith(".parquet") or os.path.isdir(path):
        result.to_parquet(path, index=False)
    elif str(path).endswith(".csv"):
        result.to_csv(path, index=False)

    get_logging_context().info(
        "SCD2 Pandas overwrite: wrote full history directly to target",
        target=path,
    )

    return context.with_df(result)
