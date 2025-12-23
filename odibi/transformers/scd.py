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
    4. **Insert**: Adds a new record with `effective_time_col` as start and open-ended end date.

    **Note:** SCD2 returns a DataFrame containing the full history. You must use a `write:` block
    to persist the result (typically with `mode: overwrite` to the same location as `target`).
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
    end_time_col: str = Field(default="valid_to", description="Name of the end timestamp column")
    current_flag_col: str = Field(
        default="is_current", description="Name of the current record flag column"
    )
    delete_col: Optional[str] = Field(
        default=None, description="Column indicating soft deletion (boolean)"
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

    Returns the FULL history dataset (to be written via Overwrite).
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
                f"SCD2: connection '{params.connection}' does not support path resolution."
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
        raise ValueError(f"Unsupported engine: {context.engine_type}")

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


def _scd2_spark(context: EngineContext, source_df, params: SCD2Params) -> EngineContext:
    from pyspark.sql import functions as F

    spark = context.spark

    # 1. Check if target exists
    target_df = None
    try:
        # Try reading as table first
        target_df = spark.table(params.target)
    except Exception:
        try:
            # Try reading as Delta path
            target_df = spark.read.format("delta").load(params.target)
        except Exception:
            # Target doesn't exist yet - First Run
            pass

    # Define Columns
    eff_col = params.effective_time_col
    end_col = params.end_time_col
    flag_col = params.current_flag_col

    # Validate effective_time_col exists in source
    source_cols = source_df.columns
    if eff_col not in source_cols:
        raise ValueError(
            f"SCD2: effective_time_col '{eff_col}' not found in source DataFrame. "
            f"Available columns: {source_cols}"
        )

    # Prepare Source: Add SCD metadata columns
    # New records start as Current
    new_records = source_df.withColumn(end_col, F.lit(None).cast("timestamp")).withColumn(
        flag_col, F.lit(True)
    )

    if target_df is None:
        # First Run: Return Source prepared
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
    eff_col_preserved = "__src_eff_time"
    source_with_eff = source_df.withColumn(eff_col_preserved, F.col(eff_col))

    # Alias source_df to ensure column references are unambiguous after join
    source_aliased = source_with_eff.alias("__source")
    join_cond = [F.col(f"__source.{k}") == renamed_target[f"{t_prefix}{k}"] for k in params.keys]

    joined = source_aliased.join(renamed_target, join_cond, "left")

    # Determine Status: Changed if track columns differ
    # Use explicit __source alias for source columns to avoid ambiguity
    change_conds = []
    for col in params.track_cols:
        s_col = F.col(f"__source.{col}")
        t_col = F.col(f"{t_prefix}{col}")
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
    # Select source columns using the __source alias
    rows_to_insert = joined.filter(
        F.col(f"{t_prefix}{params.keys[0]}").isNull() | is_changed
    ).select([F.col(f"__source.{c}").alias(c) for c in source_df.columns])

    # Add metadata to inserts (Start=eff_col, End=Null, Current=True)
    rows_to_insert = rows_to_insert.withColumn(end_col, F.lit(None).cast("timestamp")).withColumn(
        flag_col, F.lit(True)
    )

    # Drop the effective_time_col (txn_date) from inserts since it's not part of target schema
    # Target schema = source columns (minus eff_col) + end_col + flag_col
    if eff_col in rows_to_insert.columns:
        rows_to_insert = rows_to_insert.drop(eff_col)

    # B) Close Old Records
    # We need to update target_df.
    # Strategy:
    # 1. Identify keys that CHANGED (from joined result)
    # Also carry over the NEW effective date from source to use as END date
    # Use the preserved eff_col_preserved column to avoid resolution issues
    changed_keys_with_date = joined.filter(is_changed).select(
        *[F.col(f"__source.{k}").alias(k) for k in params.keys],
        F.col(f"__source.{eff_col_preserved}").alias("__new_end_date"),
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

    final_target = target_updated.select(
        *[
            (
                F.when(
                    (F.col("__new_end_date").isNotNull())
                    & (F.col(f"tgt.{flag_col}") == F.lit(True)),
                    F.col("__new_end_date"),
                )
                .otherwise(F.col(f"tgt.{end_col}"))
                .alias(end_col)
                if c == end_col
                else (
                    F.when(
                        (F.col("__new_end_date").isNotNull())
                        & (F.col(f"tgt.{flag_col}") == F.lit(True)),
                        F.lit(False),
                    )
                    .otherwise(F.col(f"tgt.{c}"))
                    .alias(c)
                    if c == flag_col
                    else F.col(f"tgt.{c}")
                )
            )
            for c in target_df.columns
        ]
    )

    # 3. Union: Updated History + New Inserts
    # Ensure schemas match
    # UnionByName handles column order differences
    final_df = final_target.unionByName(rows_to_insert)

    return context.with_df(final_df)


def _scd2_pandas(context: EngineContext, source_df, params: SCD2Params) -> EngineContext:
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
            cols_select = ", ".join([f"s.{c}" for c in src_cols])

            sql_new_inserts = f"""
                SELECT {cols_select}, NULL::TIMESTAMP as {end_col}, True as {flag_col}
                FROM source_df s
                LEFT JOIN (SELECT * FROM read_parquet('{path}') WHERE {flag_col} = True) t
                ON {join_cond}
                WHERE t.{keys[0]} IS NULL
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

    # Prepare Source
    source_df = source_df.copy()
    source_df[end_col] = None
    source_df[flag_col] = True

    if target_df.empty:
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
    def has_changed(row):
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
        keys_to_close = changed_records[keys + [eff_col]].rename(columns={eff_col: "__new_end"})

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

    return context.with_df(result)
