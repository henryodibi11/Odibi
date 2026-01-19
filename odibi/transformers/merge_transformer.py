import os
import time
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from odibi.context import EngineContext, PandasContext, SparkContext
from odibi.registry import transform
from odibi.utils.logging_context import get_logging_context

try:
    from delta.tables import DeltaTable
except ImportError:
    DeltaTable = None


class MergeStrategy(str, Enum):
    UPSERT = "upsert"
    APPEND_ONLY = "append_only"
    DELETE_MATCH = "delete_match"


class AuditColumnsConfig(BaseModel):
    created_col: Optional[str] = Field(
        default=None, description="Column to set only on first insert"
    )
    updated_col: Optional[str] = Field(default=None, description="Column to update on every merge")

    @model_validator(mode="after")
    def at_least_one(self):
        if not self.created_col and not self.updated_col:
            raise ValueError(
                "Merge.audit_cols: specify at least one of 'created_col' or 'updated_col'."
            )
        return self


class MergeParams(BaseModel):
    """
    Configuration for Merge transformer (Upsert/Append).

    ### ⚖️ "GDPR & Compliance" Guide

    **Business Problem:**
    "A user exercised their 'Right to be Forgotten'. We need to remove them from our Silver tables immediately."

    **The Solution:**
    Use the `delete_match` strategy. The source dataframe contains the IDs to be deleted, and the transformer removes them from the target.

    **Recipe 1: Right to be Forgotten (Delete)**
    ```yaml
    transformer: "merge"
    params:
      target: "silver.customers"
      keys: ["customer_id"]
      strategy: "delete_match"
    ```

    **Recipe 2: Conditional Update (SCD Type 1)**
    "Only update if the source record is newer than the target record."
    ```yaml
    transformer: "merge"
    params:
      target: "silver.products"
      keys: ["product_id"]
      strategy: "upsert"
      update_condition: "source.updated_at > target.updated_at"
    ```

    **Recipe 3: Safe Insert (Filter Bad Records)**
    "Only insert records that are not marked as deleted."
    ```yaml
    transformer: "merge"
    params:
      target: "silver.orders"
      keys: ["order_id"]
      strategy: "append_only"
      insert_condition: "source.is_deleted = false"
    ```

    **Recipe 4: Audit Columns**
    "Track when records were created or updated."
    ```yaml
    transformer: "merge"
    params:
      target: "silver.users"
      keys: ["user_id"]
      audit_cols:
        created_col: "dw_created_at"
        updated_col: "dw_updated_at"
    ```

    **Recipe 5: Full Sync (Insert + Update + Delete)**
    "Sync target with source: insert new, update changed, and remove soft-deleted."
    ```yaml
    transformer: "merge"
    params:
      target: "silver.customers"
      keys: ["id"]
      strategy: "upsert"
      # 1. Delete if source says so
      delete_condition: "source.is_deleted = true"
      # 2. Update if changed (and not deleted)
      update_condition: "source.hash != target.hash"
      # 3. Insert new (and not deleted)
      insert_condition: "source.is_deleted = false"
    ```

    **Recipe 6: Connection-based Path Resolution (ADLS)**
    "Use a connection to resolve paths, just like write config."
    ```yaml
    transform:
      steps:
        - function: merge
          params:
            connection: goat_prod
            path: OEE/silver/customers
            register_table: silver.customers
            keys: ["customer_id"]
            strategy: "upsert"
            audit_cols:
              created_col: "_created_at"
              updated_col: "_updated_at"
    ```

    **Strategies:**
    *   **upsert** (Default): Update existing records, insert new ones.
    *   **append_only**: Ignore duplicates, only insert new keys.
    *   **delete_match**: Delete records in target that match keys in source.
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
        description="Relative path within connection (e.g., 'OEE/silver/customers')",
    )
    register_table: Optional[str] = Field(
        None,
        description="Register as Unity Catalog/metastore table after merge (e.g., 'silver.customers')",
    )
    keys: List[str] = Field(..., description="List of join keys")
    strategy: MergeStrategy = Field(
        default=MergeStrategy.UPSERT,
        description="Merge behavior: 'upsert', 'append_only', 'delete_match'",
    )
    audit_cols: Optional[AuditColumnsConfig] = Field(
        None, description="{'created_col': '...', 'updated_col': '...'}"
    )
    optimize_write: bool = Field(False, description="Run OPTIMIZE after write (Spark)")
    zorder_by: Optional[List[str]] = Field(None, description="Columns to Z-Order by")
    cluster_by: Optional[List[str]] = Field(
        None, description="Columns to Liquid Cluster by (Delta)"
    )
    update_condition: Optional[str] = Field(
        None, description="SQL condition for update clause (e.g. 'source.ver > target.ver')"
    )
    insert_condition: Optional[str] = Field(
        None, description="SQL condition for insert clause (e.g. 'source.status != \"deleted\"')"
    )
    delete_condition: Optional[str] = Field(
        None, description="SQL condition for delete clause (e.g. 'source.status = \"deleted\"')"
    )
    table_properties: Optional[dict] = Field(
        None,
        description="Delta table properties for initial table creation (e.g., column mapping)",
    )

    @field_validator("keys")
    @classmethod
    def check_keys(cls, v):
        if not v:
            raise ValueError(
                "Merge: 'keys' must not be empty. "
                "Provide at least one column name to join source and target on. "
                f"Got: {v!r}"
            )
        return v

    @model_validator(mode="after")
    def check_target_or_connection(self):
        """Ensure either target or connection+path is provided."""
        if not self.target and not (self.connection and self.path):
            raise ValueError("Merge: provide either 'target' OR both 'connection' and 'path'.")
        if self.target and (self.connection or self.path):
            raise ValueError("Merge: use 'target' OR 'connection'+'path', not both.")
        return self

    @model_validator(mode="after")
    def check_strategy_and_audit(self):
        if self.strategy == MergeStrategy.DELETE_MATCH and self.audit_cols:
            raise ValueError("Merge: 'audit_cols' is not used with strategy='delete_match'.")
        return self


@transform("merge", category="transformer", param_model=MergeParams)
def merge(context, params=None, current=None, **kwargs):
    """
    Merge transformer implementation.
    Handles Upsert, Append-Only, and Delete-Match strategies.

    Args:
        context: EngineContext (preferred) or legacy PandasContext/SparkContext
        params: MergeParams object (when called via function step) or DataFrame (legacy)
        current: DataFrame (legacy positional arg, deprecated)
        **kwargs: Parameters when not using MergeParams
    """
    ctx = get_logging_context()
    start_time = time.time()

    # Handle legacy signature: merge(context, source_df, **params)
    # where params (2nd arg) is actually the DataFrame
    if params is not None and not isinstance(params, MergeParams):
        # Legacy call: params is actually the DataFrame
        current = params
        merge_params = MergeParams(**kwargs)
    elif isinstance(params, MergeParams):
        merge_params = params
    else:
        merge_params = MergeParams(**kwargs)

    # Get current DataFrame: prefer explicit current, then context.df
    if current is None:
        if hasattr(context, "df"):
            current = context.df
        else:
            raise ValueError(
                f"Merge requires a DataFrame but none was provided. "
                f"Either pass a DataFrame as the 'current' argument, or ensure context.df is set. "
                f"Context type: {type(context).__name__}. Has 'df' attr: {hasattr(context, 'df')}."
            )

    # Resolve target path from connection if provided
    target = merge_params.target
    register_table = merge_params.register_table

    if merge_params.connection and merge_params.path:
        # Resolve path via connection
        connection = None
        if hasattr(context, "engine") and hasattr(context.engine, "connections"):
            connections = context.engine.connections
            if connections and merge_params.connection in connections:
                connection = connections[merge_params.connection]

        if connection is None:
            raise ValueError(
                f"Merge: connection '{merge_params.connection}' not found. "
                "Ensure the connection is defined in your project config."
            )

        if hasattr(connection, "get_path"):
            target = connection.get_path(merge_params.path)
            ctx.debug(
                "Resolved merge target path via connection",
                connection=merge_params.connection,
                relative_path=merge_params.path,
                resolved_path=target,
            )
        else:
            raise ValueError(
                f"Merge: connection '{merge_params.connection}' (type: {type(connection).__name__}) "
                f"does not support path resolution. Expected a connection with 'get_path' method. "
                f"Connection type must be 'local', 'adls', or similar file-based connection."
            )

    ctx.debug(
        "Merge starting",
        target=target,
        keys=merge_params.keys,
        strategy=merge_params.strategy.value,
        register_table=register_table,
    )

    # Get source row count
    rows_before = None
    try:
        rows_before = current.shape[0] if hasattr(current, "shape") else None
        if rows_before is None and hasattr(current, "count"):
            rows_before = current.count()
    except Exception as e:
        ctx.debug(f"Could not get row count: {type(e).__name__}")

    ctx.debug("Merge source loaded", source_rows=rows_before)

    # Unwrap EngineContext if present
    real_context = context
    if isinstance(context, EngineContext):
        real_context = context.context

    keys = merge_params.keys
    strategy = merge_params.strategy
    audit_cols = merge_params.audit_cols

    # Optimization params
    optimize_write = merge_params.optimize_write
    zorder_by = merge_params.zorder_by
    cluster_by = merge_params.cluster_by

    if isinstance(real_context, SparkContext):
        result = _merge_spark(
            context,
            current,
            target,
            keys,
            strategy,
            audit_cols,
            optimize_write,
            zorder_by,
            cluster_by,
            merge_params.update_condition,
            merge_params.insert_condition,
            merge_params.delete_condition,
            merge_params.table_properties,
            kwargs,
        )
    elif isinstance(real_context, PandasContext):
        result = _merge_pandas(context, current, target, keys, strategy, audit_cols, kwargs)
    else:
        ctx.error("Merge failed: unsupported context", context_type=str(type(real_context)))
        raise ValueError(f"Unsupported context type: {type(real_context)}")

    # Register table in metastore if requested (Spark only)
    if register_table and isinstance(real_context, SparkContext):
        try:
            spark = context.spark
            if spark:
                ctx.debug(
                    "Registering table in metastore",
                    table_name=register_table,
                    location=target,
                )
                spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {register_table} USING DELTA LOCATION '{target}'"
                )
                ctx.info(
                    "Table registered successfully",
                    table_name=register_table,
                    location=target,
                )
        except Exception as e:
            ctx.warning(
                f"Failed to register table: {e}",
                table_name=register_table,
                error=str(e),
            )

    elapsed_ms = (time.time() - start_time) * 1000
    ctx.debug(
        "Merge completed",
        target=target,
        strategy=merge_params.strategy.value,
        source_rows=rows_before,
        elapsed_ms=round(elapsed_ms, 2),
    )

    return result


def _merge_spark(
    context,
    source_df,
    target,
    keys,
    strategy,
    audit_cols,
    optimize_write,
    zorder_by,
    cluster_by,
    update_condition,
    insert_condition,
    delete_condition,
    table_properties,
    params,
):
    """
    Internal helper for merge transformer on Spark engine.

    Handles upsert, append-only, and delete-match strategies using Delta Lake.
    Resolves target as table or path, manages audit columns, schema evolution, and post-merge optimization.
    """
    if DeltaTable is None:
        raise ImportError("Spark Merge Transformer requires 'delta-spark' package.")

    spark = context.spark

    # Import Spark functions inside the function to avoid module-level unused imports
    from pyspark.sql.functions import current_timestamp

    # Add Audit Columns to Source
    if audit_cols:
        created_col = audit_cols.created_col
        updated_col = audit_cols.updated_col

        if updated_col:
            source_df = source_df.withColumn(updated_col, current_timestamp())

        if created_col and created_col not in source_df.columns:
            source_df = source_df.withColumn(created_col, current_timestamp())

    def get_delta_table():
        # Heuristic: if it looks like a path, use forPath, else forName
        # Path indicators: /, \, :, or starts with .
        if "/" in target or "\\" in target or ":" in target or target.startswith("."):
            return DeltaTable.forPath(spark, target)
        return DeltaTable.forName(spark, target)

    def merge_batch(batch_df, batch_id=None):
        # Check if table exists
        is_delta = False
        try:
            if "/" in target or "\\" in target or ":" in target or target.startswith("."):
                is_delta = DeltaTable.isDeltaTable(spark, target)
            else:
                # For table name, try to access it
                try:
                    DeltaTable.forName(spark, target)
                    is_delta = True
                except Exception:
                    is_delta = False
        except Exception:
            is_delta = False

        if is_delta:
            delta_table = get_delta_table()

            condition = " AND ".join([f"target.`{k}` = source.`{k}`" for k in keys])
            merger = delta_table.alias("target").merge(batch_df.alias("source"), condition)

            orig_auto_merge = None
            if strategy == MergeStrategy.UPSERT:
                # Construct update map
                update_expr = {}
                for col_name in batch_df.columns:
                    # Skip created_col in update
                    if audit_cols and audit_cols.created_col == col_name:
                        continue

                    # Note: When Delta Merge UPDATE SET uses column names from source that
                    # do NOT exist in target, it throws UNRESOLVED_EXPRESSION if schema evolution
                    # is not enabled or handled automatically by the merge operation for updates.

                    update_expr[f"`{col_name}`"] = f"source.`{col_name}`"

                # Enable automatic schema evolution for the merge
                # This is critical for adding new columns (like audit cols)

                # Capture original state to avoid side effects
                orig_auto_merge = spark.conf.get(
                    "spark.databricks.delta.schema.autoMerge.enabled", "false"
                )
                spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

                if delete_condition:
                    merger = merger.whenMatchedDelete(condition=delete_condition)

                merger = merger.whenMatchedUpdate(set=update_expr, condition=update_condition)
                merger = merger.whenNotMatchedInsertAll(condition=insert_condition)

            elif strategy == MergeStrategy.APPEND_ONLY:
                merger = merger.whenNotMatchedInsertAll(condition=insert_condition)

            elif strategy == MergeStrategy.DELETE_MATCH:
                merger = merger.whenMatchedDelete(condition=delete_condition)

            try:
                merger.execute()
            finally:
                # Restore configuration if we changed it
                if orig_auto_merge is not None:
                    spark.conf.set(
                        "spark.databricks.delta.schema.autoMerge.enabled", orig_auto_merge
                    )

        else:
            # Table does not exist
            if strategy == MergeStrategy.DELETE_MATCH:
                get_logging_context().warning(
                    f"Target {target} does not exist. Delete match skipped."
                )
                return

            # Initial write
            # If cluster_by is present, we delegate to engine.write logic?
            # Or implement CTAS here similar to engine.write

            # Build TBLPROPERTIES clause if table_properties provided
            tbl_props_clause = ""
            if table_properties:
                props_str = ", ".join(f"'{k}' = '{v}'" for k, v in table_properties.items())
                tbl_props_clause = f" TBLPROPERTIES ({props_str})"

            if cluster_by:
                # Use CTAS logic for Liquid Clustering creation
                if isinstance(cluster_by, str):
                    cluster_cols = [cluster_by]
                else:
                    cluster_cols = cluster_by

                cols = ", ".join(f"`{c}`" for c in cluster_cols)
                # Create temp view
                temp_view = f"odibi_merge_init_{abs(hash(target))}"
                batch_df.createOrReplaceTempView(temp_view)

                # Determine target type (path vs table)
                is_path = "/" in target or "\\" in target or ":" in target or target.startswith(".")
                target_identifier = f"delta.`{target}`" if is_path else target

                spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {target_identifier} USING DELTA{tbl_props_clause} CLUSTER BY ({cols}) AS SELECT * FROM {temp_view}"
                )
                spark.catalog.dropTempView(temp_view)
            else:
                # Create temp view for CTAS with properties
                temp_view = f"odibi_merge_init_{abs(hash(target))}"
                batch_df.createOrReplaceTempView(temp_view)

                is_path = "/" in target or "\\" in target or ":" in target or target.startswith(".")
                target_identifier = f"delta.`{target}`" if is_path else target

                if table_properties:
                    # Use CTAS to apply table properties
                    spark.sql(
                        f"CREATE TABLE IF NOT EXISTS {target_identifier} USING DELTA{tbl_props_clause} AS SELECT * FROM {temp_view}"
                    )
                    spark.catalog.dropTempView(temp_view)
                else:
                    # Original path: use DataFrameWriter
                    spark.catalog.dropTempView(temp_view)
                    writer = batch_df.write.format("delta").mode("overwrite")

                    if is_path:
                        writer.save(target)
                    else:
                        writer.saveAsTable(target)

        # --- Post-Merge Optimization ---
        if optimize_write or zorder_by:
            try:
                # Identify if target is table or path
                is_path = "/" in target or "\\" in target or ":" in target or target.startswith(".")

                if is_path:
                    sql = f"OPTIMIZE delta.`{target}`"
                else:
                    sql = f"OPTIMIZE {target}"

                if zorder_by:
                    if isinstance(zorder_by, str):
                        zorder_cols = [zorder_by]
                    else:
                        zorder_cols = zorder_by

                    cols = ", ".join(f"`{c}`" for c in zorder_cols)
                    sql += f" ZORDER BY ({cols})"

                spark.sql(sql)
            except Exception as e:
                get_logging_context().warning(f"Optimization failed for {target}: {e}")

    if source_df.isStreaming:
        # For streaming, wraps logic in foreachBatch
        query = source_df.writeStream.foreachBatch(merge_batch).start()
        return query
    else:
        merge_batch(source_df)
        return source_df


def _merge_pandas(context, source_df, target, keys, strategy, audit_cols, params):
    """
    Internal helper for merge transformer on Pandas engine.

    Handles upsert, append-only, and delete-match strategies using Pandas and optionally DuckDB.
    Resolves target as path, manages audit columns, and performs atomic writes.
    """
    import pandas as pd

    # Try using DuckDB for scalability if available
    try:
        import duckdb

        HAS_DUCKDB = True
    except ImportError:
        HAS_DUCKDB = False

    # Pandas implementation for local dev (Parquet focus)
    path = target

    # Resolve path if context has engine (EngineContext)
    if hasattr(context, "engine") and context.engine:
        # Try to resolve 'connection.path'
        if "." in target:
            parts = target.split(".", 1)
            conn_name = parts[0]
            rel_path = parts[1]
            if conn_name in context.engine.connections:
                try:
                    path = context.engine.connections[conn_name].get_path(rel_path)
                except Exception as e:
                    get_logging_context().debug(
                        f"Could not resolve connection path: {type(e).__name__}"
                    )

    if not ("/" in path or "\\" in path or ":" in path or path.startswith(".")):
        # If it looks like a table name, try to treat as local path under data/
        # or just warn.
        # For MVP, assuming it's a path or resolved by user.
        pass

    # Audit columns
    now = pd.Timestamp.now()
    if audit_cols:
        created_col = audit_cols.created_col
        updated_col = audit_cols.updated_col

        if updated_col:
            source_df[updated_col] = now
        if created_col and created_col not in source_df.columns:
            source_df[created_col] = now

    # Check if target exists
    target_exists = False
    if os.path.exists(path):
        # Check if it's a file or directory (DuckDB handles parquet files)
        target_exists = True

    # --- DUCKDB PATH ---
    if HAS_DUCKDB and str(path).endswith(".parquet"):
        try:
            con = duckdb.connect(database=":memory:")

            # Register source_df
            con.register("source_df", source_df)

            if not target_exists:
                if strategy == MergeStrategy.DELETE_MATCH:
                    return source_df  # Nothing to delete from

                # Initial Write
                os.makedirs(os.path.dirname(path), exist_ok=True)
                con.execute(f"COPY (SELECT * FROM source_df) TO '{path}' (FORMAT PARQUET)")
                return source_df

            # Construct Merge Query
            # We need to quote columns properly? DuckDB usually handles simple names.
            # Assuming keys are simple.

            # Join condition: s.k1 = t.k1 AND s.k2 = t.k2
            # Quote column names with double quotes for DuckDB compatibility
            join_cond = " AND ".join([f's."{k}" = t."{k}"' for k in keys])

            query = ""
            if strategy == MergeStrategy.UPSERT:
                # Logic: (Source) UNION ALL (Target WHERE NOT EXISTS in Source)
                # Note: This replaces the whole row with Source version (Update)
                # Special handling for created_col: If updating, preserve target's created_col?

                # If created_col exists, we want to use Target's created_col for updates?
                # But "Source" row has new created_col (current time) which is wrong for update.
                # Ideally: SELECT s.* EXCEPT (created_col), t.created_col ...
                # But 'EXCEPT' is post-projection.
                # Simpler: Just overwrite. If user wants to preserve, they shouldn't overwrite it in source.
                # BUT audit logic above set created_col in source.
                # If we are strictly upserting, maybe we should handle it.
                # For performance, let's stick to standard Upsert (Source wins).

                query = f"""
                    SELECT * FROM source_df
                    UNION ALL
                    SELECT * FROM read_parquet('{path}') t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM source_df s WHERE {join_cond}
                    )
                """

            elif strategy == MergeStrategy.APPEND_ONLY:
                # Logic: (Source WHERE NOT EXISTS in Target) UNION ALL (Target)
                query = f"""
                    SELECT * FROM source_df s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM read_parquet('{path}') t WHERE {join_cond}
                    )
                    UNION ALL
                    SELECT * FROM read_parquet('{path}')
                """

            elif strategy == MergeStrategy.DELETE_MATCH:
                # Logic: Target WHERE NOT EXISTS in Source
                query = f"""
                    SELECT * FROM read_parquet('{path}') t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM source_df s WHERE {join_cond}
                    )
                """

            # Execute Atomic Write
            # Write to temp file then rename
            temp_path = str(path) + ".tmp.parquet"
            con.execute(f"COPY ({query}) TO '{temp_path}' (FORMAT PARQUET)")

            # Close connection before file ops
            con.close()

            # Replace
            if os.path.exists(temp_path):
                if os.path.exists(path):
                    os.remove(path)
                os.rename(temp_path, path)

            return source_df

        except Exception as e:
            # Fallback to Pandas if DuckDB fails (e.g. complex types, memory)
            get_logging_context().warning(f"DuckDB merge failed, falling back to Pandas: {e}")
            pass

    # --- PANDAS FALLBACK ---
    target_df = pd.DataFrame()
    if os.path.exists(path):
        try:
            # Try reading as parquet
            target_df = pd.read_parquet(path)
        except Exception as e:
            get_logging_context().debug(f"Could not read target file: {type(e).__name__}")

    if target_df.empty:
        if strategy == MergeStrategy.DELETE_MATCH:
            return source_df

        # Write source as initial
        os.makedirs(os.path.dirname(path), exist_ok=True)
        source_df.to_parquet(path, index=False)
        return source_df

    # Align schemas if needed (simple intersection?)
    # For now, assuming schema matches or pandas handles it (NaNs)

    # Set index for update/difference
    # Ensure keys exist
    for k in keys:
        if k not in target_df.columns or k not in source_df.columns:
            raise ValueError(
                f"Merge key column '{k}' not found in DataFrame. "
                f"Target columns: {list(target_df.columns)}. Source columns: {list(source_df.columns)}. "
                f"Check your 'keys' configuration matches actual column names."
            )

    target_df_indexed = target_df.set_index(keys)
    source_df_indexed = source_df.set_index(keys)

    if strategy == MergeStrategy.UPSERT:
        # Update existing
        # NOTE: We must ensure created_col is NOT updated if it already exists
        if audit_cols and audit_cols.created_col:
            created_col = audit_cols.created_col
            # Remove created_col from source update payload if present
            cols_to_update = [c for c in source_df_indexed.columns if c != created_col]
            target_df_indexed.update(source_df_indexed[cols_to_update])
        else:
            target_df_indexed.update(source_df_indexed)

        # Append new
        new_indices = source_df_indexed.index.difference(target_df_indexed.index)
        if not new_indices.empty:
            target_df_indexed = pd.concat([target_df_indexed, source_df_indexed.loc[new_indices]])

    elif strategy == MergeStrategy.APPEND_ONLY:
        # Only append new
        new_indices = source_df_indexed.index.difference(target_df_indexed.index)
        if not new_indices.empty:
            target_df_indexed = pd.concat([target_df_indexed, source_df_indexed.loc[new_indices]])

    elif strategy == MergeStrategy.DELETE_MATCH:
        # Drop indices present in source
        target_df_indexed = target_df_indexed.drop(source_df_indexed.index, errors="ignore")

    # Reset index
    final_df = target_df_indexed.reset_index()

    # Write back
    final_df.to_parquet(path, index=False)

    return source_df
