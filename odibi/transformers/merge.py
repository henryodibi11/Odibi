import logging
import os

from odibi.transformations import transformation
from odibi.context import SparkContext, PandasContext

try:
    from delta.tables import DeltaTable
except ImportError:
    DeltaTable = None

logger = logging.getLogger(__name__)


@transformation("merge", category="transformer")
def merge(context, current, **params):
    """
    Merge transformer implementation.
    Handles Upsert, Append-Only, and Delete-Match strategies.

    Params:
        target (str): Target table name or path.
        keys (List[str]): List of join keys.
        strategy (str): 'upsert' (default), 'append_only', 'delete_match'.
        audit_cols (Dict): {'created_col': '...', 'updated_col': '...'}
    """
    target = params.get("target")
    keys = params.get("keys")
    strategy = params.get("strategy", "upsert")
    audit_cols = params.get("audit_cols")

    if not target:
        raise ValueError("Merge transformer requires 'target' parameter")
    if not keys:
        raise ValueError("Merge transformer requires 'keys' parameter")

    if isinstance(keys, str):
        keys = [keys]

    if isinstance(context, SparkContext):
        return _merge_spark(context, current, target, keys, strategy, audit_cols, params)
    elif isinstance(context, PandasContext):
        return _merge_pandas(context, current, target, keys, strategy, audit_cols, params)
    else:
        raise ValueError(f"Unsupported context type: {type(context)}")


def _merge_spark(context, source_df, target, keys, strategy, audit_cols, params):
    if DeltaTable is None:
        raise ImportError("Spark Merge Transformer requires 'delta-spark' package.")

    spark = context.spark

    # Import Spark functions inside the function to avoid module-level unused imports
    from pyspark.sql.functions import current_timestamp

    # Add Audit Columns to Source
    if audit_cols:
        created_col = audit_cols.get("created_col")
        updated_col = audit_cols.get("updated_col")

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

            condition = " AND ".join([f"target.{k} = source.{k}" for k in keys])
            merger = delta_table.alias("target").merge(batch_df.alias("source"), condition)

            if strategy == "upsert":
                # Construct update map
                update_expr = {}
                for col_name in batch_df.columns:
                    # Skip created_col in update
                    if audit_cols and audit_cols.get("created_col") == col_name:
                        continue
                    update_expr[col_name] = f"source.{col_name}"

                merger = merger.whenMatchedUpdate(set=update_expr)
                merger = merger.whenNotMatchedInsertAll()

            elif strategy == "append_only":
                merger = merger.whenNotMatchedInsertAll()

            elif strategy == "delete_match":
                merger = merger.whenMatchedDelete()

            merger.execute()

        else:
            # Table does not exist
            if strategy == "delete_match":
                logger.warning(f"Target {target} does not exist. Delete match skipped.")
                return

            # Initial write
            writer = batch_df.write.format("delta").mode("overwrite")

            if "/" in target or "\\" in target or ":" in target or target.startswith("."):
                writer.save(target)
            else:
                writer.saveAsTable(target)

    if source_df.isStreaming:
        # For streaming, wraps logic in foreachBatch
        query = source_df.writeStream.foreachBatch(merge_batch).start()
        return query
    else:
        merge_batch(source_df)
        return source_df


def _merge_pandas(context, source_df, target, keys, strategy, audit_cols, params):
    import pandas as pd

    # Pandas implementation for local dev (Parquet focus)
    path = target
    if not ("/" in path or "\\" in path or ":" in path or path.startswith(".")):
        # If it looks like a table name, try to treat as local path under data/
        # or just warn.
        # For MVP, assuming it's a path or resolved by user.
        pass

    # Audit columns
    now = pd.Timestamp.now()
    if audit_cols:
        created_col = audit_cols.get("created_col")
        updated_col = audit_cols.get("updated_col")

        if updated_col:
            source_df[updated_col] = now
        if created_col and created_col not in source_df.columns:
            source_df[created_col] = now

    # Check if target exists
    target_df = pd.DataFrame()
    if os.path.exists(path):
        try:
            # Try reading as parquet
            target_df = pd.read_parquet(path)
        except Exception:
            # Try deltalake if installed?
            pass

    if target_df.empty:
        if strategy == "delete_match":
            return source_df

        # Write source as initial
        os.makedirs(os.path.dirname(path), exist_ok=True)
        source_df.to_parquet(path, index=False)
        return source_df

    # Align schemas if needed (simple intersection?)
    # For now assuming schema matches or pandas handles it (NaNs)

    # Set index for update/difference
    # Ensure keys exist
    for k in keys:
        if k not in target_df.columns or k not in source_df.columns:
            raise ValueError(f"Key column '{k}' missing in target or source")

    target_df_indexed = target_df.set_index(keys)
    source_df_indexed = source_df.set_index(keys)

    if strategy == "upsert":
        # Update existing
        # NOTE: We must ensure created_col is NOT updated if it already exists
        if audit_cols and "created_col" in audit_cols:
            created_col = audit_cols["created_col"]
            # Remove created_col from source update payload if present
            # Pandas update() uses all columns in 'other' that match 'self'.
            # So we need to pass a source_df_indexed WITHOUT created_col
            cols_to_update = [c for c in source_df_indexed.columns if c != created_col]
            target_df_indexed.update(source_df_indexed[cols_to_update])
        else:
            target_df_indexed.update(source_df_indexed)

        # Append new
        new_indices = source_df_indexed.index.difference(target_df_indexed.index)
        if not new_indices.empty:
            target_df_indexed = pd.concat([target_df_indexed, source_df_indexed.loc[new_indices]])

    elif strategy == "append_only":
        # Only append new
        new_indices = source_df_indexed.index.difference(target_df_indexed.index)
        if not new_indices.empty:
            target_df_indexed = pd.concat([target_df_indexed, source_df_indexed.loc[new_indices]])

    elif strategy == "delete_match":
        # Drop indices present in source
        target_df_indexed = target_df_indexed.drop(source_df_indexed.index, errors="ignore")

    # Reset index
    final_df = target_df_indexed.reset_index()

    # Write back
    final_df.to_parquet(path, index=False)

    return source_df
