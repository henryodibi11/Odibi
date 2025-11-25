import os
from typing import Any, List, Optional

from pydantic import BaseModel, Field

from odibi.context import EngineContext
from odibi.enums import EngineType


class SCD2Params(BaseModel):
    """
    Parameters for SCD Type 2 (Slowly Changing Dimensions) transformer.

    ### 🕰️ The "Time Machine" Pattern

    **Business Problem:**
    "I need to know what the customer's address was *last month*, not just where they live now."

    **The Solution:**
    SCD Type 2 tracks the full history of changes. Each record has an "effective window" (start/end dates) and a flag indicating if it is the current version.

    **Recipe:**
    ```yaml
    transformer: "scd2"
    params:
      target: "gold/customers"         # Path to existing history
      keys: ["customer_id"]            # How we identify the entity
      track_cols: ["address", "tier"]  # What changes we care about
      effective_time_col: "txn_date"   # When the change actually happened
      end_time_col: "valid_to"         # (Optional) Name of closing timestamp
      current_flag_col: "is_active"    # (Optional) Name of current flag
    ```

    **How it works:**
    1. **Match**: Finds existing records using `keys`.
    2. **Compare**: Checks `track_cols` to see if data changed.
    3. **Close**: If changed, updates the old record's `end_time_col` to the new `effective_time_col`.
    4. **Insert**: Adds a new record with `effective_time_col` as start and open-ended end date.
    """

    target: str = Field(..., description="Target table name or path containing history")
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


def scd2(context: EngineContext, params: SCD2Params, current: Any = None) -> EngineContext:
    """
    Implements SCD Type 2 Logic.

    Returns the FULL history dataset (to be written via Overwrite).
    """
    source_df = context.df if current is None else current

    if context.engine_type == EngineType.SPARK:
        return _scd2_spark(context, source_df, params)
    elif context.engine_type == EngineType.PANDAS:
        return _scd2_pandas(context, source_df, params)
    else:
        raise ValueError(f"Unsupported engine: {context.engine_type}")


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

    join_cond = [source_df[k] == renamed_target[f"{t_prefix}{k}"] for k in params.keys]

    joined = source_df.join(renamed_target, join_cond, "left")

    # Determine Status: Changed if track columns differ
    change_conds = []
    for col in params.track_cols:
        s_col = F.col(col)
        t_col = F.col(f"{t_prefix}{col}")
        # Null-safe equality check: NOT (source <=> target)
        change_conds.append(F.not_(s_col.eqNullSafe(t_col)))

    if change_conds:
        from functools import reduce

        is_changed = reduce(lambda a, b: a | b, change_conds)
    else:
        is_changed = F.lit(False)

    # A) Rows to Insert (New Keys OR Changed Keys)
    # Filter: TargetKey IS NULL OR is_changed
    rows_to_insert = joined.filter(
        F.col(f"{t_prefix}{params.keys[0]}").isNull() | is_changed
    ).select(
        source_df.columns
    )  # Select original source columns

    # Add metadata to inserts (Start=eff_col, End=Null, Current=True)
    rows_to_insert = rows_to_insert.withColumn(end_col, F.lit(None).cast("timestamp")).withColumn(
        flag_col, F.lit(True)
    )

    # B) Close Old Records
    # We need to update target_df.
    # Strategy:
    # 1. Identify keys that CHANGED (from joined result)
    # Also carry over the NEW effective date from source to use as END date
    changed_keys_with_date = joined.filter(is_changed).select(
        *[F.col(k) for k in params.keys], F.col(eff_col).alias("__new_end_date")
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
    import pandas as pd

    # 1. Load Target
    path = params.target
    target_df = pd.DataFrame()

    # Try loading if exists
    if os.path.exists(path):
        try:
            # Naive format detection or try/except
            if str(path).endswith(".parquet") or os.path.isdir(path):  # Parquet often directory
                target_df = pd.read_parquet(path)
            elif str(path).endswith(".csv"):
                target_df = pd.read_csv(path)
        except Exception:
            pass

    # Define Cols
    keys = params.keys
    eff_col = params.effective_time_col
    end_col = params.end_time_col
    flag_col = params.current_flag_col
    track = params.track_cols

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
