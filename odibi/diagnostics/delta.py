"""
Delta Lake Diagnostics
======================

Tools for analyzing Delta Lake tables, history, and drift.
"""

from typing import Any, Dict, Optional, List
from dataclasses import dataclass


@dataclass
class DeltaDiffResult:
    """Result of comparing two Delta table versions."""

    table_path: str
    version_a: int
    version_b: int

    # Metadata changes
    rows_change: int
    files_change: int
    size_change_bytes: int

    # Schema changes
    schema_added: List[str]
    schema_removed: List[str]

    schema_current: Optional[List[str]] = None
    schema_previous: Optional[List[str]] = None

    rows_added: Optional[int] = None
    rows_removed: Optional[int] = None
    rows_updated: Optional[int] = None

    # Operation info
    operations: List[str] = None  # List of operations that happened between versions

    # Data Diff Samples (Optional)
    sample_added: Optional[List[Dict[str, Any]]] = None
    sample_removed: Optional[List[Dict[str, Any]]] = None
    sample_updated: Optional[List[Dict[str, Any]]] = None


def get_delta_diff(
    table_path: str,
    version_a: int,
    version_b: int,
    spark: Optional[Any] = None,
    deep: bool = False,
    keys: Optional[List[str]] = None,
) -> DeltaDiffResult:
    """
    Compare two versions of a Delta table.

    Args:
        table_path: Path to Delta table
        version_a: Start version
        version_b: End version
        spark: Optional SparkSession. If None, uses deltalake (Pandas).
        deep: If True, perform expensive row-by-row comparison (exceptAll).
              If False, rely on metadata and stats.
        keys: List of primary key columns for detecting updates.

    Returns:
        DeltaDiffResult object
    """
    if spark:
        return _get_delta_diff_spark(spark, table_path, version_a, version_b, deep, keys)
    else:
        return _get_delta_diff_pandas(table_path, version_a, version_b, deep, keys)


def _get_delta_diff_spark(
    spark: Any,
    table_path: str,
    version_a: int,
    version_b: int,
    deep: bool = False,
    keys: Optional[List[str]] = None,
) -> DeltaDiffResult:
    """Spark implementation of delta diff."""
    try:
        from delta.tables import DeltaTable
    except ImportError:
        raise ImportError("Delta Lake support requires 'delta-spark'")

    dt = DeltaTable.forPath(spark, table_path)
    history = dt.history().collect()

    # Filter history between versions
    # We want everything happening AFTER version_a up to version_b
    # History is usually reverse ordered, but let's filter safely
    relevant_commits = [
        row
        for row in history
        if min(version_a, version_b) < row["version"] <= max(version_a, version_b)
    ]

    operations = [row["operation"] for row in relevant_commits]

    # Calculate expected row changes from metrics if available
    rows_change = 0
    files_change = 0
    bytes_change = 0

    for commit in relevant_commits:
        metrics = commit.get("operationMetrics", {}) or {}

        # This is heuristic based on operation type, but usually:
        # Inserted - Deleted
        inserted = int(metrics.get("numTargetRowsInserted", 0) or metrics.get("numOutputRows", 0))
        deleted = int(metrics.get("numTargetRowsDeleted", 0))

        # Direction matters. If we go a -> b and b > a, we sum up.
        # If b < a, we revert. Assuming a < b here for simplicity of diff
        factor = 1 if version_b > version_a else -1

        rows_change += (inserted - deleted) * factor

        # Files
        files_added = int(metrics.get("numFilesAdded", 0) or metrics.get("numAddedFiles", 0))
        files_removed = int(metrics.get("numFilesRemoved", 0) or metrics.get("numRemovedFiles", 0))
        files_change += (files_added - files_removed) * factor

        # Bytes
        bytes_added = int(metrics.get("numBytesAdded", 0) or metrics.get("numAddedBytes", 0))
        bytes_removed = int(metrics.get("numBytesRemoved", 0) or metrics.get("numRemovedBytes", 0))
        bytes_change += (bytes_added - bytes_removed) * factor

    # Get snapshots for schema
    # Note: Spark is lazy, so defining DF is cheap, but we need schema.
    # We can get schema from history? No, only from snapshot.
    df_a = spark.read.format("delta").option("versionAsOf", version_a).load(table_path)
    df_b = spark.read.format("delta").option("versionAsOf", version_b).load(table_path)

    schema_a = set(df_a.columns)
    schema_b = set(df_b.columns)

    # Deep Diff Logic
    added_rows = None
    removed_rows = None
    updated_rows = None
    rows_added_count = None
    rows_removed_count = None
    rows_updated_count = None

    if deep:
        # Actual row counts (authoritative vs metrics heuristic)
        rows_a = df_a.count()
        rows_b = df_b.count()
        rows_change = rows_b - rows_a  # Override heuristic

        common_cols = list(schema_a.intersection(schema_b))
        if common_cols:
            df_a_common = df_a.select(*common_cols)
            df_b_common = df_b.select(*common_cols)

            if keys and set(keys).issubset(common_cols):
                # --- Spark Key-Based Diff ---
                # Join on keys to find Added, Removed, and Updated

                # 1. Added: In B but not in A (based on keys)
                # df_b_common left_anti df_a_common on keys
                diff_added = df_b_common.join(df_a_common, keys, "left_anti")
                rows_added_count = diff_added.count()
                added_rows = [row.asDict() for row in diff_added.limit(10).collect()]

                # 2. Removed: In A but not in B (based on keys)
                # df_a_common left_anti df_b_common on keys
                diff_removed = df_a_common.join(df_b_common, keys, "left_anti")
                rows_removed_count = diff_removed.count()
                removed_rows = [row.asDict() for row in diff_removed.limit(10).collect()]

                # 3. Updates: In both (inner join), but value columns differ
                value_cols = [c for c in common_cols if c not in keys]

                # Rename columns in A to avoid ambiguity
                # We can alias DataFrames
                df_a_aliased = df_a_common.alias("a")
                df_b_aliased = df_b_common.alias("b")

                # Build filter condition
                from pyspark.sql import functions as F

                # Start with False
                change_condition = F.lit(False)

                for col in value_cols:
                    # logical_or of existing condition AND (col_a != col_b)
                    # utilizing equalNullSafe inverted: not(a <=> b)
                    col_changed = ~F.col(f"a.{col}").eqNullSafe(F.col(f"b.{col}"))
                    change_condition = change_condition | col_changed

                # Inner Join + Filter
                # Join condition is equality on keys
                join_cond = [F.col(f"a.{k}") == F.col(f"b.{k}") for k in keys]

                diff_updated = (
                    df_b_aliased.join(df_a_aliased, join_cond, "inner")
                    .filter(change_condition)
                    .select("b.*")  # We return the 'new' state
                )

                rows_updated_count = diff_updated.count()

                # Let's grab the top 10 updated rows (new state)
                updated_rows = [row.asDict() for row in diff_updated.limit(10).collect()]

            else:
                # Fallback to Set Diff if keys not supported/implemented fully for Spark yet
                # or if keys not provided
                diff_added = df_b_common.exceptAll(df_a_common)
                diff_removed = df_a_common.exceptAll(df_b_common)

                # Get counts
                rows_added_count = diff_added.count()
                rows_removed_count = diff_removed.count()

                added_rows = [row.asDict() for row in diff_added.limit(10).collect()]
                removed_rows = [row.asDict() for row in diff_removed.limit(10).collect()]

    return DeltaDiffResult(
        table_path=table_path,
        version_a=version_a,
        version_b=version_b,
        rows_change=rows_change,
        files_change=files_change,
        size_change_bytes=bytes_change,
        schema_added=list(schema_b - schema_a),
        schema_removed=list(schema_a - schema_b),
        schema_current=sorted(list(schema_b)),
        schema_previous=sorted(list(schema_a)),
        rows_added=rows_added_count,
        rows_removed=rows_removed_count,
        rows_updated=rows_updated_count,
        sample_added=added_rows,
        sample_removed=removed_rows,
        sample_updated=updated_rows,
        operations_between=operations,
    )


def _get_delta_diff_pandas(
    table_path: str,
    version_a: int,
    version_b: int,
    deep: bool = False,
    keys: Optional[List[str]] = None,
) -> DeltaDiffResult:
    """Pandas (deltalake) implementation of delta diff."""
    try:
        from deltalake import DeltaTable
        import pandas as pd
    except ImportError:
        raise ImportError("Delta Lake support requires 'deltalake' and 'pandas'")

    dt = DeltaTable(table_path)

    # History
    history = dt.history()
    relevant_commits = [
        h for h in history if min(version_a, version_b) < h["version"] <= max(version_a, version_b)
    ]
    operations = [h["operation"] for h in relevant_commits]

    # Heuristics for metrics not easily available in pandas wrapper directly per commit object in standard history
    # But we can just use len() since we load the table anyway in pandas logic?
    # Wait, loading entire table in pandas is expensive.
    # deltalake supports 'file_uris()' which is cheap.

    # Snapshots
    dt.load_as_version(version_a)
    # Getting schema without loading data?
    # Check for API availability (breaking changes in deltalake 0.15+)
    schema_obj = dt.schema()
    if hasattr(schema_obj, "to_pyarrow"):
        arrow_schema_a = schema_obj.to_pyarrow()
    else:
        arrow_schema_a = schema_obj.to_arrow()

    schema_a = set(arrow_schema_a.names)

    # For row count without loading:
    # dt.to_pyarrow_dataset().count_rows() ??
    # Currently deltalake 0.10+ has rudimentary stats.
    # Let's assume for Pandas local execution, data is small enough to load OR we skip stats.
    # Actually, let's just load head(0) for schema if possible? No, dt.to_pandas() loads all.

    # Optimization: Use pyarrow dataset scanner count if available
    try:

        rows_a = len(dt.to_pandas())  # Fallback for now
    except Exception:
        rows_a = 0

    # If deep=False, we might want to avoid to_pandas().
    # But `deltalake` lib is optimized for single node.
    # Let's assume we load it if we can.
    df_a = dt.to_pandas()

    dt.load_as_version(version_b)
    df_b = dt.to_pandas()

    rows_b = len(df_b)
    schema_b = set(df_b.columns)

    rows_change = rows_b - rows_a

    added_rows = None
    removed_rows = None
    updated_rows = None
    rows_added_count = None
    rows_removed_count = None
    rows_updated_count = None

    if deep:
        # Compute Data Diff
        # Pandas doesn't have exceptAll. We use merge with indicator.
        common_cols = list(schema_a.intersection(schema_b))

        if common_cols:
            # DO NOT restrict inputs to common_cols yet, or we lose new/old data for samples

            if keys and set(keys).issubset(common_cols):
                # --- KEY-BASED DIFF (Updates Supported) ---
                # Outer merge on KEYS only
                merged = df_b.merge(
                    df_a, on=keys, how="outer", suffixes=("", "_old"), indicator=True
                )

                # Added: Key in B only
                added_df = merged[merged["_merge"] == "left_only"]

                # Removed: Key in A only
                removed_df = merged[merged["_merge"] == "right_only"]

                # Potential Updates: Key in Both
                both_df = merged[merged["_merge"] == "both"]

                # For "both", check if value cols changed
                # We need to compare common cols that are not keys
                value_cols = [c for c in common_cols if c not in keys]

                updated_records = []

                for _, row in both_df.iterrows():
                    changes = {}
                    has_change = False
                    for col in value_cols:
                        new_val = row[col]
                        old_val = row[f"{col}_old"]

                        # Handle nulls/NaN equality
                        if pd.isna(new_val) and pd.isna(old_val):
                            continue
                        if new_val != old_val:
                            changes[col] = {"old": old_val, "new": new_val}
                            has_change = True

                    if has_change:
                        # Build a record that has Keys + Changes
                        rec = {k: row[k] for k in keys}
                        rec["_changes"] = changes
                        updated_records.append(rec)

                rows_added_count = len(added_df)
                rows_removed_count = len(removed_df)
                rows_updated_count = len(updated_records)

                # Format added/removed to regular dicts (drop _old cols and _merge)
                # For Added, we want ALL columns in B (schema_b)
                # Note: added_df comes from df_b mostly, but merged might have _old cols (NaNs)
                # We select columns that are in schema_b
                cols_b = list(schema_b)
                added_rows = added_df[cols_b].head(10).to_dict("records")

                # For Removed, we want ALL columns in A (schema_a)
                # 'removed_df' has columns from B (NaN) and columns from A (with _old suffix usually, OR common ones)
                # Wait, merge suffixes apply to overlapping columns.
                # Keys are shared.
                # Columns unique to B are present (NaN).
                # Columns unique to A are present?
                #   If unique to A (dropped col), it's in df_a but not df_b.
                #   Merge retains it. Does it have suffix?
                #   No, if not in df_b, no collision -> no suffix.
                #   BUT, common columns have collision -> suffix.

                # Reconstruct deleted row:
                # 1. Keys (no suffix)
                # 2. Common non-keys (suffix _old)
                # 3. Unique to A (no suffix)
                removed_clean = []
                for _, row in removed_df.head(10).iterrows():
                    rec = {}
                    for col in schema_a:
                        if col in keys:
                            rec[col] = row[col]
                        elif col in common_cols:
                            # It was common, so it collided. In right_only, we want the 'right' version.
                            # Suffix applied to 'left' (B) is "" and 'right' (A) is "_old".
                            rec[col] = row[f"{col}_old"]
                        else:
                            # Unique to A (deleted column). No collision.
                            if col in row:
                                rec[col] = row[col]
                    removed_clean.append(rec)
                removed_rows = removed_clean

                updated_rows = updated_records[:10]

            else:
                # --- SET-BASED DIFF (No Keys) ---
                # Merge on all common columns
                # Note: We can't easily detect updates here, just Add/Remove
                # If we merge on common_cols, we find rows that match on those.
                merged = df_b.merge(df_a, on=common_cols, how="outer", indicator=True)

                # Rows only in B (New/Added) -> left_only
                added_df = merged[merged["_merge"] == "left_only"]

                # Rows only in A (Old/Removed) -> right_only
                removed_df = merged[merged["_merge"] == "right_only"]

                rows_added_count = len(added_df)
                rows_removed_count = len(removed_df)

                # For Added, show columns from B
                cols_b = list(schema_b)
                # Filter to cols_b that exist in merged (should be all)
                # Note: merged might have duplicate columns if not in 'on' list?
                # Yes, if B has col X and A has col X, and X is NOT in common_cols (impossible by def), it would duplicate.
                # Columns in B but not A (Added cols) -> No collision -> Present.
                # Columns in common -> Joined -> Present.
                added_rows = added_df[cols_b].head(10).to_dict("records")

                # For Removed, show columns from A
                cols_a = list(schema_a)
                removed_rows = removed_df[cols_a].head(10).to_dict("records")

    return DeltaDiffResult(
        table_path=table_path,
        version_a=version_a,
        version_b=version_b,
        rows_change=rows_change,
        files_change=0,
        size_change_bytes=0,
        schema_added=list(schema_b - schema_a),
        schema_removed=list(schema_a - schema_b),
        schema_current=sorted(list(schema_b)),
        schema_previous=sorted(list(schema_a)),
        rows_added=rows_added_count,
        rows_removed=rows_removed_count,
        rows_updated=rows_updated_count,
        sample_added=added_rows,
        sample_removed=removed_rows,
        sample_updated=updated_rows,
        operations=operations,
    )


def detect_drift(
    table_path: str,
    current_version: int,
    baseline_version: int,
    spark: Optional[Any] = None,
    threshold_pct: float = 10.0,
) -> Optional[str]:
    """
    Check for significant drift between versions.

    Args:
        table_path: Path to Delta table
        current_version: Current version
        baseline_version: Baseline version
        spark: Optional SparkSession
        threshold_pct: Row count change percentage to trigger warning

    Returns:
        Warning message if drift detected, None otherwise
    """
    diff = get_delta_diff(table_path, baseline_version, current_version, spark=spark)

    # Check schema drift
    if diff.schema_added or diff.schema_removed:
        return (
            f"Schema drift detected: "
            f"+{len(diff.schema_added)} columns, -{len(diff.schema_removed)} columns"
        )

    # For row count baseline, we can calculate it from current - change?
    # Or read it again.
    # Let's optimize: we don't have base_count in DiffResult directly but we have rows_change.
    # We need absolute base count.

    # Helper to get base count
    if spark:
        base_count = (
            spark.read.format("delta")
            .option("versionAsOf", baseline_version)
            .load(table_path)
            .count()
        )
    else:
        from deltalake import DeltaTable

        dt = DeltaTable(table_path)
        dt.load_version(baseline_version)
        base_count = len(dt.to_pandas())

    if base_count == 0:
        if diff.rows_change > 0:
            return f"Data volume spike (0 -> {diff.rows_change} rows)"
        return None

    pct_change = abs(diff.rows_change) / base_count * 100

    if pct_change > threshold_pct:
        return f"Row count drift: {pct_change:.1f}% change (Threshold: {threshold_pct}%)"

    return None
