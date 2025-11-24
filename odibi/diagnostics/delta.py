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

    # Schema changes
    schema_added: List[str]
    schema_removed: List[str]

    # Metadata changes
    rows_change: int
    files_change: int
    size_change_bytes: int

    # Operation info
    operations_between: List[str] = None  # List of operations that happened between versions

    # Data Diff Samples (Optional)
    sample_added: Optional[List[Dict[str, Any]]] = None
    sample_removed: Optional[List[Dict[str, Any]]] = None


def get_delta_diff(
    table_path: str, version_a: int, version_b: int, spark: Optional[Any] = None
) -> DeltaDiffResult:
    """
    Compare two versions of a Delta table.

    Supports both Spark (if spark session provided) and Pandas (deltalake) backends.

    Args:
        table_path: Path to Delta table
        version_a: Start version
        version_b: End version
        spark: Optional SparkSession. If None, uses deltalake (Pandas).

    Returns:
        DeltaDiffResult object
    """
    if spark:
        return _get_delta_diff_spark(spark, table_path, version_a, version_b)
    else:
        return _get_delta_diff_pandas(table_path, version_a, version_b)


def _get_delta_diff_spark(
    spark: Any, table_path: str, version_a: int, version_b: int
) -> DeltaDiffResult:
    """Spark implementation of delta diff."""
    try:
        from delta.tables import DeltaTable
    except ImportError:
        raise ImportError("Delta Lake support requires 'delta-spark'")

    dt = DeltaTable.forPath(spark, table_path)
    history = dt.history().collect()

    # Filter history between versions
    relevant_commits = [
        row
        for row in history
        if min(version_a, version_b) < row["version"] <= max(version_a, version_b)
    ]

    operations = [row["operation"] for row in relevant_commits]

    # Get snapshots
    df_a = spark.read.format("delta").option("versionAsOf", version_a).load(table_path)
    df_b = spark.read.format("delta").option("versionAsOf", version_b).load(table_path)

    schema_a = set(df_a.columns)
    schema_b = set(df_b.columns)

    rows_a = df_a.count()
    rows_b = df_b.count()

    # Compute Data Diff (exceptAll)
    # Find rows in B that are not in A (Added)
    # Find rows in A that are not in B (Removed)
    # Note: This can be expensive for large tables.
    # For diagnostics tool, we assume interactive use or drilled-down scope.

    # Handle schema evolution: Select common columns for diffing content
    common_cols = list(schema_a.intersection(schema_b))

    added_rows = []
    removed_rows = []

    if common_cols:
        df_a_common = df_a.select(*common_cols)
        df_b_common = df_b.select(*common_cols)

        diff_added = df_b_common.exceptAll(df_a_common)
        diff_removed = df_a_common.exceptAll(df_b_common)

        added_rows = [row.asDict() for row in diff_added.limit(10).collect()]
        removed_rows = [row.asDict() for row in diff_removed.limit(10).collect()]

    return DeltaDiffResult(
        table_path=table_path,
        version_a=version_a,
        version_b=version_b,
        schema_added=list(schema_b - schema_a),
        schema_removed=list(schema_a - schema_b),
        rows_change=rows_b - rows_a,
        files_change=0,
        size_change_bytes=0,
        sample_added=added_rows,
        sample_removed=removed_rows,
        operations_between=operations,
    )


def _get_delta_diff_pandas(table_path: str, version_a: int, version_b: int) -> DeltaDiffResult:
    """Pandas (deltalake) implementation of delta diff."""
    try:
        from deltalake import DeltaTable
    except ImportError:
        raise ImportError("Delta Lake support requires 'deltalake'")

    dt = DeltaTable(table_path)

    # History
    history = dt.history()
    relevant_commits = [
        h for h in history if min(version_a, version_b) < h["version"] <= max(version_a, version_b)
    ]
    operations = [h["operation"] for h in relevant_commits]

    # Snapshots
    dt.load_as_version(version_a)
    df_a = dt.to_pandas()
    rows_a = len(df_a)
    schema_a = set(df_a.columns)

    dt.load_as_version(version_b)
    df_b = dt.to_pandas()
    rows_b = len(df_b)
    schema_b = set(df_b.columns)

    # Compute Data Diff
    # Pandas doesn't have exceptAll. We use merge with indicator.
    common_cols = list(schema_a.intersection(schema_b))

    added_rows = []
    removed_rows = []

    if common_cols:
        # Ensure we only compare common columns
        df_a_c = df_a[common_cols]
        df_b_c = df_b[common_cols]

        # Merge on all columns
        # Note: nulls in pandas merge keys can be tricky, but usually works for simple diffs
        merged = df_b_c.merge(df_a_c, on=common_cols, how="outer", indicator=True)

        # Rows only in B (New/Added) -> left_only (since B is left)
        added_df = merged[merged["_merge"] == "left_only"][common_cols]

        # Rows only in A (Old/Removed) -> right_only
        removed_df = merged[merged["_merge"] == "right_only"][common_cols]

        added_rows = added_df.head(10).to_dict("records")
        removed_rows = removed_df.head(10).to_dict("records")

    return DeltaDiffResult(
        table_path=table_path,
        version_a=version_a,
        version_b=version_b,
        schema_added=list(schema_b - schema_a),
        schema_removed=list(schema_a - schema_b),
        rows_change=rows_b - rows_a,
        files_change=0,
        size_change_bytes=0,
        sample_added=added_rows,
        sample_removed=removed_rows,
        operations_between=operations,
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
