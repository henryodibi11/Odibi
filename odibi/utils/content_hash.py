"""Content hashing utilities for skip_if_unchanged feature.

This module provides functions to compute deterministic hashes of DataFrames
for change detection in snapshot ingestion patterns.
"""

import hashlib
from typing import TYPE_CHECKING, List, Optional, Any

if TYPE_CHECKING:
    import pandas as pd


def compute_dataframe_hash(
    df: "pd.DataFrame",
    columns: Optional[List[str]] = None,
    sort_columns: Optional[List[str]] = None,
) -> str:
    """Compute a deterministic SHA256 hash of a DataFrame's content.

    Args:
        df: Pandas DataFrame to hash
        columns: Subset of columns to include in hash. If None, all columns.
        sort_columns: Columns to sort by for deterministic ordering.
            If None, DataFrame is not sorted (assumes consistent order).

    Returns:
        SHA256 hex digest string (64 characters)

    Example:
        >>> df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        >>> hash1 = compute_dataframe_hash(df, sort_columns=["id"])
        >>> hash2 = compute_dataframe_hash(df, sort_columns=["id"])
        >>> assert hash1 == hash2  # Same content = same hash
    """
    if df.empty:
        return hashlib.sha256(b"EMPTY_DATAFRAME").hexdigest()

    work_df = df

    if columns:
        missing = set(columns) - set(df.columns)
        if missing:
            raise ValueError(f"Hash columns not found in DataFrame: {missing}")
        work_df = work_df[columns]

    if sort_columns:
        missing = set(sort_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Sort columns not found in DataFrame: {missing}")
        work_df = work_df.sort_values(sort_columns).reset_index(drop=True)

    csv_bytes = work_df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()


def compute_spark_dataframe_hash(
    df: Any,
    columns: Optional[List[str]] = None,
    sort_columns: Optional[List[str]] = None,
    distributed: bool = True,
) -> str:
    """Compute a deterministic SHA256 hash of a Spark DataFrame's content.

    Args:
        df: Spark DataFrame to hash
        columns: Subset of columns to include in hash. If None, all columns are used.
        sort_columns: Columns to sort by for deterministic ordering.
            (Only used in legacy mode when distributed=False)
        distributed: If True (default), use distributed hash computation.
            If False, use legacy collect-to-driver approach.

    Returns:
        SHA256 hex digest string (64 characters)

    Note:
        The distributed mode (default) computes hash without collecting data to driver,
        making it safe for large datasets. The hash is computed as:
        1. Per-row xxhash64 of all column values
        2. Sum of all row hashes (order-independent)
        3. Combined with row count for final SHA256

        Since the sum is commutative, this produces consistent hashes regardless of
        partition ordering, without requiring a full sort operation.
    """
    if df.isEmpty():
        return hashlib.sha256(b"EMPTY_DATAFRAME").hexdigest()

    work_df = df

    if columns:
        work_df = work_df.select(columns)

    if distributed:
        return _compute_spark_hash_distributed(work_df)
    else:
        return _compute_spark_hash_legacy(work_df, sort_columns)


def _compute_spark_hash_distributed(df) -> str:
    """Compute hash distributedly using Spark's xxhash64.

    This approach:
    - Never collects data to driver (except 2 scalar values)
    - Uses xxhash64 for fast row-level hashing
    - Uses commutative sum for order-independent aggregation
    - Is safe for arbitrarily large DataFrames
    """
    from pyspark.sql import functions as F

    hash_cols = [F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in df.columns]
    work_df = df.withColumn("_row_hash", F.xxhash64(*hash_cols))

    result = work_df.agg(
        F.count("*").alias("row_count"),
        F.sum("_row_hash").alias("hash_sum"),
    ).collect()[0]

    row_count = result["row_count"] or 0
    hash_sum = result["hash_sum"] or 0
    combined = f"v2:{row_count}:{hash_sum}:{','.join(sorted(df.columns))}"
    return hashlib.sha256(combined.encode()).hexdigest()


def _compute_spark_hash_legacy(df, sort_columns: Optional[List[str]] = None) -> str:
    """Legacy hash computation that collects to driver.

    Warning: This can cause OOM on large datasets.
    Use distributed=True for production workloads.
    """
    work_df = df

    if sort_columns:
        work_df = work_df.orderBy(sort_columns)

    pandas_df = work_df.toPandas()
    csv_bytes = pandas_df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()


def make_content_hash_key(node_name: str, table_name: str) -> str:
    """Generate a state key for content hash storage.

    Args:
        node_name: Pipeline node name
        table_name: Target table name

    Returns:
        State key string
    """
    return f"content_hash:{node_name}:{table_name}"


def get_content_hash_from_state(state_backend, node_name: str, table_name: str) -> Optional[str]:
    """Retrieve stored content hash from state backend (catalog).

    Args:
        state_backend: CatalogStateBackend or compatible state backend
        node_name: Pipeline node name
        table_name: Target table name

    Returns:
        Previously stored hash string, or None if not found
    """
    if state_backend is None:
        return None

    try:
        key = make_content_hash_key(node_name, table_name)
        value = state_backend.get_hwm(key)
        if isinstance(value, dict):
            return value.get("hash")
        return None
    except Exception:
        return None


def set_content_hash_in_state(
    state_backend,
    node_name: str,
    table_name: str,
    content_hash: str,
) -> None:
    """Store content hash in state backend (catalog).

    Args:
        state_backend: CatalogStateBackend or compatible state backend
        node_name: Pipeline node name
        table_name: Target table name
        content_hash: Hash string to store
    """
    if state_backend is None:
        return

    from datetime import datetime, timezone

    key = make_content_hash_key(node_name, table_name)
    value = {
        "hash": content_hash,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    state_backend.set_hwm(key, value)
