"""Content hashing utilities for skip_if_unchanged feature.

This module provides functions to compute deterministic hashes of DataFrames
for change detection in snapshot ingestion patterns.
"""

import hashlib
from typing import TYPE_CHECKING, List, Optional

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
    df,
    columns: Optional[List[str]] = None,
    sort_columns: Optional[List[str]] = None,
) -> str:
    """Compute a deterministic SHA256 hash of a Spark DataFrame's content.

    Note: This collects the DataFrame to driver, so use with caution on large datasets.
    For very large DataFrames, consider sampling or using a distributed hash.

    Args:
        df: Spark DataFrame to hash
        columns: Subset of columns to include in hash. If None, all columns are used.
        sort_columns: Columns to sort by for deterministic ordering.

    Returns:
        SHA256 hex digest string (64 characters)
    """
    if df.isEmpty():
        return hashlib.sha256(b"EMPTY_DATAFRAME").hexdigest()

    work_df = df

    if columns:
        work_df = work_df.select(columns)

    if sort_columns:
        work_df = work_df.orderBy(sort_columns)

    pandas_df = work_df.toPandas()
    csv_bytes = pandas_df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()


HASH_METADATA_KEY = "odibi.content_hash"
HASH_TIMESTAMP_KEY = "odibi.content_hash_timestamp"


def get_delta_content_hash(
    table_path: str, storage_options: Optional[dict] = None
) -> Optional[str]:
    """Retrieve stored content hash from Delta table metadata.

    Args:
        table_path: Path to Delta table
        storage_options: Cloud storage options (for Azure, S3, etc.)

    Returns:
        Previously stored hash string, or None if not found
    """
    try:
        from deltalake import DeltaTable

        dt = DeltaTable(table_path, storage_options=storage_options)
        metadata = dt.metadata()

        if metadata.configuration:
            return metadata.configuration.get(HASH_METADATA_KEY)
        return None
    except Exception:
        return None


def set_delta_content_hash(
    table_path: str,
    content_hash: str,
    storage_options: Optional[dict] = None,
) -> None:
    """Store content hash in Delta table metadata.

    Args:
        table_path: Path to Delta table
        content_hash: Hash string to store
        storage_options: Cloud storage options (for Azure, S3, etc.)
    """
    from datetime import datetime

    from deltalake import DeltaTable

    dt = DeltaTable(table_path, storage_options=storage_options)

    current_config = dt.metadata().configuration or {}
    new_config = {
        **current_config,
        HASH_METADATA_KEY: content_hash,
        HASH_TIMESTAMP_KEY: datetime.utcnow().isoformat(),
    }

    dt.alter.set_table_properties(new_config)


def get_spark_delta_content_hash(
    spark,
    table: Optional[str] = None,
    path: Optional[str] = None,
) -> Optional[str]:
    """Retrieve stored content hash from Delta table metadata (Spark).

    Args:
        spark: SparkSession
        table: Table name (catalog)
        path: Table path

    Returns:
        Previously stored hash string, or None if not found
    """
    try:
        from delta.tables import DeltaTable

        if table:
            dt = DeltaTable.forName(spark, table)
        else:
            dt = DeltaTable.forPath(spark, path)

        detail = dt.detail().collect()[0]
        properties = detail.properties or {}
        return properties.get(HASH_METADATA_KEY)
    except Exception:
        return None


def set_spark_delta_content_hash(
    spark,
    content_hash: str,
    table: Optional[str] = None,
    path: Optional[str] = None,
) -> None:
    """Store content hash in Delta table metadata (Spark).

    Args:
        spark: SparkSession
        content_hash: Hash string to store
        table: Table name (catalog)
        path: Table path
    """
    from datetime import datetime

    target = table if table else f"delta.`{path}`"
    timestamp = datetime.utcnow().isoformat()

    spark.sql(
        f"""
        ALTER TABLE {target}
        SET TBLPROPERTIES (
            '{HASH_METADATA_KEY}' = '{content_hash}',
            '{HASH_TIMESTAMP_KEY}' = '{timestamp}'
        )
    """
    )
