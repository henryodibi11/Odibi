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


HASH_METADATA_FILENAME = "_odibi_content_hash.json"


def get_delta_content_hash(
    table_path: str, storage_options: Optional[dict] = None
) -> Optional[str]:
    """Retrieve stored content hash from Delta table metadata file.

    Args:
        table_path: Path to Delta table
        storage_options: Cloud storage options (for Azure, S3, etc.)

    Returns:
        Previously stored hash string, or None if not found
    """
    import json
    import os

    try:
        hash_file = os.path.join(table_path, HASH_METADATA_FILENAME)
        if os.path.exists(hash_file):
            with open(hash_file) as f:
                data = json.load(f)
                return data.get("content_hash")
        return None
    except Exception:
        return None


def set_delta_content_hash(
    table_path: str,
    content_hash: str,
    storage_options: Optional[dict] = None,
) -> None:
    """Store content hash in Delta table metadata file.

    Args:
        table_path: Path to Delta table
        content_hash: Hash string to store
        storage_options: Cloud storage options (for Azure, S3, etc.)
    """
    import json
    import os
    from datetime import datetime

    hash_file = os.path.join(table_path, HASH_METADATA_FILENAME)
    data = {
        "content_hash": content_hash,
        "timestamp": datetime.utcnow().isoformat(),
    }
    with open(hash_file, "w") as f:
        json.dump(data, f)


def get_spark_delta_content_hash(
    spark,
    table: Optional[str] = None,
    path: Optional[str] = None,
) -> Optional[str]:
    """Retrieve stored content hash from Delta table metadata file (Spark).

    Args:
        spark: SparkSession
        table: Table name (catalog)
        path: Table path

    Returns:
        Previously stored hash string, or None if not found
    """
    import json

    try:
        if table:
            from delta.tables import DeltaTable

            dt = DeltaTable.forName(spark, table)
            table_path = dt.detail().collect()[0].location
        else:
            table_path = path

        hash_file = f"{table_path}/{HASH_METADATA_FILENAME}"

        try:
            content = spark.read.text(hash_file).collect()
            if content:
                data = json.loads(content[0][0])
                return data.get("content_hash")
        except Exception:
            pass
        return None
    except Exception:
        return None


def set_spark_delta_content_hash(
    spark,
    content_hash: str,
    table: Optional[str] = None,
    path: Optional[str] = None,
) -> None:
    """Store content hash in Delta table metadata file (Spark).

    Args:
        spark: SparkSession
        content_hash: Hash string to store
        table: Table name (catalog)
        path: Table path
    """
    import json
    from datetime import datetime

    if table:
        from delta.tables import DeltaTable

        dt = DeltaTable.forName(spark, table)
        table_path = dt.detail().collect()[0].location
    else:
        table_path = path

    hash_file = f"{table_path}/{HASH_METADATA_FILENAME}"
    data = json.dumps(
        {
            "content_hash": content_hash,
            "timestamp": datetime.utcnow().isoformat(),
        }
    )

    spark.createDataFrame([(data,)], ["value"]).coalesce(1).write.mode("overwrite").text(hash_file)
