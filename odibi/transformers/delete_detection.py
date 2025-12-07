"""
Delete Detection Transformer for CDC-like behavior.

Detects records that existed in previous extractions but no longer exist,
enabling CDC-like behavior for sources without native Change Data Capture.
"""

import logging
from typing import Any, Dict, Optional

from odibi.config import (
    DeleteDetectionConfig,
    DeleteDetectionMode,
    FirstRunBehavior,
    ThresholdBreachAction,
)
from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.registry import transform

logger = logging.getLogger(__name__)


class DeleteThresholdExceeded(Exception):
    """Raised when delete percentage exceeds configured threshold."""

    pass


@transform("detect_deletes", category="transformer", param_model=DeleteDetectionConfig)
def detect_deletes(
    context: EngineContext, config: DeleteDetectionConfig = None, **params
) -> EngineContext:
    """
    Detects deleted records based on configured mode.

    Returns:
    - soft_delete_col set: Adds boolean column (True = deleted)
    - soft_delete_col = None: Removes deleted rows (hard delete)
    """
    if config is None:
        config = DeleteDetectionConfig(**params)

    if config.mode == DeleteDetectionMode.NONE:
        return context

    if config.mode == DeleteDetectionMode.SNAPSHOT_DIFF:
        return _detect_deletes_snapshot_diff(context, config)

    if config.mode == DeleteDetectionMode.SQL_COMPARE:
        return _detect_deletes_sql_compare(context, config)

    raise ValueError(f"Unknown delete detection mode: {config.mode}")


def _detect_deletes_snapshot_diff(
    context: EngineContext,
    config: DeleteDetectionConfig,
) -> EngineContext:
    """
    Compare current Delta version to previous version.
    Keys in previous but not in current = deleted.
    """
    if context.engine_type == EngineType.SPARK:
        return _snapshot_diff_spark(context, config)
    else:
        return _snapshot_diff_pandas(context, config)


def _snapshot_diff_spark(
    context: EngineContext,
    config: DeleteDetectionConfig,
) -> EngineContext:
    """Spark implementation of snapshot_diff using Delta time travel."""
    from delta.tables import DeltaTable

    keys = config.keys
    spark = context.spark

    table_path = _get_target_path(context)
    if not table_path:
        logger.warning(
            "detect_deletes: Could not determine target table path. Skipping. "
            "Ensure the node has a 'write' block or 'inputs' with a pipeline reference."
        )
        return context

    if not DeltaTable.isDeltaTable(spark, table_path):
        logger.info("detect_deletes: Target is not a Delta table. Skipping snapshot_diff.")
        return context

    dt = DeltaTable.forPath(spark, table_path)
    current_version = dt.history(1).collect()[0]["version"]

    if current_version == 0:
        if config.on_first_run == FirstRunBehavior.ERROR:
            raise ValueError("detect_deletes: No previous version exists for snapshot_diff.")
        logger.info("detect_deletes: First run detected (version 0). Skipping delete detection.")
        return _ensure_delete_column(context, config)

    prev_version = current_version - 1

    # Validate keys exist in current DataFrame
    curr_columns = [c.lower() for c in context.df.columns]
    missing_curr_keys = [k for k in keys if k.lower() not in curr_columns]
    if missing_curr_keys:
        logger.warning(
            f"detect_deletes: Keys {missing_curr_keys} not found in current DataFrame. "
            f"Available columns: {context.df.columns}. Skipping delete detection."
        )
        return _ensure_delete_column(context, config)

    # Load previous version and validate schema
    prev_df = spark.read.format("delta").option("versionAsOf", prev_version).load(table_path)
    prev_columns = [c.lower() for c in prev_df.columns]
    missing_prev_keys = [k for k in keys if k.lower() not in prev_columns]
    if missing_prev_keys:
        logger.warning(
            f"detect_deletes: Keys {missing_prev_keys} not found in previous version (v{prev_version}). "
            f"Schema may have changed. Skipping delete detection."
        )
        return _ensure_delete_column(context, config)

    curr_keys = context.df.select(keys).distinct()
    prev_keys = prev_df.select(keys).distinct()

    deleted_keys = prev_keys.exceptAll(curr_keys)

    return _apply_deletes(context, deleted_keys, config)


def _snapshot_diff_pandas(
    context: EngineContext,
    config: DeleteDetectionConfig,
) -> EngineContext:
    """Pandas implementation of snapshot_diff using deltalake library."""
    try:
        from deltalake import DeltaTable
    except ImportError:
        raise ImportError(
            "detect_deletes snapshot_diff mode requires 'deltalake' package. "
            "Install with: pip install deltalake"
        )

    keys = config.keys
    table_path = _get_target_path(context)

    if not table_path:
        logger.warning(
            "detect_deletes: Could not determine target table path. Skipping. "
            "Ensure the node has a 'write' block or 'inputs' with a pipeline reference."
        )
        return context

    try:
        dt = DeltaTable(table_path)
    except Exception as e:
        logger.info(f"detect_deletes: Target is not a Delta table ({e}). Skipping.")
        return context

    current_version = dt.version()

    if current_version == 0:
        if config.on_first_run == FirstRunBehavior.ERROR:
            raise ValueError("detect_deletes: No previous version exists for snapshot_diff.")
        logger.info("detect_deletes: First run detected (version 0). Skipping delete detection.")
        return _ensure_delete_column(context, config)

    prev_version = current_version - 1

    # Validate keys exist in current DataFrame
    curr_columns = [c.lower() for c in context.df.columns]
    missing_curr_keys = [k for k in keys if k.lower() not in curr_columns]
    if missing_curr_keys:
        logger.warning(
            f"detect_deletes: Keys {missing_curr_keys} not found in current DataFrame. "
            f"Available columns: {list(context.df.columns)}. Skipping delete detection."
        )
        return _ensure_delete_column(context, config)

    # Load previous version and validate schema
    prev_df = DeltaTable(table_path, version=prev_version).to_pandas()
    prev_columns = [c.lower() for c in prev_df.columns]
    missing_prev_keys = [k for k in keys if k.lower() not in prev_columns]
    if missing_prev_keys:
        logger.warning(
            f"detect_deletes: Keys {missing_prev_keys} not found in previous version (v{prev_version}). "
            f"Schema may have changed. Skipping delete detection."
        )
        return _ensure_delete_column(context, config)

    curr_keys = context.df[keys].drop_duplicates()
    prev_keys = prev_df[keys].drop_duplicates()

    merged = prev_keys.merge(curr_keys, on=keys, how="left", indicator=True)
    deleted_keys = merged[merged["_merge"] == "left_only"][keys].copy()

    return _apply_deletes(context, deleted_keys, config)


def _detect_deletes_sql_compare(
    context: EngineContext,
    config: DeleteDetectionConfig,
) -> EngineContext:
    """
    Compare Silver keys against live source.
    Keys in Silver but not in source = deleted.
    """
    if context.engine_type == EngineType.SPARK:
        return _sql_compare_spark(context, config)
    else:
        return _sql_compare_pandas(context, config)


def _sql_compare_spark(
    context: EngineContext,
    config: DeleteDetectionConfig,
) -> EngineContext:
    """Spark implementation of sql_compare using JDBC."""
    keys = config.keys
    spark = context.spark

    conn = _get_connection(context, config.source_connection)
    if conn is None:
        raise ValueError(f"detect_deletes: Connection '{config.source_connection}' not found.")

    source_keys_query = _build_source_keys_query(config)

    jdbc_url = _get_jdbc_url(conn)
    jdbc_props = _get_jdbc_properties(conn)

    source_keys = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("query", source_keys_query)
        .options(**jdbc_props)
        .load()
    )

    silver_keys = context.df.select(keys).distinct()
    deleted_keys = silver_keys.exceptAll(source_keys)

    return _apply_deletes(context, deleted_keys, config)


def _sql_compare_pandas(
    context: EngineContext,
    config: DeleteDetectionConfig,
) -> EngineContext:
    """Pandas implementation of sql_compare using SQLAlchemy."""
    import pandas as pd

    keys = config.keys

    conn = _get_connection(context, config.source_connection)
    if conn is None:
        raise ValueError(f"detect_deletes: Connection '{config.source_connection}' not found.")

    source_keys_query = _build_source_keys_query(config)

    engine = _get_sqlalchemy_engine(conn)
    source_keys = pd.read_sql(source_keys_query, engine)

    silver_keys = context.df[keys].drop_duplicates()

    merged = silver_keys.merge(source_keys, on=keys, how="left", indicator=True)
    deleted_keys = merged[merged["_merge"] == "left_only"][keys].copy()

    return _apply_deletes(context, deleted_keys, config)


def _apply_deletes(
    context: EngineContext,
    deleted_keys: Any,
    config: DeleteDetectionConfig,
) -> EngineContext:
    """Apply soft or hard delete based on config."""
    deleted_count = _get_row_count(deleted_keys, context.engine_type)
    total_count = _get_row_count(context.df, context.engine_type)

    if deleted_count == 0:
        logger.info("detect_deletes: No deleted records found.")
        return _ensure_delete_column(context, config)

    delete_percent = (deleted_count / total_count * 100) if total_count > 0 else 0

    if config.max_delete_percent is not None:
        if delete_percent > config.max_delete_percent:
            if config.on_threshold_breach == ThresholdBreachAction.ERROR:
                raise DeleteThresholdExceeded(
                    f"detect_deletes: {delete_percent:.1f}% of rows flagged for deletion "
                    f"exceeds threshold of {config.max_delete_percent}%"
                )
            elif config.on_threshold_breach == ThresholdBreachAction.WARN:
                logger.warning(
                    f"detect_deletes: {delete_percent:.1f}% of rows flagged for deletion "
                    f"(threshold: {config.max_delete_percent}%)"
                )
            elif config.on_threshold_breach == ThresholdBreachAction.SKIP:
                logger.info(
                    f"detect_deletes: Delete threshold exceeded ({delete_percent:.1f}%). "
                    "Skipping delete detection."
                )
                return _ensure_delete_column(context, config)

    logger.info(
        f"detect_deletes: Found {deleted_count} deleted records "
        f"({delete_percent:.1f}% of {total_count} rows)"
    )

    if config.soft_delete_col:
        return _apply_soft_delete(context, deleted_keys, config)
    else:
        return _apply_hard_delete(context, deleted_keys, config)


def _apply_soft_delete(
    context: EngineContext,
    deleted_keys: Any,
    config: DeleteDetectionConfig,
) -> EngineContext:
    """Add soft delete flag column."""
    keys = config.keys
    soft_delete_col = config.soft_delete_col

    if context.engine_type == EngineType.SPARK:
        from pyspark.sql.functions import col, lit, when

        deleted_keys_flagged = deleted_keys.withColumn("_del_flag", lit(True))

        result = context.df.join(deleted_keys_flagged, on=keys, how="left").withColumn(
            soft_delete_col,
            when(col("_del_flag").isNotNull(), True).otherwise(False),
        )

        result = result.drop("_del_flag")

    else:
        df = context.df.copy()
        deleted_keys_df = deleted_keys.copy()
        deleted_keys_df["_del_flag"] = True

        df = df.merge(deleted_keys_df, on=keys, how="left")
        df[soft_delete_col] = df["_del_flag"].fillna(False).astype(bool)
        df = df.drop(columns=["_del_flag"])
        result = df

    return context.with_df(result)


def _apply_hard_delete(
    context: EngineContext,
    deleted_keys: Any,
    config: DeleteDetectionConfig,
) -> EngineContext:
    """Remove deleted rows."""
    keys = config.keys

    if context.engine_type == EngineType.SPARK:
        result = context.df.join(deleted_keys, on=keys, how="left_anti")
    else:
        df = context.df.copy()
        merged = df.merge(deleted_keys, on=keys, how="left", indicator=True)
        result = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

    return context.with_df(result)


def _ensure_delete_column(
    context: EngineContext,
    config: DeleteDetectionConfig,
) -> EngineContext:
    """Ensure soft delete column exists with False values when no deletes found."""
    if not config.soft_delete_col:
        return context

    soft_delete_col = config.soft_delete_col

    if context.engine_type == EngineType.SPARK:
        if soft_delete_col not in context.df.columns:
            from pyspark.sql.functions import lit

            result = context.df.withColumn(soft_delete_col, lit(False))
            return context.with_df(result)
    else:
        if soft_delete_col not in context.df.columns:
            df = context.df.copy()
            df[soft_delete_col] = False
            return context.with_df(df)

    return context


def _build_source_keys_query(config: DeleteDetectionConfig) -> str:
    """Build SQL query to get source keys."""
    if config.source_query:
        return config.source_query

    keys = config.keys
    key_cols = ", ".join(keys)
    return f"SELECT DISTINCT {key_cols} FROM {config.source_table}"


def _get_row_count(df: Any, engine_type: EngineType) -> int:
    """Get row count from DataFrame."""
    if engine_type == EngineType.SPARK:
        return df.count()
    else:
        return len(df)


def _get_target_path(context: EngineContext) -> Optional[str]:
    """
    Get target table path from context.
    This is used for snapshot_diff to access Delta time travel.

    Priority:
    1. _current_write_path (from node's write block)
    2. _current_input_path (from node's inputs - for cross-pipeline references)
    3. current_table_path (legacy)
    """
    if hasattr(context, "engine") and context.engine:
        engine = context.engine
        if hasattr(engine, "_current_write_path") and engine._current_write_path:
            return engine._current_write_path
        if hasattr(engine, "_current_input_path") and engine._current_input_path:
            return engine._current_input_path
        if hasattr(engine, "current_table_path"):
            return engine.current_table_path

    if hasattr(context, "context"):
        inner_ctx = context.context
        if hasattr(inner_ctx, "_current_table_path"):
            return inner_ctx._current_table_path

    return None


def _get_connection(context: EngineContext, connection_name: str) -> Optional[Any]:
    """Get connection from context's engine."""
    if hasattr(context, "engine") and context.engine:
        if hasattr(context.engine, "connections"):
            return context.engine.connections.get(connection_name)
    return None


def _get_jdbc_url(conn: Any) -> str:
    """Extract JDBC URL from connection object."""
    if hasattr(conn, "jdbc_url"):
        return conn.jdbc_url
    if hasattr(conn, "get_jdbc_url"):
        return conn.get_jdbc_url()
    if hasattr(conn, "url"):
        return conn.url
    if hasattr(conn, "get_spark_options"):
        opts = conn.get_spark_options()
        if isinstance(opts, dict) and "url" in opts:
            return opts["url"]

    raise ValueError(f"Cannot determine JDBC URL from connection type {type(conn).__name__}")


def _get_jdbc_properties(conn: Any) -> Dict[str, str]:
    """Extract JDBC properties from connection object."""
    props = {}

    if hasattr(conn, "get_spark_options"):
        opts = conn.get_spark_options()
        if isinstance(opts, dict):
            if "user" in opts:
                props["user"] = opts["user"]
            if "password" in opts:
                props["password"] = opts["password"]
            if "driver" in opts:
                props["driver"] = opts["driver"]
            return props

    if hasattr(conn, "user"):
        props["user"] = conn.user
    if hasattr(conn, "password"):
        props["password"] = conn.password
    if hasattr(conn, "jdbc_driver"):
        props["driver"] = conn.jdbc_driver
    if hasattr(conn, "jdbc_properties"):
        props.update(conn.jdbc_properties)

    return props


def _get_sqlalchemy_engine(conn: Any) -> Any:
    """Get SQLAlchemy engine from connection object."""
    if hasattr(conn, "engine"):
        return conn.engine
    if hasattr(conn, "get_engine"):
        return conn.get_engine()
    if hasattr(conn, "connection_string"):
        from sqlalchemy import create_engine

        return create_engine(conn.connection_string)

    raise ValueError(f"Cannot create SQLAlchemy engine from connection type {type(conn).__name__}")
