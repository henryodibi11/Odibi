import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from odibi.config import NodeConfig
from odibi.context import EngineContext
from odibi.engine.base import Engine
from odibi.enums import EngineType
from odibi.utils.logging_context import get_logging_context


class Pattern(ABC):
    """Base class for Execution Patterns."""

    def __init__(self, engine: Engine, config: NodeConfig):
        """Initialize a pattern instance.

        Args:
            engine: The execution engine (Spark, Pandas, or Polars).
            config: Node configuration containing pattern parameters and metadata.
        """
        self.engine = engine
        self.config = config
        self.params = config.params

    @abstractmethod
    def execute(self, context: EngineContext) -> Any:
        """
        Execute the pattern logic.

        Args:
            context: EngineContext containing current DataFrame and helpers.

        Returns:
            The transformed DataFrame.
        """
        pass

    def validate(self) -> None:
        """
        Validate pattern configuration.
        Raises ValueError if invalid.
        """
        ctx = get_logging_context()
        pattern_name = self.__class__.__name__
        ctx.debug(
            f"{pattern_name} validation starting",
            pattern=pattern_name,
            params=self.params,
        )
        ctx.debug(f"{pattern_name} validation passed", pattern=pattern_name)

    def _log_execution_start(self, **kwargs) -> float:
        """
        Log pattern execution start. Returns start time for elapsed calculation.

        Args:
            **kwargs: Additional key-value pairs to log.

        Returns:
            Start time in seconds.
        """
        ctx = get_logging_context()
        pattern_name = self.__class__.__name__
        ctx.debug(f"{pattern_name} execution starting", pattern=pattern_name, **kwargs)
        return time.time()

    def _log_execution_complete(self, start_time: float, **kwargs) -> None:
        """
        Log pattern execution completion with elapsed time.

        Args:
            start_time: Start time from _log_execution_start.
            **kwargs: Additional key-value pairs to log (e.g., row counts).
        """
        ctx = get_logging_context()
        pattern_name = self.__class__.__name__
        elapsed_ms = (time.time() - start_time) * 1000
        ctx.info(
            f"{pattern_name} execution completed",
            pattern=pattern_name,
            elapsed_ms=round(elapsed_ms, 2),
            **kwargs,
        )

    def _log_error(self, error: Exception, **kwargs) -> None:
        """
        Log error context before raising exceptions.

        Args:
            error: The exception that occurred.
            **kwargs: Additional context to log.
        """
        ctx = get_logging_context()
        pattern_name = self.__class__.__name__
        ctx.error(
            f"{pattern_name} execution failed: {error}",
            pattern=pattern_name,
            error_type=type(error).__name__,
            **kwargs,
        )

    def _get_row_count(self, df, engine_type) -> Optional[int]:
        """Get row count from a DataFrame, handling both Spark and Pandas."""
        try:
            if engine_type == EngineType.SPARK:
                return df.count()
            else:
                return len(df)
        except Exception:
            return None

    def _load_existing_target(self, context: EngineContext, target: str):
        """Load existing target table if it exists.

        Args:
            context: Engine context containing engine type and configuration.
            target: The name or path of the target table to load.

        Returns:
            DataFrame containing the existing target data, or None if not found.
        """
        if context.engine_type == EngineType.SPARK:
            return self._load_existing_spark(context, target)
        else:
            return self._load_existing_pandas(context, target)

    def _load_existing_spark(self, context: EngineContext, target: str):
        """Load existing target table from Spark with multi-format support."""
        ctx = get_logging_context()
        pattern_name = self.__class__.__name__
        spark = context.spark

        # Try catalog table first
        try:
            return spark.table(target)
        except Exception:
            pass

        # Check file extension for format detection
        target_lower = target.lower()

        try:
            if target_lower.endswith(".parquet"):
                return spark.read.parquet(target)
            elif target_lower.endswith(".csv"):
                return spark.read.option("header", "true").option("inferSchema", "true").csv(target)
            elif target_lower.endswith(".json"):
                return spark.read.json(target)
            elif target_lower.endswith(".orc"):
                return spark.read.orc(target)
            else:
                # Try Delta format as fallback (for paths without extension)
                return spark.read.format("delta").load(target)
        except Exception as e:
            ctx.warning(
                f"Could not load existing target '{target}': {e}. Treating as initial load.",
                pattern=pattern_name,
                target=target,
            )
            return None

    def _load_existing_pandas(self, context: EngineContext, target: str):
        """Load existing target table from Pandas with multi-format support."""
        import os

        import pandas as pd

        ctx = get_logging_context()
        pattern_name = self.__class__.__name__
        path = target

        # Handle connection-prefixed paths
        if hasattr(context, "engine") and context.engine:
            if "." in path:
                parts = path.split(".", 1)
                conn_name = parts[0]
                rel_path = parts[1]
                if conn_name in context.engine.connections:
                    try:
                        path = context.engine.connections[conn_name].get_path(rel_path)
                    except Exception:
                        pass

        if not os.path.exists(path):
            return None

        path_lower = str(path).lower()

        try:
            # Parquet (file or directory)
            if path_lower.endswith(".parquet") or os.path.isdir(path):
                return pd.read_parquet(path)
            # CSV
            elif path_lower.endswith(".csv"):
                return pd.read_csv(path)
            # JSON
            elif path_lower.endswith(".json"):
                return pd.read_json(path)
            # Excel
            elif path_lower.endswith(".xlsx") or path_lower.endswith(".xls"):
                return pd.read_excel(path)
            # Feather / Arrow IPC
            elif path_lower.endswith(".feather") or path_lower.endswith(".arrow"):
                return pd.read_feather(path)
            # Pickle
            elif path_lower.endswith(".pickle") or path_lower.endswith(".pkl"):
                return pd.read_pickle(path)
            else:
                ctx.warning(
                    f"Unrecognized file format for target '{target}'. "
                    "Supported formats: parquet, csv, json, xlsx, xls, feather, arrow, pickle. "
                    "Attempting to read as parquet.",
                    pattern=pattern_name,
                    target=target,
                )
                try:
                    return pd.read_parquet(path)
                except Exception:
                    return None
        except Exception as e:
            ctx.warning(
                f"Could not load existing target '{target}': {e}. Treating as initial load.",
                pattern=pattern_name,
                target=target,
            )
            return None

    def _add_audit_columns(self, context: EngineContext, df, audit_config: Dict):
        """Add audit columns (load_timestamp, source_system) to the DataFrame.

        Args:
            context: Engine context containing engine type and configuration.
            df: The DataFrame to add audit columns to.
            audit_config: Configuration dictionary specifying which audit columns to add.
                Keys: 'load_timestamp' (bool), 'source_system' (str).

        Returns:
            DataFrame with audit columns added as configured.
        """
        load_timestamp = audit_config.get("load_timestamp", False)
        source_system = audit_config.get("source_system")

        if context.engine_type == EngineType.SPARK:
            from pyspark.sql import functions as F

            if load_timestamp:
                df = df.withColumn("load_timestamp", F.current_timestamp())
            if source_system:
                df = df.withColumn("source_system", F.lit(source_system))
        else:
            if load_timestamp or source_system:
                df = df.copy()
            if load_timestamp:
                df["load_timestamp"] = datetime.now(timezone.utc)
            if source_system:
                df["source_system"] = source_system

        return df
