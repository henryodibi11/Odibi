import logging
import re
import threading
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any, Dict, Optional, Union

import pandas as pd

from odibi.enums import EngineType

logger = logging.getLogger(__name__)

try:
    import polars as pl
except ImportError:
    pl = None

# Thread-local storage for unique temp view names
_thread_local = threading.local()


def _get_unique_view_name() -> str:
    """Generate a unique temp view name for thread-safe parallel execution."""
    if not hasattr(_thread_local, "view_counter"):
        _thread_local.view_counter = 0
    _thread_local.view_counter += 1
    thread_id = threading.current_thread().ident or 0
    return f"_df_{thread_id}_{_thread_local.view_counter}"


class EngineContext:
    """
    The context passed to transformations.
    Wraps the global context (other datasets) and the local state (current dataframe).
    Provides uniform API for SQL and Data operations.
    """

    def __init__(
        self,
        context: "Context",
        df: Any,
        engine_type: EngineType,
        sql_executor: Optional[Any] = None,
        engine: Optional[Any] = None,
        pii_metadata: Optional[Dict[str, bool]] = None,
    ):
        self.context = context
        self.df = df
        self.engine_type = engine_type
        self.sql_executor = sql_executor
        self.engine = engine
        self.pii_metadata = pii_metadata or {}
        self._sql_history: list[str] = []

    @property
    def columns(self) -> list[str]:
        if hasattr(self.df, "columns"):
            return list(self.df.columns)
        # Spark
        if hasattr(self.df, "schema"):
            return self.df.columns
        return []

    @property
    def schema(self) -> Dict[str, str]:
        """Get schema types."""
        if self.engine:
            return self.engine.get_schema(self.df)
        return {}

    @property
    def spark(self) -> Any:
        """Helper to access SparkSession if available in context."""
        if hasattr(self.context, "spark"):
            return self.context.spark
        return None

    def with_df(self, df: Any) -> "EngineContext":
        """Returns a new context with updated DataFrame."""
        new_ctx = EngineContext(
            self.context, df, self.engine_type, self.sql_executor, self.engine, self.pii_metadata
        )
        # Preserve history? No, we want history per-transformation scope usually.
        # But wait, if we chain, we might want to pass it?
        # For now, let's keep history tied to the specific context instance used in a transform.
        # The Node will check the context instance it passed to the function.
        # However, if the function returns a new context (via with_df), we lose the reference.
        # Actually, the user functions usually return a DataFrame, not a Context.
        # Context is just the helper.
        # So we can accumulate in the *original* context passed to the function?
        # No, context is immutable-ish.
        # Let's make sql_history shared if we branch?
        # Actually, simple approach: The user calls context.sql(). That context instance records it.
        # If they chain .sql().sql(), we need the new context to share the history list.
        new_ctx._sql_history = self._sql_history
        return new_ctx

    def get(self, name: str) -> Any:
        """Get a dataset from global context."""
        return self.context.get(name)

    def register_temp_view(self, name: str, df: Any) -> None:
        """Register a temporary view for SQL."""
        self.context.register(name, df)

    def sql(self, query: str) -> "EngineContext":
        """Execute SQL on the current DataFrame (aliased as 'df')."""
        self._sql_history.append(query)

        if self.sql_executor:
            # Use unique temp view name for thread-safe parallel execution
            view_name = _get_unique_view_name()
            self.context.register(view_name, self.df)
            try:
                # Replace 'df' references with our unique view name in the query
                # Use word boundary matching to avoid replacing 'df' inside column names
                safe_query = re.sub(r"\bdf\b", view_name, query)
                res = self.sql_executor(safe_query, self.context)
                return self.with_df(res)
            finally:
                # Cleanup temp view to avoid memory leaks
                self.context.unregister(view_name)

        raise NotImplementedError("EngineContext.sql requires sql_executor to be set.")


class Context(ABC):
    """Abstract base for execution context."""

    @abstractmethod
    def register(self, name: str, df: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Register a DataFrame for use in downstream nodes.

        Args:
            name: Identifier for the DataFrame
            df: DataFrame (Spark or Pandas) or Iterator (Pandas chunked)
            metadata: Optional metadata (e.g. PII info)
        """
        pass

    @abstractmethod
    def get(self, name: str) -> Any:
        """Retrieve a registered DataFrame.

        Args:
            name: Identifier of the DataFrame

        Returns:
            The registered DataFrame

        Raises:
            KeyError: If name not found in context
        """
        pass

    @abstractmethod
    def get_metadata(self, name: str) -> Dict[str, Any]:
        """Retrieve metadata for a registered DataFrame.

        Args:
            name: Identifier of the DataFrame

        Returns:
            Metadata dictionary (empty if none)
        """
        pass

    @abstractmethod
    def has(self, name: str) -> bool:
        """Check if a DataFrame exists in context.

        Args:
            name: Identifier to check

        Returns:
            True if exists, False otherwise
        """
        pass

    @abstractmethod
    def list_names(self) -> list[str]:
        """List all registered DataFrame names.

        Returns:
            List of registered names
        """
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear all registered DataFrames."""
        pass

    def unregister(self, name: str) -> None:
        """Unregister a DataFrame from the context.

        Default implementation does nothing (optional cleanup).
        Subclasses can override for cleanup (e.g., dropping temp views).

        Args:
            name: Identifier to unregister
        """
        pass


class PandasContext(Context):
    """Context implementation for Pandas engine."""

    def __init__(self) -> None:
        """Initialize Pandas context."""
        self._data: Dict[str, Union[pd.DataFrame, Iterator[pd.DataFrame]]] = {}
        self._metadata: Dict[str, Dict[str, Any]] = {}

    def register(
        self,
        name: str,
        df: Union[pd.DataFrame, Iterator[pd.DataFrame], Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Register a Pandas DataFrame, Iterator, or LazyDataset.

        Args:
            name: Identifier for the DataFrame
            df: Pandas DataFrame or Iterator of DataFrames or LazyDataset
            metadata: Optional metadata
        """
        # Relaxed type check to support LazyDataset
        is_valid = (
            isinstance(df, pd.DataFrame)
            or isinstance(df, Iterator)
            or type(df).__name__ == "LazyDataset"
        )

        if not is_valid:
            raise TypeError(
                f"Expected pandas.DataFrame, Iterator, or LazyDataset, got {type(df).__module__}.{type(df).__name__}"
            )

        self._data[name] = df
        if metadata:
            self._metadata[name] = metadata

    def get(self, name: str) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """Retrieve a registered Pandas DataFrame or Iterator.

        Args:
            name: Identifier of the DataFrame

        Returns:
            The registered Pandas DataFrame or Iterator

        Raises:
            KeyError: If name not found in context
        """
        if name not in self._data:
            available = ", ".join(self._data.keys()) if self._data else "none"
            raise KeyError(f"DataFrame '{name}' not found in context. Available: {available}")
        return self._data[name]

    def get_metadata(self, name: str) -> Dict[str, Any]:
        """Retrieve metadata."""
        return self._metadata.get(name, {})

    def has(self, name: str) -> bool:
        """Check if a DataFrame exists.

        Args:
            name: Identifier to check

        Returns:
            True if exists, False otherwise
        """
        return name in self._data

    def list_names(self) -> list[str]:
        """List all registered DataFrame names.

        Returns:
            List of registered names
        """
        return list(self._data.keys())

    def clear(self) -> None:
        """Clear all registered DataFrames."""
        self._data.clear()

    def unregister(self, name: str) -> None:
        """Unregister a DataFrame from the context."""
        self._data.pop(name, None)
        self._metadata.pop(name, None)


class PolarsContext(Context):
    """Context implementation for Polars engine."""

    def __init__(self) -> None:
        """Initialize Polars context."""
        self._data: Dict[str, Any] = {}
        self._metadata: Dict[str, Dict[str, Any]] = {}

    def register(self, name: str, df: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Register a Polars DataFrame or LazyFrame.

        Args:
            name: Identifier for the DataFrame
            df: Polars DataFrame or LazyFrame
            metadata: Optional metadata
        """
        self._data[name] = df
        if metadata:
            self._metadata[name] = metadata

    def get(self, name: str) -> Any:
        """Retrieve a registered Polars DataFrame.

        Args:
            name: Identifier of the DataFrame

        Returns:
            The registered DataFrame

        Raises:
            KeyError: If name not found in context
        """
        if name not in self._data:
            available = ", ".join(self._data.keys()) if self._data else "none"
            raise KeyError(f"DataFrame '{name}' not found in context. Available: {available}")
        return self._data[name]

    def get_metadata(self, name: str) -> Dict[str, Any]:
        return self._metadata.get(name, {})

    def has(self, name: str) -> bool:
        """Check if a DataFrame exists.

        Args:
            name: Identifier to check

        Returns:
            True if exists, False otherwise
        """
        return name in self._data

    def list_names(self) -> list[str]:
        """List all registered DataFrame names.

        Returns:
            List of registered names
        """
        return list(self._data.keys())

    def clear(self) -> None:
        """Clear all registered DataFrames."""
        self._data.clear()

    def unregister(self, name: str) -> None:
        """Unregister a DataFrame from the context."""
        self._data.pop(name, None)
        self._metadata.pop(name, None)


class SparkContext(Context):
    """Context implementation for Spark engine."""

    def __init__(self, spark_session: Any) -> None:
        """Initialize Spark context.

        Args:
            spark_session: Active SparkSession
        """
        try:
            from pyspark.sql import DataFrame as SparkDataFrame
        except ImportError:
            # Fallback for when pyspark is not installed (e.g. testing without spark)
            SparkDataFrame = Any

        self.spark = spark_session
        self._spark_df_type = SparkDataFrame

        # Track registered views for cleanup
        self._registered_views: set[str] = set()

        # Lock for thread safety
        self._lock = threading.RLock()

        # Metadata store
        self._metadata: Dict[str, Dict[str, Any]] = {}

    def _validate_name(self, name: str) -> None:
        """Validate that node name is a valid Spark identifier.

        Spark SQL views should be alphanumeric + underscore.
        Spaces and special characters (hyphens) cause issues in SQL generation.

        Args:
            name: Node name to validate

        Raises:
            ValueError: If name is invalid
        """
        # Regex: alphanumeric and underscore only
        if not re.match(r"^[a-zA-Z0-9_]+$", name):
            raise ValueError(
                f"Invalid node name '{name}' for Spark engine. "
                "Names must contain only alphanumeric characters and underscores "
                "(no spaces or hyphens). Please rename this node in your configuration."
            )

    def register(self, name: str, df: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Register a Spark DataFrame as temp view.

        Args:
            name: Identifier for the DataFrame
            df: Spark DataFrame
            metadata: Optional metadata
        """
        # 1. Validate Type
        if self._spark_df_type is not Any and not isinstance(df, self._spark_df_type):
            if not hasattr(df, "createOrReplaceTempView"):
                raise TypeError(
                    f"Expected pyspark.sql.DataFrame, got {type(df).__module__}.{type(df).__name__}"
                )

        # 2. Validate Name (Explicit rule)
        self._validate_name(name)

        # 3. Register
        with self._lock:
            self._registered_views.add(name)
            if metadata:
                self._metadata[name] = metadata

        # Create view (metadata op)
        df.createOrReplaceTempView(name)

    def get(self, name: str) -> Any:
        """Retrieve a registered Spark DataFrame.

        Args:
            name: Identifier of the DataFrame

        Returns:
            The registered Spark DataFrame

        Raises:
            KeyError: If name not found in context
        """
        with self._lock:
            if name not in self._registered_views:
                available = ", ".join(self._registered_views) if self._registered_views else "none"
                raise KeyError(f"DataFrame '{name}' not found in context. Available: {available}")

        return self.spark.table(name)

    def get_metadata(self, name: str) -> Dict[str, Any]:
        with self._lock:
            return self._metadata.get(name, {})

    def has(self, name: str) -> bool:
        """Check if a DataFrame exists.

        Args:
            name: Identifier to check

        Returns:
            True if exists, False otherwise
        """
        with self._lock:
            return name in self._registered_views

    def list_names(self) -> list[str]:
        """List all registered DataFrame names.

        Returns:
            List of registered names
        """
        with self._lock:
            return list(self._registered_views)

    def clear(self) -> None:
        """Clear all registered temp views."""
        with self._lock:
            views_to_drop = list(self._registered_views)
            self._registered_views.clear()

        for name in views_to_drop:
            try:
                self.spark.catalog.dropTempView(name)
            except Exception as e:
                logger.debug(
                    f"Failed to drop temp view '{name}' during cleanup: {type(e).__name__}: {e}"
                )

    def unregister(self, name: str) -> None:
        """Unregister a temp view from Spark.

        Args:
            name: View name to drop
        """
        with self._lock:
            self._registered_views.discard(name)
            self._metadata.pop(name, None)

        try:
            self.spark.catalog.dropTempView(name)
        except Exception as e:
            logger.debug(f"Failed to drop temp view '{name}': {type(e).__name__}: {e}")


def create_context(engine: str, spark_session: Optional[Any] = None) -> Context:
    """Factory function to create appropriate context.

    Args:
        engine: Engine type ('pandas' or 'spark')
        spark_session: SparkSession (required if engine='spark')

    Returns:
        Context instance for the specified engine

    Raises:
        ValueError: If engine is invalid or SparkSession missing for Spark
    """
    if engine == "pandas":
        return PandasContext()
    elif engine == "spark":
        if spark_session is None:
            raise ValueError("SparkSession required for Spark engine")
        return SparkContext(spark_session)
    elif engine == "polars":
        return PolarsContext()
    else:
        raise ValueError(f"Unsupported engine: {engine}. Use 'pandas' or 'spark'")
