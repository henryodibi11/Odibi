import threading
import re
from typing import Dict, Any, Optional, Union
from abc import ABC, abstractmethod
from collections.abc import Iterator
import pandas as pd
from odibi.enums import EngineType


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
    ):
        self.context = context
        self.df = df
        self.engine_type = engine_type
        self.sql_executor = sql_executor

    @property
    def columns(self) -> list[str]:
        if hasattr(self.df, "columns"):
            return list(self.df.columns)
        # Spark
        if hasattr(self.df, "schema"):
            return self.df.columns
        return []

    def with_df(self, df: Any) -> "EngineContext":
        """Returns a new context with updated DataFrame."""
        return EngineContext(self.context, df, self.engine_type, self.sql_executor)

    def get(self, name: str) -> Any:
        """Get a dataset from global context."""
        return self.context.get(name)

    def register_temp_view(self, name: str, df: Any) -> None:
        """Register a temporary view for SQL."""
        self.context.register(name, df)

    def sql(self, query: str) -> "EngineContext":
        """Execute SQL on the current DataFrame (aliased as 'df')."""
        if self.sql_executor:
            # Workaround: Register current df as 'df' in the context
            # Note: We might be overwriting a 'df' if it existed, but 'df' is reserved for current.
            # Context implementations usually allow overwriting.
            self.context.register("df", self.df)
            try:
                res = self.sql_executor(query, self.context)
                return self.with_df(res)
            finally:
                # Optional: Cleanup 'df' from context to avoid pollution?
                # For now, keep it simple.
                pass

        raise NotImplementedError("EngineContext.sql requires sql_executor to be set.")


class Context(ABC):
    """Abstract base for execution context."""

    @abstractmethod
    def register(self, name: str, df: Any) -> None:
        """Register a DataFrame for use in downstream nodes.

        Args:
            name: Identifier for the DataFrame
            df: DataFrame (Spark or Pandas) or Iterator (Pandas chunked)
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


class PandasContext(Context):
    """Context implementation for Pandas engine."""

    def __init__(self) -> None:
        """Initialize Pandas context."""
        self._data: Dict[str, Union[pd.DataFrame, Iterator[pd.DataFrame]]] = {}

    def register(self, name: str, df: Union[pd.DataFrame, Iterator[pd.DataFrame]]) -> None:
        """Register a Pandas DataFrame or Iterator.

        Args:
            name: Identifier for the DataFrame
            df: Pandas DataFrame or Iterator of DataFrames
        """
        from collections.abc import Iterator

        if not isinstance(df, pd.DataFrame) and not isinstance(df, Iterator):
            raise TypeError(f"Expected pandas.DataFrame or Iterator, got {type(df)}")
        self._data[name] = df

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

    def register(self, name: str, df: Any) -> None:
        """Register a Spark DataFrame as temp view.

        Args:
            name: Identifier for the DataFrame
            df: Spark DataFrame
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
            except Exception:
                pass


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
    else:
        raise ValueError(f"Unsupported engine: {engine}. Use 'pandas' or 'spark'")
