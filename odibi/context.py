"""Unified context for data passing between nodes."""

from typing import Dict, Any, Optional
from abc import ABC, abstractmethod
import pandas as pd


class Context(ABC):
    """Abstract base for execution context."""

    @abstractmethod
    def register(self, name: str, df: Any) -> None:
        """Register a DataFrame for use in downstream nodes.

        Args:
            name: Identifier for the DataFrame
            df: DataFrame (Spark or Pandas)
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
        self._data: Dict[str, pd.DataFrame] = {}

    def register(self, name: str, df: pd.DataFrame) -> None:
        """Register a Pandas DataFrame.

        Args:
            name: Identifier for the DataFrame
            df: Pandas DataFrame
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Expected pandas.DataFrame, got {type(df)}")
        self._data[name] = df

    def get(self, name: str) -> pd.DataFrame:
        """Retrieve a registered Pandas DataFrame.

        Args:
            name: Identifier of the DataFrame

        Returns:
            The registered Pandas DataFrame

        Raises:
            KeyError: If name not found in context
        """
        if name not in self._data:
            available = ", ".join(self._data.keys()) if self._data else "none"
            raise KeyError(f"DataFrame '{name}' not found in context. " f"Available: {available}")
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
        self.spark = spark_session
        self._registered_views: set[str] = set()

    def register(self, name: str, df: Any) -> None:
        """Register a Spark DataFrame as temp view.

        Args:
            name: Identifier for the DataFrame
            df: Spark DataFrame
        """
        # Validate it's a Spark DataFrame
        df_type = type(df).__name__
        if df_type != "DataFrame":
            raise TypeError(f"Expected Spark DataFrame, got {df_type}")

        # Create or replace temp view
        df.createOrReplaceTempView(name)
        self._registered_views.add(name)

    def get(self, name: str) -> Any:
        """Retrieve a registered Spark DataFrame.

        Args:
            name: Identifier of the DataFrame

        Returns:
            The registered Spark DataFrame

        Raises:
            KeyError: If name not found in context
        """
        if name not in self._registered_views:
            available = ", ".join(self._registered_views) if self._registered_views else "none"
            raise KeyError(f"DataFrame '{name}' not found in context. " f"Available: {available}")
        return self.spark.table(name)

    def has(self, name: str) -> bool:
        """Check if a DataFrame exists.

        Args:
            name: Identifier to check

        Returns:
            True if exists, False otherwise
        """
        return name in self._registered_views

    def list_names(self) -> list[str]:
        """List all registered DataFrame names.

        Returns:
            List of registered names
        """
        return list(self._registered_views)

    def clear(self) -> None:
        """Clear all registered temp views."""
        for name in self._registered_views:
            self.spark.catalog.dropTempView(name)
        self._registered_views.clear()


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
