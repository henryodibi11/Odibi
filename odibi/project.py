"""
Project Module
==============

Unified Project API that integrates pipelines and semantic layer.

The Project class provides a seamless interface for:
- Loading project configuration (connections, pipelines, semantic layer)
- Executing semantic queries with auto-resolved table paths
- No manual table registration required

Example:
    project = Project.load("odibi.yaml")
    result = project.query("revenue BY region")
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from odibi.config import ConnectionConfig, ProjectConfig, load_config_from_file
from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.semantics.metrics import SemanticLayerConfig, parse_semantic_config
from odibi.semantics.query import QueryResult, SemanticQuery
from odibi.utils.logging_context import get_logging_context


class SourceResolver:
    """
    Resolves semantic layer source references to actual paths.

    Supported source formats:

    1. **$pipeline.node** (recommended): References a pipeline node's write target.
       Example: `$build_warehouse.fact_orders` reads from wherever that node writes.

    2. **connection.path**: Explicit connection + path. Supports nested paths!
       The split happens on the FIRST dot only, so subdirectories work:
       - `gold.fact_orders` → `/mnt/data/gold/fact_orders`
       - `gold.oee/plant_a/metrics` → `/mnt/data/gold/oee/plant_a/metrics`
       - `gold.domain/v2/fact_sales` → `/mnt/data/gold/domain/v2/fact_sales`

    3. **table_name**: Uses the default connection (gold > silver > bronze > first).
       Example: `fact_orders` with a single connection named "warehouse".

    For Unity Catalog connections (catalog + schema_name):
       `gold.fact_orders` → `catalog.schema.fact_orders`
    """

    def __init__(
        self,
        connections: Dict[str, ConnectionConfig],
        base_path: str = "",
        pipelines: Optional[List[Any]] = None,
    ):
        """
        Initialize the source resolver.

        Args:
            connections: Dictionary of connection configurations
            base_path: Base path for relative paths (directory of odibi.yaml)
            pipelines: List of pipeline configs for $pipeline.node resolution
        """
        self.connections = connections
        self.base_path = base_path
        self.pipelines = pipelines or []
        self._node_index = self._build_node_index()

    def _build_node_index(self) -> Dict[str, Dict[str, Any]]:
        """Build an index of pipeline.node -> node config for fast lookup."""
        index = {}
        for pipeline in self.pipelines:
            pipeline_name = (
                pipeline.pipeline if hasattr(pipeline, "pipeline") else pipeline.get("pipeline", "")
            )
            nodes = pipeline.nodes if hasattr(pipeline, "nodes") else pipeline.get("nodes", [])
            for node in nodes:
                node_name = node.name if hasattr(node, "name") else node.get("name", "")
                key = f"{pipeline_name}.{node_name}"
                index[key] = node
        return index

    def resolve(self, source: str) -> tuple[str, str]:
        """
        Resolve a source reference to connection name and full path.

        Args:
            source: Source reference. Supported formats:
                - "$pipeline.node" (e.g., "$build_warehouse.fact_orders")
                - "connection.table" (e.g., "gold.fact_orders")
                - "table_name" (e.g., "fact_orders") - uses default connection

        Returns:
            Tuple of (connection_name, full_path)

        Raises:
            ValueError: If connection or node not found
        """
        # Handle $pipeline.node reference
        if source.startswith("$"):
            return self._resolve_node_reference(source)

        # Handle connection.table or bare table name
        if "." in source:
            connection_name, table_name = source.split(".", 1)
        else:
            connection_name = self._find_default_connection()
            table_name = source

        if connection_name not in self.connections:
            available = list(self.connections.keys())
            raise ValueError(
                f"Connection '{connection_name}' not found in source '{source}'. "
                f"Available connections: {available}"
            )

        connection = self.connections[connection_name]
        full_path = self._build_path(connection, table_name)

        return connection_name, full_path

    def _resolve_node_reference(self, source: str) -> tuple[str, str]:
        """
        Resolve a $pipeline.node reference to connection and path.

        Args:
            source: Node reference (e.g., "$build_warehouse.fact_orders")

        Returns:
            Tuple of (connection_name, full_path)

        Raises:
            ValueError: If node not found or node has no write config
        """
        # Remove $ prefix and parse
        ref = source[1:]  # Remove $

        if ref not in self._node_index:
            available = list(self._node_index.keys())
            raise ValueError(f"Node reference '{source}' not found. Available nodes: {available}")

        node = self._node_index[ref]

        # Get write config from node
        write_config = node.write if hasattr(node, "write") else node.get("write")
        if not write_config:
            raise ValueError(f"Node '{source}' has no 'write' config. Cannot resolve source path.")

        # Extract connection and path/table from write config
        if hasattr(write_config, "connection"):
            connection_name = write_config.connection
            table_name = write_config.table or write_config.path
        else:
            connection_name = write_config.get("connection")
            table_name = write_config.get("table") or write_config.get("path")

        if not connection_name:
            raise ValueError(
                f"Node '{source}' write config has no 'connection'. Cannot resolve source path."
            )

        if not table_name:
            raise ValueError(
                f"Node '{source}' write config has no 'table' or 'path'. "
                "Cannot resolve source path."
            )

        if connection_name not in self.connections:
            available = list(self.connections.keys())
            raise ValueError(
                f"Connection '{connection_name}' from node '{source}' not found. "
                f"Available connections: {available}"
            )

        connection = self.connections[connection_name]
        full_path = self._build_path(connection, table_name)

        return connection_name, full_path

    def _find_default_connection(self) -> str:
        """Find the default connection to use when not specified."""
        if len(self.connections) == 1:
            return list(self.connections.keys())[0]

        priority = ["gold", "silver", "bronze", "warehouse", "default"]
        for name in priority:
            if name in self.connections:
                return name

        return list(self.connections.keys())[0]

    def _build_path(self, connection: ConnectionConfig, table_name: str) -> str:
        """Build the full path for a table given a connection."""
        conn_dict = connection.model_dump() if hasattr(connection, "model_dump") else connection

        if "base_path" in conn_dict:
            base = conn_dict["base_path"]
        elif "path" in conn_dict:
            base = conn_dict["path"]
        elif "catalog" in conn_dict and "schema" in conn_dict:
            return f"{conn_dict['catalog']}.{conn_dict['schema']}.{table_name}"
        else:
            base = ""

        if self.base_path and not os.path.isabs(base):
            base = os.path.join(self.base_path, base)

        return os.path.join(base, table_name) if base else table_name


class Project:
    """
    Unified Project API for Odibi.

    Integrates project configuration, connections, and semantic layer
    into a single interface for seamless querying.

    Example:
        # Load project and query
        project = Project.load("odibi.yaml")
        result = project.query("revenue BY region")

        # Access the DataFrame
        print(result.df)

        # Multiple metrics and dimensions
        result = project.query("revenue, order_count BY region, month")

        # With filters
        result = project.query("revenue BY category WHERE region = 'North'")
    """

    def __init__(
        self,
        config: ProjectConfig,
        semantic_config: Optional[SemanticLayerConfig] = None,
        base_path: str = "",
        lazy: bool = True,
    ):
        """
        Initialize the Project.

        Args:
            config: ProjectConfig instance
            semantic_config: Optional SemanticLayerConfig (loaded from config.semantic if not provided)
            base_path: Base path for resolving relative paths
            lazy: If True, load tables on-demand; if False, load all upfront
        """
        self.config = config
        self.base_path = base_path
        self.lazy = lazy
        self._context: Optional[EngineContext] = None
        self._loaded_tables: Dict[str, Any] = {}

        self._resolver = SourceResolver(config.connections, base_path, config.pipelines)

        if semantic_config:
            self._semantic_config = semantic_config
        else:
            self._semantic_config = self._load_semantic_config()

        self._query_engine: Optional[SemanticQuery] = None
        if self._semantic_config:
            self._query_engine = SemanticQuery(self._semantic_config)

    @classmethod
    def load(
        cls,
        config_path: str,
        semantic_path: Optional[str] = None,
        lazy: bool = True,
    ) -> "Project":
        """
        Load a Project from configuration file(s).

        Args:
            config_path: Path to odibi.yaml
            semantic_path: Optional path to semantic config (overrides config.semantic)
            lazy: If True, load tables on-demand

        Returns:
            Project instance
        """
        ctx = get_logging_context()
        ctx.info("Loading project", config=config_path)

        config = load_config_from_file(config_path)
        base_path = str(Path(config_path).parent.absolute())

        semantic_config = None
        if semantic_path:
            semantic_config = cls._load_semantic_from_file(semantic_path, base_path)
        elif config.semantic:
            if "config" in config.semantic:
                semantic_file = config.semantic["config"]
                if not os.path.isabs(semantic_file):
                    semantic_file = os.path.join(base_path, semantic_file)
                semantic_config = cls._load_semantic_from_file(semantic_file, base_path)
            else:
                semantic_config = parse_semantic_config(config.semantic)

        return cls(
            config=config,
            semantic_config=semantic_config,
            base_path=base_path,
            lazy=lazy,
        )

    @staticmethod
    def _load_semantic_from_file(path: str, base_path: str) -> SemanticLayerConfig:
        """Load semantic config from a YAML file."""
        import yaml

        with open(path, "r") as f:
            data = yaml.safe_load(f)

        return parse_semantic_config(data)

    def _load_semantic_config(self) -> Optional[SemanticLayerConfig]:
        """Load semantic config from project config."""
        if not self.config.semantic:
            return None

        if "config" in self.config.semantic:
            semantic_file = self.config.semantic["config"]
            if not os.path.isabs(semantic_file):
                semantic_file = os.path.join(self.base_path, semantic_file)
            return self._load_semantic_from_file(semantic_file, self.base_path)
        else:
            return parse_semantic_config(self.config.semantic)

    def _get_context(self) -> EngineContext:
        """Get or create the engine context."""
        if self._context is None:
            from odibi.context import PandasContext, PolarsContext

            engine_type = EngineType(self.config.engine.value)

            if engine_type == EngineType.PANDAS:
                base_context = PandasContext()
            elif engine_type == EngineType.POLARS:
                base_context = PolarsContext()
            else:
                from odibi.context import SparkContext
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()
                base_context = SparkContext(spark)

            self._context = EngineContext(context=base_context, df=None, engine_type=engine_type)
        return self._context

    def _load_table(self, source: str) -> Any:
        """
        Load a table from its source reference.

        Args:
            source: Source reference (e.g., "gold.fact_orders")

        Returns:
            DataFrame (Pandas, Spark, or Polars depending on engine)
        """
        if source in self._loaded_tables:
            return self._loaded_tables[source]

        ctx = get_logging_context()
        connection_name, full_path = self._resolver.resolve(source)
        connection = self.config.connections[connection_name]

        ctx.debug("Loading table", source=source, path=full_path)

        df = self._read_from_connection(connection, full_path)
        self._loaded_tables[source] = df

        return df

    def _read_from_connection(self, connection: ConnectionConfig, path: str) -> Any:
        """
        Read data from a connection.

        Supports Delta, local files, and catalog references.
        """
        conn_dict = connection.model_dump() if hasattr(connection, "model_dump") else connection
        conn_type = conn_dict.get("type", "local")

        engine_type = EngineType(self.config.engine.value)

        if engine_type == EngineType.SPARK:
            return self._read_spark(conn_dict, path, conn_type)
        elif engine_type == EngineType.POLARS:
            return self._read_polars(conn_dict, path, conn_type)
        else:
            return self._read_pandas(conn_dict, path, conn_type)

    def _read_spark(self, conn_dict: Dict, path: str, conn_type: str) -> Any:
        """Read data using Spark."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        if conn_type == "delta":
            if "catalog" in conn_dict:
                return spark.table(path)
            else:
                return spark.read.format("delta").load(path)
        else:
            if os.path.exists(path):
                if path.endswith(".parquet") or os.path.isdir(path):
                    return spark.read.parquet(path)
                elif path.endswith(".csv"):
                    return spark.read.csv(path, header=True, inferSchema=True)
            return spark.table(path)

    def _read_pandas(self, conn_dict: Dict, path: str, conn_type: str) -> Any:
        """Read data using Pandas."""
        import pandas as pd

        if conn_type == "delta":
            try:
                from deltalake import DeltaTable

                dt = DeltaTable(path)
                return dt.to_pandas()
            except ImportError:
                raise ImportError(
                    "deltalake package required for Delta tables with Pandas. "
                    "Install with: pip install deltalake"
                )
        else:
            if path.endswith(".parquet") or os.path.isdir(path):
                return pd.read_parquet(path)
            elif path.endswith(".csv"):
                return pd.read_csv(path)
            else:
                return pd.read_parquet(path)

    def _read_polars(self, conn_dict: Dict, path: str, conn_type: str) -> Any:
        """Read data using Polars."""
        import polars as pl

        if conn_type == "delta":
            return pl.read_delta(path)
        else:
            if path.endswith(".parquet") or os.path.isdir(path):
                return pl.read_parquet(path)
            elif path.endswith(".csv"):
                return pl.read_csv(path)
            else:
                return pl.read_parquet(path)

    def _get_sources_for_query(self, query_string: str) -> List[str]:
        """
        Get all source tables needed for a query.

        Args:
            query_string: Semantic query string

        Returns:
            List of source references
        """
        if not self._query_engine:
            return []

        parsed = self._query_engine.parse(query_string)
        sources = set()

        for metric_name in parsed.metrics:
            metric = self._semantic_config.get_metric(metric_name)
            if metric and metric.source:
                sources.add(metric.source)

        for dim_name in parsed.dimensions:
            dim = self._semantic_config.get_dimension(dim_name)
            if dim and dim.source:
                sources.add(dim.source)

        return list(sources)

    def _ensure_tables_loaded(self, sources: List[str]) -> None:
        """
        Ensure all required tables are loaded into context.

        Args:
            sources: List of source references to load
        """
        context = self._get_context()

        for source in sources:
            table_name = source.split(".")[-1] if "." in source else source

            if table_name not in self._loaded_tables:
                df = self._load_table(source)
                context.context.register(table_name, df)

    def query(self, query_string: str) -> QueryResult:
        """
        Execute a semantic query.

        Args:
            query_string: Semantic query (e.g., "revenue BY region")

        Returns:
            QueryResult with DataFrame and metadata

        Raises:
            ValueError: If semantic layer not configured or invalid query
        """
        if not self._query_engine:
            raise ValueError(
                "Semantic layer not configured. Add 'semantic' section to odibi.yaml "
                "or provide a semantic config file."
            )

        ctx = get_logging_context()
        ctx.info("Executing project query", query=query_string)

        sources = self._get_sources_for_query(query_string)
        self._ensure_tables_loaded(sources)

        context = self._get_context()
        return self._query_engine.execute(query_string, context)

    def register(self, name: str, df: Any) -> None:
        """
        Manually register a DataFrame in the context.

        Useful for testing or when data comes from non-standard sources.

        Args:
            name: Table name to register
            df: DataFrame to register
        """
        context = self._get_context()
        context.context.register(name, df)
        self._loaded_tables[name] = df

    @property
    def semantic_config(self) -> Optional[SemanticLayerConfig]:
        """Get the semantic layer configuration."""
        return self._semantic_config

    @property
    def connections(self) -> Dict[str, ConnectionConfig]:
        """Get the connection configurations."""
        return self.config.connections

    @property
    def metrics(self) -> List[str]:
        """Get list of available metric names."""
        if not self._semantic_config:
            return []
        return [m.name for m in self._semantic_config.metrics]

    @property
    def dimensions(self) -> List[str]:
        """Get list of available dimension names."""
        if not self._semantic_config:
            return []
        return [d.name for d in self._semantic_config.dimensions]

    def describe(self) -> Dict[str, Any]:
        """
        Get a description of the project and its semantic layer.

        Returns:
            Dictionary with project info, metrics, dimensions
        """
        return {
            "project": self.config.project,
            "engine": self.config.engine.value,
            "connections": list(self.config.connections.keys()),
            "metrics": [
                {"name": m.name, "description": m.description, "source": m.source}
                for m in (self._semantic_config.metrics if self._semantic_config else [])
            ],
            "dimensions": [
                {"name": d.name, "source": d.source, "column": d.get_column()}
                for d in (self._semantic_config.dimensions if self._semantic_config else [])
            ],
        }
