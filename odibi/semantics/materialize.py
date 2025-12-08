"""
Materialization Module
======================

Execute and persist materialized metric aggregations.

Materialization pre-computes aggregated metrics at a specific grain
and writes them to an output table for faster querying.
"""

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.semantics.metrics import (
    MaterializationConfig,
    SemanticLayerConfig,
)
from odibi.semantics.query import SemanticQuery
from odibi.utils.logging_context import get_logging_context


@dataclass
class MaterializationResult:
    """Result of a materialization execution."""

    name: str
    output: str
    row_count: int
    elapsed_ms: float
    success: bool
    error: Optional[str] = None


class Materializer:
    """
    Execute materializations defined in the semantic layer config.

    Usage:
        config = SemanticLayerConfig(...)
        materializer = Materializer(config)
        result = materializer.execute("monthly_revenue_by_region", context)
    """

    def __init__(self, config: SemanticLayerConfig):
        """
        Initialize with semantic layer configuration.

        Args:
            config: SemanticLayerConfig with materializations
        """
        self.config = config
        self._query = SemanticQuery(config)

    def execute(
        self,
        name: str,
        context: EngineContext,
        write_callback: Optional[Any] = None,
    ) -> MaterializationResult:
        """
        Execute a single materialization.

        Args:
            name: Name of the materialization to execute
            context: EngineContext with source data
            write_callback: Optional callback to write output
                           Function signature: (df, output_path) -> None

        Returns:
            MaterializationResult with execution details
        """
        ctx = get_logging_context()
        start_time = time.time()

        ctx.info("Starting materialization", name=name)

        mat_config = self.config.get_materialization(name)
        if mat_config is None:
            available = [m.name for m in self.config.materializations]
            raise ValueError(f"Materialization '{name}' not found. Available: {available}")

        try:
            query_string = self._build_query_string(mat_config)
            ctx.debug("Built query for materialization", query=query_string)

            result = self._query.execute(query_string, context)

            if write_callback:
                write_callback(result.df, mat_config.output)
                ctx.debug("Wrote materialization output", output=mat_config.output)

            elapsed_ms = (time.time() - start_time) * 1000

            ctx.info(
                "Materialization completed",
                name=name,
                output=mat_config.output,
                rows=result.row_count,
                elapsed_ms=round(elapsed_ms, 2),
            )

            return MaterializationResult(
                name=name,
                output=mat_config.output,
                row_count=result.row_count,
                elapsed_ms=elapsed_ms,
                success=True,
            )

        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            ctx.error(
                f"Materialization failed: {e}",
                name=name,
                error_type=type(e).__name__,
                elapsed_ms=round(elapsed_ms, 2),
            )

            return MaterializationResult(
                name=name,
                output=mat_config.output if mat_config else "",
                row_count=0,
                elapsed_ms=elapsed_ms,
                success=False,
                error=str(e),
            )

    def execute_all(
        self,
        context: EngineContext,
        write_callback: Optional[Any] = None,
    ) -> List[MaterializationResult]:
        """
        Execute all configured materializations.

        Args:
            context: EngineContext with source data
            write_callback: Optional callback to write output

        Returns:
            List of MaterializationResult for each materialization
        """
        ctx = get_logging_context()
        results = []

        for mat_config in self.config.materializations:
            result = self.execute(mat_config.name, context, write_callback)
            results.append(result)

        success_count = sum(1 for r in results if r.success)
        ctx.info(
            "All materializations completed",
            total=len(results),
            success=success_count,
            failed=len(results) - success_count,
        )

        return results

    def _build_query_string(self, mat_config: MaterializationConfig) -> str:
        """
        Build a semantic query string from materialization config.

        Args:
            mat_config: MaterializationConfig

        Returns:
            Query string like "metric1, metric2 BY dim1, dim2"
        """
        metrics_part = ", ".join(mat_config.metrics)
        dims_part = ", ".join(mat_config.dimensions)

        return f"{metrics_part} BY {dims_part}"

    def get_schedule(self, name: str) -> Optional[str]:
        """
        Get the schedule for a materialization.

        Args:
            name: Materialization name

        Returns:
            Cron schedule string or None
        """
        mat_config = self.config.get_materialization(name)
        return mat_config.schedule if mat_config else None

    def list_materializations(self) -> List[Dict[str, Any]]:
        """
        List all configured materializations.

        Returns:
            List of materialization info dicts
        """
        return [
            {
                "name": mat.name,
                "metrics": mat.metrics,
                "dimensions": mat.dimensions,
                "output": mat.output,
                "schedule": mat.schedule,
            }
            for mat in self.config.materializations
        ]


class IncrementalMaterializer:
    """
    Execute incremental materializations with merge strategy.

    Supports:
    - Replace: Replace rows matching the grain
    - Sum: Add values to existing aggregates
    """

    def __init__(self, config: SemanticLayerConfig):
        """
        Initialize with semantic layer configuration.

        Args:
            config: SemanticLayerConfig with materializations
        """
        self.config = config
        self._base_materializer = Materializer(config)

    def execute_incremental(
        self,
        name: str,
        context: EngineContext,
        existing_df: Any,
        timestamp_column: str,
        since_timestamp: Any = None,
        merge_strategy: str = "replace",
    ) -> MaterializationResult:
        """
        Execute an incremental materialization.

        Args:
            name: Materialization name
            context: EngineContext with source data
            existing_df: Existing materialized data
            timestamp_column: Column for incremental filtering
            since_timestamp: Only process data after this timestamp
            merge_strategy: "replace" or "sum"

        Returns:
            MaterializationResult with merged data
        """
        ctx = get_logging_context()
        start_time = time.time()

        mat_config = self.config.get_materialization(name)
        if mat_config is None:
            raise ValueError(f"Materialization '{name}' not found")

        try:
            source_df = context.df

            if since_timestamp is not None:
                if context.engine_type == EngineType.SPARK:
                    from pyspark.sql import functions as F

                    source_df = source_df.filter(F.col(timestamp_column) > since_timestamp)
                else:
                    source_df = source_df[source_df[timestamp_column] > since_timestamp]

            incremental_context = context.with_df(source_df)

            query_string = self._base_materializer._build_query_string(mat_config)
            query = SemanticQuery(self.config)
            new_result = query.execute(query_string, incremental_context)

            merged_df = self._merge_results(
                context,
                existing_df,
                new_result.df,
                mat_config.dimensions,
                mat_config.metrics,
                merge_strategy,
            )

            if context.engine_type == EngineType.SPARK:
                row_count = merged_df.count()
            else:
                row_count = len(merged_df)

            elapsed_ms = (time.time() - start_time) * 1000

            ctx.info(
                "Incremental materialization completed",
                name=name,
                strategy=merge_strategy,
                rows=row_count,
                elapsed_ms=round(elapsed_ms, 2),
            )

            return MaterializationResult(
                name=name,
                output=mat_config.output,
                row_count=row_count,
                elapsed_ms=elapsed_ms,
                success=True,
            )

        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            ctx.error(
                f"Incremental materialization failed: {e}",
                name=name,
                error_type=type(e).__name__,
            )

            return MaterializationResult(
                name=name,
                output=mat_config.output if mat_config else "",
                row_count=0,
                elapsed_ms=elapsed_ms,
                success=False,
                error=str(e),
            )

    def _merge_results(
        self,
        context: EngineContext,
        existing_df: Any,
        new_df: Any,
        dimensions: List[str],
        metrics: List[str],
        strategy: str,
    ) -> Any:
        """Merge new results with existing data."""
        if context.engine_type == EngineType.SPARK:
            return self._merge_spark(existing_df, new_df, dimensions, metrics, strategy)
        else:
            return self._merge_pandas(existing_df, new_df, dimensions, metrics, strategy)

    def _merge_spark(
        self,
        existing_df: Any,
        new_df: Any,
        dimensions: List[str],
        metrics: List[str],
        strategy: str,
    ) -> Any:
        """Merge using Spark."""
        from pyspark.sql import functions as F

        if strategy == "replace":
            join_cond = [existing_df[d] == new_df[d] for d in dimensions]
            unchanged = existing_df.join(new_df, join_cond, "left_anti")
            return unchanged.unionByName(new_df, allowMissingColumns=True)

        elif strategy == "sum":
            combined = existing_df.unionByName(new_df, allowMissingColumns=True)
            agg_exprs = [F.sum(F.col(m)).alias(m) for m in metrics]
            return combined.groupBy(dimensions).agg(*agg_exprs)

        else:
            raise ValueError(f"Unknown merge strategy: {strategy}")

    def _merge_pandas(
        self,
        existing_df: Any,
        new_df: Any,
        dimensions: List[str],
        metrics: List[str],
        strategy: str,
    ) -> Any:
        """Merge using Pandas."""
        import pandas as pd

        if strategy == "replace":
            merged = pd.merge(
                existing_df,
                new_df[dimensions],
                on=dimensions,
                how="left",
                indicator=True,
            )
            unchanged = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
            unchanged = unchanged[existing_df.columns]
            return pd.concat([unchanged, new_df], ignore_index=True)

        elif strategy == "sum":
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            return combined.groupby(dimensions, as_index=False)[metrics].sum()

        else:
            raise ValueError(f"Unknown merge strategy: {strategy}")
