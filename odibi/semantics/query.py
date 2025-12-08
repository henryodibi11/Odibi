"""
Semantic Query Module
=====================

Parse and execute semantic queries in the format:
    "metric1, metric2 BY dimension1, dimension2"

Example:
    "revenue, order_count BY region, month"

This generates SQL-like aggregation queries from semantic definitions.
"""

import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.semantics.metrics import (
    DimensionDefinition,
    MetricDefinition,
    SemanticLayerConfig,
)
from odibi.utils.logging_context import get_logging_context


@dataclass
class ParsedQuery:
    """Result of parsing a semantic query string."""

    metrics: List[str] = field(default_factory=list)
    dimensions: List[str] = field(default_factory=list)
    filters: List[str] = field(default_factory=list)
    raw_query: str = ""


@dataclass
class QueryResult:
    """Result of executing a semantic query."""

    df: Any
    metrics: List[str]
    dimensions: List[str]
    row_count: int
    elapsed_ms: float
    sql_generated: Optional[str] = None


class SemanticQuery:
    """
    Execute semantic queries against a configured semantic layer.

    Usage:
        config = SemanticLayerConfig(...)
        query = SemanticQuery(config)
        result = query.execute("revenue BY region, month", context)
    """

    def __init__(self, config: SemanticLayerConfig):
        """
        Initialize with semantic layer configuration.

        Args:
            config: SemanticLayerConfig with metrics and dimensions
        """
        self.config = config
        self._metric_cache: Dict[str, MetricDefinition] = {}
        self._dimension_cache: Dict[str, DimensionDefinition] = {}

        for metric in config.metrics:
            self._metric_cache[metric.name] = metric

        for dim in config.dimensions:
            self._dimension_cache[dim.name] = dim

    def parse(self, query_string: str) -> ParsedQuery:
        """
        Parse a semantic query string.

        Format: "metric1, metric2 BY dimension1, dimension2 WHERE condition"

        Args:
            query_string: Query string to parse

        Returns:
            ParsedQuery with extracted metrics, dimensions, filters
        """
        ctx = get_logging_context()
        ctx.debug("Parsing semantic query", query=query_string)

        result = ParsedQuery(raw_query=query_string)
        query = query_string.strip()

        where_match = re.search(r"\s+WHERE\s+(.+)$", query, re.IGNORECASE)
        if where_match:
            result.filters = [where_match.group(1).strip()]
            query = query[: where_match.start()]

        by_match = re.search(r"\s+BY\s+(.+)$", query, re.IGNORECASE)
        if by_match:
            dim_part = by_match.group(1).strip()
            result.dimensions = [d.strip().lower() for d in dim_part.split(",")]
            query = query[: by_match.start()]

        metric_part = query.strip()
        if metric_part:
            result.metrics = [m.strip().lower() for m in metric_part.split(",")]

        ctx.debug(
            "Parsed semantic query",
            metrics=result.metrics,
            dimensions=result.dimensions,
            filters=result.filters,
        )

        return result

    def validate(self, parsed: ParsedQuery) -> List[str]:
        """
        Validate a parsed query against the semantic layer config.

        Args:
            parsed: ParsedQuery to validate

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        for metric_name in parsed.metrics:
            if metric_name not in self._metric_cache:
                available = list(self._metric_cache.keys())
                errors.append(f"Unknown metric '{metric_name}'. Available: {available}")

        for dim_name in parsed.dimensions:
            if dim_name not in self._dimension_cache:
                available = list(self._dimension_cache.keys())
                errors.append(f"Unknown dimension '{dim_name}'. Available: {available}")

        if not parsed.metrics:
            errors.append("At least one metric is required")

        return errors

    def generate_sql(self, parsed: ParsedQuery) -> Tuple[str, str]:
        """
        Generate SQL from a parsed query.

        Args:
            parsed: ParsedQuery with metrics and dimensions

        Returns:
            Tuple of (SQL query, source table name)
        """
        if not parsed.metrics:
            raise ValueError("At least one metric is required")

        metric_defs = [self._metric_cache[m] for m in parsed.metrics]
        sources = {m.source for m in metric_defs if m.source}
        if not sources:
            raise ValueError("No source table found for metrics")

        source_table = list(sources)[0]

        select_parts = []

        for dim_name in parsed.dimensions:
            dim_def = self._dimension_cache.get(dim_name)
            if dim_def:
                col = dim_def.get_column()
                select_parts.append(col)
            else:
                select_parts.append(dim_name)

        for metric_def in metric_defs:
            select_parts.append(f"{metric_def.expr} AS {metric_def.name}")

        select_clause = ", ".join(select_parts) if select_parts else "*"

        all_filters = []
        for metric_def in metric_defs:
            all_filters.extend(metric_def.filters)
        all_filters.extend(parsed.filters)

        where_clause = ""
        if all_filters:
            where_clause = " WHERE " + " AND ".join(f"({f})" for f in all_filters)

        group_by_clause = ""
        if parsed.dimensions:
            group_cols = []
            for dim_name in parsed.dimensions:
                dim_def = self._dimension_cache.get(dim_name)
                if dim_def:
                    group_cols.append(dim_def.get_column())
                else:
                    group_cols.append(dim_name)
            group_by_clause = " GROUP BY " + ", ".join(group_cols)

        sql = f"SELECT {select_clause} FROM {source_table}{where_clause}{group_by_clause}"

        return sql, source_table

    def execute(
        self,
        query_string: str,
        context: EngineContext,
        source_df: Optional[Any] = None,
    ) -> QueryResult:
        """
        Execute a semantic query.

        Args:
            query_string: Semantic query string (e.g., "revenue BY region")
            context: EngineContext for execution
            source_df: Optional source DataFrame (overrides context lookup)

        Returns:
            QueryResult with DataFrame and metadata
        """
        ctx = get_logging_context()
        start_time = time.time()

        ctx.info("Executing semantic query", query=query_string)

        parsed = self.parse(query_string)
        errors = self.validate(parsed)
        if errors:
            raise ValueError(f"Invalid semantic query: {'; '.join(errors)}")

        sql, source_table = self.generate_sql(parsed)
        ctx.debug("Generated SQL", sql=sql, source=source_table)

        if source_df is None:
            try:
                source_df = context.get(source_table)
            except KeyError:
                raise ValueError(f"Source table '{source_table}' not found in context")

        result_df = self._execute_query(context, source_df, parsed)

        if context.engine_type == EngineType.SPARK:
            row_count = result_df.count()
        else:
            row_count = len(result_df)

        elapsed_ms = (time.time() - start_time) * 1000

        ctx.info(
            "Semantic query completed",
            query=query_string,
            rows=row_count,
            elapsed_ms=round(elapsed_ms, 2),
        )

        return QueryResult(
            df=result_df,
            metrics=parsed.metrics,
            dimensions=parsed.dimensions,
            row_count=row_count,
            elapsed_ms=elapsed_ms,
            sql_generated=sql,
        )

    def _execute_query(
        self,
        context: EngineContext,
        source_df: Any,
        parsed: ParsedQuery,
    ) -> Any:
        """Execute the query using the appropriate engine."""
        if context.engine_type == EngineType.SPARK:
            return self._execute_spark(context, source_df, parsed)
        else:
            return self._execute_pandas(source_df, parsed)

    def _execute_spark(
        self,
        context: EngineContext,
        source_df: Any,
        parsed: ParsedQuery,
    ) -> Any:
        """Execute query using Spark."""
        from pyspark.sql import functions as F

        df = source_df

        all_filters = []
        for metric_name in parsed.metrics:
            metric_def = self._metric_cache.get(metric_name)
            if metric_def:
                all_filters.extend(metric_def.filters)
        all_filters.extend(parsed.filters)

        for filter_expr in all_filters:
            df = df.filter(filter_expr)

        group_cols = []
        for dim_name in parsed.dimensions:
            dim_def = self._dimension_cache.get(dim_name)
            if dim_def:
                group_cols.append(F.col(dim_def.get_column()))
            else:
                group_cols.append(F.col(dim_name))

        agg_exprs = []
        for metric_name in parsed.metrics:
            metric_def = self._metric_cache.get(metric_name)
            if metric_def:
                agg_exprs.append(F.expr(metric_def.expr).alias(metric_name))

        if group_cols:
            result = df.groupBy(group_cols).agg(*agg_exprs)
        else:
            result = df.agg(*agg_exprs)

        return result

    def _execute_pandas(self, source_df: Any, parsed: ParsedQuery) -> Any:
        """Execute query using Pandas."""

        df = source_df.copy()

        all_filters = []
        for metric_name in parsed.metrics:
            metric_def = self._metric_cache.get(metric_name)
            if metric_def:
                all_filters.extend(metric_def.filters)
        all_filters.extend(parsed.filters)

        for filter_expr in all_filters:
            df = df.query(filter_expr)

        group_cols = []
        for dim_name in parsed.dimensions:
            dim_def = self._dimension_cache.get(dim_name)
            if dim_def:
                group_cols.append(dim_def.get_column())
            else:
                group_cols.append(dim_name)

        agg_dict = {}
        for metric_name in parsed.metrics:
            metric_def = self._metric_cache.get(metric_name)
            if metric_def:
                agg_dict[metric_name] = self._parse_pandas_agg(metric_def.expr)

        if group_cols:
            result = self._pandas_groupby_agg(df, group_cols, parsed.metrics)
        else:
            result = self._pandas_agg_all(df, parsed.metrics)

        return result

    def _parse_pandas_agg(self, expr: str) -> Tuple[str, str]:
        """
        Parse a SQL aggregation expression to (column, function).

        Example: "SUM(total_amount)" -> ("total_amount", "sum")
        """
        expr_stripped = expr.strip()

        match = re.match(r"(\w+)\(([^)]+)\)", expr_stripped, re.IGNORECASE)
        if match:
            func = match.group(1).upper()
            col = match.group(2).strip()

            func_map = {
                "SUM": "sum",
                "AVG": "mean",
                "COUNT": "count",
                "MIN": "min",
                "MAX": "max",
            }

            return (col, func_map.get(func, func.lower()))

        return (expr_stripped, "first")

    def _pandas_groupby_agg(self, df: Any, group_cols: List[str], metric_names: List[str]) -> Any:
        """Execute pandas groupby aggregation."""
        import pandas as pd

        agg_operations = {}

        for metric_name in metric_names:
            metric_def = self._metric_cache.get(metric_name)
            if metric_def:
                col, func = self._parse_pandas_agg(metric_def.expr)

                if col == "*":
                    first_col = df.columns[0]
                    if metric_name not in agg_operations:
                        agg_operations[metric_name] = (first_col, "count")
                else:
                    agg_operations[metric_name] = (col, func)

        if not agg_operations:
            return df.groupby(group_cols, as_index=False).size()

        grouped = df.groupby(group_cols, as_index=False)

        result_frames = []
        for metric_name, (col, func) in agg_operations.items():
            if func == "count" and col == df.columns[0]:
                agg_result = grouped.size().rename(columns={"size": metric_name})
            else:
                agg_result = grouped.agg(**{metric_name: (col, func)})
            result_frames.append(agg_result)

        if len(result_frames) == 1:
            return result_frames[0]

        result = result_frames[0]
        for frame in result_frames[1:]:
            new_cols = [c for c in frame.columns if c not in group_cols]
            result = pd.merge(result, frame[group_cols + new_cols], on=group_cols)

        return result

    def _pandas_agg_all(self, df: Any, metric_names: List[str]) -> Any:
        """Execute pandas aggregation without grouping."""
        import pandas as pd

        results = {}

        for metric_name in metric_names:
            metric_def = self._metric_cache.get(metric_name)
            if metric_def:
                col, func = self._parse_pandas_agg(metric_def.expr)

                if col == "*":
                    results[metric_name] = len(df)
                elif func == "sum":
                    results[metric_name] = df[col].sum()
                elif func == "mean":
                    results[metric_name] = df[col].mean()
                elif func == "count":
                    results[metric_name] = df[col].count()
                elif func == "min":
                    results[metric_name] = df[col].min()
                elif func == "max":
                    results[metric_name] = df[col].max()
                else:
                    results[metric_name] = df[col].agg(func)

        return pd.DataFrame([results])
