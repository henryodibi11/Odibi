"""
Semantic Layer Module
=====================

This module provides a semantic layer for defining and querying metrics.

Features:
- Define metrics in YAML (expressions, filters, source tables)
- Query interface: "revenue BY region, month"
- Materialize metrics on schedule
- Dimension hierarchies and drill-down

Core Components:
- MetricDefinition: Pydantic models for metric/dimension definitions
- SemanticQuery: Parse and execute "metric BY dimensions" queries
- Materialize: Execute and persist materialized aggregations

Example Config (in odibi.yaml):
    metrics:
      - name: revenue
        description: "Total revenue from completed orders"
        expr: "SUM(total_amount)"
        source: fact_orders
        filters:
          - "status = 'completed'"

    dimensions:
      - name: order_date
        source: dim_date
        hierarchy: [year, quarter, month, full_date]

    materializations:
      - name: monthly_revenue_by_region
        metrics: [revenue, order_count]
        dimensions: [region, month]
        schedule: "0 2 1 * *"
        output: gold/agg_monthly_revenue
"""

from odibi.semantics.materialize import Materializer
from odibi.semantics.metrics import (
    DimensionDefinition,
    MaterializationConfig,
    MetricDefinition,
    SemanticLayerConfig,
    ViewConfig,
    parse_semantic_config,
)
from odibi.semantics.query import SemanticQuery
from odibi.semantics.runner import SemanticLayerRunner, run_semantic_layer
from odibi.semantics.story import SemanticStoryGenerator, SemanticStoryMetadata
from odibi.semantics.views import ViewExecutionResult, ViewGenerator, ViewResult

__all__ = [
    "MetricDefinition",
    "DimensionDefinition",
    "MaterializationConfig",
    "SemanticLayerConfig",
    "ViewConfig",
    "parse_semantic_config",
    "SemanticQuery",
    "Materializer",
    "ViewGenerator",
    "ViewResult",
    "ViewExecutionResult",
    "SemanticStoryGenerator",
    "SemanticStoryMetadata",
    "SemanticLayerRunner",
    "run_semantic_layer",
]
__version__ = "1.1.0"
