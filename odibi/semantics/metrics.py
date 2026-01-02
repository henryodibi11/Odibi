"""
Metric Definition Models
========================

Pydantic models for semantic layer configuration including:
- Metric definitions (expressions, filters, derived metrics)
- Dimension definitions with hierarchies
- Materialization configurations
"""

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class MetricType(str, Enum):
    """Type of metric calculation."""

    SIMPLE = "simple"
    DERIVED = "derived"


class TimeGrain(str, Enum):
    """Time grain for dimension transformations."""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class MetricDefinition(BaseModel):
    """
    Definition of a semantic metric.

    A metric represents a measurable value that can be aggregated
    across dimensions (e.g., revenue, order_count, avg_order_value).

    Attributes:
        name: Unique metric identifier
        description: Human-readable description
        expr: SQL aggregation expression (e.g., "SUM(total_amount)").
            Optional for derived metrics.
        source: Source table reference. Supports three formats:
            - `$pipeline.node` (recommended): e.g., `$build_warehouse.fact_orders`
            - `connection.path`: e.g., `gold.fact_orders` or `gold.oee/plant_a/metrics`
            - `table_name`: Uses default connection
        filters: Optional WHERE conditions to apply
        type: "simple" (direct aggregation) or "derived" (references other metrics)
        components: List of component metric names (required for derived metrics).
            These metrics must be additive (e.g., SUM-based) for correct
            recalculation at different grains.
        formula: Calculation formula using component names (required for derived).
            Example: "(total_revenue - total_cost) / total_revenue"
    """

    name: str = Field(..., description="Unique metric identifier")
    description: Optional[str] = Field(None, description="Human-readable description")
    expr: Optional[str] = Field(None, description="SQL aggregation expression")
    source: Optional[str] = Field(
        None,
        description=(
            "Source table reference. Formats: "
            "$pipeline.node (e.g., $build_warehouse.fact_orders), "
            "connection.path (e.g., gold.fact_orders or gold.oee/plant_a/table), "
            "or bare table_name"
        ),
    )
    filters: List[str] = Field(default_factory=list, description="WHERE conditions")
    type: MetricType = Field(default=MetricType.SIMPLE, description="Metric type")
    components: Optional[List[str]] = Field(
        None, description="Component metric names for derived metrics"
    )
    formula: Optional[str] = Field(None, description="Calculation formula using component names")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Metric name cannot be empty")
        if not v.replace("_", "").isalnum():
            raise ValueError(
                f"Metric name '{v}' must contain only alphanumeric characters and underscores"
            )
        return v.strip().lower()

    @field_validator("expr")
    @classmethod
    def validate_expr(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError("Metric expression cannot be empty if provided")
        return v.strip() if v else None

    def model_post_init(self, __context) -> None:
        """Validate derived metric requirements after model initialization."""
        if self.type == MetricType.DERIVED:
            if not self.components:
                raise ValueError(f"Derived metric '{self.name}' requires 'components' list")
            if not self.formula:
                raise ValueError(f"Derived metric '{self.name}' requires 'formula'")
        elif self.type == MetricType.SIMPLE:
            if not self.expr:
                raise ValueError(f"Simple metric '{self.name}' requires 'expr'")


class DimensionDefinition(BaseModel):
    """
    Definition of a semantic dimension.

    A dimension represents an attribute for grouping and filtering
    metrics (e.g., date, product, region).

    Attributes:
        name: Unique dimension identifier
        source: Source table reference. Supports three formats:
            - `$pipeline.node` (recommended): e.g., `$build_warehouse.dim_customer`
            - `connection.path`: e.g., `gold.dim_customer` or `gold.dims/customer`
            - `table_name`: Uses default connection
        column: Column name in source (defaults to name)
        expr: Custom SQL expression. If provided, overrides column and grain.
            Example: "YEAR(DATEADD(month, 6, Date))" for fiscal year
        hierarchy: Optional ordered list of columns for drill-down
        description: Human-readable description
        grain: Time grain transformation (day, week, month, quarter, year).
            Ignored if expr is provided.
    """

    name: str = Field(..., description="Unique dimension identifier")
    source: Optional[str] = Field(
        None,
        description=(
            "Source table reference. Formats: "
            "$pipeline.node (e.g., $build_warehouse.dim_customer), "
            "connection.path (e.g., gold.dim_customer or gold.dims/customer), "
            "or bare table_name"
        ),
    )
    column: Optional[str] = Field(None, description="Column name (defaults to name)")
    expr: Optional[str] = Field(
        None,
        description=(
            "Custom SQL expression. Overrides column and grain. "
            "Example: YEAR(DATEADD(month, 6, Date)) for fiscal year"
        ),
    )
    hierarchy: List[str] = Field(default_factory=list, description="Drill-down hierarchy")
    description: Optional[str] = Field(None, description="Human-readable description")
    grain: Optional[TimeGrain] = Field(None, description="Time grain transformation")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Dimension name cannot be empty")
        return v.strip().lower()

    def get_column(self) -> str:
        """Get the actual column name to use."""
        return self.column if self.column else self.name


class MaterializationConfig(BaseModel):
    """
    Configuration for materializing metrics to a table.

    Materialization pre-computes aggregated metrics at a specific
    grain and persists them for faster querying.

    Attributes:
        name: Unique materialization identifier
        metrics: List of metric names to include
        dimensions: List of dimension names (determines grain)
        output: Output table path
        schedule: Optional cron schedule for refresh
        incremental: Configuration for incremental refresh
    """

    name: str = Field(..., description="Unique materialization identifier")
    metrics: List[str] = Field(..., description="Metrics to materialize")
    dimensions: List[str] = Field(..., description="Dimensions for grouping")
    output: str = Field(..., description="Output table path")
    schedule: Optional[str] = Field(None, description="Cron schedule")
    incremental: Optional[Dict[str, Any]] = Field(None, description="Incremental refresh config")

    @field_validator("metrics")
    @classmethod
    def validate_metrics(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("At least one metric is required")
        return v

    @field_validator("dimensions")
    @classmethod
    def validate_dimensions(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("At least one dimension is required")
        return v


class ViewConfig(BaseModel):
    """
    Configuration for a semantic view.

    A view represents a pre-defined aggregation of metrics at a specific
    grain, materialized as a SQL Server view for analyst consumption.

    Attributes:
        name: View name (will be created as db_schema.name in SQL Server)
        description: Human-readable description of the view's purpose
        metrics: List of metric names to include
        dimensions: List of dimension names (determines grain)
        db_schema: Database schema for the view (default: semantic)
        ensure_schema: Auto-create schema if it doesn't exist (default: True)
        source_file: Optional reference to source config file for documentation
    """

    name: str = Field(..., description="View name")
    description: Optional[str] = Field(None, description="View description")
    metrics: List[str] = Field(..., description="Metrics to include")
    dimensions: List[str] = Field(..., description="Dimensions for grouping")
    db_schema: str = Field(default="semantic", description="Database schema")
    ensure_schema: bool = Field(default=True, description="Auto-create schema if it doesn't exist")
    source_file: Optional[str] = Field(None, description="Source config file reference")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("View name cannot be empty")
        return v.strip()

    @field_validator("metrics")
    @classmethod
    def validate_metrics(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("At least one metric is required")
        return v

    @field_validator("dimensions")
    @classmethod
    def validate_dimensions_list(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("At least one dimension is required")
        return v


class SemanticLayerConfig(BaseModel):
    """
    Complete semantic layer configuration.

    Contains all metrics, dimensions, materializations, and views
    for a semantic layer deployment.

    Attributes:
        metrics: List of metric definitions
        dimensions: List of dimension definitions
        materializations: List of materialization configurations
        views: List of view configurations
    """

    metrics: List[MetricDefinition] = Field(default_factory=list, description="Metric definitions")
    dimensions: List[DimensionDefinition] = Field(
        default_factory=list, description="Dimension definitions"
    )
    materializations: List[MaterializationConfig] = Field(
        default_factory=list, description="Materialization configs"
    )
    views: List[ViewConfig] = Field(default_factory=list, description="View configurations")

    def get_metric(self, name: str) -> Optional[MetricDefinition]:
        """Get a metric by name."""
        name_lower = name.lower()
        for metric in self.metrics:
            if metric.name == name_lower:
                return metric
        return None

    def get_dimension(self, name: str) -> Optional[DimensionDefinition]:
        """Get a dimension by name."""
        name_lower = name.lower()
        for dim in self.dimensions:
            if dim.name == name_lower:
                return dim
        return None

    def get_materialization(self, name: str) -> Optional[MaterializationConfig]:
        """Get a materialization config by name."""
        name_lower = name.lower()
        for mat in self.materializations:
            if mat.name.lower() == name_lower:
                return mat
        return None

    def validate_references(self) -> List[str]:
        """
        Validate that all references are valid.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        metric_names = {m.name for m in self.metrics}
        dimension_names = {d.name for d in self.dimensions}

        for metric in self.metrics:
            if metric.components:
                for component_name in metric.components:
                    if component_name.lower() not in metric_names:
                        errors.append(
                            f"Derived metric '{metric.name}' references "
                            f"unknown component '{component_name}'"
                        )

        for mat in self.materializations:
            for metric_name in mat.metrics:
                if metric_name.lower() not in metric_names:
                    errors.append(
                        f"Materialization '{mat.name}' references unknown metric '{metric_name}'"
                    )

            for dim_name in mat.dimensions:
                if dim_name.lower() not in dimension_names:
                    errors.append(
                        f"Materialization '{mat.name}' references unknown dimension '{dim_name}'"
                    )

        return errors


class ViewResult(BaseModel):
    """
    Result of view generation/execution.

    Attributes:
        name: View name
        success: Whether the operation succeeded
        sql: Generated SQL DDL
        error: Error message if failed
        sql_file_path: Path where SQL was saved (if save requested)
    """

    name: str = Field(..., description="View name")
    success: bool = Field(..., description="Whether operation succeeded")
    sql: str = Field(..., description="Generated SQL DDL")
    error: Optional[str] = Field(None, description="Error message if failed")
    sql_file_path: Optional[str] = Field(None, description="Path where SQL was saved")


def parse_semantic_config(config_dict: Dict[str, Any]) -> SemanticLayerConfig:
    """
    Parse a semantic layer configuration from a dictionary.

    Args:
        config_dict: Configuration dictionary (from YAML)

    Returns:
        SemanticLayerConfig instance
    """
    return SemanticLayerConfig(**config_dict)
