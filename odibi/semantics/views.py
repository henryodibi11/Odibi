"""
Semantic View Generation Module
===============================

Generate and execute SQL Server views from semantic layer configurations.

Views provide pre-computed aggregations at specific grains, with:
- Derived metrics calculated correctly (SUM first, then formula)
- Time grain transformations (DATETRUNC)
- NULLIF protection for division by zero
- Self-documenting SQL with metric descriptions
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from odibi.semantics.metrics import (
    DimensionDefinition,
    MetricDefinition,
    MetricType,
    SemanticLayerConfig,
    TimeGrain,
    ViewConfig,
    ViewResult,
)
from odibi.utils.logging_context import get_logging_context


def generate_ensure_schema_sql(schema: str) -> str:
    """
    Generate SQL to create schema if it doesn't exist.

    Uses SQL Server's conditional execution pattern since
    CREATE SCHEMA must be the first statement in a batch.
    """
    return f"""\
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}')
BEGIN
    EXEC('CREATE SCHEMA [{schema}]')
END"""


@dataclass
class ViewExecutionResult:
    """Result of executing multiple views."""

    views_created: List[str] = field(default_factory=list)
    sql_files_saved: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    results: List[ViewResult] = field(default_factory=list)


class ViewGenerator:
    """
    Generate SQL Server views from semantic layer configuration.

    Usage:
        config = SemanticLayerConfig(...)
        generator = ViewGenerator(config)
        ddl = generator.generate_view_ddl(view_config)
    """

    GRAIN_SQL_MAP = {
        TimeGrain.DAY: "DATETRUNC(day, {col})",
        TimeGrain.WEEK: "DATETRUNC(week, {col})",
        TimeGrain.MONTH: "DATETRUNC(month, {col})",
        TimeGrain.QUARTER: "DATETRUNC(quarter, {col})",
        TimeGrain.YEAR: "DATETRUNC(year, {col})",
    }

    def __init__(self, config: SemanticLayerConfig):
        """
        Initialize with semantic layer configuration.

        Args:
            config: SemanticLayerConfig with metrics, dimensions, and views
        """
        self.config = config
        self._metric_cache: Dict[str, MetricDefinition] = {}
        self._dimension_cache: Dict[str, DimensionDefinition] = {}

        for metric in config.metrics:
            self._metric_cache[metric.name] = metric

        for dim in config.dimensions:
            self._dimension_cache[dim.name] = dim

    def generate_view_ddl(self, view_config: ViewConfig) -> str:
        """
        Generate CREATE OR ALTER VIEW DDL statement.

        Args:
            view_config: ViewConfig with metrics, dimensions, and view name

        Returns:
            Complete SQL DDL string with documentation header
        """
        ctx = get_logging_context()
        ctx.debug("Generating view DDL", view=view_config.name)

        header = self._generate_header(view_config)
        body = self._generate_view_body(view_config)

        full_name = f"{view_config.db_schema}.{view_config.name}"
        ddl = f"{header}\nCREATE OR ALTER VIEW {full_name} AS\n{body};"

        ctx.info("Generated view DDL", view=view_config.name, lines=ddl.count("\n"))
        return ddl

    def _generate_header(self, view_config: ViewConfig) -> str:
        """Generate SQL documentation header."""
        lines = [
            "-- " + "=" * 77,
            f"-- View: {view_config.db_schema}.{view_config.name}",
        ]

        if view_config.description:
            lines.append(f"-- Description: {view_config.description}")

        lines.append(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        if view_config.source_file:
            lines.append(f"-- Source: {view_config.source_file}")

        lines.append("-- " + "=" * 77)
        lines.append("--")
        lines.append("-- Metrics included:")

        for metric_name in view_config.metrics:
            metric_def = self._metric_cache.get(metric_name.lower())
            if metric_def:
                if metric_def.type == MetricType.DERIVED:
                    lines.append(f"--   - {metric_name}: {metric_def.description or 'Derived'}")
                    lines.append(f"--     Formula: {metric_def.formula}")
                else:
                    lines.append(f"--   - {metric_name}: {metric_def.expr}")

        lines.append("--")
        lines.append("-- " + "=" * 77)

        return "\n".join(lines)

    def _generate_view_body(self, view_config: ViewConfig) -> str:
        """Generate the SELECT statement body."""
        source_table = self._get_source_table(view_config)

        select_parts = []
        group_by_parts = []

        for dim_name in view_config.dimensions:
            dim_def = self._dimension_cache.get(dim_name.lower())
            dim_sql, dim_alias = self._get_dimension_sql(dim_name, dim_def)
            select_parts.append(f"    {dim_sql} AS {dim_alias}")
            group_by_parts.append(dim_sql)

        component_metrics = set()
        derived_metrics = []
        simple_metrics = []

        for metric_name in view_config.metrics:
            metric_def = self._metric_cache.get(metric_name.lower())
            if metric_def:
                if metric_def.type == MetricType.DERIVED:
                    derived_metrics.append(metric_def)
                    if metric_def.components:
                        for comp in metric_def.components:
                            component_metrics.add(comp.lower())
                else:
                    simple_metrics.append(metric_def)

        for comp_name in sorted(component_metrics):
            comp_def = self._metric_cache.get(comp_name)
            if comp_def and comp_def.expr:
                select_parts.append(f"    {comp_def.expr} AS {comp_name}")

        for metric_def in simple_metrics:
            if metric_def.name not in component_metrics and metric_def.expr:
                select_parts.append(f"    {metric_def.expr} AS {metric_def.name}")

        for metric_def in derived_metrics:
            formula_sql = self._build_derived_formula_sql(metric_def)
            select_parts.append(f"    {formula_sql} AS {metric_def.name}")

        select_clause = ",\n".join(select_parts)
        group_by_clause = ", ".join(group_by_parts)

        body = f"SELECT\n{select_clause}\nFROM {source_table}\nGROUP BY {group_by_clause}"
        return body

    def _get_source_table(self, view_config: ViewConfig) -> str:
        """Determine the source table from metrics."""
        for metric_name in view_config.metrics:
            metric_def = self._metric_cache.get(metric_name.lower())
            if metric_def and metric_def.source:
                return metric_def.source
            if metric_def and metric_def.components:
                for comp_name in metric_def.components:
                    comp_def = self._metric_cache.get(comp_name.lower())
                    if comp_def and comp_def.source:
                        return comp_def.source
        raise ValueError(f"No source table found for view '{view_config.name}'")

    def _get_dimension_sql(self, dim_name: str, dim_def: Optional[DimensionDefinition]) -> tuple:
        """Get SQL expression and alias for a dimension."""
        if dim_def is None:
            return dim_name, dim_name

        col = dim_def.get_column()

        if dim_def.grain:
            sql_template = self.GRAIN_SQL_MAP.get(dim_def.grain)
            if sql_template:
                return sql_template.format(col=col), dim_name
            return col, dim_name

        return col, dim_name

    def _build_derived_formula_sql(self, metric_def: MetricDefinition) -> str:
        """Build SQL for a derived metric with NULLIF protection."""
        if not metric_def.formula or not metric_def.components:
            raise ValueError(f"Derived metric '{metric_def.name}' missing formula or components")

        formula = metric_def.formula

        component_exprs = {}
        for comp_name in metric_def.components:
            comp_metric = self._metric_cache.get(comp_name.lower())
            if comp_metric and comp_metric.expr:
                component_exprs[comp_name.lower()] = comp_metric.expr

        sorted_names = sorted(component_exprs.keys(), key=len, reverse=True)
        result = formula
        for name in sorted_names:
            result = result.replace(name, component_exprs[name])

        result = self._wrap_divisors_with_nullif(result)

        return result

    def _wrap_divisors_with_nullif(self, expr: str) -> str:
        """Wrap division operands with NULLIF to prevent division by zero."""
        import re

        def find_balanced_paren(s: str, start: int) -> int:
            """Find the closing paren index for a balanced parenthesized expression."""
            if start >= len(s) or s[start] != "(":
                return -1
            depth = 1
            i = start + 1
            while i < len(s) and depth > 0:
                if s[i] == "(":
                    depth += 1
                elif s[i] == ")":
                    depth -= 1
                i += 1
            return i if depth == 0 else -1

        result = []
        i = 0
        while i < len(expr):
            if expr[i] == "/":
                result.append("/")
                i += 1
                while i < len(expr) and expr[i] in " \t":
                    result.append(expr[i])
                    i += 1
                if i >= len(expr):
                    break

                if expr[i] == "(":
                    end = find_balanced_paren(expr, i)
                    if end > 0:
                        divisor = expr[i:end]
                        result.append(f"NULLIF({divisor}, 0)")
                        i = end
                    else:
                        result.append(expr[i])
                        i += 1
                else:
                    func_match = re.match(
                        r"(SUM|COUNT|AVG|MIN|MAX)\s*\([^)]+\)",
                        expr[i:],
                        re.IGNORECASE,
                    )
                    if func_match:
                        divisor = func_match.group(0)
                        result.append(f"NULLIF({divisor}, 0)")
                        i += len(divisor)
                    else:
                        ident_match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", expr[i:])
                        if ident_match:
                            divisor = ident_match.group(0)
                            result.append(f"NULLIF({divisor}, 0)")
                            i += len(divisor)
                        else:
                            result.append(expr[i])
                            i += 1
            else:
                result.append(expr[i])
                i += 1

        return "".join(result)

    def execute_view(
        self,
        view_config: ViewConfig,
        execute_sql: Callable[[str], None],
        save_sql_to: Optional[str] = None,
        write_file: Optional[Callable[[str, str], None]] = None,
    ) -> ViewResult:
        """
        Generate and execute a view.

        Args:
            view_config: View configuration
            execute_sql: Callable that executes SQL against the database
            save_sql_to: Optional path to save the SQL file
            write_file: Optional callable to write file (path, content)

        Returns:
            ViewResult with success status and details
        """
        ctx = get_logging_context()
        ctx.info("Executing view", view=view_config.name)

        try:
            if view_config.ensure_schema:
                schema_sql = generate_ensure_schema_sql(view_config.db_schema)
                ctx.debug("Ensuring schema exists", schema=view_config.db_schema)
                execute_sql(schema_sql)

            ddl = self.generate_view_ddl(view_config)

            execute_sql(ddl)

            sql_file_path = None
            if save_sql_to and write_file:
                filename = f"{view_config.name}.sql"
                sql_file_path = f"{save_sql_to.rstrip('/')}/{filename}"
                write_file(sql_file_path, ddl)
                ctx.info("Saved SQL file", path=sql_file_path)

            ctx.info("View created successfully", view=view_config.name)
            return ViewResult(
                name=view_config.name,
                success=True,
                sql=ddl,
                sql_file_path=sql_file_path,
            )

        except Exception as e:
            ctx.error("View creation failed", view=view_config.name, error=str(e))
            return ViewResult(
                name=view_config.name,
                success=False,
                sql="",
                error=str(e),
            )

    def execute_all_views(
        self,
        execute_sql: Callable[[str], None],
        save_sql_to: Optional[str] = None,
        write_file: Optional[Callable[[str, str], None]] = None,
    ) -> ViewExecutionResult:
        """
        Execute all views defined in the configuration.

        Args:
            execute_sql: Callable that executes SQL against the database
            save_sql_to: Optional path to save SQL files
            write_file: Optional callable to write files

        Returns:
            ViewExecutionResult with summary of all operations
        """
        ctx = get_logging_context()
        ctx.info("Executing all views", count=len(self.config.views))

        result = ViewExecutionResult()

        for view_config in self.config.views:
            view_result = self.execute_view(
                view_config,
                execute_sql,
                save_sql_to,
                write_file,
            )

            result.results.append(view_result)

            if view_result.success:
                result.views_created.append(view_result.name)
                if view_result.sql_file_path:
                    result.sql_files_saved.append(view_result.sql_file_path)
            else:
                result.errors.append(f"{view_result.name}: {view_result.error}")

        ctx.info(
            "View execution complete",
            created=len(result.views_created),
            errors=len(result.errors),
        )

        return result

    def get_view(self, name: str) -> Optional[ViewConfig]:
        """Get a view configuration by name."""
        name_lower = name.lower()
        for view in self.config.views:
            if view.name.lower() == name_lower:
                return view
        return None

    def list_views(self) -> List[Dict[str, Any]]:
        """List all configured views with their details."""
        return [
            {
                "name": v.name,
                "description": v.description,
                "metrics": v.metrics,
                "dimensions": v.dimensions,
                "db_schema": v.db_schema,
            }
            for v in self.config.views
        ]
