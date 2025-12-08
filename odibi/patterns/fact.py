import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.patterns.base import Pattern
from odibi.utils.logging_context import get_logging_context


class FactPattern(Pattern):
    """
    Enhanced Fact Pattern: Builds fact tables with automatic SK lookups.

    Features:
    - Automatic surrogate key lookups from dimension tables
    - Orphan handling (unknown member, reject, or quarantine)
    - Grain validation (detect duplicates at PK level)
    - Audit columns (load_timestamp, source_system)
    - Deduplication support
    - Measure calculations and renaming

    Basic Params (backward compatible):
        deduplicate (bool): If true, removes duplicates before insert.
        keys (list): Keys for deduplication.

    Enhanced Params:
        grain (list): Columns that define uniqueness (validates no duplicates)
        dimensions (list): Dimension lookup configurations
            - source_column: Column in source data
            - dimension_table: Name of dimension in context
            - dimension_key: Natural key column in dimension
            - surrogate_key: Surrogate key to retrieve
            - scd2 (bool): If true, filter is_current=true
        orphan_handling (str): "unknown" | "reject" | "quarantine"
        measures (list): Measure definitions (passthrough, rename, or calculated)
        audit (dict): Audit column configuration
            - load_timestamp (bool)
            - source_system (str)

    Example Config:
        pattern:
          type: fact
          params:
            grain: [order_id]
            dimensions:
              - source_column: customer_id
                dimension_table: dim_customer
                dimension_key: customer_id
                surrogate_key: customer_sk
                scd2: true
            orphan_handling: unknown
            measures:
              - quantity
              - total_amount: "quantity * price"
            audit:
              load_timestamp: true
              source_system: "pos"
    """

    def validate(self) -> None:
        ctx = get_logging_context()
        deduplicate = self.params.get("deduplicate")
        keys = self.params.get("keys")
        grain = self.params.get("grain")
        dimensions = self.params.get("dimensions", [])
        orphan_handling = self.params.get("orphan_handling", "unknown")

        ctx.debug(
            "FactPattern validation starting",
            pattern="FactPattern",
            deduplicate=deduplicate,
            keys=keys,
            grain=grain,
            dimensions_count=len(dimensions),
        )

        if deduplicate and not keys:
            ctx.error(
                "FactPattern validation failed: 'keys' required when 'deduplicate' is True",
                pattern="FactPattern",
            )
            raise ValueError("FactPattern: 'keys' required when 'deduplicate' is True.")

        if orphan_handling not in ("unknown", "reject", "quarantine"):
            ctx.error(
                f"FactPattern validation failed: invalid orphan_handling '{orphan_handling}'",
                pattern="FactPattern",
            )
            raise ValueError(
                f"FactPattern: 'orphan_handling' must be 'unknown', 'reject', or 'quarantine'. "
                f"Got: {orphan_handling}"
            )

        for i, dim in enumerate(dimensions):
            required_keys = ["source_column", "dimension_table", "dimension_key", "surrogate_key"]
            for key in required_keys:
                if key not in dim:
                    ctx.error(
                        f"FactPattern validation failed: dimension[{i}] missing '{key}'",
                        pattern="FactPattern",
                    )
                    raise ValueError(f"FactPattern: dimension[{i}] missing required key '{key}'.")

        ctx.debug(
            "FactPattern validation passed",
            pattern="FactPattern",
        )

    def execute(self, context: EngineContext) -> Any:
        ctx = get_logging_context()
        start_time = time.time()

        deduplicate = self.params.get("deduplicate")
        keys = self.params.get("keys")
        grain = self.params.get("grain")
        dimensions = self.params.get("dimensions", [])
        orphan_handling = self.params.get("orphan_handling", "unknown")
        measures = self.params.get("measures", [])
        audit_config = self.params.get("audit", {})

        ctx.debug(
            "FactPattern starting",
            pattern="FactPattern",
            deduplicate=deduplicate,
            keys=keys,
            grain=grain,
            dimensions_count=len(dimensions),
            orphan_handling=orphan_handling,
        )

        df = context.df
        source_count = self._get_row_count(df, context.engine_type)
        ctx.debug("Fact source loaded", pattern="FactPattern", source_rows=source_count)

        try:
            if deduplicate and keys:
                df = self._deduplicate(context, df, keys)
                ctx.debug(
                    "Fact deduplication complete",
                    pattern="FactPattern",
                    rows_after=self._get_row_count(df, context.engine_type),
                )

            if dimensions:
                df, orphan_count = self._lookup_dimensions(context, df, dimensions, orphan_handling)
                ctx.debug(
                    "Fact dimension lookups complete",
                    pattern="FactPattern",
                    orphan_count=orphan_count,
                )

            if measures:
                df = self._apply_measures(context, df, measures)

            if grain:
                self._validate_grain(context, df, grain)

            df = self._add_audit_columns(context, df, audit_config)

            result_count = self._get_row_count(df, context.engine_type)
            elapsed_ms = (time.time() - start_time) * 1000

            ctx.info(
                "FactPattern completed",
                pattern="FactPattern",
                elapsed_ms=round(elapsed_ms, 2),
                source_rows=source_count,
                result_rows=result_count,
            )

            return df

        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            ctx.error(
                f"FactPattern failed: {e}",
                pattern="FactPattern",
                error_type=type(e).__name__,
                elapsed_ms=round(elapsed_ms, 2),
            )
            raise

    def _get_row_count(self, df, engine_type) -> Optional[int]:
        try:
            if engine_type == EngineType.SPARK:
                return df.count()
            else:
                return len(df)
        except Exception:
            return None

    def _deduplicate(self, context: EngineContext, df, keys: List[str]):
        """Remove duplicates based on keys."""
        if context.engine_type == EngineType.SPARK:
            return df.dropDuplicates(keys)
        else:
            return df.drop_duplicates(subset=keys)

    def _lookup_dimensions(
        self,
        context: EngineContext,
        df,
        dimensions: List[Dict],
        orphan_handling: str,
    ):
        """
        Perform surrogate key lookups from dimension tables.

        Returns:
            Tuple of (result_df, orphan_count)
        """
        total_orphans = 0

        for dim_config in dimensions:
            source_col = dim_config["source_column"]
            dim_table = dim_config["dimension_table"]
            dim_key = dim_config["dimension_key"]
            sk_col = dim_config["surrogate_key"]
            is_scd2 = dim_config.get("scd2", False)

            dim_df = self._get_dimension_df(context, dim_table, is_scd2)
            if dim_df is None:
                raise ValueError(
                    f"FactPattern: Dimension table '{dim_table}' not found in context."
                )

            df, orphan_count = self._join_dimension(
                context, df, dim_df, source_col, dim_key, sk_col, orphan_handling
            )
            total_orphans += orphan_count

        return df, total_orphans

    def _get_dimension_df(self, context: EngineContext, dim_table: str, is_scd2: bool):
        """Get dimension DataFrame from context, optionally filtering for current records."""
        try:
            dim_df = context.get(dim_table)
        except KeyError:
            return None

        if is_scd2:
            is_current_col = "is_current"
            if context.engine_type == EngineType.SPARK:
                from pyspark.sql import functions as F

                if is_current_col in dim_df.columns:
                    dim_df = dim_df.filter(F.col(is_current_col) == True)  # noqa: E712
            else:
                if is_current_col in dim_df.columns:
                    dim_df = dim_df[dim_df[is_current_col] == True].copy()  # noqa: E712

        return dim_df

    def _join_dimension(
        self,
        context: EngineContext,
        fact_df,
        dim_df,
        source_col: str,
        dim_key: str,
        sk_col: str,
        orphan_handling: str,
    ):
        """
        Join fact to dimension and retrieve surrogate key.

        Returns:
            Tuple of (result_df, orphan_count)
        """
        if context.engine_type == EngineType.SPARK:
            return self._join_dimension_spark(
                context, fact_df, dim_df, source_col, dim_key, sk_col, orphan_handling
            )
        else:
            return self._join_dimension_pandas(
                fact_df, dim_df, source_col, dim_key, sk_col, orphan_handling
            )

    def _join_dimension_spark(
        self,
        context: EngineContext,
        fact_df,
        dim_df,
        source_col: str,
        dim_key: str,
        sk_col: str,
        orphan_handling: str,
    ):
        from pyspark.sql import functions as F

        dim_subset = dim_df.select(
            F.col(dim_key).alias(f"_dim_{dim_key}"),
            F.col(sk_col).alias(sk_col),
        )

        joined = fact_df.join(
            dim_subset,
            fact_df[source_col] == dim_subset[f"_dim_{dim_key}"],
            "left",
        )

        orphan_count = joined.filter(F.col(sk_col).isNull()).count()

        if orphan_handling == "reject" and orphan_count > 0:
            raise ValueError(
                f"FactPattern: {orphan_count} orphan records found for dimension "
                f"lookup on '{source_col}'. Orphan handling is set to 'reject'."
            )

        if orphan_handling == "unknown":
            joined = joined.withColumn(sk_col, F.coalesce(F.col(sk_col), F.lit(0)))

        result = joined.drop(f"_dim_{dim_key}")

        return result, orphan_count

    def _join_dimension_pandas(
        self,
        fact_df,
        dim_df,
        source_col: str,
        dim_key: str,
        sk_col: str,
        orphan_handling: str,
    ):
        import pandas as pd

        dim_subset = dim_df[[dim_key, sk_col]].copy()
        dim_subset = dim_subset.rename(columns={dim_key: f"_dim_{dim_key}"})

        merged = pd.merge(
            fact_df,
            dim_subset,
            left_on=source_col,
            right_on=f"_dim_{dim_key}",
            how="left",
        )

        orphan_count = merged[sk_col].isna().sum()

        if orphan_handling == "reject" and orphan_count > 0:
            raise ValueError(
                f"FactPattern: {orphan_count} orphan records found for dimension "
                f"lookup on '{source_col}'. Orphan handling is set to 'reject'."
            )

        if orphan_handling == "unknown":
            merged[sk_col] = merged[sk_col].fillna(0).astype(int)

        result = merged.drop(columns=[f"_dim_{dim_key}"])

        return result, int(orphan_count)

    def _apply_measures(self, context: EngineContext, df, measures: List):
        """
        Apply measure transformations.

        Measures can be:
        - String: passthrough column name
        - Dict with single key-value: rename or calculate
          - {"new_name": "old_name"} -> rename
          - {"new_name": "expr"} -> calculate (if expr contains operators)
        """
        for measure in measures:
            if isinstance(measure, str):
                continue
            elif isinstance(measure, dict):
                for new_name, expr in measure.items():
                    if self._is_expression(expr):
                        df = self._add_calculated_measure(context, df, new_name, expr)
                    else:
                        df = self._rename_column(context, df, expr, new_name)

        return df

    def _is_expression(self, expr: str) -> bool:
        """Check if string is a calculation expression."""
        operators = ["+", "-", "*", "/", "(", ")"]
        return any(op in expr for op in operators)

    def _add_calculated_measure(self, context: EngineContext, df, name: str, expr: str):
        """Add a calculated measure column."""
        if context.engine_type == EngineType.SPARK:
            from pyspark.sql import functions as F

            return df.withColumn(name, F.expr(expr))
        else:
            df = df.copy()
            df[name] = df.eval(expr)
            return df

    def _rename_column(self, context: EngineContext, df, old_name: str, new_name: str):
        """Rename a column."""
        if context.engine_type == EngineType.SPARK:
            return df.withColumnRenamed(old_name, new_name)
        else:
            return df.rename(columns={old_name: new_name})

    def _validate_grain(self, context: EngineContext, df, grain: List[str]):
        """
        Validate that no duplicate rows exist at the grain level.

        Raises ValueError if duplicates are found.
        """
        ctx = get_logging_context()

        if context.engine_type == EngineType.SPARK:
            total_count = df.count()
            distinct_count = df.select(grain).distinct().count()
        else:
            total_count = len(df)
            distinct_count = len(df.drop_duplicates(subset=grain))

        if total_count != distinct_count:
            duplicate_count = total_count - distinct_count
            ctx.error(
                f"FactPattern grain validation failed: {duplicate_count} duplicate rows",
                pattern="FactPattern",
                grain=grain,
                total_rows=total_count,
                distinct_rows=distinct_count,
            )
            raise ValueError(
                f"FactPattern: Grain validation failed. Found {duplicate_count} duplicate "
                f"rows at grain level {grain}. Total rows: {total_count}, "
                f"Distinct rows: {distinct_count}."
            )

        ctx.debug(
            "FactPattern grain validation passed",
            pattern="FactPattern",
            grain=grain,
            total_rows=total_count,
        )

    def _add_audit_columns(self, context: EngineContext, df, audit_config: Dict):
        """Add audit columns (load_timestamp, source_system)."""
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
                df["load_timestamp"] = datetime.now()
            if source_system:
                df["source_system"] = source_system

        return df
