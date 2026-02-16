import time
from typing import Any, Dict, List, Optional

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.patterns.base import Pattern
from odibi.utils.logging_context import get_logging_context


class AggregationPattern(Pattern):
    """
    Aggregation Pattern: Declarative aggregation with time-grain rollups.

    Features:
    - Declare grain (GROUP BY columns)
    - Declare measures with aggregation functions
    - Incremental aggregation (merge new data with existing)
    - Time rollups (generate multiple grain levels)
    - Audit columns

    Configuration Options (via params dict):
        - **grain** (list): Columns to GROUP BY (defines uniqueness)
        - **measures** (list): Measure definitions with name and aggregation expr
            - name: Output column name
            - expr: SQL aggregation expression (e.g., "SUM(amount)")
        - **incremental** (dict): Incremental merge configuration (optional)
            - timestamp_column: Column to identify new data
            - merge_strategy: "replace", "sum", "min", or "max"
        - **having** (str): Optional HAVING clause for filtering aggregates
        - **audit** (dict): Audit column configuration

    Example Config:
        pattern:
          type: aggregation
          params:
            grain: [date_sk, product_sk]
            measures:
              - name: total_revenue
                expr: "SUM(total_amount)"
              - name: order_count
                expr: "COUNT(*)"
              - name: avg_order_value
                expr: "AVG(total_amount)"
            having: "COUNT(*) > 0"
            audit:
              load_timestamp: true
    """

    def validate(self) -> None:
        """Validate aggregation pattern configuration parameters.

        Ensures that all required parameters are present and valid. Checks that:
        - grain is specified (list of GROUP BY columns)
        - measures are provided with correct structure (list of dicts with 'name' and 'expr')
        - incremental config is valid if provided (requires 'timestamp_column' and valid merge_strategy)

        Raises:
            ValueError: If grain is missing, measures are missing/invalid, or incremental config is invalid.
        """
        ctx = get_logging_context()
        grain = self.params.get("grain")
        measures = self.params.get("measures", [])

        ctx.debug(
            "AggregationPattern validation starting",
            pattern="AggregationPattern",
            grain=grain,
            measures_count=len(measures),
        )

        if not grain:
            ctx.error(
                "AggregationPattern validation failed: 'grain' is required",
                pattern="AggregationPattern",
                node=self.config.name,
            )
            raise ValueError(
                f"AggregationPattern (node '{self.config.name}'): 'grain' parameter is required. "
                "Grain defines the grouping columns for aggregation (e.g., ['date', 'region']). "
                "Provide a list of column names to group by."
            )

        if not measures:
            ctx.error(
                "AggregationPattern validation failed: 'measures' is required",
                pattern="AggregationPattern",
                node=self.config.name,
            )
            raise ValueError(
                f"AggregationPattern (node '{self.config.name}'): 'measures' parameter is required. "
                "Measures define the aggregations to compute (e.g., [{'name': 'total_sales', 'expr': 'sum(amount)'}]). "
                "Provide a list of dicts, each with 'name' and 'expr' keys."
            )

        for i, measure in enumerate(measures):
            if not isinstance(measure, dict):
                ctx.error(
                    f"AggregationPattern validation failed: measure[{i}] must be a dict",
                    pattern="AggregationPattern",
                    node=self.config.name,
                )
                raise ValueError(
                    f"AggregationPattern (node '{self.config.name}'): measure[{i}] must be a dict with 'name' and 'expr'. "
                    f"Got {type(measure).__name__}: {measure!r}. "
                    "Example: {'name': 'total_sales', 'expr': 'sum(amount)'}"
                )
            if "name" not in measure:
                ctx.error(
                    f"AggregationPattern validation failed: measure[{i}] missing 'name'",
                    pattern="AggregationPattern",
                    node=self.config.name,
                )
                raise ValueError(
                    f"AggregationPattern (node '{self.config.name}'): measure[{i}] missing 'name'. "
                    f"Got: {measure!r}. Add a 'name' key for the output column name."
                )
            if "expr" not in measure:
                ctx.error(
                    f"AggregationPattern validation failed: measure[{i}] missing 'expr'",
                    pattern="AggregationPattern",
                    node=self.config.name,
                )
                raise ValueError(
                    f"AggregationPattern (node '{self.config.name}'): measure[{i}] missing 'expr'. "
                    f"Got: {measure!r}. Add an 'expr' key with the aggregation expression (e.g., 'sum(amount)')."
                )

        incremental = self.params.get("incremental")
        if incremental:
            if "timestamp_column" not in incremental:
                ctx.error(
                    "AggregationPattern validation failed: incremental missing 'timestamp_column'",
                    pattern="AggregationPattern",
                    node=self.config.name,
                )
                raise ValueError(
                    f"AggregationPattern (node '{self.config.name}'): incremental config requires 'timestamp_column'. "
                    f"Got: {incremental!r}. "
                    "Add 'timestamp_column' to specify which column tracks record timestamps."
                )
            merge_strategy = incremental.get("merge_strategy", "replace")
            if merge_strategy not in ("replace", "sum", "min", "max"):
                ctx.error(
                    f"AggregationPattern validation failed: invalid merge_strategy '{merge_strategy}'",
                    pattern="AggregationPattern",
                    node=self.config.name,
                )
                raise ValueError(
                    f"AggregationPattern (node '{self.config.name}'): 'merge_strategy' must be 'replace', 'sum', 'min', or 'max'. "
                    f"Got: {merge_strategy}"
                )

        ctx.debug(
            "AggregationPattern validation passed",
            pattern="AggregationPattern",
        )

    def execute(self, context: EngineContext) -> Any:
        """Execute the aggregation pattern on the input data.

        Performs aggregation operations on the source DataFrame, optionally applying incremental
        merge with existing target data. The execution flow:
        1. Aggregate source data by grain columns with specified measures
        2. Apply HAVING clause filtering if configured
        3. Merge with existing target data if incremental mode is enabled
        4. Add audit columns (load_timestamp, source_system) if configured

        Args:
            context: Engine context containing the source DataFrame and execution environment.

        Returns:
            Aggregated DataFrame with measures computed at the specified grain level.

        Raises:
            Exception: If aggregation fails, incremental merge fails, or target loading fails.
        """
        ctx = get_logging_context()
        start_time = time.time()

        grain = self.params.get("grain")
        measures = self.params.get("measures", [])
        having = self.params.get("having")
        incremental = self.params.get("incremental")
        audit_config = self.params.get("audit", {})
        target = self.params.get("target")

        ctx.debug(
            "AggregationPattern starting",
            pattern="AggregationPattern",
            grain=grain,
            measures_count=len(measures),
            incremental=incremental is not None,
        )

        df = context.df
        source_count = self._get_row_count(df, context.engine_type)
        ctx.debug(
            "Aggregation source loaded",
            pattern="AggregationPattern",
            source_rows=source_count,
        )

        try:
            result_df = self._aggregate(context, df, grain, measures, having)

            if incremental and target:
                result_df = self._apply_incremental(
                    context, result_df, grain, measures, incremental, target
                )

            result_df = self._add_audit_columns(context, result_df, audit_config)

            result_count = self._get_row_count(result_df, context.engine_type)
            elapsed_ms = (time.time() - start_time) * 1000

            ctx.info(
                "AggregationPattern completed",
                pattern="AggregationPattern",
                elapsed_ms=round(elapsed_ms, 2),
                source_rows=source_count,
                result_rows=result_count,
                grain=grain,
            )

            return result_df

        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            ctx.error(
                f"AggregationPattern failed: {e}",
                pattern="AggregationPattern",
                node=self.config.name,
                error_type=type(e).__name__,
                elapsed_ms=round(elapsed_ms, 2),
            )
            raise

    def _aggregate(
        self,
        context: EngineContext,
        df,
        grain: List[str],
        measures: List[Dict],
        having: Optional[str],
    ):
        """Perform the aggregation using SQL.

        Args:
            context: Engine context containing engine type and configuration.
            df: The source DataFrame to aggregate.
            grain: List of column names to group by.
            measures: List of measure definitions with aggregation functions.
            having: Optional SQL HAVING clause filter for post-aggregation filtering.

        Returns:
            Aggregated DataFrame with grain columns and calculated measures.
        """
        if context.engine_type == EngineType.SPARK:
            return self._aggregate_spark(context, df, grain, measures, having)
        else:
            return self._aggregate_pandas(context, df, grain, measures, having)

    def _aggregate_spark(
        self,
        context: EngineContext,
        df,
        grain: List[str],
        measures: List[Dict],
        having: Optional[str],
    ):
        """Aggregate using Spark SQL."""
        from pyspark.sql import functions as F

        grain_cols = [F.col(c) for c in grain]

        agg_exprs = []
        for measure in measures:
            name = measure["name"]
            expr = measure["expr"]
            agg_exprs.append(F.expr(expr).alias(name))

        result = df.groupBy(*grain_cols).agg(*agg_exprs)

        if having:
            result = result.filter(F.expr(having))

        return result

    def _aggregate_pandas(
        self,
        context: EngineContext,
        df,
        grain: List[str],
        measures: List[Dict],
        having: Optional[str],
    ):
        """Aggregate using DuckDB SQL via context.sql()."""
        grain_str = ", ".join(grain)

        measure_exprs = []
        for measure in measures:
            name = measure["name"]
            expr = measure["expr"]
            measure_exprs.append(f"{expr} AS {name}")
        measures_str = ", ".join(measure_exprs)

        sql = f"SELECT {grain_str}, {measures_str} FROM df GROUP BY {grain_str}"

        if having:
            sql += f" HAVING {having}"

        temp_context = context.with_df(df)
        result_context = temp_context.sql(sql)
        return result_context.df

    def _apply_incremental(
        self,
        context: EngineContext,
        new_agg_df,
        grain: List[str],
        measures: List[Dict],
        incremental: Dict,
        target: str,
    ):
        """Apply incremental merge with existing aggregations."""
        merge_strategy = incremental.get("merge_strategy", "replace")

        existing_df = self._load_existing_target(context, target)
        if existing_df is None:
            return new_agg_df

        if merge_strategy == "replace":
            return self._merge_replace(context, existing_df, new_agg_df, grain)
        elif merge_strategy == "sum":
            return self._merge_sum(context, existing_df, new_agg_df, grain, measures)
        elif merge_strategy == "min":
            return self._merge_min(context, existing_df, new_agg_df, grain, measures)
        else:  # max
            return self._merge_max(context, existing_df, new_agg_df, grain, measures)

    def _merge_replace(self, context: EngineContext, existing_df, new_df, grain: List[str]):
        """
        Replace strategy: New aggregates overwrite existing for matching grain keys.
        """
        if context.engine_type == EngineType.SPARK:
            new_keys = new_df.select(grain).distinct()

            unchanged = existing_df.join(new_keys, on=grain, how="left_anti")

            return unchanged.unionByName(new_df, allowMissingColumns=True)
        else:
            import pandas as pd

            new_keys = new_df[grain].drop_duplicates()

            merged = pd.merge(existing_df, new_keys, on=grain, how="left", indicator=True)
            unchanged = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

            return pd.concat([unchanged, new_df], ignore_index=True)

    def _merge_sum(
        self,
        context: EngineContext,
        existing_df,
        new_df,
        grain: List[str],
        measures: List[Dict],
    ):
        """
        Sum strategy: Add new measure values to existing for matching grain keys.
        """
        measure_names = [m["name"] for m in measures]

        if context.engine_type == EngineType.SPARK:
            from pyspark.sql import functions as F

            joined = existing_df.alias("e").join(new_df.alias("n"), on=grain, how="full_outer")

            select_cols = []
            for col in grain:
                select_cols.append(F.coalesce(F.col(f"e.{col}"), F.col(f"n.{col}")).alias(col))

            for name in measure_names:
                select_cols.append(
                    (
                        F.coalesce(F.col(f"e.{name}"), F.lit(0))
                        + F.coalesce(F.col(f"n.{name}"), F.lit(0))
                    ).alias(name)
                )

            other_cols = [
                c for c in existing_df.columns if c not in grain and c not in measure_names
            ]
            for col in other_cols:
                select_cols.append(F.coalesce(F.col(f"e.{col}"), F.col(f"n.{col}")).alias(col))

            return joined.select(select_cols)
        else:
            import pandas as pd

            merged = pd.merge(existing_df, new_df, on=grain, how="outer", suffixes=("_e", "_n"))

            result = merged[grain].copy()

            for name in measure_names:
                e_col = f"{name}_e" if f"{name}_e" in merged.columns else name
                n_col = f"{name}_n" if f"{name}_n" in merged.columns else name

                if e_col in merged.columns and n_col in merged.columns:
                    result[name] = merged[e_col].fillna(0).infer_objects(copy=False) + merged[
                        n_col
                    ].fillna(0).infer_objects(copy=False)
                elif e_col in merged.columns:
                    result[name] = merged[e_col].fillna(0).infer_objects(copy=False)
                elif n_col in merged.columns:
                    result[name] = merged[n_col].fillna(0).infer_objects(copy=False)
                else:
                    result[name] = 0

            other_cols = [
                c for c in existing_df.columns if c not in grain and c not in measure_names
            ]
            for col in other_cols:
                e_col = f"{col}_e" if f"{col}_e" in merged.columns else col
                n_col = f"{col}_n" if f"{col}_n" in merged.columns else col
                if e_col in merged.columns:
                    result[col] = merged[e_col]
                elif n_col in merged.columns:
                    result[col] = merged[n_col]

            return result

    def _merge_min(
        self,
        context: EngineContext,
        existing_df,
        new_df,
        grain: List[str],
        measures: List[Dict],
    ):
        """
        Min strategy: Keep the minimum value for each measure across existing and new.
        """
        measure_names = [m["name"] for m in measures]

        if context.engine_type == EngineType.SPARK:
            from pyspark.sql import functions as F

            joined = existing_df.alias("e").join(new_df.alias("n"), on=grain, how="full_outer")

            select_cols = []
            for col in grain:
                select_cols.append(F.coalesce(F.col(f"e.{col}"), F.col(f"n.{col}")).alias(col))

            for name in measure_names:
                select_cols.append(
                    F.least(
                        F.coalesce(F.col(f"e.{name}"), F.col(f"n.{name}")),
                        F.coalesce(F.col(f"n.{name}"), F.col(f"e.{name}")),
                    ).alias(name)
                )

            other_cols = [
                c for c in existing_df.columns if c not in grain and c not in measure_names
            ]
            for col in other_cols:
                select_cols.append(F.coalesce(F.col(f"e.{col}"), F.col(f"n.{col}")).alias(col))

            return joined.select(select_cols)
        else:
            import pandas as pd

            merged = pd.merge(existing_df, new_df, on=grain, how="outer", suffixes=("_e", "_n"))

            result = merged[grain].copy()

            for name in measure_names:
                e_col = f"{name}_e" if f"{name}_e" in merged.columns else name
                n_col = f"{name}_n" if f"{name}_n" in merged.columns else name

                if e_col in merged.columns and n_col in merged.columns:
                    result[name] = merged[[e_col, n_col]].min(axis=1)
                elif e_col in merged.columns:
                    result[name] = merged[e_col]
                elif n_col in merged.columns:
                    result[name] = merged[n_col]

            other_cols = [
                c for c in existing_df.columns if c not in grain and c not in measure_names
            ]
            for col in other_cols:
                e_col = f"{col}_e" if f"{col}_e" in merged.columns else col
                n_col = f"{col}_n" if f"{col}_n" in merged.columns else col
                if e_col in merged.columns:
                    result[col] = merged[e_col]
                elif n_col in merged.columns:
                    result[col] = merged[n_col]

            return result

    def _merge_max(
        self,
        context: EngineContext,
        existing_df,
        new_df,
        grain: List[str],
        measures: List[Dict],
    ):
        """
        Max strategy: Keep the maximum value for each measure across existing and new.
        """
        measure_names = [m["name"] for m in measures]

        if context.engine_type == EngineType.SPARK:
            from pyspark.sql import functions as F

            joined = existing_df.alias("e").join(new_df.alias("n"), on=grain, how="full_outer")

            select_cols = []
            for col in grain:
                select_cols.append(F.coalesce(F.col(f"e.{col}"), F.col(f"n.{col}")).alias(col))

            for name in measure_names:
                select_cols.append(
                    F.greatest(
                        F.coalesce(F.col(f"e.{name}"), F.col(f"n.{name}")),
                        F.coalesce(F.col(f"n.{name}"), F.col(f"e.{name}")),
                    ).alias(name)
                )

            other_cols = [
                c for c in existing_df.columns if c not in grain and c not in measure_names
            ]
            for col in other_cols:
                select_cols.append(F.coalesce(F.col(f"e.{col}"), F.col(f"n.{col}")).alias(col))

            return joined.select(select_cols)
        else:
            import pandas as pd

            merged = pd.merge(existing_df, new_df, on=grain, how="outer", suffixes=("_e", "_n"))

            result = merged[grain].copy()

            for name in measure_names:
                e_col = f"{name}_e" if f"{name}_e" in merged.columns else name
                n_col = f"{name}_n" if f"{name}_n" in merged.columns else name

                if e_col in merged.columns and n_col in merged.columns:
                    result[name] = merged[[e_col, n_col]].max(axis=1)
                elif e_col in merged.columns:
                    result[name] = merged[e_col]
                elif n_col in merged.columns:
                    result[name] = merged[n_col]

            other_cols = [
                c for c in existing_df.columns if c not in grain and c not in measure_names
            ]
            for col in other_cols:
                e_col = f"{col}_e" if f"{col}_e" in merged.columns else col
                n_col = f"{col}_n" if f"{col}_n" in merged.columns else col
                if e_col in merged.columns:
                    result[col] = merged[e_col]
                elif n_col in merged.columns:
                    result[col] = merged[n_col]

            return result
