import time
from datetime import datetime
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
            )
            raise ValueError("AggregationPattern: 'grain' parameter is required.")

        if not measures:
            ctx.error(
                "AggregationPattern validation failed: 'measures' is required",
                pattern="AggregationPattern",
            )
            raise ValueError("AggregationPattern: 'measures' parameter is required.")

        for i, measure in enumerate(measures):
            if not isinstance(measure, dict):
                ctx.error(
                    f"AggregationPattern validation failed: measure[{i}] must be a dict",
                    pattern="AggregationPattern",
                )
                raise ValueError(
                    f"AggregationPattern: measure[{i}] must be a dict with 'name' and 'expr'."
                )
            if "name" not in measure:
                ctx.error(
                    f"AggregationPattern validation failed: measure[{i}] missing 'name'",
                    pattern="AggregationPattern",
                )
                raise ValueError(f"AggregationPattern: measure[{i}] missing 'name'.")
            if "expr" not in measure:
                ctx.error(
                    f"AggregationPattern validation failed: measure[{i}] missing 'expr'",
                    pattern="AggregationPattern",
                )
                raise ValueError(f"AggregationPattern: measure[{i}] missing 'expr'.")

        incremental = self.params.get("incremental")
        if incremental:
            if "timestamp_column" not in incremental:
                ctx.error(
                    "AggregationPattern validation failed: incremental missing 'timestamp_column'",
                    pattern="AggregationPattern",
                )
                raise ValueError(
                    "AggregationPattern: incremental config requires 'timestamp_column'."
                )
            merge_strategy = incremental.get("merge_strategy", "replace")
            if merge_strategy not in ("replace", "sum", "min", "max"):
                ctx.error(
                    f"AggregationPattern validation failed: invalid merge_strategy '{merge_strategy}'",
                    pattern="AggregationPattern",
                )
                raise ValueError(
                    f"AggregationPattern: 'merge_strategy' must be 'replace', 'sum', 'min', or 'max'. "
                    f"Got: {merge_strategy}"
                )

        ctx.debug(
            "AggregationPattern validation passed",
            pattern="AggregationPattern",
        )

    def execute(self, context: EngineContext) -> Any:
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

    def _aggregate(
        self,
        context: EngineContext,
        df,
        grain: List[str],
        measures: List[Dict],
        having: Optional[str],
    ):
        """Perform the aggregation using SQL."""
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

    def _load_existing_target(self, context: EngineContext, target: str):
        """Load existing target table if it exists."""
        if context.engine_type == EngineType.SPARK:
            return self._load_existing_spark(context, target)
        else:
            return self._load_existing_pandas(context, target)

    def _load_existing_spark(self, context: EngineContext, target: str):
        spark = context.spark
        try:
            return spark.table(target)
        except Exception:
            try:
                return spark.read.format("delta").load(target)
            except Exception:
                return None

    def _load_existing_pandas(self, context: EngineContext, target: str):
        import os

        import pandas as pd

        path = target
        if hasattr(context, "engine") and context.engine:
            if "." in path:
                parts = path.split(".", 1)
                conn_name = parts[0]
                rel_path = parts[1]
                if conn_name in context.engine.connections:
                    try:
                        path = context.engine.connections[conn_name].get_path(rel_path)
                    except Exception:
                        pass

        if not os.path.exists(path):
            return None

        try:
            if str(path).endswith(".parquet") or os.path.isdir(path):
                return pd.read_parquet(path)
            elif str(path).endswith(".csv"):
                return pd.read_csv(path)
        except Exception:
            return None

        return None

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
