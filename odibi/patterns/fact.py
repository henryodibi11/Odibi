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
        quarantine (dict): Quarantine configuration (required if orphan_handling=quarantine)
            - connection: Connection name for quarantine writes
            - path: Path for quarantine data (or use 'table')
            - table: Table name for quarantine (or use 'path')
            - add_columns (dict): Metadata columns to add
                - _rejection_reason (bool): Add rejection reason column
                - _rejected_at (bool): Add rejection timestamp column
                - _source_dimension (bool): Add source dimension name column
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

    Example with Quarantine:
        pattern:
          type: fact
          params:
            dimensions:
              - source_column: customer_id
                dimension_table: dim_customer
                dimension_key: customer_id
                surrogate_key: customer_sk
            orphan_handling: quarantine
            quarantine:
              connection: silver
              path: fact_orders_orphans
              add_columns:
                _rejection_reason: true
                _rejected_at: true
                _source_dimension: true
    """

    def validate(self) -> None:
        """Validate fact pattern configuration parameters.

        Ensures that all required parameters are present and valid. Checks that:
        - keys are provided when deduplicate is True
        - orphan_handling is valid ('unknown', 'reject', or 'quarantine')
        - quarantine config is complete when orphan_handling='quarantine'
        - all dimension lookups have required fields (source_column, dimension_table, etc.)

        Raises:
            ValueError: If keys are missing for deduplication, orphan_handling is invalid,
                quarantine config is incomplete, or dimension configs are missing required fields.
        """
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
                node=self.config.name,
            )
            raise ValueError(
                f"FactPattern (node '{self.config.name}'): 'keys' required when 'deduplicate' is True. "
                "Keys define which columns uniquely identify a fact row for deduplication. "
                "Provide keys=['col1', 'col2'] to specify the deduplication columns."
            )

        if orphan_handling not in ("unknown", "reject", "quarantine"):
            ctx.error(
                f"FactPattern validation failed: invalid orphan_handling '{orphan_handling}'",
                pattern="FactPattern",
                node=self.config.name,
            )
            raise ValueError(
                f"FactPattern (node '{self.config.name}'): 'orphan_handling' must be 'unknown', 'reject', or 'quarantine'. "
                f"Got: {orphan_handling}"
            )

        if orphan_handling == "quarantine":
            quarantine_config = self.params.get("quarantine")
            if not quarantine_config:
                ctx.error(
                    "FactPattern validation failed: 'quarantine' config required "
                    "when orphan_handling='quarantine'",
                    pattern="FactPattern",
                    node=self.config.name,
                )
                raise ValueError(
                    f"FactPattern (node '{self.config.name}'): 'quarantine' configuration is required when "
                    "orphan_handling='quarantine'."
                )
            if not quarantine_config.get("connection"):
                ctx.error(
                    "FactPattern validation failed: quarantine.connection is required",
                    pattern="FactPattern",
                    node=self.config.name,
                )
                raise ValueError(
                    f"FactPattern (node '{self.config.name}'): 'quarantine.connection' is required. "
                    "The connection specifies where to write quarantined orphan records "
                    "(e.g., a Spark session or database connection). "
                    "Add 'connection' to your quarantine config."
                )
            if not quarantine_config.get("path") and not quarantine_config.get("table"):
                ctx.error(
                    "FactPattern validation failed: quarantine requires 'path' or 'table'",
                    pattern="FactPattern",
                    node=self.config.name,
                )
                raise ValueError(
                    f"FactPattern (node '{self.config.name}'): 'quarantine' requires either 'path' or 'table'. "
                    f"Got config: {quarantine_config}. "
                    "Add 'path' for file storage or 'table' for database storage."
                )

        for i, dim in enumerate(dimensions):
            required_keys = ["source_column", "dimension_table", "dimension_key", "surrogate_key"]
            for key in required_keys:
                if key not in dim:
                    ctx.error(
                        f"FactPattern validation failed: dimension[{i}] missing '{key}'",
                        pattern="FactPattern",
                        node=self.config.name,
                    )
                    raise ValueError(
                        f"FactPattern (node '{self.config.name}'): dimension[{i}] missing required key '{key}'. "
                        f"Required keys: {required_keys}. "
                        f"Got: {dim}. "
                        f"Ensure all required keys are provided in the dimension config."
                    )

        ctx.debug(
            "FactPattern validation passed",
            pattern="FactPattern",
        )

    def execute(self, context: EngineContext) -> Any:
        """Execute the fact pattern to build a fact table with surrogate key lookups.

        Builds a fact table with automatic surrogate key lookups from dimension tables.
        The execution flow:
        1. Deduplicate source data if configured
        2. Perform dimension lookups to replace natural keys with surrogate keys
        3. Handle orphan records (unknown SK=0, reject with error, or quarantine)
        4. Apply measure calculations and transformations if configured
        5. Validate grain (check for duplicates) if grain is specified
        6. Add audit columns (load_timestamp, source_system) if configured

        Args:
            context: Engine context containing the source DataFrame, dimension tables,
                and execution environment.

        Returns:
            Fact DataFrame with surrogate keys from dimension lookups and all transformations applied.

        Raises:
            ValueError: If orphan records are found and orphan_handling='reject', if grain
                validation fails (duplicates found), or if dimension lookup fails.
            Exception: If quarantine write fails or measure calculations fail.
        """
        ctx = get_logging_context()
        start_time = time.time()

        deduplicate = self.params.get("deduplicate")
        keys = self.params.get("keys")
        grain = self.params.get("grain")
        dimensions = self.params.get("dimensions", [])
        orphan_handling = self.params.get("orphan_handling", "unknown")
        quarantine_config = self.params.get("quarantine", {})
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
                df, orphan_count, quarantined_df = self._lookup_dimensions(
                    context, df, dimensions, orphan_handling, quarantine_config
                )
                ctx.debug(
                    "Fact dimension lookups complete",
                    pattern="FactPattern",
                    orphan_count=orphan_count,
                )

                if orphan_handling == "quarantine" and quarantined_df is not None:
                    self._write_quarantine(context, quarantined_df, quarantine_config)
                    ctx.info(
                        f"Quarantined {orphan_count} orphan records",
                        pattern="FactPattern",
                        quarantine_path=quarantine_config.get("path")
                        or quarantine_config.get("table"),
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
                node=self.config.name,
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
        quarantine_config: Dict,
    ):
        """
        Perform surrogate key lookups from dimension tables.

        Returns:
            Tuple of (result_df, orphan_count, quarantined_df)
        """
        total_orphans = 0
        all_quarantined = []

        for dim_config in dimensions:
            source_col = dim_config["source_column"]
            dim_table = dim_config["dimension_table"]
            dim_key = dim_config["dimension_key"]
            sk_col = dim_config["surrogate_key"]
            is_scd2 = dim_config.get("scd2", False)

            dim_df = self._get_dimension_df(context, dim_table, is_scd2)
            if dim_df is None:
                raise ValueError(
                    f"FactPattern (node '{self.config.name}'): Dimension table '{dim_table}' not found in context. "
                    f"Expected dimension: '{dim_table}'. Available context: {list(context.dataframes.keys()) if hasattr(context, 'dataframes') else 'N/A'}."
                )

            df, orphan_count, quarantined = self._join_dimension(
                context,
                df,
                dim_df,
                source_col,
                dim_key,
                sk_col,
                orphan_handling,
                dim_table,
                quarantine_config,
            )
            total_orphans += orphan_count
            if quarantined is not None:
                all_quarantined.append(quarantined)

        quarantined_df = None
        if all_quarantined:
            quarantined_df = self._union_dataframes(context, all_quarantined)

        return df, total_orphans, quarantined_df

    def _union_dataframes(self, context: EngineContext, dfs: List):
        """Union multiple DataFrames together."""
        if not dfs:
            return None
        if context.engine_type == EngineType.SPARK:
            result = dfs[0]
            for df in dfs[1:]:
                result = result.unionByName(df, allowMissingColumns=True)
            return result
        else:
            import pandas as pd

            return pd.concat(dfs, ignore_index=True)

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
        dim_table: str,
        quarantine_config: Dict,
    ):
        """
        Join fact to dimension and retrieve surrogate key.

        Returns:
            Tuple of (result_df, orphan_count, quarantined_df)
        """
        if context.engine_type == EngineType.SPARK:
            return self._join_dimension_spark(
                context,
                fact_df,
                dim_df,
                source_col,
                dim_key,
                sk_col,
                orphan_handling,
                dim_table,
                quarantine_config,
            )
        else:
            return self._join_dimension_pandas(
                fact_df,
                dim_df,
                source_col,
                dim_key,
                sk_col,
                orphan_handling,
                dim_table,
                quarantine_config,
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
        dim_table: str,
        quarantine_config: Dict,
    ):
        from pyspark.sql import functions as F

        # Use a unique alias for the SK column to avoid ambiguity if sk_col
        # already exists in fact_df (e.g., from a previous dimension lookup)
        sk_alias = f"_dim_{sk_col}"
        dim_subset = dim_df.select(
            F.col(dim_key).alias(f"_dim_{dim_key}"),
            F.col(sk_col).alias(sk_alias),
        )

        joined = fact_df.join(
            dim_subset,
            fact_df[source_col] == dim_subset[f"_dim_{dim_key}"],
            "left",
        )

        # If sk_col already existed in fact_df, drop the old one and rename the new one
        if sk_col in fact_df.columns:
            joined = joined.drop(fact_df[sk_col])
        joined = joined.withColumnRenamed(sk_alias, sk_col)

        orphan_mask = F.col(sk_col).isNull()
        orphan_count = joined.filter(orphan_mask).count()
        quarantined_df = None

        if orphan_handling == "reject" and orphan_count > 0:
            raise ValueError(
                f"FactPattern: {orphan_count} orphan records found for dimension "
                f"lookup on '{source_col}'. Orphan handling is set to 'reject'."
            )

        if orphan_handling == "unknown":
            joined = joined.withColumn(sk_col, F.coalesce(F.col(sk_col), F.lit(0)))

        if orphan_handling == "quarantine" and orphan_count > 0:
            orphan_rows = joined.filter(orphan_mask).drop(f"_dim_{dim_key}")
            orphan_rows = self._add_quarantine_metadata_spark(
                orphan_rows, dim_table, source_col, quarantine_config
            )
            quarantined_df = orphan_rows
            joined = joined.filter(~orphan_mask)

        result = joined.drop(f"_dim_{dim_key}")

        return result, orphan_count, quarantined_df

    def _join_dimension_pandas(
        self,
        fact_df,
        dim_df,
        source_col: str,
        dim_key: str,
        sk_col: str,
        orphan_handling: str,
        dim_table: str,
        quarantine_config: Dict,
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

        orphan_mask = merged[sk_col].isna()
        orphan_count = orphan_mask.sum()
        quarantined_df = None

        if orphan_handling == "reject" and orphan_count > 0:
            raise ValueError(
                f"FactPattern (node '{self.config.name}'): {orphan_count} orphan records found for dimension "
                f"lookup on '{source_col}' -> '{dim_table}'. Orphan handling is set to 'reject'. "
                f"Fix: Either update orphan_handling to 'unknown' or 'quarantine', or ensure all '{source_col}' values exist in '{dim_table}.{dim_key}'."
            )

        if orphan_handling == "unknown":
            merged[sk_col] = merged[sk_col].fillna(0).infer_objects(copy=False).astype(int)

        if orphan_handling == "quarantine" and orphan_count > 0:
            orphan_rows = merged[orphan_mask].drop(columns=[f"_dim_{dim_key}"]).copy()
            orphan_rows = self._add_quarantine_metadata_pandas(
                orphan_rows, dim_table, source_col, quarantine_config
            )
            quarantined_df = orphan_rows
            merged = merged[~orphan_mask].copy()

        result = merged.drop(columns=[f"_dim_{dim_key}"])

        return result, int(orphan_count), quarantined_df

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
                node=self.config.name,
                grain=grain,
                total_rows=total_count,
                distinct_rows=distinct_count,
            )
            raise ValueError(
                f"FactPattern (node '{self.config.name}'): Grain validation failed. Found {duplicate_count} duplicate "
                f"rows at grain level {grain}. Total rows: {total_count}, "
                f"Distinct rows: {distinct_count}. "
                f"Fix: Check for duplicate data in source or add deduplication before grain validation."
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
                # Use timezone-aware timestamp for Delta Lake compatibility
                from datetime import timezone

                df["load_timestamp"] = datetime.now(timezone.utc)
            if source_system:
                df["source_system"] = source_system

        return df

    def _add_quarantine_metadata_spark(
        self,
        df,
        dim_table: str,
        source_col: str,
        quarantine_config: Dict,
    ):
        """Add metadata columns to quarantined Spark DataFrame."""
        from pyspark.sql import functions as F

        add_columns = quarantine_config.get("add_columns", {})

        if add_columns.get("_rejection_reason", False):
            reason = f"Orphan record: no match in dimension '{dim_table}' on column '{source_col}'"
            df = df.withColumn("_rejection_reason", F.lit(reason))

        if add_columns.get("_rejected_at", False):
            df = df.withColumn("_rejected_at", F.current_timestamp())

        if add_columns.get("_source_dimension", False):
            df = df.withColumn("_source_dimension", F.lit(dim_table))

        return df

    def _add_quarantine_metadata_pandas(
        self,
        df,
        dim_table: str,
        source_col: str,
        quarantine_config: Dict,
    ):
        """Add metadata columns to quarantined Pandas DataFrame."""
        add_columns = quarantine_config.get("add_columns", {})

        if add_columns.get("_rejection_reason", False):
            reason = f"Orphan record: no match in dimension '{dim_table}' on column '{source_col}'"
            df["_rejection_reason"] = reason

        if add_columns.get("_rejected_at", False):
            # Use timezone-aware timestamp for Delta Lake compatibility
            from datetime import timezone

            df["_rejected_at"] = datetime.now(timezone.utc)

        if add_columns.get("_source_dimension", False):
            df["_source_dimension"] = dim_table

        return df

    def _write_quarantine(
        self,
        context: EngineContext,
        quarantined_df,
        quarantine_config: Dict,
    ):
        """Write quarantined records to the configured destination."""
        ctx = get_logging_context()
        connection = quarantine_config.get("connection")
        path = quarantine_config.get("path")
        table = quarantine_config.get("table")

        if context.engine_type == EngineType.SPARK:
            self._write_quarantine_spark(context, quarantined_df, connection, path, table)
        else:
            self._write_quarantine_pandas(context, quarantined_df, connection, path, table)

        ctx.debug(
            "Quarantine data written",
            pattern="FactPattern",
            connection=connection,
            destination=path or table,
        )

    def _write_quarantine_spark(
        self,
        context: EngineContext,
        df,
        connection: str,
        path: Optional[str],
        table: Optional[str],
    ):
        """Write quarantine data using Spark."""
        if table:
            full_table = f"{connection}.{table}" if connection else table
            df.write.format("delta").mode("append").saveAsTable(full_table)
        elif path:
            full_path = path
            if hasattr(context, "engine") and context.engine:
                if connection in getattr(context.engine, "connections", {}):
                    try:
                        full_path = context.engine.connections[connection].get_path(path)
                    except Exception:
                        pass
            df.write.format("delta").mode("append").save(full_path)

    def _write_quarantine_pandas(
        self,
        context: EngineContext,
        df,
        connection: str,
        path: Optional[str],
        table: Optional[str],
    ):
        """Write quarantine data using Pandas."""
        import os

        destination = path or table
        full_path = destination

        if hasattr(context, "engine") and context.engine:
            if connection in getattr(context.engine, "connections", {}):
                try:
                    full_path = context.engine.connections[connection].get_path(destination)
                except Exception:
                    pass

        path_lower = str(full_path).lower()

        if path_lower.endswith(".csv"):
            if os.path.exists(full_path):
                df.to_csv(full_path, mode="a", header=False, index=False)
            else:
                df.to_csv(full_path, index=False)
        elif path_lower.endswith(".json"):
            if os.path.exists(full_path):
                import pandas as pd

                existing = pd.read_json(full_path)
                combined = pd.concat([existing, df], ignore_index=True)
                combined.to_json(full_path, orient="records")
            else:
                df.to_json(full_path, orient="records")
        else:
            if os.path.exists(full_path):
                import pandas as pd

                existing = pd.read_parquet(full_path)
                combined = pd.concat([existing, df], ignore_index=True)
                combined.to_parquet(full_path, index=False)
            else:
                df.to_parquet(full_path, index=False)
