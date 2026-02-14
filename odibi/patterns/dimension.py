import time
from datetime import datetime, timezone
from typing import Any, List, Optional

from pydantic import BaseModel, Field

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.patterns.base import Pattern
from odibi.transformers.scd import SCD2Params, scd2
from odibi.utils.logging_context import get_logging_context


class AuditConfig(BaseModel):
    """Configuration for audit columns."""

    load_timestamp: bool = Field(default=True, description="Add load_timestamp column")
    source_system: Optional[str] = Field(
        default=None, description="Source system name for source_system column"
    )


class DimensionPattern(Pattern):
    """
    Dimension Pattern: Builds complete dimension tables with surrogate keys and SCD support.

    Features:
    - Auto-generate integer surrogate keys (MAX(existing) + ROW_NUMBER for new rows)
    - SCD Type 0 (static), 1 (overwrite), 2 (history tracking)
    - Optional unknown member row (SK=0) for orphan FK handling
    - Audit columns (load_timestamp, source_system)

    Configuration Options (via params dict):
        - **natural_key** (str): Natural/business key column name
        - **surrogate_key** (str): Surrogate key column name to generate
        - **scd_type** (int): 0=static, 1=overwrite, 2=history tracking (default: 1)
        - **track_cols** (list): Columns to track for SCD1/2 changes
        - **target** (str): Target table path (required for SCD2 to read existing history)
        - **unknown_member** (bool): If true, insert a row with SK=0 for orphan FK handling
        - **audit** (dict): Audit configuration with load_timestamp and source_system

    Supported target formats:
        Spark:
            - Catalog tables: catalog.schema.table, warehouse.dim_customer
            - Delta paths: /path/to/delta (no extension)
            - Parquet: /path/to/file.parquet
            - CSV: /path/to/file.csv
            - JSON: /path/to/file.json
            - ORC: /path/to/file.orc
        Pandas:
            - Parquet: path/to/file.parquet (or directory)
            - CSV: path/to/file.csv
            - JSON: path/to/file.json
            - Excel: path/to/file.xlsx, path/to/file.xls
            - Feather/Arrow: path/to/file.feather, path/to/file.arrow
            - Pickle: path/to/file.pickle, path/to/file.pkl
            - Connection-prefixed: warehouse.dim_customer
    """

    def validate(self) -> None:
        """Validate dimension pattern configuration parameters.

        Ensures that all required parameters are present and valid. Checks that:
        - natural_key is provided (business key column(s))
        - surrogate_key is provided (auto-generated primary key column name)
        - scd_type is valid (0, 1, or 2)
        - target is provided for SCD Type 2 (required for history comparison)
        - track_cols is provided for SCD Type 1 and 2 (columns to monitor for changes)

        Raises:
            ValueError: If natural_key or surrogate_key is missing, scd_type is invalid,
                target is missing for SCD2, or track_cols is missing for SCD1/2.
        """
        ctx = get_logging_context()
        ctx.debug(
            "DimensionPattern validation starting",
            pattern="DimensionPattern",
            params=self.params,
        )

        if not self.params.get("natural_key"):
            ctx.error(
                "DimensionPattern validation failed: 'natural_key' is required",
                pattern="DimensionPattern",
                node=self.config.name,
            )
            raise ValueError(
                f"DimensionPattern (node '{self.config.name}'): 'natural_key' parameter is required. "
                "The natural_key identifies the business key column(s) that uniquely identify "
                "each dimension record in the source system. "
                "Provide natural_key as a string (single column) or list of strings (composite key)."
            )

        if not self.params.get("surrogate_key"):
            ctx.error(
                "DimensionPattern validation failed: 'surrogate_key' is required",
                pattern="DimensionPattern",
                node=self.config.name,
            )
            raise ValueError(
                f"DimensionPattern (node '{self.config.name}'): 'surrogate_key' parameter is required. "
                "The surrogate_key is the auto-generated primary key column for the dimension table, "
                "used to join with fact tables instead of the natural key. "
                "Provide surrogate_key as a string specifying the column name (e.g., 'customer_sk')."
            )

        scd_type = self.params.get("scd_type", 1)
        if scd_type not in (0, 1, 2):
            ctx.error(
                f"DimensionPattern validation failed: invalid scd_type {scd_type}",
                pattern="DimensionPattern",
                node=self.config.name,
            )
            raise ValueError(
                f"DimensionPattern (node '{self.config.name}'): 'scd_type' must be 0, 1, or 2. Got: {scd_type}. "
                "SCD Type 0: No changes tracked (static dimension). "
                "SCD Type 1: Overwrite changes (no history). "
                "SCD Type 2: Track full history with valid_from/valid_to dates."
            )

        if scd_type == 2 and not self.params.get("target"):
            ctx.error(
                "DimensionPattern validation failed: 'target' required for SCD2",
                pattern="DimensionPattern",
                node=self.config.name,
            )
            raise ValueError(
                f"DimensionPattern (node '{self.config.name}'): 'target' parameter is required for scd_type=2. "
                "SCD Type 2 compares incoming data against existing records to detect changes, "
                "so a target DataFrame containing current dimension data must be provided. "
                "Pass the existing dimension table as the 'target' parameter."
            )

        if scd_type in (1, 2) and not self.params.get("track_cols"):
            ctx.error(
                "DimensionPattern validation failed: 'track_cols' required for SCD1/2",
                pattern="DimensionPattern",
                node=self.config.name,
            )
            raise ValueError(
                f"DimensionPattern (node '{self.config.name}'): 'track_cols' parameter is required for scd_type 1 or 2. "
                "The track_cols specifies which columns to monitor for changes. "
                "When these columns change, SCD1 overwrites values or SCD2 creates new history records. "
                "Provide track_cols as a list of column names (e.g., ['address', 'phone', 'email'])."
            )

        ctx.debug(
            "DimensionPattern validation passed",
            pattern="DimensionPattern",
        )

    def execute(self, context: EngineContext) -> Any:
        """Execute the dimension pattern to build a dimension table with surrogate keys.

        Builds a complete dimension table with auto-generated surrogate keys and optional
        slowly changing dimension (SCD) support. The execution flow varies by SCD type:
        - SCD Type 0: Insert new records only, never update existing
        - SCD Type 1: Update existing records in-place, insert new records
        - SCD Type 2: Track full history with valid_from/valid_to dates and is_current flag

        All modes generate surrogate keys starting from MAX(existing_sk) + 1 for new records.
        Optionally adds audit columns and ensures unknown member row (SK=0) exists.

        Args:
            context: Engine context containing the source DataFrame and execution environment.

        Returns:
            DataFrame with surrogate keys assigned and SCD logic applied as configured.

        Raises:
            Exception: If target loading fails, SCD processing fails, or surrogate key
                generation encounters errors.
        """
        ctx = get_logging_context()
        start_time = time.time()

        natural_key = self.params.get("natural_key")
        surrogate_key = self.params.get("surrogate_key")
        scd_type = self.params.get("scd_type", 1)
        track_cols = self.params.get("track_cols", [])
        target = self.params.get("target")
        unknown_member = self.params.get("unknown_member", False)
        audit_config = self.params.get("audit", {})

        ctx.debug(
            "DimensionPattern starting",
            pattern="DimensionPattern",
            natural_key=natural_key,
            surrogate_key=surrogate_key,
            scd_type=scd_type,
            track_cols=track_cols,
            target=target,
            unknown_member=unknown_member,
        )

        source_count = self._get_row_count(context.df, context.engine_type)
        ctx.debug("Dimension source loaded", pattern="DimensionPattern", source_rows=source_count)

        try:
            if scd_type == 0:
                result_df = self._execute_scd0(context, natural_key, surrogate_key, target)
            elif scd_type == 1:
                result_df = self._execute_scd1(
                    context, natural_key, surrogate_key, track_cols, target
                )
            else:
                result_df = self._execute_scd2(
                    context, natural_key, surrogate_key, track_cols, target
                )

            result_df = self._add_audit_columns(context, result_df, audit_config)

            if unknown_member:
                result_df = self._ensure_unknown_member(
                    context, result_df, natural_key, surrogate_key, audit_config
                )

            result_count = self._get_row_count(result_df, context.engine_type)
            elapsed_ms = (time.time() - start_time) * 1000

            ctx.info(
                "DimensionPattern completed",
                pattern="DimensionPattern",
                elapsed_ms=round(elapsed_ms, 2),
                source_rows=source_count,
                result_rows=result_count,
                scd_type=scd_type,
            )

            return result_df

        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            ctx.error(
                f"DimensionPattern failed: {e}",
                pattern="DimensionPattern",
                node=self.config.name,
                error_type=type(e).__name__,
                elapsed_ms=round(elapsed_ms, 2),
            )
            raise

    def _load_existing_target(self, context: EngineContext, target: str):
        """Load existing target table if it exists."""
        if context.engine_type == EngineType.SPARK:
            return self._load_existing_spark(context, target)
        else:
            return self._load_existing_pandas(context, target)

    def _load_existing_spark(self, context: EngineContext, target: str):
        """Load existing target table from Spark with multi-format support."""
        ctx = get_logging_context()
        spark = context.spark

        # Try catalog table first
        try:
            return spark.table(target)
        except Exception:
            pass

        # Check file extension for format detection
        target_lower = target.lower()

        try:
            if target_lower.endswith(".parquet"):
                return spark.read.parquet(target)
            elif target_lower.endswith(".csv"):
                return spark.read.option("header", "true").option("inferSchema", "true").csv(target)
            elif target_lower.endswith(".json"):
                return spark.read.json(target)
            elif target_lower.endswith(".orc"):
                return spark.read.orc(target)
            else:
                # Try Delta format as fallback (for paths without extension)
                return spark.read.format("delta").load(target)
        except Exception as e:
            ctx.warning(
                f"Could not load existing target '{target}': {e}. Treating as initial load.",
                pattern="DimensionPattern",
                target=target,
            )
            return None

    def _load_existing_pandas(self, context: EngineContext, target: str):
        """Load existing target table from Pandas with multi-format support."""
        import os

        import pandas as pd

        ctx = get_logging_context()
        path = target

        # Handle connection-prefixed paths
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

        path_lower = str(path).lower()

        try:
            # Parquet (file or directory)
            if path_lower.endswith(".parquet") or os.path.isdir(path):
                return pd.read_parquet(path)
            # CSV
            elif path_lower.endswith(".csv"):
                return pd.read_csv(path)
            # JSON
            elif path_lower.endswith(".json"):
                return pd.read_json(path)
            # Excel
            elif path_lower.endswith(".xlsx") or path_lower.endswith(".xls"):
                return pd.read_excel(path)
            # Feather / Arrow IPC
            elif path_lower.endswith(".feather") or path_lower.endswith(".arrow"):
                return pd.read_feather(path)
            # Pickle
            elif path_lower.endswith(".pickle") or path_lower.endswith(".pkl"):
                return pd.read_pickle(path)
            else:
                ctx.warning(
                    f"Unrecognized file format for target '{target}'. "
                    "Supported formats: parquet, csv, json, xlsx, xls, feather, arrow, pickle. "
                    "Treating as initial load.",
                    pattern="DimensionPattern",
                    target=target,
                )
                return None
        except Exception as e:
            ctx.warning(
                f"Could not load existing target '{target}': {e}. Treating as initial load.",
                pattern="DimensionPattern",
                target=target,
            )
            return None

    def _get_max_sk(self, df, surrogate_key: str, engine_type) -> int:
        """Get the maximum surrogate key value from existing data."""
        if df is None:
            return 0
        try:
            if engine_type == EngineType.SPARK:
                from pyspark.sql import functions as F

                max_row = df.agg(F.max(surrogate_key)).collect()[0]
                max_val = max_row[0]
                return max_val if max_val is not None else 0
            else:
                if surrogate_key not in df.columns:
                    return 0
                max_val = df[surrogate_key].max()
                return int(max_val) if max_val is not None and not (max_val != max_val) else 0
        except Exception:
            return 0

    def _generate_surrogate_keys(
        self,
        context: EngineContext,
        df,
        natural_key: str,
        surrogate_key: str,
        start_sk: int,
    ):
        """Generate surrogate keys starting from start_sk + 1."""
        if context.engine_type == EngineType.SPARK:
            from pyspark.sql import functions as F
            from pyspark.sql.window import Window

            window = Window.orderBy(natural_key)
            df = df.withColumn(
                surrogate_key, (F.row_number().over(window) + F.lit(start_sk)).cast("int")
            )
            return df
        else:
            df = df.copy()
            df = df.sort_values(by=natural_key).reset_index(drop=True)
            df[surrogate_key] = range(start_sk + 1, start_sk + 1 + len(df))
            df[surrogate_key] = df[surrogate_key].astype("int64")
            return df

    def _execute_scd0(
        self,
        context: EngineContext,
        natural_key: str,
        surrogate_key: str,
        target: Optional[str],
    ):
        """
        SCD Type 0: Static dimension - never update existing records.
        Only insert new records that don't exist in target.
        """
        existing_df = self._load_existing_target(context, target) if target else None
        source_df = context.df

        if existing_df is None:
            return self._generate_surrogate_keys(
                context, source_df, natural_key, surrogate_key, start_sk=0
            )

        max_sk = self._get_max_sk(existing_df, surrogate_key, context.engine_type)

        if context.engine_type == EngineType.SPARK:
            existing_keys = existing_df.select(natural_key).distinct()
            new_records = source_df.join(existing_keys, on=natural_key, how="left_anti")
        else:
            existing_keys = set(existing_df[natural_key].unique())
            new_records = source_df[~source_df[natural_key].isin(existing_keys)].copy()

        if self._get_row_count(new_records, context.engine_type) == 0:
            return existing_df

        new_with_sk = self._generate_surrogate_keys(
            context, new_records, natural_key, surrogate_key, start_sk=max_sk
        )

        if context.engine_type == EngineType.SPARK:
            return existing_df.unionByName(new_with_sk, allowMissingColumns=True)
        else:
            import pandas as pd

            return pd.concat([existing_df, new_with_sk], ignore_index=True)

    def _execute_scd1(
        self,
        context: EngineContext,
        natural_key: str,
        surrogate_key: str,
        track_cols: List[str],
        target: Optional[str],
    ):
        """
        SCD Type 1: Overwrite changes - no history tracking.
        Update existing records in place, insert new records.
        """
        existing_df = self._load_existing_target(context, target) if target else None
        source_df = context.df

        if existing_df is None:
            return self._generate_surrogate_keys(
                context, source_df, natural_key, surrogate_key, start_sk=0
            )

        max_sk = self._get_max_sk(existing_df, surrogate_key, context.engine_type)

        if context.engine_type == EngineType.SPARK:
            return self._execute_scd1_spark(
                context, source_df, existing_df, natural_key, surrogate_key, track_cols, max_sk
            )
        else:
            return self._execute_scd1_pandas(
                context, source_df, existing_df, natural_key, surrogate_key, track_cols, max_sk
            )

    def _execute_scd1_spark(
        self,
        context: EngineContext,
        source_df,
        existing_df,
        natural_key: str,
        surrogate_key: str,
        track_cols: List[str],
        max_sk: int,
    ):
        from pyspark.sql import functions as F

        t_prefix = "__existing_"
        renamed_existing = existing_df
        for c in existing_df.columns:
            renamed_existing = renamed_existing.withColumnRenamed(c, f"{t_prefix}{c}")

        joined = source_df.join(
            renamed_existing,
            source_df[natural_key] == renamed_existing[f"{t_prefix}{natural_key}"],
            "left",
        )

        new_records = joined.filter(F.col(f"{t_prefix}{natural_key}").isNull()).select(
            source_df.columns
        )

        update_records = joined.filter(F.col(f"{t_prefix}{natural_key}").isNotNull())
        update_cols = [F.col(f"{t_prefix}{surrogate_key}").alias(surrogate_key)] + [
            F.col(c) for c in source_df.columns
        ]
        updated_records = update_records.select(update_cols)

        unchanged_keys = update_records.select(F.col(f"{t_prefix}{natural_key}").alias(natural_key))
        unchanged = existing_df.join(unchanged_keys, on=natural_key, how="left_anti")

        new_with_sk = self._generate_surrogate_keys(
            context, new_records, natural_key, surrogate_key, start_sk=max_sk
        )

        result = unchanged.unionByName(updated_records, allowMissingColumns=True).unionByName(
            new_with_sk, allowMissingColumns=True
        )
        return result

    def _execute_scd1_pandas(
        self,
        context: EngineContext,
        source_df,
        existing_df,
        natural_key: str,
        surrogate_key: str,
        track_cols: List[str],
        max_sk: int,
    ):
        import pandas as pd

        merged = pd.merge(
            source_df,
            existing_df[[natural_key, surrogate_key]],
            on=natural_key,
            how="left",
            suffixes=("", "_existing"),
        )

        has_existing_sk = f"{surrogate_key}_existing" in merged.columns
        if has_existing_sk:
            merged[surrogate_key] = merged[f"{surrogate_key}_existing"]
            merged = merged.drop(columns=[f"{surrogate_key}_existing"])

        new_mask = merged[surrogate_key].isna()
        new_records = merged[new_mask].drop(columns=[surrogate_key])
        existing_records = merged[~new_mask]

        if len(new_records) > 0:
            new_with_sk = self._generate_surrogate_keys(
                context, new_records, natural_key, surrogate_key, start_sk=max_sk
            )
        else:
            new_with_sk = pd.DataFrame()

        unchanged = existing_df[~existing_df[natural_key].isin(source_df[natural_key])]

        result = pd.concat([unchanged, existing_records, new_with_sk], ignore_index=True)
        return result

    def _execute_scd2(
        self,
        context: EngineContext,
        natural_key: str,
        surrogate_key: str,
        track_cols: List[str],
        target: str,
    ):
        """
        SCD Type 2: History tracking - reuse existing scd2 transformer.
        Surrogate keys are generated for new/changed records.
        """
        existing_df = self._load_existing_target(context, target)

        valid_from_col = self.params.get("valid_from_col", "valid_from")
        valid_to_col = self.params.get("valid_to_col", "valid_to")
        is_current_col = self.params.get("is_current_col", "is_current")

        if context.engine_type == EngineType.SPARK:
            from pyspark.sql import functions as F

            source_with_time = context.df.withColumn(valid_from_col, F.current_timestamp())
        else:
            source_df = context.df.copy()
            # Use timezone-aware timestamp for Delta Lake compatibility
            from datetime import timezone

            source_df[valid_from_col] = datetime.now(timezone.utc)
            source_with_time = source_df

        temp_context = context.with_df(source_with_time)

        scd_params = SCD2Params(
            target=target,
            keys=[natural_key],
            track_cols=track_cols,
            effective_time_col=valid_from_col,
            end_time_col=valid_to_col,
            current_flag_col=is_current_col,
        )

        result_context = scd2(temp_context, scd_params)
        result_df = result_context.df

        max_sk = self._get_max_sk(existing_df, surrogate_key, context.engine_type)

        if context.engine_type == EngineType.SPARK:
            from pyspark.sql import functions as F
            from pyspark.sql.window import Window

            if surrogate_key not in result_df.columns:
                window = Window.orderBy(natural_key, valid_from_col)
                result_df = result_df.withColumn(
                    surrogate_key, (F.row_number().over(window) + F.lit(max_sk)).cast("int")
                )
            else:
                null_sk_df = result_df.filter(F.col(surrogate_key).isNull())
                has_sk_df = result_df.filter(F.col(surrogate_key).isNotNull())

                if null_sk_df.count() > 0:
                    window = Window.orderBy(natural_key, valid_from_col)
                    null_sk_df = null_sk_df.withColumn(
                        surrogate_key, (F.row_number().over(window) + F.lit(max_sk)).cast("int")
                    )
                    result_df = has_sk_df.unionByName(null_sk_df)
        else:
            import pandas as pd

            if surrogate_key not in result_df.columns:
                result_df = result_df.sort_values([natural_key, valid_from_col]).reset_index(
                    drop=True
                )
                result_df[surrogate_key] = range(max_sk + 1, max_sk + 1 + len(result_df))
            else:
                null_mask = result_df[surrogate_key].isna()
                if null_mask.any():
                    null_df = result_df[null_mask].copy()
                    null_df = null_df.sort_values([natural_key, valid_from_col]).reset_index(
                        drop=True
                    )
                    null_df[surrogate_key] = range(max_sk + 1, max_sk + 1 + len(null_df))
                    result_df = pd.concat([result_df[~null_mask], null_df], ignore_index=True)

        return result_df

    def _add_audit_columns(self, context: EngineContext, df, audit_config: dict):
        """Add audit columns (load_timestamp, source_system) to the dataframe."""
        load_timestamp = audit_config.get("load_timestamp", True)
        source_system = audit_config.get("source_system")

        if context.engine_type == EngineType.SPARK:
            from pyspark.sql import functions as F

            if load_timestamp:
                df = df.withColumn("load_timestamp", F.current_timestamp())
            if source_system:
                df = df.withColumn("source_system", F.lit(source_system))
        else:
            df = df.copy()
            if load_timestamp:
                # Use timezone-aware timestamp for Delta Lake compatibility
                from datetime import timezone

                df["load_timestamp"] = datetime.now(timezone.utc)
            if source_system:
                df["source_system"] = source_system

        return df

    def _ensure_unknown_member(
        self,
        context: EngineContext,
        df,
        natural_key: str,
        surrogate_key: str,
        audit_config: dict,
    ):
        """Ensure unknown member row exists with SK=0."""
        valid_from_col = self.params.get("valid_from_col", "valid_from")
        valid_to_col = self.params.get("valid_to_col", "valid_to")
        is_current_col = self.params.get("is_current_col", "is_current")

        if context.engine_type == EngineType.SPARK:
            from pyspark.sql import functions as F

            existing_unknown = df.filter(F.col(surrogate_key) == 0)
            if existing_unknown.count() > 0:
                return df

            columns = df.columns
            unknown_values = []
            for col in columns:
                if col == surrogate_key:
                    unknown_values.append(0)
                elif col == natural_key:
                    unknown_values.append("-1")
                elif col == valid_from_col:
                    unknown_values.append(datetime(1900, 1, 1, tzinfo=timezone.utc))
                elif col == valid_to_col:
                    unknown_values.append(None)
                elif col == is_current_col:
                    unknown_values.append(True)
                elif col == "load_timestamp":
                    # Use timezone-aware timestamp for Delta Lake compatibility
                    unknown_values.append(datetime.now(timezone.utc))
                elif col == "source_system":
                    unknown_values.append(audit_config.get("source_system", "Unknown"))
                else:
                    unknown_values.append("Unknown")

            unknown_row = context.spark.createDataFrame([unknown_values], columns)
            return unknown_row.unionByName(df)
        else:
            import pandas as pd

            if (df[surrogate_key] == 0).any():
                return df

            unknown_row = {}
            for col in df.columns:
                if col == surrogate_key:
                    unknown_row[col] = 0
                elif col == natural_key:
                    unknown_row[col] = "-1"
                elif col == valid_from_col:
                    unknown_row[col] = datetime(1900, 1, 1, tzinfo=timezone.utc)
                elif col == valid_to_col:
                    unknown_row[col] = None
                elif col == is_current_col:
                    unknown_row[col] = True
                elif col == "load_timestamp":
                    # Use timezone-aware timestamp for Delta Lake compatibility
                    unknown_row[col] = datetime.now(timezone.utc)
                elif col == "source_system":
                    unknown_row[col] = audit_config.get("source_system", "Unknown")
                else:
                    dtype = df[col].dtype
                    if pd.api.types.is_numeric_dtype(dtype):
                        unknown_row[col] = 0
                    else:
                        unknown_row[col] = "Unknown"

            unknown_df = pd.DataFrame([unknown_row])
            for col in unknown_df.columns:
                if col in df.columns:
                    unknown_df[col] = unknown_df[col].astype(df[col].dtype)
            return pd.concat([unknown_df, df], ignore_index=True)
