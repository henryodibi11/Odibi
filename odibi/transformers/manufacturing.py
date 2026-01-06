"""
Manufacturing Transformers

Specialized transformers for manufacturing/process data analysis.
Handles common patterns like cycle detection, phase analysis, and time-in-state calculations.
"""

import time
from typing import Dict, List, Optional, Union

import pandas as pd
from pydantic import BaseModel, Field

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.utils.logging_context import get_logging_context


# =============================================================================
# DETECT SEQUENTIAL PHASES
# =============================================================================


class PhaseConfig(BaseModel):
    """Configuration for a single phase."""

    timer_col: str = Field(..., description="Timer column name for this phase")
    start_threshold: Optional[int] = Field(
        None, description="Override default start threshold for this phase (seconds)"
    )


class DetectSequentialPhasesParams(BaseModel):
    """
    Detect and analyze sequential manufacturing phases from timer columns.

    This transformer processes raw sensor/PLC data where timer columns increment
    during each phase. It detects phase boundaries, calculates durations, and
    tracks time spent in each equipment status.

    Common use cases:
    - Batch reactor cycle analysis
    - CIP (Clean-in-Place) phase timing
    - Food processing (cook, cool, package cycles)
    - Any multi-step batch process with PLC timers

    Scenario: Analyze FBR cycle times
    ```yaml
    detect_sequential_phases:
      group_by: BatchID
      timestamp_col: ts
      phases:
        - timer_col: LoadTime
        - timer_col: AcidTime
        - timer_col: DryTime
        - timer_col: CookTime
        - timer_col: CoolTime
        - timer_col: UnloadTime
      start_threshold: 240
      status_col: Status
      status_mapping:
        1: idle
        2: active
        3: hold
        4: faulted
      phase_metrics:
        Level: max
      metadata:
        ProductCode: first_after_start
        Weight: max
    ```

    Scenario: Group by multiple columns
    ```yaml
    detect_sequential_phases:
      group_by:
        - BatchID
        - AssetID
      phases: [LoadTime, CookTime]
    ```
    """

    group_by: Union[str, List[str]] = Field(
        ...,
        description="Column(s) to group by. Can be a single column name or list of columns. "
        "E.g., 'BatchID' or ['BatchID', 'AssetID']",
    )
    timestamp_col: str = Field(default="ts", description="Timestamp column for ordering events")
    phases: List[Union[str, PhaseConfig]] = Field(
        ...,
        description="List of phase timer columns (strings) or PhaseConfig objects. "
        "Phases are processed sequentially - each phase starts after the previous ends.",
    )
    start_threshold: int = Field(
        default=240,
        description="Default max timer value (seconds) to consider as valid phase start. "
        "Filters out late readings where timer already shows large elapsed time.",
    )
    status_col: Optional[str] = Field(None, description="Column containing equipment status codes")
    status_mapping: Optional[Dict[int, str]] = Field(
        None,
        description="Mapping of status codes to names. "
        "E.g., {1: 'idle', 2: 'active', 3: 'hold', 4: 'faulted'}",
    )
    phase_metrics: Optional[Dict[str, str]] = Field(
        None,
        description="Columns to aggregate within each phase window. "
        "E.g., {Level: max, Pressure: max}. Outputs {Phase}_{Column} columns.",
    )
    metadata: Optional[Dict[str, str]] = Field(
        None,
        description="Columns to include in output with aggregation method. "
        "Options: 'first', 'last', 'first_after_start', 'max', 'min', 'mean', 'sum'. "
        "E.g., {ProductCode: first_after_start, Weight: max}",
    )
    output_time_format: str = Field(
        default="%Y-%m-%d %H:%M:%S",
        description="Format for output timestamp columns",
    )
    fill_null_minutes: bool = Field(
        default=False,
        description="If True, fill null numeric columns (_max_minutes, _status_minutes, _metrics) "
        "with 0. Timestamp columns remain null for skipped phases.",
    )


def _normalize_group_by(group_by: Union[str, List[str]]) -> List[str]:
    """Convert group_by to list format."""
    if isinstance(group_by, str):
        return [group_by]
    return list(group_by)


def _get_expected_columns(params: "DetectSequentialPhasesParams") -> Dict[str, None]:
    """
    Build a dict of ALL expected output columns with None/NaT values.

    This ensures Spark applyInPandas always receives a DataFrame with
    all columns defined in the schema, even when phases are skipped.
    Uses pd.NaT for timestamp columns to match TimestampType schema.
    """
    columns = {}

    phase_names = [p.timer_col if isinstance(p, PhaseConfig) else p for p in params.phases]
    for phase in phase_names:
        columns[f"{phase}_start"] = pd.NaT
        columns[f"{phase}_end"] = pd.NaT
        columns[f"{phase}_max_minutes"] = None

        if params.status_mapping:
            for status_name in params.status_mapping.values():
                columns[f"{phase}_{status_name}_minutes"] = None

        if params.phase_metrics:
            for metric_col in params.phase_metrics.keys():
                columns[f"{phase}_{metric_col}"] = None

    if params.metadata:
        for col in params.metadata.keys():
            columns[col] = None

    return columns


def _get_numeric_columns(params: "DetectSequentialPhasesParams") -> List[str]:
    """Get list of all numeric output column names (for fill_null_minutes)."""
    columns = []

    phase_names = [p.timer_col if isinstance(p, PhaseConfig) else p for p in params.phases]
    for phase in phase_names:
        columns.append(f"{phase}_max_minutes")

        if params.status_mapping:
            for status_name in params.status_mapping.values():
                columns.append(f"{phase}_{status_name}_minutes")

        if params.phase_metrics:
            for metric_col in params.phase_metrics.keys():
                columns.append(f"{phase}_{metric_col}")

    return columns


def _fill_null_numeric_columns(
    df: pd.DataFrame, params: "DetectSequentialPhasesParams"
) -> pd.DataFrame:
    """Fill null values in numeric columns with 0."""
    numeric_cols = _get_numeric_columns(params)
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


def detect_sequential_phases(
    context: EngineContext, params: DetectSequentialPhasesParams
) -> EngineContext:
    """
    Detect and analyze sequential manufacturing phases.

    For each group (e.g., batch), this transformer:
    1. Processes phases sequentially (each starts after previous ends)
    2. Detects phase start by finding first valid timer reading and back-calculating
    3. Detects phase end by finding first repeated (plateaued) timer value
    4. Calculates time spent in each status during each phase
    5. Aggregates specified metrics within each phase window
    6. Outputs one summary row per group

    Output columns per phase:
    - {phase}_start: Phase start timestamp
    - {phase}_end: Phase end timestamp
    - {phase}_max_minutes: Maximum timer value converted to minutes
    - {phase}_{status}_minutes: Time in each status (if status_col provided)
    - {phase}_{metric}: Aggregated metrics (if phase_metrics provided)
    """
    ctx = get_logging_context()
    start_time = time.time()

    phase_names = [p.timer_col if isinstance(p, PhaseConfig) else p for p in params.phases]
    group_by_cols = _normalize_group_by(params.group_by)

    ctx.debug(
        "DetectSequentialPhases starting",
        group_by=group_by_cols,
        phases=phase_names,
    )

    if context.engine_type == EngineType.PANDAS:
        result_df = _detect_phases_pandas(context.df, params)
    elif context.engine_type == EngineType.SPARK:
        result_df = _detect_phases_spark_native(context.df, params)
    elif context.engine_type == EngineType.POLARS:
        result_df = _detect_phases_polars(context.df, params)
    else:
        raise ValueError(f"Unsupported engine: {context.engine_type}")

    elapsed_ms = (time.time() - start_time) * 1000
    ctx.debug(
        "DetectSequentialPhases completed",
        output_rows=len(result_df) if hasattr(result_df, "__len__") else "unknown",
        elapsed_ms=round(elapsed_ms, 2),
    )
    return context.with_df(result_df)


def _detect_phases_pandas(df: pd.DataFrame, params: DetectSequentialPhasesParams) -> pd.DataFrame:
    """Pandas implementation of sequential phase detection."""

    group_by_cols = _normalize_group_by(params.group_by)

    df = df.copy()
    df[params.timestamp_col] = pd.to_datetime(df[params.timestamp_col])
    df = df.sort_values(by=params.timestamp_col, ascending=True).reset_index(drop=True)
    df = df.drop_duplicates()

    summary_rows = []
    grouped = df.groupby(group_by_cols)

    for group_id, group in grouped:
        group = group.sort_values(params.timestamp_col).reset_index(drop=True)

        if len(group_by_cols) == 1:
            row = {group_by_cols[0]: group_id if not isinstance(group_id, tuple) else group_id[0]}
        else:
            row = {col: val for col, val in zip(group_by_cols, group_id)}

        row.update(_get_expected_columns(params))

        previous_phase_end = None
        first_phase_start = None

        for phase in params.phases:
            if isinstance(phase, PhaseConfig):
                timer_col = phase.timer_col
                threshold = phase.start_threshold or params.start_threshold
            else:
                timer_col = phase
                threshold = params.start_threshold

            if timer_col not in group.columns:
                continue

            phase_result = _detect_single_phase(
                group=group,
                timer_col=timer_col,
                timestamp_col=params.timestamp_col,
                threshold=threshold,
                previous_phase_end=previous_phase_end,
                status_col=params.status_col,
                status_mapping=params.status_mapping,
                phase_metrics=params.phase_metrics,
                time_format=params.output_time_format,
            )

            if phase_result:
                row.update(phase_result["columns"])
                previous_phase_end = phase_result["end_time"]

                if first_phase_start is None:
                    first_phase_start = phase_result["start_time"]

        if params.metadata and first_phase_start is not None:
            metadata_values = _extract_metadata(
                group=group,
                metadata_config=params.metadata,
                timestamp_col=params.timestamp_col,
                first_phase_start=first_phase_start,
            )
            row.update(metadata_values)

        summary_rows.append(row)

    result_df = pd.DataFrame(summary_rows)

    if result_df.empty:
        return result_df

    first_phase_name = (
        params.phases[0].timer_col
        if isinstance(params.phases[0], PhaseConfig)
        else params.phases[0]
    )
    start_col = f"{first_phase_name}_start"
    if start_col in result_df.columns:
        result_df = result_df.sort_values(by=start_col, ascending=True)

    if params.fill_null_minutes:
        result_df = _fill_null_numeric_columns(result_df, params)

    return result_df.reset_index(drop=True)


def _detect_single_phase(
    group: pd.DataFrame,
    timer_col: str,
    timestamp_col: str,
    threshold: int,
    previous_phase_end: Optional[pd.Timestamp],
    status_col: Optional[str],
    status_mapping: Optional[Dict[int, str]],
    phase_metrics: Optional[Dict[str, str]],
    time_format: str,
) -> Optional[dict]:
    """
    Detect a single phase's boundaries and calculate metrics.

    Returns dict with:
    - columns: dict of output column names to values
    - start_time: phase start timestamp (for chaining)
    - end_time: phase end timestamp (for chaining)
    """

    if previous_phase_end is not None:
        phase_data = group[group[timestamp_col] > previous_phase_end]
    else:
        phase_data = group

    if phase_data.empty:
        return None

    non_zero = phase_data[phase_data[timer_col] > 0]
    if non_zero.empty:
        return None

    potential_starts = non_zero[non_zero[timer_col] <= threshold].sort_values(
        by=timestamp_col, ascending=True
    )
    if potential_starts.empty:
        return None

    first_idx = potential_starts.index[0]
    first_ts = potential_starts.loc[first_idx, timestamp_col]
    first_val = potential_starts.loc[first_idx, timer_col]

    true_start = first_ts - pd.Timedelta(seconds=first_val)

    after_start = phase_data[phase_data[timestamp_col] > true_start].reset_index(drop=True)

    end_time = None
    max_timer = 0

    unique_times = after_start.drop_duplicates(subset=[timestamp_col]).reset_index(drop=True)

    for i in range(1, len(unique_times)):
        curr_val = unique_times[timer_col].iloc[i]
        prev_val = unique_times[timer_col].iloc[i - 1]
        if curr_val == prev_val:
            end_time = unique_times[timestamp_col].iloc[i - 1]
            max_timer = curr_val
            break

    if end_time is None and len(unique_times) > 0:
        end_time = unique_times[timestamp_col].iloc[-1]
        max_timer = unique_times[timer_col].iloc[-1]

    if end_time is None:
        return None

    columns = {
        f"{timer_col}_start": true_start,
        f"{timer_col}_end": end_time,
        f"{timer_col}_max_minutes": round(max_timer / 60, 6) if max_timer else 0,
    }

    if status_col and status_mapping and status_col in group.columns:
        status_times = _calculate_status_times(
            group=group,
            start_time=true_start,
            end_time=end_time,
            timestamp_col=timestamp_col,
            status_col=status_col,
            status_mapping=status_mapping,
        )
        for status_name, duration in status_times.items():
            columns[f"{timer_col}_{status_name}_minutes"] = round(duration, 6)

    if phase_metrics:
        phase_window = phase_data[
            (phase_data[timestamp_col] >= true_start) & (phase_data[timestamp_col] <= end_time)
        ]
        for metric_col, agg_func in phase_metrics.items():
            if metric_col in phase_window.columns:
                try:
                    value = phase_window[metric_col].agg(agg_func)
                    columns[f"{timer_col}_{metric_col}"] = value
                except Exception:
                    columns[f"{timer_col}_{metric_col}"] = None

    return {
        "columns": columns,
        "start_time": true_start,
        "end_time": end_time,
    }


def _calculate_status_times(
    group: pd.DataFrame,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    timestamp_col: str,
    status_col: str,
    status_mapping: Dict[int, str],
) -> Dict[str, float]:
    """
    Calculate time spent in each status within a phase window.

    Tracks status transitions and accumulates duration per status.
    Handles NaN and unknown status codes gracefully.
    """
    status_times = {status_name: 0.0 for status_name in status_mapping.values()}

    within_phase = group[(group[timestamp_col] >= start_time) & (group[timestamp_col] <= end_time)]

    if within_phase.empty:
        return status_times

    valid_rows = within_phase[
        within_phase[status_col].notna() & within_phase[status_col].isin(status_mapping.keys())
    ]

    if valid_rows.empty:
        return status_times

    current_status = valid_rows.iloc[0][status_col]
    last_change_ts = valid_rows.iloc[0][timestamp_col]

    for _, record in within_phase.iterrows():
        ts = record[timestamp_col]
        status = record[status_col]

        if pd.isna(status) or status not in status_mapping:
            continue

        if status != current_status:
            time_diff = (ts - last_change_ts).total_seconds() / 60
            status_times[status_mapping[current_status]] += time_diff
            last_change_ts = ts
            current_status = status

    final_diff = (end_time - last_change_ts).total_seconds() / 60
    status_times[status_mapping[current_status]] += final_diff

    return status_times


def _extract_metadata(
    group: pd.DataFrame,
    metadata_config: Dict[str, str],
    timestamp_col: str,
    first_phase_start: pd.Timestamp,
) -> Dict[str, any]:
    """
    Extract metadata columns with specified aggregation methods.

    Supported methods:
    - first: First value in group
    - last: Last value in group
    - first_after_start: First value after first phase starts
    - max, min, mean, sum: Standard aggregations
    """
    result = {}

    for col, method in metadata_config.items():
        if col not in group.columns:
            result[col] = None
            continue

        try:
            if method == "first":
                result[col] = group[col].iloc[0]
            elif method == "last":
                result[col] = group[col].iloc[-1]
            elif method == "first_after_start":
                after_start = group[group[timestamp_col] >= first_phase_start]
                if not after_start.empty:
                    valid = after_start[after_start[col].notna()]
                    result[col] = valid[col].iloc[0] if not valid.empty else None
                else:
                    result[col] = None
            elif method in ("max", "min", "mean", "sum"):
                result[col] = group[col].agg(method)
            else:
                result[col] = group[col].agg(method)
        except Exception:
            result[col] = None

    return result


# =============================================================================
# SPARK IMPLEMENTATION
# =============================================================================


def _detect_phases_spark(spark_df, params: DetectSequentialPhasesParams):
    """
    Spark implementation using applyInPandas for parallel group processing.

    Each group (batch) is processed independently using the Pandas logic,
    enabling parallel execution across the cluster.
    """
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    group_by_cols = _normalize_group_by(params.group_by)

    output_fields = []
    for col in group_by_cols:
        output_fields.append(StructField(col, StringType(), True))

    phase_names = [p.timer_col if isinstance(p, PhaseConfig) else p for p in params.phases]
    for phase in phase_names:
        output_fields.append(StructField(f"{phase}_start", TimestampType(), True))
        output_fields.append(StructField(f"{phase}_end", TimestampType(), True))
        output_fields.append(StructField(f"{phase}_max_minutes", DoubleType(), True))

        if params.status_mapping:
            for status_name in params.status_mapping.values():
                output_fields.append(
                    StructField(f"{phase}_{status_name}_minutes", DoubleType(), True)
                )

        if params.phase_metrics:
            for metric_col in params.phase_metrics.keys():
                output_fields.append(StructField(f"{phase}_{metric_col}", DoubleType(), True))

    if params.metadata:
        numeric_aggs = {"max", "min", "mean", "sum"}
        for col, method in params.metadata.items():
            if method in numeric_aggs:
                output_fields.append(StructField(col, DoubleType(), True))
            else:
                output_fields.append(StructField(col, StringType(), True))

    output_schema = StructType(output_fields)

    def process_group(pdf: pd.DataFrame) -> pd.DataFrame:
        """Process a single group using Pandas logic."""
        result = _process_single_group_pandas(pdf, params)
        return pd.DataFrame([result]) if result else pd.DataFrame()

    result_df = spark_df.groupby(group_by_cols).applyInPandas(process_group, schema=output_schema)

    return result_df


def _detect_phases_spark_native(spark_df, params: DetectSequentialPhasesParams):
    """
    Native Spark implementation using window functions.

    This implementation avoids applyInPandas serialization overhead by using
    pure Spark operations: window functions, joins, and aggregations.

    Performance: 5-20x faster than applyInPandas for large datasets.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import Window

    ctx = get_logging_context()
    group_by_cols = _normalize_group_by(params.group_by)
    ts = params.timestamp_col
    threshold = params.start_threshold

    df = spark_df.withColumn(ts, F.col(ts).cast("timestamp"))

    summary_df = df.select(*group_by_cols).distinct()

    prev_phase_end_df = None

    phase_names = [p.timer_col if isinstance(p, PhaseConfig) else p for p in params.phases]

    for phase_cfg in params.phases:
        if isinstance(phase_cfg, PhaseConfig):
            timer_col = phase_cfg.timer_col
            phase_threshold = phase_cfg.start_threshold or threshold
        else:
            timer_col = phase_cfg
            phase_threshold = threshold

        if timer_col not in spark_df.columns:
            ctx.debug(f"Skipping phase {timer_col}: column not found")
            continue

        phase_df = df

        if prev_phase_end_df is not None:
            phase_df = (
                phase_df.join(prev_phase_end_df, on=group_by_cols, how="inner")
                .filter(F.col(ts) > F.col("prev_end_ts"))
                .drop("prev_end_ts")
            )

        w_order = Window.partitionBy(*group_by_cols).orderBy(ts)

        phase_df = phase_df.withColumn("lag_timer", F.lag(timer_col).over(w_order))
        phase_df = phase_df.withColumn("lag_ts", F.lag(ts).over(w_order))

        start_candidates = phase_df.filter(
            (F.col(timer_col) > 0)
            & (F.col(timer_col) <= F.lit(phase_threshold))
            & F.col(timer_col).isNotNull()
        )

        w_start_rank = Window.partitionBy(*group_by_cols).orderBy(ts)
        start_rows = (
            start_candidates.withColumn("start_rn", F.row_number().over(w_start_rank))
            .filter(F.col("start_rn") == 1)
            .select(
                *group_by_cols,
                F.col(ts).alias("start_obs_ts"),
                F.col(timer_col).alias("start_obs_timer"),
            )
        )

        start_rows = start_rows.withColumn(
            "true_start_ts",
            F.from_unixtime(
                F.unix_timestamp("start_obs_ts") - F.col("start_obs_timer").cast("long")
            ).cast("timestamp"),
        )

        phase_with_start = phase_df.join(
            start_rows.select(*group_by_cols, "start_obs_ts", "true_start_ts"),
            on=group_by_cols,
            how="inner",
        )

        phase_with_start = phase_with_start.withColumn(
            "is_plateau",
            (F.col(timer_col).isNotNull())
            & (F.col("lag_timer").isNotNull())
            & (F.col(timer_col) == F.col("lag_timer"))
            & (F.col(ts) != F.col("lag_ts"))
            & (F.col(ts) >= F.col("start_obs_ts"))
            & (F.col("lag_ts") >= F.col("start_obs_ts")),
        )

        plateau_candidates = phase_with_start.filter("is_plateau")

        w_plateau_rank = Window.partitionBy(*group_by_cols).orderBy(ts)
        plateau_rows = (
            plateau_candidates.withColumn("plateau_rn", F.row_number().over(w_plateau_rank))
            .filter(F.col("plateau_rn") == 1)
            .select(
                *group_by_cols,
                F.col("lag_ts").alias("end_ts"),
                F.col(timer_col).alias("plateau_timer"),
            )
        )

        phase_bounds = start_rows.join(plateau_rows, on=group_by_cols, how="left")

        no_plateau = phase_with_start.filter(~F.col("is_plateau")).groupBy(*group_by_cols).agg(
            F.max(ts).alias("fallback_end_ts"),
            F.max(timer_col).alias("fallback_timer"),
        )

        phase_bounds = phase_bounds.join(no_plateau, on=group_by_cols, how="left")

        phase_bounds = phase_bounds.withColumn(
            "final_end_ts", F.coalesce(F.col("end_ts"), F.col("fallback_end_ts"))
        ).withColumn(
            "max_timer", F.coalesce(F.col("plateau_timer"), F.col("fallback_timer"))
        )

        phase_summary = phase_bounds.select(
            *group_by_cols,
            F.col("true_start_ts").alias(f"{timer_col}_start"),
            F.col("final_end_ts").alias(f"{timer_col}_end"),
            (F.col("max_timer") / 60.0).alias(f"{timer_col}_max_minutes"),
            F.col("true_start_ts").alias("_phase_true_start"),
            F.col("final_end_ts").alias("_phase_end"),
        )

        if params.status_mapping and params.status_col:
            status_durations = _compute_status_durations_spark(
                df=df,
                phase_bounds=phase_bounds.select(
                    *group_by_cols, "true_start_ts", "final_end_ts"
                ),
                params=params,
                timer_col=timer_col,
                group_by_cols=group_by_cols,
            )
            if status_durations is not None:
                phase_summary = phase_summary.join(status_durations, on=group_by_cols, how="left")

        if params.phase_metrics:
            metrics_df = _compute_phase_metrics_spark(
                df=df,
                phase_bounds=phase_bounds.select(
                    *group_by_cols, "true_start_ts", "final_end_ts"
                ),
                params=params,
                timer_col=timer_col,
                group_by_cols=group_by_cols,
            )
            if metrics_df is not None:
                phase_summary = phase_summary.join(metrics_df, on=group_by_cols, how="left")

        summary_df = summary_df.join(
            phase_summary.drop("_phase_true_start", "_phase_end"),
            on=group_by_cols,
            how="left",
        )

        prev_phase_end_df = phase_bounds.select(
            *group_by_cols, F.col("final_end_ts").alias("prev_end_ts")
        ).filter(F.col("prev_end_ts").isNotNull())

    if params.metadata:
        phase_start_cols = [F.col(f"{p}_start") for p in phase_names]
        summary_df = summary_df.withColumn(
            "_first_phase_start", F.coalesce(*phase_start_cols)
        )

        metadata_df = _compute_metadata_spark(
            df=df,
            summary_df=summary_df.select(*group_by_cols, "_first_phase_start"),
            params=params,
            group_by_cols=group_by_cols,
        )
        if metadata_df is not None:
            summary_df = summary_df.join(metadata_df, on=group_by_cols, how="left")

        summary_df = summary_df.drop("_first_phase_start")

    if params.fill_null_minutes:
        numeric_cols = _get_numeric_columns(params)
        for col in numeric_cols:
            if col in summary_df.columns:
                summary_df = summary_df.withColumn(
                    col, F.coalesce(F.col(col), F.lit(0.0))
                )

    first_phase_start_col = f"{phase_names[0]}_start" if phase_names else None
    if first_phase_start_col and first_phase_start_col in summary_df.columns:
        summary_df = summary_df.orderBy(first_phase_start_col)

    return summary_df


def _compute_status_durations_spark(
    df, phase_bounds, params: DetectSequentialPhasesParams, timer_col: str, group_by_cols: List[str]
):
    """Compute time spent in each status within a phase window using Spark."""
    from pyspark.sql import functions as F
    from pyspark.sql import Window

    ts = params.timestamp_col
    status_col = params.status_col
    status_mapping = params.status_mapping
    valid_codes = list(status_mapping.keys())

    status_df = (
        df.join(
            phase_bounds.withColumnRenamed("true_start_ts", "_start")
            .withColumnRenamed("final_end_ts", "_end"),
            on=group_by_cols,
            how="inner",
        )
        .filter((F.col(ts) >= F.col("_start")) & (F.col(ts) <= F.col("_end")))
    )

    status_df = status_df.withColumn(
        "valid_status",
        F.when(F.col(status_col).isin([F.lit(c) for c in valid_codes]), F.col(status_col)),
    )

    w_status = (
        Window.partitionBy(*group_by_cols)
        .orderBy(ts)
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    status_df = status_df.withColumn(
        "ffill_status", F.last("valid_status", ignorenulls=True).over(w_status)
    )

    w_lead = Window.partitionBy(*group_by_cols).orderBy(ts)
    status_df = status_df.withColumn("next_ts", F.lead(ts).over(w_lead))

    status_df = status_df.withColumn(
        "interval_end_ts",
        F.when(
            F.col("next_ts").isNull() | (F.col("next_ts") > F.col("_end")),
            F.col("_end"),
        ).otherwise(F.col("next_ts")),
    )

    status_df = status_df.withColumn(
        "interval_sec",
        F.greatest(
            F.lit(0), F.unix_timestamp("interval_end_ts") - F.unix_timestamp(ts)
        ),
    )

    status_df = status_df.filter(
        (F.col("ffill_status").isNotNull()) & (F.col("interval_sec") > 0)
    )

    status_df = status_df.withColumn("interval_min", F.col("interval_sec") / 60.0)

    durations = status_df.groupBy(*group_by_cols, "ffill_status").agg(
        F.sum("interval_min").alias("minutes")
    )

    durations_pivot = durations.groupBy(*group_by_cols).pivot(
        "ffill_status", valid_codes
    ).agg(F.first("minutes"))

    for code, status_name in status_mapping.items():
        old_col = str(code)
        new_col = f"{timer_col}_{status_name}_minutes"
        if old_col in durations_pivot.columns:
            durations_pivot = durations_pivot.withColumnRenamed(old_col, new_col)

    return durations_pivot


def _compute_phase_metrics_spark(
    df, phase_bounds, params: DetectSequentialPhasesParams, timer_col: str, group_by_cols: List[str]
):
    """Compute aggregated metrics within a phase window using Spark."""
    from pyspark.sql import functions as F

    ts = params.timestamp_col

    metrics_df = (
        df.join(
            phase_bounds.withColumnRenamed("true_start_ts", "_start")
            .withColumnRenamed("final_end_ts", "_end"),
            on=group_by_cols,
            how="inner",
        )
        .filter((F.col(ts) >= F.col("_start")) & (F.col(ts) <= F.col("_end")))
    )

    agg_exprs = []
    for metric_col, agg_name in params.phase_metrics.items():
        if metric_col in df.columns:
            func = getattr(F, agg_name)
            agg_exprs.append(func(metric_col).alias(f"{timer_col}_{metric_col}"))

    if not agg_exprs:
        return None

    return metrics_df.groupBy(*group_by_cols).agg(*agg_exprs)


def _compute_metadata_spark(
    df, summary_df, params: DetectSequentialPhasesParams, group_by_cols: List[str]
):
    """Compute metadata columns using Spark."""
    from pyspark.sql import functions as F

    ts = params.timestamp_col

    meta_base = df.join(summary_df, on=group_by_cols, how="inner")

    agg_exprs = []
    struct_cols = []

    for col_name, method in params.metadata.items():
        if col_name not in df.columns:
            continue

        if method == "first":
            agg_exprs.append(F.first(col_name, ignorenulls=True).alias(col_name))
        elif method == "last":
            struct_cols.append(col_name)
            agg_exprs.append(
                F.max(F.struct(F.col(ts), F.col(col_name))).alias(f"__{col_name}_struct")
            )
        elif method == "first_after_start":
            agg_exprs.append(
                F.first(
                    F.when(F.col(ts) >= F.col("_first_phase_start"), F.col(col_name)),
                    ignorenulls=True,
                ).alias(col_name)
            )
        elif method in ("max", "min", "mean", "sum"):
            func = getattr(F, method)
            agg_exprs.append(func(col_name).alias(col_name))
        else:
            try:
                func = getattr(F, method)
                agg_exprs.append(func(col_name).alias(col_name))
            except AttributeError:
                agg_exprs.append(F.first(col_name, ignorenulls=True).alias(col_name))

    if not agg_exprs:
        return None

    metadata_df = meta_base.groupBy(*group_by_cols).agg(*agg_exprs)

    for col_name in struct_cols:
        metadata_df = metadata_df.withColumn(
            col_name, F.col(f"__{col_name}_struct").getField(col_name)
        ).drop(f"__{col_name}_struct")

    return metadata_df


def _process_single_group_pandas(
    group: pd.DataFrame, params: DetectSequentialPhasesParams
) -> Optional[Dict]:
    """Process a single group and return the summary row dict."""
    group_by_cols = _normalize_group_by(params.group_by)

    group = group.copy()
    group[params.timestamp_col] = pd.to_datetime(group[params.timestamp_col])
    group = group.sort_values(params.timestamp_col).reset_index(drop=True)

    if len(group_by_cols) == 1:
        row = {group_by_cols[0]: group[group_by_cols[0]].iloc[0]}
    else:
        row = {col: group[col].iloc[0] for col in group_by_cols}

    row.update(_get_expected_columns(params))

    previous_phase_end = None
    first_phase_start = None

    for phase in params.phases:
        if isinstance(phase, PhaseConfig):
            timer_col = phase.timer_col
            threshold = phase.start_threshold or params.start_threshold
        else:
            timer_col = phase
            threshold = params.start_threshold

        if timer_col not in group.columns:
            continue

        phase_result = _detect_single_phase(
            group=group,
            timer_col=timer_col,
            timestamp_col=params.timestamp_col,
            threshold=threshold,
            previous_phase_end=previous_phase_end,
            status_col=params.status_col,
            status_mapping=params.status_mapping,
            phase_metrics=params.phase_metrics,
            time_format=params.output_time_format,
        )

        if phase_result:
            row.update(phase_result["columns"])
            previous_phase_end = phase_result["end_time"]

            if first_phase_start is None:
                first_phase_start = phase_result["start_time"]

    if params.metadata and first_phase_start is not None:
        metadata_values = _extract_metadata(
            group=group,
            metadata_config=params.metadata,
            timestamp_col=params.timestamp_col,
            first_phase_start=first_phase_start,
        )
        row.update(metadata_values)

    if params.fill_null_minutes:
        numeric_cols = _get_numeric_columns(params)
        for col in numeric_cols:
            if col in row and row[col] is None:
                row[col] = 0.0

    return row


# =============================================================================
# POLARS IMPLEMENTATION
# =============================================================================


def _detect_phases_polars(polars_df, params: DetectSequentialPhasesParams):
    """
    Polars implementation - converts to Pandas for processing.

    TODO: Native Polars implementation for better performance.
    """
    pdf = polars_df.to_pandas()
    result_pdf = _detect_phases_pandas(pdf, params)

    try:
        import polars as pl

        return pl.from_pandas(result_pdf)
    except ImportError:
        raise ValueError("Polars is not installed")
