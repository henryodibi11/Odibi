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


def _normalize_group_by(group_by: Union[str, List[str]]) -> List[str]:
    """Convert group_by to list format."""
    if isinstance(group_by, str):
        return [group_by]
    return list(group_by)


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
        result_df = _detect_phases_spark(context.df, params)
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

    for i in range(1, len(after_start)):
        curr_val = after_start[timer_col].iloc[i]
        prev_val = after_start[timer_col].iloc[i - 1]
        if curr_val == prev_val:
            end_time = after_start[timestamp_col].iloc[i - 1]
            max_timer = curr_val
            break

    if end_time is None and len(after_start) > 0:
        end_time = after_start[timestamp_col].iloc[-1]
        max_timer = after_start[timer_col].iloc[-1]

    if end_time is None:
        return None

    columns = {
        f"{timer_col}_start": true_start.strftime(time_format),
        f"{timer_col}_end": end_time.strftime(time_format),
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
    )

    group_by_cols = _normalize_group_by(params.group_by)

    output_fields = []
    for col in group_by_cols:
        output_fields.append(StructField(col, StringType(), True))

    phase_names = [p.timer_col if isinstance(p, PhaseConfig) else p for p in params.phases]
    for phase in phase_names:
        output_fields.append(StructField(f"{phase}_start", StringType(), True))
        output_fields.append(StructField(f"{phase}_end", StringType(), True))
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
        for col in params.metadata.keys():
            output_fields.append(StructField(col, StringType(), True))

    output_schema = StructType(output_fields)

    def process_group(pdf: pd.DataFrame) -> pd.DataFrame:
        """Process a single group using Pandas logic."""
        result = _process_single_group_pandas(pdf, params)
        return pd.DataFrame([result]) if result else pd.DataFrame()

    result_df = spark_df.groupby(group_by_cols).applyInPandas(process_group, schema=output_schema)

    return result_df


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
