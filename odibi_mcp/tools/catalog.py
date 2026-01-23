# odibi_mcp/tools/catalog.py
"""Catalog-related MCP tools - wired to real catalog tables."""

import logging
from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime, timedelta

from odibi_mcp.contracts.time import TimeWindow
from odibi_mcp.contracts.stats import NodeStatsResponse
from odibi_mcp.contracts.schema import SchemaChange
from odibi_mcp.context import get_project_context

logger = logging.getLogger(__name__)


@dataclass
class PipelineStatsResponse:
    """Pipeline-level statistics."""

    pipeline: str
    window: TimeWindow
    run_count: int
    success_count: int
    failure_count: int
    avg_duration_seconds: float


@dataclass
class FailureRecord:
    """Single failure record."""

    pipeline: str
    node: str
    error_type: str
    error_message: str
    timestamp: datetime
    run_id: str


@dataclass
class FailureSummaryResponse:
    """Summary of failures."""

    window: TimeWindow
    total_failures: int
    failures: List[FailureRecord]
    by_error_type: dict


@dataclass
class SchemaHistoryResponse:
    """Schema change history."""

    pipeline: str
    node: str
    changes: List[SchemaChange]


def _get_catalog_connection():
    """Get the catalog SQL connection if available."""
    ctx = get_project_context()
    if not ctx or not ctx.catalog_connection:
        return None

    try:
        conn = ctx.get_connection(ctx.catalog_connection)
        if hasattr(conn, "execute_query"):
            return conn
    except Exception as e:
        logger.warning(f"Could not get catalog connection: {e}")

    return None


def node_stats(
    pipeline: str,
    node: str,
    window: Optional[TimeWindow] = None,
) -> NodeStatsResponse:
    """
    Get statistics for a specific node.

    Queries the catalog tables if available.
    """
    if window is None:
        now = datetime.now()
        window = TimeWindow(start=now - timedelta(days=7), end=now)

    conn = _get_catalog_connection()

    if not conn:
        return NodeStatsResponse(
            pipeline=pipeline,
            node=node,
            window=window,
            run_count=0,
            success_count=0,
            failure_count=0,
            failure_rate=0.0,
            avg_duration_seconds=0.0,
            duration_trend=[],
            row_count_trend=[],
        )

    try:
        start_str = window.start.strftime("%Y-%m-%d %H:%M:%S") if window.start else ""
        end_str = window.end.strftime("%Y-%m-%d %H:%M:%S") if window.end else ""

        query = f"""
        SELECT
            COUNT(*) as run_count,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN status IN ('failed', 'error') THEN 1 ELSE 0 END) as failure_count,
            AVG(duration_seconds) as avg_duration
        FROM meta_node_runs
        WHERE pipeline_name = '{pipeline}'
        AND node_name = '{node}'
        AND start_time >= '{start_str}'
        AND start_time < '{end_str}'
        """

        result = conn.execute_query(query)

        if result and len(result) > 0:
            row = result[0]
            if isinstance(row, tuple):
                run_count, success_count, failure_count, avg_duration = row
            else:
                run_count = row.get("run_count", 0)
                success_count = row.get("success_count", 0)
                failure_count = row.get("failure_count", 0)
                avg_duration = row.get("avg_duration", 0)

            run_count = run_count or 0
            success_count = success_count or 0
            failure_count = failure_count or 0
            avg_duration = avg_duration or 0.0

            failure_rate = failure_count / run_count if run_count > 0 else 0.0

            return NodeStatsResponse(
                pipeline=pipeline,
                node=node,
                window=window,
                run_count=run_count,
                success_count=success_count,
                failure_count=failure_count,
                failure_rate=failure_rate,
                avg_duration_seconds=avg_duration,
                duration_trend=[],
                row_count_trend=[],
            )

    except Exception:
        logger.exception(f"Error querying node stats: {pipeline}/{node}")

    return NodeStatsResponse(
        pipeline=pipeline,
        node=node,
        window=window,
        run_count=0,
        success_count=0,
        failure_count=0,
        failure_rate=0.0,
        avg_duration_seconds=0.0,
        duration_trend=[],
        row_count_trend=[],
    )


def pipeline_stats(
    pipeline: str,
    window: Optional[TimeWindow] = None,
) -> PipelineStatsResponse:
    """
    Get statistics for a pipeline.

    Queries the catalog tables if available.
    """
    if window is None:
        now = datetime.now()
        window = TimeWindow(start=now - timedelta(days=7), end=now)

    conn = _get_catalog_connection()

    if not conn:
        return PipelineStatsResponse(
            pipeline=pipeline,
            window=window,
            run_count=0,
            success_count=0,
            failure_count=0,
            avg_duration_seconds=0.0,
        )

    try:
        start_str = window.start.strftime("%Y-%m-%d %H:%M:%S") if window.start else ""
        end_str = window.end.strftime("%Y-%m-%d %H:%M:%S") if window.end else ""

        query = f"""
        SELECT
            COUNT(*) as run_count,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN status IN ('failed', 'error') THEN 1 ELSE 0 END) as failure_count,
            AVG(duration_seconds) as avg_duration
        FROM meta_pipeline_runs
        WHERE pipeline_name = '{pipeline}'
        AND start_time >= '{start_str}'
        AND start_time < '{end_str}'
        """

        result = conn.execute_query(query)

        if result and len(result) > 0:
            row = result[0]
            if isinstance(row, tuple):
                run_count, success_count, failure_count, avg_duration = row
            else:
                run_count = row.get("run_count", 0)
                success_count = row.get("success_count", 0)
                failure_count = row.get("failure_count", 0)
                avg_duration = row.get("avg_duration", 0)

            return PipelineStatsResponse(
                pipeline=pipeline,
                window=window,
                run_count=run_count or 0,
                success_count=success_count or 0,
                failure_count=failure_count or 0,
                avg_duration_seconds=avg_duration or 0.0,
            )

    except Exception:
        logger.exception(f"Error querying pipeline stats: {pipeline}")

    return PipelineStatsResponse(
        pipeline=pipeline,
        window=window,
        run_count=0,
        success_count=0,
        failure_count=0,
        avg_duration_seconds=0.0,
    )


def failure_summary(
    pipeline: Optional[str] = None,
    window: Optional[TimeWindow] = None,
    max_failures: int = 100,
) -> FailureSummaryResponse:
    """
    Get summary of failures, optionally filtered by pipeline.

    Queries the catalog tables if available.
    """
    if window is None:
        now = datetime.now()
        window = TimeWindow(start=now - timedelta(days=7), end=now)

    conn = _get_catalog_connection()

    if not conn:
        return FailureSummaryResponse(
            window=window,
            total_failures=0,
            failures=[],
            by_error_type={},
        )

    try:
        start_str = window.start.strftime("%Y-%m-%d %H:%M:%S") if window.start else ""
        end_str = window.end.strftime("%Y-%m-%d %H:%M:%S") if window.end else ""

        pipeline_filter = f"AND pipeline_name = '{pipeline}'" if pipeline else ""

        query = f"""
        SELECT TOP {max_failures}
            pipeline_name,
            node_name,
            error_type,
            error_message,
            start_time,
            run_id
        FROM meta_node_runs
        WHERE status IN ('failed', 'error')
        AND start_time >= '{start_str}'
        AND start_time < '{end_str}'
        {pipeline_filter}
        ORDER BY start_time DESC
        """

        result = conn.execute_query(query)

        failures = []
        by_error_type = {}

        for row in result:
            if isinstance(row, tuple):
                p_name, n_name, e_type, e_msg, ts, r_id = row
            else:
                p_name = row.get("pipeline_name", "unknown")
                n_name = row.get("node_name", "unknown")
                e_type = row.get("error_type", "unknown")
                e_msg = row.get("error_message", "")
                ts = row.get("start_time")
                r_id = row.get("run_id", "")

            failures.append(
                FailureRecord(
                    pipeline=p_name,
                    node=n_name,
                    error_type=e_type or "unknown",
                    error_message=e_msg or "",
                    timestamp=ts,
                    run_id=r_id or "",
                )
            )

            error_key = e_type or "unknown"
            by_error_type[error_key] = by_error_type.get(error_key, 0) + 1

        return FailureSummaryResponse(
            window=window,
            total_failures=len(failures),
            failures=failures,
            by_error_type=by_error_type,
        )

    except Exception:
        logger.exception("Error querying failure summary")

    return FailureSummaryResponse(
        window=window,
        total_failures=0,
        failures=[],
        by_error_type={},
    )


def schema_history(
    pipeline: str,
    node: str,
    max_changes: int = 50,
) -> SchemaHistoryResponse:
    """
    Get schema change history for a node.

    Queries the catalog tables if available.
    """
    conn = _get_catalog_connection()

    if not conn:
        return SchemaHistoryResponse(
            pipeline=pipeline,
            node=node,
            changes=[],
        )

    try:
        query = f"""
        SELECT TOP {max_changes}
            run_id,
            change_timestamp,
            added_columns,
            removed_columns,
            type_changes
        FROM meta_schema_changes
        WHERE pipeline_name = '{pipeline}'
        AND node_name = '{node}'
        ORDER BY change_timestamp DESC
        """

        result = conn.execute_query(query)

        changes = []
        for row in result:
            if isinstance(row, tuple):
                r_id, ts, added, removed, type_chg = row
            else:
                r_id = row.get("run_id", "")
                ts = row.get("change_timestamp")
                added = row.get("added_columns", [])
                removed = row.get("removed_columns", [])
                type_chg = row.get("type_changes", [])

            changes.append(
                SchemaChange(
                    run_id=r_id,
                    timestamp=ts,
                    added_columns=added if isinstance(added, list) else [],
                    removed_columns=removed if isinstance(removed, list) else [],
                    type_changes=type_chg if isinstance(type_chg, list) else [],
                )
            )

        return SchemaHistoryResponse(
            pipeline=pipeline,
            node=node,
            changes=changes,
        )

    except Exception:
        logger.exception(f"Error querying schema history: {pipeline}/{node}")

    return SchemaHistoryResponse(
        pipeline=pipeline,
        node=node,
        changes=[],
    )
