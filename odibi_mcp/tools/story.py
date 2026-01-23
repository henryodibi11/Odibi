# odibi_mcp/tools/story.py
"""Story-related MCP tools - wired to real story files (local and cloud)."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Optional

import fsspec

from odibi_mcp.contracts.selectors import RunSelector, DEFAULT_RUN_SELECTOR
from odibi_mcp.contracts.diff import DiffSummary
from odibi_mcp.contracts.schema import SchemaChange
from odibi_mcp.context import get_project_context

logger = logging.getLogger(__name__)


def _get_story_fs_and_path(
    ctx,
) -> tuple[Optional[fsspec.AbstractFileSystem], Optional[str], Optional[dict]]:
    """Get filesystem and base path for stories (supports local and cloud)."""
    if not ctx or not ctx.story_connection or not ctx.story_path:
        return None, None, None

    try:
        conn = ctx.get_connection(ctx.story_connection)

        # Check if it's a cloud connection with storage options
        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()
            # Build the container/path for ADLS
            # The container is part of the connection config
            container = getattr(conn, "container", None)
            if container:
                base_path = f"{container}/{ctx.story_path.rstrip('/')}"
                fs = fsspec.filesystem("abfs", **storage_options)
                return fs, base_path, storage_options

        # Fall back to local filesystem
        local_path = conn.get_path(ctx.story_path)
        fs = fsspec.filesystem("file")
        return fs, local_path, None

    except Exception as e:
        logger.warning(f"Could not get story filesystem: {e}")
        return None, None, None


@dataclass
class StoryReadResult:
    pipeline: str
    status: str
    duration_seconds: float
    run_id: Optional[str]
    start_time: Optional[str]
    end_time: Optional[str]
    nodes: list
    node_count: int
    success_count: int
    failure_count: int
    error_message: Optional[str]


@dataclass
class NodeDescribeResult:
    pipeline: str
    node: str
    operation: str
    inputs: list
    outputs: list
    transform_steps: list
    validations: list
    duration: float
    row_count_in: Optional[int]
    row_count_out: Optional[int]
    status: str
    error: Optional[str]


def _find_story_file(
    pipeline: str, run_selector: Optional[RunSelector] = None
) -> tuple[Optional[fsspec.AbstractFileSystem], Optional[str]]:
    """Find the story file for a pipeline run. Returns (filesystem, path) tuple."""
    ctx = get_project_context()
    if not ctx:
        return None, None

    fs, story_base, _ = _get_story_fs_and_path(ctx)
    if not fs or not story_base:
        logger.warning("Story filesystem not available")
        return None, None

    # Check if base path exists
    try:
        if not fs.exists(story_base):
            logger.warning(f"Story base path not found: {story_base}")
            return None, None
    except Exception as e:
        logger.warning(f"Error checking story base: {e}")
        return None, None

    # Look for story files matching the pipeline
    # Stories are at: stories/{pipeline}/{date}/run_{time}.json
    pipeline_dir = f"{story_base}/{pipeline}"

    try:
        if fs.exists(pipeline_dir) and fs.isdir(pipeline_dir):
            # Find all .json files recursively
            story_files = []
            for date_dir in fs.ls(pipeline_dir):
                if fs.isdir(date_dir):
                    for f in fs.ls(date_dir):
                        if f.endswith(".json"):
                            story_files.append(f)

            if story_files:
                # Sort by name (includes date/time), newest first
                story_files.sort(reverse=True)

                # If run_selector specifies an ID, find it
                if run_selector and isinstance(run_selector, dict) and run_selector.get("run_id"):
                    target_id = run_selector["run_id"]
                    for sf in story_files:
                        if target_id in sf:
                            return fs, sf

                # Return latest
                return fs, story_files[0]
    except Exception as e:
        logger.warning(f"Error finding story files: {e}")

    return None, None


def _load_story(fs: fsspec.AbstractFileSystem, story_path: str) -> dict:
    """Load story JSON file from any filesystem."""
    with fs.open(story_path, "r") as f:
        return json.load(f)


def story_read(
    pipeline: str,
    run_selector: RunSelector = DEFAULT_RUN_SELECTOR,
) -> StoryReadResult:
    """
    Read story metadata for a pipeline run.

    Loads actual story JSON from the configured story path.
    """
    fs, story_path = _find_story_file(pipeline, run_selector)

    if not fs or not story_path:
        return StoryReadResult(
            pipeline=pipeline,
            status="not_found",
            duration_seconds=0.0,
            run_id=None,
            start_time=None,
            end_time=None,
            nodes=[],
            node_count=0,
            success_count=0,
            failure_count=0,
            error_message=f"No story found for pipeline: {pipeline}",
        )

    try:
        story = _load_story(fs, story_path)

        # Extract node summaries
        nodes = []
        success_count = 0
        failure_count = 0

        for node in story.get("nodes", []):
            # Support both 'node_name' (new format) and 'name' (legacy format)
            node_name = node.get("node_name", node.get("name", "unknown"))
            node_status = node.get("status", "unknown")

            if node_status == "success":
                success_count += 1
            elif node_status in ("failed", "error"):
                failure_count += 1

            nodes.append(
                {
                    "name": node_name,
                    "status": node_status,
                    "duration": node.get("duration", 0),
                    "row_count": node.get("rows_out", node.get("row_count")),
                }
            )

        # Determine overall status from node counts
        if story.get("failed_nodes", failure_count) > 0:
            overall_status = "failed"
        elif success_count == len(nodes) and len(nodes) > 0:
            overall_status = "success"
        else:
            overall_status = story.get("status", "unknown")

        return StoryReadResult(
            pipeline=pipeline,
            status=overall_status,
            duration_seconds=story.get("duration", story.get("duration_seconds", 0)),
            run_id=story.get("run_id"),
            start_time=story.get("started_at", story.get("start_time")),
            end_time=story.get("completed_at", story.get("end_time")),
            nodes=nodes,
            node_count=len(nodes),
            success_count=success_count,
            failure_count=failure_count,
            error_message=story.get("error"),
        )

    except Exception as e:
        logger.exception(f"Error reading story: {story_path}")
        return StoryReadResult(
            pipeline=pipeline,
            status="error",
            duration_seconds=0.0,
            run_id=None,
            start_time=None,
            end_time=None,
            nodes=[],
            node_count=0,
            success_count=0,
            failure_count=0,
            error_message=str(e),
        )


def node_describe(
    pipeline: str,
    node: str,
    run_selector: RunSelector = DEFAULT_RUN_SELECTOR,
) -> NodeDescribeResult:
    """
    Describe a specific node within a pipeline run.

    Returns detailed node information from the story.
    """
    fs, story_path = _find_story_file(pipeline, run_selector)

    if not fs or not story_path:
        return NodeDescribeResult(
            pipeline=pipeline,
            node=node,
            operation="unknown",
            inputs=[],
            outputs=[],
            transform_steps=[],
            validations=[],
            duration=0.0,
            row_count_in=None,
            row_count_out=None,
            status="not_found",
            error=f"No story found for pipeline: {pipeline}",
        )

    try:
        story = _load_story(fs, story_path)

        # Find the node (support both 'node_name' and 'name' fields)
        node_data = None
        for n in story.get("nodes", []):
            n_name = n.get("node_name", n.get("name"))
            if n_name == node:
                node_data = n
                break

        if not node_data:
            return NodeDescribeResult(
                pipeline=pipeline,
                node=node,
                operation="unknown",
                inputs=[],
                outputs=[],
                transform_steps=[],
                validations=[],
                duration=0.0,
                row_count_in=None,
                row_count_out=None,
                status="not_found",
                error=f"Node not found: {node}",
            )

        return NodeDescribeResult(
            pipeline=pipeline,
            node=node,
            operation=node_data.get("operation", node_data.get("pattern", "transform")),
            inputs=node_data.get("inputs", []),
            outputs=node_data.get("outputs", []),
            transform_steps=node_data.get("transform", {}).get("steps", []),
            validations=node_data.get("validations", []),
            duration=node_data.get("duration", 0.0),
            row_count_in=node_data.get("rows_in", node_data.get("row_count_in")),
            row_count_out=node_data.get(
                "rows_out", node_data.get("row_count_out", node_data.get("row_count"))
            ),
            status=node_data.get("status", "unknown"),
            error=node_data.get("error"),
        )

    except Exception as e:
        logger.exception(f"Error reading node from story: {story_path}")
        return NodeDescribeResult(
            pipeline=pipeline,
            node=node,
            operation="unknown",
            inputs=[],
            outputs=[],
            transform_steps=[],
            validations=[],
            duration=0.0,
            row_count_in=None,
            row_count_out=None,
            status="error",
            error=str(e),
        )


def story_diff(
    pipeline: str,
    run_a: str,
    run_b: str,
    include_sample_diff: bool = False,
) -> DiffSummary:
    """
    Compute DiffSummary for two pipeline runs.
    """
    fs_a, story_a_path = _find_story_file(pipeline, {"run_id": run_a})
    fs_b, story_b_path = _find_story_file(pipeline, {"run_id": run_b})

    if not fs_a or not story_a_path or not fs_b or not story_b_path:
        return DiffSummary(
            schema_diff=SchemaChange(run_id="unknown", timestamp=None),
            row_count_diff=0,
            null_count_changes={},
            distinct_count_changes={},
            sample_diff_included=False,
            sample_diff=None,
        )

    try:
        story_a = _load_story(fs_a, story_a_path)
        story_b = _load_story(fs_b, story_b_path)

        # Calculate row count diff across all nodes
        rows_a = sum(
            n.get("row_count", n.get("rows_out", 0)) or 0 for n in story_a.get("nodes", [])
        )
        rows_b = sum(
            n.get("row_count", n.get("rows_out", 0)) or 0 for n in story_b.get("nodes", [])
        )

        return DiffSummary(
            schema_diff=SchemaChange(run_id=run_b, timestamp=story_b.get("end_time")),
            row_count_diff=rows_b - rows_a,
            null_count_changes={},
            distinct_count_changes={},
            sample_diff_included=False,
            sample_diff=None,
        )

    except Exception:
        logger.exception("Error computing story diff")
        return DiffSummary(
            schema_diff=SchemaChange(run_id="error", timestamp=None),
            row_count_diff=0,
            null_count_changes={},
            distinct_count_changes={},
            sample_diff_included=False,
            sample_diff=None,
        )
