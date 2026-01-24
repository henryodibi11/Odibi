# odibi_mcp/tools/sample.py
"""Sample-related MCP tools - wired to real data (local and cloud)."""

import json
import logging
from dataclasses import dataclass
from typing import List, Optional, Tuple
from pathlib import Path

import fsspec

from odibi_mcp.contracts.selectors import RunSelector, DEFAULT_RUN_SELECTOR
from odibi_mcp.contracts.enums import TruncatedReason
from odibi_mcp.context import get_project_context

logger = logging.getLogger(__name__)


def _get_story_fs_and_path(
    ctx,
) -> Tuple[Optional[fsspec.AbstractFileSystem], Optional[str], Optional[dict]]:
    """Get filesystem and base path for stories (supports local and cloud)."""
    if not ctx or not ctx.story_connection or not ctx.story_path:
        return None, None, None

    try:
        conn = ctx.get_connection(ctx.story_connection)

        if hasattr(conn, "pandas_storage_options"):
            storage_options = conn.pandas_storage_options()
            container = getattr(conn, "container", None)
            if container:
                base_path = f"{container}/{ctx.story_path.rstrip('/')}"
                fs = fsspec.filesystem("abfs", **storage_options)
                return fs, base_path, storage_options

        local_path = conn.get_path(ctx.story_path)
        fs = fsspec.filesystem("file")
        return fs, local_path, None

    except Exception as e:
        logger.warning(f"Could not get story filesystem: {e}")
        return None, None, None


def _find_story_and_load(
    pipeline: str, run_selector: Optional[RunSelector] = None
) -> Optional[dict]:
    """Find and load story JSON for a pipeline."""
    ctx = get_project_context()
    if not ctx:
        return None

    fs, story_base, _ = _get_story_fs_and_path(ctx)
    if not fs or not story_base:
        return None

    try:
        pipeline_dir = f"{story_base}/{pipeline}"
        if not fs.exists(pipeline_dir):
            return None

        # Find all JSON story files
        story_files = []
        for date_dir in fs.ls(pipeline_dir):
            if fs.isdir(date_dir):
                for f in fs.ls(date_dir):
                    if f.endswith(".json"):
                        story_files.append(f)

        if not story_files:
            return None

        story_files.sort(reverse=True)

        # Load the latest story
        with fs.open(story_files[0], "r") as f:
            return json.load(f)

    except Exception as e:
        logger.warning(f"Error loading story: {e}")
        return None


@dataclass
class SampleResult:
    """Result of a sample operation."""

    pipeline: str
    node: str
    rows: List[dict]
    truncated: bool
    truncated_reason: Optional[TruncatedReason]
    original_row_count: int
    error: Optional[str] = None


def _find_node_output_path(
    pipeline: str, node: str, run_selector: Optional[RunSelector] = None
) -> Optional[Path]:
    """Find the output file path for a node from its story."""
    ctx = get_project_context()
    if not ctx:
        return None

    # First, try to find the story to get output path
    story_base = ctx.get_story_base_path()
    if story_base:
        # Look for story file
        pipeline_dir = story_base / pipeline
        story_files = list(pipeline_dir.glob("*/story.json")) if pipeline_dir.exists() else []
        if not story_files:
            story_files = list(story_base.glob(f"{pipeline}*.json"))

        if story_files:
            story_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            story_path = story_files[0]

            try:
                with open(story_path) as f:
                    story = json.load(f)

                for n in story.get("nodes", []):
                    if n.get("name") == node:
                        outputs = n.get("outputs", {})
                        for output_name, output_info in outputs.items():
                            if isinstance(output_info, dict):
                                output_path = output_info.get("path")
                                if output_path:
                                    # Resolve path using connection
                                    conn_name = output_info.get("connection")
                                    if conn_name:
                                        try:
                                            conn = ctx.get_connection(conn_name)
                                            return Path(conn.get_path(output_path))
                                        except Exception:
                                            pass
                                    # Try direct path
                                    p = Path(output_path)
                                    if p.exists():
                                        return p
            except Exception as e:
                logger.warning(f"Could not read story for output path: {e}")

    return None


def node_sample(
    pipeline: str,
    node: str,
    run_selector: RunSelector = DEFAULT_RUN_SELECTOR,
    max_rows: int = 100,
) -> SampleResult:
    """
    Get sample output data from a node.

    Reads sample_data from the story JSON (embedded during pipeline run).
    """
    ctx = get_project_context()
    if ctx and ctx.is_exploration_mode():
        return SampleResult(
            pipeline=pipeline,
            node=node,
            rows=[],
            truncated=False,
            truncated_reason=None,
            original_row_count=0,
            error="Sample tools require full project.yaml (exploration mode active)",
        )

    story = _find_story_and_load(pipeline, run_selector)

    if not story:
        return SampleResult(
            pipeline=pipeline,
            node=node,
            rows=[],
            truncated=False,
            truncated_reason=None,
            original_row_count=0,
            error=f"No story found for pipeline: {pipeline}",
        )

    # Find the node in the story
    for n in story.get("nodes", []):
        node_name = n.get("node_name", n.get("name"))
        if node_name == node:
            sample_data = n.get("sample_data", [])

            if not sample_data:
                return SampleResult(
                    pipeline=pipeline,
                    node=node,
                    rows=[],
                    truncated=False,
                    truncated_reason=None,
                    original_row_count=0,
                    error=f"No sample data available for node: {node}",
                )

            original_count = len(sample_data)
            truncated = original_count > max_rows
            rows = sample_data[:max_rows]

            return SampleResult(
                pipeline=pipeline,
                node=node,
                rows=rows,
                truncated=truncated,
                truncated_reason=TruncatedReason.ROW_LIMIT if truncated else None,
                original_row_count=original_count,
            )

    return SampleResult(
        pipeline=pipeline,
        node=node,
        rows=[],
        truncated=False,
        truncated_reason=None,
        original_row_count=0,
        error=f"Node not found in story: {node}",
    )


def node_sample_in(
    pipeline: str,
    node: str,
    input_name: str = "default",
    run_selector: RunSelector = DEFAULT_RUN_SELECTOR,
    max_rows: int = 100,
) -> SampleResult:
    """
    Get sample input data for a node.

    Reads sample_in from the story JSON (embedded during pipeline run).
    """
    ctx = get_project_context()
    if ctx and ctx.is_exploration_mode():
        return SampleResult(
            pipeline=pipeline,
            node=f"{node}.{input_name}",
            rows=[],
            truncated=False,
            truncated_reason=None,
            original_row_count=0,
            error="Sample tools require full project.yaml (exploration mode active)",
        )

    story = _find_story_and_load(pipeline, run_selector)

    if not story:
        return SampleResult(
            pipeline=pipeline,
            node=f"{node}.{input_name}",
            rows=[],
            truncated=False,
            truncated_reason=None,
            original_row_count=0,
            error=f"No story found for pipeline: {pipeline}",
        )

    # Find the node in the story
    for n in story.get("nodes", []):
        node_name = n.get("node_name", n.get("name"))
        if node_name == node:
            sample_in = n.get("sample_in", [])

            if not sample_in:
                return SampleResult(
                    pipeline=pipeline,
                    node=f"{node}.{input_name}",
                    rows=[],
                    truncated=False,
                    truncated_reason=None,
                    original_row_count=0,
                    error=f"No input sample data available for node: {node}",
                )

            original_count = len(sample_in)
            truncated = original_count > max_rows
            rows = sample_in[:max_rows]

            return SampleResult(
                pipeline=pipeline,
                node=f"{node}.{input_name}",
                rows=rows,
                truncated=truncated,
                truncated_reason=TruncatedReason.ROW_LIMIT if truncated else None,
                original_row_count=original_count,
            )

    return SampleResult(
        pipeline=pipeline,
        node=f"{node}.{input_name}",
        rows=[],
        truncated=False,
        truncated_reason=None,
        original_row_count=0,
        error=f"Node not found in story: {node}",
    )


def node_failed_rows(
    pipeline: str,
    node: str,
    run_selector: RunSelector = DEFAULT_RUN_SELECTOR,
    max_rows: int = 50,
) -> SampleResult:
    """
    Get rows that failed validation for a node.

    Reads failed_rows_samples from the story JSON (embedded during pipeline run).
    """
    ctx = get_project_context()
    if ctx and ctx.is_exploration_mode():
        return SampleResult(
            pipeline=pipeline,
            node=node,
            rows=[],
            truncated=False,
            truncated_reason=None,
            original_row_count=0,
            error="Sample tools require full project.yaml (exploration mode active)",
        )

    story = _find_story_and_load(pipeline, run_selector)

    if not story:
        return SampleResult(
            pipeline=pipeline,
            node=node,
            rows=[],
            truncated=False,
            truncated_reason=None,
            original_row_count=0,
            error=f"No story found for pipeline: {pipeline}",
        )

    # Find the node in the story
    for n in story.get("nodes", []):
        node_name = n.get("node_name", n.get("name"))
        if node_name == node:
            failed_rows = n.get("failed_rows_samples", [])

            if not failed_rows:
                return SampleResult(
                    pipeline=pipeline,
                    node=node,
                    rows=[],
                    truncated=False,
                    truncated_reason=None,
                    original_row_count=0,
                    error=f"No failed rows for node: {node} (validation passed)",
                )

            original_count = len(failed_rows)
            truncated = original_count > max_rows
            rows = failed_rows[:max_rows]

            return SampleResult(
                pipeline=pipeline,
                node=node,
                rows=rows,
                truncated=truncated,
                truncated_reason=TruncatedReason.ROW_LIMIT if truncated else None,
                original_row_count=original_count,
            )

    return SampleResult(
        pipeline=pipeline,
        node=node,
        rows=[],
        truncated=False,
        truncated_reason=None,
        original_row_count=0,
        error=f"Node not found in story: {node}",
    )
