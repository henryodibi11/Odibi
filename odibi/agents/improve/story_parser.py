"""StoryParser: Parse odibi story markdown files into structured data.

Parses the markdown output from odibi pipeline runs to extract
structured information about node execution, failures, and metrics.
"""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class NodeFailure:
    """Details about a failed node execution."""

    node_name: str
    operation: str
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    duration: float = 0.0

    def __str__(self) -> str:
        return f"{self.node_name}: {self.error_message or 'Unknown error'}"


@dataclass
class NodeExecution:
    """Parsed execution data for a single node."""

    node_name: str
    operation: str
    status: str  # "success", "failed", "skipped"
    duration: float = 0.0
    rows_in: Optional[int] = None
    rows_out: Optional[int] = None
    error_message: Optional[str] = None
    error_type: Optional[str] = None


@dataclass
class ParsedStory:
    """Structured data parsed from an odibi story markdown file.

    Contains all relevant information extracted from the story
    for analysis and decision-making.
    """

    pipeline_name: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration: float = 0.0
    status: str = "unknown"  # "success", "failed", "partial"

    # Node counts
    total_nodes: int = 0
    completed_nodes: int = 0
    failed_nodes: int = 0
    skipped_nodes: int = 0

    # Rows processed
    total_rows_processed: int = 0

    # Node details
    nodes: list[NodeExecution] = field(default_factory=list)

    # Project context (optional)
    project: Optional[str] = None
    layer: Optional[str] = None

    # Raw content for reference
    raw_content: str = ""

    @property
    def success_rate(self) -> float:
        """Calculate node success rate."""
        if self.total_nodes == 0:
            return 0.0
        return (self.completed_nodes / self.total_nodes) * 100

    def get_failures(self) -> list[NodeFailure]:
        """Get all failed nodes as NodeFailure objects."""
        failures = []
        for node in self.nodes:
            if node.status == "failed":
                failures.append(
                    NodeFailure(
                        node_name=node.node_name,
                        operation=node.operation,
                        error_type=node.error_type,
                        error_message=node.error_message,
                        duration=node.duration,
                    )
                )
        return failures


class StoryParser:
    """Parse odibi story markdown files into structured data.

    Handles both the markdown format and the JSON format that
    odibi can produce for stories.
    """

    def parse(self, story_path: Path) -> ParsedStory:
        """Parse a story file into structured data.

        Args:
            story_path: Path to the story file (.md or .json).

        Returns:
            ParsedStory with extracted data.

        Raises:
            FileNotFoundError: If story file doesn't exist.
            ValueError: If story format is unrecognized.
        """
        story_path = Path(story_path)

        if not story_path.exists():
            raise FileNotFoundError(f"Story file not found: {story_path}")

        content = story_path.read_text(encoding="utf-8")

        if story_path.suffix == ".json":
            return self._parse_json(content)
        else:
            return self._parse_markdown(content)

    def parse_content(self, content: str, format: str = "markdown") -> ParsedStory:
        """Parse story content from a string.

        Args:
            content: Story content as string.
            format: Format of content ("markdown" or "json").

        Returns:
            ParsedStory with extracted data.
        """
        if format == "json":
            return self._parse_json(content)
        else:
            return self._parse_markdown(content)

    def _parse_markdown(self, content: str) -> ParsedStory:
        """Parse markdown story format."""
        story = ParsedStory(
            pipeline_name="unknown",
            raw_content=content,
        )

        # Extract pipeline name from header
        # Format: "# 📊 Pipeline Run Story: pipeline_name"
        name_match = re.search(r"#.*Pipeline.*Story:\s*(.+?)(?:\n|$)", content, re.I)
        if name_match:
            story.pipeline_name = name_match.group(1).strip()

        # Extract timing info
        started_match = re.search(r"\*\*Started:\*\*\s*(.+?)(?:\n|$)", content)
        if started_match:
            story.started_at = started_match.group(1).strip()

        completed_match = re.search(r"\*\*Completed:\*\*\s*(.+?)(?:\n|$)", content)
        if completed_match:
            story.completed_at = completed_match.group(1).strip()

        duration_match = re.search(r"\*\*Duration:\*\*\s*([\d.]+)s", content)
        if duration_match:
            story.duration = float(duration_match.group(1))

        # Extract status
        if "❌ Failed" in content:
            story.status = "failed"
        elif "✅ Success" in content:
            story.status = "success"
        elif "⚠️ Partial" in content:
            story.status = "partial"

        # Extract summary counts
        # Format: "- ✅ **Completed:** 5 nodes"
        completed_match = re.search(r"✅\s*\*\*Completed:\*\*\s*(\d+)", content)
        if completed_match:
            story.completed_nodes = int(completed_match.group(1))

        failed_match = re.search(r"❌\s*\*\*Failed:\*\*\s*(\d+)", content)
        if failed_match:
            story.failed_nodes = int(failed_match.group(1))

        skipped_match = re.search(r"⏭️\s*\*\*Skipped:\*\*\s*(\d+)", content)
        if skipped_match:
            story.skipped_nodes = int(skipped_match.group(1))

        story.total_nodes = story.completed_nodes + story.failed_nodes + story.skipped_nodes

        # Extract total rows
        rows_match = re.search(r"\*\*Total Rows Processed:\*\*\s*([\d,]+)", content)
        if rows_match:
            story.total_rows_processed = int(rows_match.group(1).replace(",", ""))

        # Extract layer
        layer_match = re.search(r"\*\*Layer:\*\*\s*(.+?)(?:\n|$)", content)
        if layer_match:
            story.layer = layer_match.group(1).strip()

        # Extract project
        project_match = re.search(r"\*\*Project:\*\*\s*(.+?)(?:\s*\||$)", content)
        if project_match:
            story.project = project_match.group(1).strip()

        # Parse node sections
        # Format: "### ✅ node_name" or "### ❌ node_name"
        node_sections = re.split(r"###\s+[✅❌⏭️❓]\s+", content)

        for section in node_sections[1:]:  # Skip content before first node
            node = self._parse_node_section(section)
            if node:
                story.nodes.append(node)

        return story

    def _parse_node_section(self, section: str) -> Optional[NodeExecution]:
        """Parse a single node section from markdown."""
        lines = section.strip().split("\n")
        if not lines:
            return None

        # First line is node name
        node_name = lines[0].strip()
        if not node_name:
            return None

        # Determine status from section header (passed in as part of section split)
        # We need to check the original content for status markers
        status = "success"  # Default

        # Look for operation
        operation = "unknown"
        operation_match = re.search(r"\*\*Operation:\*\*\s*`?([^`\n]+)`?", section)
        if operation_match:
            operation = operation_match.group(1).strip()

        # Look for duration
        duration = 0.0
        duration_match = re.search(r"\*\*Duration:\*\*\s*([\d.]+)s", section)
        if duration_match:
            duration = float(duration_match.group(1))

        # Look for rows
        rows_in = None
        rows_out = None
        rows_in_match = re.search(r"\*\*Rows In:\*\*\s*([\d,]+)", section)
        if rows_in_match:
            rows_in = int(rows_in_match.group(1).replace(",", ""))

        rows_out_match = re.search(r"\*\*Rows Out:\*\*\s*([\d,]+)", section)
        if rows_out_match:
            rows_out = int(rows_out_match.group(1).replace(",", ""))

        # Look for error info
        error_message = None
        error_type = None

        if "Error" in section or "error" in section.lower():
            status = "failed"
            # Try to extract error message from code block
            error_block = re.search(r"```(?:python)?\n(.+?)```", section, re.DOTALL)
            if error_block:
                error_message = error_block.group(1).strip()[:500]  # Limit length

            # Try to get error type
            type_match = re.search(r"\*\*Error Type:\*\*\s*`?([^`\n]+)`?", section)
            if type_match:
                error_type = type_match.group(1).strip()

        return NodeExecution(
            node_name=node_name,
            operation=operation,
            status=status,
            duration=duration,
            rows_in=rows_in,
            rows_out=rows_out,
            error_message=error_message,
            error_type=error_type,
        )

    def _parse_json(self, content: str) -> ParsedStory:
        """Parse JSON story format."""
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON story format: {e}")

        story = ParsedStory(
            pipeline_name=data.get("pipeline_name", "unknown"),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            duration=data.get("duration", 0.0),
            total_nodes=data.get("total_nodes", 0),
            completed_nodes=data.get("completed_nodes", 0),
            failed_nodes=data.get("failed_nodes", 0),
            skipped_nodes=data.get("skipped_nodes", 0),
            project=data.get("project"),
            layer=data.get("pipeline_layer"),
            raw_content=content,
        )

        # Determine status
        if story.failed_nodes > 0:
            story.status = "failed"
        elif story.completed_nodes == story.total_nodes:
            story.status = "success"
        else:
            story.status = "partial"

        # Parse nodes
        for node_data in data.get("nodes", []):
            node = NodeExecution(
                node_name=node_data.get("node_name", "unknown"),
                operation=node_data.get("operation", "unknown"),
                status=node_data.get("status", "unknown"),
                duration=node_data.get("duration", 0.0),
                rows_in=node_data.get("rows_in"),
                rows_out=node_data.get("rows_out"),
                error_message=node_data.get("error_message"),
                error_type=node_data.get("error_type"),
            )
            story.nodes.append(node)

            if node.rows_out:
                story.total_rows_processed += node.rows_out

        return story

    def get_failures(self, story: ParsedStory) -> list[NodeFailure]:
        """Extract failed nodes with error details.

        Args:
            story: Parsed story data.

        Returns:
            List of NodeFailure objects.
        """
        return story.get_failures()

    def get_success_rate(self, story: ParsedStory) -> float:
        """Calculate node success percentage.

        Args:
            story: Parsed story data.

        Returns:
            Success rate as percentage (0-100).
        """
        return story.success_rate
