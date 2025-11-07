"""Story generator for pipeline execution documentation."""

from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path
import pandas as pd

from odibi.node import NodeResult


class StoryGenerator:
    """Generates markdown documentation of pipeline execution."""

    def __init__(
        self, pipeline_name: str, max_sample_rows: int = 10, output_path: str = "stories/"
    ):
        """Initialize story generator.

        Args:
            pipeline_name: Name of the pipeline
            max_sample_rows: Maximum rows to show in samples
            output_path: Directory for story output
        """
        self.pipeline_name = pipeline_name
        self.max_sample_rows = max_sample_rows
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)

    def generate(
        self,
        node_results: Dict[str, NodeResult],
        completed: List[str],
        failed: List[str],
        skipped: List[str],
        duration: float,
        start_time: str,
        end_time: str,
        context: Any = None,
    ) -> str:
        """Generate story markdown.

        Args:
            node_results: Dictionary of node name -> NodeResult
            completed: List of completed node names
            failed: List of failed node names
            skipped: List of skipped node names
            duration: Total pipeline duration
            start_time: ISO timestamp of start
            end_time: ISO timestamp of end
            context: Optional context to access intermediate DataFrames

        Returns:
            Path to generated story file
        """
        lines = []

        # Header
        lines.append(f"# Pipeline Run Story: {self.pipeline_name}")
        lines.append("")
        lines.append(f"**Executed:** {start_time}")
        lines.append(f"**Completed:** {end_time}")
        lines.append(f"**Duration:** {duration:.2f}s")

        # Status
        if failed:
            lines.append(f"**Status:** ❌ Failed ({len(failed)} nodes failed)")
        elif completed:
            lines.append("**Status:** ✅ Success")
        else:
            lines.append("**Status:** ⚠️ No nodes executed")

        lines.append("")
        lines.append("---")
        lines.append("")

        # Summary
        lines.append("## Summary")
        lines.append("")
        lines.append(f"- ✅ **Completed:** {len(completed)} nodes")
        lines.append(f"- ❌ **Failed:** {len(failed)} nodes")
        lines.append(f"- ⏭️ **Skipped:** {len(skipped)} nodes")
        lines.append(f"- ⏱️ **Duration:** {duration:.2f}s")

        if completed:
            lines.append(f"\n**Completed nodes:** {', '.join(completed)}")
        if failed:
            lines.append(f"\n**Failed nodes:** {', '.join(failed)}")
        if skipped:
            lines.append(f"\n**Skipped nodes:** {', '.join(skipped)}")

        lines.append("")
        lines.append("---")
        lines.append("")

        # Node details
        all_nodes = completed + failed + skipped
        for node_name in all_nodes:
            if node_name in node_results:
                node_result = node_results[node_name]
                node_section = self._generate_node_section(node_name, node_result, context)
                lines.extend(node_section)

        # Write to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.pipeline_name}_{timestamp}.md"
        story_path = self.output_path / filename

        with open(story_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        return str(story_path)

    def _generate_node_section(self, node_name: str, result: NodeResult, context: Any) -> List[str]:
        """Generate markdown section for a single node.

        Args:
            node_name: Node name
            result: Node execution result
            context: Execution context

        Returns:
            List of markdown lines
        """
        lines = []

        # Node header
        lines.append(f"## Node: {node_name}")
        lines.append("")

        # Status and duration
        status_icon = "✅" if result.success else "❌"
        status_text = "Success" if result.success else "Failed"
        lines.append(f"**Status:** {status_icon} {status_text}")
        lines.append(f"**Duration:** {result.duration:.4f}s")

        # Metadata
        if result.metadata and "steps" in result.metadata:
            lines.append("")
            lines.append("**Execution steps:**")
            for step in result.metadata["steps"]:
                lines.append(f"- {step}")

        # Schema info
        if result.result_schema:
            lines.append("")
            lines.append("**Output schema:**")
            lines.append(
                f"- Columns ({len(result.result_schema)}): {', '.join(result.result_schema)}"
            )

        if result.rows_processed is not None:
            lines.append(f"- Rows: {result.rows_processed:,}")

        # Sample data
        if context and context.has(node_name) and result.success:
            try:
                df = context.get(node_name)
                lines.append("")
                lines.append(f"**Sample output** (first {self.max_sample_rows} rows):")
                lines.append("")
                lines.append(self._dataframe_to_markdown(df, self.max_sample_rows))
            except Exception:
                # If we can't get the data, skip sample
                pass

        # Error details
        if result.error:
            lines.append("")
            lines.append("**Error:**")
            lines.append("```")
            lines.append(str(result.error))
            lines.append("```")

        lines.append("")
        lines.append("---")
        lines.append("")

        return lines

    def _dataframe_to_markdown(self, df: pd.DataFrame, max_rows: int) -> str:
        """Convert DataFrame to markdown table.

        Args:
            df: DataFrame to convert
            max_rows: Maximum rows to include

        Returns:
            Markdown table string
        """
        if len(df) == 0:
            return "*No data*"

        # Get sample
        sample = df.head(max_rows)

        # Build markdown table
        lines = []

        # Header
        header = "| " + " | ".join(str(col) for col in sample.columns) + " |"
        lines.append(header)

        # Separator
        separator = "| " + " | ".join("---" for _ in sample.columns) + " |"
        lines.append(separator)

        # Rows
        for _, row in sample.iterrows():
            row_str = "| " + " | ".join(str(val) for val in row.values) + " |"
            lines.append(row_str)

        if len(df) > max_rows:
            lines.append(f"\n*... and {len(df) - max_rows} more rows*")

        return "\n".join(lines)
