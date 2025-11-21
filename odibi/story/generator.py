"""Story generator for pipeline execution documentation."""

from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import pandas as pd
import yaml
import subprocess
from odibi import __version__

from odibi.node import NodeResult


# Custom class to force block style for multiline strings
class MultilineString(str):
    """String subclass to force YAML block scalar style."""

    pass


def multiline_presenter(dumper, data):
    """YAML representer for MultilineString."""
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


yaml.add_representer(MultilineString, multiline_presenter)


class StoryGenerator:
    """Generates markdown documentation of pipeline execution."""

    def __init__(
        self,
        pipeline_name: str,
        max_sample_rows: int = 10,
        output_path: str = "stories/",
        retention_days: int = 30,
        retention_count: int = 100,
    ):
        """Initialize story generator.

        Args:
            pipeline_name: Name of the pipeline
            max_sample_rows: Maximum rows to show in samples
            output_path: Directory for story output
            retention_days: Days to keep stories
            retention_count: Max number of stories to keep
        """
        self.pipeline_name = pipeline_name
        self.max_sample_rows = max_sample_rows
        self.output_path_str = output_path  # Store original string
        self.is_remote = "://" in output_path

        if not self.is_remote:
            self.output_path = Path(output_path)
            self.output_path.mkdir(parents=True, exist_ok=True)
        else:
            self.output_path = None  # Handle remote paths differently

        self.retention_days = retention_days
        self.retention_count = retention_count

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
        config: Optional[Dict[str, Any]] = None,
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
            config: Optional pipeline configuration snapshot

        Returns:
            Path to generated story file
        """
        lines = []
        git_info = self._get_git_info()

        # Header
        lines.append(f"# Pipeline Run Story: {self.pipeline_name}")
        lines.append("")
        lines.append("### Execution Metadata")
        lines.append(f"- **Started:** {start_time}")
        lines.append(f"- **Completed:** {end_time}")
        lines.append(f"- **Duration:** {duration:.2f}s")
        lines.append(f"- **Odibi Version:** v{__version__}")
        lines.append(f"- **Git Commit:** `{git_info['commit']}` (Branch: `{git_info['branch']}`)")

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

        if self.is_remote:
            # Remote path handling
            full_path = f"{self.output_path_str.rstrip('/')}/{filename}"
            self._write_remote(full_path, "\n".join(lines))
            story_path = full_path
        else:
            # Local path handling
            story_path = self.output_path / filename
            with open(story_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))

        # Auto-cleanup old stories
        self.cleanup()

        return str(story_path)

    def _write_remote(self, path: str, content: str) -> None:
        """Write content to remote path using fsspec."""
        try:
            import fsspec

            with fsspec.open(path, "w", encoding="utf-8") as f:
                f.write(content)
        except ImportError:
            # Fallback for environments without fsspec (e.g., minimal Spark)
            # Try dbutils if on Databricks
            try:
                from pyspark.dbutils import DBUtils
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()
                dbutils = DBUtils(spark)
                # dbutils.fs.put expects string
                dbutils.fs.put(path, content, True)
            except Exception:
                print(f"Warning: Could not write story to {path}. Install 'fsspec' or 'adlfs'.")

    def _clean_config_for_dump(self, config: Any) -> Any:
        """Clean configuration for YAML dumping.

        Handles multiline strings to force block style.
        """
        if isinstance(config, dict):
            return {k: self._clean_config_for_dump(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._clean_config_for_dump(v) for v in config]
        elif isinstance(config, str) and "\n" in config:
            # Use custom class to force block style
            # Strip trailing spaces from lines to allow block style
            cleaned = config.replace(" \n", "\n").strip()
            return MultilineString(cleaned)
        return config

    def _get_git_info(self) -> Dict[str, str]:
        """Get current git commit and branch."""
        try:
            # Run git commands silently
            commit = (
                subprocess.check_output(
                    ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL
                )
                .decode("utf-8")
                .strip()
            )

            branch = (
                subprocess.check_output(
                    ["git", "rev-parse", "--abbrev-ref", "HEAD"], stderr=subprocess.DEVNULL
                )
                .decode("utf-8")
                .strip()
            )

            return {"commit": commit, "branch": branch}
        except Exception:
            return {"commit": "unknown", "branch": "unknown"}

    def cleanup(self) -> None:
        """Remove old stories based on retention policy."""
        try:
            from datetime import timedelta

            # Find all stories for this pipeline
            stories = sorted(
                self.output_path.glob(f"{self.pipeline_name}_*.md"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )

            # 1. Count retention
            if len(stories) > self.retention_count:
                to_delete = stories[self.retention_count :]
                for path in to_delete:
                    path.unlink(missing_ok=True)

                # Update list for next check
                stories = stories[: self.retention_count]

            # 2. Time retention
            now = datetime.now()
            cutoff = now - timedelta(days=self.retention_days)

            for path in stories:
                mtime = datetime.fromtimestamp(path.stat().st_mtime)
                if mtime < cutoff:
                    path.unlink(missing_ok=True)

        except Exception as e:
            # Don't fail generation if cleanup fails
            print(f"Warning: Story cleanup failed: {e}")

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
        if result.metadata and "schema_in" in result.metadata:
            schema_in = result.metadata["schema_in"]
            lines.append("")
            lines.append("**Input schema:**")
            lines.append(f"- Columns ({len(schema_in)}): {', '.join(schema_in)}")

            # Add input sample
            if "sample_data_in" in result.metadata:
                sample_in = result.metadata["sample_data_in"]
                if sample_in:
                    lines.append("")
                    lines.append(f"**Sample input** (first {len(sample_in)} rows):")
                    lines.append("")
                    lines.append(self._sample_to_markdown(sample_in, schema_in))

        if result.result_schema:
            lines.append("")
            lines.append("**Output schema:**")
            lines.append(
                f"- Columns ({len(result.result_schema)}): {', '.join(result.result_schema)}"
            )

            # Schema changes
            if result.metadata:
                added = result.metadata.get("columns_added")
                removed = result.metadata.get("columns_removed")

                if added or removed:
                    lines.append("")
                    lines.append("**Schema Changes:**")
                    if added:
                        lines.append(f"- 🟢 **Added:** {', '.join(added)}")
                    if removed:
                        lines.append(f"- 🔴 **Removed:** {', '.join(removed)}")

        if result.rows_processed is not None:
            lines.append(f"- Rows: {result.rows_processed:,}")

        # Sample data
        if result.metadata and "sample_data" in result.metadata:
            sample_data = result.metadata["sample_data"]
            if sample_data:
                lines.append("")
                lines.append(f"**Sample output** (first {len(sample_data)} rows):")
                lines.append("")
                lines.append(self._sample_to_markdown(sample_data, result.result_schema))

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

    def _sample_to_markdown(self, sample: List[Dict[str, Any]], schema: Optional[List[str]]) -> str:
        """Convert sample data to markdown table.

        Args:
            sample: List of row dictionaries
            schema: Optional list of column names (for ordering)

        Returns:
            Markdown table string
        """
        if not sample:
            return "*No data*"

        # Check for special redaction message (sensitive: true)
        if len(sample) == 1 and "message" in sample[0] and len(sample[0]) == 1:
            msg = sample[0]["message"]
            if "[REDACTED" in str(msg):
                return f"**{msg}**"

        # Determine columns
        if schema:
            columns = schema
        else:
            columns = list(sample[0].keys())

        # Build markdown table
        lines = []

        # Header
        header = "| " + " | ".join(str(col) for col in columns) + " |"
        lines.append(header)

        # Separator
        separator = "| " + " | ".join("---" for _ in columns) + " |"
        lines.append(separator)

        # Rows
        for row in sample:
            values = [str(row.get(col, "")) for col in columns]
            row_str = "| " + " | ".join(values) + " |"
            lines.append(row_str)

        return "\n".join(lines)

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
