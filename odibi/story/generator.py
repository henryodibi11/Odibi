"""Story generator for pipeline execution documentation."""

import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from odibi.node import NodeResult
from odibi.story.metadata import DeltaWriteInfo, NodeExecutionMetadata, PipelineStoryMetadata
from odibi.story.renderers import HTMLStoryRenderer, JSONStoryRenderer
from odibi.utils.logging_context import get_logging_context


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
        storage_options: Optional[Dict[str, Any]] = None,
        catalog_manager: Optional[Any] = None,
    ):
        """Initialize story generator.

        Args:
            pipeline_name: Name of the pipeline
            max_sample_rows: Maximum rows to show in samples
            output_path: Directory for story output
            retention_days: Days to keep stories
            retention_count: Max number of stories to keep
            storage_options: Credentials for remote storage (e.g. ADLS)
            catalog_manager: System Catalog Manager for historical context
        """
        self.pipeline_name = pipeline_name
        self.max_sample_rows = max_sample_rows
        self.output_path_str = output_path  # Store original string
        self.is_remote = "://" in output_path
        self.storage_options = storage_options or {}
        self.catalog_manager = catalog_manager

        # Track last generated story for alert enrichment
        self._last_story_path: Optional[str] = None
        self._last_metadata: Optional[PipelineStoryMetadata] = None

        if not self.is_remote:
            self.output_path = Path(output_path)
            self.output_path.mkdir(parents=True, exist_ok=True)
        else:
            self.output_path = None  # Handle remote paths differently

        self.retention_days = retention_days
        self.retention_count = retention_count

        ctx = get_logging_context()
        ctx.debug(
            "StoryGenerator initialized",
            pipeline=pipeline_name,
            output_path=output_path,
            is_remote=self.is_remote,
            retention_days=retention_days,
            retention_count=retention_count,
        )

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
        """Generate story HTML and JSON.

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
            Path to generated HTML story file
        """
        ctx = get_logging_context()
        ctx.debug(
            "Generating story",
            pipeline=self.pipeline_name,
            node_count=len(node_results),
            completed=len(completed),
            failed=len(failed),
            skipped=len(skipped),
        )

        # 1. Build metadata object
        metadata = PipelineStoryMetadata(
            pipeline_name=self.pipeline_name,
            pipeline_layer=config.get("layer") if config else None,
            started_at=start_time,
            completed_at=end_time,
            duration=duration,
            total_nodes=len(completed) + len(failed) + len(skipped),
            completed_nodes=len(completed),
            failed_nodes=len(failed),
            skipped_nodes=len(skipped),
            project=config.get("project") if config else None,
            plant=config.get("plant") if config else None,
            asset=config.get("asset") if config else None,
            business_unit=config.get("business_unit") if config else None,
        )

        # Add Git Info
        # git_info = self._get_git_info()
        # We can't easily add arbitrary fields to dataclass without changing it,
        # but we can rely on the fact that it's just metadata.
        # For now, let's skip adding git info to the core model or extend it later.

        # Process all nodes in order
        all_nodes = completed + failed + skipped

        # If we have config, try to follow config order instead of list order
        if config and "nodes" in config:
            config_order = [n["name"] for n in config["nodes"]]
            # Sort all_nodes based on index in config_order
            all_nodes.sort(key=lambda x: config_order.index(x) if x in config_order else 999)

        for node_name in all_nodes:
            if node_name in node_results:
                result = node_results[node_name]
                node_meta = self._convert_result_to_metadata(result, node_name)

                # Status overrides (result object has success bool, but we have lists)
                if node_name in failed:
                    node_meta.status = "failed"
                elif node_name in skipped:
                    node_meta.status = "skipped"
                else:
                    node_meta.status = "success"

                metadata.nodes.append(node_meta)
            else:
                # Skipped node without result
                metadata.nodes.append(
                    NodeExecutionMetadata(
                        node_name=node_name, operation="skipped", status="skipped", duration=0.0
                    )
                )

            # Enrich with Historical Context (if available)
            current_node = metadata.nodes[-1]
            if self.catalog_manager:
                try:
                    avg_rows = self.catalog_manager.get_average_volume(node_name)
                    avg_duration = self.catalog_manager.get_average_duration(node_name)

                    current_node.historical_avg_rows = avg_rows
                    current_node.historical_avg_duration = avg_duration
                except Exception:
                    # Don't fail story generation if history fetch fails
                    pass

        # 2. Render outputs
        timestamp_obj = datetime.now()
        date_str = timestamp_obj.strftime("%Y-%m-%d")
        time_str = timestamp_obj.strftime("%H-%M-%S")

        # Create structured path: {pipeline_name}/{date}/
        relative_folder = f"{self.pipeline_name}/{date_str}"

        if self.is_remote:
            base_path = f"{self.output_path_str.rstrip('/')}/{relative_folder}"
        else:
            base_path = self.output_path / relative_folder
            base_path.mkdir(parents=True, exist_ok=True)

        base_filename = f"run_{time_str}"

        # Prepare renderers
        html_renderer = HTMLStoryRenderer()
        json_renderer = JSONStoryRenderer()

        # Paths
        if self.is_remote:
            html_path = f"{base_path}/{base_filename}.html"
            json_path = f"{base_path}/{base_filename}.json"
        else:
            html_path = str(base_path / f"{base_filename}.html")
            json_path = str(base_path / f"{base_filename}.json")

        # Render HTML
        html_content = html_renderer.render(metadata)

        # Render JSON
        json_content = json_renderer.render(metadata)

        # Write files
        try:
            if self.is_remote:
                self._write_remote(html_path, html_content)
                self._write_remote(json_path, json_content)
            else:
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(html_content)
                with open(json_path, "w", encoding="utf-8") as f:
                    f.write(json_content)

            ctx.debug(
                "Story files written",
                html_path=html_path,
                html_size=len(html_content),
                json_path=json_path,
                json_size=len(json_content),
            )
        except Exception as e:
            ctx.error(
                "Failed to write story files",
                error=str(e),
                html_path=html_path,
                json_path=json_path,
            )
            raise

        # Store for alert enrichment
        self._last_story_path = html_path
        self._last_metadata = metadata

        # Cleanup
        self.cleanup()

        ctx.info(
            "Story generated",
            path=html_path,
            nodes=len(metadata.nodes),
            success_rate=metadata.get_success_rate(),
        )

        return html_path

    def get_alert_summary(self) -> Dict[str, Any]:
        """Get a summary of the last generated story for alerts.

        Returns:
            Dictionary with metrics suitable for alert payloads
        """
        if not self._last_metadata:
            return {}

        summary = self._last_metadata.get_alert_summary()
        summary["story_path"] = self._last_story_path
        return summary

    def _convert_result_to_metadata(
        self, result: NodeResult, node_name: str
    ) -> NodeExecutionMetadata:
        """Convert NodeResult to NodeExecutionMetadata."""
        meta = result.metadata or {}

        # Extract Delta Info
        delta_info = None
        if "delta_info" in meta:
            d = meta["delta_info"]
            # Check if it's already an object or dict
            if isinstance(d, DeltaWriteInfo):
                delta_info = d
            else:
                # It might be a dict if coming from loose dict
                pass

        node_meta = NodeExecutionMetadata(
            node_name=node_name,
            operation="transform",  # Generic default
            status="success" if result.success else "failed",
            duration=result.duration,
            rows_out=result.rows_processed,
            schema_out=result.result_schema,
            # From metadata dict
            rows_in=None,  # Calculated if we had input info
            sample_in=meta.get("sample_data_in"),
            executed_sql=meta.get("executed_sql", []),
            sql_hash=meta.get("sql_hash"),
            transformation_stack=meta.get("transformation_stack", []),
            config_snapshot=meta.get("config_snapshot"),
            delta_info=delta_info,
            data_diff=meta.get("data_diff"),
            environment=meta.get("environment"),
            source_files=meta.get("source_files", []),
            null_profile=meta.get("null_profile"),
            schema_in=meta.get("schema_in"),
            sample_data=meta.get("sample_data"),
            columns_added=meta.get("columns_added", []),
            columns_removed=meta.get("columns_removed", []),
            error_message=str(result.error) if result.error else None,
            error_type=type(result.error).__name__ if result.error else None,
            validation_warnings=meta.get("validation_warnings", []),
        )

        # Calculate derived metrics
        node_meta.calculate_row_change()  # Needs rows_in
        # schema changes are already in metadata from Node logic

        return node_meta

    def _write_remote(self, path: str, content: str) -> None:
        """Write content to remote path using fsspec."""
        ctx = get_logging_context()
        try:
            import fsspec

            # Use provided storage options (credentials)
            with fsspec.open(path, "w", encoding="utf-8", **self.storage_options) as f:
                f.write(content)
            ctx.debug("Remote file written", path=path, size=len(content))
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
                ctx.debug("Remote file written via dbutils", path=path, size=len(content))
            except Exception as e:
                ctx.error(
                    "Failed to write remote story",
                    path=path,
                    error=str(e),
                )
                raise RuntimeError(
                    f"Could not write story to {path}. Install 'fsspec' or 'adlfs'."
                ) from e

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
        ctx = get_logging_context()

        if self.is_remote:
            ctx.warning("Remote cleanup for stories is not yet supported. Storage usage may grow.")
            return

        if self.output_path is None:
            return

        try:
            # 1. Clean new nested structure: {pipeline}/{date}/run_*.html
            pipeline_dir = self.output_path / self.pipeline_name
            if pipeline_dir.exists():
                # Find all files recursively
                stories = sorted(
                    pipeline_dir.glob("**/*.html"),
                    key=lambda p: str(p),  # Sort by path (date/time)
                    reverse=True,
                )
                json_stories = sorted(
                    pipeline_dir.glob("**/*.json"),
                    key=lambda p: str(p),
                    reverse=True,
                )

                self._apply_retention(stories, json_stories)

                # Clean empty date directories
                for date_dir in pipeline_dir.iterdir():
                    if date_dir.is_dir() and not any(date_dir.iterdir()):
                        try:
                            date_dir.rmdir()
                        except Exception:
                            pass

            # 2. Clean legacy flat structure: {pipeline}_*.html in root
            legacy_stories = sorted(
                self.output_path.glob(f"{self.pipeline_name}_*.html"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            # Only clean legacy if we have them
            if legacy_stories:
                # We don't want to count legacy + new against the same limit technically,
                # but for simplicity let's just clean legacy based on their own existence
                self._apply_retention(legacy_stories, [])

            ctx.debug(
                "Retention policy applied",
                pipeline=self.pipeline_name,
                retention_days=self.retention_days,
                retention_count=self.retention_count,
            )

        except Exception as e:
            ctx.warning("Story cleanup failed", error=str(e))

    def _apply_retention(self, stories: List[Path], json_stories: List[Path]) -> None:
        """Apply count and time retention policies."""
        from datetime import timedelta

        # 1. Count retention
        if self.retention_count is not None and len(stories) > self.retention_count:
            to_delete = stories[self.retention_count :]
            for path in to_delete:
                path.unlink(missing_ok=True)

        if self.retention_count is not None and len(json_stories) > self.retention_count:
            to_delete = json_stories[self.retention_count :]
            for path in to_delete:
                path.unlink(missing_ok=True)

        # 2. Time retention
        now = datetime.now()
        if self.retention_days is None:
            return
        cutoff = now - timedelta(days=self.retention_days)

        # Check remaining files
        # For nested files, we could parse date from folder name, but mtime is safer fallback
        retention_count = self.retention_count or 100
        remaining = stories[:retention_count] + json_stories[:retention_count]

        for path in remaining:
            if path.exists():
                # Try to infer date from path first (faster/more accurate than mtime)
                # Path format: .../{date}/run_{time}.html
                try:
                    # Try to parse parent folder as date
                    file_date = datetime.strptime(path.parent.name, "%Y-%m-%d")
                    if file_date < cutoff.replace(hour=0, minute=0, second=0, microsecond=0):
                        path.unlink(missing_ok=True)
                        continue
                except ValueError:
                    pass

                # Fallback to mtime
                mtime = datetime.fromtimestamp(path.stat().st_mtime)
                if mtime < cutoff:
                    path.unlink(missing_ok=True)

    # Legacy methods removed as they are now handled by renderers
    # _generate_node_section, _sample_to_markdown, _dataframe_to_markdown
