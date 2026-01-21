"""Tests for the DocGenerator class."""

import tempfile
from pathlib import Path

import pytest

from odibi.config import DocsConfig, DocsIncludeConfig, DocsOutputConfig
from odibi.story.doc_generator import DocGenerator
from odibi.story.metadata import NodeExecutionMetadata, PipelineStoryMetadata


@pytest.fixture
def sample_metadata():
    """Create sample pipeline metadata for testing."""
    nodes = [
        NodeExecutionMetadata(
            node_name="load_customers",
            operation="read_csv",
            status="success",
            duration=1.5,
            rows_in=0,
            rows_out=1000,
            schema_in=[],
            schema_out=["customer_id: int", "name: string", "email: string"],
            transformation_stack=["read_csv", "add_metadata"],
        ),
        NodeExecutionMetadata(
            node_name="transform_customers",
            operation="sql",
            status="success",
            duration=2.3,
            rows_in=1000,
            rows_out=950,
            rows_change=-50,
            rows_change_pct=-5.0,
            schema_in=["customer_id: int", "name: string", "email: string"],
            schema_out=["customer_id: int", "name: string", "email: string", "is_active: bool"],
            columns_added=["is_active"],
            executed_sql=[
                "SELECT *, CASE WHEN status = 'A' THEN true ELSE false END AS is_active FROM customers"
            ],
            transformation_stack=["sql_transform"],
            config_snapshot={"operation": "sql", "sql": "SELECT ..."},
        ),
        NodeExecutionMetadata(
            node_name="dim_customer",
            operation="dimension",
            status="success",
            duration=3.1,
            rows_in=950,
            rows_out=950,
            rows_written=50,
            schema_in=["customer_id: int", "name: string", "email: string", "is_active: bool"],
            schema_out=[
                "customer_sk: bigint",
                "customer_id: int",
                "name: string",
                "email: string",
                "is_active: bool",
            ],
            columns_added=["customer_sk"],
            description="Customer dimension table with surrogate keys",
        ),
    ]

    return PipelineStoryMetadata(
        pipeline_name="test_pipeline",
        pipeline_layer="silver",
        started_at="2026-01-21T10:00:00",
        completed_at="2026-01-21T10:00:07",
        duration=6.9,
        total_nodes=3,
        completed_nodes=3,
        failed_nodes=0,
        skipped_nodes=0,
        project="TestProject",
        nodes=nodes,
    )


@pytest.fixture
def failed_metadata():
    """Create sample failed pipeline metadata."""
    nodes = [
        NodeExecutionMetadata(
            node_name="load_data",
            operation="read_csv",
            status="success",
            duration=1.0,
            rows_out=100,
        ),
        NodeExecutionMetadata(
            node_name="transform_data",
            operation="sql",
            status="failed",
            duration=0.5,
            error_message="Column 'missing_col' not found",
            error_type="AnalysisException",
            error_suggestions=["Check column names in SQL", "Verify schema"],
        ),
    ]

    return PipelineStoryMetadata(
        pipeline_name="failed_pipeline",
        pipeline_layer="bronze",
        started_at="2026-01-21T11:00:00",
        completed_at="2026-01-21T11:00:02",
        duration=1.5,
        total_nodes=2,
        completed_nodes=1,
        failed_nodes=1,
        skipped_nodes=0,
        nodes=nodes,
    )


@pytest.fixture
def docs_config():
    """Create default docs configuration."""
    return DocsConfig(
        enabled=True,
        output_path="docs/generated/",
        outputs=DocsOutputConfig(),
        include=DocsIncludeConfig(schema_tables=True),
    )


class TestDocGenerator:
    """Tests for DocGenerator class."""

    def test_init(self, docs_config):
        """Test DocGenerator initialization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=docs_config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            assert generator.pipeline_name == "test_pipeline"
            assert generator.config.enabled is True

    def test_generate_disabled(self, sample_metadata):
        """Test that no docs are generated when disabled."""
        config = DocsConfig(enabled=False)

        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            result = generator.generate(sample_metadata)
            assert result == {}

    def test_generate_readme(self, docs_config, sample_metadata):
        """Test README.md generation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=docs_config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            result = generator.generate(sample_metadata)

            assert "readme" in result
            readme_path = Path(result["readme"])
            assert readme_path.exists()

            content = readme_path.read_text()
            # Project-level README shows project name and pipeline table
            assert "# TestProject" in content
            assert "test_pipeline" in content
            assert "load_customers" in content
            assert "transform_customers" in content
            assert "dim_customer" in content

    def test_generate_technical_details(self, docs_config, sample_metadata):
        """Test TECHNICAL_DETAILS.md generation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=docs_config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            result = generator.generate(sample_metadata)

            assert "technical_details" in result
            td_path = Path(result["technical_details"])
            assert td_path.exists()

            content = td_path.read_text()
            assert "Technical Details" in content
            assert "Execution Summary" in content
            assert "6.9" in content  # duration

    def test_generate_node_cards(self, docs_config, sample_metadata):
        """Test NODE_CARDS/*.md generation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=docs_config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            result = generator.generate(sample_metadata)

            assert "node_cards" in result
            assert "node_card:load_customers" in result
            assert "node_card:transform_customers" in result
            assert "node_card:dim_customer" in result

            # Check transform_customers card has SQL
            transform_card = Path(result["node_card:transform_customers"])
            assert transform_card.exists()

            content = transform_card.read_text()
            assert "Executed SQL" in content
            assert "SELECT" in content
            assert "is_active" in content

    def test_generate_run_memo(self, docs_config, sample_metadata):
        """Test RUN_HISTORY.md generation (consolidated run memos)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=docs_config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            result = generator.generate(sample_metadata)

            assert "run_memo" in result
            assert Path(result["run_memo"]).exists()
            assert result["run_memo"].endswith("RUN_HISTORY.md")

            content = Path(result["run_memo"]).read_text()
            assert "Run History" in content
            assert "Success" in content
            assert "3/3 nodes" in content

    def test_failed_run_skips_project_docs(self, docs_config, failed_metadata):
        """Test that failed runs don't update project docs but still update RUN_HISTORY."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=docs_config,
                pipeline_name="failed_pipeline",
                workspace_root=tmpdir,
            )

            result = generator.generate(failed_metadata)

            # README should NOT be generated
            assert "readme" not in result
            assert "technical_details" not in result
            assert "node_cards" not in result

            # But RUN_HISTORY should be generated
            assert "run_memo" in result
            content = Path(result["run_memo"]).read_text(encoding="utf-8")
            assert "Failed" in content
            assert "Column 'missing_col' not found" in content

    def test_run_history_consolidates_multiple_runs(self, docs_config, sample_metadata):
        """Test that multiple runs are consolidated into single RUN_HISTORY.md."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=docs_config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            # First run with explicit run_id
            sample_metadata.run_id = "20260121_100000"
            result1 = generator.generate(sample_metadata)
            content1 = Path(result1["run_memo"]).read_text()
            assert "Run History" in content1
            assert "20260121_100000" in content1

            # Second run with different run_id
            sample_metadata.run_id = "20260121_120000"
            result2 = generator.generate(sample_metadata)

            # Should be the same file
            assert result1["run_memo"] == result2["run_memo"]

            # Content should have both runs (newest first)
            content2 = Path(result2["run_memo"]).read_text()
            assert "20260121_100000" in content2
            assert "20260121_120000" in content2
            # Newest should be first (prepended)
            assert content2.index("20260121_120000") < content2.index("20260121_100000")

    def test_node_card_with_schema_changes(self, docs_config, sample_metadata):
        """Test node card shows schema changes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=docs_config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            result = generator.generate(sample_metadata)

            # Check transform card shows added column
            transform_card = Path(result["node_card:transform_customers"])
            content = transform_card.read_text()
            assert "Schema Changes" in content
            assert "is_active" in content

    def test_node_card_with_description(self, docs_config, sample_metadata):
        """Test node card includes description."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=docs_config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            result = generator.generate(sample_metadata)

            # Check dim_customer card has description
            dim_card = Path(result["node_card:dim_customer"])
            content = dim_card.read_text()
            assert "Customer dimension table" in content

    def test_exclude_sql(self, sample_metadata):
        """Test excluding SQL from node cards."""
        config = DocsConfig(
            enabled=True,
            output_path="docs/generated/",
            outputs=DocsOutputConfig(),
            include=DocsIncludeConfig(sql=False),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            result = generator.generate(sample_metadata)

            transform_card = Path(result["node_card:transform_customers"])
            content = transform_card.read_text()
            assert "Executed SQL" not in content

    def test_exclude_config_snapshot(self, sample_metadata):
        """Test excluding config snapshot from node cards."""
        config = DocsConfig(
            enabled=True,
            output_path="docs/generated/",
            outputs=DocsOutputConfig(),
            include=DocsIncludeConfig(config_snapshot=False),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            result = generator.generate(sample_metadata)

            transform_card = Path(result["node_card:transform_customers"])
            content = transform_card.read_text()
            assert "## Configuration" not in content

    def test_selective_outputs(self, sample_metadata):
        """Test generating only specific outputs."""
        config = DocsConfig(
            enabled=True,
            output_path="docs/generated/",
            outputs=DocsOutputConfig(
                readme=True,
                technical_details=False,
                node_cards=False,
                run_memo=False,
            ),
            include=DocsIncludeConfig(schema_tables=True),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            result = generator.generate(sample_metadata)

            assert "readme" in result
            assert "technical_details" not in result
            assert "node_cards" not in result
            assert "run_memo" not in result

    def test_sanitize_filename(self, docs_config):
        """Test filename sanitization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=docs_config,
                pipeline_name="test",
                workspace_root=tmpdir,
            )

            assert generator._sanitize_filename("my_node") == "my_node"
            assert generator._sanitize_filename("My Node") == "my_node"
            assert generator._sanitize_filename("path/to/node") == "path_to_node"
            assert generator._sanitize_filename("schema.table") == "schema_table"


class TestDocGeneratorStoryUrls:
    """Tests for story URL resolution."""

    def test_local_path_in_readme(self, sample_metadata):
        """Test that local paths appear in README."""
        config = DocsConfig(
            enabled=True,
            output_path="docs/",
            outputs=DocsOutputConfig(),
            include=DocsIncludeConfig(schema_tables=True),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            result = generator.generate(
                sample_metadata,
                story_html_path="../stories/test_pipeline/2026-01-21/run.html",
            )

            readme = Path(result["readme"]).read_text(encoding="utf-8")
            assert "../stories/test_pipeline/2026-01-21/run.html" in readme

    def test_remote_path_shown_as_is(self, sample_metadata):
        """Test that remote paths are shown as-is (informational, may not be clickable)."""
        config = DocsConfig(
            enabled=True,
            output_path="docs/",
            outputs=DocsOutputConfig(),
            include=DocsIncludeConfig(schema_tables=True),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            result = generator.generate(
                sample_metadata,
                story_html_path="abfss://container@account.dfs.core.windows.net/stories/run.html",
            )

            readme = Path(result["readme"]).read_text(encoding="utf-8")
            # Remote path is shown as-is
            assert "abfss://container@account.dfs.core.windows.net/stories/run.html" in readme


class TestDocGeneratorIntegration:
    """Integration tests for DocGenerator."""

    def test_full_generation_flow(self, docs_config, sample_metadata):
        """Test complete documentation generation flow."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=docs_config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            story_html = str(Path(tmpdir) / "stories" / "run.html")

            result = generator.generate(
                sample_metadata,
                story_html_path=story_html,
            )

            # Verify all expected files
            assert len(result) >= 5  # readme, technical, node_cards dir, 3 cards, memo

            # Verify directory structure (node cards in pipeline subfolder)
            docs_dir = Path(tmpdir) / "docs" / "generated"
            assert docs_dir.exists()
            assert (docs_dir / "README.md").exists()
            assert (docs_dir / "TECHNICAL_DETAILS.md").exists()
            assert (docs_dir / "node_cards").exists()
            assert (docs_dir / "node_cards" / "test_pipeline" / "load_customers.md").exists()
            assert (docs_dir / "RUN_HISTORY.md").exists()

    def test_readme_has_node_links(self, docs_config, sample_metadata):
        """Test that README links to node cards with pipeline subfolder."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = DocGenerator(
                config=docs_config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
            )

            result = generator.generate(sample_metadata)

            readme = Path(result["readme"]).read_text()
            # Node links now include pipeline name in path
            assert "[load_customers](node_cards/test_pipeline/load_customers.md)" in readme
            assert (
                "[transform_customers](node_cards/test_pipeline/transform_customers.md)" in readme
            )

    def test_multi_pipeline_aggregation(self, docs_config, sample_metadata):
        """Test that multiple pipelines are aggregated in README."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # First pipeline: bronze
            bronze_gen = DocGenerator(
                config=docs_config,
                pipeline_name="bronze_ingest",
                workspace_root=tmpdir,
            )
            sample_metadata.pipeline_name = "bronze_ingest"
            sample_metadata.pipeline_layer = "bronze"
            bronze_gen.generate(sample_metadata)

            # Second pipeline: silver
            silver_gen = DocGenerator(
                config=docs_config,
                pipeline_name="silver_transform",
                workspace_root=tmpdir,
            )
            sample_metadata.pipeline_name = "silver_transform"
            sample_metadata.pipeline_layer = "silver"
            result = silver_gen.generate(sample_metadata)

            # README should show both pipelines
            readme = Path(result["readme"]).read_text()
            assert "bronze_ingest" in readme
            assert "silver_transform" in readme

            # Node cards should be in separate folders
            docs_dir = Path(tmpdir) / "docs" / "generated"
            assert (docs_dir / "node_cards" / "bronze_ingest").exists()
            assert (docs_dir / "node_cards" / "silver_transform").exists()

            # State file should track both
            state_file = docs_dir / ".pipelines.json"
            assert state_file.exists()


class TestDocGeneratorRemoteStorage:
    """Tests for remote storage support."""

    def test_init_with_remote_workspace_root(self):
        """Test initialization with remote workspace root detects remote mode."""
        config = DocsConfig(enabled=True, output_path="docs/generated/")

        generator = DocGenerator(
            config=config,
            pipeline_name="test_pipeline",
            workspace_root="abfss://container@account.dfs.core.windows.net/data",
            storage_options={"account_key": "test_key"},
        )

        assert generator.is_remote is True
        assert "abfss://" in generator.output_path_str
        assert generator.output_path is None
        assert generator.storage_options == {"account_key": "test_key"}

    def test_init_with_remote_output_path(self):
        """Test initialization with remote output path in config."""
        config = DocsConfig(
            enabled=True,
            output_path="abfss://container@account.dfs.core.windows.net/docs/",
        )

        generator = DocGenerator(
            config=config,
            pipeline_name="test_pipeline",
        )

        assert generator.is_remote is True
        assert "abfss://" in generator.output_path_str

    def test_init_local_path(self):
        """Test initialization with local path remains local."""
        config = DocsConfig(enabled=True, output_path="docs/generated/")

        generator = DocGenerator(
            config=config,
            pipeline_name="test_pipeline",
            workspace_root="/local/path",
        )

        assert generator.is_remote is False
        assert generator.output_path is not None

    def test_write_file_uses_callback_when_provided(self, sample_metadata):
        """Test that write_file callback is used when provided."""
        import tempfile

        written_files = {}

        def mock_write(path: str, content: str) -> None:
            written_files[path] = content

        with tempfile.TemporaryDirectory() as tmpdir:
            config = DocsConfig(
                enabled=True,
                output_path="docs/",
                outputs=DocsOutputConfig(readme=True),
            )

            generator = DocGenerator(
                config=config,
                pipeline_name="test_pipeline",
                workspace_root=tmpdir,
                write_file=mock_write,
            )

            result = generator.generate(sample_metadata)

            # Files should have been written via callback
            assert len(written_files) > 0
            # At least README and state file
            readme_paths = [p for p in written_files.keys() if "README.md" in p]
            assert len(readme_paths) == 1

    def test_get_state_path_remote(self):
        """Test state path construction for remote storage."""
        config = DocsConfig(enabled=True, output_path="docs/generated/")

        generator = DocGenerator(
            config=config,
            pipeline_name="test_pipeline",
            workspace_root="abfss://container@account.dfs.core.windows.net/data",
        )

        state_path = generator._get_state_path()
        assert (
            state_path
            == "abfss://container@account.dfs.core.windows.net/data/docs/generated/.pipelines.json"
        )

    def test_output_path_str_construction(self):
        """Test output path string is correctly constructed for remote."""
        config = DocsConfig(enabled=True, output_path="docs/generated/")

        generator = DocGenerator(
            config=config,
            pipeline_name="test_pipeline",
            workspace_root="abfss://container@account.dfs.core.windows.net/project",
        )

        assert (
            generator.output_path_str
            == "abfss://container@account.dfs.core.windows.net/project/docs/generated/"
        )
