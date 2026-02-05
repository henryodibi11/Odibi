"""Tests for explanation_file resolution in NodeConfig."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.config import NodeConfig, TransformConfig
from odibi.node import NodeExecutor


@pytest.fixture
def mock_context():
    ctx = MagicMock()
    ctx.register = MagicMock()
    ctx.get = MagicMock(return_value=None)
    return ctx


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.name = "pandas"
    engine.execute_sql = MagicMock(return_value=pd.DataFrame({"id": [1, 2], "value": [10, 20]}))
    engine.materialize = MagicMock(side_effect=lambda x: x)
    return engine


@pytest.fixture
def connections():
    return {"src": MagicMock(), "dst": MagicMock()}


class TestExplanationFileResolution:
    """Test _resolve_explanation_file method."""

    def test_resolve_explanation_file_success(self, mock_context, mock_engine, connections):
        """explanation_file resolves correctly relative to config file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "odibi.yaml"
            docs_dir = Path(tmpdir) / "docs"
            docs_dir.mkdir()
            explanation_file = docs_dir / "node_explanation.md"
            explanation_file.write_text(
                "## Business Logic\n\nThis node processes sales data.", encoding="utf-8"
            )

            executor = NodeExecutor(
                mock_context,
                mock_engine,
                connections,
                config_file=str(config_file),
            )

            result = executor._resolve_explanation_file("docs/node_explanation.md")
            assert "## Business Logic" in result
            assert "sales data" in result

    def test_resolve_explanation_file_nested_path(self, mock_context, mock_engine, connections):
        """explanation_file resolves nested paths like pipelines/silver/docs/file.md."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "odibi.yaml"
            docs_path = Path(tmpdir) / "pipelines" / "silver" / "docs"
            docs_path.mkdir(parents=True)
            explanation_file = docs_path / "fact_sales.md"
            explanation_file.write_text(
                "# Fact Sales\n\n| Column | Type |\n|--------|------|\n| id | int |"
            )

            executor = NodeExecutor(
                mock_context,
                mock_engine,
                connections,
                config_file=str(config_file),
            )

            result = executor._resolve_explanation_file("pipelines/silver/docs/fact_sales.md")
            assert "# Fact Sales" in result
            assert "| Column | Type |" in result

    def test_resolve_explanation_file_not_found(self, mock_context, mock_engine, connections):
        """explanation_file raises FileNotFoundError with helpful message."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "odibi.yaml"

            executor = NodeExecutor(
                mock_context,
                mock_engine,
                connections,
                config_file=str(config_file),
            )

            with pytest.raises(FileNotFoundError) as exc_info:
                executor._resolve_explanation_file("docs/nonexistent.md")

            error_msg = str(exc_info.value)
            assert "docs/nonexistent.md" in error_msg
            assert "not found" in error_msg.lower()

    def test_resolve_explanation_file_no_config_file(self, mock_context, mock_engine, connections):
        """explanation_file raises ValueError if config_file not set."""
        executor = NodeExecutor(
            mock_context,
            mock_engine,
            connections,
            config_file=None,
        )

        with pytest.raises(ValueError) as exc_info:
            executor._resolve_explanation_file("docs/explanation.md")

        assert "config_file" in str(exc_info.value).lower()


class TestResolveExplanation:
    """Test _resolve_explanation method that handles both inline and file."""

    def test_resolve_inline_explanation(self, mock_context, mock_engine, connections):
        """Inline explanation is returned directly."""
        executor = NodeExecutor(mock_context, mock_engine, connections)

        config = NodeConfig(
            name="test_node",
            explanation="This is an inline explanation.",
            transform=TransformConfig(steps=["SELECT * FROM df"]),
        )

        result = executor._resolve_explanation(config)
        assert result == "This is an inline explanation."

    def test_resolve_explanation_file(self, mock_context, mock_engine, connections):
        """explanation_file content is loaded and returned."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "odibi.yaml"
            docs_dir = Path(tmpdir) / "docs"
            docs_dir.mkdir()
            explanation_file = docs_dir / "node_docs.md"
            explanation_file.write_text("## External Documentation\n\nLoaded from file.")

            executor = NodeExecutor(
                mock_context,
                mock_engine,
                connections,
                config_file=str(config_file),
            )

            config = NodeConfig(
                name="test_node",
                explanation_file="docs/node_docs.md",
                transform=TransformConfig(steps=["SELECT * FROM df"]),
            )

            result = executor._resolve_explanation(config)
            assert "## External Documentation" in result
            assert "Loaded from file" in result

    def test_resolve_no_explanation(self, mock_context, mock_engine, connections):
        """Returns None when no explanation is set."""
        executor = NodeExecutor(mock_context, mock_engine, connections)

        config = NodeConfig(
            name="test_node",
            transform=TransformConfig(steps=["SELECT * FROM df"]),
        )

        result = executor._resolve_explanation(config)
        assert result is None


class TestExplanationExclusivity:
    """Test that explanation and explanation_file are mutually exclusive."""

    def test_explanation_and_explanation_file_mutually_exclusive(self):
        """Cannot have both explanation and explanation_file."""
        with pytest.raises(ValueError) as exc_info:
            NodeConfig(
                name="test_node",
                explanation="Inline explanation",
                explanation_file="docs/explanation.md",
                transform=TransformConfig(steps=["SELECT * FROM df"]),
            )

        error_msg = str(exc_info.value)
        assert "explanation" in error_msg.lower()
        assert "explanation_file" in error_msg.lower()
        assert "cannot have both" in error_msg.lower() or "mutually exclusive" in error_msg.lower()

    def test_explanation_only_is_valid(self):
        """explanation alone is valid."""
        config = NodeConfig(
            name="test_node",
            explanation="Inline explanation",
            transform=TransformConfig(steps=["SELECT * FROM df"]),
        )
        assert config.explanation == "Inline explanation"
        assert config.explanation_file is None

    def test_explanation_file_only_is_valid(self):
        """explanation_file alone is valid."""
        config = NodeConfig(
            name="test_node",
            explanation_file="docs/explanation.md",
            transform=TransformConfig(steps=["SELECT * FROM df"]),
        )
        assert config.explanation_file == "docs/explanation.md"
        assert config.explanation is None


class TestExplanationInMetadata:
    """Test that explanation is correctly included in node metadata."""

    def test_metadata_includes_resolved_explanation(self, mock_context, mock_engine, connections):
        """Node execution metadata includes resolved explanation from file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "odibi.yaml"
            docs_dir = Path(tmpdir) / "docs"
            docs_dir.mkdir()
            explanation_file = docs_dir / "fact_sales.md"
            explanation_file.write_text("## Sales Fact\n\nBusiness documentation here.")

            mock_engine.read = MagicMock(return_value=pd.DataFrame({"id": [1, 2]}))
            mock_engine.execute_sql = MagicMock(
                return_value=pd.DataFrame({"id": [1, 2], "value": [10, 20]})
            )

            executor = NodeExecutor(
                mock_context,
                mock_engine,
                connections,
                config_file=str(config_file),
            )

            config = NodeConfig(
                name="fact_sales",
                explanation_file="docs/fact_sales.md",
                transform=TransformConfig(steps=["SELECT * FROM df"]),
            )

            input_df = pd.DataFrame({"id": [1, 2]})

            with patch.object(executor, "_execute_read_phase", return_value=(input_df, None)):
                result = executor.execute(config)

            assert result.success
            assert result.metadata is not None
            assert "## Sales Fact" in result.metadata.get("explanation", "")
            assert "Business documentation here" in result.metadata.get("explanation", "")
