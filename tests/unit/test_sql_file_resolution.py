"""Tests for sql_file resolution in transform steps and read config."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.config import NodeConfig, ReadConfig, TransformConfig, TransformStep
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


class TestSqlFileResolution:
    """Test _resolve_sql_file method."""

    def test_resolve_sql_file_success(self, mock_context, mock_engine, connections):
        """sql_file resolves correctly relative to config file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "odibi.yaml"
            sql_dir = Path(tmpdir) / "sql"
            sql_dir.mkdir()
            sql_file = sql_dir / "transform.sql"
            sql_file.write_text("SELECT * FROM df WHERE id > 0", encoding="utf-8")

            executor = NodeExecutor(
                mock_context,
                mock_engine,
                connections,
                config_file=str(config_file),
            )

            result = executor._resolve_sql_file("sql/transform.sql")
            assert result == "SELECT * FROM df WHERE id > 0"

    def test_resolve_sql_file_nested_path(self, mock_context, mock_engine, connections):
        """sql_file resolves nested paths like pipelines/silver/sql/file.sql."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "odibi.yaml"
            sql_path = Path(tmpdir) / "pipelines" / "silver" / "sql"
            sql_path.mkdir(parents=True)
            sql_file = sql_path / "aggregate.sql"
            sql_file.write_text("SELECT id, SUM(value) FROM df GROUP BY id")

            executor = NodeExecutor(
                mock_context,
                mock_engine,
                connections,
                config_file=str(config_file),
            )

            result = executor._resolve_sql_file("pipelines/silver/sql/aggregate.sql")
            assert "SUM(value)" in result

    def test_resolve_sql_file_not_found(self, mock_context, mock_engine, connections):
        """sql_file raises FileNotFoundError with helpful message."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "odibi.yaml"

            executor = NodeExecutor(
                mock_context,
                mock_engine,
                connections,
                config_file=str(config_file),
            )

            with pytest.raises(FileNotFoundError) as exc_info:
                executor._resolve_sql_file("sql/nonexistent.sql")

            error_msg = str(exc_info.value)
            assert "sql/nonexistent.sql" in error_msg
            assert "not found" in error_msg.lower()

    def test_resolve_sql_file_no_config_file(self, mock_context, mock_engine, connections):
        """sql_file raises ValueError if config_file not set."""
        executor = NodeExecutor(
            mock_context,
            mock_engine,
            connections,
            config_file=None,
        )

        with pytest.raises(ValueError) as exc_info:
            executor._resolve_sql_file("sql/transform.sql")

        assert "config_file" in str(exc_info.value).lower()


class TestSqlFileExecution:
    """Test sql_file step execution in transform pipeline."""

    def test_sql_file_step_executes(self, mock_context, mock_engine, connections):
        """sql_file step loads and executes SQL content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "odibi.yaml"
            sql_dir = Path(tmpdir) / "sql"
            sql_dir.mkdir()
            sql_file = sql_dir / "transform.sql"
            sql_file.write_text("SELECT * FROM df WHERE active = true")

            executor = NodeExecutor(
                mock_context,
                mock_engine,
                connections,
                config_file=str(config_file),
            )

            step = TransformStep(sql_file="sql/transform.sql")
            config = NodeConfig(
                name="test_node",
                transform=TransformConfig(steps=[step]),
            )

            input_df = pd.DataFrame({"id": [1], "active": [True]})

            with patch.object(executor, "_execute_read_phase", return_value=(input_df, None)):
                result = executor.execute(config)

            assert result.success
            mock_engine.execute_sql.assert_called()
            call_args = mock_engine.execute_sql.call_args[0]
            # 'df' is replaced with unique view name for thread-safety
            assert "WHERE active = true" in call_args[0]
            assert "_df_" in call_args[0]  # unique view name pattern

    def test_mixed_sql_and_sql_file_steps(self, mock_context, mock_engine, connections):
        """Can mix inline sql and sql_file steps in same transform."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "odibi.yaml"
            sql_dir = Path(tmpdir) / "sql"
            sql_dir.mkdir()
            sql_file = sql_dir / "aggregate.sql"
            sql_file.write_text("SELECT id, COUNT(*) as cnt FROM df GROUP BY id")

            executor = NodeExecutor(
                mock_context,
                mock_engine,
                connections,
                config_file=str(config_file),
            )

            config = NodeConfig(
                name="test_node",
                transform=TransformConfig(
                    steps=[
                        "SELECT * FROM df WHERE status = 'ACTIVE'",
                        TransformStep(sql_file="sql/aggregate.sql"),
                    ]
                ),
            )

            input_df = pd.DataFrame({"id": [1, 2], "status": ["ACTIVE", "INACTIVE"]})

            with patch.object(executor, "_execute_read_phase", return_value=(input_df, None)):
                result = executor.execute(config)

            assert result.success
            assert mock_engine.execute_sql.call_count >= 2


class TestGetStepName:
    """Test _get_step_name includes sql_file."""

    def test_step_name_for_sql_file(self, mock_context, mock_engine, connections):
        """_get_step_name returns sql_file path."""
        executor = NodeExecutor(mock_context, mock_engine, connections)

        step = TransformStep(sql_file="pipelines/silver/sql/transform.sql")
        name = executor._get_step_name(step)

        assert name == "sql_file:pipelines/silver/sql/transform.sql"


class TestReadConfigSqlFile:
    """Test sql_file support in ReadConfig."""

    def test_read_sql_file_resolves_correctly(self, mock_context, mock_engine, connections):
        """sql_file in read config resolves and sets query in options."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "odibi.yaml"
            sql_dir = Path(tmpdir) / "sql"
            sql_dir.mkdir()
            sql_file = sql_dir / "read_query.sql"
            sql_file.write_text("SELECT * FROM customers WHERE active = 1", encoding="utf-8")

            mock_engine.read = MagicMock(return_value=pd.DataFrame({"id": [1, 2]}))

            executor = NodeExecutor(
                mock_context,
                mock_engine,
                connections,
                config_file=str(config_file),
            )

            config = NodeConfig(
                name="test_read_sql_file",
                read=ReadConfig(
                    connection="src",
                    format="sql",
                    table="customers",
                    sql_file="sql/read_query.sql",
                ),
            )

            result = executor.execute(config)

            assert result.success
            mock_engine.read.assert_called_once()
            call_kwargs = mock_engine.read.call_args[1]
            assert call_kwargs["options"]["query"] == "SELECT * FROM customers WHERE active = 1"

    def test_read_query_and_sql_file_mutually_exclusive(self):
        """query and sql_file cannot both be specified."""
        with pytest.raises(ValueError) as exc_info:
            ReadConfig(
                connection="src",
                format="sql",
                table="customers",
                query="SELECT * FROM customers",
                sql_file="sql/query.sql",
            )

        error_msg = str(exc_info.value)
        assert "mutually exclusive" in error_msg.lower()
        assert "query" in error_msg
        assert "sql_file" in error_msg

    def test_read_sql_file_only_is_valid(self):
        """sql_file alone (without query) is valid."""
        config = ReadConfig(
            connection="src",
            format="sql",
            table="customers",
            sql_file="sql/query.sql",
        )
        assert config.sql_file == "sql/query.sql"
        assert config.query is None

    def test_read_query_only_is_valid(self):
        """query alone (without sql_file) is valid."""
        config = ReadConfig(
            connection="src",
            format="sql",
            table="customers",
            query="SELECT * FROM customers",
        )
        assert config.query == "SELECT * FROM customers"
        assert config.sql_file is None
