"""Unit tests for NodeExecutor transform phase: step dispatch, SQL, functions, operations.

Covers issue #284 — lines 1388-1907 of node.py.
"""

import inspect
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.config import NodeConfig
from odibi.context import EngineContext
from odibi.exceptions import NodeExecutionError
from odibi.node import NodeExecutor


@pytest.fixture
def mock_context():
    ctx = MagicMock()
    ctx.register = MagicMock()
    ctx.unregister = MagicMock()
    return ctx


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.name = "pandas"
    engine.materialize.side_effect = lambda df: df
    engine.count_rows.side_effect = lambda df: len(df) if df is not None else None
    engine.execute_sql.return_value = pd.DataFrame({"id": [1]})
    engine.execute_operation.return_value = pd.DataFrame({"id": [1]})
    return engine


@pytest.fixture
def connections():
    return {"src": MagicMock(), "dst": MagicMock()}


def _make_executor(mock_context, mock_engine, connections, **kwargs):
    return NodeExecutor(mock_context, mock_engine, connections, **kwargs)


def _make_config(name="test_node", **kwargs):
    base = {"read": {"connection": "src", "format": "csv", "path": "input.csv"}}
    base.update(kwargs)
    return NodeConfig(name=name, **base)


# ============================================================
# _get_step_name
# ============================================================


class TestGetStepName:
    def test_string_step_short(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        result = executor._get_step_name("SELECT * FROM df")
        assert result == "sql:SELECT * FROM df"

    def test_string_step_long_truncated(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        long_sql = "SELECT " + "a, " * 30 + "z FROM df"
        result = executor._get_step_name(long_sql)
        assert result.startswith("sql:")
        assert result.endswith("...")
        assert len(result) <= len("sql:") + 53  # 50 + "..."

    def test_function_step(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        step = MagicMock()
        step.function = "deduplicate"
        step.operation = None
        step.sql = None
        step.sql_file = None
        result = executor._get_step_name(step)
        assert result == "function:deduplicate"

    def test_operation_step(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        step = MagicMock()
        step.function = None
        step.operation = "drop_duplicates"
        step.sql = None
        step.sql_file = None
        result = executor._get_step_name(step)
        assert result == "operation:drop_duplicates"

    def test_sql_file_step(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        step = MagicMock()
        step.function = None
        step.operation = None
        step.sql = None
        step.sql_file = "transform.sql"
        result = executor._get_step_name(step)
        assert result == "sql_file:transform.sql"

    def test_unknown_step(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        step = MagicMock(spec=[])  # no attributes
        result = executor._get_step_name(step)
        assert result == "unknown"


# ============================================================
# _execute_sql_step
# ============================================================


class TestExecuteSqlStep:
    @patch("odibi.node._get_unique_view_name", return_value="tmp_view_123")
    def test_sql_with_current_df(self, mock_view, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        df = pd.DataFrame({"id": [1, 2]})

        executor._execute_sql_step("SELECT * FROM df WHERE id > 0", df)

        mock_context.register.assert_any_call("tmp_view_123", df)
        mock_engine.execute_sql.assert_called_once()
        call_args = mock_engine.execute_sql.call_args
        assert "tmp_view_123" in call_args[0][0]
        assert "df" not in call_args[0][0].split("tmp_view_123")[-1]
        mock_context.unregister.assert_called_with("tmp_view_123")

    def test_sql_without_current_df(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)

        executor._execute_sql_step("SELECT 1 AS val", None)

        mock_engine.execute_sql.assert_called_once_with("SELECT 1 AS val", mock_context)

    def test_executed_sql_tracked(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        executor._execute_sql_step("SELECT 1", None)
        assert "SELECT 1" in executor._executed_sql

    @patch("odibi.node._get_unique_view_name", return_value="tmp_view_456")
    def test_view_unregistered_even_on_error(
        self, mock_view, mock_context, mock_engine, connections
    ):
        executor = _make_executor(mock_context, mock_engine, connections)
        mock_engine.execute_sql.side_effect = RuntimeError("SQL error")
        df = pd.DataFrame({"id": [1]})

        with pytest.raises(RuntimeError, match="SQL error"):
            executor._execute_sql_step("SELECT * FROM df", df)

        mock_context.unregister.assert_called_with("tmp_view_456")


# ============================================================
# _resolve_sql_file
# ============================================================


class TestResolveSqlFile:
    def test_no_config_file_raises(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections, config_file=None)

        with pytest.raises(ValueError, match="config_file path is not available"):
            executor._resolve_sql_file("transform.sql")

    def test_file_not_found_raises(self, mock_context, mock_engine, connections, tmp_path):
        executor = _make_executor(
            mock_context,
            mock_engine,
            connections,
            config_file=str(tmp_path / "pipeline.yaml"),
        )

        with pytest.raises(FileNotFoundError, match="SQL file not found"):
            executor._resolve_sql_file("missing.sql")

    def test_successful_read(self, mock_context, mock_engine, connections, tmp_path):
        sql_file = tmp_path / "transform.sql"
        sql_file.write_text("SELECT * FROM df WHERE active = 1", encoding="utf-8")

        executor = _make_executor(
            mock_context,
            mock_engine,
            connections,
            config_file=str(tmp_path / "pipeline.yaml"),
        )

        result = executor._resolve_sql_file("transform.sql")
        assert result == "SELECT * FROM df WHERE active = 1"


# ============================================================
# _resolve_explanation / _resolve_explanation_file
# ============================================================


class TestResolveExplanation:
    def test_inline_explanation(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(explanation="This node loads sales data")
        result = executor._resolve_explanation(config)
        assert result == "This node loads sales data"

    def test_no_explanation_returns_none(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config()
        result = executor._resolve_explanation(config)
        assert result is None

    def test_explanation_file_resolved(self, mock_context, mock_engine, connections, tmp_path):
        md_file = tmp_path / "explain.md"
        md_file.write_text("# Sales\nLoads daily sales.", encoding="utf-8")

        executor = _make_executor(
            mock_context,
            mock_engine,
            connections,
            config_file=str(tmp_path / "pipeline.yaml"),
        )
        config = _make_config(explanation_file="explain.md")
        result = executor._resolve_explanation(config)
        assert "Sales" in result

    def test_explanation_file_no_config_file_raises(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections, config_file=None)
        with pytest.raises(ValueError, match="config_file path is not available"):
            executor._resolve_explanation_file("explain.md")

    def test_explanation_file_not_found(self, mock_context, mock_engine, connections, tmp_path):
        executor = _make_executor(
            mock_context,
            mock_engine,
            connections,
            config_file=str(tmp_path / "pipeline.yaml"),
        )
        with pytest.raises(FileNotFoundError, match="Explanation file not found"):
            executor._resolve_explanation_file("missing.md")


# ============================================================
# _execute_operation_step
# ============================================================


class TestExecuteOperationStep:
    def test_delegates_to_engine(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        df = pd.DataFrame({"id": [1, 1, 2]})
        expected = pd.DataFrame({"id": [1, 2]})
        mock_engine.execute_operation.return_value = expected

        result = executor._execute_operation_step("drop_duplicates", {"subset": ["id"]}, df)

        mock_engine.materialize.assert_called_with(df)
        mock_engine.execute_operation.assert_called_once_with(
            "drop_duplicates", {"subset": ["id"]}, df
        )
        pd.testing.assert_frame_equal(result, expected)

    def test_none_df_not_materialized(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)

        executor._execute_operation_step("create_empty", {}, None)

        mock_engine.materialize.assert_not_called()
        mock_engine.execute_operation.assert_called_once_with("create_empty", {}, None)


# ============================================================
# _execute_function_step
# ============================================================


class TestExecuteFunctionStep:
    @patch("odibi.registry.FunctionRegistry.get_param_model", return_value=None)
    @patch("odibi.registry.FunctionRegistry.get")
    @patch("odibi.registry.FunctionRegistry.validate_params")
    def test_function_without_param_model(
        self,
        mock_validate,
        mock_get,
        mock_param_model,
        mock_context,
        mock_engine,
        connections,
    ):
        result_df = pd.DataFrame({"out": [1]})
        mock_func = MagicMock(return_value=result_df)
        mock_func.__signature__ = inspect.Signature(
            parameters=[inspect.Parameter("ctx", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        )
        mock_get.return_value = mock_func

        executor = _make_executor(mock_context, mock_engine, connections)
        df = pd.DataFrame({"id": [1]})

        result = executor._execute_function_step("my_func", {"key": "val"}, df, None)

        mock_validate.assert_called_once_with("my_func", {"key": "val"})
        assert result is result_df

    @patch("odibi.registry.FunctionRegistry.get_param_model")
    @patch("odibi.registry.FunctionRegistry.get")
    @patch("odibi.registry.FunctionRegistry.validate_params")
    def test_function_with_param_model(
        self,
        mock_validate,
        mock_get,
        mock_param_model,
        mock_context,
        mock_engine,
        connections,
    ):
        result_df = pd.DataFrame({"out": [1]})
        mock_func = MagicMock(return_value=result_df)
        mock_func.__signature__ = inspect.Signature(
            parameters=[inspect.Parameter("ctx", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        )
        mock_get.return_value = mock_func

        param_cls = MagicMock()
        param_obj = MagicMock()
        param_cls.return_value = param_obj
        mock_param_model.return_value = param_cls

        executor = _make_executor(mock_context, mock_engine, connections)

        result = executor._execute_function_step("my_func", {"key": "val"}, None, None)

        param_cls.assert_called_once_with(key="val")
        mock_func.assert_called_once()
        assert result is result_df

    @patch("odibi.registry.FunctionRegistry.get_param_model", return_value=None)
    @patch("odibi.registry.FunctionRegistry.get")
    @patch("odibi.registry.FunctionRegistry.validate_params")
    def test_function_with_current_param(
        self,
        mock_validate,
        mock_get,
        mock_param_model,
        mock_context,
        mock_engine,
        connections,
    ):
        result_df = pd.DataFrame({"out": [1]})
        mock_func = MagicMock(return_value=result_df)
        mock_func.__signature__ = inspect.Signature(
            parameters=[
                inspect.Parameter("ctx", inspect.Parameter.POSITIONAL_OR_KEYWORD),
                inspect.Parameter("current", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            ]
        )
        mock_get.return_value = mock_func

        executor = _make_executor(mock_context, mock_engine, connections)
        df = pd.DataFrame({"id": [1]})

        executor._execute_function_step("my_func", {}, df, None)

        _, kwargs = mock_func.call_args
        assert "current" in kwargs

    @patch("odibi.registry.FunctionRegistry.get_param_model", return_value=None)
    @patch("odibi.registry.FunctionRegistry.get")
    @patch("odibi.registry.FunctionRegistry.validate_params")
    def test_engine_context_result_extracts_df(
        self,
        mock_validate,
        mock_get,
        mock_param_model,
        mock_context,
        mock_engine,
        connections,
    ):
        expected_df = pd.DataFrame({"result": [42]})
        engine_ctx = MagicMock(spec=EngineContext)
        engine_ctx.df = expected_df
        engine_ctx._sql_history = []
        mock_func = MagicMock(return_value=engine_ctx)
        mock_func.__signature__ = inspect.Signature(
            parameters=[inspect.Parameter("ctx", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        )
        mock_get.return_value = mock_func

        executor = _make_executor(mock_context, mock_engine, connections)

        result = executor._execute_function_step("my_func", {}, None, None)

        assert result is expected_df

    @patch("odibi.registry.FunctionRegistry.get_param_model")
    @patch("odibi.registry.FunctionRegistry.get")
    @patch("odibi.registry.FunctionRegistry.validate_params")
    def test_invalid_params_raises(
        self,
        mock_validate,
        mock_get,
        mock_param_model,
        mock_context,
        mock_engine,
        connections,
    ):
        mock_func = MagicMock()
        mock_func.__signature__ = inspect.Signature(
            parameters=[inspect.Parameter("ctx", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        )
        mock_get.return_value = mock_func

        param_cls = MagicMock(side_effect=TypeError("bad param"))
        mock_param_model.return_value = param_cls

        executor = _make_executor(mock_context, mock_engine, connections)

        with pytest.raises(ValueError, match="Invalid parameters"):
            executor._execute_function_step("my_func", {"bad": "val"}, None, None)

    @patch("odibi.registry.FunctionRegistry.get_param_model", return_value=None)
    @patch("odibi.registry.FunctionRegistry.get")
    @patch("odibi.registry.FunctionRegistry.validate_params")
    def test_sql_history_captured(
        self,
        mock_validate,
        mock_get,
        mock_param_model,
        mock_context,
        mock_engine,
        connections,
    ):
        # The function modifies the EngineContext passed TO it (the internal one),
        # so we make the mock inject sql_history on the ctx argument.
        def side_effect(ctx, **kwargs):
            ctx._sql_history = ["SELECT 1", "SELECT 2"]
            return pd.DataFrame({"id": [1]})

        mock_func = MagicMock(side_effect=side_effect)
        mock_func.__signature__ = inspect.Signature(
            parameters=[inspect.Parameter("ctx", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        )
        mock_get.return_value = mock_func

        executor = _make_executor(mock_context, mock_engine, connections)
        executor._execute_function_step("my_func", {}, None, None)

        assert "SELECT 1" in executor._executed_sql
        assert "SELECT 2" in executor._executed_sql


# ============================================================
# _execute_transformer_node
# ============================================================


class TestExecuteTransformerNode:
    @patch("odibi.registry.FunctionRegistry.get_param_model", return_value=None)
    @patch("odibi.registry.FunctionRegistry.get")
    @patch("odibi.registry.FunctionRegistry.validate_params")
    def test_basic_transformer(
        self,
        mock_validate,
        mock_get,
        mock_param_model,
        mock_context,
        mock_engine,
        connections,
    ):
        result_df = pd.DataFrame({"out": [1]})
        mock_func = MagicMock(return_value=result_df)
        mock_func.__signature__ = inspect.Signature(
            parameters=[inspect.Parameter("ctx", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        )
        mock_get.return_value = mock_func

        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(transformer="deduplicate", params={"subset": ["id"]})
        df = pd.DataFrame({"id": [1, 1]})

        result = executor._execute_transformer_node(config, df)

        mock_validate.assert_called_once()
        assert result is result_df

    @patch("odibi.registry.FunctionRegistry.get_param_model", return_value=None)
    @patch("odibi.registry.FunctionRegistry.get")
    @patch("odibi.registry.FunctionRegistry.validate_params")
    def test_merge_transformer_with_table_properties(
        self,
        mock_validate,
        mock_get,
        mock_param_model,
        mock_context,
        mock_engine,
        connections,
    ):
        result_df = pd.DataFrame({"out": [1]})
        mock_func = MagicMock(return_value=result_df)
        mock_func.__signature__ = inspect.Signature(
            parameters=[inspect.Parameter("ctx", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        )
        mock_get.return_value = mock_func

        perf_config = MagicMock()
        perf_config.delta_table_properties = {"prop1": "val1"}

        executor = _make_executor(
            mock_context, mock_engine, connections, performance_config=perf_config
        )
        config = _make_config(transformer="merge", params={"keys": ["id"], "target": "/t"})

        executor._execute_transformer_node(config, None)

        call_args = mock_validate.call_args
        assert call_args[0][1].get("table_properties") == {"prop1": "val1"}

    @patch("odibi.registry.FunctionRegistry.get_param_model", return_value=None)
    @patch("odibi.registry.FunctionRegistry.get")
    @patch("odibi.registry.FunctionRegistry.validate_params")
    def test_engine_context_result(
        self,
        mock_validate,
        mock_get,
        mock_param_model,
        mock_context,
        mock_engine,
        connections,
    ):
        expected_df = pd.DataFrame({"result": [42]})
        engine_ctx = MagicMock(spec=EngineContext)
        engine_ctx.df = expected_df
        engine_ctx._sql_history = []
        mock_func = MagicMock(return_value=engine_ctx)
        mock_func.__signature__ = inspect.Signature(
            parameters=[inspect.Parameter("ctx", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        )
        mock_get.return_value = mock_func

        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(transformer="my_transform", params={"x": 1})

        result = executor._execute_transformer_node(config, None)

        assert result is expected_df


# ============================================================
# _execute_transform (step chain with error handling)
# ============================================================


class TestExecuteTransform:
    def test_single_sql_step(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            transform={"steps": [{"sql": "SELECT * FROM df"}]},
        )
        df = pd.DataFrame({"id": [1]})

        result = executor._execute_transform(config, df)

        assert result is not None

    def test_multiple_steps_chain(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            transform={
                "steps": [
                    {"sql": "SELECT * FROM df"},
                    {"sql": "SELECT * FROM df WHERE id > 0"},
                ]
            },
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_transform(config, df)

        assert mock_engine.execute_sql.call_count == 2

    @patch("odibi.registry.FunctionRegistry.get_param_model", return_value=None)
    @patch("odibi.registry.FunctionRegistry.get")
    @patch("odibi.registry.FunctionRegistry.validate_params")
    def test_function_step_dispatch(
        self,
        mock_validate,
        mock_get,
        mock_param_model,
        mock_context,
        mock_engine,
        connections,
    ):
        result_df = pd.DataFrame({"out": [1]})
        mock_func = MagicMock(return_value=result_df)
        mock_func.__signature__ = inspect.Signature(
            parameters=[inspect.Parameter("ctx", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        )
        mock_get.return_value = mock_func

        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            transform={"steps": [{"function": "my_func", "params": {"x": 1}}]},
        )
        df = pd.DataFrame({"id": [1]})

        result = executor._execute_transform(config, df)

        mock_get.assert_called_with("my_func")
        assert result is result_df

    def test_operation_step_dispatch(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            transform={"steps": [{"operation": "drop_duplicates", "params": {"subset": ["id"]}}]},
        )
        df = pd.DataFrame({"id": [1, 1]})

        executor._execute_transform(config, df)

        mock_engine.execute_operation.assert_called_once()

    def test_sql_file_step_dispatch(self, mock_context, mock_engine, connections, tmp_path):
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT * FROM df", encoding="utf-8")

        executor = _make_executor(
            mock_context,
            mock_engine,
            connections,
            config_file=str(tmp_path / "pipeline.yaml"),
        )
        config = _make_config(
            transform={"steps": [{"sql_file": "query.sql"}]},
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_transform(config, df)

        assert mock_engine.execute_sql.call_count == 1

    def test_error_raises_node_execution_error(self, mock_context, mock_engine, connections):
        mock_engine.execute_sql.side_effect = RuntimeError("SQL syntax error")

        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            transform={"steps": [{"sql": "INVALID SQL"}]},
        )
        df = pd.DataFrame({"id": [1]})

        with pytest.raises(NodeExecutionError):
            executor._execute_transform(config, df)

    def test_none_transform_config_returns_df(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config()
        config_copy = config.model_copy(update={"transform": None})
        df = pd.DataFrame({"id": [1]})

        result = executor._execute_transform(config_copy, df)
        assert result is df


# ============================================================
# _execute_transform_phase (full orchestration)
# ============================================================


class TestExecuteTransformPhase:
    def test_input_dataframes_registered_in_context(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config()
        result_df = pd.DataFrame({"id": [1]})
        input_dfs = {"orders": pd.DataFrame({"oid": [10]})}

        executor._execute_transform_phase(config, result_df, None, input_dataframes=input_dfs)

        mock_context.register.assert_any_call("orders", input_dfs["orders"])

    @patch("odibi.patterns.get_pattern_class")
    def test_pattern_engine_path(self, mock_get_pattern, mock_context, mock_engine, connections):
        pattern_cls = MagicMock()
        pattern_instance = MagicMock()
        pattern_instance.execute.return_value = pd.DataFrame({"out": [1]})
        pattern_cls.return_value = pattern_instance
        mock_get_pattern.return_value = pattern_cls

        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            transformer="dimension", params={"natural_key": "id", "surrogate_key": "id_sk"}
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_transform_phase(config, df, None)

        pattern_instance.validate.assert_called_once()
        pattern_instance.execute.assert_called_once()
        assert any("pattern" in s.lower() for s in executor._execution_steps)

    @patch("odibi.patterns.get_pattern_class", side_effect=ValueError("not a pattern"))
    @patch("odibi.registry.FunctionRegistry.get_param_model", return_value=None)
    @patch("odibi.registry.FunctionRegistry.get")
    @patch("odibi.registry.FunctionRegistry.validate_params")
    def test_non_pattern_transformer_fallthrough(
        self,
        mock_validate,
        mock_get,
        mock_param_model,
        mock_get_pattern,
        mock_context,
        mock_engine,
        connections,
    ):
        result_df = pd.DataFrame({"out": [1]})
        mock_func = MagicMock(return_value=result_df)
        mock_func.__signature__ = inspect.Signature(
            parameters=[inspect.Parameter("ctx", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        )
        mock_get.return_value = mock_func

        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(transformer="custom_transform", params={"x": 1})
        df = pd.DataFrame({"id": [1]})

        executor._execute_transform_phase(config, df, None)

        assert any("transformer" in s.lower() for s in executor._execution_steps)

    @patch("odibi.patterns.get_pattern_class")
    def test_pattern_with_catalog_logs_pattern(
        self,
        mock_get_pattern,
        mock_context,
        mock_engine,
        connections,
    ):
        pattern_cls = MagicMock()
        pattern_instance = MagicMock()
        pattern_instance.execute.return_value = pd.DataFrame({"out": [1]})
        pattern_cls.return_value = pattern_instance
        mock_get_pattern.return_value = pattern_cls

        catalog = MagicMock()
        executor = _make_executor(mock_context, mock_engine, connections, catalog_manager=catalog)
        config = _make_config(
            transformer="dimension",
            params={"natural_key": "id", "surrogate_key": "id_sk"},
            write={"connection": "dst", "format": "csv", "path": "out.csv"},
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_transform_phase(config, df, None)

        catalog.log_pattern.assert_called_once()

    def test_transform_steps_path(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            transform={"steps": [{"sql": "SELECT * FROM df"}]},
        )
        df = pd.DataFrame({"id": [1]})

        result = executor._execute_transform_phase(config, df, None)

        assert result is not None
        assert any("transform" in s.lower() for s in executor._execution_steps)

    def test_privacy_suite_anonymizes_pii(self, mock_context, mock_engine, connections):
        executor = _make_executor(mock_context, mock_engine, connections)
        anonymized_df = pd.DataFrame({"email": ["***"]})
        mock_engine.anonymize.return_value = anonymized_df

        config = _make_config(
            privacy={"method": "hash"},
            columns={"email": {"pii": True}},
        )
        df = pd.DataFrame({"email": ["test@example.com"]})

        executor._execute_transform_phase(config, df, None)

        mock_engine.anonymize.assert_called_once()
        assert any("anonymized" in s.lower() for s in executor._execution_steps)

    def test_result_df_none_uses_input_df_for_transformer(
        self,
        mock_context,
        mock_engine,
        connections,
    ):
        """When result_df is None but input_df is provided, transformer uses input_df."""
        executor = _make_executor(mock_context, mock_engine, connections)

        # Use a non-pattern transformer to avoid complex mocking
        with patch("odibi.patterns.get_pattern_class", side_effect=ValueError("not a pattern")):
            with patch("odibi.registry.FunctionRegistry.validate_params"):
                mock_func = MagicMock(return_value=pd.DataFrame({"out": [1]}))
                mock_func.__signature__ = inspect.Signature(
                    parameters=[inspect.Parameter("ctx", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
                )
                with patch("odibi.registry.FunctionRegistry.get", return_value=mock_func):
                    with patch(
                        "odibi.registry.FunctionRegistry.get_param_model", return_value=None
                    ):
                        config = _make_config(transformer="my_transform", params={"x": 1})
                        input_df = pd.DataFrame({"id": [1]})

                        executor._execute_transform_phase(config, None, input_df)

                        # The transformer should have been called
                        mock_func.assert_called_once()

    def test_write_path_set_on_engine_for_transforms(
        self,
        mock_context,
        mock_engine,
        connections,
    ):
        executor = _make_executor(mock_context, mock_engine, connections)
        config = _make_config(
            transform={"steps": [{"sql": "SELECT * FROM df"}]},
            write={"connection": "dst", "format": "csv", "path": "output.csv"},
        )
        df = pd.DataFrame({"id": [1]})

        executor._execute_transform_phase(config, df, None)

        assert mock_engine._current_write_path == "output.csv"
