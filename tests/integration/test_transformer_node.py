import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from odibi.node import Node
from odibi.config import NodeConfig
from odibi.context import PandasContext
from odibi.registry import FunctionRegistry
import odibi.transformers.merge_transformer  # Auto-register merge transformer


class TestTransformerNodeIntegration:
    @pytest.fixture
    def context(self):
        return PandasContext()

    @pytest.fixture
    def engine(self):
        return MagicMock()  # Mock engine

    @pytest.fixture
    def connections(self):
        return {"local_dev": MagicMock()}

    def test_node_calls_transformer(self, context, engine, connections):
        # Register a mock transformer
        mock_transformer = MagicMock(return_value=pd.DataFrame({"res": [1]}))
        # Mock name for registry
        mock_transformer.__name__ = "mock_test_transformer"

        registry = FunctionRegistry
        registry.register(mock_transformer, name="mock_test_transformer")

        # Create Node Config
        config = NodeConfig(
            name="test_node",
            transformer="mock_test_transformer",
            params={"foo": "bar"},
            cache=False,
        )

        # Create Node
        node = Node(config, context, engine, connections)

        # Register Input (from dependency or just mock context has it?
        # Transformer usually takes 'current' which comes from dependency or previous step.
        # If no dependency, input_df is None.

        # Execute
        result = node.execute()

        assert result.success

        # Verify transformer called
        mock_transformer.assert_called_once()

        # Verify params passed
        # The first arg is context (now wrapped in EngineContext), second is current (None), kwargs are params
        # Since Node wraps context, we need to unwrap or check type
        call_args = mock_transformer.call_args
        # Check if it's EngineContext wrapping our mock context
        from odibi.context import EngineContext

        passed_context = call_args[0][0]
        assert isinstance(passed_context, EngineContext)
        assert passed_context.context == context

        assert call_args[1]["current"] is None
        assert call_args[1]["foo"] == "bar"

        # Cleanup
        if "mock_test_transformer" in registry._functions:
            del registry._functions["mock_test_transformer"]

    def test_node_calls_merge_transformer(self, context, engine, connections):
        # Explicitly register merge for this test to ensure availability
        import odibi.transformers.merge_transformer as mt
        from odibi.registry import FunctionRegistry

        # Force re-registration if needed, or just ensure it is registered
        # The import above should trigger decorator, but let's be explicit if previous tests messed it up
        if "merge" not in FunctionRegistry._functions:
            FunctionRegistry.register(mt.merge, name="merge")

        # This tests that 'merge' is registered and callable via Node
        # We don't need to mock it, but we will mock the internal pandas merge logic
        # by mocking _merge_pandas via patch if we wanted strict isolation,
        # but here we just want to ensure pipeline finds 'merge'.

        # Create a dummy input DF
        input_df = pd.DataFrame({"id": [1], "val": ["a"]})

        # Register in context so it can be picked up if we had dependencies
        # But here we pass it manually via _execute_transformer_node if we were testing internal method
        # For full execute(), we need input_df.
        # Node.execute() logic:
        # if config.transformer:
        #    if result_df is None and input_df is not None: result_df = input_df
        #    result_df = _execute_transformer_node(result_df)

        # So we need to simulate input_df coming from somewhere.
        # 1. config.depends_on

        context.register("upstream_node", input_df)

        config = NodeConfig(
            name="merge_node",
            depends_on=["upstream_node"],
            transformer="merge",
            params={"target": "mock_target", "keys": ["id"], "strategy": "upsert"},
        )

        # We need to patch _merge_pandas to avoid actual file I/O and dep issues
        with patch("odibi.transformers.merge_transformer._merge_pandas") as mock_merge_impl:
            mock_merge_impl.return_value = input_df

            node = Node(config, context, engine, connections)
            result = node.execute()

            assert result.success
            mock_merge_impl.assert_called_once()

            # Verify 'merge' was found in registry (implied by success call to impl)
