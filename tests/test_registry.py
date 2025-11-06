"""Tests for function registry and @transform decorator."""

import pytest
import pandas as pd

from odibi.registry import (
    FunctionRegistry,
    transform,
    get_registered_function,
    validate_function_params,
)
from odibi.context import PandasContext


class TestTransformDecorator:
    """Test @transform decorator."""
    
    def setup_method(self):
        """Clear registry before each test."""
        FunctionRegistry._functions.clear()
        FunctionRegistry._signatures.clear()
    
    def test_decorator_registers_function(self):
        """@transform decorator registers function."""
        @transform
        def my_transform(context, param1: str):
            return pd.DataFrame({"result": [param1]})
        
        assert "my_transform" in FunctionRegistry.list_functions()
    
    def test_decorated_function_still_callable(self):
        """Decorated function can still be called normally."""
        @transform
        def double_value(context, value: int):
            return value * 2
        
        ctx = PandasContext()
        result = double_value(ctx, 5)
        assert result == 10
    
    def test_can_register_multiple_functions(self):
        """Can register multiple transform functions."""
        @transform
        def func1(context):
            pass
        
        @transform
        def func2(context):
            pass
        
        functions = FunctionRegistry.list_functions()
        assert "func1" in functions
        assert "func2" in functions


class TestFunctionRegistry:
    """Test FunctionRegistry class."""
    
    def setup_method(self):
        """Clear registry before each test."""
        FunctionRegistry._functions.clear()
        FunctionRegistry._signatures.clear()
    
    def test_get_registered_function(self):
        """Can retrieve registered function."""
        @transform
        def my_func(context):
            return "test"
        
        func = FunctionRegistry.get("my_func")
        assert func is not None
        assert callable(func)
    
    def test_get_unregistered_function_raises_error(self):
        """Getting unregistered function raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FunctionRegistry.get("does_not_exist")
        
        error_msg = str(exc_info.value)
        assert "does_not_exist" in error_msg
        assert "not registered" in error_msg
    
    def test_error_shows_available_functions(self):
        """Error message lists available functions."""
        @transform
        def available_func(context):
            pass
        
        with pytest.raises(ValueError) as exc_info:
            FunctionRegistry.get("missing_func")
        
        error_msg = str(exc_info.value)
        assert "available_func" in error_msg.lower()
    
    def test_list_functions_returns_all_names(self):
        """list_functions() returns all registered names."""
        @transform
        def func_a(context):
            pass
        
        @transform
        def func_b(context):
            pass
        
        names = FunctionRegistry.list_functions()
        assert set(names) == {"func_a", "func_b"}


class TestParameterValidation:
    """Test parameter validation against function signatures."""
    
    def setup_method(self):
        """Clear registry before each test."""
        FunctionRegistry._functions.clear()
        FunctionRegistry._signatures.clear()
    
    def test_validate_required_parameters(self):
        """Validation passes when all required params provided."""
        @transform
        def my_func(context, required_param: str, required_int: int):
            pass
        
        # Should not raise
        FunctionRegistry.validate_params(
            "my_func",
            {"required_param": "value", "required_int": 42}
        )
    
    def test_validate_catches_missing_required_param(self):
        """Validation fails when required param is missing."""
        @transform
        def my_func(context, required_param: str):
            pass
        
        with pytest.raises(ValueError) as exc_info:
            FunctionRegistry.validate_params("my_func", {})
        
        error_msg = str(exc_info.value)
        assert "Missing required parameters" in error_msg
        assert "required_param" in error_msg
    
    def test_validate_allows_optional_params(self):
        """Validation passes when optional params omitted."""
        @transform
        def my_func(context, required: str, optional: int = 10):
            pass
        
        # Should not raise (optional can be omitted)
        FunctionRegistry.validate_params("my_func", {"required": "value"})
    
    def test_validate_catches_unexpected_params(self):
        """Validation fails when unexpected params provided."""
        @transform
        def my_func(context, expected: str):
            pass
        
        with pytest.raises(ValueError) as exc_info:
            FunctionRegistry.validate_params(
                "my_func",
                {"expected": "value", "unexpected": "oops"}
            )
        
        error_msg = str(exc_info.value)
        assert "Unexpected parameters" in error_msg
        assert "unexpected" in error_msg
    
    def test_validate_ignores_context_param(self):
        """Validation ignores 'context' parameter (injected by framework)."""
        @transform
        def my_func(context, data_param: str):
            pass
        
        # Don't need to provide 'context' in params
        FunctionRegistry.validate_params("my_func", {"data_param": "value"})


class TestFunctionInfo:
    """Test function metadata extraction."""
    
    def setup_method(self):
        """Clear registry before each test."""
        FunctionRegistry._functions.clear()
        FunctionRegistry._signatures.clear()
    
    def test_get_function_info(self):
        """Can retrieve function metadata."""
        @transform
        def example_func(context, param1: str, param2: int = 10):
            """Example transform function.
            
            Args:
                param1: First parameter
                param2: Second parameter with default
            """
            pass
        
        info = FunctionRegistry.get_function_info("example_func")
        
        assert info["name"] == "example_func"
        assert "Example transform function" in info["docstring"]
        assert "param1" in info["parameters"]
        assert "param2" in info["parameters"]
    
    def test_function_info_shows_required_vs_optional(self):
        """Function info distinguishes required vs optional params."""
        @transform
        def my_func(context, required: str, optional: int = 42):
            pass
        
        info = FunctionRegistry.get_function_info("my_func")
        
        assert info["parameters"]["required"]["required"] is True
        assert info["parameters"]["required"]["default"] is None
        
        assert info["parameters"]["optional"]["required"] is False
        assert info["parameters"]["optional"]["default"] == 42


class TestGetRegisteredFunction:
    """Test convenience function get_registered_function."""
    
    def setup_method(self):
        """Clear registry before each test."""
        FunctionRegistry._functions.clear()
        FunctionRegistry._signatures.clear()
    
    def test_get_registered_function_helper(self):
        """get_registered_function() helper works."""
        @transform
        def test_func(context):
            return "result"
        
        func = get_registered_function("test_func")
        ctx = PandasContext()
        assert func(ctx) == "result"


class TestValidateFunctionParams:
    """Test convenience function validate_function_params."""
    
    def setup_method(self):
        """Clear registry before each test."""
        FunctionRegistry._functions.clear()
        FunctionRegistry._signatures.clear()
    
    def test_validate_function_params_helper(self):
        """validate_function_params() helper works."""
        @transform
        def test_func(context, param: str):
            pass
        
        # Should not raise
        validate_function_params("test_func", {"param": "value"})
        
        # Should raise
        with pytest.raises(ValueError):
            validate_function_params("test_func", {})


class TestRealWorldUsage:
    """Test realistic usage patterns."""
    
    def setup_method(self):
        """Clear registry before each test."""
        FunctionRegistry._functions.clear()
        FunctionRegistry._signatures.clear()
    
    def test_transform_function_with_dataframe_operations(self):
        """Transform function can operate on DataFrames from context."""
        @transform
        def filter_by_threshold(context, source_table: str, threshold: float):
            """Filter data by threshold value."""
            df = context.get(source_table)
            return df[df["value"] > threshold]
        
        # Set up context with data
        ctx = PandasContext()
        data = pd.DataFrame({"id": [1, 2, 3], "value": [5.0, 15.0, 25.0]})
        ctx.register("source_data", data)
        
        # Execute transform
        func = get_registered_function("filter_by_threshold")
        result = func(ctx, source_table="source_data", threshold=10.0)
        
        assert len(result) == 2
        assert result["value"].min() > 10.0
    
    def test_multiple_params_with_defaults(self):
        """Transform with multiple params and defaults."""
        @transform
        def enrich_data(
            context,
            main_table: str,
            reference_table: str,
            join_column: str = "id",
            how: str = "left"
        ):
            """Enrich main table with reference data."""
            main = context.get(main_table)
            ref = context.get(reference_table)
            return main.merge(ref, on=join_column, how=how)
        
        # Validate params (use defaults)
        validate_function_params(
            "enrich_data",
            {"main_table": "sales", "reference_table": "products"}
        )
        
        # Validate params (override defaults)
        validate_function_params(
            "enrich_data",
            {
                "main_table": "sales",
                "reference_table": "products",
                "join_column": "product_id",
                "how": "inner"
            }
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
