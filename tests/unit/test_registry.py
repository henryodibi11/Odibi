import pytest
from odibi.registry import (
    FunctionRegistry,
    transform,
    get_registered_function,
    validate_function_params,
)


# Fixture to clear the registry between tests
@pytest.fixture(autouse=True)
def clear_registry():
    FunctionRegistry.clear()


# Dummy functions for testing
def dummy_func(a, b):
    """Dummy function that adds two numbers."""
    return a + b


def dummy_func_with_default(a, b=10):
    """Dummy function with a default parameter."""
    return a * b


# Dummy parameter model to simulate Pydantic models
class DummyParamModel:
    def __init__(self, **kwargs):
        if "x" not in kwargs:
            raise ValueError("Missing required parameter: x")

    # Accept extra parameters without error for testing purposes


# ---------------------
# Tests for register methods
# ---------------------
def test_register_function_default_name():
    # Register a function without providing a custom name
    FunctionRegistry.register(dummy_func)
    assert FunctionRegistry.has_function("dummy_func")
    retrieved = FunctionRegistry.get("dummy_func")
    assert retrieved is dummy_func


def test_register_function_with_custom_name_and_param_model():
    FunctionRegistry.register(dummy_func, name="custom_dummy", param_model=DummyParamModel)
    assert FunctionRegistry.has_function("custom_dummy")
    assert FunctionRegistry.get_param_model("custom_dummy") is DummyParamModel


def test_duplicate_registration_overwrites():
    # Register a function and then register a different function under the same name
    FunctionRegistry.register(dummy_func, name="dup_test")

    def new_dummy(a, b):
        return a - b

    FunctionRegistry.register(new_dummy, name="dup_test")
    retrieved = FunctionRegistry.get("dup_test")
    # Expect the new function to overwrite the previous one
    assert retrieved is new_dummy


# ---------------------
# Tests for get and get_function methods
# ---------------------
def test_get_registered_function_success():
    FunctionRegistry.register(dummy_func)
    func = get_registered_function("dummy_func")
    assert func is dummy_func


def test_get_registered_function_not_found():
    with pytest.raises(ValueError) as exc_info:
        FunctionRegistry.get("non_existent")
    assert "not registered" in str(exc_info.value)


def test_get_function_returns_none_for_unregistered():
    assert FunctionRegistry.get_function("non_existent") is None


# ---------------------
# Tests for has_function and list_functions
# ---------------------
def test_has_function_returns_true_if_registered():
    FunctionRegistry.register(dummy_func)
    assert FunctionRegistry.has_function("dummy_func") is True


def test_has_function_returns_false_if_not_registered():
    assert FunctionRegistry.has_function("unknown_func") is False


def test_list_functions_returns_registered_names():
    FunctionRegistry.register(dummy_func)
    FunctionRegistry.register(dummy_func_with_default)
    func_list = FunctionRegistry.list_functions()
    assert "dummy_func" in func_list
    assert "dummy_func_with_default" in func_list


# ---------------------
# Tests for get_function_info
# ---------------------
def test_get_function_info_success():
    FunctionRegistry.register(dummy_func)
    info = FunctionRegistry.get_function_info("dummy_func")
    assert info["name"] == "dummy_func"
    assert "Dummy function that adds two numbers." in info["docstring"]
    params = info.get("parameters", {})
    # Verify that parameters 'a' and 'b' are present and marked as required (since no defaults)
    assert "a" in params
    assert "b" in params
    assert params["a"]["required"] is True
    assert params["b"]["required"] is True


def test_get_function_info_unregistered():
    with pytest.raises(ValueError) as exc_info:
        FunctionRegistry.get_function_info("non_existent")
    assert "not registered" in str(exc_info.value)


# ---------------------
# Tests for validate_params with param_model and signature fallback
# ---------------------
def test_validate_params_with_param_model_success():
    # Register using a dummy parameter model that requires "x"
    FunctionRegistry.register(dummy_func, name="dummy_with_model", param_model=DummyParamModel)
    # Should validate successfully when 'x' is provided
    FunctionRegistry.validate_params("dummy_with_model", {"x": 5, "extra": "ignored"})


def test_validate_params_with_param_model_failure():
    FunctionRegistry.register(dummy_func, name="dummy_with_model", param_model=DummyParamModel)
    with pytest.raises(ValueError) as exc_info:
        FunctionRegistry.validate_params("dummy_with_model", {"y": 10})
    assert "Validation failed for" in str(exc_info.value)


def test_validate_params_signature_missing_required():
    # Register a function without a param_model, so fallback to signature validation
    FunctionRegistry.register(dummy_func, name="dummy_sig")
    with pytest.raises(ValueError) as exc_info:
        # Missing required parameter 'b'
        FunctionRegistry.validate_params("dummy_sig", {"a": 1})
    assert "Missing required parameters" in str(exc_info.value)


def test_validate_params_signature_unexpected_parameter():
    FunctionRegistry.register(dummy_func, name="dummy_sig")
    with pytest.raises(ValueError) as exc_info:
        # Provide an unexpected parameter 'c'
        FunctionRegistry.validate_params("dummy_sig", {"a": 1, "b": 2, "c": 3})
    assert "Unexpected parameters" in str(exc_info.value)


def test_validate_params_signature_success():
    FunctionRegistry.register(dummy_func, name="dummy_sig")
    # Provide all required parameters
    FunctionRegistry.validate_params("dummy_sig", {"a": 10, "b": 20})


# ---------------------
# Tests for the transform decorator
# ---------------------
def test_transform_decorator_without_arguments():
    @transform
    def decorated_func(a, b):
        return a + b

    # The function should be registered under its original name "decorated_func"
    assert FunctionRegistry.has_function("decorated_func")
    func = FunctionRegistry.get("decorated_func")
    assert func(2, 3) == 5


def test_transform_decorator_with_custom_name():
    @transform("custom_transform")
    def another_func(x):
        return x * 2

    assert FunctionRegistry.has_function("custom_transform")
    func = FunctionRegistry.get("custom_transform")
    assert func(4) == 8


# ---------------------
# Tests for free function wrappers
# ---------------------
def test_get_registered_function_wrapper():
    FunctionRegistry.register(dummy_func)
    func = get_registered_function("dummy_func")
    assert func(3, 4) == 7


def test_validate_function_params_wrapper():
    FunctionRegistry.register(dummy_func, name="dummy_val")
    # Valid parameters
    validate_function_params("dummy_val", {"a": 5, "b": 10})
    with pytest.raises(ValueError) as exc_info:
        validate_function_params("dummy_val", {"a": 5})
    assert "Missing required parameters" in str(exc_info.value)
