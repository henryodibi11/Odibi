"""Function registry for transform functions."""

import inspect
from functools import wraps
from typing import Any, Callable, Dict, Optional, Union


class FunctionRegistry:
    """Global registry of transform functions with type validation."""

    _functions: Dict[str, Callable] = {}
    _signatures: Dict[str, inspect.Signature] = {}
    _param_models: Dict[str, Any] = {}  # New: Store Pydantic models

    @classmethod
    def register(cls, func: Callable, name: str = None, param_model: Any = None) -> Callable:
        """Register a transform function.

        Args:
            func: Function to register
            name: Optional name override (default: func.__name__)
            param_model: Optional Pydantic model for validation

        Returns:
            The original function
        """
        if name is None:
            name = func.__name__

        cls._functions[name] = func
        cls._signatures[name] = inspect.signature(func)
        if param_model:
            cls._param_models[name] = param_model

        return func

    @classmethod
    def get(cls, name: str) -> Callable:
        """Retrieve a registered function.

        Args:
            name: Function name

        Returns:
            The registered function

        Raises:
            ValueError: If function not found
        """
        if name not in cls._functions:
            available = ", ".join(cls._functions.keys()) if cls._functions else "none"
            raise ValueError(
                f"Transform function '{name}' not registered. Available functions: {available}"
            )
        return cls._functions[name]

    @classmethod
    def has_function(cls, name: str) -> bool:
        """Check if a function is registered."""
        return name in cls._functions

    @classmethod
    def get_function(cls, name: str) -> Optional[Callable]:
        """Get a function without raising if not found."""
        return cls._functions.get(name)

    @classmethod
    def get_param_model(cls, name: str) -> Optional[Any]:
        """Get the Pydantic model for a function's parameters."""
        return cls._param_models.get(name)

    @classmethod
    def validate_params(cls, name: str, params: Dict[str, Any]) -> None:
        """Validate parameters against function signature or Pydantic model.

        Args:
            name: Function name
            params: Parameters to validate

        Raises:
            ValueError: If parameters are invalid
            TypeError: If parameter types don't match
        """
        if name not in cls._functions:
            raise ValueError(f"Function '{name}' not registered")

        # Priority: Check Pydantic Model
        if name in cls._param_models:
            model = cls._param_models[name]
            try:
                model(**params)
                return  # Validated successfully
            except Exception as e:
                raise ValueError(f"Validation failed for '{name}': {e}")

        # Fallback: Check function signature (Legacy)
        sig = cls._signatures[name]

        # Get function parameters (excluding 'context' and 'current' which are injected)
        func_params = {k: v for k, v in sig.parameters.items() if k not in ["context", "current"]}

        # Check for missing required parameters
        missing = []
        for param_name, param in func_params.items():
            if param.default is inspect.Parameter.empty:
                # Required parameter
                if param_name not in params:
                    missing.append(param_name)

        if missing:
            raise ValueError(
                f"Missing required parameters for function '{name}': {', '.join(missing)}"
            )

        # Check for unexpected parameters
        unexpected = set(params.keys()) - set(func_params.keys())
        if unexpected:
            raise ValueError(
                f"Unexpected parameters for function '{name}': {', '.join(unexpected)}"
            )

    @classmethod
    def list_functions(cls) -> list[str]:
        """List all registered function names.

        Returns:
            List of function names
        """
        return list(cls._functions.keys())

    @classmethod
    def get_function_info(cls, name: str) -> Dict[str, Any]:
        """Get detailed information about a registered function.

        Args:
            name: Function name

        Returns:
            Dictionary with function metadata
        """
        if name not in cls._functions:
            raise ValueError(f"Function '{name}' not registered")

        func = cls._functions[name]
        sig = cls._signatures[name]

        # Extract parameter info
        params_info = {}
        for param_name, param in sig.parameters.items():
            if param_name == "context":
                continue  # Skip context param

            param_info = {
                "required": param.default is inspect.Parameter.empty,
                "default": None if param.default is inspect.Parameter.empty else param.default,
                "annotation": (
                    param.annotation if param.annotation != inspect.Parameter.empty else None
                ),
            }
            params_info[param_name] = param_info

        return {
            "name": name,
            "docstring": inspect.getdoc(func),
            "parameters": params_info,
            "return_annotation": (
                sig.return_annotation if sig.return_annotation != inspect.Signature.empty else None
            ),
        }


def transform(name_or_func: Union[str, Callable] = None, **kwargs) -> Callable:
    """Decorator to register a transform function.

    Usage:
        @transform
        def my_transform(...): ...

        @transform("my_name")
        def my_transform(...): ...

        @transform(name="my_name", category="foo")
        def my_transform(...): ...

    Args:
        name_or_func: Function (if used without args) or Name (if used with args)
        **kwargs: Additional metadata (ignored for now)

    Returns:
        The decorated function
    """

    # If called with keyword args only (e.g. @transform(name="foo")), name_or_func might be None
    if name_or_func is None and "name" in kwargs:
        name_or_func = kwargs["name"]

    def _register(func, name=None):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        # Register the function
        # If name passed to decorator is None, use func.__name__
        # But FunctionRegistry.register handles None name by using func.__name__
        # However, we want to use the explicit name if provided.
        reg_name = name or func.__name__

        # Extract param_model from kwargs (captured from decorator args)
        # Note: kwargs here are from the outer scope (transform arguments), NOT wrapper args
        # Wait, _register closes over kwargs from transform(..., **kwargs)
        param_model = kwargs.get("param_model")

        FunctionRegistry.register(wrapper, name=reg_name, param_model=param_model)
        return wrapper

    if callable(name_or_func):
        # Called as @transform
        return _register(name_or_func)
    else:
        # Called as @transform("name") or @transform(name="name")
        def decorator(func):
            return _register(func, name=name_or_func)

        return decorator


def get_registered_function(name: str) -> Callable:
    """Get a registered transform function.

    Args:
        name: Function name

    Returns:
        The registered function
    """
    return FunctionRegistry.get(name)


def validate_function_params(name: str, params: Dict[str, Any]) -> None:
    """Validate parameters for a registered function.

    Args:
        name: Function name
        params: Parameters to validate
    """
    FunctionRegistry.validate_params(name, params)
