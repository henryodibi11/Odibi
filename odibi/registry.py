"""Function registry for transform functions."""

from typing import Callable, Dict, Any
import inspect
from functools import wraps


class FunctionRegistry:
    """Global registry of transform functions with type validation."""

    _functions: Dict[str, Callable] = {}
    _signatures: Dict[str, inspect.Signature] = {}

    @classmethod
    def register(cls, func: Callable) -> Callable:
        """Register a transform function.

        Args:
            func: Function to register

        Returns:
            The original function (for use as decorator)
        """
        name = func.__name__
        cls._functions[name] = func
        cls._signatures[name] = inspect.signature(func)
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
                f"Transform function '{name}' not registered. " f"Available functions: {available}"
            )
        return cls._functions[name]

    @classmethod
    def validate_params(cls, name: str, params: Dict[str, Any]) -> None:
        """Validate parameters against function signature.

        Args:
            name: Function name
            params: Parameters to validate

        Raises:
            ValueError: If parameters are invalid
            TypeError: If parameter types don't match
        """
        if name not in cls._signatures:
            raise ValueError(f"Function '{name}' not registered")

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


def transform(func: Callable) -> Callable:
    """Decorator to register a transform function.

    Usage:
        @transform
        def my_transform(context, param1: str, param2: int = 10):
            '''Transform description.'''
            data = context.get("input_table")
            # ... transformation logic
            return result

    Args:
        func: Function to register

    Returns:
        The decorated function
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    # Register the function
    FunctionRegistry.register(wrapper)

    return wrapper


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
