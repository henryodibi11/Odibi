"""
Explanation System
==================

Enables transformations to explain themselves with @func.explain pattern.
"""

from typing import Callable, Optional


class ExplanationDecorator:
    """
    Decorator class that adds explain() method to transformation functions.

    Usage:
        @transformation("my_transform")
        def my_transform(df, threshold):
            '''Filter records.'''
            return df[df.value > threshold]

        @my_transform.explain
        def explain(threshold, **context):
            plant = context.get('plant', 'Unknown')
            return f"Filter {plant} records above {threshold}"
    """

    def __init__(self, func: Callable):
        """
        Initialize explanation decorator.

        Args:
            func: The transformation function to add explain() to
        """
        self.func = func
        self._explain_func: Optional[Callable] = None
        # Copy over function metadata
        self.__name__ = func.__name__
        self.__doc__ = func.__doc__
        self.__dict__.update(func.__dict__)
        self.__module__ = func.__module__
        self.__qualname__ = func.__qualname__
        self.__annotations__ = func.__annotations__

    def __call__(self, *args, **kwargs):
        """Call the wrapped function."""
        return self.func(*args, **kwargs)

    def explain(self, explain_func: Callable) -> Callable:
        """
        Register an explanation function for this transformation.

        Args:
            explain_func: Function that generates explanation text

        Returns:
            The original transformation function (for chaining)
        """
        self._explain_func = explain_func
        return self.func

    def get_explanation(self, **kwargs) -> str:
        """
        Get explanation text for this transformation.

        Args:
            **kwargs: Parameters and context to pass to explain function

        Returns:
            Explanation text as markdown string

        Raises:
            ValueError: If no explanation function registered
        """
        if self._explain_func is None:
            raise ValueError(
                f"No explanation registered for transformation '{self.func.__name__}'. "
                f"Use @{self.func.__name__}.explain to register one."
            )

        return self._explain_func(**kwargs)

    def has_explanation(self) -> bool:
        """Check if this transformation has an explanation registered."""
        return self._explain_func is not None


def wrap_with_explanation(func: Callable) -> ExplanationDecorator:
    """
    Wrap a function with explanation capability.

    This is called automatically by the @transformation decorator.

    Args:
        func: Function to wrap

    Returns:
        ExplanationDecorator instance
    """
    return ExplanationDecorator(func)
