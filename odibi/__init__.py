"""ODIBI - Explicit, Traceable, Simple Data Engineering Framework."""

__version__ = "1.4.0"

# Core components (available now)
from odibi.registry import transform
from odibi.context import Context
import odibi.transformers  # noqa: F401 # Register built-in transformers

# Pipeline and other components will be imported when available
__all__ = [
    "transform",
    "Context",
    "__version__",
]


# Lazy imports for components not yet implemented
def __getattr__(name):
    if name == "Pipeline":
        from odibi.pipeline import Pipeline

        return Pipeline
    if name == "PipelineManager":
        from odibi.pipeline import PipelineManager

        return PipelineManager
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
