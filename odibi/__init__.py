"""ODIBI - Explicit, Traceable, Simple Data Engineering Framework."""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("odibi")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"  # Fallback for editable installs without metadata

# Core components (available now)
import odibi.transformers  # noqa: F401 # Register built-in transformers
from odibi.context import Context
from odibi.registry import transform

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
    if name == "Project":
        from odibi.project import Project

        return Project
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
