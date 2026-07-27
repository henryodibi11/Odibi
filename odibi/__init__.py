"""ODIBI - Explicit, Traceable, Simple Data Engineering Framework."""

# Pipeline and other components will be imported when available
__all__ = [
    "transform",
    "Context",
    "__version__",
]


# Lazy imports for components not yet implemented
def __getattr__(name):
    """Resolve public runtime symbols without making package import effectful."""
    if name == "__version__":
        from importlib.metadata import PackageNotFoundError, version

        try:
            resolved_version = version("odibi")
        except PackageNotFoundError:
            resolved_version = "0.0.0.dev0"
        globals()[name] = resolved_version
        return resolved_version
    if name == "Context":
        from odibi.context import Context

        return Context
    if name == "transform":
        from odibi.registry import transform

        return transform
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
