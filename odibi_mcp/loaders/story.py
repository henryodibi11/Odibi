"""
odibi_mcp.loaders.story

StoryLoader implementation for MCP, with enforced project access via AccessContext.
"""

from typing import Any, Optional
from odibi_mcp.contracts.access import AccessContext
from odibi_mcp.contracts.selectors import RunSelector


class StoryLoader:
    """
    Loads Story metadata for a pipeline and enforces project access via injected AccessContext.
    See MCP spec 3.c and unified access enforcement.
    """

    def __init__(self, access: AccessContext):
        self._access = access

    def set_access_context(self, access: AccessContext) -> None:
        """Inject/replace AccessContext."""
        self._access = access

    def load(self, pipeline: str, run_selector: Optional[RunSelector] = None) -> Any:
        """
        Loads metadata/story for a pipeline run. Enforces project scoping via AccessContext.
        * pipeline: str - Pipeline name
        * run_selector: RunSelector - Which run to load (default: latest_successful)
        Returns pipeline story metadata (opaque, depends on Odibi core).
        """
        # Fake implementation for contract: In practice, call self._load_story and enforce project.
        story = self._load_story(pipeline, run_selector)  # type: ignore
        self._access.check_project(story.project)  # Project access enforced
        return story

    def _load_story(self, pipeline: str, run_selector: Optional[RunSelector]) -> Any:
        """
        Placeholder; to be implemented with connection to story artifact store/resolver.
        This should fetch the resolved story for the given pipeline/run_selector.
        Must set `project` attribute on story model.
        """
        raise NotImplementedError("_load_story() must be implemented by concrete backend.")
