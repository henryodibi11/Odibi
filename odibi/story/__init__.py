"""
Story Generation Module
=======================

Provides automatic documentation and audit trail generation for pipeline runs.

Components:
- metadata: Story metadata tracking
- generator: Core story generation logic
- renderers: HTML/Markdown/JSON output formatters
"""

from odibi.story.doc_story import DocStoryGenerator
from odibi.story.generator import StoryGenerator
from odibi.story.lineage import LineageGenerator, LineageResult
from odibi.story.metadata import (
    NodeExecutionMetadata,
    PipelineStoryMetadata,
)
from odibi.story.renderers import (
    HTMLStoryRenderer,
    JSONStoryRenderer,
    MarkdownStoryRenderer,
    get_renderer,
)
from odibi.story.themes import (
    CORPORATE_THEME,
    DARK_THEME,
    DEFAULT_THEME,
    MINIMAL_THEME,
    StoryTheme,
    get_theme,
    list_themes,
)

__all__ = [
    "NodeExecutionMetadata",
    "PipelineStoryMetadata",
    "StoryGenerator",
    "HTMLStoryRenderer",
    "MarkdownStoryRenderer",
    "JSONStoryRenderer",
    "get_renderer",
    "DocStoryGenerator",
    "LineageGenerator",
    "LineageResult",
    "StoryTheme",
    "get_theme",
    "list_themes",
    "DEFAULT_THEME",
    "CORPORATE_THEME",
    "DARK_THEME",
    "MINIMAL_THEME",
]
