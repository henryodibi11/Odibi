"""
Story Generation Module
=======================

Provides automatic documentation and audit trail generation for pipeline runs.

Components:
- metadata: Story metadata tracking
- generator: Core story generation logic
- renderers: HTML/Markdown/JSON output formatters
"""

from odibi.story.metadata import (
    NodeExecutionMetadata,
    PipelineStoryMetadata,
)
from odibi.story.generator import StoryGenerator
from odibi.story.renderers import (
    HTMLStoryRenderer,
    MarkdownStoryRenderer,
    JSONStoryRenderer,
    get_renderer,
)
from odibi.story.doc_story import DocStoryGenerator
from odibi.story.themes import (
    StoryTheme,
    get_theme,
    list_themes,
    DEFAULT_THEME,
    CORPORATE_THEME,
    DARK_THEME,
    MINIMAL_THEME,
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
    "StoryTheme",
    "get_theme",
    "list_themes",
    "DEFAULT_THEME",
    "CORPORATE_THEME",
    "DARK_THEME",
    "MINIMAL_THEME",
]
__version__ = "1.3.0-alpha.5-phase3b"
