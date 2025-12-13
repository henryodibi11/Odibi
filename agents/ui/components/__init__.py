"""UI components for the Odibi Assistant.

Modular Gradio components for:
- Settings panel with LLM/memory configuration
- Chat interface with tool execution (standard and enhanced)
- Memory viewer and search
- Folder picker for project selection
- Todo panel for task tracking
- Activity feed for real-time progress
- Thinking panel for visible reasoning
"""

from .chat import create_chat_interface
from .enhanced_chat import (
    create_enhanced_chat_interface,
    setup_enhanced_chat_handlers,
    EnhancedChatHandler,
)
from .folder_picker import ProjectState, create_folder_picker, get_active_project
from .memories import create_memory_panel
from .settings import create_settings_panel
from .todo_panel import (
    TodoManager,
    create_todo_panel,
    get_todo_manager,
    todo_read,
    todo_write,
)
from .activity_feed import (
    create_activity_panel,
    add_activity,
    complete_activity,
    clear_activity,
    refresh_activity_display,
)
from .thinking_panel import (
    create_thinking_panel,
    start_thinking,
    update_thinking,
    complete_thinking,
    reset_thinking,
)

__all__ = [
    # Settings
    "create_settings_panel",
    # Chat interfaces
    "create_chat_interface",
    "create_enhanced_chat_interface",
    "setup_enhanced_chat_handlers",
    "EnhancedChatHandler",
    # Memory
    "create_memory_panel",
    # Project
    "create_folder_picker",
    "get_active_project",
    "ProjectState",
    # Todo
    "create_todo_panel",
    "get_todo_manager",
    "todo_read",
    "todo_write",
    "TodoManager",
    # Activity feed
    "create_activity_panel",
    "add_activity",
    "complete_activity",
    "clear_activity",
    "refresh_activity_display",
    # Thinking panel
    "create_thinking_panel",
    "start_thinking",
    "update_thinking",
    "complete_thinking",
    "reset_thinking",
]
