"""UI components for the Odibi Assistant.

Modular Gradio components for:
- Settings panel with LLM/memory configuration
- Chat interface with tool execution
- Memory viewer and search
- Folder picker for project selection
- Todo panel for task tracking
"""

from .chat import create_chat_interface
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

__all__ = [
    "create_settings_panel",
    "create_chat_interface",
    "create_memory_panel",
    "create_folder_picker",
    "get_active_project",
    "ProjectState",
    "create_todo_panel",
    "get_todo_manager",
    "todo_read",
    "todo_write",
    "TodoManager",
]
