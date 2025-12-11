"""UI components for the Odibi Assistant.

Modular Gradio components for:
- Settings panel with LLM/memory configuration
- Chat interface with tool execution
- Memory viewer and search
"""

from .chat import create_chat_interface
from .memories import create_memory_panel
from .settings import create_settings_panel

__all__ = [
    "create_settings_panel",
    "create_chat_interface",
    "create_memory_panel",
]
