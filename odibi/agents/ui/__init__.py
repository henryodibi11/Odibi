"""Odibi AI Assistant - Gradio UI.

A conversational UI for the Odibi AI Agent Suite with:
- Settings panel for LLM and memory configuration
- Chat interface with tool execution
- Memory viewer for insights and decisions

Works seamlessly in both local browser and Databricks notebooks.

Example:
    ```python
    # Local - opens in browser
    from odibi.agents.ui import launch
    launch()

    # Databricks - renders inline
    from odibi.agents.ui import launch
    launch()
    ```
"""


def launch(*args, **kwargs):
    """Launch the Odibi Assistant UI.

    Lazy import to avoid requiring gradio at import time.
    """
    from .app import launch as _launch

    return _launch(*args, **kwargs)


def create_app(*args, **kwargs):
    """Create the Odibi Assistant Gradio app.

    Lazy import to avoid requiring gradio at import time.
    """
    from .app import create_app as _create_app

    return _create_app(*args, **kwargs)


__all__ = ["create_app", "launch"]
