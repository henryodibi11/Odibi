"""Main Gradio application for the Odibi AI Assistant.

Combines settings, chat, and memory components into a unified interface
that works both locally and in Databricks notebooks.
"""

import os
from typing import Optional

import gradio as gr

from .components.chat import create_chat_interface, setup_chat_handlers
from .components.memories import (
    create_memory_panel,
    format_memory_list,
    get_memory_manager,
    setup_memory_handlers,
)
from .components.settings import create_settings_panel
from .components.todo_panel import create_todo_panel
from .components.conversation import (
    create_conversation_panel,
    setup_conversation_handlers,
)
from .config import AgentUIConfig, load_config

CSS = """
.memory-list {
    max-height: 400px;
    overflow-y: auto;
}

.gradio-container {
    max-width: 1400px !important;
}

footer {
    display: none !important;
}
"""


def create_app(
    working_project: str = "",
    config: Optional[AgentUIConfig] = None,
) -> gr.Blocks:
    """Create the Odibi Assistant Gradio app.

    Args:
        working_project: Root directory of the project to work on.
        config: Optional pre-loaded configuration.

    Returns:
        Gradio Blocks application.
    """
    if config is None:
        config = load_config(working_project)

    current_config = [config]

    def get_config() -> AgentUIConfig:
        return current_config[0]

    def on_config_save(new_config: AgentUIConfig):
        current_config[0] = new_config

    with gr.Blocks(title="🧠 Odibi Assistant") as app:
        gr.Markdown(
            """
            # 🧠 Odibi AI Assistant

            A conversational AI assistant that works with any codebase.
            """
        )

        with gr.Row():
            with gr.Column(scale=1):
                settings_column, settings_components = create_settings_panel(
                    initial_config=config,
                    on_save=on_config_save,
                )

                todo_column, todo_components = create_todo_panel()

                conv_column, conv_components = create_conversation_panel()

                memory_column, memory_components = create_memory_panel(config=config)

            with gr.Column(scale=2):
                chat_column, chat_components = create_chat_interface(config=config)

        setup_chat_handlers(chat_components, get_config)

        setup_conversation_handlers(conv_components, chat_components, get_config)

        setup_memory_handlers(memory_components, get_config)

        def load_initial_memories():
            try:
                cfg = get_config()
                manager = get_memory_manager(cfg)
                memories = manager.store.get_recent(days=30, limit=20)
                return format_memory_list(memories)
            except Exception:
                return "_Could not load memories_"

        app.load(
            fn=load_initial_memories,
            outputs=[memory_components["memory_list"]],
        )

    return app


def launch(
    working_project: str = "",
    share: bool = False,
    server_name: Optional[str] = None,
    server_port: Optional[int] = None,
    **kwargs,
) -> gr.Blocks:
    """Launch the Odibi Assistant UI.

    Works seamlessly in both local browser and Databricks notebooks:
    - Local: Opens in default browser
    - Databricks: Renders inline in notebook

    Args:
        working_project: Root directory of the project to work on.
        share: Create a public shareable link.
        server_name: Server hostname (default: localhost).
        server_port: Server port (default: auto-select).
        **kwargs: Additional Gradio launch arguments.

    Returns:
        The Gradio app instance.

    Example:
        ```python
        # Local - opens in browser
        from agents.ui import launch
        launch(working_project="d:/my-project")

        # Databricks - renders inline
        from agents.ui import launch
        launch()

        # Custom port
        launch(server_port=7860)

        # Public link
        launch(share=True)
        ```
    """
    app = create_app(working_project=working_project)

    is_databricks = (
        "DATABRICKS_RUNTIME_VERSION" in os.environ
        or "SPARK_HOME" in os.environ
        or os.path.exists("/databricks")
    )

    launch_kwargs = {
        "share": share,
        "css": CSS,
    }

    try:
        launch_kwargs["theme"] = gr.themes.Soft()
    except Exception:
        pass

    if is_databricks:
        return app.launch(
            inline=True,
            quiet=True,
            **launch_kwargs,
            **kwargs,
        )
    else:
        return app.launch(
            server_name=server_name or "127.0.0.1",
            server_port=server_port,
            inbrowser=True,
            **launch_kwargs,
            **kwargs,
        )


def create_demo() -> gr.Blocks:
    """Create a demo version of the app for testing.

    Returns:
        Gradio Blocks demo app.
    """
    return create_app()


if __name__ == "__main__":
    launch()
