"""Main Gradio application for the Odibi AI Assistant.

Combines settings, chat, and memory components into a unified interface
that works both locally and in Databricks notebooks.

Enhanced version with:
- Token-by-token streaming
- Visible thinking/reasoning
- Activity feed
- Collapsible tool results
- File link clickability
- Token/cost display
- Dark/light theme toggle
- Sound notifications
- Conversation branching
"""

import os
from typing import Optional

import gradio as gr

from .components.cycle_panel import (
    create_cycle_panel,
    create_guided_execution_panel,
    setup_cycle_handlers,
)
from .components.issue_discovery_panel import (
    create_issue_discovery_panel,
    setup_issue_discovery_handlers,
)
from .components.escalation_panel import (
    create_escalation_panel,
    setup_escalation_handlers,
)
from .components.explorer_panel import (
    create_explorer_panel,
    setup_explorer_handlers,
)
from .components.enhanced_chat import create_enhanced_chat_interface, setup_enhanced_chat_handlers
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
from .components.campaigns import (
    create_campaigns_panel,
    setup_campaigns_handlers,
)
from .config import AgentUIConfig, load_config
from .constants import ENHANCED_CSS, ENHANCED_JS, ENHANCED_JS_RAW

CSS = ENHANCED_CSS


def create_app(
    working_project: str = "",
    reference_repos: list[str] | None = None,
    config: Optional[AgentUIConfig] = None,
) -> gr.Blocks:
    """Create the Odibi Assistant Gradio app.

    Args:
        working_project: Root directory of the project to work on.
        reference_repos: Additional repos for grep/read access.
        config: Optional pre-loaded configuration.

    Returns:
        Gradio Blocks application.
    """
    if config is None:
        config = load_config(working_project)

    # Set reference repos from launch() repos parameter
    if reference_repos and not config.project.reference_repos:
        config.project.reference_repos = reference_repos

    current_config = [config]

    def get_config() -> AgentUIConfig:
        return current_config[0]

    def on_config_save(new_config: AgentUIConfig):
        current_config[0] = new_config

    with gr.Blocks(title="🧠 Odibi Assistant", css=CSS, js=ENHANCED_JS_RAW) as app:
        gr.HTML(ENHANCED_JS)  # Fallback for older Gradio

        gr.Markdown(
            """
            # 🧠 Odibi AI Assistant

            A conversational AI assistant with visible thinking and real-time activity tracking.

            **Modes:** Interactive (chat), Guided Execution (run cycles), Scheduled Assistant (background work)
            """
        )

        agent_runner_ref = [None]

        def get_runner():
            from odibi.agents.pipelines.agent_runner import AgentRunner, AgentRunnerConfig
            from odibi.agents.core.azure_client import AzureConfig
            from odibi.agents.core.agent_base import OdibiAgent
            from odibi.agents.ui.llm_client import LLMClient, LLMConfig

            cfg = get_config()

            # ALWAYS refresh the LLM client with current settings
            # This ensures API key changes in UI are picked up
            llm_config = LLMConfig(
                endpoint=cfg.llm.endpoint,
                api_key=cfg.llm.api_key,
                model=cfg.llm.model,
                api_type=cfg.llm.api_type,
                api_version=cfg.llm.api_version,
            )
            llm_client = LLMClient(llm_config)
            OdibiAgent.set_global_llm_client(llm_client)

            if agent_runner_ref[0] is None:
                try:
                    azure_config = AzureConfig(
                        openai_endpoint=cfg.llm.endpoint,
                        openai_api_key=cfg.llm.api_key,
                        chat_deployment=cfg.llm.model,
                    )
                    runner_config = AgentRunnerConfig(
                        odibi_root=cfg.project.working_project or "d:/odibi",
                    )
                    agent_runner_ref[0] = AgentRunner(azure_config, runner_config)
                except Exception as e:
                    print(f"Error creating runner: {e}")
            return agent_runner_ref[0]

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
                cycle_column, cycle_components = create_cycle_panel()

                guided_column, guided_components = create_guided_execution_panel()

                discovery_column, discovery_components = create_issue_discovery_panel()

                escalation_column, escalation_components = create_escalation_panel()

                explorer_column, explorer_components = create_explorer_panel()

                campaigns_column, campaigns_components = create_campaigns_panel()

                chat_column, chat_components = create_enhanced_chat_interface(config=config)

        chat_handler = setup_enhanced_chat_handlers(
            chat_components, get_config, todo_display=todo_components.get("todo_display")
        )

        setup_explorer_handlers(explorer_components, chat_components)

        setup_cycle_handlers(
            cycle_components,
            guided_components,
            get_runner,
            chat_components,
            discovery_components,
            explorer_components,
        )

        setup_issue_discovery_handlers(
            discovery_components, guided_components, get_runner, chat_components
        )

        setup_escalation_handlers(
            escalation_components, discovery_components, get_runner, chat_components
        )

        setup_conversation_handlers(conv_components, chat_components, get_config, chat_handler)

        setup_memory_handlers(memory_components, get_config)

        setup_campaigns_handlers(campaigns_components, chat_components)

        def run_selected_task(selected_task: str) -> str:
            """Return the selected task to be sent to chat input."""
            return selected_task if selected_task else ""

        todo_components["run_task_btn"].click(
            fn=run_selected_task,
            inputs=[todo_components["task_selector"]],
            outputs=[chat_components["message_input"]],
        )

        def load_initial_memories():
            try:
                cfg = get_config()
                manager = get_memory_manager(cfg)
                memories = manager.store.get_recent(days=30, limit=20)
                return format_memory_list(memories)
            except Exception as e:
                import traceback

                print(f"Memory load error: {e}")
                traceback.print_exc()
                return f"_Could not load memories: {e}_"

        app.load(
            fn=load_initial_memories,
            outputs=[memory_components["memory_list"]],
        )

    return app


def launch(
    working_project: str = "",
    repos: list[str] | None = None,
    index_dir: str | None = None,
    reindex: bool = False,
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
        repos: List of repository paths to index for RAG search.
        index_dir: Directory for the shared index (default: first repo's .odibi/index).
        reindex: Clear existing index and rebuild (default: False, uses cached).
        share: Create a public shareable link.
        server_name: Server hostname (default: localhost).
        server_port: Server port (default: auto-select).
        **kwargs: Additional Gradio launch arguments.

    Returns:
        The Gradio app instance.

    Example:
        ```python
        # Simple launch with working project
        from odibi.agents.ui import launch
        launch(working_project="d:/my-project")

        # Multi-repo: index odibi and your project
        launch(repos=["d:/odibi", "d:/my-project"])

        # Force reindex
        launch(repos=["d:/odibi", "d:/my-project"], reindex=True)

        # Databricks - renders inline
        launch()
        ```
    """
    if repos:
        from odibi.agents.pipelines.indexer import MultiRepoIndexer

        _index_dir = index_dir or f"{repos[0]}/.odibi/index"
        indexer = MultiRepoIndexer(repos=repos, index_dir=_index_dir)
        print(f"Indexing {len(repos)} repos: {repos}")
        result = indexer.run_indexing(force_recreate=reindex)
        print(f"Indexed {result['total_chunks']} chunks from {result['total_repos']} repos")

        # Use first repo as working project, rest as reference repos
        if not working_project:
            working_project = repos[0]
        reference_repos = repos[1:] if len(repos) > 1 else []
    else:
        reference_repos = []

    app = create_app(
        working_project=working_project,
        reference_repos=reference_repos,
    )

    is_databricks = (
        "DATABRICKS_RUNTIME_VERSION" in os.environ
        or "SPARK_HOME" in os.environ
        or os.path.exists("/databricks")
    )

    launch_kwargs = {
        "share": share,
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
