"""Settings panel component for the Odibi Assistant.

Provides no-code configuration for LLM, memory, and project settings.
Supports any OpenAI-compatible LLM provider.
"""

from typing import Any, Callable, Optional

import gradio as gr

from ..config import (
    AgentUIConfig,
    LLMConfig,
    MemoryConfig,
    ProjectConfig,
    load_config,
    load_odibi_connections,
    save_config,
)

PROVIDER_PRESETS = {
    "OpenAI": ("https://api.openai.com/v1", "openai"),
    "Azure OpenAI": ("https://your-resource.openai.azure.com", "azure"),
    "Ollama (local)": ("http://localhost:11434/v1", "openai"),
    "LM Studio (local)": ("http://localhost:1234/v1", "openai"),
    "Custom": ("", "openai"),
}


def create_settings_panel(
    initial_config: Optional[AgentUIConfig] = None,
    on_save: Optional[Callable[[AgentUIConfig], None]] = None,
) -> tuple[gr.Column, dict[str, Any]]:
    """Create the settings panel component.

    Args:
        initial_config: Initial configuration to populate fields.
        on_save: Callback when settings are saved.

    Returns:
        Tuple of (Gradio Column, dict of component references).
    """
    config = initial_config or load_config()
    connections = load_odibi_connections(config.project.project_yaml_path)

    components: dict[str, Any] = {}

    with gr.Column(scale=1) as settings_column:
        gr.Markdown("## ⚙️ Settings")

        with gr.Accordion("🤖 LLM Provider", open=True):
            components["provider"] = gr.Dropdown(
                label="Provider",
                choices=list(PROVIDER_PRESETS.keys()),
                value="Custom",
                allow_custom_value=False,
            )
            components["endpoint"] = gr.Textbox(
                label="API Endpoint",
                value=config.llm.endpoint,
                placeholder="https://api.openai.com/v1",
                type="text",
            )
            components["model"] = gr.Dropdown(
                label="Model",
                choices=[
                    "gpt-4o",
                    "gpt-4o-mini",
                    "gpt-4-turbo",
                    "gpt-3.5-turbo",
                    "llama3.2",
                    "mistral",
                    "codellama",
                ],
                value=config.llm.model,
                allow_custom_value=True,
            )
            components["api_key"] = gr.Textbox(
                label="API Key",
                value=config.llm.api_key,
                type="password",
                placeholder="sk-... or your API key",
            )
            components["api_type"] = gr.Radio(
                label="API Type",
                choices=["openai", "azure"],
                value=config.llm.api_type,
                visible=True,
            )

        with gr.Accordion("💾 Memory Backend", open=True):
            components["backend_type"] = gr.Radio(
                label="Storage Type",
                choices=["local", "odibi", "delta"],
                value=config.memory.backend_type,
            )

            connection_choices = connections if connections else ["(no connections)"]
            components["connection_name"] = gr.Dropdown(
                label="Odibi Connection",
                choices=connection_choices,
                value=config.memory.connection_name
                or (connection_choices[0] if connection_choices else None),
                visible=config.memory.backend_type != "local",
                interactive=True,
            )

            components["memory_path"] = gr.Textbox(
                label="Path Prefix",
                value=config.memory.path_prefix,
                placeholder="agent/memories",
                visible=config.memory.backend_type == "odibi",
            )

            components["delta_table"] = gr.Textbox(
                label="Delta Table Path",
                value=config.memory.table_path,
                placeholder="system.agent_memories",
                visible=config.memory.backend_type == "delta",
            )

            with gr.Row():
                components["test_memory_btn"] = gr.Button(
                    "🔌 Test Connection",
                    size="sm",
                    visible=config.memory.backend_type != "local",
                )
            components["memory_status"] = gr.Markdown(
                "",
                visible=True,
            )

        with gr.Accordion("📂 Working Project", open=True):
            from .folder_picker import create_folder_picker, ProjectState

            project_state = ProjectState()
            picker_column, picker_components = create_folder_picker(
                initial_path=config.project.working_project,
                project_state=project_state,
            )
            components["folder_picker"] = picker_components
            components["project_state"] = project_state
            components["working_project"] = picker_components["path_input"]

            components["project_yaml"] = gr.Textbox(
                label="project.yaml Path (optional)",
                value=config.project.project_yaml_path or "",
                placeholder="path/to/project.yaml",
                info="For loading Odibi connections",
            )
            with gr.Row():
                components["refresh_connections_btn"] = gr.Button(
                    "🔄 Refresh Connections",
                    size="sm",
                )

        with gr.Accordion("📚 Reference Repos (optional)", open=False):
            ref_repos = getattr(config.project, "reference_repos", []) or []
            components["reference_repos"] = gr.Textbox(
                label="Reference Repo Paths",
                value="\n".join(ref_repos) if isinstance(ref_repos, list) else str(ref_repos),
                placeholder="/path/to/repo1\n/path/to/repo2",
                info="Additional codebases the agent can grep/read (one per line)",
                lines=3,
            )

        with gr.Row():
            components["save_btn"] = gr.Button(
                "💾 Save Settings",
                variant="primary",
                size="lg",
            )

        components["status"] = gr.Markdown("")

        def on_provider_change(provider: str):
            if provider in PROVIDER_PRESETS:
                endpoint, api_type = PROVIDER_PRESETS[provider]
                return gr.update(value=endpoint), gr.update(value=api_type)
            return gr.update(), gr.update()

        components["provider"].change(
            fn=on_provider_change,
            inputs=[components["provider"]],
            outputs=[components["endpoint"], components["api_type"]],
        )

        def update_visibility(backend_type: str):
            return (
                gr.update(visible=backend_type != "local"),
                gr.update(visible=backend_type == "odibi"),
                gr.update(visible=backend_type == "delta"),
                gr.update(visible=backend_type != "local"),
            )

        components["backend_type"].change(
            fn=update_visibility,
            inputs=[components["backend_type"]],
            outputs=[
                components["connection_name"],
                components["memory_path"],
                components["delta_table"],
                components["test_memory_btn"],
            ],
        )

        def test_memory_connection(
            backend_type: str,
            connection_name: str,
            memory_path: str,
            delta_table: str,
            working_project: str,
            project_yaml: str,
        ) -> str:
            """Test if the memory backend connection works."""
            if backend_type == "local":
                return "✅ Local backend is always available"

            if backend_type == "odibi":
                if not connection_name or connection_name == "(no connections)":
                    return "❌ No connection selected"
                try:
                    from odibi.engine.pandas_engine import PandasEngine
                    from pathlib import Path
                    from ..config import get_odibi_connection

                    proj_yaml = project_yaml
                    if not proj_yaml:
                        candidate = Path(working_project) / "project.yaml"
                        if candidate.exists():
                            proj_yaml = str(candidate)

                    if not proj_yaml:
                        return f"❌ No project.yaml found (checked: {working_project}/project.yaml)"

                    connection = get_odibi_connection(proj_yaml, connection_name)
                    if not connection:
                        # Show available connections for debugging
                        from ..config import load_odibi_connections

                        available = load_odibi_connections(proj_yaml)
                        return f"❌ Connection '{connection_name}' not found in {proj_yaml}. Available: {available}"

                    engine = PandasEngine()
                    test_path = f"{memory_path}/_connection_test.json"
                    import pandas as pd

                    test_df = pd.DataFrame([{"test": "connection_check"}])
                    engine.write(
                        df=test_df,
                        connection=connection,
                        format="json",
                        path=test_path,
                        mode="overwrite",
                        options={},
                    )
                    return f"✅ ADLS connection works! Path: {memory_path}"
                except Exception as e:
                    return f"❌ Connection failed: {e}"

            if backend_type == "delta":
                try:
                    from pyspark.sql import SparkSession

                    spark = SparkSession.builder.getOrCreate()
                    spark.sql(f"DESCRIBE TABLE {delta_table}")
                    return f"✅ Delta table accessible: {delta_table}"
                except Exception as e:
                    return f"❌ Delta table check failed: {e}"

            return "❓ Unknown backend type"

        components["test_memory_btn"].click(
            fn=test_memory_connection,
            inputs=[
                components["backend_type"],
                components["connection_name"],
                components["memory_path"],
                components["delta_table"],
                components["working_project"],
                components["project_yaml"],
            ],
            outputs=[components["memory_status"]],
        )

        def refresh_connections(project_yaml_path: str):
            connections = load_odibi_connections(project_yaml_path)
            if connections:
                return gr.update(choices=connections, value=connections[0])
            return gr.update(choices=["(no connections found)"], value=None)

        components["refresh_connections_btn"].click(
            fn=refresh_connections,
            inputs=[components["project_yaml"]],
            outputs=[components["connection_name"]],
        )

        def save_settings(
            endpoint: str,
            model: str,
            api_key: str,
            api_type: str,
            backend_type: str,
            connection_name: str,
            memory_path: str,
            delta_table: str,
            working_project: str,
            project_yaml: str,
            reference_repos_text: str,
        ) -> str:
            if not working_project:
                return "❌ Working Project path is required"

            # Parse reference repos from newline-separated text
            reference_repos = [r.strip() for r in reference_repos_text.split("\n") if r.strip()]

            # If api_key is empty, preserve the current key from config (which may have come from env vars)
            # This handles the case where user edits other settings without re-entering the API key
            effective_api_key = api_key if api_key else config.llm.api_key

            new_config = AgentUIConfig(
                llm=LLMConfig(
                    endpoint=endpoint,
                    model=model,
                    api_key=effective_api_key,
                    api_type=api_type,
                    api_version=config.llm.api_version,  # Preserve api_version
                ),
                memory=MemoryConfig(
                    backend_type=backend_type,
                    connection_name=(
                        connection_name if connection_name != "(no connections)" else None
                    ),
                    path_prefix=memory_path,
                    table_path=delta_table,
                ),
                project=ProjectConfig(
                    working_project=working_project,
                    reference_repos=reference_repos,
                    project_yaml_path=project_yaml if project_yaml else None,
                ),
            )

            success = save_config(new_config)

            if on_save:
                on_save(new_config)

            if success:
                return "✅ Settings saved successfully!"
            return "❌ Failed to save settings"

        components["save_btn"].click(
            fn=save_settings,
            inputs=[
                components["endpoint"],
                components["model"],
                components["api_key"],
                components["api_type"],
                components["backend_type"],
                components["connection_name"],
                components["memory_path"],
                components["delta_table"],
                components["working_project"],
                components["project_yaml"],
                components["reference_repos"],
            ],
            outputs=[components["status"]],
        )

    return settings_column, components


def get_config_from_components(components: dict[str, Any]) -> AgentUIConfig:
    """Extract current config from component values."""
    return AgentUIConfig(
        llm=LLMConfig(
            endpoint=components["endpoint"].value,
            model=components["model"].value,
            api_key=components["api_key"].value,
            api_type=components["api_type"].value,
        ),
        memory=MemoryConfig(
            backend_type=components["backend_type"].value,
            connection_name=components["connection_name"].value,
            path_prefix=components["memory_path"].value,
            table_path=components["delta_table"].value,
        ),
        project=ProjectConfig(
            working_project=components["working_project"].value,
            reference_repos=(
                [
                    r.strip()
                    for r in (components.get("reference_repos") or gr.Textbox()).value.split("\n")
                    if r.strip()
                ]
                if components.get("reference_repos")
                else []
            ),
            project_yaml_path=components["project_yaml"].value,
        ),
    )
