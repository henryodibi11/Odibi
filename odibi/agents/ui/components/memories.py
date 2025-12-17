"""Memory panel component for the Odibi Assistant.

View, search, and manage agent memories.
"""

import logging
from typing import Any, Callable, Optional

import gradio as gr
from odibi.agents.core.memory import Memory, MemoryManager, MemoryType

from ..config import AgentUIConfig

logger = logging.getLogger(__name__)

_current_backend_info: str = "not initialized"


def get_current_backend_info() -> str:
    """Return the current backend description."""
    return _current_backend_info


MEMORY_TYPE_COLORS = {
    MemoryType.DECISION: "🔵",
    MemoryType.LEARNING: "🟢",
    MemoryType.BUG_FIX: "🔴",
    MemoryType.PREFERENCE: "🟣",
    MemoryType.TODO: "🟡",
    MemoryType.FEATURE: "🟠",
    MemoryType.CONTEXT: "⚪",
    MemoryType.CONVERSATION: "💬",
}


def create_memory_panel(
    config: Optional[AgentUIConfig] = None,
) -> tuple[gr.Column, dict[str, Any]]:
    """Create the memory viewer panel.

    Args:
        config: Configuration for memory backend.

    Returns:
        Tuple of (Gradio Column, dict of component references).
    """
    components: dict[str, Any] = {}

    with gr.Column(scale=1) as memory_column:
        gr.Markdown("## 📚 Memories")

        with gr.Row():
            components["search_input"] = gr.Textbox(
                label="Search",
                placeholder="Search memories...",
                scale=3,
            )
            components["search_btn"] = gr.Button("🔍", scale=1, size="sm")

        components["memory_type_filter"] = gr.CheckboxGroup(
            label="Filter by Type",
            choices=[
                ("🔵 Decision", "decision"),
                ("🟢 Learning", "learning"),
                ("🔴 Bug Fix", "bug_fix"),
                ("🟡 TODO", "todo"),
                ("🟠 Feature", "feature"),
            ],
            value=["decision", "learning", "bug_fix"],
        )

        components["memory_list"] = gr.Markdown(
            value="_Loading memories..._",
            elem_classes=["memory-list"],
        )

        gr.Markdown("---")
        gr.Markdown(
            "_Memories are auto-saved by the agent. "
            "Use 'remember' in chat to save specific learnings._"
        )

    return memory_column, components


def format_memory_list(memories: list[Memory]) -> str:
    """Format memories for display.

    Args:
        memories: List of memories to format.

    Returns:
        Markdown-formatted memory list.
    """
    if not memories:
        return "_No memories found_"

    lines = []
    for mem in memories[:20]:
        emoji = MEMORY_TYPE_COLORS.get(mem.memory_type, "📝")
        type_label = mem.memory_type.value.upper()
        date = mem.created_at[:10] if mem.created_at else ""

        lines.append(f"**{emoji} [{type_label}]** {mem.summary}")
        lines.append(f"_{date}_ | Importance: {mem.importance:.1f}")

        if mem.tags:
            tags_str = " ".join(f"`{t}`" for t in mem.tags[:5])
            lines.append(f"Tags: {tags_str}")

        lines.append("")

    return "\n".join(lines)


def get_memory_manager(config: Optional[AgentUIConfig] = None) -> MemoryManager:
    """Get or create a memory manager.

    Args:
        config: UI configuration.

    Returns:
        MemoryManager instance.
    """
    global _current_backend_info

    if config is None:
        return MemoryManager(backend_type="local")

    backend_type = config.memory.backend_type
    project_root = config.project.project_root or "."

    if backend_type == "local":
        return MemoryManager(
            backend_type="local",
            local_path=f"{project_root}/.odibi/memories",
        )

    if backend_type == "odibi" and config.memory.connection_name:
        try:
            from odibi.engine.pandas_engine import PandasEngine
            from ..config import get_odibi_connection

            # Try to find project.yaml
            project_yaml = config.project.project_yaml_path
            if not project_yaml:
                from pathlib import Path

                candidate = Path(project_root) / "project.yaml"
                if candidate.exists():
                    project_yaml = str(candidate)

            if project_yaml:
                connection = get_odibi_connection(project_yaml, config.memory.connection_name)
                if connection:
                    _current_backend_info = (
                        f"ADLS via '{config.memory.connection_name}' "
                        f"-> {config.memory.path_prefix}"
                    )
                    logger.info("Memory backend: %s", _current_backend_info)
                    return MemoryManager(
                        backend_type="odibi",
                        connection=connection,
                        engine=PandasEngine(),
                        path_prefix=config.memory.path_prefix or "agent/memories",
                    )
                else:
                    logger.warning(
                        "Connection '%s' not found in %s",
                        config.memory.connection_name,
                        project_yaml,
                    )
            else:
                logger.warning(
                    "No project.yaml found. Cannot load connection '%s'. Falling back to local.",
                    config.memory.connection_name,
                )
        except Exception as e:
            logger.error("Failed to create odibi backend: %s", e)

    if backend_type == "delta":
        try:
            return MemoryManager(
                backend_type="delta",
                table_path=config.memory.table_path or "system.agent_memories",
            )
        except Exception as e:
            logger.error("Failed to create delta backend: %s", e)

    # Fallback to local
    _current_backend_info = f"local -> {project_root}/.odibi/memories"
    logger.info("Memory backend: %s", _current_backend_info)
    return MemoryManager(
        backend_type="local",
        local_path=f"{project_root}/.odibi/memories",
    )


def setup_memory_handlers(
    components: dict[str, Any],
    get_config: Callable[[], Any],
) -> None:
    """Set up event handlers for memory panel.

    Args:
        components: Dict of Gradio components.
        get_config: Function to get current config.
    """

    def search_memories(query: str, type_filters: list[str]) -> str:
        try:
            config = get_config()
            manager = get_memory_manager(config)

            if query:
                memory_types = [MemoryType(t) for t in type_filters] if type_filters else None
                memories = manager.recall(query, memory_types=memory_types, top_k=20)
            else:
                memory_types = [MemoryType(t) for t in type_filters] if type_filters else None
                memories = manager.store.get_recent(days=30, memory_types=memory_types, limit=20)

            return format_memory_list(memories)
        except Exception as e:
            logger.error("Memory search error: %s", e, exc_info=True)
            return f"❌ Error loading memories: {e}"

    def refresh_memories(type_filters: list[str]) -> str:
        return search_memories("", type_filters)

    components["search_btn"].click(
        fn=search_memories,
        inputs=[components["search_input"], components["memory_type_filter"]],
        outputs=[components["memory_list"]],
    )

    components["search_input"].submit(
        fn=search_memories,
        inputs=[components["search_input"], components["memory_type_filter"]],
        outputs=[components["memory_list"]],
    )

    components["memory_type_filter"].change(
        fn=refresh_memories,
        inputs=[components["memory_type_filter"]],
        outputs=[components["memory_list"]],
    )
