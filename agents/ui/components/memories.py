"""Memory panel component for the Odibi Assistant.

View, search, and manage agent memories.
"""

from typing import Any, Optional

import gradio as gr
from odibi.agents.core.memory import Memory, MemoryManager, MemoryType

from ..config import AgentUIConfig

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
        gr.Markdown("### ➕ Quick Save")

        components["new_memory_type"] = gr.Dropdown(
            label="Type",
            choices=[
                ("🔵 Decision", "decision"),
                ("🟢 Learning", "learning"),
                ("🔴 Bug Fix", "bug_fix"),
                ("🟡 TODO", "todo"),
                ("🟠 Feature", "feature"),
                ("⚪ Context", "context"),
            ],
            value="learning",
        )

        components["new_memory_summary"] = gr.Textbox(
            label="Summary",
            placeholder="Brief summary...",
            lines=1,
        )

        components["new_memory_content"] = gr.Textbox(
            label="Details",
            placeholder="Full details...",
            lines=3,
        )

        components["new_memory_tags"] = gr.Textbox(
            label="Tags (comma-separated)",
            placeholder="tag1, tag2, tag3",
            lines=1,
        )

        components["save_memory_btn"] = gr.Button(
            "💾 Save Memory",
            variant="secondary",
        )

        components["save_status"] = gr.Markdown("")

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
    if config is None:
        return MemoryManager(backend_type="local")

    if config.memory.backend_type == "local":
        return MemoryManager(
            backend_type="local",
            local_path=f"{config.project.odibi_root}/.odibi/memories",
        )

    return MemoryManager(backend_type="local")


def setup_memory_handlers(
    components: dict[str, Any],
    get_config: callable,
) -> None:
    """Set up event handlers for memory panel.

    Args:
        components: Dict of Gradio components.
        get_config: Function to get current config.
    """

    def search_memories(query: str, type_filters: list[str]) -> str:
        config = get_config()
        manager = get_memory_manager(config)

        if query:
            memory_types = [MemoryType(t) for t in type_filters] if type_filters else None
            memories = manager.recall(query, memory_types=memory_types, top_k=20)
        else:
            memory_types = [MemoryType(t) for t in type_filters] if type_filters else None
            memories = manager.store.get_recent(days=30, memory_types=memory_types, limit=20)

        return format_memory_list(memories)

    def save_new_memory(
        memory_type: str,
        summary: str,
        content: str,
        tags_str: str,
    ) -> tuple[str, str, str, str, str]:
        if not summary:
            return (
                "❌ Summary is required",
                gr.update(),
                gr.update(),
                gr.update(),
                gr.update(),
            )

        config = get_config()
        manager = get_memory_manager(config)

        tags = [t.strip() for t in tags_str.split(",") if t.strip()] if tags_str else []

        manager.remember(
            memory_type=MemoryType(memory_type),
            summary=summary,
            content=content or summary,
            tags=tags,
            importance=0.7,
        )

        memories = manager.store.get_recent(days=30, limit=20)
        memory_list_md = format_memory_list(memories)

        return (
            "✅ Memory saved!",
            "",
            "",
            "",
            memory_list_md,
        )

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

    components["save_memory_btn"].click(
        fn=save_new_memory,
        inputs=[
            components["new_memory_type"],
            components["new_memory_summary"],
            components["new_memory_content"],
            components["new_memory_tags"],
        ],
        outputs=[
            components["save_status"],
            components["new_memory_summary"],
            components["new_memory_content"],
            components["new_memory_tags"],
            components["memory_list"],
        ],
    )
