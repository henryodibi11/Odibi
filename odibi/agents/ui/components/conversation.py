"""Conversation management - persistence, export, and history.

Save and load conversations, export to markdown.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

import gradio as gr


@dataclass
class Conversation:
    """A saved conversation."""

    id: str
    title: str
    messages: list[dict[str, str]]
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    project_path: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "title": self.title,
            "messages": self.messages,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "project_path": self.project_path,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Conversation":
        """Create from dictionary."""
        return cls(
            id=data["id"],
            title=data["title"],
            messages=data["messages"],
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            project_path=data.get("project_path"),
        )


class ConversationStore:
    """Stores and retrieves conversations."""

    def __init__(self, storage_dir: Optional[str] = None):
        """Initialize the store.

        Args:
            storage_dir: Directory to store conversations.
                        Defaults to ~/.odibi/conversations.
        """
        if storage_dir:
            self.storage_dir = Path(storage_dir)
        else:
            self.storage_dir = Path.home() / ".odibi" / "conversations"

        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self._conversations: dict[str, Conversation] = {}
        self._load_all()

    def _load_all(self) -> None:
        """Load all saved conversations."""
        for file in self.storage_dir.glob("*.json"):
            try:
                with open(file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                conv = Conversation.from_dict(data)
                self._conversations[conv.id] = conv
            except Exception:
                continue

    def save(self, conversation: Conversation) -> None:
        """Save a conversation.

        Args:
            conversation: Conversation to save.
        """
        conversation.updated_at = datetime.now()
        self._conversations[conversation.id] = conversation

        file_path = self.storage_dir / f"{conversation.id}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(conversation.to_dict(), f, indent=2)

    def get(self, conv_id: str) -> Optional[Conversation]:
        """Get a conversation by ID.

        Args:
            conv_id: Conversation ID.

        Returns:
            Conversation or None.
        """
        return self._conversations.get(conv_id)

    def list_recent(self, limit: int = 20) -> list[Conversation]:
        """List recent conversations.

        Args:
            limit: Maximum conversations to return.

        Returns:
            List of conversations sorted by updated_at.
        """
        convs = list(self._conversations.values())
        convs.sort(key=lambda c: c.updated_at, reverse=True)
        return convs[:limit]

    def delete(self, conv_id: str) -> bool:
        """Delete a conversation.

        Args:
            conv_id: Conversation ID.

        Returns:
            True if deleted.
        """
        if conv_id in self._conversations:
            del self._conversations[conv_id]
            file_path = self.storage_dir / f"{conv_id}.json"
            if file_path.exists():
                file_path.unlink()
            return True
        return False

    def create(
        self,
        messages: list[dict[str, str]],
        title: Optional[str] = None,
        project_path: Optional[str] = None,
    ) -> Conversation:
        """Create a new conversation.

        Args:
            messages: Initial messages.
            title: Optional title (auto-generated if not provided).
            project_path: Associated project path.

        Returns:
            New Conversation.
        """
        import uuid

        conv_id = f"conv-{uuid.uuid4().hex[:8]}"

        if not title and messages:
            first_user = next(
                (m["content"][:50] for m in messages if m["role"] == "user"),
                "New Conversation",
            )
            title = first_user + ("..." if len(first_user) >= 50 else "")
        else:
            title = title or "New Conversation"

        conv = Conversation(
            id=conv_id,
            title=title,
            messages=messages,
            project_path=project_path,
        )
        self.save(conv)
        return conv


def export_to_markdown(
    messages: list[dict[str, str]],
    title: Optional[str] = None,
    include_metadata: bool = True,
) -> str:
    """Export a conversation to markdown.

    Args:
        messages: List of message dicts.
        title: Optional conversation title.
        include_metadata: Include timestamp and metadata.

    Returns:
        Markdown formatted conversation.
    """
    lines = []

    if title:
        lines.append(f"# {title}")
        lines.append("")

    if include_metadata:
        lines.append(f"*Exported: {datetime.now().strftime('%Y-%m-%d %H:%M')}*")
        lines.append("")
        lines.append("---")
        lines.append("")

    for msg in messages:
        role = msg.get("role", "unknown")
        content = msg.get("content", "")

        if role == "user":
            lines.append("## 👤 User")
        elif role == "assistant":
            lines.append("## 🤖 Assistant")
        elif role == "system":
            lines.append("## ⚙️ System")
        else:
            lines.append(f"## {role}")

        lines.append("")
        lines.append(content)
        lines.append("")
        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def generate_title(messages: list[dict[str, str]], max_length: int = 50) -> str:
    """Generate a title from messages.

    Args:
        messages: List of messages.
        max_length: Maximum title length.

    Returns:
        Generated title.
    """
    for msg in messages:
        if msg.get("role") == "user":
            content = msg.get("content", "").strip()
            content = content.split("\n")[0]
            if len(content) > max_length:
                content = content[: max_length - 3] + "..."
            return content
    return "New Conversation"


_conversation_store: Optional[ConversationStore] = None


def get_conversation_store() -> ConversationStore:
    """Get the global conversation store."""
    global _conversation_store
    if _conversation_store is None:
        _conversation_store = ConversationStore()
    return _conversation_store


def create_conversation_panel() -> tuple[gr.Column, dict[str, Any]]:
    """Create the conversation history panel.

    Returns:
        Tuple of (Gradio Column, component references).
    """
    components: dict[str, Any] = {}
    store = get_conversation_store()

    with gr.Column() as conv_column:
        gr.Markdown("## 💬 Conversations")

        def format_conv_list() -> str:
            convs = store.list_recent(10)
            if not convs:
                return "*No saved conversations*"

            lines = []
            for c in convs:
                date = c.updated_at.strftime("%m/%d %H:%M")
                lines.append(f"- **{c.title}** ({date})")
            return "\n".join(lines)

        components["conv_list"] = gr.Markdown(
            value=format_conv_list(),
            elem_classes=["conversation-list"],
        )

        with gr.Row():
            components["save_conv_btn"] = gr.Button(
                "💾 Save",
                size="sm",
                scale=1,
            )
            components["export_btn"] = gr.Button(
                "📤 Export",
                size="sm",
                scale=1,
            )

        components["export_output"] = gr.Textbox(
            label="Exported Markdown",
            visible=False,
            lines=10,
            max_lines=20,
        )

        with gr.Accordion("Load Conversation", open=False):
            recent_convs = store.list_recent(10)
            choices = [(c.title, c.id) for c in recent_convs]

            components["conv_dropdown"] = gr.Dropdown(
                label="Select Conversation",
                choices=choices,
                value=None,
            )
            components["load_conv_btn"] = gr.Button(
                "📂 Load",
                size="sm",
            )

    components["store"] = store
    return conv_column, components


def setup_conversation_handlers(
    conv_components: dict[str, Any],
    chat_components: dict[str, Any],
    get_config: Callable[[], Any],
) -> None:
    """Set up event handlers for conversation management.

    Args:
        conv_components: Conversation panel components.
        chat_components: Chat interface components.
        get_config: Function to get current config.
    """
    store = conv_components["store"]

    def save_conversation(history: list[dict]) -> str:
        if not history:
            return "*Nothing to save*"

        config = get_config()
        project_path = config.project.project_root if config else None

        store.create(
            messages=history,
            project_path=project_path,
        )

        convs = store.list_recent(10)
        lines = []
        for c in convs:
            date = c.updated_at.strftime("%m/%d %H:%M")
            lines.append(f"- **{c.title}** ({date})")

        return "\n".join(lines) if lines else "*No saved conversations*"

    conv_components["save_conv_btn"].click(
        fn=save_conversation,
        inputs=[chat_components["chatbot"]],
        outputs=[conv_components["conv_list"]],
    )

    def export_conversation(history: list[dict]) -> tuple[str, Any]:
        if not history:
            return "", gr.update(visible=False)

        markdown = export_to_markdown(history)
        return markdown, gr.update(visible=True)

    conv_components["export_btn"].click(
        fn=export_conversation,
        inputs=[chat_components["chatbot"]],
        outputs=[conv_components["export_output"], conv_components["export_output"]],
    )

    def load_conversation(conv_id: str) -> list[dict]:
        if not conv_id:
            return []

        conv = store.get(conv_id)
        if conv:
            return conv.messages
        return []

    conv_components["load_conv_btn"].click(
        fn=load_conversation,
        inputs=[conv_components["conv_dropdown"]],
        outputs=[chat_components["chatbot"]],
    )
