"""Conversation management - persistence, export, and history.

Save and load conversations, export to markdown.
Supports multiple backends: local, odibi (ADLS), delta.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional, TYPE_CHECKING

import gradio as gr

if TYPE_CHECKING:
    from ..config import AgentUIConfig

logger = logging.getLogger(__name__)


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
    """Stores and retrieves conversations.

    Supports multiple backends via the memory backend system:
    - local: JSON files in ~/.odibi/conversations or project/.odibi/conversations
    - odibi: ADLS/Azure Blob via Odibi connection
    - delta: Delta Lake table
    """

    def __init__(
        self,
        storage_dir: Optional[str] = None,
        backend_type: str = "local",
        connection: Optional[Any] = None,
        engine: Optional[Any] = None,
        path_prefix: str = "agent/conversations",
    ):
        """Initialize the store.

        Args:
            storage_dir: Directory for local storage (if backend_type="local").
            backend_type: "local", "odibi", or "delta".
            connection: Odibi connection (for odibi/delta backends).
            engine: Odibi engine (for odibi/delta backends).
            path_prefix: Path prefix for remote backends.
        """
        self.backend_type = backend_type
        self._conversations: dict[str, Conversation] = {}

        if backend_type == "local":
            if storage_dir:
                self.storage_dir = Path(storage_dir)
            else:
                self.storage_dir = Path.home() / ".odibi" / "conversations"
            self.storage_dir.mkdir(parents=True, exist_ok=True)
            self._backend = None
        else:
            self.storage_dir = None
            from odibi.agents.core.memory_backends import create_memory_backend

            self._backend = create_memory_backend(
                backend_type,
                connection=connection,
                engine=engine,
                path_prefix=path_prefix,
            )

        self._load_all()

    def _load_all(self) -> None:
        """Load all saved conversations."""
        if self.backend_type == "local" and self.storage_dir:
            for file in self.storage_dir.glob("*.json"):
                try:
                    with open(file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    conv = Conversation.from_dict(data)
                    self._conversations[conv.id] = conv
                except Exception:
                    continue
        elif self._backend:
            try:
                for conv_id in self._backend.list_all():
                    data = self._backend.load(conv_id)
                    if data:
                        conv = Conversation.from_dict(data)
                        self._conversations[conv.id] = conv
            except Exception as e:
                logger.warning("Failed to load conversations from backend: %s", e)

    def save(self, conversation: Conversation) -> None:
        """Save a conversation.

        Args:
            conversation: Conversation to save.
        """
        conversation.updated_at = datetime.now()
        self._conversations[conversation.id] = conversation

        if self.backend_type == "local" and self.storage_dir:
            file_path = self.storage_dir / f"{conversation.id}.json"
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(conversation.to_dict(), f, indent=2)
        elif self._backend:
            try:
                self._backend.save(conversation.id, conversation.to_dict())
            except Exception as e:
                logger.error("Failed to save conversation to backend: %s", e)

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
            if self.backend_type == "local" and self.storage_dir:
                file_path = self.storage_dir / f"{conv_id}.json"
                if file_path.exists():
                    file_path.unlink()
            elif self._backend:
                try:
                    self._backend.delete(conv_id)
                except Exception as e:
                    logger.warning("Failed to delete conversation from backend: %s", e)
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
            # Find first user message content
            first_user_content = None
            for m in messages:
                if isinstance(m, dict) and m.get("role") == "user":
                    content = m.get("content", "")
                    # Handle both string and list content formats
                    if isinstance(content, list):
                        # Extract text from content parts
                        content = " ".join(
                            p.get("text", str(p)) if isinstance(p, dict) else str(p)
                            for p in content
                        )
                    first_user_content = str(content)[:50] if content else None
                    break
                elif isinstance(m, (list, tuple)) and len(m) >= 1:
                    # Gradio tuple format: (user_msg, assistant_msg)
                    content = m[0] if m[0] else ""
                    if isinstance(content, list):
                        content = " ".join(str(p) for p in content)
                    first_user_content = str(content)[:50] if content else None
                    break

            if first_user_content:
                title = first_user_content + ("..." if len(first_user_content) >= 50 else "")
            else:
                title = "New Conversation"
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
_conversation_store_config_hash: Optional[str] = None


def get_conversation_store(
    config: Optional["AgentUIConfig"] = None,
) -> ConversationStore:
    """Get or create a conversation store based on config.

    Args:
        config: UI configuration. If provided, uses the memory backend settings.
                If None, uses local storage.

    Returns:
        ConversationStore instance configured for the appropriate backend.
    """
    global _conversation_store, _conversation_store_config_hash

    # Calculate config hash to detect changes
    if config:
        config_hash = f"{config.memory.backend_type}:{config.memory.connection_name}:{config.memory.path_prefix}"
    else:
        config_hash = "local:none:none"

    # Return cached store if config hasn't changed
    if _conversation_store is not None and _conversation_store_config_hash == config_hash:
        return _conversation_store

    # Create new store based on config
    if config is None or config.memory.backend_type == "local":
        project_root = config.project.project_root if config else None
        storage_dir = f"{project_root}/.odibi/conversations" if project_root else None
        _conversation_store = ConversationStore(storage_dir=storage_dir)
    elif config.memory.backend_type == "odibi" and config.memory.connection_name:
        try:
            from odibi.engine.pandas_engine import PandasEngine
            from pathlib import Path
            from ..config import get_odibi_connection

            # Find project.yaml
            project_yaml = config.project.project_yaml_path
            if not project_yaml:
                candidate = Path(config.project.project_root or ".") / "project.yaml"
                if candidate.exists():
                    project_yaml = str(candidate)

            if project_yaml:
                connection = get_odibi_connection(project_yaml, config.memory.connection_name)
                if connection:
                    # Use conversations subfolder within memory path
                    conv_path = f"{config.memory.path_prefix or 'agent'}/conversations"
                    _conversation_store = ConversationStore(
                        backend_type="odibi",
                        connection=connection,
                        engine=PandasEngine(),
                        path_prefix=conv_path,
                    )
                    logger.info(
                        "Conversation store: ADLS via '%s' -> %s",
                        config.memory.connection_name,
                        conv_path,
                    )
                else:
                    logger.warning(
                        "Connection '%s' not found, falling back to local",
                        config.memory.connection_name,
                    )
                    _conversation_store = ConversationStore()
            else:
                logger.warning("No project.yaml found, falling back to local")
                _conversation_store = ConversationStore()
        except Exception as e:
            logger.error("Failed to create ADLS conversation store: %s", e)
            _conversation_store = ConversationStore()
    elif config.memory.backend_type == "delta":
        try:
            _conversation_store = ConversationStore(
                backend_type="delta",
                path_prefix=config.memory.table_path or "system.agent_conversations",
            )
            logger.info("Conversation store: Delta -> %s", config.memory.table_path)
        except Exception as e:
            logger.error("Failed to create Delta conversation store: %s", e)
            _conversation_store = ConversationStore()
    else:
        _conversation_store = ConversationStore()

    _conversation_store_config_hash = config_hash
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
            with gr.Row():
                components["load_conv_btn"] = gr.Button(
                    "📂 Load",
                    size="sm",
                )
                components["delete_conv_btn"] = gr.Button(
                    "🗑️ Delete",
                    size="sm",
                    variant="stop",
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

    def save_conversation(history: list[dict]) -> str:
        if not history:
            return "*Nothing to save*"

        try:
            config = get_config()
            # Get store with config to use correct backend (local/ADLS/delta)
            store = get_conversation_store(config)
            project_path = config.project.project_root if config else None

            conv = store.create(
                messages=history,
                project_path=project_path,
            )
            logger.info("Saved conversation: %s", conv.id if conv else "unknown")

            convs = store.list_recent(10)
            lines = []
            for c in convs:
                date = c.updated_at.strftime("%m/%d %H:%M")
                lines.append(f"- **{c.title}** ({date})")

            # Also update dropdown choices
            choices = [(c.title, c.id) for c in convs]
            return (
                "\n".join(lines) if lines else "*No saved conversations*",
                gr.update(choices=choices),
            )
        except Exception as e:
            logger.error("Failed to save conversation: %s", e, exc_info=True)
            return f"❌ Save failed: {e}", gr.update()

    conv_components["save_conv_btn"].click(
        fn=save_conversation,
        inputs=[chat_components["chatbot"]],
        outputs=[conv_components["conv_list"], conv_components["conv_dropdown"]],
    )

    def export_conversation(history: list[dict]) -> Any:
        if not history:
            return gr.update(visible=True, value="*No conversation to export*")

        try:
            markdown = export_to_markdown(history)
            return gr.update(visible=True, value=markdown)
        except Exception as e:
            logger.error("Export failed: %s", e, exc_info=True)
            return gr.update(visible=True, value=f"❌ Export failed: {e}")

    conv_components["export_btn"].click(
        fn=export_conversation,
        inputs=[chat_components["chatbot"]],
        outputs=[conv_components["export_output"]],
    )

    def load_conversation(conv_id: str) -> list[dict]:
        if not conv_id:
            return []

        try:
            config = get_config()
            store = get_conversation_store(config)
            conv = store.get(conv_id)
            if conv:
                logger.info("Loaded conversation: %s (%d messages)", conv_id, len(conv.messages))
                return conv.messages
            logger.warning("Conversation not found: %s", conv_id)
            return []
        except Exception as e:
            logger.error("Failed to load conversation: %s", e, exc_info=True)
            return []

    conv_components["load_conv_btn"].click(
        fn=load_conversation,
        inputs=[conv_components["conv_dropdown"]],
        outputs=[chat_components["chatbot"]],
    )

    def delete_conversation(conv_id: str) -> tuple[str, Any]:
        """Delete selected conversation and refresh the list."""
        if not conv_id:
            return "*Select a conversation to delete*", gr.update()

        try:
            config = get_config()
            store = get_conversation_store(config)
            deleted = store.delete(conv_id)

            if deleted:
                # Refresh the conversation list
                convs = store.list_recent(10)
                lines = []
                for c in convs:
                    date = c.updated_at.strftime("%m/%d %H:%M")
                    lines.append(f"- **{c.title}** ({date})")

                # Update dropdown choices
                choices = [(c.title, c.id) for c in convs]
                return (
                    "\n".join(lines) if lines else "*No saved conversations*",
                    gr.update(choices=choices, value=None),
                )
            else:
                return "*Conversation not found*", gr.update()
        except Exception as e:
            logger.error("Failed to delete conversation: %s", e, exc_info=True)
            return f"❌ Delete failed: {e}", gr.update()

    conv_components["delete_conv_btn"].click(
        fn=delete_conversation,
        inputs=[conv_components["conv_dropdown"]],
        outputs=[conv_components["conv_list"], conv_components["conv_dropdown"]],
    )
