"""Todo/Progress panel component.

Shows task breakdown and progress as the agent works.
Similar to Amp's todo tracking feature.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

import gradio as gr


class TodoStatus(Enum):
    """Status of a todo item."""

    TODO = "todo"
    IN_PROGRESS = "in-progress"
    COMPLETED = "completed"


@dataclass
class TodoItem:
    """A single todo item."""

    id: str
    content: str
    status: TodoStatus = TodoStatus.TODO
    created_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "content": self.content,
            "status": self.status.value,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TodoItem":
        """Create from dictionary."""
        return cls(
            id=data["id"],
            content=data["content"],
            status=TodoStatus(data.get("status", "todo")),
        )


class TodoManager:
    """Manages the todo list for a session."""

    def __init__(self):
        self.todos: list[TodoItem] = []
        self._id_counter = 0

    def _next_id(self) -> str:
        self._id_counter += 1
        return f"todo-{self._id_counter}"

    def add(self, content: str, status: TodoStatus = TodoStatus.TODO) -> TodoItem:
        """Add a new todo item."""
        item = TodoItem(id=self._next_id(), content=content, status=status)
        self.todos.append(item)
        return item

    def update_status(self, todo_id: str, status: TodoStatus) -> Optional[TodoItem]:
        """Update a todo's status."""
        for item in self.todos:
            if item.id == todo_id:
                item.status = status
                if status == TodoStatus.COMPLETED:
                    item.completed_at = datetime.now()
                return item
        return None

    def mark_completed(self, todo_id: str) -> Optional[TodoItem]:
        """Mark a todo as completed."""
        return self.update_status(todo_id, TodoStatus.COMPLETED)

    def mark_in_progress(self, todo_id: str) -> Optional[TodoItem]:
        """Mark a todo as in progress."""
        return self.update_status(todo_id, TodoStatus.IN_PROGRESS)

    def clear(self) -> None:
        """Clear all todos."""
        self.todos = []
        self._id_counter = 0

    def replace_all(self, todos: list[dict]) -> None:
        """Replace all todos with a new list."""
        self.todos = [TodoItem.from_dict(t) for t in todos]
        if self.todos:
            valid_ids = [
                int(t.id.split("-")[1])
                for t in self.todos
                if "-" in t.id and t.id.split("-")[1].isdigit()
            ]
            self._id_counter = max(valid_ids) if valid_ids else len(self.todos)

    def to_list(self) -> list[dict]:
        """Convert all todos to list of dicts."""
        return [t.to_dict() for t in self.todos]

    def format_markdown(self) -> str:
        """Format todos as markdown for display."""
        if not self.todos:
            return "*No tasks*"

        lines = []
        for item in self.todos:
            if item.status == TodoStatus.COMPLETED:
                icon = "✅"
                style = "~~"
            elif item.status == TodoStatus.IN_PROGRESS:
                icon = "🔄"
                style = "**"
            else:
                icon = "⬜"
                style = ""

            content = item.content
            if style:
                content = f"{style}{content}{style}"
            lines.append(f"{icon} {content}")

        completed = sum(1 for t in self.todos if t.status == TodoStatus.COMPLETED)
        total = len(self.todos)
        progress = f"\n\n**Progress:** {completed}/{total}"

        return "\n".join(lines) + progress


_todo_manager: Optional[TodoManager] = None


def get_todo_manager() -> TodoManager:
    """Get the global todo manager instance."""
    global _todo_manager
    if _todo_manager is None:
        _todo_manager = TodoManager()
    return _todo_manager


def create_todo_panel() -> tuple[gr.Column, dict[str, Any]]:
    """Create the todo panel component.

    Returns:
        Tuple of (Gradio Column, dict of component references).
    """
    components: dict[str, Any] = {}
    manager = get_todo_manager()

    with gr.Column() as todo_column:
        gr.Markdown("## 📋 Tasks")

        components["todo_display"] = gr.Markdown(
            value=manager.format_markdown(),
            elem_classes=["todo-list"],
        )

        with gr.Accordion("Manage Tasks", open=False):
            with gr.Row():
                components["new_todo_input"] = gr.Textbox(
                    label="New Task",
                    placeholder="Add a task...",
                    scale=3,
                )
                components["add_todo_btn"] = gr.Button("➕", scale=1, size="sm")

            components["clear_todos_btn"] = gr.Button(
                "🗑️ Clear All",
                size="sm",
                variant="secondary",
            )

        def add_todo(content: str) -> tuple[str, str]:
            if content.strip():
                manager.add(content.strip())
            return manager.format_markdown(), ""

        components["add_todo_btn"].click(
            fn=add_todo,
            inputs=[components["new_todo_input"]],
            outputs=[components["todo_display"], components["new_todo_input"]],
        )

        components["new_todo_input"].submit(
            fn=add_todo,
            inputs=[components["new_todo_input"]],
            outputs=[components["todo_display"], components["new_todo_input"]],
        )

        def clear_todos() -> str:
            manager.clear()
            return manager.format_markdown()

        components["clear_todos_btn"].click(
            fn=clear_todos,
            outputs=[components["todo_display"]],
        )

    components["manager"] = manager
    return todo_column, components


def update_todo_display() -> str:
    """Get the current todo display markdown."""
    return get_todo_manager().format_markdown()


def todo_write(todos: list[dict]) -> str:
    """Tool function: Replace all todos.

    Args:
        todos: List of todo items with id, content, status.

    Returns:
        Confirmation message.
    """
    manager = get_todo_manager()
    manager.replace_all(todos)
    return f"Updated {len(todos)} tasks"


def todo_read() -> list[dict]:
    """Tool function: Read current todos.

    Returns:
        List of current todo items.
    """
    return get_todo_manager().to_list()
