"""Folder picker component for selecting any project directory.

Provides a user-friendly way to select and switch between projects/codebases.
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

import gradio as gr


@dataclass
class RecentProject:
    """A recently used project."""

    path: str
    name: str
    last_used: datetime = field(default_factory=datetime.now)
    indexed: bool = False


@dataclass
class ProjectState:
    """Tracks the currently active project and recent projects."""

    active_path: str = ""
    recent_projects: list[RecentProject] = field(default_factory=list)
    max_recent: int = 10

    def set_active(self, path: str) -> None:
        """Set the active project path."""
        if not path:
            return

        try:
            path = str(Path(path).resolve())
        except (OSError, RuntimeError):
            path = str(Path(path))
        self.active_path = path

        existing = next((p for p in self.recent_projects if p.path == path), None)
        if existing:
            existing.last_used = datetime.now()
            self.recent_projects.remove(existing)
            self.recent_projects.insert(0, existing)
        else:
            name = Path(path).name or path
            self.recent_projects.insert(0, RecentProject(path=path, name=name))

        self.recent_projects = self.recent_projects[: self.max_recent]

    def get_recent_choices(self) -> list[tuple[str, str]]:
        """Get recent projects as dropdown choices."""
        return [(f"📁 {p.name} ({p.path})", p.path) for p in self.recent_projects]

    def mark_indexed(self, path: str) -> None:
        """Mark a project as indexed."""
        for p in self.recent_projects:
            if p.path == path:
                p.indexed = True
                break


def validate_project_path(path: str) -> tuple[bool, str]:
    """Validate a project path.

    Args:
        path: Path to validate.

    Returns:
        Tuple of (is_valid, message).
    """
    if not path:
        return False, "Please enter a path"

    path_obj = Path(path)

    if not path_obj.exists():
        return False, f"Path does not exist: {path}"

    if not path_obj.is_dir():
        return False, f"Path is not a directory: {path}"

    is_git_repo = (path_obj / ".git").exists()
    has_python = list(path_obj.glob("*.py")) or list(path_obj.glob("**/*.py"))

    if is_git_repo:
        return True, f"✅ Git repository: {path_obj.name}"
    elif has_python:
        return True, f"✅ Python project: {path_obj.name}"
    else:
        return True, f"✅ Directory: {path_obj.name}"


def get_project_info(path: str) -> dict[str, Any]:
    """Get information about a project directory.

    Args:
        path: Project path.

    Returns:
        Dict with project info.
    """
    path_obj = Path(path)
    info = {
        "name": path_obj.name,
        "path": str(path_obj),
        "exists": path_obj.exists(),
        "is_git": False,
        "languages": [],
        "file_count": 0,
    }

    if not path_obj.exists():
        return info

    info["is_git"] = (path_obj / ".git").exists()

    extensions = {}
    for f in path_obj.rglob("*"):
        if f.is_file() and not any(p.startswith(".") for p in f.parts):
            ext = f.suffix.lower()
            if ext:
                extensions[ext] = extensions.get(ext, 0) + 1
                info["file_count"] += 1

    lang_map = {
        ".py": "Python",
        ".js": "JavaScript",
        ".ts": "TypeScript",
        ".java": "Java",
        ".go": "Go",
        ".rs": "Rust",
        ".rb": "Ruby",
        ".cpp": "C++",
        ".c": "C",
    }

    for ext, lang in lang_map.items():
        if ext in extensions:
            info["languages"].append(lang)

    return info


def create_folder_picker(
    initial_path: str = "",
    on_change: Optional[Callable[[str], None]] = None,
    project_state: Optional[ProjectState] = None,
) -> tuple[gr.Column, dict[str, Any]]:
    """Create a folder picker component.

    Args:
        initial_path: Initial folder path.
        on_change: Callback when folder changes.
        project_state: Optional shared project state.

    Returns:
        Tuple of (Gradio Column, dict of component references).
    """
    if project_state is None:
        project_state = ProjectState()

    if initial_path:
        project_state.set_active(initial_path)

    components: dict[str, Any] = {}

    with gr.Column() as picker_column:
        gr.Markdown("### 📂 Active Project")

        with gr.Row():
            components["path_input"] = gr.Textbox(
                label="Project Path",
                value=initial_path,
                placeholder="Enter path or browse...",
                scale=4,
                info="Any folder you want the assistant to help with",
            )
            components["browse_btn"] = gr.Button(
                "📁 Browse",
                scale=1,
                size="sm",
            )

        with gr.Row():
            components["recent_dropdown"] = gr.Dropdown(
                label="Recent Projects",
                choices=project_state.get_recent_choices(),
                value=None,
                allow_custom_value=False,
                scale=3,
                visible=len(project_state.recent_projects) > 0,
            )
            components["set_project_btn"] = gr.Button(
                "✓ Set as Active",
                variant="primary",
                scale=1,
                size="sm",
            )

        components["project_status"] = gr.Markdown(
            value="*No project selected*",
        )

        with gr.Accordion("🔍 Index Options", open=False):
            with gr.Row():
                components["index_btn"] = gr.Button(
                    "📊 Index Codebase",
                    variant="primary",
                    size="sm",
                )
                components["check_index_btn"] = gr.Button(
                    "🔄 Check Index",
                    size="sm",
                )
            components["index_status"] = gr.Markdown(
                value="*Index status: Not checked*",
            )
            components["file_patterns"] = gr.Textbox(
                label="File Patterns (comma-separated)",
                value="*.py",
                placeholder="*.py, *.js, *.ts",
                info="Which file types to index for semantic search",
            )

        def on_browse():
            """Open folder browser dialog."""
            try:
                import tkinter as tk
                from tkinter import filedialog

                root = tk.Tk()
                root.withdraw()
                root.attributes("-topmost", True)
                folder = filedialog.askdirectory(
                    title="Select Project Folder",
                    mustexist=True,
                )
                root.destroy()

                if folder:
                    return folder
                return gr.update()
            except ImportError:
                return gr.update()

        components["browse_btn"].click(
            fn=on_browse,
            outputs=[components["path_input"]],
        )

        def on_set_project(path: str) -> tuple[str, Any]:
            """Set the active project."""
            is_valid, message = validate_project_path(path)

            if not is_valid:
                return f"❌ {message}", gr.update()

            project_state.set_active(path)

            if on_change:
                on_change(path)

            info = get_project_info(path)
            status_parts = [f"**{message}**"]

            if info["languages"]:
                status_parts.append(f"Languages: {', '.join(info['languages'][:3])}")
            if info["file_count"] > 0:
                status_parts.append(f"Files: {info['file_count']}")

            recent_choices = project_state.get_recent_choices()
            return (
                "\n\n".join(status_parts),
                gr.update(
                    choices=recent_choices,
                    visible=len(recent_choices) > 0,
                ),
            )

        components["set_project_btn"].click(
            fn=on_set_project,
            inputs=[components["path_input"]],
            outputs=[components["project_status"], components["recent_dropdown"]],
        )

        def on_recent_select(selection: str) -> str:
            """Handle selection from recent projects dropdown."""
            if selection:
                return selection
            return gr.update()

        components["recent_dropdown"].change(
            fn=on_recent_select,
            inputs=[components["recent_dropdown"]],
            outputs=[components["path_input"]],
        )

        def check_index_status(path: str) -> str:
            """Check if the codebase is indexed."""
            if not path:
                return "*No project path set*"

            try:
                from agents.core.index_manager import needs_reindex

                needs, reason = needs_reindex(path)
                if needs:
                    return f"⚠️ **Index needed:** {reason}"
                else:
                    return f"✅ **Index up to date:** {reason}"
            except ImportError:
                return "⚠️ Index manager not available. Install chromadb and sentence-transformers."
            except Exception as e:
                return f"❌ Error checking index: {e}"

        components["check_index_btn"].click(
            fn=check_index_status,
            inputs=[components["path_input"]],
            outputs=[components["index_status"]],
        )

        def index_codebase(path: str, file_patterns: str) -> str:
            """Index the codebase for semantic search."""
            if not path:
                return "❌ Please enter a project path"

            is_valid, message = validate_project_path(path)
            if not is_valid:
                return f"❌ {message}"

            try:
                from agents.core.code_parser import OdibiCodeParser

                target_path = Path(path)
                odibi_subdir = target_path / "odibi"

                debug_info = []
                debug_info.append(f"Path: {path}")
                debug_info.append(f"Path exists: {target_path.exists()}")
                debug_info.append(f"odibi/ exists: {odibi_subdir.exists()}")

                scan_dir = odibi_subdir if odibi_subdir.exists() else target_path
                debug_info.append(f"Scanning: {scan_dir}")

                try:
                    py_files = list(scan_dir.rglob("*.py"))
                    debug_info.append(f"rglob found: {len(py_files)} files")
                    if py_files[:3]:
                        debug_info.append(f"First 3: {[str(f.name) for f in py_files[:3]]}")
                except Exception as e:
                    debug_info.append(f"rglob error: {e}")

                parser = OdibiCodeParser(str(target_path))
                chunks = parser.parse_directory()
                debug_info.append(f"Parser returned: {len(chunks)} chunks")

                from agents.pipelines.indexer import LocalIndexer
                from agents.core.embeddings import LocalEmbedder
                from agents.core.chroma_store import ChromaVectorStore
                import shutil

                path_str = str(target_path)
                if path_str.startswith("/Workspace/") or path_str.startswith("/Repos/"):
                    import hashlib
                    path_hash = hashlib.md5(path_str.encode()).hexdigest()[:8]
                    index_dir = Path(f"/tmp/.odibi_index/{path_hash}")
                    debug_info.append("Using temp dir (Databricks read-only workspace)")
                else:
                    index_dir = target_path / ".odibi" / "index"
                debug_info.append(f"Index dir: {index_dir}")

                if index_dir.exists():
                    shutil.rmtree(index_dir, ignore_errors=True)
                    debug_info.append("Deleted old index")

                index_dir.mkdir(parents=True, exist_ok=True)

                store = ChromaVectorStore(persist_dir=str(index_dir))
                debug_info.append(f"Store created, initial count: {store.count()}")

                embedder = LocalEmbedder()
                debug_info.append(f"Embedder: {embedder.model_name}")

                indexer = LocalIndexer(
                    odibi_root=str(target_path),
                    embedder=embedder,
                    vector_store=store,
                )

                summary = indexer.run_indexing()
                debug_info.append(f"Indexing summary: {summary}")

                count = summary.get("uploaded", 0)

                project_state.mark_indexed(path)

                if count == 0:
                    return f"⚠️ Indexed **{count}** chunks\n\nDebug:\n" + "\n".join(debug_info)
                return f"✅ Indexed **{count}** code chunks from `{Path(path).name}`"
            except ImportError as e:
                return f"❌ Missing dependencies: {e}\n\nInstall: `pip install chromadb sentence-transformers`"
            except Exception as e:
                import traceback
                return f"❌ Indexing failed: {e}\n\n{traceback.format_exc()}"

        components["index_btn"].click(
            fn=index_codebase,
            inputs=[components["path_input"], components["file_patterns"]],
            outputs=[components["index_status"]],
        )

    components["project_state"] = project_state

    return picker_column, components


def get_active_project(components: dict[str, Any]) -> str:
    """Get the currently active project path from components.

    Args:
        components: Folder picker components dict.

    Returns:
        Active project path or empty string.
    """
    state = components.get("project_state")
    if state:
        return state.active_path
    return components.get("path_input", {}).value or ""
