"""Folder picker component for selecting any project directory.

Provides a user-friendly way to select and switch between projects/codebases.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

import gradio as gr

_EMBEDDER_CACHE = {"embedder": None}


def get_cached_embedder():
    """Get or create the cached embedder (loads model once)."""
    if _EMBEDDER_CACHE["embedder"] is None:
        from odibi.agents.core.embeddings import LocalEmbedder

        print("[Indexer] Loading embedding model (one-time)...")
        embedder = LocalEmbedder()
        _ = embedder.dimension  # Force load
        print("[Indexer] Model ready!")
        _EMBEDDER_CACHE["embedder"] = embedder
    return _EMBEDDER_CACHE["embedder"]


@dataclass
class RecentProject:
    """A recently used project."""

    path: str
    name: str
    last_used: datetime = field(default_factory=datetime.now)
    indexed: bool = False
    index_dir: str = ""


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
            # On Databricks, resolve() can fail for /Workspace paths
            resolved = Path(path).resolve()
            path = str(resolved) if resolved.exists() else path
        except (OSError, RuntimeError, Exception):
            pass  # Keep original path
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

    def mark_indexed(self, path: str, index_dir: str = "") -> None:
        """Mark a project as indexed."""
        for p in self.recent_projects:
            if p.path == path:
                p.indexed = True
                if index_dir:
                    p.index_dir = index_dir
                break

    def get_index_dir(self, path: str) -> str:
        """Get the index directory for a project."""
        for p in self.recent_projects:
            if p.path == path:
                return p.index_dir
        return ""


_INDEX_REGISTRY_FILE = os.path.join(tempfile.gettempdir(), ".odibi_index_registry.json")


def _get_existing_chunk_count(index_dir: Path) -> int:
    """Get count of chunks in existing index."""
    try:
        from odibi.agents.core.chroma_store import ChromaVectorStore

        store = ChromaVectorStore(persist_dir=str(index_dir))
        return store.count()
    except Exception:
        return 0


def _load_index_registry() -> dict:
    """Load the index registry from disk."""
    import json

    try:
        with open(_INDEX_REGISTRY_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_index_registry(registry: dict) -> None:
    """Save the index registry to disk."""
    import json

    with open(_INDEX_REGISTRY_FILE, "w") as f:
        json.dump(registry, f)


def register_index_dir(project_path: str, index_dir: str) -> None:
    """Register an index directory for a project path."""
    try:
        registry = _load_index_registry()
        registry[project_path] = index_dir
        _save_index_registry(registry)
    except Exception:
        pass  # Non-critical, don't fail indexing


def _set_agent_vector_store(store, embedder) -> None:
    """Wire up the vector store to OdibiAgent for context retrieval.

    Args:
        store: ChromaVectorStore instance.
        embedder: Embedder for query vectors.
    """
    from odibi.agents.core.agent_base import OdibiAgent

    OdibiAgent.set_global_vector_store(store, embedder)


def load_existing_vector_store(index_dir: str) -> bool:
    """Load an existing vector store and wire it to agents.

    Args:
        index_dir: Path to the index directory.

    Returns:
        True if successfully loaded, False otherwise.
    """
    try:
        from odibi.agents.core.chroma_store import ChromaVectorStore

        store = ChromaVectorStore(persist_dir=index_dir)
        if store.exists():
            embedder = get_cached_embedder()
            _set_agent_vector_store(store, embedder)
            return True
    except Exception:
        pass
    return False


def get_registered_index_dir(project_path: str) -> str | None:
    """Get the registered index directory for a project path."""
    registry = _load_index_registry()
    registered = registry.get(project_path)
    if registered and Path(registered).exists():
        return registered
    return None


def find_existing_index_dir(project_path: str) -> str | None:
    """Find an existing index directory for a project path.

    Scans /tmp/.odibi_index/ for directories matching the project hash.
    Returns the most recently modified one.
    """
    import hashlib

    if not (project_path.startswith("/Workspace/") or project_path.startswith("/Repos/")):
        local_index = Path(project_path) / ".odibi" / "index"
        if local_index.exists():
            return str(local_index)
        return None

    path_hash = hashlib.md5(project_path.encode()).hexdigest()[:8]
    base_dir = Path("/tmp/.odibi_index")

    if not base_dir.exists():
        return None

    matching_dirs = []
    for d in base_dir.iterdir():
        if d.is_dir() and d.name.startswith(path_hash):
            try:
                mtime = d.stat().st_mtime
                matching_dirs.append((mtime, str(d)))
            except OSError:
                continue

    if matching_dirs:
        matching_dirs.sort(reverse=True)
        return matching_dirs[0][1]

    return None


def get_index_dir(project_path: str, unique: bool = False) -> Path:
    """Get the appropriate index directory for a project.

    On Databricks (/Workspace/ or /Repos/), uses /tmp/ since the
    workspace is read-only for SQLite operations.

    Args:
        project_path: The project root path.
        unique: If True, append a timestamp to create a unique directory.

    Returns:
        Path to the index directory.
    """
    import hashlib
    import time

    if project_path.startswith("/Workspace/") or project_path.startswith("/Repos/"):
        path_hash = hashlib.md5(project_path.encode()).hexdigest()[:8]
        base = f"/tmp/.odibi_index/{path_hash}"
        if unique:
            base = f"{base}_{int(time.time())}"
        return Path(base)
    return Path(project_path) / ".odibi" / "index"


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

    lang_checks = [
        ("*.py", "Python"),
        ("*.js", "JavaScript"),
        ("*.ts", "TypeScript"),
        ("*.java", "Java"),
        ("*.go", "Go"),
    ]

    for pattern, lang in lang_checks:
        try:
            if next(path_obj.glob(pattern), None) is not None:
                info["languages"].append(lang)
                if len(info["languages"]) >= 3:
                    break
        except (OSError, StopIteration):
            continue

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
        gr.Markdown("### 📂 Workspace Root")

        with gr.Row():
            components["path_input"] = gr.Textbox(
                label="Directory Path",
                value=initial_path,
                placeholder="Enter path or browse...",
                scale=4,
                info="Directory containing projects or pipelines to work on",
            )
            components["browse_btn"] = gr.Button(
                "📁 Browse",
                scale=1,
                size="sm",
            )

        with gr.Row():
            components["recent_dropdown"] = gr.Dropdown(
                label="Recent Workspaces",
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
            components["force_reindex"] = gr.Checkbox(
                label="Force re-index (delete existing)",
                value=False,
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
                from odibi.agents.core.index_manager import needs_reindex

                index_dir = (
                    project_state.get_index_dir(path)
                    or get_registered_index_dir(path)
                    or find_existing_index_dir(path)
                    or str(get_index_dir(path))
                )

                needs, reason = needs_reindex(path, index_dir=index_dir)
                if needs:
                    return f"⚠️ **Index needed:** {reason}\n\nIndex dir: `{index_dir}`"
                else:
                    return f"✅ **Index up to date:** {reason}\n\nIndex dir: `{index_dir}`"
            except ImportError:
                return "⚠️ Index manager not available. Install chromadb and sentence-transformers."
            except Exception as e:
                return f"❌ Error checking index: {e}"

        components["check_index_btn"].click(
            fn=check_index_status,
            inputs=[components["path_input"]],
            outputs=[components["index_status"]],
        )

        def index_codebase(path: str, file_patterns: str, force_reindex: bool) -> str:
            """Index the codebase for semantic search."""
            if not path:
                return "❌ Please enter a project path"

            is_valid, message = validate_project_path(path)
            if not is_valid:
                return f"❌ {message}"

            try:
                from odibi.agents.core.chroma_store import ChromaVectorStore
                from odibi.agents.core.code_parser import OdibiCodeParser
                import shutil

                target_path = Path(path)
                is_databricks = path.startswith("/Workspace/") or path.startswith("/Repos/")

                if is_databricks:
                    index_dir = get_index_dir(path, unique=True)
                else:
                    index_dir = get_index_dir(path)
                    if index_dir.exists() and (index_dir / "chroma.sqlite3").exists():
                        if force_reindex:
                            print("[0/5] Deleting existing index...")
                            shutil.rmtree(index_dir, ignore_errors=True)
                        else:
                            count = _get_existing_chunk_count(index_dir)
                            if count > 0:
                                project_state.mark_indexed(path, index_dir=str(index_dir))
                                # Wire up existing index to agents
                                load_existing_vector_store(str(index_dir))
                                return f"✅ Index already exists with **{count}** chunks. Check 'Force re-index' to rebuild."

                index_dir.mkdir(parents=True, exist_ok=True)

                print("[1/5] Initializing vector store...")
                store = ChromaVectorStore(persist_dir=str(index_dir))

                print("[2/5] Getting embedding model...")
                embedder = get_cached_embedder()
                print("[2/5] Model ready!")

                print("[3/5] Parsing codebase...")
                parser = OdibiCodeParser(str(target_path))
                chunks = parser.parse_directory()
                chunk_dicts = [chunk.to_dict() for chunk in chunks]
                total_chunks = len(chunk_dicts)
                print(f"[3/5] Parsed {total_chunks} chunks")

                if total_chunks == 0:
                    return "⚠️ No code chunks found to index"

                print(f"[4/5] Embedding {total_chunks} chunks...")

                texts = []
                for chunk in chunk_dicts:
                    text_parts = [
                        f"Name: {chunk['name']}",
                        f"Type: {chunk['chunk_type']}",
                        f"Module: {chunk['module_name']}",
                    ]
                    if chunk.get("docstring"):
                        text_parts.append(f"Docstring: {chunk['docstring']}")
                    if chunk.get("signature"):
                        text_parts.append(f"Signature: {chunk['signature']}")
                    text_parts.append(f"Code:\n{chunk['content'][:2000]}")
                    texts.append("\n".join(text_parts))

                batch_size = 16
                all_embeddings = []
                for i in range(0, len(texts), batch_size):
                    batch = texts[i : i + batch_size]
                    batch_embeddings = embedder.embed_texts(batch, batch_size=batch_size)
                    all_embeddings.extend(batch_embeddings)
                    done = min(i + batch_size, len(texts))
                    print(f"[4/5] Embedding: {done}/{len(texts)}")

                for chunk, embedding in zip(chunk_dicts, all_embeddings):
                    chunk["content_vector"] = embedding

                print("[5/5] Uploading to vector store...")
                result = store.add_chunks(chunk_dicts)
                count = result.get("succeeded", 0)

                project_state.mark_indexed(path, index_dir=str(index_dir))
                register_index_dir(path, str(index_dir))

                # Wire up vector store to agents
                _set_agent_vector_store(store, embedder)

                print(f"[DONE] Indexed {count} chunks!")

                if count == 0:
                    error_msg = result.get("error", "Unknown error")
                    return f"⚠️ Indexed **0** chunks. Error: {error_msg}"
                return f"✅ Indexed **{count}** code chunks from `{Path(path).name}`"
            except ImportError as e:
                return f"❌ Missing dependencies: {e}\n\nInstall: `pip install chromadb sentence-transformers`"
            except Exception as e:
                return f"❌ Indexing failed: {e}"

        components["index_btn"].click(
            fn=index_codebase,
            inputs=[
                components["path_input"],
                components["file_patterns"],
                components["force_reindex"],
            ],
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
