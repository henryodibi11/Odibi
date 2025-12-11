"""Automatic index management for the Odibi Agent Suite.

Handles automatic indexing of the codebase:
- First-run indexing when no index exists
- Re-indexing when code files change
- Re-indexing when embedding model changes
"""

import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

from agents.core.chroma_store import ChromaVectorStore
from agents.core.embeddings import BaseEmbedder, LocalEmbedder

if TYPE_CHECKING:
    from agents.core.vector_store import BaseVectorStore

logger = logging.getLogger(__name__)

META_FILE = "index_meta.json"


def _compute_code_mtime(odibi_root: Path) -> float:
    """Get the most recent modification time of Python files.

    Args:
        odibi_root: Root directory of the odibi codebase.

    Returns:
        Latest mtime as a float timestamp.
    """
    latest = 0.0
    odibi_src = odibi_root / "odibi"

    if not odibi_src.exists():
        odibi_src = odibi_root

    for py_file in odibi_src.rglob("*.py"):
        if "__pycache__" in str(py_file):
            continue
        if ".odibi_index" in str(py_file):
            continue
        try:
            mtime = py_file.stat().st_mtime
            latest = max(latest, mtime)
        except OSError:
            continue

    return latest


def _load_meta(meta_path: Path) -> dict:
    """Load index metadata from file.

    Args:
        meta_path: Path to metadata file.

    Returns:
        Metadata dictionary or empty dict if not found.
    """
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_meta(meta_path: Path, meta: dict) -> None:
    """Save index metadata to file.

    Args:
        meta_path: Path to metadata file.
        meta: Metadata dictionary.
    """
    meta_path.write_text(json.dumps(meta, indent=2))


def needs_reindex(
    odibi_root: str,
    index_dir: str | None = None,
    embedding_model: str | None = None,
) -> tuple[bool, str]:
    """Check if the index needs to be rebuilt.

    Args:
        odibi_root: Root directory of the odibi codebase.
        index_dir: Directory for the index (default: <odibi_root>/.odibi_index).
        embedding_model: Embedding model name to check against.

    Returns:
        Tuple of (needs_reindex: bool, reason: str).
    """
    odibi_root_path = Path(odibi_root)
    index_dir_path = Path(index_dir or (odibi_root_path / ".odibi_index"))
    meta_path = index_dir_path / META_FILE

    embedding_model = embedding_model or LocalEmbedder.DEFAULT_MODEL

    meta = _load_meta(meta_path)
    if not meta:
        return True, "No index metadata found"

    last_indexed_mtime = meta.get("last_indexed_mtime", 0)
    last_model = meta.get("embedding_model")
    total_chunks = meta.get("total_chunks", 0)

    if total_chunks == 0:
        return True, "Index is empty"

    if last_model != embedding_model:
        return True, f"Embedding model changed: {last_model} -> {embedding_model}"

    current_mtime = _compute_code_mtime(odibi_root_path)
    if current_mtime > last_indexed_mtime:
        return True, "Code files have been modified since last index"

    return False, "Index is up to date"


def ensure_index(
    odibi_root: str,
    index_dir: str | None = None,
    embedding_model: str | None = None,
    embedder: BaseEmbedder | None = None,
    force_reindex: bool = False,
) -> "BaseVectorStore":
    """Ensure the local index exists and is up to date.

    This function:
    1. Checks if an index exists
    2. Checks if code has changed since last indexing
    3. Checks if embedding model has changed
    4. Rebuilds the index if needed

    Args:
        odibi_root: Root directory of the odibi codebase.
        index_dir: Directory for the index (default: <odibi_root>/.odibi_index).
        embedding_model: Embedding model name (default: all-MiniLM-L6-v2).
        embedder: Optional pre-configured embedder.
        force_reindex: Force a full reindex regardless of status.

    Returns:
        Configured vector store ready for queries.
    """
    from agents.pipelines.indexer import LocalIndexer

    odibi_root_path = Path(odibi_root)
    index_dir_path = Path(index_dir or (odibi_root_path / ".odibi_index"))
    index_dir_path.mkdir(parents=True, exist_ok=True)
    meta_path = index_dir_path / META_FILE

    embedding_model = embedding_model or LocalEmbedder.DEFAULT_MODEL

    store = ChromaVectorStore(persist_dir=str(index_dir_path))

    if force_reindex:
        should_reindex = True
        reason = "Forced reindex requested"
    else:
        should_reindex, reason = needs_reindex(
            odibi_root=odibi_root,
            index_dir=str(index_dir_path),
            embedding_model=embedding_model,
        )

    if not should_reindex:
        logger.info(f"Using existing index: {reason}")
        return store

    logger.info(f"Indexing required: {reason}")

    if embedder is None:
        embedder = LocalEmbedder(model_name=embedding_model)

    store.delete_all()

    indexer = LocalIndexer(
        odibi_root=str(odibi_root_path),
        embedder=embedder,
        vector_store=store,
    )

    summary = indexer.run_indexing()

    current_mtime = _compute_code_mtime(odibi_root_path)
    meta = {
        "last_indexed_mtime": current_mtime,
        "embedding_model": embedding_model,
        "embedding_dimension": embedder.dimension,
        "total_chunks": summary.get("total_chunks", 0),
        "indexed_at": time.time(),
    }
    _save_meta(meta_path, meta)

    logger.info(f"Index created: {summary.get('total_chunks', 0)} chunks")
    return store


def get_index(
    odibi_root: str,
    index_dir: str | None = None,
) -> ChromaVectorStore | None:
    """Get an existing index without triggering reindexing.

    Args:
        odibi_root: Root directory of the odibi codebase.
        index_dir: Directory for the index.

    Returns:
        Vector store if index exists, None otherwise.
    """
    odibi_root_path = Path(odibi_root)
    index_dir_path = Path(index_dir or (odibi_root_path / ".odibi_index"))

    if not index_dir_path.exists():
        return None

    store = ChromaVectorStore(persist_dir=str(index_dir_path))
    if not store.exists():
        return None

    return store
