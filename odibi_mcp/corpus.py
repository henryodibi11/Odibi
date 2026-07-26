"""Corpus root resolution for the Odibi MCP server.

The agent-facing content (docs, examples, .assistant skills) must be reachable in
two deployment modes:

* **repo mode** — the agent runs from a checkout; the live ``docs/``/``examples/``
  /``.assistant/`` trees are used (always current, grep-able).
* **pip mode** — ``pip install odibi[mcp]`` with no repo; none of those trees ship
  at the repo root, so a packaged snapshot under ``odibi_mcp/_corpus/`` is used.

``corpus_root()`` returns whichever applies, so the KnowledgeBase doc/search/example
methods (which read ``<root>/docs`` etc.) work identically in both. Set
``ODIBI_DOCS_ROOT`` to force a specific tree.
"""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath, PureWindowsPath

_PKG_DIR = Path(__file__).resolve().parent


def corpus_root() -> Path:
    """Resolve the root that contains ``docs/``, ``examples/`` and ``.assistant/``."""
    override = os.environ.get("ODIBI_DOCS_ROOT")
    if override and (Path(override) / "docs").is_dir():
        return Path(override)

    # repo mode: the package's parent is the repo root and ships the live trees.
    repo_root = _PKG_DIR.parent
    if (repo_root / "docs").is_dir():
        return repo_root

    # pip mode: fall back to the packaged snapshot.
    return _PKG_DIR / "_corpus"


def resolve_corpus_file(
    readable_root: Path,
    relative_path: str,
    *,
    allowed_suffixes: tuple[str, ...],
) -> Path | None:
    """Resolve a supported regular file contained by one corpus subtree."""
    if not isinstance(relative_path, str) or not relative_path or "\x00" in relative_path:
        return None
    if "\\" in relative_path or ":" in relative_path:
        return None

    posix_path = PurePosixPath(relative_path)
    windows_path = PureWindowsPath(relative_path)
    if posix_path.is_absolute() or windows_path.is_absolute() or windows_path.drive:
        return None
    if any(part in {"", ".", ".."} for part in relative_path.split("/")):
        return None
    if posix_path.suffix not in allowed_suffixes:
        return None

    try:
        resolved_root = readable_root.resolve(strict=True)
        candidate = (resolved_root / Path(*posix_path.parts)).resolve(strict=True)
        candidate.relative_to(resolved_root)
        return candidate if candidate.is_file() else None
    except (OSError, RuntimeError, ValueError):
        return None


def resolve_corpus_directory(readable_root: Path, relative_path: str) -> Path | None:
    """Resolve a directory contained by one canonical corpus subtree."""
    if not isinstance(relative_path, str) or not relative_path or "\x00" in relative_path:
        return None
    if "\\" in relative_path or ":" in relative_path:
        return None

    posix_path = PurePosixPath(relative_path)
    windows_path = PureWindowsPath(relative_path)
    if posix_path.is_absolute() or windows_path.is_absolute() or windows_path.drive:
        return None
    if any(part in {"", ".", ".."} for part in relative_path.split("/")):
        return None

    try:
        resolved_root = readable_root.resolve(strict=True)
        candidate = (resolved_root / Path(*posix_path.parts)).resolve(strict=True)
        candidate.relative_to(resolved_root)
        return candidate if candidate.is_dir() else None
    except (OSError, RuntimeError, ValueError):
        return None


def is_packaged() -> bool:
    """True when serving the packaged snapshot (pip mode), not a live repo."""
    return corpus_root() == _PKG_DIR / "_corpus"
