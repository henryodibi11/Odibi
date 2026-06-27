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
from pathlib import Path

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


def is_packaged() -> bool:
    """True when serving the packaged snapshot (pip mode), not a live repo."""
    return corpus_root() == _PKG_DIR / "_corpus"
