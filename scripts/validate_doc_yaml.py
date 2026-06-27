"""Validate the YAML snippets embedded in shipped docs against the real models.

The docs ship in the MCP agent corpus (odibi_mcp/_corpus), so an agent reads them
via get_doc — a stale snippet that no longer matches the strict config models would
mislead it. This scans the user-facing docs, extracts ```yaml blocks, and validates:

* full configs (have ``project:`` + ``pipelines:``)  -> ProjectConfig
* connection blocks (``connections:`` with a known ``type``) -> ConnectionConfig

Illustrative fragments (ellipses / <placeholders> / ${vars}) are skipped.

Exit code 1 with a report if any snippet fails. Used in CI and as the gate when
editing docs.

Run:  python scripts/validate_doc_yaml.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
DOC_DIRS = [
    "features",
    "guides",
    "patterns",
    "tutorials",
    "reference",
    "validation",
    "simulation",
    "skills",
    "examples",
]
TOP = ["ODIBI_DEEP_CONTEXT.md", "golden_path.md", "troubleshooting.md", "index.md"]
KNOWN_CONN_TYPES = {"local", "azure_blob", "sql_server", "http", "delta"}
FENCE = re.compile(r"```ya?ml\s*\n(.*?)```", re.DOTALL)


def _has_placeholder(block: str) -> bool:
    return (
        "..." in block
        or ("<" in block and ">" in block)
        or "YOUR_" in block
        or ("${" in block and "vars." in block)
    )


def _doc_files():
    d = REPO / "docs"
    for sub in DOC_DIRS:
        if (d / sub).is_dir():
            yield from sorted((d / sub).rglob("*.md"))
    for f in TOP:
        if (d / f).is_file():
            yield d / f


def validate() -> list[tuple[str, int, str]]:
    from odibi.connections.factory import register_builtins
    from odibi.transformers import register_standard_library

    register_standard_library()
    register_builtins()
    from pydantic import TypeAdapter, ValidationError

    from odibi.config import ConnectionConfig, ProjectConfig

    conn_ta = TypeAdapter(ConnectionConfig)
    failures: list[tuple[str, int, str]] = []

    for md in _doc_files():
        text = md.read_text(encoding="utf-8")
        for m in FENCE.finditer(text):
            block = m.group(1)
            line = text[: m.start()].count("\n") + 1
            if _has_placeholder(block):
                continue
            try:
                data = yaml.safe_load(block)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            rel = str(md.relative_to(REPO)).replace("\\", "/")
            if "project" in data and "pipelines" in data:
                try:
                    ProjectConfig(**data)
                except ValidationError as e:
                    failures.append((rel, line, _first_error(e)))
                except Exception as e:  # noqa: BLE001
                    failures.append((rel, line, f"{type(e).__name__}: {str(e)[:120]}"))
            elif isinstance(data.get("connections"), dict):
                for cname, ccfg in data["connections"].items():
                    if isinstance(ccfg, dict) and ccfg.get("type") in KNOWN_CONN_TYPES:
                        if _has_placeholder(yaml.safe_dump(ccfg)):
                            continue
                        try:
                            conn_ta.validate_python(ccfg)
                        except ValidationError as e:
                            failures.append((rel, line, f"connection '{cname}': {_first_error(e)}"))
                        except Exception:  # noqa: BLE001
                            pass
    return failures


def _first_error(e) -> str:
    errs = e.errors()
    if not errs:
        return str(e)[:160]
    err = errs[0]
    loc = ".".join(str(p) for p in err.get("loc", ()))
    return f"{loc}: {err.get('msg', '')}"[:200]


def main() -> int:
    failures = validate()
    if not failures:
        print("All doc YAML snippets validate against the current models.")
        return 0
    print(f"{len(failures)} stale doc YAML snippet(s) found:\n")
    for path, line, msg in failures:
        print(f"  {path}:{line}\n      {msg}")
    print("\nFix the snippets to match the current models (see get_schema / the skills).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
