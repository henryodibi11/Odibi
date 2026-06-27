"""Guard: every YAML snippet in the shipped docs validates against the current models.

The docs ship in the MCP agent corpus (odibi_mcp/_corpus), so a stale snippet would
mislead an agent reading it via get_doc. This runs the same scanner as
scripts/validate_doc_yaml.py and fails if any shipped-doc YAML drifts from the models.
"""

import importlib.util
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
_SCRIPT = REPO / "scripts" / "validate_doc_yaml.py"


def _load_validator():
    spec = importlib.util.spec_from_file_location("_validate_doc_yaml", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_all_shipped_doc_yaml_validates():
    validator = _load_validator()
    failures = validator.validate()
    if failures:
        report = "\n".join(f"  {p}:{ln}  {msg}" for p, ln, msg in failures)
        raise AssertionError(
            f"{len(failures)} stale doc YAML snippet(s) (run scripts/validate_doc_yaml.py):\n"
            + report
        )
