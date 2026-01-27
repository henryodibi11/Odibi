import os
import sys
import tempfile
from pathlib import Path

import pytest

from odibi.introspect import generate_docs


@pytest.mark.skipif(
    sys.version_info[:2] != (3, 11) or os.environ.get("CI") == "true",
    reason="Type hint rendering differs between Python versions; CI regenerates docs before tests",
)
def test_docs_are_synced():
    """
    Ensure that docs/reference/yaml_schema.md is up-to-date with the codebase.
    If this fails, run 'python odibi/introspect.py' to update the docs.

    Note: This test is skipped in CI because the workflow regenerates docs
    before running tests, making this check redundant.
    """
    docs_path = Path("docs/reference/yaml_schema.md")
    if not docs_path.exists():
        assert False, "docs/reference/yaml_schema.md does not exist"

    existing_content = docs_path.read_text(encoding="utf-8")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / "api_check.md"
        generate_docs(output_path=str(tmp_path))

        generated_content = tmp_path.read_text(encoding="utf-8")

        # Normalize line endings just in case
        existing_content = existing_content.replace("\r\n", "\n")
        generated_content = generated_content.replace("\r\n", "\n")

        assert existing_content == generated_content, (
            "docs/reference/yaml_schema.md is out of sync with code. Run 'python odibi/introspect.py' to update."
        )
