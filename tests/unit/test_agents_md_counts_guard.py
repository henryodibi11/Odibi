"""Guard AGENTS.md against drift-prone counts.

AGENTS.md used to carry per-file test counts, per-module coverage percentages and
"Remaining: ~N lines" annotations in its coverage sections. They drifted (the catalog
mock test counts were stale by the time issue #358 defect 1 was fixed), so they were
removed and must not come back. The dated Coverage Roadmap section keeps its numbers as
a historical record and is skipped here.

Pure text scan - no pyspark, no Spark - so it runs on Windows and in CI.
"""

import re
from pathlib import Path

AGENTS_MD = Path(__file__).resolve().parents[2] / "AGENTS.md"

# The dated historical section whose numbers are kept on purpose.
ROADMAP_MARKER = "Coverage Roadmap"

DRIFT_PATTERNS = [
    # '(22 tests)', '(10+31 tests)'
    re.compile(r"\(\d[\d+]* tests?\)"),
    # '(126 tests across 5 files)'
    re.compile(r"\(\d[\d+]* tests? across \d+ files?\)"),
    # '(50+39 utility/execution tests)'
    re.compile(r"\(\d[\d+]*\+\d[\d+]* [^()]*tests?\)"),
    # '70 new tests', '118 new tests across 3 files'
    re.compile(r"\b\d[\d+]* new tests?\b"),
    # '**85% covered**', '**~95% covered**'
    re.compile(r"\*\*~?\d+% covered\*\*"),
    # '**100%**'
    re.compile(r"\*\*~?\d+%\*\*"),
    # 'The 4% coverage is misleading'
    re.compile(r"\b\d+% coverage\b"),
    # '**35 tests**'
    re.compile(r"\*\*\d[\d+]* tests?\*\*"),
    # 'Remaining: ~466 stmts', 'Remaining: ~5 lines'
    re.compile(r"Remaining:[^\n]*?~?\d[\d,]* (?:lines?|stmts)\b"),
    # 'Remaining: (lines 366-570)', 'Remaining: _merge_spark lines 424-631'
    re.compile(r"Remaining:[^\n]*?\blines? \d+-\d+"),
    # '(lines 22-137)' in any line
    re.compile(r"\(lines \d+-\d+\)"),
    # 'This 2,200+ line document'
    re.compile(r"\b\d[\d,]*\+ line document\b"),
    # 'All 6 patterns', 'List all 6 patterns'
    re.compile(r"\ball \d+ patterns\b", re.IGNORECASE),
    # '52+ transformers', 'List all 54 transformers'
    re.compile(r"\b\d+\+? transformers\b"),
    # 'all 11 validation test types'
    re.compile(r"all \d+ validation test types", re.IGNORECASE),
]


def _scannable_lines() -> list[tuple[int, str]]:
    """AGENTS.md lines with the dated Coverage Roadmap section removed."""
    lines = AGENTS_MD.read_text(encoding="utf-8").splitlines()
    result: list[tuple[int, str]] = []
    skipping = False
    for lineno, line in enumerate(lines, start=1):
        if skipping:
            # Any heading of level <= 3 ends the skipped section (level 4+ headings
            # belong to it).
            if (line.startswith("## ") or line.startswith("### ")) and ROADMAP_MARKER not in line:
                skipping = False
            else:
                continue
        if line.startswith("### ") and ROADMAP_MARKER in line:
            skipping = True
            continue
        result.append((lineno, line))
    return result


def test_agents_md_has_no_drift_prone_counts() -> None:
    offenders = []
    for lineno, line in _scannable_lines():
        for pattern in DRIFT_PATTERNS:
            match = pattern.search(line)
            if match:
                offenders.append(f"line {lineno}: {match.group(0)!r} in {line.strip()!r}")
    assert not offenders, (
        "AGENTS.md re-acquired drift-prone counts. Keep behavioural descriptions, not "
        "numbers (the dated Coverage Roadmap section is exempt):\n" + "\n".join(offenders)
    )
