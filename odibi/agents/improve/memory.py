"""CampaignMemory: Persistent memory across campaign cycles.

Stores lessons learned, successful patterns, and failed approaches
in JSONL files. This memory prevents the campaign from repeating
mistakes and helps it build on successful strategies.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from odibi.agents.improve.results import CycleResult

logger = logging.getLogger(__name__)


@dataclass
class Lesson:
    """A lesson learned from a cycle."""

    cycle_id: str
    timestamp: str
    category: str  # "success", "failure", "pattern"
    description: str
    approach: Optional[str] = None
    outcome: Optional[str] = None
    tags: list[str] = field(default_factory=list)


class CampaignMemory:
    """Persistent memory across campaign cycles.

    Stores data in JSONL files for easy appending and reading.
    Files:
    - lessons.jsonl: All lessons (successes and failures)
    - avoided_issues.jsonl: Approaches that didn't work
    - patterns.jsonl: Successful patterns to prefer
    """

    def __init__(self, memory_path: Path) -> None:
        """Initialize memory.

        Args:
            memory_path: Directory containing memory files.
        """
        self._memory_path = Path(memory_path)
        self._lessons_file = self._memory_path / "lessons.jsonl"
        self._avoided_file = self._memory_path / "avoided_issues.jsonl"
        self._patterns_file = self._memory_path / "patterns.jsonl"

        # Ensure files exist
        self._ensure_files_exist()

    @property
    def memory_path(self) -> Path:
        """Path to memory directory."""
        return self._memory_path

    def _ensure_files_exist(self) -> None:
        """Ensure memory files exist."""
        self._memory_path.mkdir(parents=True, exist_ok=True)
        for file in [self._lessons_file, self._avoided_file, self._patterns_file]:
            if not file.exists():
                file.touch()

    def _append_jsonl(self, file_path: Path, data: dict) -> None:
        """Append a JSON object to a JSONL file."""
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(data) + "\n")

    def _read_jsonl(self, file_path: Path) -> list[dict]:
        """Read all JSON objects from a JSONL file."""
        if not file_path.exists():
            return []

        records = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSONL line: {e}")
        return records

    def record(self, result: CycleResult) -> None:
        """Record what happened this cycle.

        Args:
            result: The completed cycle result.
        """
        timestamp = datetime.now().isoformat()

        # Extract detailed failure info for useful memories
        detailed_failures = self._extract_detailed_failures(result)

        # Record the cycle as a lesson
        lesson = {
            "cycle_id": result.cycle_id,
            "timestamp": timestamp,
            "promoted": result.promoted,
            "learning": result.learning,
            "lesson": result.lesson,
            "rejection_reason": result.rejection_reason,
            "files_modified": result.files_modified,
            "tests_passed": result.tests_passed,
            "tests_failed": result.tests_failed,
            "lint_errors": result.lint_errors,
            "golden_passed": result.golden_passed,
            "golden_failed": result.golden_failed,
            "detailed_failures": detailed_failures,  # NEW: specific errors
        }
        self._append_jsonl(self._lessons_file, lesson)

        # If rejected, record what to avoid
        if not result.promoted:
            if detailed_failures:
                # Record specific failures (lint errors, test failures, etc.)
                for failure in detailed_failures[:5]:
                    avoided = {
                        "cycle_id": result.cycle_id,
                        "timestamp": timestamp,
                        "type": failure.get("type", "unknown"),
                        "approach": failure.get("description", "unknown"),
                        "file": failure.get("file", ""),
                        "line": failure.get("line"),
                        "code": failure.get("code", ""),
                        "reason": failure.get("message", result.rejection_reason or ""),
                        "files": result.files_modified,
                    }
                    self._append_jsonl(self._avoided_file, avoided)
            elif result.lesson:
                # No detailed failures but has a lesson - record it
                avoided = {
                    "cycle_id": result.cycle_id,
                    "timestamp": timestamp,
                    "type": "general",
                    "approach": result.lesson,
                    "reason": result.rejection_reason or "",
                    "files": result.files_modified,
                }
                self._append_jsonl(self._avoided_file, avoided)

        # If promoted, record successful patterns with detail
        if result.promoted and result.learning:
            pattern = {
                "cycle_id": result.cycle_id,
                "timestamp": timestamp,
                "pattern": result.lesson or "improvement",
                "files": result.files_modified,
                "tests_passed": result.tests_passed,
            }
            self._append_jsonl(self._patterns_file, pattern)

        logger.debug(f"Recorded cycle {result.cycle_id} to memory")

    def _extract_detailed_failures(self, result: CycleResult) -> list[dict]:
        """Extract specific, actionable failure details from cycle result.

        Returns list of dicts with: type, file, line, code, message, description
        """
        import re

        failures = []

        # Extract lint errors with file/line info
        if result.lint_result and hasattr(result.lint_result, "errors"):
            for error in result.lint_result.errors[:10]:
                # Parse ruff output like "F401 [*] `threading` imported but unused"
                # or "file.py:10:5: E501 line too long"
                match = re.match(
                    r"(?:([^:]+):(\d+):(\d+):\s*)?([A-Z]\d+)\s*(.+)",
                    str(error).strip(),
                )
                if match:
                    failures.append(
                        {
                            "type": "lint",
                            "file": match.group(1) or "",
                            "line": int(match.group(2)) if match.group(2) else None,
                            "code": match.group(4),
                            "message": match.group(5),
                            "description": f"Lint {match.group(4)}: {match.group(5)[:100]}",
                        }
                    )
                else:
                    failures.append(
                        {
                            "type": "lint",
                            "description": str(error)[:150],
                            "message": str(error),
                        }
                    )

        # Extract test failures with test names - only if there are actual failures
        if (
            result.test_result
            and hasattr(result.test_result, "failed_tests")
            and getattr(result.test_result, "failed", 0) > 0
        ):
            for test in result.test_result.failed_tests[:10]:
                # Parse test names like "tests/test_foo.py::test_bar"
                parts = str(test).split("::")
                failures.append(
                    {
                        "type": "test",
                        "file": parts[0] if parts else "",
                        "test_name": parts[-1] if len(parts) > 1 else test,
                        "description": f"Test failed: {test}",
                        "message": f"Test {test} failed",
                    }
                )

        # Extract golden project failures
        if result.golden_results:
            for golden in result.golden_results:
                if not golden.passed:
                    failures.append(
                        {
                            "type": "golden",
                            "file": golden.config_path,
                            "description": f"Golden project failed: {golden.config_name}",
                            "message": golden.error_message or "Unknown error",
                        }
                    )

        return failures

    def get_failed_approaches(self) -> list[str]:
        """Get list of approaches that didn't work.

        Returns:
            List of approach descriptions to avoid.
        """
        avoided = self._read_jsonl(self._avoided_file)
        return [record.get("approach", "") for record in avoided if record.get("approach")]

    def get_successful_patterns(self) -> list[str]:
        """Get list of patterns that worked.

        Returns:
            List of successful pattern descriptions.
        """
        patterns = self._read_jsonl(self._patterns_file)
        return [record.get("pattern", "") for record in patterns if record.get("pattern")]

    def get_recent_cycles(self, n: int) -> list[CycleResult]:
        """Get N most recent cycle results.

        Args:
            n: Number of recent cycles to return.

        Returns:
            List of CycleResult objects (simplified reconstruction).
        """
        lessons = self._read_jsonl(self._lessons_file)
        recent = lessons[-n:] if len(lessons) >= n else lessons

        results = []
        for lesson in recent:
            result = CycleResult(
                cycle_id=lesson.get("cycle_id", "unknown"),
                sandbox_path=Path("."),  # Placeholder
                started_at=datetime.fromisoformat(
                    lesson.get("timestamp", datetime.now().isoformat())
                ),
                promoted=lesson.get("promoted", False),
                learning=lesson.get("learning", False),
                lesson=lesson.get("lesson"),
                rejection_reason=lesson.get("rejection_reason"),
                files_modified=lesson.get("files_modified", []),
                tests_passed=lesson.get("tests_passed", 0),
                tests_failed=lesson.get("tests_failed", 0),
                lint_errors=lesson.get("lint_errors", 0),
                golden_passed=lesson.get("golden_passed", 0),
                golden_failed=lesson.get("golden_failed", 0),
            )
            results.append(result)

        return results

    def get_all_lessons(self) -> list[Lesson]:
        """Get all recorded lessons.

        Returns:
            List of Lesson objects.
        """
        records = self._read_jsonl(self._lessons_file)
        lessons = []
        for record in records:
            lesson = Lesson(
                cycle_id=record.get("cycle_id", "unknown"),
                timestamp=record.get("timestamp", ""),
                category="success" if record.get("promoted") else "failure",
                description=record.get("lesson") or record.get("rejection_reason") or "",
                approach=record.get("lesson"),
                outcome="promoted" if record.get("promoted") else "rejected",
            )
            lessons.append(lesson)
        return lessons

    def query_similar(self, issue: str) -> list[Lesson]:
        """Find similar past issues and what we learned.

        Simple keyword matching for now.

        Args:
            issue: Description of the issue.

        Returns:
            List of related lessons.
        """
        keywords = set(issue.lower().split())
        lessons = self.get_all_lessons()

        similar = []
        for lesson in lessons:
            lesson_text = f"{lesson.description} {lesson.approach or ''}".lower()
            lesson_words = set(lesson_text.split())

            # Simple overlap score
            overlap = len(keywords & lesson_words)
            if overlap > 0:
                similar.append((overlap, lesson))

        # Sort by overlap score descending
        similar.sort(key=lambda x: x[0], reverse=True)
        return [lesson for _, lesson in similar[:10]]

    def get_cycle_count(self) -> int:
        """Get total number of recorded cycles.

        Returns:
            Number of cycles in memory.
        """
        return len(self._read_jsonl(self._lessons_file))

    def get_promotion_count(self) -> int:
        """Get number of promoted cycles.

        Returns:
            Number of successful promotions.
        """
        lessons = self._read_jsonl(self._lessons_file)
        return sum(1 for lesson in lessons if lesson.get("promoted"))

    def clear(self) -> None:
        """Clear all memory files.

        WARNING: This is destructive and removes all lessons.
        """
        for file in [self._lessons_file, self._avoided_file, self._patterns_file]:
            if file.exists():
                file.unlink()
        self._ensure_files_exist()
        logger.info("Cleared all memory files")
