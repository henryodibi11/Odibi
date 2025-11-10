"""
Explanation Quality Linter
===========================

Validates that explanations meet Odibi quality standards.
"""

from typing import List
from dataclasses import dataclass
import re


@dataclass
class LintIssue:
    """A linting issue found in an explanation."""

    severity: str  # "error", "warning", "info"
    message: str
    rule: str

    def __str__(self):
        symbol = {"error": "❌", "warning": "⚠️", "info": "ℹ️"}[self.severity]
        return f"{symbol} {self.message} [{self.rule}]"


class ExplanationLinter:
    """
    Lints explanation text for quality issues.

    Checks:
    - Minimum length
    - Required sections (Purpose, Details, Result)
    - Generic/lazy phrases
    - TODO placeholders
    - Formula formatting
    """

    REQUIRED_SECTIONS = ["Purpose", "Details", "Result"]

    LAZY_PHRASES = [
        "calculates stuff",
        "does things",
        "processes data",
        "handles records",
        "TODO",
        "[placeholder]",
        "TBD",
        "to be determined",
    ]

    MIN_LENGTH = 50  # characters

    def __init__(self):
        self.issues: List[LintIssue] = []

    def lint(self, explanation: str, operation_name: str = "unknown") -> List[LintIssue]:
        """
        Lint an explanation and return issues.

        Args:
            explanation: The explanation text
            operation_name: Name of the operation (for error messages)

        Returns:
            List of LintIssue objects
        """
        self.issues = []

        if not explanation or not explanation.strip():
            self.issues.append(
                LintIssue(
                    severity="error",
                    message=f"Explanation for '{operation_name}' is empty",
                    rule="E001",
                )
            )
            return self.issues

        # Check length
        self._check_length(explanation, operation_name)

        # Check required sections
        self._check_required_sections(explanation, operation_name)

        # Check for lazy phrases
        self._check_lazy_phrases(explanation, operation_name)

        # Check formula formatting
        self._check_formula_formatting(explanation, operation_name)

        return self.issues

    def _check_length(self, text: str, op_name: str):
        """Check minimum length requirement."""
        if len(text.strip()) < self.MIN_LENGTH:
            self.issues.append(
                LintIssue(
                    severity="error",
                    message=f"Explanation for '{op_name}' too short ({len(text.strip())} chars, minimum {self.MIN_LENGTH})",
                    rule="E002",
                )
            )

    def _check_required_sections(self, text: str, op_name: str):
        """Check for required sections."""
        for section in self.REQUIRED_SECTIONS:
            pattern = f"\\*\\*{section}:?\\*\\*"
            if not re.search(pattern, text, re.IGNORECASE):
                self.issues.append(
                    LintIssue(
                        severity="error",
                        message=f"Explanation for '{op_name}' missing required section: {section}",
                        rule="E003",
                    )
                )

    def _check_lazy_phrases(self, text: str, op_name: str):
        """Check for generic/lazy phrases."""
        text_lower = text.lower()
        for phrase in self.LAZY_PHRASES:
            if phrase.lower() in text_lower:
                self.issues.append(
                    LintIssue(
                        severity="error",
                        message=f"Explanation for '{op_name}' contains generic phrase: '{phrase}'",
                        rule="E004",
                    )
                )

    def _check_formula_formatting(self, text: str, op_name: str):
        """Check formula formatting."""
        # If mentions "formula" but no code block
        if "formula" in text.lower():
            if "```" not in text:
                self.issues.append(
                    LintIssue(
                        severity="warning",
                        message=f"Explanation for '{op_name}' mentions formula but no code block found",
                        rule="W001",
                    )
                )

    def has_errors(self) -> bool:
        """Check if any errors were found."""
        return any(issue.severity == "error" for issue in self.issues)

    def format_issues(self) -> str:
        """Format all issues as string."""
        if not self.issues:
            return "✅ No issues found"

        lines = []
        for issue in self.issues:
            lines.append(str(issue))
        return "\n".join(lines)
