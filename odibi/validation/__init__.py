"""
Quality Enforcement and Validation
===================================

This module enforces Odibi's quality standards through automated validation.

Features:
- Explanation linting: Ensure transformations are documented
- Quality scoring: Detect generic/lazy documentation
- Schema validation: Verify config structure
- Pre-run validation: Catch errors before execution

Principle: Enforce excellence, don't hope for it.
"""

from .explanation_linter import ExplanationLinter, LintIssue

__all__ = ["ExplanationLinter", "LintIssue"]
__version__ = "1.3.0-alpha.1"
