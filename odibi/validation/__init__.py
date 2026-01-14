"""
Quality Enforcement and Validation
===================================

This module enforces Odibi's quality standards through automated validation.

Features:
- Explanation linting: Ensure transformations are documented
- Quality scoring: Detect generic/lazy documentation
- Schema validation: Verify config structure
- Pre-run validation: Catch errors before execution
- Quarantine tables: Route failed rows to dedicated tables
- Quality gates: Batch-level validation thresholds
- FK validation: Referential integrity checks for star schemas

Principle: Enforce excellence, don't hope for it.
"""

from .engine import Validator
from .explanation_linter import ExplanationLinter, LintIssue
from .fk import (
    FKValidationReport,
    FKValidationResult,
    FKValidator,
    OrphanRecord,
    RelationshipConfig,
    RelationshipRegistry,
    get_orphan_records,
    parse_relationships_config,
    validate_fk_on_load,
)
from .gate import GateResult, evaluate_gate
from .quarantine import (
    QuarantineResult,
    add_quarantine_metadata,
    has_quarantine_tests,
    split_valid_invalid,
    write_quarantine,
)

__all__ = [
    "ExplanationLinter",
    "LintIssue",
    "Validator",
    "GateResult",
    "evaluate_gate",
    "QuarantineResult",
    "add_quarantine_metadata",
    "has_quarantine_tests",
    "split_valid_invalid",
    "write_quarantine",
    "FKValidator",
    "FKValidationResult",
    "FKValidationReport",
    "OrphanRecord",
    "RelationshipConfig",
    "RelationshipRegistry",
    "get_orphan_records",
    "validate_fk_on_load",
    "parse_relationships_config",
]
