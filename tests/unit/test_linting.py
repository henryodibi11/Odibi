"""Tests for explanation linting."""

from odibi.validation import ExplanationLinter, LintIssue


class TestExplanationLinter:
    """Tests for ExplanationLinter class."""

    def test_empty_explanation_fails(self):
        """Empty explanation should fail validation."""
        linter = ExplanationLinter()
        issues = linter.lint("", "test_op")

        assert len(issues) == 1
        assert issues[0].severity == "error"
        assert "empty" in issues[0].message.lower()
        assert issues[0].rule == "E001"

    def test_too_short_explanation_fails(self):
        """Short explanation should fail validation."""
        linter = ExplanationLinter()
        issues = linter.lint("Short", "test_op")

        assert len(issues) > 0
        assert any("too short" in issue.message.lower() for issue in issues)
        assert linter.has_errors()

    def test_valid_explanation_passes(self):
        """Valid explanation should pass validation."""
        valid_explanation = """
**Purpose:**
Calculate efficiency for plant operations

**Details:**
- Uses fuel consumption and output
- Applies standard efficiency calculation

**Result:**
Efficiency percentage for performance tracking
"""
        linter = ExplanationLinter()
        issues = linter.lint(valid_explanation, "calc_efficiency")

        assert len(issues) == 0
        assert not linter.has_errors()

    def test_missing_purpose_section_fails(self):
        """Missing Purpose section should fail."""
        missing_purpose = """
**Details:**
- Some detail here

**Result:**
Some result here
"""
        linter = ExplanationLinter()
        issues = linter.lint(missing_purpose, "test_op")

        assert any("missing required section: Purpose" in issue.message for issue in issues)
        assert linter.has_errors()

    def test_missing_details_section_fails(self):
        """Missing Details section should fail."""
        missing_details = """
**Purpose:**
Some purpose here

**Result:**
Some result here
"""
        linter = ExplanationLinter()
        issues = linter.lint(missing_details, "test_op")

        assert any("Details" in issue.message for issue in issues)
        assert linter.has_errors()

    def test_missing_result_section_fails(self):
        """Missing Result section should fail."""
        missing_result = """
**Purpose:**
Some purpose here

**Details:**
- Some detail here
"""
        linter = ExplanationLinter()
        issues = linter.lint(missing_result, "test_op")

        assert any("Result" in issue.message for issue in issues)
        assert linter.has_errors()

    def test_lazy_phrase_todo_fails(self):
        """Lazy phrase 'TODO' should fail."""
        lazy_explanation = """
**Purpose:**
TODO: Write this later

**Details:**
- Something something

**Result:**
Some result
"""
        linter = ExplanationLinter()
        issues = linter.lint(lazy_explanation, "test_op")

        assert any("generic phrase" in issue.message.lower() for issue in issues)
        assert any("TODO" in issue.message for issue in issues)
        assert linter.has_errors()

    def test_lazy_phrase_processes_data_fails(self):
        """Lazy phrase 'processes data' should fail."""
        lazy_explanation = """
**Purpose:**
This transformation processes data for the pipeline

**Details:**
- Processes data efficiently

**Result:**
Processed data ready for next step
"""
        linter = ExplanationLinter()
        issues = linter.lint(lazy_explanation, "test_op")

        assert any("processes data" in issue.message.lower() for issue in issues)
        assert linter.has_errors()

    def test_formula_mention_without_code_block_warns(self):
        """Formula mention without code block should warn."""
        formula_without_block = """
**Purpose:**
Calculate using a formula

**Details:**
- The formula is simple: output / input

**Result:**
Calculated value
"""
        linter = ExplanationLinter()
        issues = linter.lint(formula_without_block, "test_op")

        assert any(issue.severity == "warning" for issue in issues)
        assert any("formula but no code block" in issue.message.lower() for issue in issues)

    def test_formula_with_code_block_passes(self):
        """Formula with code block should not warn."""
        formula_with_block = """
**Purpose:**
Calculate efficiency using formula

**Details:**
- Uses standard efficiency calculation

**Formula:**
```
efficiency = output / input
```

**Result:**
Efficiency percentage
"""
        linter = ExplanationLinter()
        issues = linter.lint(formula_with_block, "test_op")

        # Should not have formula warning
        assert not any("formula but no code block" in issue.message.lower() for issue in issues)

    def test_format_issues_no_issues(self):
        """format_issues() should return success message when no issues."""
        valid_explanation = """
**Purpose:**
Calculate efficiency for plant operations

**Details:**
- Uses fuel consumption and output

**Result:**
Efficiency percentage
"""
        linter = ExplanationLinter()
        linter.lint(valid_explanation, "test_op")
        output = linter.format_issues()

        assert "✅" in output
        assert "No issues" in output

    def test_format_issues_with_issues(self):
        """format_issues() should format issues nicely."""
        linter = ExplanationLinter()
        linter.lint("Short", "test_op")
        output = linter.format_issues()

        assert len(output) > 0
        # Should contain emoji symbols
        assert "❌" in output or "⚠️" in output

    def test_lint_issue_str_formatting(self):
        """LintIssue string formatting should include emoji."""
        issue = LintIssue(severity="error", message="Test error message", rule="E001")
        output = str(issue)

        assert "❌" in output
        assert "Test error message" in output
        assert "[E001]" in output

    def test_case_insensitive_section_matching(self):
        """Section matching should be case-insensitive."""
        mixed_case = """
**purpose:**
Test case insensitivity

**DETAILS:**
- Uses mixed case

**ReSuLt:**
Should still pass
"""
        linter = ExplanationLinter()
        issues = linter.lint(mixed_case, "test_op")

        # Should not fail on missing sections
        assert not any("missing required section" in issue.message for issue in issues)

    def test_multiple_lazy_phrases(self):
        """Should detect multiple lazy phrases."""
        multiple_lazy = """
**Purpose:**
TODO: This transformation processes data and does things

**Details:**
- Handles records
- TBD on the details

**Result:**
Calculates stuff
"""
        linter = ExplanationLinter()
        issues = linter.lint(multiple_lazy, "test_op")

        # Should find multiple issues
        assert len([i for i in issues if "generic phrase" in i.message.lower()]) > 1
        assert linter.has_errors()

    def test_whitespace_only_explanation_fails(self):
        """Whitespace-only explanation should fail."""
        linter = ExplanationLinter()
        issues = linter.lint("   \n\t  \n  ", "test_op")

        assert len(issues) > 0
        assert any("empty" in issue.message.lower() for issue in issues)
        assert linter.has_errors()
