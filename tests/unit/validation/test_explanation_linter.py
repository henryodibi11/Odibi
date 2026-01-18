"""Tests for Explanation Quality Linter."""

import pytest

from odibi.validation.explanation_linter import (
    LintIssue,
    ExplanationLinter,
)

# =============================================================================
# LintIssue Tests
# =============================================================================


class TestLintIssue:
    """Tests for LintIssue dataclass."""

    def test_lint_issue_creation(self):
        """Test creating a LintIssue."""
        issue = LintIssue(
            severity="error",
            message="Test error message",
            rule="E001",
        )
        assert issue.severity == "error"
        assert issue.message == "Test error message"
        assert issue.rule == "E001"

    def test_lint_issue_str_error(self):
        """Test __str__ for error severity."""
        issue = LintIssue(
            severity="error",
            message="Missing section",
            rule="E003",
        )
        result = str(issue)
        assert "❌" in result
        assert "Missing section" in result
        assert "[E003]" in result

    def test_lint_issue_str_warning(self):
        """Test __str__ for warning severity."""
        issue = LintIssue(
            severity="warning",
            message="Formula without code block",
            rule="W001",
        )
        result = str(issue)
        assert "⚠️" in result
        assert "Formula without code block" in result
        assert "[W001]" in result

    def test_lint_issue_str_info(self):
        """Test __str__ for info severity."""
        issue = LintIssue(
            severity="info",
            message="Informational message",
            rule="I001",
        )
        result = str(issue)
        assert "ℹ️" in result
        assert "Informational message" in result
        assert "[I001]" in result


# =============================================================================
# ExplanationLinter Tests
# =============================================================================


class TestExplanationLinter:
    """Tests for ExplanationLinter class."""

    @pytest.fixture
    def linter(self):
        """Create a fresh linter instance."""
        return ExplanationLinter()

    def test_linter_initialization(self, linter):
        """Test linter initializes with empty issues."""
        assert linter.issues == []

    def test_lint_empty_explanation(self, linter):
        """Test linting empty explanation."""
        issues = linter.lint("", "my_operation")
        assert len(issues) == 1
        assert issues[0].severity == "error"
        assert issues[0].rule == "E001"
        assert "empty" in issues[0].message.lower()
        assert "my_operation" in issues[0].message

    def test_lint_none_explanation(self, linter):
        """Test linting None explanation."""
        issues = linter.lint(None, "my_operation")
        assert len(issues) == 1
        assert issues[0].rule == "E001"

    def test_lint_whitespace_only(self, linter):
        """Test linting whitespace-only explanation."""
        issues = linter.lint("   \n\t  ", "my_operation")
        assert len(issues) == 1
        assert issues[0].rule == "E001"

    def test_lint_too_short(self, linter):
        """Test linting explanation that's too short."""
        issues = linter.lint("Short text.", "my_operation")
        assert any(i.rule == "E002" for i in issues)
        short_issue = next(i for i in issues if i.rule == "E002")
        assert "too short" in short_issue.message.lower()
        assert str(linter.MIN_LENGTH) in short_issue.message

    def test_lint_minimum_length_boundary(self, linter):
        """Test linting at exactly minimum length."""
        text = "a" * linter.MIN_LENGTH
        issues = linter.lint(text, "my_operation")
        assert not any(i.rule == "E002" for i in issues)

    def test_lint_missing_purpose_section(self, linter):
        """Test linting missing Purpose section."""
        text = """
        **Details:** Some details here
        **Result:** The result is good
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        purpose_issues = [i for i in issues if i.rule == "E003" and "Purpose" in i.message]
        assert len(purpose_issues) == 1

    def test_lint_missing_details_section(self, linter):
        """Test linting missing Details section."""
        text = """
        **Purpose:** Some purpose here
        **Result:** The result is good
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        details_issues = [i for i in issues if i.rule == "E003" and "Details" in i.message]
        assert len(details_issues) == 1

    def test_lint_missing_result_section(self, linter):
        """Test linting missing Result section."""
        text = """
        **Purpose:** Some purpose here
        **Details:** Some details here
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        result_issues = [i for i in issues if i.rule == "E003" and "Result" in i.message]
        assert len(result_issues) == 1

    def test_lint_all_sections_present(self, linter):
        """Test linting with all required sections present."""
        text = """
        **Purpose:** This operation transforms data
        **Details:** It uses specific algorithms
        **Result:** Outputs a transformed dataset
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        assert not any(i.rule == "E003" for i in issues)

    def test_lint_sections_with_colon(self, linter):
        """Test sections work with or without colon."""
        text = """
        **Purpose** This operation transforms data
        **Details** It uses specific algorithms
        **Result** Outputs a transformed dataset
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        assert not any(i.rule == "E003" for i in issues)

    def test_lint_sections_case_insensitive(self, linter):
        """Test section matching is case-insensitive."""
        text = """
        **PURPOSE:** This operation transforms data
        **DETAILS:** It uses specific algorithms
        **RESULT:** Outputs a transformed dataset
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        assert not any(i.rule == "E003" for i in issues)

    def test_lint_lazy_phrase_calculates_stuff(self, linter):
        """Test detecting 'calculates stuff' lazy phrase."""
        text = """
        **Purpose:** This calculates stuff
        **Details:** Some details
        **Result:** The result
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        lazy_issues = [i for i in issues if i.rule == "E004"]
        assert len(lazy_issues) >= 1
        assert any("calculates stuff" in i.message.lower() for i in lazy_issues)

    def test_lint_lazy_phrase_does_things(self, linter):
        """Test detecting 'does things' lazy phrase."""
        text = """
        **Purpose:** This does things
        **Details:** Some details
        **Result:** The result
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        assert any(i.rule == "E004" and "does things" in i.message.lower() for i in issues)

    def test_lint_lazy_phrase_processes_data(self, linter):
        """Test detecting 'processes data' lazy phrase."""
        text = """
        **Purpose:** This processes data
        **Details:** Some details
        **Result:** The result
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        assert any(i.rule == "E004" and "processes data" in i.message.lower() for i in issues)

    def test_lint_lazy_phrase_handles_records(self, linter):
        """Test detecting 'handles records' lazy phrase."""
        text = """
        **Purpose:** This handles records
        **Details:** Some details
        **Result:** The result
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        assert any(i.rule == "E004" and "handles records" in i.message.lower() for i in issues)

    def test_lint_lazy_phrase_todo(self, linter):
        """Test detecting 'TODO' lazy phrase."""
        text = """
        **Purpose:** TODO fill this in
        **Details:** Some details
        **Result:** The result
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        assert any(i.rule == "E004" and "todo" in i.message.lower() for i in issues)

    def test_lint_lazy_phrase_placeholder(self, linter):
        """Test detecting '[placeholder]' lazy phrase."""
        text = """
        **Purpose:** [placeholder]
        **Details:** Some details
        **Result:** The result
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        assert any(i.rule == "E004" and "[placeholder]" in i.message.lower() for i in issues)

    def test_lint_lazy_phrase_tbd(self, linter):
        """Test detecting 'TBD' lazy phrase."""
        text = """
        **Purpose:** TBD
        **Details:** Some details
        **Result:** The result
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        assert any(i.rule == "E004" and "tbd" in i.message.lower() for i in issues)

    def test_lint_lazy_phrase_to_be_determined(self, linter):
        """Test detecting 'to be determined' lazy phrase."""
        text = """
        **Purpose:** The approach is to be determined
        **Details:** Some details
        **Result:** The result
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        assert any(i.rule == "E004" and "to be determined" in i.message.lower() for i in issues)

    def test_lint_lazy_phrase_case_insensitive(self, linter):
        """Test lazy phrase detection is case-insensitive."""
        text = """
        **Purpose:** This DOES THINGS here
        **Details:** Some details
        **Result:** The result
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        assert any(i.rule == "E004" for i in issues)

    def test_lint_formula_without_code_block(self, linter):
        """Test warning when formula mentioned without code block."""
        text = """
        **Purpose:** Calculate using formula
        **Details:** Some details
        **Result:** The result
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        assert any(i.rule == "W001" and i.severity == "warning" for i in issues)

    def test_lint_formula_with_code_block(self, linter):
        """Test no warning when formula has code block."""
        text = """
        **Purpose:** Calculate using formula
        **Details:** Here's the formula:
        ```
        result = a + b
        ```
        **Result:** The result is calculated
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        assert not any(i.rule == "W001" for i in issues)

    def test_lint_no_formula_no_warning(self, linter):
        """Test no formula warning when formula not mentioned."""
        text = """
        **Purpose:** This transforms data
        **Details:** Some details here
        **Result:** Outputs a transformed dataset
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        assert not any(i.rule == "W001" for i in issues)

    def test_lint_valid_explanation(self, linter):
        """Test linting a fully valid explanation."""
        text = """
        **Purpose:** This operation transforms raw customer data into a normalized format.
        **Details:** The transformation applies the following steps:
        1. Remove duplicates based on customer_id
        2. Standardize phone numbers to E.164 format
        3. Validate email addresses using regex pattern
        **Result:** A clean DataFrame with unique customers and validated contact info.
        """
        issues = linter.lint(text, "my_operation")
        assert len([i for i in issues if i.severity == "error"]) == 0

    def test_has_errors_true(self, linter):
        """Test has_errors returns True when errors exist."""
        linter.lint("", "my_operation")
        assert linter.has_errors() is True

    def test_has_errors_false_no_issues(self, linter):
        """Test has_errors returns False when no issues."""
        text = """
        **Purpose:** This operation transforms raw customer data into a normalized format.
        **Details:** The transformation applies the following steps.
        **Result:** A clean DataFrame with unique customers.
        """
        linter.lint(text, "my_operation")
        assert linter.has_errors() is False

    def test_has_errors_false_only_warnings(self, linter):
        """Test has_errors returns False when only warnings."""
        text = """
        **Purpose:** This formula calculates revenue correctly.
        **Details:** The formula uses multiplication.
        **Result:** Returns the calculated revenue.
        """
        linter.lint(text, "my_operation")
        assert linter.has_errors() is False

    def test_format_issues_no_issues(self, linter):
        """Test format_issues with no issues."""
        text = """
        **Purpose:** This operation transforms raw customer data into a normalized format.
        **Details:** The transformation applies the following steps.
        **Result:** A clean DataFrame with unique customers.
        """
        linter.lint(text, "my_operation")
        result = linter.format_issues()
        assert "No issues found" in result
        assert "✅" in result

    def test_format_issues_with_issues(self, linter):
        """Test format_issues with issues present."""
        linter.lint("", "my_operation")
        result = linter.format_issues()
        assert "❌" in result
        assert "empty" in result.lower()
        assert "[E001]" in result

    def test_format_issues_multiple(self, linter):
        """Test format_issues with multiple issues."""
        text = "short"  # Too short, missing sections
        linter.lint(text, "my_operation")
        result = linter.format_issues()
        lines = result.strip().split("\n")
        assert len(lines) > 1

    def test_lint_resets_issues(self, linter):
        """Test that lint() resets issues from previous call."""
        linter.lint("", "first")
        assert len(linter.issues) == 1

        linter.lint("", "second")
        assert len(linter.issues) == 1
        assert "second" in linter.issues[0].message

    def test_lint_default_operation_name(self, linter):
        """Test lint with default operation name."""
        issues = linter.lint("")
        assert "unknown" in issues[0].message

    def test_required_sections_constant(self, linter):
        """Test REQUIRED_SECTIONS class constant."""
        assert "Purpose" in linter.REQUIRED_SECTIONS
        assert "Details" in linter.REQUIRED_SECTIONS
        assert "Result" in linter.REQUIRED_SECTIONS
        assert len(linter.REQUIRED_SECTIONS) == 3

    def test_lazy_phrases_constant(self, linter):
        """Test LAZY_PHRASES class constant."""
        assert "calculates stuff" in linter.LAZY_PHRASES
        assert "does things" in linter.LAZY_PHRASES
        assert "processes data" in linter.LAZY_PHRASES
        assert "handles records" in linter.LAZY_PHRASES
        assert "TODO" in linter.LAZY_PHRASES
        assert "[placeholder]" in linter.LAZY_PHRASES
        assert "TBD" in linter.LAZY_PHRASES
        assert "to be determined" in linter.LAZY_PHRASES

    def test_min_length_constant(self, linter):
        """Test MIN_LENGTH class constant."""
        assert linter.MIN_LENGTH == 50

    def test_multiple_lazy_phrases(self, linter):
        """Test detecting multiple lazy phrases in one explanation."""
        text = """
        **Purpose:** This does things and calculates stuff
        **Details:** TODO fill in later
        **Result:** TBD
        """ + "x" * 50
        issues = linter.lint(text, "my_operation")
        lazy_issues = [i for i in issues if i.rule == "E004"]
        assert len(lazy_issues) >= 4

    def test_multiple_missing_sections(self, linter):
        """Test detecting all missing sections."""
        text = "Just some random text without any sections" + "x" * 50
        issues = linter.lint(text, "my_operation")
        section_issues = [i for i in issues if i.rule == "E003"]
        assert len(section_issues) == 3
