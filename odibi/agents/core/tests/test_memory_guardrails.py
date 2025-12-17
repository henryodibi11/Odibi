"""Tests for Memory Query Guardrails.

Phase 5.D: Pre-Autonomy Memory Guardrails tests.
"""

import pytest

from odibi.agents.core.memory_guardrails import (
    ALLOWED_QUERY_EXAMPLES,
    DISALLOWED_QUERY_EXAMPLES,
    DisallowedQueryError,
    DisallowedQueryType,
    MemoryQuery,
    MemoryQueryIntent,
    MemoryQueryValidator,
    MissingIntentError,
)


class TestMemoryQueryIntent:
    """Tests for MemoryQueryIntent enum."""

    def test_all_intents_defined(self):
        """Verify all required intents exist."""
        required_intents = [
            "PATTERN_LOOKUP",
            "RISK_ASSESSMENT",
            "PRECEDENT_SUMMARY",
            "CONVERGENCE_SIGNAL",
            "HUMAN_REVIEW_SUPPORT",
        ]
        for intent_name in required_intents:
            assert hasattr(MemoryQueryIntent, intent_name)

    def test_intent_values(self):
        """Test intent enum values."""
        assert MemoryQueryIntent.PATTERN_LOOKUP.value == "PATTERN_LOOKUP"
        assert MemoryQueryIntent.RISK_ASSESSMENT.value == "RISK_ASSESSMENT"


class TestMemoryQueryValidator:
    """Tests for MemoryQueryValidator."""

    @pytest.fixture
    def validator(self):
        return MemoryQueryValidator()

    def test_valid_intent_enum(self, validator):
        """Test validation with intent as enum."""
        intent = validator.validate_intent(MemoryQueryIntent.PATTERN_LOOKUP)
        assert intent == MemoryQueryIntent.PATTERN_LOOKUP

    def test_valid_intent_string(self, validator):
        """Test validation with intent as string."""
        intent = validator.validate_intent("PATTERN_LOOKUP")
        assert intent == MemoryQueryIntent.PATTERN_LOOKUP

    def test_valid_intent_lowercase_string(self, validator):
        """Test validation with lowercase intent string."""
        intent = validator.validate_intent("pattern_lookup")
        assert intent == MemoryQueryIntent.PATTERN_LOOKUP

    def test_missing_intent_raises_error(self, validator):
        """Test that None intent raises MissingIntentError."""
        with pytest.raises(MissingIntentError) as exc_info:
            validator.validate_intent(None)
        assert "MUST include an explicit intent" in str(exc_info.value)

    def test_invalid_intent_raises_error(self, validator):
        """Test that invalid intent raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            validator.validate_intent("INVALID_INTENT")
        assert "Invalid intent" in str(exc_info.value)

    def test_empty_query_raises_error(self, validator):
        """Test that empty query raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            validator.validate_query_text("")
        assert "cannot be empty" in str(exc_info.value)

    def test_whitespace_query_raises_error(self, validator):
        """Test that whitespace-only query raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            validator.validate_query_text("   ")
        assert "cannot be empty" in str(exc_info.value)


class TestDisallowedQueries:
    """Tests for disallowed query detection."""

    @pytest.fixture
    def validator(self):
        return MemoryQueryValidator()

    def test_blocks_decide_next_change(self, validator):
        """Test blocking queries that decide next change."""
        queries = [
            "What should I change next?",
            "Tell me what to modify",
            "Which change to make next?",
        ]
        for query in queries:
            with pytest.raises(DisallowedQueryError) as exc_info:
                validator.validate_query_text(query)
            assert exc_info.value.query_type == DisallowedQueryType.DECIDE_NEXT_CHANGE

    def test_blocks_select_files(self, validator):
        """Test blocking queries that select files to modify."""
        queries = [
            "Which files should I modify?",
            "Select files to change",
            "What files to edit for this fix?",
        ]
        for query in queries:
            with pytest.raises(DisallowedQueryError) as exc_info:
                validator.validate_query_text(query)
            assert exc_info.value.query_type == DisallowedQueryType.SELECT_FILES

    def test_blocks_apply_changes(self, validator):
        """Test blocking queries that apply changes."""
        queries = [
            "Apply the change from yesterday",
            "Replay the fix automatically",
            "Execute the improvement",
        ]
        for query in queries:
            with pytest.raises(DisallowedQueryError) as exc_info:
                validator.validate_query_text(query)
            assert exc_info.value.query_type == DisallowedQueryType.APPLY_CHANGES

    def test_blocks_bypass_review(self, validator):
        """Test blocking queries that bypass review."""
        queries = [
            "Bypass review for this",
            "Skip the regression check",
            "Force approval of this change",
        ]
        for query in queries:
            with pytest.raises(DisallowedQueryError) as exc_info:
                validator.validate_query_text(query)
            assert exc_info.value.query_type == DisallowedQueryType.BYPASS_REVIEW

    def test_blocks_raw_agent_reasoning(self, validator):
        """Test blocking queries for raw agent data."""
        queries = [
            "Show me raw agent reasoning",
            "Get the agent scratchpad",
            "Retrieve unfiltered agent output",
        ]
        for query in queries:
            with pytest.raises(DisallowedQueryError) as exc_info:
                validator.validate_query_text(query)
            assert exc_info.value.query_type == DisallowedQueryType.RAW_AGENT_REASONING


class TestAllowedQueries:
    """Tests for allowed query patterns."""

    @pytest.fixture
    def validator(self):
        return MemoryQueryValidator()

    def test_allows_pattern_lookup(self, validator):
        """Test that pattern lookup queries are allowed."""
        queries = [
            "What patterns exist for null handling?",
            "Show me previous solutions for schema validation",
            "How was this type of error handled before?",
        ]
        for query in queries:
            validator.validate_query_text(query)

    def test_allows_risk_assessment(self, validator):
        """Test that risk assessment queries are allowed."""
        queries = [
            "Have similar changes caused regressions?",
            "What is the risk of this type of modification?",
            "Were there failures with this pattern before?",
        ]
        for query in queries:
            validator.validate_query_text(query)

    def test_allows_precedent_summary(self, validator):
        """Test that precedent summary queries are allowed."""
        queries = [
            "Summarize how this was handled previously",
            "What precedents exist for this situation?",
            "Show me similar cases from past cycles",
        ]
        for query in queries:
            validator.validate_query_text(query)

    def test_documented_allowed_examples_pass(self, validator):
        """Test that all documented allowed examples pass validation."""
        for example in ALLOWED_QUERY_EXAMPLES:
            query_text = example["query"]
            intent = example["intent"]
            query = validator.validate_and_create(query_text, intent)
            assert query.query_text == query_text

    def test_documented_disallowed_examples_blocked(self, validator):
        """Test that all documented disallowed examples are blocked."""
        for example in DISALLOWED_QUERY_EXAMPLES:
            query_text = example["query"]
            with pytest.raises(DisallowedQueryError):
                validator.validate_query_text(query_text)


class TestValidateAndCreate:
    """Tests for the validate_and_create method."""

    @pytest.fixture
    def validator(self):
        return MemoryQueryValidator()

    def test_creates_valid_query(self, validator):
        """Test creating a valid MemoryQuery."""
        query = validator.validate_and_create(
            query_text="What patterns exist for error handling?",
            intent=MemoryQueryIntent.PATTERN_LOOKUP,
            top_k=10,
        )

        assert isinstance(query, MemoryQuery)
        assert query.query_text == "What patterns exist for error handling?"
        assert query.intent == MemoryQueryIntent.PATTERN_LOOKUP
        assert query.top_k == 10

    def test_strips_whitespace(self, validator):
        """Test that query text is stripped."""
        query = validator.validate_and_create(
            query_text="  Test query  ",
            intent=MemoryQueryIntent.PATTERN_LOOKUP,
        )
        assert query.query_text == "Test query"

    def test_rejects_invalid_top_k(self, validator):
        """Test that invalid top_k values are rejected."""
        with pytest.raises(ValueError) as exc_info:
            validator.validate_and_create(
                query_text="Test query",
                intent=MemoryQueryIntent.PATTERN_LOOKUP,
                top_k=0,
            )
        assert "top_k must be between" in str(exc_info.value)

        with pytest.raises(ValueError):
            validator.validate_and_create(
                query_text="Test query",
                intent=MemoryQueryIntent.PATTERN_LOOKUP,
                top_k=101,
            )

    def test_accepts_filters(self, validator):
        """Test that filters are accepted."""
        query = validator.validate_and_create(
            query_text="Test query",
            intent=MemoryQueryIntent.PATTERN_LOOKUP,
            filters={"source": "cycle_report"},
        )
        assert query.filters == {"source": "cycle_report"}


class TestMemoryQuery:
    """Tests for MemoryQuery dataclass."""

    def test_to_dict(self):
        """Test dictionary conversion."""
        query = MemoryQuery(
            query_text="Test query",
            intent=MemoryQueryIntent.RISK_ASSESSMENT,
            filters={"key": "value"},
            top_k=5,
        )
        data = query.to_dict()

        assert data["query_text"] == "Test query"
        assert data["intent"] == "RISK_ASSESSMENT"
        assert data["filters"] == {"key": "value"}
        assert data["top_k"] == 5
