"""Memory Query Guardrails for Pre-Autonomy Safety.

Phase 5.D: Implements mechanical constraints on how agents can query
indexed memory, preventing action-directing queries.

CRITICAL CONSTRAINTS:
- Queries MUST include an explicit, allowed intent
- Action-directing queries are MECHANICALLY blocked
- No raw agent reasoning or scratchpads can be retrieved
- This is enforcement at the API boundary, not prompt wording
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class MemoryQueryIntent(Enum):
    """Allowed intents for querying indexed memory.

    These are the ONLY valid intents. Queries without a valid intent
    will be rejected at the API boundary.
    """

    PATTERN_LOOKUP = "PATTERN_LOOKUP"
    """Look up previously observed patterns or solutions."""

    RISK_ASSESSMENT = "RISK_ASSESSMENT"
    """Assess risk based on past regressions or failures."""

    PRECEDENT_SUMMARY = "PRECEDENT_SUMMARY"
    """Summarize precedents for similar situations."""

    CONVERGENCE_SIGNAL = "CONVERGENCE_SIGNAL"
    """Check for convergence signals from past cycles."""

    HUMAN_REVIEW_SUPPORT = "HUMAN_REVIEW_SUPPORT"
    """Provide context to support human review decisions."""


class DisallowedQueryType(Enum):
    """Types of queries that are explicitly disallowed.

    These patterns indicate action-directing queries that bypass
    human oversight.
    """

    DECIDE_NEXT_CHANGE = "DECIDE_NEXT_CHANGE"
    """Queries attempting to decide what change to make next."""

    SELECT_FILES = "SELECT_FILES"
    """Queries attempting to select files to modify."""

    APPLY_CHANGES = "APPLY_CHANGES"
    """Queries attempting to apply or replay changes."""

    BYPASS_REVIEW = "BYPASS_REVIEW"
    """Queries attempting to bypass review or regression checks."""

    RAW_AGENT_REASONING = "RAW_AGENT_REASONING"
    """Queries attempting to retrieve raw agent reasoning."""


class MemoryQueryError(Exception):
    """Base error for memory query violations."""

    pass


class MissingIntentError(MemoryQueryError):
    """Raised when a query is made without an explicit intent."""

    def __init__(self):
        super().__init__(
            "Memory queries MUST include an explicit intent. "
            f"Valid intents: {[i.value for i in MemoryQueryIntent]}"
        )


class DisallowedQueryError(MemoryQueryError):
    """Raised when an action-directing query is detected."""

    def __init__(self, query_type: DisallowedQueryType, reason: str):
        self.query_type = query_type
        self.reason = reason
        super().__init__(
            f"BLOCKED: {query_type.value} - {reason}. Action-directing queries are not permitted."
        )


@dataclass
class MemoryQuery:
    """A validated memory query with explicit intent.

    All queries to indexed memory MUST go through this structure
    to ensure intent is declared and validated.
    """

    query_text: str
    intent: MemoryQueryIntent
    filters: dict[str, Any] | None = None
    top_k: int = 5

    def to_dict(self) -> dict[str, Any]:
        return {
            "query_text": self.query_text,
            "intent": self.intent.value,
            "filters": self.filters,
            "top_k": self.top_k,
        }


@dataclass
class MemoryQueryResult:
    """Result of a validated memory query."""

    query: MemoryQuery
    results: list[dict[str, Any]]
    result_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "query": self.query.to_dict(),
            "results": self.results,
            "result_count": self.result_count,
        }


class MemoryQueryValidator:
    """Validates memory queries against guardrails.

    This class enforces the API boundary rules:
    1. All queries MUST have an explicit intent
    2. Action-directing queries are BLOCKED
    3. Raw agent reasoning cannot be retrieved

    USAGE:
        validator = MemoryQueryValidator()
        query = validator.validate_and_create(
            query_text="What patterns exist for null handling?",
            intent=MemoryQueryIntent.PATTERN_LOOKUP,
        )
    """

    DISALLOWED_PATTERNS: list[tuple[DisallowedQueryType, list[str]]] = [
        (
            DisallowedQueryType.DECIDE_NEXT_CHANGE,
            [
                r"what\s+should\s+i\s+(change|modify|edit|fix)",
                r"what\s+(should|to)\s+(change|modify|edit|fix)\s+next",
                r"decide\s+what\s+(to\s+)?(change|modify|edit)",
                r"tell\s+me\s+what\s+to\s+(do|change|modify)",
                r"which\s+(change|modification)\s+to\s+make",
                r"next\s+(action|step|change)\s+to\s+take",
            ],
        ),
        (
            DisallowedQueryType.SELECT_FILES,
            [
                r"(which|what)\s+files?\s+should\s+i\s+(modify|change|edit)",
                r"(which|what)\s+files?\s+(should|to)\s+(modify|change|edit)",
                r"select\s+files?\s+(to|for)\s+(modify|change|edit)",
                r"pick\s+files?\s+(to|for)",
                r"choose\s+files?\s+(to|for)\s+(modify|change)",
                r"files?\s+to\s+(change|edit|modify)\s+for",
            ],
        ),
        (
            DisallowedQueryType.APPLY_CHANGES,
            [
                r"apply\s+(the\s+)?(change|fix|improvement)",
                r"replay\s+(the\s+)?(change|fix|improvement)",
                r"execute\s+(the\s+)?(change|fix|improvement)",
                r"implement\s+automatically",
                r"auto-?apply",
                r"automatically\b",
            ],
        ),
        (
            DisallowedQueryType.BYPASS_REVIEW,
            [
                r"bypass\s+(the\s+)?review",
                r"skip\s+(the\s+)?(review|regression)",
                r"without\s+(review|approval)",
                r"auto-?approve",
                r"force\s+approval",
            ],
        ),
        (
            DisallowedQueryType.RAW_AGENT_REASONING,
            [
                r"raw\s+agent\s+(reasoning|thoughts|messages)",
                r"agent\s+scratchpad",
                r"internal\s+agent\s+(state|thoughts)",
                r"agent\s+conversation\s+history",
                r"unfiltered\s+agent\s+(output|messages)",
            ],
        ),
    ]

    def __init__(self):
        self._compiled_patterns: dict[DisallowedQueryType, list[re.Pattern]] = {}
        for query_type, patterns in self.DISALLOWED_PATTERNS:
            self._compiled_patterns[query_type] = [re.compile(p, re.IGNORECASE) for p in patterns]

    def _check_disallowed_patterns(self, query_text: str) -> DisallowedQueryType | None:
        """Check if query matches any disallowed patterns.

        Returns:
            DisallowedQueryType if matched, None otherwise.
        """
        for query_type, patterns in self._compiled_patterns.items():
            for pattern in patterns:
                if pattern.search(query_text):
                    return query_type
        return None

    def validate_intent(self, intent: MemoryQueryIntent | str | None) -> MemoryQueryIntent:
        """Validate and convert intent to enum.

        Args:
            intent: Intent as enum or string.

        Returns:
            Validated MemoryQueryIntent.

        Raises:
            MissingIntentError: If intent is None.
            ValueError: If intent is invalid.
        """
        if intent is None:
            raise MissingIntentError()

        if isinstance(intent, MemoryQueryIntent):
            return intent

        if isinstance(intent, str):
            try:
                return MemoryQueryIntent(intent.upper())
            except ValueError:
                raise ValueError(
                    f"Invalid intent: {intent}. "
                    f"Valid intents: {[i.value for i in MemoryQueryIntent]}"
                )

        raise ValueError(f"Intent must be MemoryQueryIntent or str, got {type(intent)}")

    def validate_query_text(self, query_text: str) -> None:
        """Validate query text against disallowed patterns.

        Args:
            query_text: The query text to validate.

        Raises:
            DisallowedQueryError: If query matches a disallowed pattern.
        """
        if not query_text or not query_text.strip():
            raise ValueError("Query text cannot be empty")

        disallowed_type = self._check_disallowed_patterns(query_text)
        if disallowed_type:
            raise DisallowedQueryError(
                disallowed_type,
                f"Query contains disallowed pattern for {disallowed_type.value}",
            )

    def validate_and_create(
        self,
        query_text: str,
        intent: MemoryQueryIntent | str | None,
        filters: dict[str, Any] | None = None,
        top_k: int = 5,
    ) -> MemoryQuery:
        """Validate inputs and create a MemoryQuery.

        This is the ONLY way to create a valid MemoryQuery.
        All validation happens here at the API boundary.

        Args:
            query_text: The query text.
            intent: The explicit intent for this query.
            filters: Optional metadata filters.
            top_k: Number of results to return.

        Returns:
            Validated MemoryQuery.

        Raises:
            MissingIntentError: If intent is not provided.
            DisallowedQueryError: If query is action-directing.
            ValueError: If inputs are invalid.
        """
        validated_intent = self.validate_intent(intent)
        self.validate_query_text(query_text)

        if top_k < 1 or top_k > 100:
            raise ValueError("top_k must be between 1 and 100")

        return MemoryQuery(
            query_text=query_text.strip(),
            intent=validated_intent,
            filters=filters,
            top_k=top_k,
        )


ALLOWED_QUERY_EXAMPLES: list[dict[str, str]] = [
    {
        "intent": "PATTERN_LOOKUP",
        "query": "What patterns exist for handling null values in transformers?",
        "description": "Looking up previously observed patterns",
    },
    {
        "intent": "RISK_ASSESSMENT",
        "query": "Have similar schema changes caused regressions before?",
        "description": "Assessing risk based on past failures",
    },
    {
        "intent": "PRECEDENT_SUMMARY",
        "query": "Summarize how YAML validation errors were handled previously",
        "description": "Summarizing precedents for similar issues",
    },
    {
        "intent": "CONVERGENCE_SIGNAL",
        "query": "Are there signs of improvement convergence in recent cycles?",
        "description": "Checking for convergence signals",
    },
    {
        "intent": "HUMAN_REVIEW_SUPPORT",
        "query": "What context would help a reviewer evaluate this proposal?",
        "description": "Supporting human review decisions",
    },
]

DISALLOWED_QUERY_EXAMPLES: list[dict[str, str]] = [
    {
        "query": "What should I change next?",
        "blocked_reason": "DECIDE_NEXT_CHANGE - Agents cannot autonomously decide next actions",
    },
    {
        "query": "Which files should I modify to fix this?",
        "blocked_reason": "SELECT_FILES - Agents cannot autonomously select files to modify",
    },
    {
        "query": "Apply the change from the previous cycle",
        "blocked_reason": "APPLY_CHANGES - Agents cannot apply or replay changes",
    },
    {
        "query": "Bypass review for this minor fix",
        "blocked_reason": "BYPASS_REVIEW - Review cannot be bypassed",
    },
    {
        "query": "Show me the raw agent reasoning from the last cycle",
        "blocked_reason": "RAW_AGENT_REASONING - Raw agent output is not indexed",
    },
]
