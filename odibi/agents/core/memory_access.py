"""Memory Access Contract for Odibi Agents.

Phase 5.F: Documents and enforces which agents may query indexed memory.

This module defines the explicit contract for memory access:
- Which agents have memory access
- What query intents they may use
- What they are explicitly forbidden from doing

CRITICAL: This is a documentation and enforcement module.
It does NOT grant new capabilities or autonomy.
"""

from enum import Enum
from typing import Any

from odibi.agents.core.agent_base import AgentRole
from odibi.agents.core.memory_guardrails import MemoryQueryIntent


class MemoryAccessLevel(str, Enum):
    """Memory access levels for agents."""

    NONE = "NONE"
    """Agent has no memory access."""

    READ_ONLY_ADVISORY = "READ_ONLY_ADVISORY"
    """Agent can read memory for advisory context only.

    Cannot use memory to:
    - Make decisions
    - Approve/reject proposals
    - Select files to modify
    - Bypass review
    """


# Explicit memory access contract for each agent role
MEMORY_ACCESS_CONTRACT: dict[AgentRole, dict[str, Any]] = {
    AgentRole.OBSERVER: {
        "access_level": MemoryAccessLevel.NONE,
        "allowed_intents": [],
        "rationale": "Observer analyzes current execution only. Memory would bias observations.",
        "forbidden": [
            "Query indexed memory",
            "Access previous cycle data",
            "Reference past improvements",
        ],
    },
    AgentRole.IMPROVEMENT: {
        "access_level": MemoryAccessLevel.NONE,
        "allowed_intents": [],
        "rationale": "Improvement agent proposes based on current observations. "
        "Memory-influenced proposals would bypass human review.",
        "forbidden": [
            "Query indexed memory",
            "Copy previous improvements",
            "Auto-generate proposals from memory",
        ],
    },
    AgentRole.REVIEWER: {
        "access_level": MemoryAccessLevel.READ_ONLY_ADVISORY,
        "allowed_intents": [
            MemoryQueryIntent.PATTERN_LOOKUP,
            MemoryQueryIntent.RISK_ASSESSMENT,
            MemoryQueryIntent.PRECEDENT_SUMMARY,
            MemoryQueryIntent.HUMAN_REVIEW_SUPPORT,
        ],
        "rationale": "Reviewer may see advisory context from memory, "
        "but MUST NOT use it for approval decisions.",
        "forbidden": [
            "Use memory to approve/reject proposals",
            "Auto-approve based on precedent",
            "Bypass review based on similarity",
        ],
        "explicit_constraint": (
            "Advisory context is for HUMAN REFERENCE ONLY. "
            "ReviewerAgent decision logic MUST ignore advisory_context."
        ),
    },
    AgentRole.ENVIRONMENT: {
        "access_level": MemoryAccessLevel.NONE,
        "allowed_intents": [],
        "rationale": "Environment agent validates current setup only.",
        "forbidden": ["Query indexed memory"],
    },
    AgentRole.PROJECT: {
        "access_level": MemoryAccessLevel.NONE,
        "allowed_intents": [],
        "rationale": "Project selection is user-driven, not memory-driven.",
        "forbidden": [
            "Select projects based on memory",
            "Prioritize based on past cycles",
        ],
    },
    AgentRole.USER: {
        "access_level": MemoryAccessLevel.NONE,
        "allowed_intents": [],
        "rationale": "User agent simulates user behavior, not memory recall.",
        "forbidden": ["Query indexed memory"],
    },
    AgentRole.REGRESSION_GUARD: {
        "access_level": MemoryAccessLevel.NONE,
        "allowed_intents": [],
        "rationale": "Regression guard runs tests deterministically. "
        "Memory would introduce non-determinism.",
        "forbidden": [
            "Skip tests based on memory",
            "Modify thresholds based on history",
        ],
    },
    AgentRole.CURRICULUM: {
        "access_level": MemoryAccessLevel.NONE,
        "allowed_intents": [],
        "rationale": "Curriculum agent writes to memory but does not query it.",
        "forbidden": ["Query memory for write decisions"],
    },
    AgentRole.CONVERGENCE: {
        "access_level": MemoryAccessLevel.READ_ONLY_ADVISORY,
        "allowed_intents": [
            MemoryQueryIntent.CONVERGENCE_SIGNAL,
            MemoryQueryIntent.PRECEDENT_SUMMARY,
        ],
        "rationale": "Convergence agent may check for convergence signals "
        "but cannot influence execution.",
        "forbidden": [
            "Decide execution based on memory",
            "Skip steps based on history",
        ],
    },
    AgentRole.CODE_ANALYST: {
        "access_level": MemoryAccessLevel.NONE,
        "allowed_intents": [],
        "rationale": "Code analyst analyzes current code only.",
        "forbidden": ["Query indexed memory"],
    },
    AgentRole.REFACTOR_ENGINEER: {
        "access_level": MemoryAccessLevel.NONE,
        "allowed_intents": [],
        "rationale": "Refactor engineer works on current code only.",
        "forbidden": ["Query indexed memory"],
    },
    AgentRole.TEST_ARCHITECT: {
        "access_level": MemoryAccessLevel.NONE,
        "allowed_intents": [],
        "rationale": "Test architect designs tests for current code.",
        "forbidden": ["Query indexed memory"],
    },
    AgentRole.DOCUMENTATION: {
        "access_level": MemoryAccessLevel.NONE,
        "allowed_intents": [],
        "rationale": "Documentation agent works on current state.",
        "forbidden": ["Query indexed memory"],
    },
    AgentRole.ORCHESTRATOR: {
        "access_level": MemoryAccessLevel.NONE,
        "allowed_intents": [],
        "rationale": "Orchestrator coordinates agents but does not query memory.",
        "forbidden": ["Query indexed memory"],
    },
    AgentRole.PRODUCT_OBSERVER: {
        "access_level": MemoryAccessLevel.NONE,
        "allowed_intents": [],
        "rationale": "Product observer analyzes current behavior only.",
        "forbidden": ["Query indexed memory"],
    },
    AgentRole.UX_FRICTION: {
        "access_level": MemoryAccessLevel.NONE,
        "allowed_intents": [],
        "rationale": "UX friction agent identifies current friction points.",
        "forbidden": ["Query indexed memory"],
    },
}


# Components that can query memory (non-agent components)
COMPONENT_MEMORY_ACCESS: dict[str, dict[str, Any]] = {
    "ScorecardGenerator": {
        "access_level": MemoryAccessLevel.READ_ONLY_ADVISORY,
        "allowed_intents": [
            MemoryQueryIntent.PATTERN_LOOKUP,
            MemoryQueryIntent.RISK_ASSESSMENT,
            MemoryQueryIntent.PRECEDENT_SUMMARY,
            MemoryQueryIntent.CONVERGENCE_SIGNAL,
        ],
        "rationale": "Generates advisory scorecard for human review.",
        "forbidden": [
            "Approve or reject proposals",
            "Influence execution flow",
            "Rank proposals",
        ],
        "output_constraint": "Output is advisory only, attached to reports.",
    },
    "CycleIndexManager": {
        "access_level": MemoryAccessLevel.NONE,
        "allowed_intents": [],
        "rationale": "Index manager writes to memory, queries only for stats.",
        "forbidden": ["Decision-making queries"],
    },
}


def get_memory_access(role: AgentRole) -> dict[str, Any]:
    """Get the memory access contract for an agent role.

    Args:
        role: The agent role to check.

    Returns:
        Dictionary with access_level, allowed_intents, and constraints.
    """
    return MEMORY_ACCESS_CONTRACT.get(
        role,
        {
            "access_level": MemoryAccessLevel.NONE,
            "allowed_intents": [],
            "rationale": "Unknown role - no memory access by default.",
            "forbidden": ["All memory queries"],
        },
    )


def can_query_memory(role: AgentRole) -> bool:
    """Check if an agent role can query indexed memory.

    Args:
        role: The agent role to check.

    Returns:
        True if the role has any memory access.
    """
    contract = get_memory_access(role)
    return contract["access_level"] != MemoryAccessLevel.NONE


def get_allowed_intents(role: AgentRole) -> list[MemoryQueryIntent]:
    """Get the allowed query intents for an agent role.

    Args:
        role: The agent role to check.

    Returns:
        List of allowed MemoryQueryIntent values.
    """
    contract = get_memory_access(role)
    return contract.get("allowed_intents", [])


def validate_query_for_role(
    role: AgentRole,
    intent: MemoryQueryIntent,
) -> tuple[bool, str]:
    """Validate that a query intent is allowed for a role.

    Args:
        role: The agent role making the query.
        intent: The query intent being used.

    Returns:
        Tuple of (is_allowed, reason).
    """
    if not can_query_memory(role):
        return False, f"Agent role {role.value} has no memory access"

    allowed = get_allowed_intents(role)
    if intent not in allowed:
        return False, (
            f"Intent {intent.value} not allowed for {role.value}. "
            f"Allowed: {[i.value for i in allowed]}"
        )

    return True, "Query allowed"


def generate_contract_documentation() -> str:
    """Generate human-readable documentation of the memory access contract.

    Returns:
        Markdown documentation string.
    """
    lines = [
        "# Memory Query Contract",
        "",
        "This document defines which agents may query indexed memory and why.",
        "",
        "## Access Levels",
        "",
        "| Level | Description |",
        "|-------|-------------|",
        f"| {MemoryAccessLevel.NONE.value} | No memory access |",
        f"| {MemoryAccessLevel.READ_ONLY_ADVISORY.value} | Read-only, advisory context only |",
        "",
        "## Agent Access",
        "",
        "| Agent | Access | Allowed Intents |",
        "|-------|--------|-----------------|",
    ]

    for role, contract in MEMORY_ACCESS_CONTRACT.items():
        access = contract["access_level"].value
        intents = ", ".join(i.value for i in contract.get("allowed_intents", []))
        if not intents:
            intents = "None"
        lines.append(f"| {role.value} | {access} | {intents} |")

    lines.extend(
        [
            "",
            "## Component Access",
            "",
            "| Component | Access | Allowed Intents |",
            "|-----------|--------|-----------------|",
        ]
    )

    for name, contract in COMPONENT_MEMORY_ACCESS.items():
        access = contract["access_level"].value
        intents = ", ".join(i.value for i in contract.get("allowed_intents", []))
        if not intents:
            intents = "None"
        lines.append(f"| {name} | {access} | {intents} |")

    lines.extend(
        [
            "",
            "## Critical Constraints",
            "",
            "1. **All queries MUST include an explicit MemoryQueryIntent**",
            "2. **Advisory context does NOT influence approval decisions**",
            "3. **Memory cannot be used to select files, rank proposals, or bypass review**",
            "4. **Observer and Improvement agents have NO memory access**",
        ]
    )

    return "\n".join(lines)
