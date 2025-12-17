"""Tests for Memory Access Contract.

Phase 5.F: Memory query contract enforcement.
"""

from odibi.agents.core.agent_base import AgentRole
from odibi.agents.core.memory_access import (
    MEMORY_ACCESS_CONTRACT,
    COMPONENT_MEMORY_ACCESS,
    MemoryAccessLevel,
    can_query_memory,
    generate_contract_documentation,
    get_allowed_intents,
    get_memory_access,
    validate_query_for_role,
)
from odibi.agents.core.memory_guardrails import MemoryQueryIntent


class TestMemoryAccessLevel:
    """Tests for MemoryAccessLevel enum."""

    def test_all_levels_defined(self):
        """Verify all required levels exist."""
        assert hasattr(MemoryAccessLevel, "NONE")
        assert hasattr(MemoryAccessLevel, "READ_ONLY_ADVISORY")

    def test_level_values(self):
        """Test level enum values."""
        assert MemoryAccessLevel.NONE.value == "NONE"
        assert MemoryAccessLevel.READ_ONLY_ADVISORY.value == "READ_ONLY_ADVISORY"


class TestAgentMemoryAccess:
    """Tests for agent memory access contract."""

    def test_observer_has_no_memory_access(self):
        """Test that Observer agent has no memory access."""
        contract = get_memory_access(AgentRole.OBSERVER)

        assert contract["access_level"] == MemoryAccessLevel.NONE
        assert contract["allowed_intents"] == []
        assert not can_query_memory(AgentRole.OBSERVER)

    def test_improvement_has_no_memory_access(self):
        """Test that Improvement agent has no memory access."""
        contract = get_memory_access(AgentRole.IMPROVEMENT)

        assert contract["access_level"] == MemoryAccessLevel.NONE
        assert contract["allowed_intents"] == []
        assert not can_query_memory(AgentRole.IMPROVEMENT)

    def test_reviewer_has_read_only_advisory(self):
        """Test that Reviewer has read-only advisory access."""
        contract = get_memory_access(AgentRole.REVIEWER)

        assert contract["access_level"] == MemoryAccessLevel.READ_ONLY_ADVISORY
        assert len(contract["allowed_intents"]) > 0
        assert can_query_memory(AgentRole.REVIEWER)

    def test_reviewer_allowed_intents(self):
        """Test that Reviewer has specific allowed intents."""
        intents = get_allowed_intents(AgentRole.REVIEWER)

        assert MemoryQueryIntent.PATTERN_LOOKUP in intents
        assert MemoryQueryIntent.RISK_ASSESSMENT in intents
        assert MemoryQueryIntent.PRECEDENT_SUMMARY in intents
        assert MemoryQueryIntent.HUMAN_REVIEW_SUPPORT in intents

    def test_environment_has_no_memory_access(self):
        """Test that Environment agent has no memory access."""
        assert not can_query_memory(AgentRole.ENVIRONMENT)

    def test_project_has_no_memory_access(self):
        """Test that Project agent has no memory access."""
        assert not can_query_memory(AgentRole.PROJECT)

    def test_regression_guard_has_no_memory_access(self):
        """Test that Regression Guard has no memory access."""
        contract = get_memory_access(AgentRole.REGRESSION_GUARD)

        assert contract["access_level"] == MemoryAccessLevel.NONE
        assert not can_query_memory(AgentRole.REGRESSION_GUARD)

    def test_convergence_has_limited_access(self):
        """Test that Convergence agent has limited access."""
        contract = get_memory_access(AgentRole.CONVERGENCE)

        assert contract["access_level"] == MemoryAccessLevel.READ_ONLY_ADVISORY
        intents = contract["allowed_intents"]
        assert MemoryQueryIntent.CONVERGENCE_SIGNAL in intents


class TestQueryValidation:
    """Tests for query validation by role."""

    def test_validate_allows_reviewer_pattern_lookup(self):
        """Test that reviewer can use PATTERN_LOOKUP."""
        allowed, reason = validate_query_for_role(
            AgentRole.REVIEWER,
            MemoryQueryIntent.PATTERN_LOOKUP,
        )

        assert allowed is True

    def test_validate_blocks_observer_any_intent(self):
        """Test that observer is blocked for any intent."""
        allowed, reason = validate_query_for_role(
            AgentRole.OBSERVER,
            MemoryQueryIntent.PATTERN_LOOKUP,
        )

        assert allowed is False
        assert "no memory access" in reason

    def test_validate_blocks_improvement_any_intent(self):
        """Test that improvement agent is blocked for any intent."""
        allowed, reason = validate_query_for_role(
            AgentRole.IMPROVEMENT,
            MemoryQueryIntent.RISK_ASSESSMENT,
        )

        assert allowed is False

    def test_validate_blocks_reviewer_unsupported_intent(self):
        """Test that reviewer is blocked for unsupported intent."""
        allowed, reason = validate_query_for_role(
            AgentRole.REVIEWER,
            MemoryQueryIntent.CONVERGENCE_SIGNAL,
        )

        assert allowed is False
        assert "not allowed" in reason


class TestContractCompleteness:
    """Tests for contract completeness."""

    def test_all_agent_roles_have_contracts(self):
        """Verify all agent roles have defined contracts."""
        for role in AgentRole:
            contract = get_memory_access(role)
            assert "access_level" in contract
            assert "allowed_intents" in contract

    def test_contracts_have_rationale(self):
        """Verify all contracts include rationale."""
        for role, contract in MEMORY_ACCESS_CONTRACT.items():
            assert "rationale" in contract
            assert len(contract["rationale"]) > 0

    def test_contracts_have_forbidden_list(self):
        """Verify all contracts include forbidden actions."""
        for role, contract in MEMORY_ACCESS_CONTRACT.items():
            assert "forbidden" in contract
            assert isinstance(contract["forbidden"], list)

    def test_component_contracts_defined(self):
        """Verify component contracts are defined."""
        assert "ScorecardGenerator" in COMPONENT_MEMORY_ACCESS

        sg_contract = COMPONENT_MEMORY_ACCESS["ScorecardGenerator"]
        assert sg_contract["access_level"] == MemoryAccessLevel.READ_ONLY_ADVISORY


class TestContractDocumentation:
    """Tests for contract documentation generation."""

    def test_generate_documentation_returns_markdown(self):
        """Test that documentation is markdown formatted."""
        doc = generate_contract_documentation()

        assert "# Memory Query Contract" in doc
        assert "| Agent |" in doc
        assert "| Component |" in doc

    def test_documentation_includes_all_roles(self):
        """Test that documentation includes all agent roles."""
        doc = generate_contract_documentation()

        for role in AgentRole:
            assert role.value in doc

    def test_documentation_includes_constraints(self):
        """Test that documentation includes critical constraints."""
        doc = generate_contract_documentation()

        assert "MemoryQueryIntent" in doc
        assert "advisory" in doc.lower()


class TestSecurityConstraints:
    """Tests for security constraints in memory access."""

    def test_no_agent_can_bypass_review(self):
        """Test that agents with memory access have decision-related forbidden items."""
        for role, contract in MEMORY_ACCESS_CONTRACT.items():
            forbidden = contract.get("forbidden", [])
            forbidden_text = " ".join(forbidden).lower()

            if contract["access_level"] != MemoryAccessLevel.NONE:
                has_decision_constraint = any(
                    term in forbidden_text
                    for term in ["bypass", "approve", "reject", "decision", "execution", "skip"]
                )
                assert has_decision_constraint, (
                    f"Agent {role.value} with memory access should have "
                    f"decision-related forbidden actions"
                )

    def test_reviewer_has_explicit_constraint(self):
        """Test that reviewer has explicit constraint documented."""
        contract = MEMORY_ACCESS_CONTRACT[AgentRole.REVIEWER]

        assert "explicit_constraint" in contract
        assert (
            "ignore" in contract["explicit_constraint"].lower()
            or "MUST NOT" in contract["explicit_constraint"]
        )

    def test_observer_improvement_are_isolated(self):
        """Test that observer and improvement are memory-isolated."""
        observer_intents = get_allowed_intents(AgentRole.OBSERVER)
        improvement_intents = get_allowed_intents(AgentRole.IMPROVEMENT)

        assert observer_intents == []
        assert improvement_intents == []
