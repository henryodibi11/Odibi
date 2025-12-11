"""Agent prompts and implementations for the Odibi AI Agent Suite."""

from agents.prompts.code_analyst import (
    CODE_ANALYST_SYSTEM_PROMPT,
    CodeAnalystAgent,
)
from agents.prompts.documentation import (
    DOCUMENTATION_SYSTEM_PROMPT,
    DocumentationAgent,
)
from agents.prompts.orchestrator import (
    ORCHESTRATOR_SYSTEM_PROMPT,
    OrchestratorAgent,
    create_agent_suite,
)
from agents.prompts.refactor_engineer import (
    REFACTOR_ENGINEER_SYSTEM_PROMPT,
    RefactorEngineerAgent,
)
from agents.prompts.test_architect import (
    TEST_ARCHITECT_SYSTEM_PROMPT,
    TestArchitectAgent,
)

__all__ = [
    "CODE_ANALYST_SYSTEM_PROMPT",
    "CodeAnalystAgent",
    "DOCUMENTATION_SYSTEM_PROMPT",
    "DocumentationAgent",
    "ORCHESTRATOR_SYSTEM_PROMPT",
    "OrchestratorAgent",
    "REFACTOR_ENGINEER_SYSTEM_PROMPT",
    "RefactorEngineerAgent",
    "TEST_ARCHITECT_SYSTEM_PROMPT",
    "TestArchitectAgent",
    "create_agent_suite",
]
