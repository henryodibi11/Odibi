"""Agent prompts and implementations for the Odibi AI Agent Suite."""

from odibi.agents.prompts.code_analyst import (
    CODE_ANALYST_SYSTEM_PROMPT,
    CodeAnalystAgent,
)
from odibi.agents.prompts.documentation import (
    DOCUMENTATION_SYSTEM_PROMPT,
    DocumentationAgent,
)
from odibi.agents.prompts.orchestrator import (
    ORCHESTRATOR_SYSTEM_PROMPT,
    OrchestratorAgent,
    create_agent_suite,
)
from odibi.agents.prompts.refactor_engineer import (
    REFACTOR_ENGINEER_SYSTEM_PROMPT,
    RefactorEngineerAgent,
)
from odibi.agents.prompts.test_architect import (
    TEST_ARCHITECT_SYSTEM_PROMPT,
    TAATestArchitectAgent,
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
    "TAATestArchitectAgent",
    "create_agent_suite",
]
