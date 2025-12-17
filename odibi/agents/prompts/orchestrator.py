"""RAG Orchestrator Agent (ROA) - System prompt and implementation.

Purpose: Coordinate all agents using Azure RAG.
"""

ORCHESTRATOR_SYSTEM_PROMPT = """
# You are the RAG Orchestrator Agent (ROA) for the Odibi Framework

## Your Identity
You are the central coordinator for the Odibi AI Agent Suite.
You orchestrate multiple specialized agents to answer user queries.

## Your Purpose
1. **Route tasks** - Determine which agent(s) should handle a request
2. **Coordinate** - Manage multi-agent workflows
3. **Synthesize** - Combine outputs from multiple agents
4. **Maintain context** - Track conversation history and state

## Available Agents

### 1. Code Analyst Agent (CAA)
**Use for:**
- Understanding code structure
- Explaining how things work
- Mapping dependencies
- Finding code locations
- Engine-specific analysis

**Example queries:**
- "How does the NodeExecutor work?"
- "What does the graph module do?"
- "Where is the SCD logic implemented?"

### 2. Refactor Engineer Agent (REA)
**Use for:**
- Code improvement suggestions
- Type hint recommendations
- Pydantic model improvements
- Pattern compliance
- Reducing boilerplate

**Example queries:**
- "How can I improve the node.py code?"
- "Suggest type hint improvements for X"
- "Review this module for best practices"

### 3. Test Architect Agent (TAA)
**Use for:**
- Generating unit tests
- Creating integration tests
- Test data generation
- SCD transformation tests
- Validation tests

**Example queries:**
- "Generate tests for the SCD transformer"
- "Create unit tests for NodeConfig"
- "Write integration tests for the pipeline"

### 4. Documentation Agent (DA)
**Use for:**
- API documentation
- YAML schema docs
- Tutorials and guides
- Architecture diagrams
- Getting started guides

**Example queries:**
- "Document the config module"
- "Create a tutorial for custom transformers"
- "Generate YAML schema docs for NodeConfig"

## Routing Decision Process

### Step 1: Classify the Query Type
- **Understanding** → CAA
- **Improving** → REA
- **Testing** → TAA
- **Documenting** → DA
- **Mixed** → Multiple agents

### Step 2: Determine Scope
- **Single file/class** → Targeted agent call
- **Multiple components** → Multi-agent workflow
- **Full system** → Orchestrated pipeline

### Step 3: Execute and Synthesize
- Call appropriate agent(s)
- Combine outputs if multiple
- Add your own synthesis and recommendations

## Multi-Agent Workflow Examples

### Example 1: "Audit the derive_columns transformer"
1. **CAA** - Analyze the transformer implementation
2. **REA** - Suggest improvements
3. **TAA** - Generate test cases
4. **Synthesize** - Combine into audit report

### Example 2: "Help me add a new transformer"
1. **CAA** - Explain existing transformer patterns
2. **DA** - Document the new transformer
3. **TAA** - Generate tests
4. **Synthesize** - Complete implementation guide

### Example 3: "Document the config schema"
1. **CAA** - Extract all Pydantic models
2. **DA** - Generate YAML schema docs
3. **Synthesize** - Complete documentation

## Response Format

When orchestrating:

1. **Query Analysis**
   - What is the user asking for?
   - What agents are needed?

2. **Agent Calls**
   - Which agent(s) to invoke
   - What specific task for each

3. **Synthesis**
   - Combined response
   - Cross-references between agent outputs
   - Overall recommendations

## Handling Ambiguous Queries

If the query is unclear:
1. Ask for clarification
2. Offer options: "Would you like me to:
   - Explain how X works? (Code Analysis)
   - Suggest improvements? (Refactoring)
   - Generate tests? (Testing)
   - Create documentation? (Documentation)"

## Maintaining Context

Track:
- Previous queries and responses
- Files/modules already discussed
- User's apparent goal
- Agent outputs for cross-referencing

## Error Handling

If an agent fails:
1. Try alternative approach
2. Provide partial results
3. Explain what worked and what didn't
4. Suggest manual investigation paths
"""

from dataclasses import dataclass, field  # noqa: E402
from typing import Optional  # noqa: E402

from odibi.agents.core.agent_base import (  # noqa: E402
    AgentContext,
    AgentRegistry,
    AgentResponse,
    AgentRole,
    OdibiAgent,
)
from odibi.agents.core.azure_client import AzureConfig  # noqa: E402


@dataclass
class OrchestratorState:
    """State maintained by the orchestrator across interactions."""

    conversation_history: list[dict] = field(default_factory=list)
    discussed_files: set[str] = field(default_factory=set)
    discussed_topics: list[str] = field(default_factory=list)
    last_agent_used: Optional[AgentRole] = None
    context_chunks: list[dict] = field(default_factory=list)


class OrchestratorAgent(OdibiAgent):
    """RAG Orchestrator Agent - coordinates all specialized agents."""

    ROUTING_KEYWORDS = {
        AgentRole.CODE_ANALYST: [
            "explain",
            "how does",
            "what is",
            "where is",
            "understand",
            "analyze",
            "find",
            "locate",
            "dependency",
            "architecture",
            "structure",
            "works",
            "implementation",
        ],
        AgentRole.REFACTOR_ENGINEER: [
            "improve",
            "refactor",
            "cleanup",
            "better",
            "optimize",
            "type hint",
            "pydantic",
            "pattern",
            "best practice",
            "suggestion",
            "review",
            "fix",
        ],
        AgentRole.TEST_ARCHITECT: [
            "test",
            "unit test",
            "integration test",
            "fixture",
            "pytest",
            "coverage",
            "mock",
            "assert",
            "verify",
            "validate",
            "scd test",
        ],
        AgentRole.DOCUMENTATION: [
            "document",
            "documentation",
            "tutorial",
            "guide",
            "yaml schema",
            "api doc",
            "readme",
            "example",
            "diagram",
            "getting started",
        ],
    }

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=ORCHESTRATOR_SYSTEM_PROMPT,
            role=AgentRole.ORCHESTRATOR,
        )
        self.state = OrchestratorState()

    def process(self, context: AgentContext) -> AgentResponse:
        """Process a user query by routing to appropriate agents.

        Args:
            context: The agent context with query and history.

        Returns:
            AgentResponse with orchestrated results.
        """
        query = context.query
        query_lower = query.lower()

        self.state.conversation_history.append({"role": "user", "content": query})

        target_agents = self._route_query(query_lower)

        if not target_agents:
            return self._handle_ambiguous_query(query)

        if len(target_agents) == 1:
            response = self._call_single_agent(target_agents[0], context)
        else:
            response = self._call_multiple_agents(target_agents, context)

        self.state.conversation_history.append({"role": "assistant", "content": response.content})

        return response

    def _route_query(self, query_lower: str) -> list[AgentRole]:
        """Determine which agents should handle the query."""
        matched_agents = []
        match_scores = {}

        for agent_role, keywords in self.ROUTING_KEYWORDS.items():
            score = sum(1 for kw in keywords if kw in query_lower)
            if score > 0:
                match_scores[agent_role] = score

        if not match_scores:
            return [AgentRole.CODE_ANALYST]

        sorted_agents = sorted(match_scores.items(), key=lambda x: x[1], reverse=True)

        if len(sorted_agents) >= 2 and sorted_agents[0][1] == sorted_agents[1][1]:
            matched_agents = [sorted_agents[0][0], sorted_agents[1][0]]
        else:
            matched_agents = [sorted_agents[0][0]]

        if "audit" in query_lower or "full" in query_lower:
            matched_agents = [
                AgentRole.CODE_ANALYST,
                AgentRole.REFACTOR_ENGINEER,
                AgentRole.TEST_ARCHITECT,
            ]

        return matched_agents

    def _call_single_agent(self, agent_role: AgentRole, context: AgentContext) -> AgentResponse:
        """Call a single specialized agent."""
        agent = AgentRegistry.get(agent_role)

        if agent is None:
            return AgentResponse(
                content=f"Agent {agent_role.value} is not registered.",
                agent_role=self.role,
                confidence=0.0,
            )

        agent_context = AgentContext(
            query=context.query,
            conversation_history=self.state.conversation_history[:-1],
            metadata=context.metadata,
            odibi_root=context.odibi_root,
        )

        response = agent.process(agent_context)

        self.state.last_agent_used = agent_role
        for source in response.sources:
            if "file" in source:
                self.state.discussed_files.add(source["file"])

        return response

    def _call_multiple_agents(
        self, agent_roles: list[AgentRole], context: AgentContext
    ) -> AgentResponse:
        """Call multiple agents and synthesize their outputs."""
        responses = []
        all_sources = []

        for agent_role in agent_roles:
            agent = AgentRegistry.get(agent_role)
            if agent is None:
                continue

            agent_context = AgentContext(
                query=context.query,
                conversation_history=self.state.conversation_history[:-1],
                metadata=context.metadata,
                odibi_root=context.odibi_root,
            )

            response = agent.process(agent_context)
            responses.append((agent_role, response))
            all_sources.extend(response.sources)

        synthesized = self._synthesize_responses(context.query, responses)

        return AgentResponse(
            content=synthesized,
            agent_role=self.role,
            sources=all_sources[:10],
            confidence=min(r.confidence for _, r in responses) if responses else 0.5,
            metadata={
                "agents_used": [role.value for role, _ in responses],
                "individual_responses": len(responses),
            },
        )

    def _synthesize_responses(
        self,
        query: str,
        responses: list[tuple[AgentRole, AgentResponse]],
    ) -> str:
        """Synthesize multiple agent responses into a cohesive answer."""
        synthesis_prompt = f"""
You are synthesizing outputs from multiple specialized agents to answer:
"{query}"

Here are the agent responses:

"""
        for agent_role, response in responses:
            synthesis_prompt += f"""
## {agent_role.value.replace("_", " ").title()} Agent Response:
{response.content}

---
"""

        synthesis_prompt += """
Please synthesize these responses into a cohesive, well-structured answer.
Combine insights, eliminate redundancy, and add cross-references where appropriate.
"""

        synthesized = self.openai_client.chat_completion(
            messages=[{"role": "user", "content": synthesis_prompt}],
            system_prompt=self.system_prompt,
            temperature=0.1,
        )

        return synthesized

    def _handle_ambiguous_query(self, query: str) -> AgentResponse:
        """Handle queries that don't match any specific agent."""
        clarification = f"""
I'd be happy to help with "{query}". Could you clarify what you'd like me to do?

**I can help you:**

1. **Understand code** (Code Analyst)
   - Explain how components work
   - Find where things are implemented
   - Trace dependencies

2. **Improve code** (Refactor Engineer)
   - Suggest refactoring improvements
   - Review for best practices
   - Improve type hints and Pydantic models

3. **Generate tests** (Test Architect)
   - Create unit tests
   - Generate integration tests
   - Build test fixtures

4. **Create documentation** (Documentation)
   - Write API docs
   - Create tutorials
   - Generate YAML schema docs

Which would be most helpful for your needs?
"""

        return AgentResponse(
            content=clarification,
            agent_role=self.role,
            confidence=0.3,
            follow_up_needed=True,
            metadata={"requires_clarification": True},
        )

    def reset_state(self) -> None:
        """Reset the orchestrator state."""
        self.state = OrchestratorState()

    def audit_component(self, component_name: str) -> AgentResponse:
        """Perform a full audit of a component using all agents.

        Args:
            component_name: Name of the component to audit.

        Returns:
            AgentResponse with comprehensive audit.
        """
        audit_query = f"Perform a complete audit of {component_name}"

        audit_agents = [
            AgentRole.CODE_ANALYST,
            AgentRole.REFACTOR_ENGINEER,
            AgentRole.TEST_ARCHITECT,
            AgentRole.DOCUMENTATION,
        ]

        context = AgentContext(query=audit_query)
        return self._call_multiple_agents(audit_agents, context)


def create_agent_suite(config: AzureConfig) -> OrchestratorAgent:
    """Create and register all agents, returning the orchestrator.

    Args:
        config: Azure configuration.

    Returns:
        The orchestrator agent with all specialized agents registered.
    """
    from odibi.agents.prompts.code_analyst import CodeAnalystAgent
    from odibi.agents.prompts.documentation import DocumentationAgent
    from odibi.agents.prompts.refactor_engineer import RefactorEngineerAgent
    from odibi.agents.prompts.test_architect import TAATestArchitectAgent

    AgentRegistry.clear()

    caa = CodeAnalystAgent(config)
    rea = RefactorEngineerAgent(config)
    taa = TAATestArchitectAgent(config)
    da = DocumentationAgent(config)
    orchestrator = OrchestratorAgent(config)

    AgentRegistry.register(caa)
    AgentRegistry.register(rea)
    AgentRegistry.register(taa)
    AgentRegistry.register(da)
    AgentRegistry.register(orchestrator)

    return orchestrator
