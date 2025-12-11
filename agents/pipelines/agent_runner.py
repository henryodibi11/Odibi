"""Agent runner for interactive and batch execution.

This module provides:
1. Interactive CLI for agent conversations
2. Batch execution for automated workflows
3. Databricks-compatible execution patterns
"""

import os
from dataclasses import dataclass
from typing import Optional

from agents.core.agent_base import AgentContext, AgentResponse, AgentRole
from agents.core.azure_client import AzureConfig
from agents.core.memory import Memory, MemoryManager, MemoryType
from agents.prompts.orchestrator import create_agent_suite


@dataclass
class AgentRunnerConfig:
    """Configuration for the agent runner."""

    odibi_root: str = "d:/odibi"
    max_history_turns: int = 10
    default_temperature: float = 0.0
    verbose: bool = True
    enable_memory: bool = True
    memory_path: str = "d:/odibi/.odibi/memories"
    auto_save_memories: bool = True


class AgentRunner:
    """Runs agents interactively or in batch mode with long-term memory."""

    def __init__(
        self,
        azure_config: AzureConfig,
        runner_config: Optional[AgentRunnerConfig] = None,
    ):
        self.azure_config = azure_config
        self.runner_config = runner_config or AgentRunnerConfig()
        self.orchestrator = create_agent_suite(azure_config)

        self.session_history: list[dict[str, str]] = []

        if self.runner_config.enable_memory:
            from agents.core.azure_client import AzureOpenAIClient

            llm_client = None
            try:
                llm_client = AzureOpenAIClient(azure_config)
            except Exception:
                pass

            self.memory = MemoryManager(
                backend_type="local",
                local_path=self.runner_config.memory_path,
                llm_client=llm_client,
            )
        else:
            self.memory = None

    def ask(self, query: str, agent_role: Optional[AgentRole] = None) -> AgentResponse:
        """Ask a question and get a response.

        Args:
            query: The user's question.
            agent_role: Optional specific agent to use (bypasses orchestrator routing).

        Returns:
            AgentResponse with the answer.
        """
        memory_context = ""
        if self.memory:
            memory_context = self.memory.get_context(query)

        enhanced_query = query
        if memory_context:
            enhanced_query = f"{query}\n\n{memory_context}"

        context = AgentContext(
            query=enhanced_query,
            odibi_root=self.runner_config.odibi_root,
        )

        if agent_role and agent_role != AgentRole.ORCHESTRATOR:
            from agents.core.agent_base import AgentRegistry

            agent = AgentRegistry.get(agent_role)
            if agent:
                response = agent.process(context)
            else:
                response = self.orchestrator.process(context)
        else:
            response = self.orchestrator.process(context)

        self.session_history.append({"role": "user", "content": query})
        self.session_history.append({"role": "assistant", "content": response.content})

        return response

    def run_workflow(self, workflow: list[dict]) -> list[AgentResponse]:
        """Run a predefined workflow of queries.

        Args:
            workflow: List of workflow steps, each with 'query' and optional 'agent'.

        Returns:
            List of responses for each step.
        """
        responses = []

        for step in workflow:
            query = step.get("query", "")
            agent_name = step.get("agent")

            agent_role = None
            if agent_name:
                agent_role = AgentRole(agent_name)

            response = self.ask(query, agent_role=agent_role)
            responses.append(response)

            if self.runner_config.verbose:
                print(f"\n{'='*60}")
                print(f"Query: {query}")
                print(f"Agent: {response.agent_role.value}")
                print(f"Response:\n{response.content[:500]}...")
                print(f"{'='*60}")

        return responses

    def audit_transformer(self, transformer_name: str) -> dict:
        """Run a full audit workflow for a transformer.

        Args:
            transformer_name: Name of the transformer to audit.

        Returns:
            Dictionary with audit results.
        """
        workflow = [
            {
                "query": f"Explain how the {transformer_name} transformer works",
                "agent": "code_analyst",
            },
            {
                "query": f"Suggest improvements for the {transformer_name} transformer",
                "agent": "refactor_engineer",
            },
            {
                "query": f"Generate comprehensive tests for the {transformer_name} transformer",
                "agent": "test_architect",
            },
            {
                "query": f"Document the {transformer_name} transformer with examples",
                "agent": "documentation",
            },
        ]

        responses = self.run_workflow(workflow)

        return {
            "transformer": transformer_name,
            "analysis": responses[0].content,
            "improvements": responses[1].content,
            "tests": responses[2].content,
            "documentation": responses[3].content,
        }

    def explain_dependency_resolution(self) -> str:
        """Explain how dependency resolution works in Odibi.

        Returns:
            Explanation string.
        """
        response = self.ask(
            "Explain dependency resolution in the DependencyGraph class, "
            "including how topological sort and execution layers work",
            agent_role=AgentRole.CODE_ANALYST,
        )
        return response.content

    def generate_scd_tests(self) -> str:
        """Generate tests for all SCD transformations.

        Returns:
            Generated test code.
        """
        response = self.ask(
            "Generate comprehensive pytest tests for all SCD transformations "
            "(Type 1, Type 2, Type 4) including edge cases",
            agent_role=AgentRole.TEST_ARCHITECT,
        )
        return response.content

    def document_config_schema(self) -> str:
        """Document the entire config schema.

        Returns:
            Generated documentation.
        """
        response = self.ask(
            "Generate complete YAML schema documentation for all Odibi "
            "configuration models (NodeConfig, PipelineConfig, ReadConfig, "
            "WriteConfig, TransformConfig)",
            agent_role=AgentRole.DOCUMENTATION,
        )
        return response.content

    def reset_conversation(self) -> None:
        """Reset the conversation state."""
        self.orchestrator.reset_state()
        self.session_history = []

    def save_session_memories(self, auto_extract: bool = True) -> list[Memory]:
        """Save important information from the current session to long-term memory.

        Args:
            auto_extract: Use LLM to extract key information.

        Returns:
            List of memories created.
        """
        if not self.memory or not self.session_history:
            return []

        memories = self.memory.extract_and_store(
            self.session_history,
            auto_extract=auto_extract,
        )

        if self.runner_config.verbose:
            print(f"\nSaved {len(memories)} memories from this session:")
            for mem in memories:
                print(f"  [{mem.memory_type.value}] {mem.summary}")

        return memories

    def remember(
        self,
        content: str,
        summary: str,
        memory_type: MemoryType = MemoryType.LEARNING,
        tags: Optional[list[str]] = None,
        importance: float = 0.7,
    ) -> Optional[Memory]:
        """Manually store a memory.

        Args:
            content: Full content to remember.
            summary: Short summary.
            memory_type: Type of memory.
            tags: Optional tags for filtering.
            importance: 0-1 importance score.

        Returns:
            The stored memory, or None if memory is disabled.
        """
        if not self.memory:
            return None

        return self.memory.remember(
            memory_type=memory_type,
            content=content,
            summary=summary,
            tags=tags,
            importance=importance,
        )

    def recall(self, query: str, top_k: int = 5) -> list[Memory]:
        """Recall relevant memories.

        Args:
            query: What to recall.
            top_k: Number of results.

        Returns:
            List of relevant memories.
        """
        if not self.memory:
            return []

        return self.memory.recall(query, top_k=top_k)

    def show_recent_memories(self, days: int = 7, limit: int = 10) -> list[Memory]:
        """Show recent memories.

        Args:
            days: How many days back.
            limit: Max results.

        Returns:
            List of recent memories.
        """
        if not self.memory:
            return []

        memories = self.memory.store.get_recent(days=days, limit=limit)

        if self.runner_config.verbose:
            print(f"\nRecent memories (last {days} days):")
            for mem in memories:
                date = mem.created_at[:10]
                print(f"  [{date}] [{mem.memory_type.value}] {mem.summary}")

        return memories


def run_interactive_cli():
    """Run an interactive CLI for the agent system."""
    import argparse

    parser = argparse.ArgumentParser(description="Odibi AI Agent Suite - Interactive Mode")
    parser.add_argument(
        "--chat-model",
        default=None,
        help="Azure OpenAI chat deployment (e.g., gpt-4.1, gpt-4o, gpt-35-turbo)",
    )
    parser.add_argument(
        "--embedding-model",
        default=None,
        help="Azure OpenAI embedding deployment (e.g., text-embedding-3-large)",
    )
    parser.add_argument(
        "--odibi-root",
        default="d:/odibi",
        help="Path to Odibi repository root",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Odibi AI Agent Suite - Interactive Mode")
    print("=" * 60)
    print("\nCommands:")
    print("  /quit - Exit and save memories")
    print("  /reset - Reset conversation history")
    print("  /agent <role> - Switch to a specific agent")
    print("  /audit <name> - Run full audit on a component")
    print("  /config - Show current configuration")
    print("  /save - Save session memories now")
    print("  /recall <query> - Search past memories")
    print("  /remember <text> - Manually save a memory")
    print("  /memories - Show recent memories")
    print("\nAgents: code_analyst, refactor_engineer, test_architect, documentation")
    print("=" * 60)

    config = AzureConfig.from_env()

    if args.chat_model:
        config.chat_deployment = args.chat_model
    if args.embedding_model:
        config.embedding_deployment = args.embedding_model

    errors = config.validate()
    if errors:
        print("\nConfiguration errors:")
        for error in errors:
            print(f"  - {error}")
        print("\nSet environment variables or use CLI arguments.")
        return

    print(f"\nUsing model: {config.chat_deployment}")
    print(f"Embeddings: {config.embedding_deployment}")

    runner = AgentRunner(config)
    current_agent: Optional[AgentRole] = None

    while True:
        try:
            prompt = f"\n[{current_agent.value if current_agent else 'orchestrator'}] > "
            user_input = input(prompt).strip()

            if not user_input:
                continue

            if user_input.lower() == "/quit":
                if runner.session_history:
                    print("\nSaving session memories...")
                    runner.save_session_memories(auto_extract=True)
                print("Goodbye!")
                break

            if user_input.lower() == "/reset":
                runner.reset_conversation()
                current_agent = None
                print("Conversation reset.")
                continue

            if user_input.lower().startswith("/agent "):
                agent_name = user_input[7:].strip()
                try:
                    current_agent = AgentRole(agent_name)
                    print(f"Switched to {current_agent.value} agent")
                except ValueError:
                    print(f"Unknown agent: {agent_name}")
                    print(
                        "Available: code_analyst, refactor_engineer, test_architect, documentation"
                    )
                continue

            if user_input.lower().startswith("/audit "):
                component = user_input[7:].strip()
                print(f"\nRunning full audit on: {component}")
                result = runner.audit_transformer(component)
                print("\n" + "=" * 60)
                print("AUDIT COMPLETE")
                print("=" * 60)
                for section, content in result.items():
                    if section != "transformer":
                        print(f"\n## {section.upper()}")
                        print(content[:1000])
                        print("...")
                continue

            if user_input.lower() == "/config":
                print("\nCurrent Configuration:")
                print(f"  OpenAI Endpoint: {config.openai_endpoint}")
                print(f"  Chat Model: {config.chat_deployment}")
                print(
                    f"  Embedding Model: {config.embedding_deployment} ({config.embedding_dimensions}d)"
                )
                print(f"  Search Endpoint: {config.search_endpoint}")
                print(f"  Search Index: {config.search_index}")
                print(f"  Memory Enabled: {runner.memory is not None}")
                continue

            if user_input.lower() == "/save":
                runner.save_session_memories(auto_extract=True)
                continue

            if user_input.lower().startswith("/recall "):
                query = user_input[8:].strip()
                memories = runner.recall(query, top_k=5)
                if memories:
                    print(f"\nFound {len(memories)} relevant memories:")
                    for mem in memories:
                        print(f"\n  [{mem.memory_type.value}] {mem.summary}")
                        print(f"  {mem.content[:200]}...")
                else:
                    print("\nNo relevant memories found.")
                continue

            if user_input.lower().startswith("/remember "):
                text = user_input[10:].strip()
                memory = runner.remember(
                    content=text,
                    summary=text[:100],
                    memory_type=MemoryType.LEARNING,
                    importance=0.8,
                )
                if memory:
                    print(f"\nMemory saved: {memory.summary}")
                continue

            if user_input.lower() == "/memories":
                runner.show_recent_memories(days=30, limit=10)
                continue

            response = runner.ask(user_input, agent_role=current_agent)

            print(f"\n[{response.agent_role.value}]:")
            print(response.content)

            if response.sources:
                print("\nSources:")
                for source in response.sources[:3]:
                    print(f"  - {source.get('file', 'unknown')}: {source.get('name', 'unknown')}")

        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"\nError: {e}")


def run_databricks_agent(
    query: str,
    agent_role: Optional[str] = None,
    odibi_root: str = "/Workspace/odibi",
) -> dict:
    """Run agent in Databricks environment.

    This function is designed for use in Databricks notebooks.

    Args:
        query: The user's question.
        agent_role: Optional specific agent to use.
        odibi_root: Path to Odibi in Databricks workspace.

    Returns:
        Dictionary with response details.

    Example:
        ```python
        # In Databricks notebook
        from agents.pipelines.agent_runner import run_databricks_agent

        result = run_databricks_agent(
            "Explain how the NodeExecutor works",
            agent_role="code_analyst"
        )
        print(result["content"])
        ```
    """
    openai_key = os.getenv("AZURE_OPENAI_API_KEY")
    search_key = os.getenv("AZURE_SEARCH_API_KEY")

    if not openai_key or not search_key:
        return {
            "error": "Missing API keys",
            "message": "Set AZURE_OPENAI_API_KEY and AZURE_SEARCH_API_KEY",
        }

    config = AzureConfig(
        openai_api_key=openai_key,
        search_api_key=search_key,
    )

    runner_config = AgentRunnerConfig(odibi_root=odibi_root, verbose=False)
    runner = AgentRunner(config, runner_config)

    role = AgentRole(agent_role) if agent_role else None
    response = runner.ask(query, agent_role=role)

    return {
        "content": response.content,
        "agent": response.agent_role.value,
        "sources": response.sources,
        "confidence": response.confidence,
        "metadata": response.metadata,
    }


if __name__ == "__main__":
    run_interactive_cli()
