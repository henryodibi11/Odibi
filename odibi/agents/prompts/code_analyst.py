from __future__ import annotations

"""Code Analyst Agent (CAA) - System prompt and implementation.

Purpose: Understand, summarize, and map the Odibi codebase.
"""

CODE_ANALYST_SYSTEM_PROMPT = """
# You are the Code Analyst Agent (CAA) for the Odibi Framework

## Your Identity
You are an expert code analyst specializing in the Odibi declarative data engineering framework.
You have deep knowledge of Python, data engineering patterns, Pydantic, and both Pandas and Spark.

## Your Purpose
1. **Understand** - Parse and comprehend Odibi's codebase structure
2. **Summarize** - Provide clear explanations of modules, classes, and functions
3. **Map** - Build mental models of dependencies and relationships
4. **Identify** - Recognize patterns, anti-patterns, and engine-specific logic

## Odibi Design Principles (You MUST Understand These)
- **Declarative over imperative** - YAML configs drive behavior
- **Explicit over implicit** - No hidden magic
- **Pydantic validation** - All configs are validated models
- **match/case polymorphism** - Used for operation dispatching
- **Composition over inheritance** - Prefer composition patterns
- **Reproducibility** - Deterministic, auditable pipelines
- **Fail-fast errors** - Validate early, fail with clear messages
- **Engine abstraction** - Same logic runs on Pandas, Spark, or Polars

## Odibi Architecture Overview
```
odibi/
├── engine/          # Execution engines (Pandas, Spark, Polars)
│   ├── base.py      # Abstract Engine interface
│   ├── pandas_engine.py
│   ├── spark_engine.py
│   └── polars_engine.py
├── transformers/    # Data transformation operations
│   ├── scd.py       # Slowly Changing Dimensions
│   ├── relational.py
│   ├── advanced.py
│   └── validation.py
├── story/           # Data Story generation (audit reports)
│   ├── generator.py
│   ├── renderers.py
│   └── metadata.py
├── config.py        # Pydantic configuration models
├── node.py          # Node execution (read, transform, write)
├── pipeline.py      # Pipeline orchestration
├── graph.py         # Dependency graph (DAG) builder
├── lineage.py       # OpenLineage integration
├── registry.py      # Transform function registry (@transform decorator)
├── context.py       # Execution context (DataFrame store)
└── exceptions.py    # Custom exceptions
```

## Key Concepts You Must Explain Clearly
1. **Nodes** - Units of work (read → transform → write)
2. **Pipelines** - Collections of nodes with dependencies
3. **Stories** - Audit artifacts showing what happened to data
4. **Engines** - Abstraction layer for different DataFrame implementations
5. **Transformers** - Registered functions applied to data
6. **Lineage** - Tracking data flow across pipelines
7. **Registry** - The @transform decorator and FunctionRegistry

## How to Analyze Code
When asked about code:
1. First retrieve relevant code chunks using the RAG context
2. Identify the module's purpose from its docstring and structure
3. Map out class hierarchies and function relationships
4. Note engine-specific implementations (Pandas vs Spark)
5. Identify Pydantic models and their validation rules
6. Trace the flow of data through the system

## Response Format
When explaining code:
1. Start with a high-level summary
2. Explain the purpose and design rationale
3. List key classes, functions, and their responsibilities
4. Show relationships and dependencies
5. Note any engine-specific behavior
6. Reference specific file paths and line numbers

## Constraints
- Always cite file paths and line numbers
- Be precise about which engine (Pandas/Spark/Polars) code applies to
- Explain Odibi-specific patterns (like @transform decorator)
- If unsure, say so and suggest what to investigate

## Example Analysis Format
```
## Module: odibi/node.py

### Purpose
Handles the execution logic for individual pipeline nodes (read → transform → write).

### Key Classes
1. **NodeExecutor** (line 111-414)
   - Orchestrates the execution phases
   - Manages timing, caching, and error handling

2. **NodeResult** (line 76-87)
   - Pydantic model for execution results
   - Contains: success, duration, rows_processed, schema, errors

### Key Methods
- `execute()` - Main entry point, runs all phases
- `_execute_read_phase()` - Handles data reading
- `_execute_transform_phase()` - Applies transformations
- `_execute_write_phase()` - Persists data

### Dependencies
- Uses FunctionRegistry for transform lookups
- Integrates with Context for DataFrame storage
- Supports RetryConfig for fault tolerance
```
"""

from odibi.agents.core.agent_base import (  # noqa: E402
    AgentContext,
    AgentResponse,
    AgentRole,
    OdibiAgent,
)
from odibi.agents.core.azure_client import AzureConfig  # noqa: E402


class CodeAnalystAgent(OdibiAgent):
    """Code Analyst Agent - understands and maps the Odibi codebase."""

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=CODE_ANALYST_SYSTEM_PROMPT,
            role=AgentRole.CODE_ANALYST,
        )

    def process(self, context: AgentContext) -> AgentResponse:
        """Analyze code based on the query.

        Args:
            context: The agent context with query and history.

        Returns:
            AgentResponse with analysis results.
        """
        query = context.query

        if context.retrieved_chunks:
            chunks = context.retrieved_chunks
        else:
            analysis_filter = self._build_filter(query)
            chunks = self.retrieve_context(
                query=query,
                top_k=15,
                filter_expression=analysis_filter,
            )

        messages = [{"role": "user", "content": query}]

        if context.conversation_history:
            messages = context.conversation_history + messages

        response_text = self.chat(
            messages=messages,
            context_chunks=chunks,
            temperature=0.0,
        )

        sources = [
            {
                "file": chunk.get("file_path"),
                "name": chunk.get("name"),
                "lines": f"{chunk.get('line_start')}-{chunk.get('line_end')}",
            }
            for chunk in chunks[:5]
        ]

        return AgentResponse(
            content=response_text,
            agent_role=self.role,
            sources=sources,
            confidence=0.9 if chunks else 0.5,
            metadata={"chunks_retrieved": len(chunks)},
        )

    def _build_filter(self, query: str) -> str | None:
        """Build OData filter based on query keywords."""
        query_lower = query.lower()

        if "engine" in query_lower or "spark" in query_lower or "pandas" in query_lower:
            return "tags/any(t: t eq 'engine')"
        if "transform" in query_lower:
            return "chunk_type eq 'function' and tags/any(t: t eq 'transform')"
        if "config" in query_lower or "pydantic" in query_lower:
            return "tags/any(t: t eq 'pydantic' or t eq 'config')"
        if "story" in query_lower or "lineage" in query_lower:
            return "tags/any(t: t eq 'story' or t eq 'lineage')"

        return None

    def analyze_module(self, module_path: str) -> AgentResponse:
        """Deep analysis of a specific module.

        Args:
            module_path: Path to the module (e.g., 'odibi/node.py').

        Returns:
            AgentResponse with detailed module analysis.
        """
        filter_expr = f"file_path eq '{module_path}'"
        chunks = self.retrieve_context(
            query=f"Analyze module {module_path}",
            top_k=20,
            filter_expression=filter_expr,
        )

        prompt = f"""
Provide a comprehensive analysis of the module at {module_path}.

Include:
1. Module purpose and design rationale
2. All classes with their responsibilities
3. All public functions with their purposes
4. Key internal functions worth noting
5. Dependencies on other Odibi modules
6. Engine-specific behavior (if any)
7. Pydantic models (if any)
8. Notable patterns or anti-patterns
"""

        context = AgentContext(query=prompt, retrieved_chunks=chunks)
        return self.process(context)

    def trace_dependency(self, symbol_name: str) -> AgentResponse:
        """Trace all usages and dependencies of a symbol.

        Args:
            symbol_name: Name of the class, function, or variable.

        Returns:
            AgentResponse with dependency analysis.
        """
        chunks = self.retrieve_context(
            query=f"Find all usages of {symbol_name}",
            top_k=20,
        )

        prompt = f"""
Trace the dependencies and usages of '{symbol_name}' in the Odibi codebase.

Include:
1. Where is it defined?
2. What does it depend on?
3. What depends on it?
4. How is it used across different modules?
5. Is it engine-specific?
"""

        context = AgentContext(query=prompt, retrieved_chunks=chunks)
        return self.process(context)
