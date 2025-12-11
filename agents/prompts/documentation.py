"""Documentation Agent (DA) - System prompt and implementation.

Purpose: Build scalable documentation for the Odibi framework.
"""

DOCUMENTATION_SYSTEM_PROMPT = """
# You are the Documentation Agent (DA) for the Odibi Framework

## Your Identity
You are an expert technical writer specializing in:
- API documentation
- Developer guides and tutorials
- Architecture documentation
- YAML schema documentation
- Diagram creation (Mermaid)
- README best practices

## Your Purpose
1. **Generate API docs** - Document classes, functions, and modules
2. **Create guides** - Write tutorials and how-to guides
3. **Document YAML schemas** - Explain configuration options
4. **Create diagrams** - Visualize architecture and flows
5. **Build reference docs** - Comprehensive reference material

## Odibi Documentation Philosophy
- **"Stories over magic"** - Explain the WHY, not just the WHAT
- **Progressive disclosure** - Start simple, add detail as needed
- **Working examples** - Every concept has a working example
- **Copy-paste ready** - Code examples should work out of the box

## Documentation Types

### 1. Module Documentation
```markdown
# Module: odibi.node

## Overview
The `node` module handles individual pipeline node execution,
orchestrating the read → transform → write lifecycle.

## Key Classes

### NodeExecutor
Manages the execution phases of a node.

**Constructor:**
```python
NodeExecutor(
    context: Context,
    engine: Engine,
    connections: Dict[str, Any],
    catalog_manager: Optional[CatalogManager] = None,
)
```

**Key Methods:**
- `execute(config, input_df, dry_run)` - Execute the node
- `_execute_read_phase()` - Handle data reading
- `_execute_transform_phase()` - Apply transformations
```

### 2. Pydantic Model Documentation
```markdown
# Configuration: NodeConfig

Defines the configuration for a pipeline node.

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | `str` | Yes | - | Unique node identifier |
| `depends_on` | `list[str]` | No | `[]` | Node dependencies |
| `read` | `ReadConfig` | No | `None` | Data source configuration |
| `transform` | `TransformConfig` | No | `None` | Transformation steps |
| `write` | `WriteConfig` | No | `None` | Output destination |

## Example
```yaml
nodes:
  - name: clean_customers
    depends_on: [raw_customers]
    transform:
      steps:
        - operation: drop_nulls
          params:
            columns: [customer_id]
    write:
      connection: silver
      format: delta
      path: customers_clean
```
```

### 3. YAML Schema Documentation
```markdown
# YAML Schema: odibi.yaml

## Top-Level Structure
```yaml
pipeline: my_pipeline           # Required: Pipeline name
description: My data pipeline   # Optional: Description
engine: pandas                  # Optional: pandas | spark | polars

connections:                    # Required: Connection definitions
  bronze:
    type: local
    base_path: ./data/bronze

nodes:                          # Required: Node definitions
  - name: node_1
    ...
```

## Field Reference

### `pipeline` (required)
The unique name of this pipeline.

**Type:** `string`
**Pattern:** `^[a-z][a-z0-9_]*$`

### `engine` (optional)
The execution engine to use.

**Type:** `string`
**Enum:** `pandas`, `spark`, `polars`
**Default:** `pandas`
```

### 4. Tutorial Documentation
```markdown
# Tutorial: Building Your First Odibi Pipeline

## What You'll Learn
- Creating connections
- Defining nodes
- Running the pipeline
- Viewing the Story

## Prerequisites
- Python 3.9+
- `pip install odibi`

## Step 1: Create Project Structure
```bash
mkdir my_pipeline
cd my_pipeline
touch odibi.yaml
```

## Step 2: Define Connections
```yaml
connections:
  input:
    type: local
    base_path: ./data/input
  output:
    type: local
    base_path: ./data/output
```

[Continue with steps...]
```

### 5. Architecture Documentation
```markdown
# Odibi Architecture

## High-Level Overview

```mermaid
graph TB
    YAML[odibi.yaml] --> Parser[Config Parser]
    Parser --> Pipeline[Pipeline]
    Pipeline --> Graph[Dependency Graph]
    Graph --> Executor[Executor]
    Executor --> Node1[Node 1]
    Executor --> Node2[Node 2]
    Node1 --> Engine[Engine]
    Node2 --> Engine
    Engine --> Pandas[Pandas]
    Engine --> Spark[Spark]
```

## Component Descriptions

### Config Parser
Parses YAML files into Pydantic models.
Validates all configuration at load time.

### Dependency Graph
Uses Kahn's algorithm for topological sorting.
Enables parallel execution of independent nodes.
```

## Documentation Standards

### Markdown Conventions
- Use `#` for titles, `##` for sections, `###` for subsections
- Code blocks with language specifiers: \`\`\`python, \`\`\`yaml
- Tables for structured data
- Mermaid diagrams for visualizations

### Code Example Requirements
- Complete, runnable examples
- Include imports
- Include expected output or assertions
- Use realistic but simple data

### API Documentation Requirements
For each class/function:
1. One-line summary
2. Detailed description
3. Args with types and descriptions
4. Returns with type and description
5. Raises with exception types
6. Example usage

## Response Format

When generating documentation:

1. **Document Type** - What kind of doc this is
2. **Title** - Clear, descriptive title
3. **Content** - The actual documentation
4. **Cross-References** - Links to related docs
5. **Examples** - Working code examples
"""

from agents.core.agent_base import (  # noqa: E402
    AgentContext,
    AgentResponse,
    AgentRole,
    OdibiAgent,
)
from agents.core.azure_client import AzureConfig  # noqa: E402


class DocumentationAgent(OdibiAgent):
    """Documentation Agent - generates comprehensive documentation."""

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=DOCUMENTATION_SYSTEM_PROMPT,
            role=AgentRole.DOCUMENTATION,
        )

    def process(self, context: AgentContext) -> AgentResponse:
        """Generate documentation based on the query.

        Args:
            context: The agent context with query and history.

        Returns:
            AgentResponse with generated documentation.
        """
        query = context.query

        if context.retrieved_chunks:
            chunks = context.retrieved_chunks
        else:
            chunks = self.retrieve_context(
                query=query,
                top_k=15,
            )

        messages = [{"role": "user", "content": query}]

        if context.conversation_history:
            messages = context.conversation_history + messages

        response_text = self.chat(
            messages=messages,
            context_chunks=chunks,
            temperature=0.3,
        )

        sources = [
            {"file": chunk.get("file_path"), "name": chunk.get("name")} for chunk in chunks[:5]
        ]

        return AgentResponse(
            content=response_text,
            agent_role=self.role,
            sources=sources,
            confidence=0.9,
            metadata={"chunks_retrieved": len(chunks)},
        )

    def generate_api_docs(self, module_path: str) -> AgentResponse:
        """Generate API documentation for a module.

        Args:
            module_path: Path to the module.

        Returns:
            AgentResponse with API documentation.
        """
        filter_expr = f"file_path eq '{module_path}'"
        chunks = self.retrieve_context(
            query=f"Document module {module_path}",
            top_k=25,
            filter_expression=filter_expr,
        )

        prompt = f"""
Generate comprehensive API documentation for {module_path}.

Include:
1. Module overview
2. All public classes with:
   - Description
   - Constructor parameters
   - Public methods with full signatures
   - Example usage
3. All public functions with:
   - Description
   - Parameters with types
   - Return type and description
   - Example usage
4. Type definitions and enums
5. Cross-references to related modules

Format: Markdown suitable for mkdocs
"""

        context = AgentContext(query=prompt, retrieved_chunks=chunks)
        return self.process(context)

    def generate_yaml_schema_docs(self, config_model: str) -> AgentResponse:
        """Generate YAML schema documentation for a config model.

        Args:
            config_model: Name of the Pydantic config model.

        Returns:
            AgentResponse with YAML schema docs.
        """
        filter_expr = "tags/any(t: t eq 'pydantic' or t eq 'config')"
        chunks = self.retrieve_context(
            query=f"Pydantic model {config_model} fields validation",
            top_k=15,
            filter_expression=filter_expr,
        )

        prompt = f"""
Generate YAML schema documentation for the '{config_model}' Pydantic model.

Include:
1. Full YAML structure example
2. Field reference table with:
   - Field name
   - Type
   - Required/Optional
   - Default value
   - Description
   - Constraints (min, max, pattern, etc.)
3. Validation rules explained
4. Common configurations
5. Error messages and troubleshooting

Format: Markdown with tables and code blocks
"""

        context = AgentContext(query=prompt, retrieved_chunks=chunks)
        return self.process(context)

    def generate_tutorial(self, topic: str) -> AgentResponse:
        """Generate a tutorial on a specific topic.

        Args:
            topic: The tutorial topic.

        Returns:
            AgentResponse with tutorial content.
        """
        chunks = self.retrieve_context(
            query=f"tutorial {topic} example usage",
            top_k=20,
        )

        prompt = f"""
Create a tutorial on '{topic}' for Odibi.

Structure:
1. **What You'll Learn** - Learning objectives
2. **Prerequisites** - What's needed
3. **Step-by-Step Guide** - Numbered steps with code
4. **Complete Example** - Full working code
5. **Common Issues** - Troubleshooting
6. **Next Steps** - Where to go from here

Requirements:
- All code must be copy-paste ready
- Use realistic but simple examples
- Explain the "why" not just the "how"
- Include expected output
"""

        context = AgentContext(query=prompt, retrieved_chunks=chunks)
        return self.process(context)

    def generate_architecture_diagram(self, component: str) -> AgentResponse:
        """Generate architecture documentation with Mermaid diagrams.

        Args:
            component: The component to document.

        Returns:
            AgentResponse with architecture docs and diagrams.
        """
        chunks = self.retrieve_context(
            query=f"architecture {component} classes relationships",
            top_k=20,
        )

        prompt = f"""
Create architecture documentation for '{component}' in Odibi.

Include:
1. **Overview** - What this component does
2. **Architecture Diagram** - Mermaid diagram showing:
   - Key classes and their relationships
   - Data flow
   - Dependencies
3. **Component Descriptions** - Explain each part
4. **Interaction Flow** - Sequence diagram if applicable
5. **Extension Points** - How to extend/customize

Use Mermaid syntax for diagrams:
- `graph TB` for architecture diagrams
- `sequenceDiagram` for interaction flows
- `classDiagram` for class relationships
"""

        context = AgentContext(query=prompt, retrieved_chunks=chunks)
        return self.process(context)

    def generate_getting_started_guide(self) -> AgentResponse:
        """Generate a getting started guide for Odibi.

        Returns:
            AgentResponse with getting started guide.
        """
        chunks = self.retrieve_context(
            query="pipeline config connection node example tutorial",
            top_k=20,
        )

        prompt = """
Create a comprehensive "Getting Started with Odibi" guide.

Sections:
1. **Introduction** - What is Odibi and why use it
2. **Installation** - pip install and extras
3. **Quick Start** - 5-minute hello world
4. **Core Concepts** - Explain:
   - Connections
   - Nodes
   - Pipelines
   - Stories
5. **Your First Pipeline** - Step-by-step tutorial
6. **Running Pipelines** - CLI commands
7. **Viewing Results** - Story output
8. **Next Steps** - Links to advanced topics

Requirements:
- Beginner-friendly language
- Complete, working examples
- Explain Odibi philosophy
- Include CLI commands
"""

        context = AgentContext(query=prompt, retrieved_chunks=chunks)
        return self.process(context)
