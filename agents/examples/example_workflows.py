"""Example workflows demonstrating the Odibi AI Agent Suite.

These examples show how to use the multi-agent system for common tasks.
"""

import os
from typing import Optional

from odibi.agents.core.azure_client import AzureConfig
from odibi.agents.core.agent_base import AgentRole
from odibi.agents.pipelines.agent_runner import AgentRunner


def get_config() -> Optional[AzureConfig]:
    """Get Azure configuration from environment variables."""
    openai_key = os.getenv("AZURE_OPENAI_API_KEY")
    search_key = os.getenv("AZURE_SEARCH_API_KEY")

    if not openai_key or not search_key:
        print("Error: Set AZURE_OPENAI_API_KEY and AZURE_SEARCH_API_KEY")
        return None

    return AzureConfig(
        openai_api_key=openai_key,
        search_api_key=search_key,
    )


# =============================================================================
# EXAMPLE 1: Audit a Transformer
# =============================================================================


def audit_derive_columns_transformer():
    """
    Audit the derive_columns transformer.

    This workflow:
    1. Analyzes the transformer implementation
    2. Suggests improvements
    3. Generates comprehensive tests
    4. Creates documentation
    """
    config = get_config()
    if not config:
        return

    runner = AgentRunner(config)
    result = runner.audit_transformer("derive_columns")

    print("=" * 80)
    print("AUDIT REPORT: derive_columns Transformer")
    print("=" * 80)

    print("\n## CODE ANALYSIS")
    print("-" * 40)
    print(result["analysis"])

    print("\n## IMPROVEMENT SUGGESTIONS")
    print("-" * 40)
    print(result["improvements"])

    print("\n## GENERATED TESTS")
    print("-" * 40)
    print(result["tests"])

    print("\n## DOCUMENTATION")
    print("-" * 40)
    print(result["documentation"])


# =============================================================================
# EXAMPLE 2: Generate Tests for All SCD Transformations
# =============================================================================


def generate_scd_tests():
    """
    Generate comprehensive tests for SCD transformations.

    Tests cover:
    - SCD Type 1 (overwrite)
    - SCD Type 2 (history preservation)
    - SCD Type 4 (hybrid)
    - Edge cases and error conditions
    """
    config = get_config()
    if not config:
        return

    runner = AgentRunner(config)

    test_code = runner.ask(
        """
        Generate a complete pytest test suite for all SCD transformations in Odibi.

        Include tests for:
        1. SCD Type 1 - Simple overwrite
           - New records insertion
           - Existing record updates
           - No-change scenarios

        2. SCD Type 2 - Historical tracking
           - New records with effective dates
           - Change detection and history creation
           - is_current flag management
           - effective_to date closing

        3. Edge Cases
           - Empty source DataFrame
           - Empty target DataFrame
           - All records changed
           - Null values in key columns
           - Duplicate keys

        Use realistic test data with customers, orders, or products.
        Include detailed assertions and docstrings.
        """,
        agent_role=AgentRole.TEST_ARCHITECT,
    )

    print("=" * 80)
    print("GENERATED SCD TEST SUITE")
    print("=" * 80)
    print(test_code.content)


# =============================================================================
# EXAMPLE 3: Explain Dependency Resolution in SparkWorkflowNode
# =============================================================================


def explain_dependency_resolution():
    """
    Explain how dependency resolution works in Odibi's DependencyGraph.

    This covers:
    - Graph construction
    - Topological sorting (Kahn's algorithm)
    - Execution layer calculation
    - Cycle detection
    """
    config = get_config()
    if not config:
        return

    runner = AgentRunner(config)

    explanation = runner.ask(
        """
        Provide a detailed explanation of dependency resolution in Odibi.

        Cover:
        1. How DependencyGraph is constructed from NodeConfig objects
        2. How topological_sort() works using Kahn's algorithm
        3. How get_execution_layers() groups nodes for parallel execution
        4. How cycles are detected and reported
        5. How missing dependencies are handled

        Include code examples showing the key algorithms.
        Reference specific file paths and line numbers.
        """,
        agent_role=AgentRole.CODE_ANALYST,
    )

    print("=" * 80)
    print("DEPENDENCY RESOLUTION EXPLAINED")
    print("=" * 80)
    print(explanation.content)


# =============================================================================
# EXAMPLE 4: Document the Config Schema Automatically
# =============================================================================


def document_config_schema():
    """
    Automatically generate YAML schema documentation for Odibi configs.

    Documents:
    - PipelineConfig
    - NodeConfig
    - ReadConfig / WriteConfig
    - TransformConfig
    - All validation rules
    """
    config = get_config()
    if not config:
        return

    runner = AgentRunner(config)

    documentation = runner.ask(
        """
        Generate comprehensive YAML schema documentation for Odibi.

        Document each config model with:
        1. Full example YAML
        2. Field reference table (name, type, required, default, description)
        3. Validation rules and constraints
        4. Common configurations
        5. Troubleshooting tips

        Cover these models:
        - PipelineConfig (top-level pipeline configuration)
        - NodeConfig (individual node configuration)
        - ReadConfig (data source configuration)
        - WriteConfig (data destination configuration)
        - TransformConfig (transformation steps)
        - ValidationConfig (data validation rules)
        - IncrementalConfig (incremental/HWM processing)

        Format for mkdocs with tables and code blocks.
        """,
        agent_role=AgentRole.DOCUMENTATION,
    )

    print("=" * 80)
    print("ODIBI YAML SCHEMA DOCUMENTATION")
    print("=" * 80)
    print(documentation.content)


# =============================================================================
# EXAMPLE 5: Multi-Agent Workflow - Implement New Transformer
# =============================================================================


def guide_new_transformer_implementation():
    """
    Multi-agent workflow to help implement a new transformer.

    Uses multiple agents to:
    1. Explain existing transformer patterns
    2. Suggest implementation approach
    3. Generate tests
    4. Create documentation
    """
    config = get_config()
    if not config:
        return

    runner = AgentRunner(config)

    workflow = [
        {
            "query": """
            Analyze the existing transformer patterns in Odibi.
            Show how transformers are:
            1. Registered with @transform decorator
            2. Structured (function signature, parameters)
            3. Invoked during node execution
            4. Engine-agnostic (work with Pandas and Spark)
            """,
            "agent": "code_analyst",
        },
        {
            "query": """
            I want to create a new 'deduplicate' transformer that:
            - Takes a DataFrame and a list of columns
            - Removes duplicate rows based on those columns
            - Keeps the first or last occurrence (configurable)
            - Logs how many duplicates were removed

            Suggest the best implementation approach following Odibi patterns.
            Include complete code.
            """,
            "agent": "refactor_engineer",
        },
        {
            "query": """
            Generate comprehensive tests for the new 'deduplicate' transformer.
            Test:
            - Single column deduplication
            - Multi-column deduplication
            - keep='first' vs keep='last'
            - No duplicates scenario
            - All duplicates scenario
            - Null handling
            """,
            "agent": "test_architect",
        },
        {
            "query": """
            Document the new 'deduplicate' transformer.
            Include:
            - YAML usage example
            - Python usage example
            - Parameter descriptions
            - Use cases and best practices
            """,
            "agent": "documentation",
        },
    ]

    print("=" * 80)
    print("NEW TRANSFORMER IMPLEMENTATION GUIDE")
    print("=" * 80)

    responses = runner.run_workflow(workflow)

    sections = [
        "1. EXISTING PATTERNS",
        "2. IMPLEMENTATION",
        "3. TESTS",
        "4. DOCUMENTATION",
    ]

    for section, response in zip(sections, responses):
        print(f"\n{'='*60}")
        print(section)
        print("=" * 60)
        print(response.content)


# =============================================================================
# EXAMPLE 6: Code Review Workflow
# =============================================================================


def review_node_module():
    """
    Comprehensive code review of the node.py module.

    Combines:
    - Code analysis
    - Refactoring suggestions
    - Test coverage analysis
    """
    config = get_config()
    if not config:
        return

    runner = AgentRunner(config)

    response = runner.ask(
        """
        Perform a comprehensive review of odibi/node.py.

        Include:
        1. Architecture overview
        2. Key classes and their responsibilities
        3. Potential improvements
        4. Missing test coverage
        5. Documentation gaps

        Be specific with file paths and line numbers.
        """,
    )

    print("=" * 80)
    print("NODE.PY CODE REVIEW")
    print("=" * 80)
    print(response.content)


# =============================================================================
# MAIN: Run Examples
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Odibi Agent Suite examples")
    parser.add_argument(
        "--example",
        choices=[
            "audit",
            "scd-tests",
            "dependency",
            "config-docs",
            "new-transformer",
            "review",
        ],
        default="audit",
        help="Which example to run",
    )

    args = parser.parse_args()

    examples = {
        "audit": audit_derive_columns_transformer,
        "scd-tests": generate_scd_tests,
        "dependency": explain_dependency_resolution,
        "config-docs": document_config_schema,
        "new-transformer": guide_new_transformer_implementation,
        "review": review_node_module,
    }

    examples[args.example]()
