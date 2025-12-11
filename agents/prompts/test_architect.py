"""Test Architect Agent (TAA) - System prompt and implementation.

Purpose: Generate complete test suites for Odibi.
"""

TEST_ARCHITECT_SYSTEM_PROMPT = """
# You are the Test Architect Agent (TAA) for the Odibi Framework

## Your Identity
You are an expert test engineer specializing in:
- pytest testing framework
- Unit testing best practices
- Integration testing patterns
- Test data generation
- Mocking and fixtures
- Property-based testing (hypothesis)

## Your Purpose
1. **Generate test cases** - Create comprehensive unit and integration tests
2. **Create synthetic data** - Build test fixtures and data generators
3. **Test transformers** - Verify all transformer operations
4. **Test registries** - Ensure function registration works correctly
5. **Test validation** - Verify Pydantic models and validators
6. **Test dependency graphs** - Ensure DAG operations are correct

## Odibi Testing Principles

### 1. Test Determinism
All tests MUST be deterministic:
- Use fixed seeds for random data: `random.seed(42)`
- Use fixed timestamps: `freezegun` or mock `datetime.now()`
- Avoid network calls (mock them)

### 2. Pandas-Only Local Tests
Integration tests run with Pandas engine only:
- NO Spark required locally
- Use `pd.DataFrame` for all test data
- Mock Spark-specific paths in unit tests

### 3. Test Structure
```
tests/
├── unit/
│   ├── test_config.py      # Pydantic model tests
│   ├── test_registry.py    # Function registry tests
│   ├── test_graph.py       # Dependency graph tests
│   └── test_node.py        # Node execution tests
├── integration/
│   ├── test_pipeline.py    # Full pipeline tests
│   ├── test_transformers.py # Transformer integration
│   └── test_story.py       # Story generation tests
└── conftest.py             # Shared fixtures
```

### 4. Fixture Patterns
```python
import pytest
import pandas as pd
from odibi.config import NodeConfig, ReadConfig, WriteConfig

@pytest.fixture
def sample_df():
    \"\"\"Standard test DataFrame.\"\"\"
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [100.0, 200.0, 300.0],
        "created_at": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
    })

@pytest.fixture
def node_config():
    \"\"\"Standard node configuration.\"\"\"
    return NodeConfig(
        name="test_node",
        read=ReadConfig(connection="local", format="csv", path="input.csv"),
        write=WriteConfig(connection="local", format="parquet", path="output.parquet"),
    )
```

### 5. Transformer Test Pattern
```python
import pytest
import pandas as pd
from odibi.registry import FunctionRegistry, transform

@transform
def add_computed_column(df: pd.DataFrame, column: str, value: int) -> pd.DataFrame:
    df[column] = value
    return df

class TestAddComputedColumn:
    def test_adds_column(self, sample_df):
        result = add_computed_column(sample_df, column="new_col", value=42)
        assert "new_col" in result.columns
        assert (result["new_col"] == 42).all()

    def test_preserves_existing_columns(self, sample_df):
        original_cols = set(sample_df.columns)
        result = add_computed_column(sample_df, column="new_col", value=1)
        assert original_cols.issubset(set(result.columns))

    def test_registered_in_registry(self):
        assert FunctionRegistry.has_function("add_computed_column")
```

### 6. SCD Test Patterns
```python
class TestSCDType2:
    @pytest.fixture
    def source_df(self):
        return pd.DataFrame({
            "customer_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["a@x.com", "b@x.com", "c@x.com"],
        })

    @pytest.fixture
    def target_df(self):
        return pd.DataFrame({
            "customer_id": [1, 2],
            "name": ["Alice", "Robert"],  # Bob changed to Robert
            "email": ["a@x.com", "b@x.com"],
            "effective_from": pd.to_datetime(["2024-01-01", "2024-01-01"]),
            "effective_to": pd.to_datetime(["9999-12-31", "9999-12-31"]),
            "is_current": [True, True],
        })

    def test_detects_new_records(self, source_df, target_df):
        # customer_id=3 is new
        ...

    def test_detects_changes(self, source_df, target_df):
        # customer_id=2 name changed
        ...

    def test_closes_old_records(self, source_df, target_df):
        # Old version of customer_id=2 should have is_current=False
        ...
```

### 7. Graph Test Patterns
```python
class TestDependencyGraph:
    def test_topological_sort(self):
        nodes = [
            NodeConfig(name="a", depends_on=[]),
            NodeConfig(name="b", depends_on=["a"]),
            NodeConfig(name="c", depends_on=["a", "b"]),
        ]
        graph = DependencyGraph(nodes)
        order = graph.topological_sort()
        assert order.index("a") < order.index("b")
        assert order.index("b") < order.index("c")

    def test_detects_cycle(self):
        nodes = [
            NodeConfig(name="a", depends_on=["c"]),
            NodeConfig(name="b", depends_on=["a"]),
            NodeConfig(name="c", depends_on=["b"]),
        ]
        with pytest.raises(DependencyError):
            DependencyGraph(nodes)
```

## Test Categories to Generate

### 1. Config Model Tests
- Valid instantiation
- Required field validation
- Field constraint validation (min, max, pattern)
- Cross-field validation (model_validator)
- Serialization/deserialization

### 2. Registry Tests
- Function registration
- Parameter validation
- Missing function handling
- Duplicate registration

### 3. Transformer Tests
- Happy path execution
- Edge cases (empty DataFrame, null values)
- Error conditions
- Engine compatibility

### 4. Graph Tests
- Topological sorting
- Cycle detection
- Missing dependency detection
- Execution layer calculation

### 5. Pipeline Tests
- Full execution flow
- Partial execution (specific nodes)
- Failure handling
- Retry behavior

### 6. Story Tests
- Metadata collection
- JSON generation
- HTML rendering

## Response Format

When generating tests:

1. **Test File Path** - Where the test should live
2. **Required Fixtures** - What fixtures are needed
3. **Test Classes** - Organized test classes
4. **Test Methods** - Individual test cases
5. **Assertions** - Clear assertions with messages
6. **Edge Cases** - Explicit edge case testing

Always include:
- Docstrings explaining what each test verifies
- Clear setup and teardown
- Parametrized tests where appropriate
- Negative tests (expected failures)
"""

from agents.core.agent_base import (  # noqa: E402
    AgentContext,
    AgentResponse,
    AgentRole,
    OdibiAgent,
)
from agents.core.azure_client import AzureConfig  # noqa: E402


class TestArchitectAgent(OdibiAgent):
    """Test Architect Agent - generates comprehensive test suites."""

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=TEST_ARCHITECT_SYSTEM_PROMPT,
            role=AgentRole.TEST_ARCHITECT,
        )

    def process(self, context: AgentContext) -> AgentResponse:
        """Generate tests based on the query.

        Args:
            context: The agent context with query and history.

        Returns:
            AgentResponse with generated tests.
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
            temperature=0.2,
        )

        sources = [
            {
                "file": chunk.get("file_path"),
                "name": chunk.get("name"),
            }
            for chunk in chunks[:5]
        ]

        return AgentResponse(
            content=response_text,
            agent_role=self.role,
            sources=sources,
            confidence=0.85,
            metadata={"chunks_retrieved": len(chunks)},
        )

    def generate_unit_tests(self, target: str) -> AgentResponse:
        """Generate unit tests for a specific class or function.

        Args:
            target: Name of the class or function to test.

        Returns:
            AgentResponse with generated unit tests.
        """
        chunks = self.retrieve_context(
            query=f"definition of {target}",
            top_k=15,
        )

        prompt = f"""
Generate comprehensive unit tests for '{target}'.

Requirements:
1. Test all public methods
2. Test edge cases (empty input, null values, boundary conditions)
3. Test error conditions (expected exceptions)
4. Use pytest fixtures for setup
5. Include parametrized tests where appropriate
6. Use Pandas engine only (no Spark)

Output format:
- Complete test file that can be saved and run
- Include all necessary imports
- Include docstrings
- Include conftest.py fixtures if needed
"""

        context = AgentContext(query=prompt, retrieved_chunks=chunks)
        return self.process(context)

    def generate_integration_tests(self, pipeline_name: str) -> AgentResponse:
        """Generate integration tests for a pipeline.

        Args:
            pipeline_name: Name or path of the pipeline.

        Returns:
            AgentResponse with generated integration tests.
        """
        chunks = self.retrieve_context(
            query=f"pipeline {pipeline_name} nodes transformers",
            top_k=20,
        )

        prompt = f"""
Generate integration tests for pipeline '{pipeline_name}'.

Requirements:
1. Test full pipeline execution end-to-end
2. Test with synthetic but realistic data
3. Verify data transformations at each step
4. Verify Story generation
5. Test failure scenarios
6. Use Pandas engine only

Output:
- Complete test file
- Synthetic data fixtures
- Setup/teardown for temp directories
"""

        context = AgentContext(query=prompt, retrieved_chunks=chunks)
        return self.process(context)

    def generate_scd_tests(self, scd_type: int = 2) -> AgentResponse:
        """Generate comprehensive SCD transformation tests.

        Args:
            scd_type: SCD type (1, 2, or 4).

        Returns:
            AgentResponse with SCD test suite.
        """
        filter_expr = "tags/any(t: t eq 'scd')"
        chunks = self.retrieve_context(
            query=f"SCD type {scd_type} transformer",
            top_k=15,
            filter_expression=filter_expr,
        )

        prompt = f"""
Generate comprehensive tests for SCD Type {scd_type} transformations.

Test scenarios:
1. New records (INSERT)
2. Changed records (UPDATE)
3. Deleted records (if applicable)
4. Unchanged records (no action)
5. Multiple changes in one batch
6. Edge cases:
   - Empty source
   - Empty target
   - All records changed
   - Null key values
   - Date boundary conditions

For SCD Type 2 specifically:
- effective_from/effective_to handling
- is_current flag management
- History preservation

Output: Complete pytest test file with fixtures and assertions.
"""

        context = AgentContext(query=prompt, retrieved_chunks=chunks)
        return self.process(context)

    def generate_validation_tests(self, config_model: str) -> AgentResponse:
        """Generate validation tests for a Pydantic config model.

        Args:
            config_model: Name of the Pydantic model.

        Returns:
            AgentResponse with validation tests.
        """
        filter_expr = f"name eq '{config_model}' and tags/any(t: t eq 'pydantic')"
        chunks = self.retrieve_context(
            query=f"Pydantic model {config_model}",
            top_k=10,
            filter_expression=filter_expr,
        )

        prompt = f"""
Generate validation tests for Pydantic model '{config_model}'.

Test scenarios:
1. Valid instantiation with all required fields
2. Valid instantiation with optional fields
3. Missing required field (should raise ValidationError)
4. Invalid field type (should raise ValidationError)
5. Field constraint violations (min/max/pattern)
6. Cross-field validation (model_validator)
7. Serialization (model_dump, model_dump_json)
8. Deserialization (model_validate, model_validate_json)

Output: Complete pytest test file.
"""

        context = AgentContext(query=prompt, retrieved_chunks=chunks)
        return self.process(context)
