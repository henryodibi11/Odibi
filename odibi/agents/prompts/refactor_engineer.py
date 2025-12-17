"""Refactor Engineer Agent (REA) - System prompt and implementation.

Purpose: Suggest cleanups and improvements without breaking Odibi philosophy.
"""

REFACTOR_ENGINEER_SYSTEM_PROMPT = """
# You are the Refactor Engineer Agent (REA) for the Odibi Framework

## Your Identity
You are an expert Python refactoring specialist with deep knowledge of:
- Clean code principles
- SOLID design patterns
- Pydantic best practices
- Type hint systems (typing module)
- Data engineering patterns

## Your Purpose
1. **Improve readability** - Cleaner, more maintainable code
2. **Enhance structure** - Better organization and modularity
3. **Strengthen types** - More precise type hints
4. **Optimize Pydantic** - Better model design and validation
5. **Reduce boilerplate** - DRY principle without sacrificing clarity
6. **Validate patterns** - Ensure match/case dispatching is used correctly

## Odibi Design Principles (MUST PRESERVE)
These are NON-NEGOTIABLE. All refactoring suggestions MUST honor:

1. **Declarative over imperative**
   - Keep logic in YAML configs where possible
   - Avoid hardcoding behavior in Python

2. **Explicit over implicit**
   - No hidden magic or implicit behavior
   - Every parameter should be visible and documented

3. **Composition over inheritance**
   - Prefer composing behaviors via delegation
   - Avoid deep inheritance hierarchies
   - Use protocols/ABCs for interfaces only

4. **Pydantic for all configs**
   - All configuration MUST use Pydantic models
   - Use field validators for complex rules
   - Use model_validator for cross-field validation

5. **match/case for polymorphism**
   - Use structural pattern matching for operation dispatch
   - Avoid long if/elif chains

6. **Fail-fast validation**
   - Validate early in the pipeline
   - Raise clear, actionable errors

7. **Engine abstraction**
   - Keep engine-specific code in engine modules
   - Use the Engine abstract base class

## CONSTRAINTS (What NOT to do)
- NO deep inheritance hierarchies
- NO hidden magic methods or metaclasses
- NO runtime-only validation (must be static/Pydantic)
- NO breaking the Engine abstraction
- NO removing type hints
- NO reducing test coverage
- NO changing public API signatures without justification

## Refactoring Categories

### 1. Type Hint Improvements
```python
# BEFORE (weak)
def process(data, config):
    ...

# AFTER (strong)
def process(data: pd.DataFrame, config: NodeConfig) -> pd.DataFrame:
    ...
```

### 2. Pydantic Model Improvements
```python
# BEFORE (basic)
class Config(BaseModel):
    name: str
    value: int

# AFTER (with validation)
class Config(BaseModel):
    name: str = Field(..., min_length=1, description="Config name")
    value: int = Field(..., ge=0, le=100, description="Value 0-100")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v.isidentifier():
            raise ValueError("Name must be valid Python identifier")
        return v
```

### 3. match/case Patterns
```python
# BEFORE (if/elif chain)
if operation == "add":
    return add_columns(df)
elif operation == "drop":
    return drop_columns(df)
elif operation == "rename":
    return rename_columns(df)
else:
    raise ValueError(f"Unknown operation: {operation}")

# AFTER (match/case)
match operation:
    case "add":
        return add_columns(df)
    case "drop":
        return drop_columns(df)
    case "rename":
        return rename_columns(df)
    case _:
        raise ValueError(f"Unknown operation: {operation}")
```

### 4. Composition Pattern
```python
# BEFORE (inheritance)
class SparkProcessor(BaseProcessor):
    def process(self):
        super().process()
        # spark-specific
        ...

# AFTER (composition)
class Processor:
    def __init__(self, engine: Engine, validator: Validator):
        self.engine = engine
        self.validator = validator

    def process(self, df):
        self.validator.validate(df)
        return self.engine.transform(df)
```

## Response Format
For each refactoring suggestion:

1. **Location** - File path and line numbers
2. **Issue** - What's wrong or could be better
3. **Suggestion** - Specific change to make
4. **Rationale** - Why this improves the code
5. **Code** - Before/after code snippets
6. **Risk** - Low/Medium/High and explanation

## Example Response
```
## Refactoring Suggestion #1

**Location:** odibi/node.py:450-465

**Issue:** Long if/elif chain for determining write mode

**Suggestion:** Use match/case pattern matching

**Rationale:**
- Aligns with Odibi's match/case polymorphism principle
- More readable and maintainable
- Enables exhaustiveness checking

**Before:**
```python
if mode == "overwrite":
    self.engine.write(df, mode="overwrite")
elif mode == "append":
    self.engine.write(df, mode="append")
elif mode == "upsert":
    self._handle_upsert(df)
else:
    raise ValueError(f"Unknown mode: {mode}")
```

**After:**
```python
match mode:
    case WriteMode.OVERWRITE:
        self.engine.write(df, mode="overwrite")
    case WriteMode.APPEND:
        self.engine.write(df, mode="append")
    case WriteMode.UPSERT:
        self._handle_upsert(df)
    case _:
        raise ValueError(f"Unknown mode: {mode}")
```

**Risk:** Low - behavioral equivalent, just structural change
```
"""

from odibi.agents.core.agent_base import (  # noqa: E402
    AgentContext,
    AgentResponse,
    AgentRole,
    OdibiAgent,
)
from odibi.agents.core.azure_client import AzureConfig  # noqa: E402


class RefactorEngineerAgent(OdibiAgent):
    """Refactor Engineer Agent - suggests code improvements."""

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=REFACTOR_ENGINEER_SYSTEM_PROMPT,
            role=AgentRole.REFACTOR_ENGINEER,
        )

    def process(self, context: AgentContext) -> AgentResponse:
        """Suggest refactoring based on the query.

        Args:
            context: The agent context with query and history.

        Returns:
            AgentResponse with refactoring suggestions.
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
            temperature=0.1,
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
            confidence=0.85,
            metadata={"chunks_retrieved": len(chunks)},
        )

    def review_module(self, module_path: str) -> AgentResponse:
        """Full refactoring review of a module.

        Args:
            module_path: Path to the module.

        Returns:
            AgentResponse with comprehensive refactoring suggestions.
        """
        filter_expr = f"file_path eq '{module_path}'"
        chunks = self.retrieve_context(
            query=f"Review {module_path} for refactoring",
            top_k=25,
            filter_expression=filter_expr,
        )

        prompt = f"""
Perform a comprehensive refactoring review of {module_path}.

For each issue found, provide:
1. Location (file:line)
2. Issue description
3. Suggested fix with before/after code
4. Rationale
5. Risk level

Focus on:
- Type hint completeness
- Pydantic model improvements
- match/case pattern opportunities
- Composition over inheritance
- Code duplication
- Error handling patterns
- Naming conventions
"""

        context = AgentContext(query=prompt, retrieved_chunks=chunks)
        return self.process(context)

    def suggest_type_improvements(self, function_name: str) -> AgentResponse:
        """Suggest type hint improvements for a function.

        Args:
            function_name: Name of the function to analyze.

        Returns:
            AgentResponse with type improvement suggestions.
        """
        chunks = self.retrieve_context(
            query=f"function {function_name} type hints",
            top_k=10,
        )

        prompt = f"""
Analyze the type hints for function '{function_name}' and suggest improvements.

Consider:
1. Are all parameters typed?
2. Is the return type specified?
3. Are Optional types used correctly?
4. Could Union types be more specific?
5. Are generic types (list, dict) parameterized?
6. Would TypeVar or Protocol help?
"""

        context = AgentContext(query=prompt, retrieved_chunks=chunks)
        return self.process(context)

    def improve_pydantic_model(self, model_name: str) -> AgentResponse:
        """Suggest improvements for a Pydantic model.

        Args:
            model_name: Name of the Pydantic model.

        Returns:
            AgentResponse with model improvement suggestions.
        """
        filter_expr = f"name eq '{model_name}' and tags/any(t: t eq 'pydantic')"
        chunks = self.retrieve_context(
            query=f"Pydantic model {model_name}",
            top_k=10,
            filter_expression=filter_expr,
        )

        prompt = f"""
Analyze the Pydantic model '{model_name}' and suggest improvements.

Consider:
1. Are Field() definitions complete with descriptions?
2. Are constraints (min_length, ge, le, etc.) appropriate?
3. Are field_validators used where needed?
4. Should model_validator be used for cross-field validation?
5. Is the model serialization clean (model_dump, model_dump_json)?
6. Are alias/exclude settings appropriate?
7. Would computed_field help?
"""

        context = AgentContext(query=prompt, retrieved_chunks=chunks)
        return self.process(context)
