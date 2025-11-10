# Phase 3A: Foundation

**Duration:** 4 weeks  
**Prerequisites:** Phase 2.5 complete  
**Goal:** Build transformation registry, explanation system, and quality enforcement

---

## Overview

Phase 3A creates the foundation for self-documenting pipelines by:
1. Building a transformation registry
2. Enabling transformations to explain themselves
3. Enforcing documentation quality
4. Implementing built-in operations

**Principle:** Build the infrastructure that enforces transparency.

---

## Week 1: Transformation Registry

### Goals
- Create transformation registry
- Implement `@transformation` decorator
- Enable function registration and lookup
- Test thoroughly before moving forward

### Day 1-2: Base Registry Implementation

**File:** `odibi/transformations/registry.py`

```python
"""
Transformation Registry
=======================

Global registry for all transformations (built-in and user-defined).
"""
from typing import Dict, Callable, Optional, Any
import functools


class TransformationRegistry:
    """
    Central registry for all transformations.
    
    The registry stores transformation functions and enables lookup by name.
    It maintains metadata about each transformation including version,
    documentation, and explanation methods.
    """
    
    def __init__(self):
        self._transformations: Dict[str, Callable] = {}
        self._metadata: Dict[str, Dict[str, Any]] = {}
    
    def register(
        self,
        name: str,
        func: Callable,
        version: str = "1.0.0",
        category: Optional[str] = None,
        tags: Optional[list] = None
    ) -> Callable:
        """
        Register a transformation function.
        
        Args:
            name: Unique name for the transformation
            func: The transformation function
            version: Semantic version (default: "1.0.0")
            category: Optional category (e.g., "aggregation", "filtering")
            tags: Optional list of tags for discovery
        
        Returns:
            The registered function (for decorator chaining)
        
        Raises:
            ValueError: If name already registered
        """
        if name in self._transformations:
            raise ValueError(
                f"Transformation '{name}' already registered. "
                f"Use a unique name or unregister the existing transformation."
            )
        
        # Validate function has docstring
        if not func.__doc__ or len(func.__doc__.strip()) < 10:
            raise ValueError(
                f"Transformation '{name}' must have a docstring (minimum 10 characters).\n"
                f"Documentation is mandatory in Odibi."
            )
        
        self._transformations[name] = func
        self._metadata[name] = {
            'version': version,
            'category': category,
            'tags': tags or [],
            'docstring': func.__doc__
        }
        
        return func
    
    def get(self, name: str) -> Optional[Callable]:
        """
        Get a transformation by name.
        
        Args:
            name: Transformation name
        
        Returns:
            The transformation function, or None if not found
        """
        return self._transformations.get(name)
    
    def get_metadata(self, name: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a transformation."""
        return self._metadata.get(name)
    
    def list_all(self) -> Dict[str, Dict[str, Any]]:
        """List all registered transformations with metadata."""
        return {
            name: {
                'func': func,
                **self._metadata[name]
            }
            for name, func in self._transformations.items()
        }
    
    def unregister(self, name: str) -> bool:
        """
        Unregister a transformation.
        
        Args:
            name: Transformation name
        
        Returns:
            True if unregistered, False if not found
        """
        if name in self._transformations:
            del self._transformations[name]
            del self._metadata[name]
            return True
        return False
    
    def clear(self):
        """Clear all registrations (primarily for testing)."""
        self._transformations.clear()
        self._metadata.clear()


# Global registry instance
_global_registry = TransformationRegistry()


def get_registry() -> TransformationRegistry:
    """Get the global transformation registry."""
    return _global_registry
```

**File:** `odibi/transformations/decorators.py`

```python
"""
Transformation Decorators
=========================

Decorators for registering transformations.
"""
from typing import Callable, Optional
from .registry import get_registry


def transformation(
    name: str,
    version: str = "1.0.0",
    category: Optional[str] = None,
    tags: Optional[list] = None
):
    """
    Decorator to register a transformation.
    
    Usage:
        @transformation("my_transform", category="filtering")
        def my_transform(df, threshold):
            '''Filter records above threshold.'''
            return df[df.value > threshold]
    
    Args:
        name: Unique name for transformation
        version: Semantic version
        category: Optional category
        tags: Optional tags for discovery
    
    Returns:
        Decorated function (registered in global registry)
    """
    def decorator(func: Callable) -> Callable:
        registry = get_registry()
        registry.register(
            name=name,
            func=func,
            version=version,
            category=category,
            tags=tags
        )
        return func
    
    return decorator
```

**File:** `odibi/transformations/__init__.py`

```python
"""
Transformations Module
=====================

This module provides the transformation registry and decorators for
defining custom data transformations.

Example:
    from odibi import transformation
    
    @transformation("filter_high_values")
    def filter_high_values(df, threshold=100):
        '''Filter records above threshold.'''
        return df[df.value > threshold]
"""
from .registry import TransformationRegistry, get_registry
from .decorators import transformation

__all__ = [
    'TransformationRegistry',
    'get_registry',
    'transformation'
]
__version__ = "1.3.0-alpha.1"
```

**Tests:** `tests/unit/test_registry.py`

```python
"""Tests for transformation registry."""
import pytest
from odibi.transformations import transformation, get_registry


class TestTransformationRegistry:
    """Tests for TransformationRegistry class."""
    
    def setup_method(self):
        """Clear registry before each test."""
        get_registry().clear()
    
    def test_register_transformation(self):
        """Should register transformation successfully."""
        @transformation("test_transform")
        def test_transform(df):
            """Test transformation."""
            return df
        
        registry = get_registry()
        assert registry.get("test_transform") == test_transform
    
    def test_register_requires_docstring(self):
        """Should reject transformations without docstring."""
        with pytest.raises(ValueError, match="must have a docstring"):
            @transformation("no_docstring")
            def no_docstring(df):
                return df
    
    def test_register_duplicate_name_fails(self):
        """Should reject duplicate transformation names."""
        @transformation("duplicate")
        def first(df):
            """First function."""
            return df
        
        with pytest.raises(ValueError, match="already registered"):
            @transformation("duplicate")
            def second(df):
                """Second function."""
                return df
    
    def test_get_nonexistent_returns_none(self):
        """Should return None for unregistered transformation."""
        registry = get_registry()
        assert registry.get("nonexistent") is None
    
    def test_metadata_stored(self):
        """Should store metadata with transformation."""
        @transformation("with_metadata", version="2.0.0", category="test", tags=["example"])
        def with_metadata(df):
            """Test function with metadata."""
            return df
        
        registry = get_registry()
        metadata = registry.get_metadata("with_metadata")
        
        assert metadata['version'] == "2.0.0"
        assert metadata['category'] == "test"
        assert "example" in metadata['tags']
        assert "Test function" in metadata['docstring']
    
    def test_list_all_transformations(self):
        """Should list all registered transformations."""
        @transformation("transform1")
        def transform1(df):
            """First transformation."""
            return df
        
        @transformation("transform2")
        def transform2(df):
            """Second transformation."""
            return df
        
        registry = get_registry()
        all_transforms = registry.list_all()
        
        assert len(all_transforms) == 2
        assert "transform1" in all_transforms
        assert "transform2" in all_transforms
    
    def test_unregister_transformation(self):
        """Should unregister transformation."""
        @transformation("to_unregister")
        def to_unregister(df):
            """Will be unregistered."""
            return df
        
        registry = get_registry()
        assert registry.get("to_unregister") is not None
        
        success = registry.unregister("to_unregister")
        assert success is True
        assert registry.get("to_unregister") is None
    
    def test_unregister_nonexistent_returns_false(self):
        """Should return False when unregistering nonexistent transformation."""
        registry = get_registry()
        success = registry.unregister("nonexistent")
        assert success is False
```

**Deliverables:**
- TransformationRegistry class implemented
- @transformation decorator working
- 10+ tests passing
- All tests green

**Testing:**
```bash
pytest tests/unit/test_registry.py -v
pytest -v  # All tests should still pass
```

---

### Day 3-4: Context Passing System

**Goal:** Enable transformations to receive pipeline metadata (plant, asset, etc.)

**File:** `odibi/transformations/context.py`

```python
"""
Context Passing System
======================

Enables transformations to receive pipeline-level metadata.
"""
from typing import Dict, Any, Optional
from dataclasses import dataclass, field


@dataclass
class TransformationContext:
    """
    Context passed to transformation functions and explain() methods.
    
    Contains:
    - Node parameters (from YAML)
    - Pipeline metadata (plant, asset, layer, etc.)
    - Project metadata (project name, business unit, etc.)
    - Runtime information (timestamps, etc.)
    """
    
    # Node-level
    node_name: str
    operation_name: str
    params: Dict[str, Any] = field(default_factory=dict)
    
    # Pipeline-level
    pipeline_name: Optional[str] = None
    layer: Optional[str] = None
    
    # Project-level (from top of YAML)
    project: Optional[str] = None
    plant: Optional[str] = None
    asset: Optional[str] = None
    business_unit: Optional[str] = None
    
    # Runtime
    environment: str = "development"
    
    # Additional metadata
    extra: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for **kwargs passing."""
        return {
            'node': self.node_name,
            'operation': self.operation_name,
            'params': self.params,
            'pipeline': self.pipeline_name,
            'layer': self.layer,
            'project': self.project,
            'plant': self.plant,
            'asset': self.asset,
            'business_unit': self.business_unit,
            'environment': self.environment,
            **self.extra
        }


def build_context_from_config(
    node_config: Dict[str, Any],
    pipeline_config: Dict[str, Any],
    project_config: Dict[str, Any]
) -> TransformationContext:
    """
    Build TransformationContext from config dictionaries.
    
    Args:
        node_config: Node configuration (operation, params, etc.)
        pipeline_config: Pipeline configuration
        project_config: Project-level configuration
    
    Returns:
        TransformationContext instance
    """
    return TransformationContext(
        node_name=node_config.get('node', 'unnamed'),
        operation_name=node_config.get('operation', 'unknown'),
        params=node_config.get('params', {}),
        pipeline_name=pipeline_config.get('name'),
        layer=pipeline_config.get('layer'),
        project=project_config.get('project'),
        plant=project_config.get('plant'),
        asset=project_config.get('asset'),
        business_unit=project_config.get('business_unit'),
        environment=project_config.get('environment', 'development'),
        extra=project_config.get('metadata', {})
    )
```

**Update:** `odibi/transformations/__init__.py`

```python
# Add context exports
from .context import TransformationContext, build_context_from_config

__all__ = [
    'TransformationRegistry',
    'get_registry',
    'transformation',
    'TransformationContext',
    'build_context_from_config'
]
```

**Tests:** `tests/unit/test_context.py`

```python
"""Tests for context passing system."""
import pytest
from odibi.transformations import TransformationContext, build_context_from_config


def test_context_creation():
    """Should create context with required fields."""
    ctx = TransformationContext(
        node_name="test_node",
        operation_name="test_op"
    )
    
    assert ctx.node_name == "test_node"
    assert ctx.operation_name == "test_op"
    assert ctx.params == {}


def test_context_to_dict():
    """Should convert context to dictionary."""
    ctx = TransformationContext(
        node_name="test_node",
        operation_name="test_op",
        plant="NKC",
        asset="Germ Dryer 1"
    )
    
    d = ctx.to_dict()
    assert d['node'] == "test_node"
    assert d['plant'] == "NKC"
    assert d['asset'] == "Germ Dryer 1"


def test_build_context_from_config():
    """Should build context from config dictionaries."""
    node_config = {
        'node': 'my_node',
        'operation': 'my_op',
        'params': {'threshold': 100}
    }
    
    pipeline_config = {
        'name': 'my_pipeline',
        'layer': 'Silver'
    }
    
    project_config = {
        'project': 'Energy Efficiency',
        'plant': 'NKC',
        'asset': 'Germ Dryer 1'
    }
    
    ctx = build_context_from_config(node_config, pipeline_config, project_config)
    
    assert ctx.node_name == 'my_node'
    assert ctx.operation_name == 'my_op'
    assert ctx.params == {'threshold': 100}
    assert ctx.pipeline_name == 'my_pipeline'
    assert ctx.layer == 'Silver'
    assert ctx.project == 'Energy Efficiency'
    assert ctx.plant == 'NKC'
    assert ctx.asset == 'Germ Dryer 1'
```

**Deliverables:**
- TransformationContext class
- build_context_from_config() function
- 5+ tests passing

---

### Day 5: Week 1 Integration & Testing

**Tasks:**
1. Integration test: register transformation + call with context
2. Update main exports
3. Documentation
4. Week 1 review

**Integration Test:** `tests/integration/test_transformation_integration.py`

```python
"""Integration tests for transformation system."""
import pandas as pd
from odibi.transformations import transformation, get_registry, TransformationContext


def test_transformation_with_context():
    """Should register transformation and call with context."""
    
    # Clear registry
    get_registry().clear()
    
    # Define transformation
    @transformation("context_aware_filter")
    def context_aware_filter(df, threshold, **context):
        """
        Filter records above threshold.
        
        Context-aware: Uses plant information if available.
        """
        plant = context.get('plant', 'Unknown')
        print(f"Filtering {plant} data with threshold {threshold}")
        return df[df.value > threshold]
    
    # Create test data
    df = pd.DataFrame({'value': [10, 20, 30, 40, 50]})
    
    # Get function from registry
    registry = get_registry()
    func = registry.get("context_aware_filter")
    
    # Create context
    ctx = TransformationContext(
        node_name="filter",
        operation_name="context_aware_filter",
        plant="NKC",
        asset="Germ Dryer 1"
    )
    
    # Call with context
    result = func(df, threshold=25, **ctx.to_dict())
    
    # Verify
    assert len(result) == 3  # Values 30, 40, 50
    assert result['value'].min() == 30
```

**Update:** `odibi/__init__.py`

```python
# Add transformation exports
from .transformations import transformation, get_registry

__all__ = [
    'Pipeline',
    'Config',
    'ProjectConfig',
    'transformation',  # NEW
    'get_registry'     # NEW
]
```

**Documentation:** Update README example

```markdown
## Creating Custom Transformations

```python
from odibi import transformation

@transformation("filter_high_values")
def filter_high_values(df, threshold=100):
    '''
    Filter records above threshold.
    
    Args:
        df: Input DataFrame
        threshold: Minimum value to keep
    
    Returns:
        Filtered DataFrame
    '''
    return df[df.value > threshold]
```

Use in YAML:
```yaml
pipelines:
  my_pipeline:
    - node: filtered
      operation: filter_high_values
      params:
        threshold: 200
```
```

**Deliverables:**
- Week 1 complete
- 15+ new tests passing
- All 167+ tests passing (152 from Phase 2.5 + 15 new)
- Documentation updated

---

## Week 2: Explanation System

### Goals
- Enable transformations to define explain() methods
- Explain methods receive context
- Test context-aware explanations

### Day 6-7: Explanation Method Support

**File:** `odibi/transformations/explanation.py`

```python
"""
Explanation System
==================

Enables transformations to generate context-aware documentation.
"""
from typing import Callable, Optional, Dict, Any
import inspect


class ExplanationMethod:
    """
    Wrapper for transformation explanation methods.
    
    Explanation methods receive:
    - All transformation parameters
    - Full context (plant, asset, pipeline info, etc.)
    
    And return:
    - Markdown-formatted explanation string
    """
    
    def __init__(self, func: Callable):
        """
        Initialize explanation method.
        
        Args:
            func: The explanation function
        """
        self.func = func
    
    def __call__(self, **kwargs) -> str:
        """
        Call explanation method with parameters and context.
        
        Args:
            **kwargs: Parameters and context
        
        Returns:
            Markdown-formatted explanation
        """
        return self.func(**kwargs)


def attach_explanation(transformation_func: Callable, explanation_func: Callable):
    """
    Attach an explanation method to a transformation function.
    
    Args:
        transformation_func: The transformation function
        explanation_func: The explanation function
    
    Example:
        @transformation("my_transform")
        def my_transform(df, threshold):
            return df[df.value > threshold]
        
        @my_transform.explain
        def explain(threshold, **context):
            plant = context.get('plant', 'Unknown')
            return f"Filter {plant} records above {threshold}"
    """
    transformation_func.explain = ExplanationMethod(explanation_func)


# Monkey-patch to enable @func.explain decorator syntax
def _make_explainable(func: Callable) -> Callable:
    """
    Make a function support the .explain decorator.
    
    After calling this, you can do:
        @func.explain
        def my_explanation(...):
            ...
    """
    def explain_decorator(explanation_func: Callable) -> Callable:
        attach_explanation(func, explanation_func)
        return explanation_func
    
    func.explain = explain_decorator
    return func


# Apply to all registered transformations
def make_transformation_explainable(func: Callable) -> Callable:
    """
    Called by @transformation decorator to make function explainable.
    
    This enables:
        @transformation("my_op")
        def my_op(df):
            ...
        
        @my_op.explain
        def explain(**context):
            return "Explanation here"
    """
    return _make_explainable(func)
```

**Update:** `odibi/transformations/decorators.py`

```python
from .explanation import make_transformation_explainable

def transformation(...):
    def decorator(func: Callable) -> Callable:
        registry = get_registry()
        registry.register(...)
        
        # Make explainable
        func = make_transformation_explainable(func)
        
        return func
    
    return decorator
```

**Tests:** `tests/unit/test_explanations.py`

```python
"""Tests for explanation system."""
import pytest
from odibi.transformations import transformation, get_registry


class TestExplanations:
    
    def setup_method(self):
        get_registry().clear()
    
    def test_transformation_without_explanation(self):
        """Transformation without explain() should work."""
        @transformation("no_explain")
        def no_explain(df):
            """Simple transformation."""
            return df
        
        func = get_registry().get("no_explain")
        assert not hasattr(func, 'explain') or callable(func.explain)
    
    def test_attach_explanation(self):
        """Should attach explanation method to transformation."""
        @transformation("with_explain")
        def with_explain(df, threshold):
            """Filter records."""
            return df[df.value > threshold]
        
        @with_explain.explain
        def explain(threshold, **context):
            return f"Filter records above {threshold}"
        
        # Get function from registry
        func = get_registry().get("with_explain")
        
        # Call explain method
        explanation = func.explain(threshold=100)
        
        assert "Filter records above 100" in explanation
    
    def test_context_aware_explanation(self):
        """Explanation should receive context."""
        @transformation("context_explain")
        def context_explain(df):
            """Context-aware transformation."""
            return df
        
        @context_explain.explain
        def explain(**context):
            plant = context.get('plant', 'Unknown')
            asset = context.get('asset', 'Unknown')
            return f"Process data for {plant} {asset}"
        
        func = get_registry().get("context_explain")
        explanation = func.explain(plant="NKC", asset="Germ Dryer 1")
        
        assert "NKC" in explanation
        assert "Germ Dryer 1" in explanation
    
    def test_explanation_with_parameters_and_context(self):
        """Explanation should receive both parameters and context."""
        @transformation("full_explain")
        def full_explain(df, threshold, multiplier=1.0):
            """Complex transformation."""
            return df
        
        @full_explain.explain
        def explain(threshold, multiplier=1.0, **context):
            plant = context.get('plant', 'Unknown')
            return f"""
            **Purpose:**
            Filter {plant} records above {threshold} and multiply by {multiplier}.
            
            **Details:**
            - Threshold: {threshold}
            - Multiplier: {multiplier}
            """
        
        func = get_registry().get("full_explain")
        explanation = func.explain(
            threshold=100,
            multiplier=2.0,
            plant="NKC"
        )
        
        assert "100" in explanation
        assert "2.0" in explanation
        assert "NKC" in explanation
```

**Deliverables:**
- Explanation system implemented
- @func.explain decorator working
- 6+ tests passing

---

### Day 8-9: Built-in Explanation Templates

**Goal:** Create helper functions for common explanation patterns

**File:** `odibi/transformations/templates.py`

```python
"""
Explanation Templates
=====================

Helper functions for generating common explanation patterns.
"""
from typing import List, Dict, Any


def purpose_detail_result(purpose: str, details: List[str], result: str) -> str:
    """
    Generate standard Purpose/Details/Result explanation.
    
    Args:
        purpose: One-line purpose statement
        details: List of detail bullet points
        result: One-line result statement
    
    Returns:
        Formatted markdown explanation
    """
    details_text = "\n".join(f"- {detail}" for detail in details)
    
    return f"""
**Purpose:**
{purpose}

**Details:**
{details_text}

**Result:**
{result}
"""


def with_formula(
    purpose: str,
    details: List[str],
    formula: str,
    result: str
) -> str:
    """
    Generate explanation with formula.
    
    Useful for calculations and transformations with mathematical operations.
    """
    details_text = "\n".join(f"- {detail}" for detail in details)
    
    return f"""
**Purpose:**
{purpose}

**Details:**
{details_text}

**Formula:**
```
{formula}
```

**Result:**
{result}
"""


def table_explanation(
    purpose: str,
    details: List[str],
    table_data: List[Dict[str, str]],
    result: str
) -> str:
    """
    Generate explanation with embedded table.
    
    Args:
        purpose: Purpose statement
        details: Detail bullet points
        table_data: List of dicts with column headers as keys
        result: Result statement
    
    Returns:
        Markdown with table
    """
    if not table_data:
        return purpose_detail_result(purpose, details, result)
    
    # Generate markdown table
    headers = list(table_data[0].keys())
    header_row = "| " + " | ".join(headers) + " |"
    separator_row = "| " + " | ".join(["---"] * len(headers)) + " |"
    
    data_rows = []
    for row in table_data:
        values = [str(row.get(h, "")) for h in headers]
        data_rows.append("| " + " | ".join(values) + " |")
    
    table = "\n".join([header_row, separator_row] + data_rows)
    
    details_text = "\n".join(f"- {detail}" for detail in details)
    
    return f"""
**Purpose:**
{purpose}

**Details:**
{details_text}

{table}

**Result:**
{result}
"""
```

**Example Usage:**

```python
from odibi import transformation
from odibi.transformations.templates import purpose_detail_result, with_formula

@transformation("calc_efficiency")
def calc_efficiency(df, fuel_col, output_col):
    """Calculate thermal efficiency."""
    df['efficiency'] = df[output_col] / df[fuel_col]
    return df

@calc_efficiency.explain
def explain(fuel_col, output_col, **context):
    plant = context.get('plant', 'Unknown')
    asset = context.get('asset', 'Unknown')
    
    return with_formula(
        purpose=f"Calculate thermal efficiency for {plant} {asset}",
        details=[
            f"Fuel column: {fuel_col}",
            f"Output column: {output_col}",
            "Uses simple efficiency formula"
        ],
        formula=f"efficiency = {output_col} / {fuel_col}",
        result="Efficiency percentage for performance benchmarking"
    )
```

**Tests:** `tests/unit/test_templates.py`

```python
"""Tests for explanation templates."""
from odibi.transformations.templates import (
    purpose_detail_result,
    with_formula,
    table_explanation
)


def test_purpose_detail_result():
    """Should generate standard explanation format."""
    explanation = purpose_detail_result(
        purpose="Test purpose",
        details=["Detail 1", "Detail 2"],
        result="Test result"
    )
    
    assert "**Purpose:**" in explanation
    assert "Test purpose" in explanation
    assert "- Detail 1" in explanation
    assert "- Detail 2" in explanation
    assert "**Result:**" in explanation
    assert "Test result" in explanation


def test_with_formula():
    """Should generate explanation with formula."""
    explanation = with_formula(
        purpose="Calculate efficiency",
        details=["Uses fuel and output"],
        formula="efficiency = output / fuel",
        result="Efficiency percentage"
    )
    
    assert "**Formula:**" in explanation
    assert "efficiency = output / fuel" in explanation


def test_table_explanation():
    """Should generate explanation with table."""
    table_data = [
        {"Column": "Fuel", "Type": "Input"},
        {"Column": "Output", "Type": "Result"}
    ]
    
    explanation = table_explanation(
        purpose="Data overview",
        details=["Shows columns"],
        table_data=table_data,
        result="Complete dataset"
    )
    
    assert "| Column | Type |" in explanation
    assert "| Fuel | Input |" in explanation
```

**Deliverables:**
- Template helpers implemented
- 5+ tests passing
- Week 2 complete

---

## Week 3: Quality Enforcement

### Goals
- Implement explanation linter
- Validate explanation quality
- Enforce documentation requirements

### Day 10-11: Explanation Linter

**File:** `odibi/validation/explanation_linter.py`

```python
"""
Explanation Quality Linter
===========================

Validates that explanations meet Odibi quality standards.
"""
from typing import List, Dict, Any
from dataclasses import dataclass
import re


@dataclass
class LintIssue:
    """A linting issue found in an explanation."""
    severity: str  # "error", "warning", "info"
    message: str
    rule: str
    
    def __str__(self):
        symbol = {"error": "❌", "warning": "⚠️", "info": "ℹ️"}[self.severity]
        return f"{symbol} {self.message} [{self.rule}]"


class ExplanationLinter:
    """
    Lints explanation text for quality issues.
    
    Checks:
    - Minimum length
    - Required sections (Purpose, Details, Result)
    - Generic/lazy phrases
    - TODO placeholders
    - Formula formatting
    """
    
    REQUIRED_SECTIONS = ["Purpose", "Details", "Result"]
    
    LAZY_PHRASES = [
        "calculates stuff",
        "does things",
        "processes data",
        "handles records",
        "TODO",
        "[placeholder]",
        "TBD",
        "to be determined"
    ]
    
    MIN_LENGTH = 50  # characters
    
    def __init__(self):
        self.issues: List[LintIssue] = []
    
    def lint(self, explanation: str, operation_name: str = "unknown") -> List[LintIssue]:
        """
        Lint an explanation and return issues.
        
        Args:
            explanation: The explanation text
            operation_name: Name of the operation (for error messages)
        
        Returns:
            List of LintIssue objects
        """
        self.issues = []
        
        if not explanation or not explanation.strip():
            self.issues.append(LintIssue(
                severity="error",
                message=f"Explanation for '{operation_name}' is empty",
                rule="E001"
            ))
            return self.issues
        
        # Check length
        self._check_length(explanation, operation_name)
        
        # Check required sections
        self._check_required_sections(explanation, operation_name)
        
        # Check for lazy phrases
        self._check_lazy_phrases(explanation, operation_name)
        
        # Check formula formatting
        self._check_formula_formatting(explanation, operation_name)
        
        return self.issues
    
    def _check_length(self, text: str, op_name: str):
        """Check minimum length requirement."""
        if len(text.strip()) < self.MIN_LENGTH:
            self.issues.append(LintIssue(
                severity="error",
                message=f"Explanation for '{op_name}' too short ({len(text)} chars, minimum {self.MIN_LENGTH})",
                rule="E002"
            ))
    
    def _check_required_sections(self, text: str, op_name: str):
        """Check for required sections."""
        for section in self.REQUIRED_SECTIONS:
            pattern = f"\\*\\*{section}:?\\*\\*"
            if not re.search(pattern, text, re.IGNORECASE):
                self.issues.append(LintIssue(
                    severity="error",
                    message=f"Explanation for '{op_name}' missing required section: {section}",
                    rule="E003"
                ))
    
    def _check_lazy_phrases(self, text: str, op_name: str):
        """Check for generic/lazy phrases."""
        text_lower = text.lower()
        for phrase in self.LAZY_PHRASES:
            if phrase.lower() in text_lower:
                self.issues.append(LintIssue(
                    severity="error",
                    message=f"Explanation for '{op_name}' contains generic phrase: '{phrase}'",
                    rule="E004"
                ))
    
    def _check_formula_formatting(self, text: str, op_name: str):
        """Check formula formatting."""
        # If mentions "formula" but no code block
        if "formula" in text.lower():
            if "```" not in text:
                self.issues.append(LintIssue(
                    severity="warning",
                    message=f"Explanation for '{op_name}' mentions formula but no code block found",
                    rule="W001"
                ))
    
    def has_errors(self) -> bool:
        """Check if any errors were found."""
        return any(issue.severity == "error" for issue in self.issues)
    
    def format_issues(self) -> str:
        """Format all issues as string."""
        if not self.issues:
            return "✅ No issues found"
        
        lines = []
        for issue in self.issues:
            lines.append(str(issue))
        return "\n".join(lines)
```

**Tests:** (40+ lines in actual test file)

```python
# tests/unit/test_linting.py
from odibi.validation import ExplanationLinter, LintIssue

def test_empty_explanation_fails():
    linter = ExplanationLinter()
    issues = linter.lint("", "test_op")
    assert len(issues) == 1
    assert "empty" in issues[0].message.lower()

def test_too_short_explanation_fails():
    linter = ExplanationLinter()
    issues = linter.lint("Short", "test_op")
    assert any("too short" in issue.message.lower() for issue in issues)

# ... 15+ more tests
```

**Deliverables:**
- Linter implemented
- 15+ tests passing
- Clear error messages

---

### Day 12-13: Validation Integration

**Goal:** Integrate linter with transformation registry

**File:** `odibi/validation/__init__.py`

```python
from .explanation_linter import ExplanationLinter, LintIssue

def validate_transformation_explanation(
    transformation_name: str,
    explanation_func: callable,
    **sample_context
) -> List[LintIssue]:
    """
    Validate that a transformation's explanation meets quality standards.
    
    Args:
        transformation_name: Name of the transformation
        explanation_func: The explain() function
        **sample_context: Sample context to pass to explain()
    
    Returns:
        List of lint issues
    """
    # Call explain() with sample context
    explanation_text = explanation_func(**sample_context)
    
    # Lint
    linter = ExplanationLinter()
    issues = linter.lint(explanation_text, transformation_name)
    
    return issues
```

**CLI Integration:** `odibi/cli/validate.py`

```python
def validate_command(args):
    """Validate config file."""
    try:
        config = Config.load(args.config)
        
        # NEW: Check explanations if flag set
        if args.check_explanations:
            issues = validate_all_explanations(config)
            if issues:
                print("❌ Explanation quality issues found:")
                for issue in issues:
                    print(f"  {issue}")
                return 1
        
        print(f"✅ Config is valid")
        return 0
    except Exception as e:
        print(f"❌ Config validation failed: {e}")
        return 1
```

**Testing:** Integration test that runs full validation

**Deliverables:**
- Validation integrated
- CLI updated
- 5+ integration tests
- Week 3 complete

---

## Week 4: Built-in Operations

### Goals
- Implement pivot, unpivot, join, sql operations
- Each with execute() and explain() methods
- Comprehensive tests

### Day 14-16: Core Operations

**Implement 4 core operations:**
1. pivot
2. unpivot
3. join
4. sql

(Each operation is ~100 lines of code + ~100 lines of tests)

**Example:** `odibi/operations/pivot.py`

```python
"""Pivot Operation"""
from odibi import transformation
from odibi.transformations.templates import purpose_detail_result

@transformation("pivot", category="reshaping")
def pivot(df, group_by, pivot_column, value_column, agg_func="first"):
    """
    Convert long-format data into wide-format structure.
    
    Args:
        df: Input DataFrame
        group_by: Columns to group by
        pivot_column: Column whose values become new columns
        value_column: Column whose values fill the new columns
        agg_func: Aggregation function (default: "first")
    
    Returns:
        Wide-format DataFrame
    """
    return df.pivot_table(
        index=group_by,
        columns=pivot_column,
        values=value_column,
        aggfunc=agg_func
    ).reset_index()

@pivot.explain
def explain(group_by, pivot_column, value_column, agg_func="first", **context):
    """Generate context-aware explanation for pivot operation."""
    
    return purpose_detail_result(
        purpose="Convert long-format data into wide-format structure",
        details=[
            f"Groups by: {', '.join(group_by)}",
            f"Pivots column: `{pivot_column}` into separate columns",
            f"Aggregates: `{value_column}` using `{agg_func}()`",
            "Produces one row per unique combination of group columns"
        ],
        result=f"Wide-format dataset with one column per `{pivot_column}` value, ready for calculations"
    )
```

**Tests:** ~25 tests per operation

**Deliverables:**
- 4 core operations implemented
- Each with explain() method
- 100+ tests total
- Week 4 complete

---

### Day 17: Phase 3A Wrap-Up

**Tasks:**
1. Run full test suite (200+ tests)
2. Update documentation
3. Create release
4. Review before Phase 3B

**Testing:**
```bash
pytest -v  # All 200+ tests
pytest --cov=odibi --cov-report=html  # Coverage report
```

**Documentation:**
- Update CHANGELOG.md
- Document all new features
- Create examples

**Release:**
```bash
git tag v1.3.0-alpha.1-phase3a
git push origin main --tags
```

**Deliverables:**
- Phase 3A complete
- 200+ tests passing
- Coverage ≥ 80%
- Ready for Phase 3B

---

## Phase 3A Acceptance Criteria

**Before moving to Phase 3B:**

### Transformation Registry
- [ ] @transformation decorator works
- [ ] Transformations require docstrings
- [ ] Registry stores metadata
- [ ] 15+ tests passing

### Context System
- [ ] TransformationContext class works
- [ ] Context flows from YAML to functions
- [ ] 5+ tests passing

### Explanation System
- [ ] @func.explain decorator works
- [ ] Explanations receive context
- [ ] Template helpers available
- [ ] 15+ tests passing

### Quality Enforcement
- [ ] Explanation linter validates quality
- [ ] Required sections enforced
- [ ] Lazy phrases detected
- [ ] 20+ tests passing

### Built-in Operations
- [ ] pivot, unpivot, join, sql implemented
- [ ] Each has explain() method
- [ ] 100+ tests passing

### Overall
- [ ] 200+ total tests passing
- [ ] Coverage ≥ 80%
- [ ] Documentation complete
- [ ] No breaking changes

---

**Next:** See [PHASE_3B_PLAN.md](PHASE_3B_PLAN.md) for Story Generation (3 weeks)

---

**Last Updated:** 2025-11-10  
**Maintainer:** Henry Odibi
