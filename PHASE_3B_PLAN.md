# Phase 3B: Stories (Run & Doc Story Engines)

**Duration:** 3 weeks  
**Prerequisites:** Phase 3A complete  
**Goal:** Build comprehensive story generation for pipeline runs and stakeholder documentation

---

## Overview

Phase 3B enhances Odibi's story generation capabilities to provide:
1. **Run Stories** - Automatic audit trails of every pipeline execution
2. **Doc Stories** - Stakeholder-ready documentation with explanations
3. **Theme System** - Customizable branding and formatting

**Principle:** Every run is documented. Every operation explains itself.

---

## Week 1: Enhanced Run Story Engine

### Goals
- Enhance existing story generator with detailed metadata
- Capture timing, row counts, schema changes automatically
- Add HTML output format alongside Markdown
- Implement story metadata tracking

### Day 1-2: Story Metadata Enhancement

**File:** `odibi/story/metadata.py` (NEW)

```python
"""Story metadata tracking."""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class NodeExecutionMetadata:
    """Metadata for a single node execution."""

    node_name: str
    operation: str
    status: str  # "success", "failed", "skipped"
    duration: float

    # Data metrics
    rows_in: Optional[int] = None
    rows_out: Optional[int] = None
    rows_change: Optional[int] = None
    rows_change_pct: Optional[float] = None

    # Schema tracking
    schema_in: Optional[List[str]] = None
    schema_out: Optional[List[str]] = None
    columns_added: List[str] = field(default_factory=list)
    columns_removed: List[str] = field(default_factory=list)
    columns_renamed: List[str] = field(default_factory=list)

    # Error info
    error_message: Optional[str] = None
    error_type: Optional[str] = None

    # Timestamps
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


@dataclass
class PipelineStoryMetadata:
    """Complete metadata for a pipeline run story."""

    pipeline_name: str
    pipeline_layer: Optional[str] = None

    # Execution info
    started_at: str = field(default_factory=lambda: datetime.now().isoformat())
    completed_at: Optional[str] = None
    duration: float = 0.0

    # Status
    total_nodes: int = 0
    completed_nodes: int = 0
    failed_nodes: int = 0
    skipped_nodes: int = 0

    # Node details
    nodes: List[NodeExecutionMetadata] = field(default_factory=list)

    # Project context
    project: Optional[str] = None
    plant: Optional[str] = None
    asset: Optional[str] = None
    business_unit: Optional[str] = None

    # Story settings
    theme: str = "default"
    include_samples: bool = True
    max_sample_rows: int = 10

    def add_node(self, node_metadata: NodeExecutionMetadata):
        """Add node execution metadata."""
        self.nodes.append(node_metadata)
        self.total_nodes += 1

        if node_metadata.status == "success":
            self.completed_nodes += 1
        elif node_metadata.status == "failed":
            self.failed_nodes += 1
        elif node_metadata.status == "skipped":
            self.skipped_nodes += 1

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "pipeline_name": self.pipeline_name,
            "pipeline_layer": self.pipeline_layer,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration": self.duration,
            "total_nodes": self.total_nodes,
            "completed_nodes": self.completed_nodes,
            "failed_nodes": self.failed_nodes,
            "skipped_nodes": self.skipped_nodes,
            "nodes": [
                {
                    "node_name": n.node_name,
                    "operation": n.operation,
                    "status": n.status,
                    "duration": n.duration,
                    "rows_in": n.rows_in,
                    "rows_out": n.rows_out,
                    "rows_change": n.rows_change,
                    "rows_change_pct": n.rows_change_pct,
                }
                for n in self.nodes
            ],
            "project": self.project,
            "plant": self.plant,
            "asset": self.asset,
        }
```

**Deliverables:**
- NodeExecutionMetadata class
- PipelineStoryMetadata class
- Automatic schema change detection
- Row count delta tracking

---

### Day 3-4: HTML Story Templates

**File:** `odibi/story/templates/run_story.html` (NEW)

Create professional HTML template with:
- Responsive design
- Syntax highlighting for SQL/code
- Collapsible sections
- Timeline visualization
- Status indicators (✅ ❌ ⏭️)

**File:** `odibi/story/renderers.py` (NEW)

```python
"""Story renderers for different output formats."""

from typing import Dict, Any
from pathlib import Path
from jinja2 import Template

class HTMLStoryRenderer:
    """Renders stories as HTML."""

    def __init__(self, template_path: Optional[str] = None):
        """Initialize renderer."""
        self.template_path = template_path or self._default_template()

    def render(self, metadata: PipelineStoryMetadata, context: Any) -> str:
        """Render story as HTML."""
        template = self._load_template()
        return template.render(
            metadata=metadata,
            context=context
        )
```

**Deliverables:**
- HTML template with professional styling
- HTMLStoryRenderer class
- MarkdownStoryRenderer (enhanced)
- JSON export option

---

## Week 2: Doc Story Generator

### Goals
- Build doc story generator for stakeholder communication
- Pull operation explanations automatically
- Support custom descriptions and references
- Quality validation before generation

### Day 5-7: Doc Story Engine

**File:** `odibi/story/doc_story.py` (NEW)

```python
"""Documentation story generator."""

from typing import Dict, Any, List, Optional
from odibi.transformations import get_registry
from odibi.validation import ExplanationLinter


class DocStoryGenerator:
    """Generates stakeholder documentation stories."""

    def __init__(self, pipeline_config: PipelineConfig, project_config: ProjectConfig):
        """Initialize doc story generator."""
        self.pipeline_config = pipeline_config
        self.project_config = project_config
        self.registry = get_registry()
        self.linter = ExplanationLinter()

    def generate(self, validate: bool = True) -> str:
        """
        Generate documentation story.

        Args:
            validate: Whether to validate explanation quality

        Returns:
            Path to generated documentation
        """
        # Build documentation sections
        sections = []

        # 1. Pipeline overview
        sections.append(self._generate_overview())

        # 2. Data flow diagram (ASCII or Mermaid)
        sections.append(self._generate_flow_diagram())

        # 3. Operation details (with explanations)
        sections.append(self._generate_operation_details(validate))

        # 4. Expected outputs
        sections.append(self._generate_outputs())

        # Render to HTML/Markdown
        return self._render(sections)

    def _generate_operation_details(self, validate: bool) -> Dict[str, Any]:
        """Generate detailed operation explanations."""
        operations = []

        for node in self.pipeline_config.nodes:
            # Get operation function
            func = self.registry.get(node.operation)

            if func and hasattr(func, 'get_explanation'):
                # Get explanation
                explanation = func.get_explanation(
                    **node.params,
                    **self._build_context(node)
                )

                # Validate if required
                if validate:
                    issues = self.linter.lint(explanation, node.operation)
                    if self.linter.has_errors():
                        raise ValueError(
                            f"Operation '{node.operation}' has explanation quality issues:\n"
                            f"{self.linter.format_issues()}"
                        )

                operations.append({
                    'node': node.node,
                    'operation': node.operation,
                    'explanation': explanation,
                    'params': node.params
                })

        return {'operations': operations}
```

**Deliverables:**
- DocStoryGenerator class
- Automatic explanation extraction
- Quality validation integration
- Flow diagram generation

---

### Day 8-9: CLI Integration

**File:** `odibi/cli/story.py` (NEW)

```bash
# Generate doc story
odibi story generate config.yaml --output docs/pipeline_doc.html

# Generate doc story without validation (draft mode)
odibi story generate config.yaml --no-validate --output draft.md

# Generate from last run
odibi story from-run stories/runs/pipeline_20250110_143022.json --format html
```

**Deliverables:**
- `odibi story generate` command
- `odibi story from-run` command  
- Format selection (--format html|markdown|json)
- Validation controls

---

## Week 3: Theme System & Polish

### Goals
- Implement theme system for customizable branding
- Support custom CSS and templates
- Add charts and visualizations
- Final polish and testing

### Day 10-12: Theme System

**File:** `odibi/story/themes.py` (NEW)

```python
"""Theme system for story customization."""

from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class StoryTheme:
    """Story theme configuration."""

    name: str

    # Colors
    primary_color: str = "#0066cc"
    success_color: str = "#28a745"
    error_color: str = "#dc3545"
    warning_color: str = "#ffc107"

    # Typography
    font_family: str = "system-ui, -apple-system, sans-serif"
    heading_font: str = "Georgia, serif"
    code_font: str = "Consolas, Monaco, monospace"

    # Branding
    logo_url: Optional[str] = None
    company_name: Optional[str] = None
    footer_text: Optional[str] = None

    # Layout
    max_width: str = "1200px"
    sidebar: bool = True

    # Custom CSS
    custom_css: Optional[str] = None

    def to_css_vars(self) -> Dict[str, str]:
        """Convert theme to CSS variables."""
        return {
            '--primary-color': self.primary_color,
            '--success-color': self.success_color,
            '--error-color': self.error_color,
            '--warning-color': self.warning_color,
            '--font-family': self.font_family,
            '--heading-font': self.heading_font,
            '--code-font': self.code_font,
        }


# Built-in themes
DEFAULT_THEME = StoryTheme(name="default")

CORPORATE_THEME = StoryTheme(
    name="corporate",
    primary_color="#003366",
    font_family="Arial, sans-serif",
    heading_font="Arial, sans-serif"
)

DARK_THEME = StoryTheme(
    name="dark",
    primary_color="#00bfff",
    success_color="#4caf50",
    error_color="#f44336",
    custom_css="""
    body { background: #1e1e1e; color: #e0e0e0; }
    """
)
```

**Deliverables:**
- StoryTheme class
- Built-in themes (default, corporate, dark)
- Custom theme loading from YAML
- Theme preview command

---

### Day 13-14: Visualizations & Charts

Add visual elements to stories:
- **Timeline chart** - Node execution order and duration
- **Row count flow** - Sankey diagram showing data flow
- **Schema changes** - Visual diff of column changes
- **Success rate** - Historical trends (if available)

Use lightweight charting (Chart.js or similar, embedded in HTML)

**Deliverables:**
- Timeline visualization
- Row count flow diagram
- Schema diff visualization
- Embedded charts in HTML stories

---

## Phase 3B Acceptance Criteria

**Run Stories:**
- [ ] Every pipeline run auto-generates story
- [ ] Stories capture timing, row counts, schema changes
- [ ] Stories saved to `stories/runs/` automatically
- [ ] HTML and Markdown formats supported
- [ ] 20+ tests for run story generation

**Doc Stories:**
- [ ] `odibi story generate` command works
- [ ] Pulls explanations from registered operations
- [ ] Validates explanation quality before generation
- [ ] Supports custom descriptions
- [ ] 15+ tests for doc story generation

**Theme System:**
- [ ] Multiple built-in themes available
- [ ] Custom themes loadable from YAML
- [ ] Theme applies to HTML output
- [ ] 10+ tests for theme system

**Overall:**
- [ ] 50+ new tests passing
- [ ] All existing tests still pass
- [ ] Documentation complete
- [ ] Examples provided

---

## Testing Strategy

### Unit Tests
- Story metadata tracking
- HTML/Markdown rendering
- Theme loading and application
- Explanation extraction

### Integration Tests
- Full pipeline → story generation
- CLI story commands
- Theme application in rendered output

### Visual Tests
- HTML story manual review
- Theme variations visual check
- Chart rendering verification

---

**Next:** See [PHASE_3C_PLAN.md](PHASE_3C_PLAN.md) for CLI + Diffing (2 weeks)

---

**Last Updated:** 2025-11-10  
**Maintainer:** Henry Odibi
