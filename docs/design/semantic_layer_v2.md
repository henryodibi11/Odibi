# Semantic Layer V2 - Design Document

## Overview

This document outlines the enhanced semantic layer for Odibi, addressing the limitations of simple aggregation expressions and adding view generation, SQL documentation, and lineage tracking.

## Problem Statement

### The Grain/Recalculation Issue

The current semantic layer uses simple SQL aggregation expressions like `AVG(OEE)` for metrics. This approach is **wrong for ratio metrics** like OEE because:

- OEE = (Good Units / Total Units) × (Runtime / Planned Time)
- When changing grain (e.g., daily → monthly), you can't just `AVG(OEE)`
- You must SUM the underlying components, then recalculate the ratio

**Example:**
```
Day 1: Good=80, Total=100 → OEE=80%
Day 2: Good=90, Total=200 → OEE=45%

WRONG: AVG(80%, 45%) = 62.5%
RIGHT: (80+90)/(100+200) = 56.7%
```

### The Documentation Problem

- Analysts ask "how is this calculated?"
- Different analysts connect to data and recalculate differently
- No single source of truth for metric definitions

### The Lineage Problem

- Each pipeline layer generates its own story
- No end-to-end view connecting raw → bronze → silver → gold → semantic

---

## Solution Architecture

### Data Flow

```
Delta Lake → Odibi Pipelines → gold.oee_fact (with components)
                                    ↓
                            Semantic Layer
                                    ↓
                    ┌───────────────┼───────────────┐
                    ↓               ↓               ↓
            vw_oee_daily    vw_oee_monthly   vw_oee_yearly
            (SQL View)       (SQL View)       (SQL View)
                                    ↓
                              .sql files → ADLS (documentation)
                                    ↓
                              Power BI (consumption)
```

### Key Principles

1. **Pre-compute everything** - Analysts get final numbers, no formulas, no mistakes
2. **SQL as documentation** - Generated SQL can be shared when asked "how is this calculated?"
3. **Views over tables** - Use SQL Server views; logic is self-documenting in view definition
4. **Lineage from stories** - Stitch existing story graph_data, don't rebuild

---

## Phase 1: Derived Metrics

### Changes to MetricDefinition

**File:** `odibi/semantics/metrics.py`

```python
class MetricDefinition(BaseModel):
    name: str
    description: Optional[str] = None
    expr: Optional[str] = None  # For simple metrics: "SUM(revenue)"
    source: Optional[str] = None
    filters: List[str] = []
    type: MetricType = MetricType.SIMPLE
    
    # NEW: For derived metrics
    components: Optional[List[str]] = None  # ["good_units", "total_units", ...]
    formula: Optional[str] = None  # "(good_units / total_units) * ..."
```

### YAML Configuration

```yaml
semantic:
  metrics:
    # Component metrics (additive - safe to SUM)
    - name: good_units
      expr: "SUM(Good_Units)"
      source: gold.oee_fact
      
    - name: total_units
      expr: "SUM(Total_Units)"
      source: gold.oee_fact
      
    - name: runtime_hrs
      expr: "SUM(Runtime_Hrs)"
      source: gold.oee_fact
      
    - name: planned_hrs
      expr: "SUM(Planned_Hrs)"
      source: gold.oee_fact

    # Derived metric (ratio - must be recalculated after aggregation)
    - name: oee
      type: derived
      description: "OEE = Quality × Availability"
      components: [good_units, total_units, runtime_hrs, planned_hrs]
      formula: "(good_units / total_units) * (runtime_hrs / planned_hrs)"
```

### Query Generation Logic

When `type: derived`:
1. Resolve all component metrics
2. Generate SQL that aggregates components first
3. Apply formula to aggregated values

**Generated SQL for `oee BY month, plant`:**
```sql
SELECT 
    DATETRUNC(month, Date) AS month,
    Plant,
    SUM(Good_Units) AS good_units,
    SUM(Total_Units) AS total_units,
    SUM(Runtime_Hrs) AS runtime_hrs,
    SUM(Planned_Hrs) AS planned_hrs,
    (SUM(Good_Units) * 1.0 / NULLIF(SUM(Total_Units), 0)) 
      * (SUM(Runtime_Hrs) * 1.0 / NULLIF(SUM(Planned_Hrs), 0)) AS oee
FROM gold.oee_fact
GROUP BY DATETRUNC(month, Date), Plant
```

---

## Phase 2: View Generation

### New Module: `odibi/semantics/views.py`

```python
class ViewGenerator:
    def __init__(self, config: SemanticLayerConfig):
        self.config = config
    
    def generate_view_ddl(self, view_config: ViewConfig) -> str:
        """Generate CREATE VIEW statement."""
        ...
    
    def execute_view(
        self, 
        view_config: ViewConfig,
        connection: Connection,
        save_sql_to: Optional[str] = None
    ) -> ViewResult:
        """Create view in SQL Server, optionally save .sql to ADLS."""
        ...
```

### YAML Configuration

```yaml
semantic:
  metrics:
    # ... as above
    
  dimensions:
    - name: date
      column: Date
      source: gold.oee_fact
    - name: month
      column: Date
      grain: month  # NEW: indicates time grain transformation
    - name: plant
      column: P_ID
      source: gold.oee_fact

  views:
    - name: vw_oee_daily
      description: "Daily OEE metrics by plant"
      metrics: [oee, good_units, total_units, availability]
      dimensions: [date, plant]
      
    - name: vw_oee_monthly
      description: "Monthly OEE rollup for executive reporting"
      metrics: [oee, good_units, total_units, availability]
      dimensions: [month, plant]
```

### Execution

```python
from odibi.semantics import SemanticLayer

semantic = SemanticLayer(config)

# Generate and execute all views
result = semantic.execute_views(
    connection=sql_server_conn,
    save_sql_to="abfss://container@storage.dfs.core.windows.net/gold/views/"
)

# Result contains:
# - views_created: ["vw_oee_daily", "vw_oee_monthly"]
# - sql_files_saved: ["gold/views/vw_oee_daily.sql", ...]
# - errors: []
```

### SQL File Output

Each `.sql` file saved to ADLS:

```sql
-- =============================================================================
-- View: vw_oee_monthly
-- Description: Monthly OEE rollup for executive reporting
-- Generated: 2025-01-02 14:45:00
-- Source: semantic/oee_metrics.yaml
-- =============================================================================
-- 
-- Metrics included:
--   - oee: OEE = Quality × Availability
--     Formula: (good_units / total_units) * (runtime_hrs / planned_hrs)
--   - good_units: SUM(Good_Units)
--   - total_units: SUM(Total_Units)
--
-- =============================================================================

CREATE OR ALTER VIEW gold.vw_oee_monthly AS
SELECT 
    DATETRUNC(month, Date) AS month,
    P_ID AS plant,
    SUM(Good_Units) AS good_units,
    SUM(Total_Units) AS total_units,
    SUM(Runtime_Hrs) AS runtime_hrs,
    SUM(Planned_Hrs) AS planned_hrs,
    (SUM(Good_Units) * 1.0 / NULLIF(SUM(Total_Units), 0)) 
      * (SUM(Runtime_Hrs) * 1.0 / NULLIF(SUM(Planned_Hrs), 0)) AS oee
FROM gold.oee_fact
GROUP BY DATETRUNC(month, Date), P_ID;
```

---

## Phase 3: Semantic Stories

### New Metadata Model

**File:** `odibi/semantics/story.py`

```python
@dataclass
class ViewExecutionMetadata:
    view_name: str
    source_table: str
    status: str  # "success", "failed"
    duration: float
    sql_generated: str
    sql_file_path: Optional[str] = None
    error_message: Optional[str] = None
    row_count: Optional[int] = None  # If we query to verify

@dataclass  
class SemanticStoryMetadata:
    name: str
    started_at: str
    completed_at: str
    duration: float
    views: List[ViewExecutionMetadata]
    views_created: int
    views_failed: int
    sql_files_saved: List[str]
    graph_data: Dict[str, Any]  # For lineage stitching
```

### Story Output Structure

```
stories/
  oee_semantic/
    2025-01-02/
      run_14-45-00.json
      run_14-45-00.html
```

### Graph Data for Lineage

```json
{
  "graph_data": {
    "nodes": [
      {"id": "gold.oee_fact", "type": "table", "layer": "gold"},
      {"id": "vw_oee_daily", "type": "view", "layer": "semantic"},
      {"id": "vw_oee_monthly", "type": "view", "layer": "semantic"}
    ],
    "edges": [
      {"from": "gold.oee_fact", "to": "vw_oee_daily"},
      {"from": "gold.oee_fact", "to": "vw_oee_monthly"}
    ]
  }
}
```

---

## Phase 4: Lineage Stitcher

### Purpose

Read all story JSONs from a pipeline run and connect them into one end-to-end lineage view.

### New Module: `odibi/story/lineage.py`

```python
class LineageGenerator:
    def __init__(self, stories_path: str, storage_options: Optional[Dict] = None):
        self.stories_path = stories_path
        self.storage_options = storage_options or {}
    
    def generate(self, date: str = None) -> LineageResult:
        """
        Generate lineage from all stories for a given date.
        
        Args:
            date: Date string (YYYY-MM-DD), defaults to today
            
        Returns:
            LineageResult with combined graph and links to stories
        """
        ...
```

### Lineage JSON Output

**File:** `stories/lineage/2025-01-02.json`

```json
{
  "generated_at": "2025-01-02T15:00:00",
  "date": "2025-01-02",
  "layers": [
    {
      "name": "bronze_pipeline",
      "story_path": "bronze_pipeline/2025-01-02/run_14-30-00.html",
      "status": "success",
      "duration": 45.2
    },
    {
      "name": "silver_pipeline",
      "story_path": "silver_pipeline/2025-01-02/run_14-35-00.html",
      "status": "success",
      "duration": 32.1
    },
    {
      "name": "gold_pipeline",
      "story_path": "gold_pipeline/2025-01-02/run_14-40-00.html",
      "status": "success",
      "duration": 28.5
    },
    {
      "name": "oee_semantic",
      "story_path": "oee_semantic/2025-01-02/run_14-45-00.html",
      "status": "success",
      "duration": 5.3
    }
  ],
  "nodes": [
    {"id": "raw.equipment_logs", "layer": "bronze"},
    {"id": "bronze.equipment", "layer": "bronze"},
    {"id": "silver.equipment_clean", "layer": "silver"},
    {"id": "gold.oee_fact", "layer": "gold"},
    {"id": "vw_oee_daily", "layer": "semantic"},
    {"id": "vw_oee_monthly", "layer": "semantic"}
  ],
  "edges": [
    {"from": "raw.equipment_logs", "to": "bronze.equipment"},
    {"from": "bronze.equipment", "to": "silver.equipment_clean"},
    {"from": "silver.equipment_clean", "to": "gold.oee_fact"},
    {"from": "gold.oee_fact", "to": "vw_oee_daily"},
    {"from": "gold.oee_fact", "to": "vw_oee_monthly"}
  ]
}
```

### Lineage HTML Output

**File:** `stories/lineage/2025-01-02.html`

- Mermaid diagram showing full flow
- Each node clickable → links to that layer's story
- Summary table with layer status/duration

---

## Phase 5: Configuration Integration

### Top-Level YAML Structure

```yaml
project: oee_analytics
engine: spark
version: "1.0"
owner: henry

connections:
  gold_delta:
    type: delta
    path: "abfss://..."
  sql_server:
    type: sqlserver
    host: server.database.windows.net
    database: analytics

system:
  connection: gold_delta

performance:
  # ...

story:
  connection: gold_delta
  path: stories/

semantic:                    # NEW top-level key
  metrics: []
  dimensions: []
  views: []

imports:
  # Pipelines
  - pipelines/bronze/bronze.yaml
  - pipelines/silver/silver.yaml
  - pipelines/gold/gold.yaml
  
  # Semantic (can import multiple)
  - semantic/oee_metrics.yaml
  - semantic/downtime_metrics.yaml
```

### Imported Semantic File

**File:** `semantic/oee_metrics.yaml`

```yaml
semantic:
  metrics:
    - name: good_units
      expr: "SUM(Good_Units)"
      source: gold.oee_fact
    # ...
    
    - name: oee
      type: derived
      components: [good_units, total_units, runtime_hrs, planned_hrs]
      formula: "(good_units / total_units) * (runtime_hrs / planned_hrs)"

  dimensions:
    - name: date
      column: Date
      source: gold.oee_fact
    - name: plant
      column: P_ID

  views:
    - name: vw_oee_daily
      metrics: [oee, good_units, total_units]
      dimensions: [date, plant]
      
    - name: vw_oee_monthly
      metrics: [oee, good_units, total_units]
      dimensions: [month, plant]
```

---

## Implementation Order

| Phase | Scope | Files to Create/Modify | Status |
|-------|-------|------------------------|--------|
| 1 | Derived Metrics | `odibi/semantics/metrics.py`, `odibi/semantics/query.py` | ✅ Complete |
| 2 | View Generation | `odibi/semantics/views.py` (new) | ✅ Complete |
| 3 | Semantic Stories | `odibi/semantics/story.py` (new) | ✅ Complete |
| 4 | Lineage Stitcher | `odibi/story/lineage.py` (new) | ✅ Complete |
| 5 | Config Integration | `odibi/config.py`, loader updates | ✅ Complete |

### Phase 1 Completion Notes (2026-01-02)
- Added `components` and `formula` fields to `MetricDefinition`
- Updated `SemanticQuery` to aggregate components first, then apply formula
- Added NULLIF wrapping for division-by-zero protection in SQL generation
- Full engine parity: Pandas, Spark, and Polars all supported
- 16 new tests in `tests/unit/semantics/test_derived_metrics.py`

### Phase 2 Completion Notes (2026-01-02)
- Added `TimeGrain` enum and `grain` field to `DimensionDefinition`
- Added `ViewConfig` and `ViewResult` models
- Created `odibi/semantics/views.py` with `ViewGenerator` class
- Generates SQL Server `CREATE OR ALTER VIEW` DDL with:
  - Documentation header with metric descriptions
  - Time grain transformations (DATETRUNC)
  - Derived metric formulas with NULLIF protection
- `execute_view()` and `execute_all_views()` for batch execution
- Optional SQL file save for documentation
- 20 new tests in `tests/unit/semantics/test_views.py`

### Phase 3 Completion Notes (2026-01-02)
- Created `odibi/semantics/story.py` with:
  - `ViewExecutionMetadata` dataclass for per-view execution details
  - `SemanticStoryMetadata` dataclass with graph_data for lineage
  - `SemanticStoryGenerator` class for story generation
- `execute_with_story()` executes views and captures metadata
- Graph data includes nodes (tables, views) and edges for lineage
- JSON rendering with `to_dict()` for serialization
- HTML rendering with:
  - Summary statistics (views created, failed, duration)
  - Mermaid diagram for lineage visualization
  - Collapsible SQL sections for each view
- Story saving to local filesystem or remote storage
- 16 new tests in `tests/unit/semantics/test_story.py`

### Phase 4 Completion Notes (2026-01-02)
- Created `odibi/story/lineage.py` with:
  - `LineageNode`, `LineageEdge`, `LayerInfo` dataclasses
  - `LineageResult` for combined lineage output
  - `LineageGenerator` class for story stitching
- `generate()` reads all story JSONs for a date and combines graph_data
- Node/edge deduplication across multiple stories
- Layer sorting by medallion order (raw→bronze→silver→gold→semantic)
- Layer inference from node IDs (e.g., "vw_" prefix → semantic)
- JSON output with all nodes, edges, and layer links
- Interactive HTML with:
  - Mermaid flowchart with color-coded layers
  - Legend for layer colors
  - Clickable links to individual story files
  - Summary statistics (layers, nodes, edges, success rate)
- Support for remote storage via `write_file` callback
- 27 new tests in `tests/unit/story/test_lineage.py`

### Phase 5 Completion Notes (2026-01-02)
- Updated `odibi/utils/config_loader.py`:
  - Added `_merge_semantic_config()` for semantic-aware merging
  - Semantic lists (metrics, dimensions, views, materializations) are appended
  - Enables accumulating semantic configs from multiple imports
- Created `odibi/semantics/runner.py` with:
  - `SemanticLayerRunner` class for orchestrated execution
  - `run_semantic_layer()` convenience function
  - Parses semantic config from ProjectConfig
  - Executes views with story generation
  - Optional combined lineage generation
- Updated `odibi/semantics/__init__.py` with new exports:
  - ViewConfig, ViewGenerator, ViewResult, ViewExecutionResult
  - SemanticStoryGenerator, SemanticStoryMetadata
  - SemanticLayerRunner, run_semantic_layer, parse_semantic_config
- Updated `odibi/story/__init__.py` with LineageGenerator, LineageResult

---

## Testing Strategy

### Phase 1 Tests
- Derived metric resolves components correctly
- Generated SQL aggregates then applies formula
- NULLIF prevents division by zero

### Phase 2 Tests
- View DDL generates valid SQL Server syntax
- SQL file written to ADLS with correct header
- View created successfully in SQL Server

### Phase 3 Tests
- Semantic story JSON matches expected schema
- Graph data includes all views and source tables
- HTML renders correctly

### Phase 4 Tests
- Lineage reads stories from multiple pipelines
- Edges connect correctly across layers
- HTML diagram is interactive with working links

---

## Success Criteria

1. **Analyst asks "how is OEE calculated?"**
   - Point to YAML definition OR
   - Share the `.sql` file from ADLS

2. **Monthly OEE is mathematically correct**
   - Components summed first, then ratio applied
   - Not averaged

3. **Full traceability exists**
   - raw → bronze → silver → gold → views
   - One lineage.html shows everything

4. **Handoff-ready when you leave**
   - Views keep working
   - SQL files document the logic
   - Next person can understand the flow
