# Odibi MCP Tool Redesign: Safe Pipeline Construction for AI Agents

**Author:** Henry Odibi  
**Status:** Design Document  
**Created:** March 2026  
**Version:** 1.0  

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Goal & Purpose](#2-goal--purpose)
3. [Problem Analysis](#3-problem-analysis)
4. [Design Principles](#4-design-principles)
5. [Architecture Overview](#5-architecture-overview)
6. [Current State Inventory](#6-current-state-inventory)
7. [Phase 1: The 80% Solution](#7-phase-1-the-80-solution)
8. [Phase 2: Incremental Builder](#8-phase-2-incremental-builder)
9. [Phase 3: Smart Chaining & Templates](#9-phase-3-smart-chaining--templates)
10. [Error Response Contract](#10-error-response-contract)
11. [Cheap Model Compatibility](#11-cheap-model-compatibility)
12. [Agent Workflow Examples](#12-agent-workflow-examples)
13. [Existing Infrastructure to Leverage](#13-existing-infrastructure-to-leverage)
14. [File & Module Layout](#14-file--module-layout)
15. [Testing Strategy](#15-testing-strategy)
16. [Success Criteria](#16-success-criteria)
17. [Anti-Patterns to Avoid](#17-anti-patterns-to-avoid)

---

## 1. Executive Summary

The Odibi MCP server currently has **30+ disabled tools** with comments like "generates wrong YAML." The server is effectively read-only—useful for data discovery but unable to help AI agents build valid pipeline configurations. The root cause is that agents generate raw YAML strings instead of calling typed construction APIs.

This document describes a phased redesign that replaces free-form YAML generation with **typed, deterministic MCP tools** backed by Odibi's own Pydantic models. The goal: make it structurally impossible for any AI model—including cheap ones like GPT-4o-mini or Claude Haiku—to produce invalid pipeline YAML.

**One sentence:** Agents should never write YAML. They call typed tools; YAML falls out at the end.

---

## 2. Goal & Purpose

### The Problem We're Solving

When an AI agent tries to build an Odibi pipeline today, it must:

1. Read documentation about YAML structure
2. Remember correct field names (`read:` not `source:`, `query:` not `sql:`)
3. Remember valid enum values (`overwrite`, `append`, `upsert`, `append_once`, `merge`)
4. Know all 56+ transformer names and their parameters
5. Know which of the 6 patterns requires which fields
6. Hand-write YAML with correct indentation
7. Hope validation passes

This fails **constantly**. The `CRITICAL_CONTEXT` prose injection in `knowledge.py` is a bandage that cheap models ignore entirely.

### The Solution

Build an MCP tool surface where:

- **Agents never write YAML.** They pass JSON parameters to typed MCP tools.
- **Every tool parameter is constrained.** Enums, required fields, typed objects—all enforced by MCP input schemas.
- **YAML is only generated at the end** by serializing validated Pydantic model instances.
- **Errors are structured and actionable.** Not prose strings, but machine-readable objects with `field_path`, `allowed_values`, and `fix` instructions.

### The Ultimate Test

> Can a GPT-4o-mini agent, with no prior training on Odibi, call `list_patterns` → `apply_pattern_template` and get a working pipeline YAML on the first try?

If yes, the tool surface is correct.

---

## 3. Problem Analysis

### 3.1 What's Broken Today

| Problem | Root Cause | Evidence |
|---------|-----------|----------|
| 30+ disabled MCP tools | String template YAML generation drifted from Pydantic schema | `# REMOVED: generates wrong YAML` comments in `server.py` |
| `CRITICAL_CONTEXT` injection | Prose instructions to fix YAML syntax errors retroactively | `knowledge.py` lines 28-108: tells AI "NEVER_USE: source:, sink:" |
| Wrong field names | Agents hallucinate `source:`, `sink:`, `sql:`, `inputs:`, `outputs:` | `_validate_nodes()` in `yaml_builder.py` explicitly checks for these |
| Wrong enum values | Agents guess `replace` instead of `overwrite` for write mode | Free-text string params with no constraint |
| Pattern confusion | No tool tells agents what fields each pattern requires | Discovery tools suggest patterns but provide no construction path |
| Transformer hallucination | Agents invent function names that don't exist in `FunctionRegistry` | No discovery-first workflow enforced |

### 3.2 What Works Today

| Working Component | Location | What It Does |
|-------------------|----------|--------------|
| Smart Discovery tools | `odibi_mcp/tools/smart.py` | `map_environment`, `profile_source`, `profile_folder` with SmartResponse pattern |
| SmartResponse contracts | `odibi_mcp/contracts/smart.py` | Typed dataclasses with `next_step`, `ready_for` chaining fields |
| FunctionRegistry | `odibi/registry.py` | All 56+ transformers registered with Pydantic param models |
| Config Pydantic models | `odibi/config.py` (~4500 lines) | `ProjectConfig`, `PipelineConfig`, `NodeConfig`, `ReadConfig`, `WriteConfig`, etc. |
| Template generator | `odibi/tools/templates.py` | Generates YAML templates from Pydantic models for CLI |
| YAML validation | `odibi_mcp/tools/yaml_builder.py` | `validate_odibi_config()` with structured error output |
| `generate_project_yaml` | `odibi_mcp/tools/yaml_builder.py` | Builds project.yaml from typed params (working tool) |
| `ready_for` chaining | SmartResponse pattern | Tools return pre-filled params for the next tool call |

### 3.3 The 14 Currently Active MCP Tools

```
Discovery:     map_environment, profile_source, profile_folder
Download:      download_sql, download_table, download_file
Validation:    validate_yaml
Construction:  generate_project_yaml
Diagnostics:   diagnose, diagnose_path
Observability: lineage_graph, story_read, node_sample, node_failed_rows
```

Everything else is commented out or disabled.

---

## 4. Design Principles

### Principle 1: Pydantic Models Are the Source of Truth

Every MCP tool that constructs pipeline configuration must build `odibi.config` Pydantic model instances. Never raw dicts. Never string templates. The Pydantic models in `config.py` define what is valid—tools must go through them.

### Principle 2: Agents Select from Lists, Never Invent

Every string parameter an agent provides must come from a prior discovery call or be constrained by an enum in the MCP input schema. If a transformer name doesn't appear in `list_transformers` output, it can't be used. If a write mode isn't in the enum, it can't be set.

### Principle 3: Fail Fast with Actionable Errors

When validation fails, return structured errors with:
- **What's wrong:** field path and error code
- **What was expected:** type, enum values, constraints
- **How to fix it:** concrete suggested repair action

Never return raw Pydantic `ValidationError` traceback strings.

### Principle 4: Round-Trip Validation

`render_pipeline_yaml` must re-parse its own YAML output through the Pydantic model before returning it. If our own output doesn't validate, we have a bug—not the agent.

### Principle 5: Progressive Disclosure

Don't overwhelm agents with all 56 transformers and all 11 validation types upfront. Let them discover what's available via `list_*` tools, then drill into specifics via `describe_*` tools.

### Principle 6: Cheap Model Friendly

Design for the weakest model in the chain. If Claude Haiku can use the tools correctly, any model can. This means: enum-constrained params, no prose dependencies, structured everything.

---

## 5. Architecture Overview

### Current Architecture (Broken)

```
Agent → writes YAML string → validate_yaml → fix errors → iterate
         ↑ ignores
   CRITICAL_CONTEXT prose
```

### Target Architecture

```
Agent → list_patterns()           → sees "dimension" with requirements
      → list_transformers()       → sees available functions + param schemas
      → apply_pattern_template()  → passes typed JSON params
           ↓
      Pydantic models constructed → ProjectConfig/PipelineConfig/NodeConfig
           ↓
      render_pipeline_yaml()      → serialize + round-trip validate
           ↓
      Valid YAML output           → ready to run with PipelineManager
```

### Config Model Hierarchy

```
ProjectConfig
├── project: str
├── engine: EngineType (spark | pandas | polars)
├── connections: Dict[str, ConnectionConfig]
│   ├── LocalConnection
│   ├── AzureBlobConnection
│   ├── SQLServerConnection
│   ├── DeltaConnection
│   └── HTTPConnection
├── pipelines: List[PipelineConfig]
│   ├── pipeline: str (name)
│   ├── layer: str (bronze | silver | gold)
│   ├── pattern: str (dimension | fact | scd2 | merge | aggregation | date_dimension)
│   └── nodes: List[NodeConfig]
│       ├── name: str (alphanumeric + underscore only)
│       ├── read: ReadConfig
│       │   ├── connection: str
│       │   ├── format: str (csv | parquet | json | delta | sql | excel | avro)
│       │   ├── path: str | table: str | query: str
│       │   └── options: Dict
│       ├── transform: TransformConfig
│       │   └── steps: List[TransformStep]
│       │       ├── function: str (from FunctionRegistry)
│       │       ├── params: Dict
│       │       └── sql: str (raw SQL alternative)
│       ├── write: WriteConfig
│       │   ├── connection: str
│       │   ├── format: str
│       │   ├── path: str | table: str
│       │   ├── mode: WriteMode (overwrite | append | upsert | append_once | merge)
│       │   └── keys: List[str] (for upsert/merge/append_once)
│       ├── validation: ValidationConfig
│       │   ├── rules: List[ValidationRule] (11 types, discriminated union)
│       │   ├── on_fail: str (quarantine | fail | warn)
│       │   └── quarantine_path: str
│       ├── transformer: str (top-level pattern transformer)
│       ├── params: Dict (top-level pattern params)
│       ├── depends_on: List[str]
│       └── incremental: IncrementalConfig
├── story: StoryConfig
│   ├── connection: str
│   └── path: str
└── system: SystemConfig
    ├── connection: str
    └── path: str
```

---

## 6. Current State Inventory

### 6.1 FunctionRegistry (`odibi/registry.py`)

The FunctionRegistry is ready for MCP exposure. Key methods:

| Method | What It Does | MCP-Ready? |
|--------|-------------|------------|
| `list_functions()` | Returns all 56+ registered transformer names | ✅ Yes |
| `get_function_info(name)` | Returns docstring, params, types, defaults | ✅ Yes |
| `get_param_model(name)` | Returns Pydantic model for params (if registered) | ✅ Yes |
| `validate_params(name, params)` | Validates params against model or signature | ✅ Yes |
| `has_function(name)` | Checks if a function exists | ✅ Yes |

### 6.2 Six Warehouse Patterns (`odibi/patterns/`)

| Pattern | File | Required Config |
|---------|------|----------------|
| `dimension` | `patterns/dimension.py` | `keys`, write mode `overwrite` |
| `fact` | `patterns/fact.py` | `keys`, write mode `append` or `upsert` |
| `scd2` | `patterns/scd2.py` | `key_columns`, `tracked_columns`, `effective_date_column`, `end_date_column`, `current_flag_column` |
| `merge` | `patterns/merge.py` | `merge_keys`, `update_columns` |
| `aggregation` | `patterns/aggregation.py` | `group_by`, `aggregations` |
| `date_dimension` | `patterns/date_dimension.py` | `start_date`, `end_date` |

### 6.3 Validation Test Types (Discriminated Union)

Defined in `config.py` as `TestConfig = Annotated[Union[...], Field(discriminator="type")]`. **Do not hardcode this list — introspect it at runtime** (see Section 18.2).

Current members of the union (as of this writing):

| Class | `type` discriminator | Description |
|-------|---------------------|-------------|
| `NotNullTest` | `not_null` | Column must not contain NULL |
| `UniqueTest` | `unique` | Column values must be unique |
| `AcceptedValuesTest` | `accepted_values` | Value in allowed set |
| `RowCountTest` | `row_count` | Table-level row count check |
| `CustomSQLTest` | `custom_sql` | SQL expression evaluates TRUE |
| `RangeTest` | `range` | Numeric within bounds |
| `RegexMatchTest` | `regex_match` | String matches regex |
| `VolumeDropTest` | `volume_drop` | Detect unexpected volume drops |
| `SchemaContract` | `schema_contract` | Enforce expected schema |
| `DistributionContract` | `distribution_contract` | Statistical distribution checks |
| `FreshnessContract` | `freshness_contract` | Data freshness/timeliness |

**Note:** This list is derived from `get_args(TestConfig)`. MCP tools must introspect the union, not hardcode these names.

### 6.4 SmartResponse Pattern (`odibi_mcp/contracts/smart.py`)

The existing contracts define a response pattern that includes:
- `confidence: float` — 0.0–1.0 quality score
- `warnings: List[str]` — non-fatal issues
- `errors: List[str]` — fatal issues
- `next_step: str` — suggested next tool to call
- `ready_for: Dict[str, Any]` — pre-filled params for the next tool

New builder tools should follow this same contract pattern.

---

## 7. Phase 1: The 80% Solution

**Timeline:** 1–2 weeks  
**Goal:** Handle the most common pipeline types with 4 new tools. An agent can go from "I have a SQL table" to "here's a working pipeline YAML" in 2–3 tool calls.

### 7.1 Tool: `list_transformers`

**Purpose:** Expose the FunctionRegistry data so agents discover what's available instead of hallucinating transformer names.

**MCP Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "category": {
      "type": "string",
      "description": "Optional filter: 'scd', 'merge', 'column', 'filter', 'join', 'aggregate', 'all'",
      "enum": ["scd", "merge", "column", "filter", "join", "aggregate", "manufacturing", "all"]
    },
    "search": {
      "type": "string",
      "description": "Optional search term to filter by name or description"
    }
  },
  "required": []
}
```

**Response Contract:**
```json
{
  "transformers": [
    {
      "name": "deduplicate",
      "description": "Remove duplicate rows based on key columns",
      "category": "filter",
      "parameters": {
        "columns": {"type": "list[str]", "required": true, "description": "Columns to deduplicate on"},
        "keep": {"type": "str", "required": false, "default": "first", "allowed_values": ["first", "last"]}
      },
      "example_yaml": "- function: deduplicate\n  params:\n    columns: [id]\n    keep: first"
    }
  ],
  "count": 56,
  "categories": {"scd": 3, "merge": 2, "column": 15, "filter": 8, ...}
}
```

**Implementation:**
```python
def list_transformers(category: str = "all", search: str = "") -> dict:
    from odibi.transformers import register_standard_library
    register_standard_library()

    functions = FunctionRegistry.list_functions()
    result = []
    for name in sorted(functions):
        info = FunctionRegistry.get_function_info(name)
        param_model = FunctionRegistry.get_param_model(name)

        # Build param schema from Pydantic model if available
        params_schema = {}
        if param_model:
            for field_name, field_info in param_model.model_fields.items():
                params_schema[field_name] = {
                    "type": str(field_info.annotation),
                    "required": field_info.is_required(),
                    "default": field_info.default if not field_info.is_required() else None,
                    "description": field_info.description or "",
                }

        result.append({
            "name": name,
            "description": info.get("docstring", ""),
            "parameters": params_schema,
        })

    return {"transformers": result, "count": len(result)}
```

### 7.2 Tool: `list_patterns`

**Purpose:** Return the 6 warehouse patterns with their required config fields, so agents know exactly what parameters each pattern needs.

**MCP Input Schema:**
```json
{
  "type": "object",
  "properties": {},
  "required": []
}
```

**Response Contract:**
```json
{
  "patterns": [
    {
      "name": "dimension",
      "description": "Slowly changing dimension (Type 1). Full replace on each run.",
      "use_when": "Reference/lookup data that changes infrequently (customers, products, locations)",
      "required_params": {
        "keys": {"type": "list[str]", "description": "Business key columns for the dimension"},
        "write_mode": {"fixed_value": "overwrite", "description": "Always overwrite for dimensions"}
      },
      "optional_params": {
        "tracked_columns": {"type": "list[str]", "description": "Columns to track for change detection"}
      },
      "example_call": {
        "tool": "apply_pattern_template",
        "params": {
          "pattern": "dimension",
          "source_connection": "my_sql",
          "source_table": "dbo.DimCustomer",
          "target_connection": "local",
          "target_path": "gold/dim_customer",
          "keys": ["customer_id"]
        }
      }
    },
    {
      "name": "scd2",
      "description": "Slowly Changing Dimension Type 2. Tracks history by versioning rows.",
      "use_when": "Need full history of changes (employee records, pricing history)",
      "required_params": {
        "key_columns": {"type": "list[str]", "description": "Business key columns"},
        "tracked_columns": {"type": "list[str]", "description": "Columns to track for changes"},
        "effective_date_column": {"type": "str", "description": "Column name for row effective date"},
        "end_date_column": {"type": "str", "description": "Column name for row end date"},
        "current_flag_column": {"type": "str", "description": "Column name for is-current flag"}
      }
    },
    {
      "name": "fact",
      "description": "Fact table for transactional/event data.",
      "use_when": "High-volume transactional data (orders, shipments, logs)"
    },
    {
      "name": "merge",
      "description": "Merge/upsert pattern using match keys.",
      "use_when": "Need to update existing rows and insert new ones"
    },
    {
      "name": "aggregation",
      "description": "Pre-computed aggregation/summary table.",
      "use_when": "Building summary tables, KPI tables, rollups"
    },
    {
      "name": "date_dimension",
      "description": "Generate a date dimension table with calendar attributes.",
      "use_when": "Need a standard date dimension for time-based analysis"
    }
  ]
}
```

### 7.3 Tool: `apply_pattern_template`

**Purpose:** The single most important tool. Takes a pattern name and business parameters, constructs Pydantic model instances internally, and returns valid YAML. One tool call → one working pipeline.

**MCP Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "pattern": {
      "type": "string",
      "enum": ["dimension", "fact", "scd2", "merge", "aggregation", "date_dimension"],
      "description": "Warehouse pattern to apply"
    },
    "pipeline_name": {
      "type": "string",
      "description": "Name for the pipeline (alphanumeric + underscore)"
    },
    "layer": {
      "type": "string",
      "enum": ["bronze", "silver", "gold"],
      "default": "gold",
      "description": "Pipeline layer"
    },
    "source_connection": {
      "type": "string",
      "description": "Connection name for reading data"
    },
    "source_table": {
      "type": "string",
      "description": "Table name (e.g., 'dbo.DimCustomer') or file path"
    },
    "source_format": {
      "type": "string",
      "enum": ["sql", "csv", "parquet", "json", "delta", "excel", "avro"],
      "default": "sql",
      "description": "Source data format"
    },
    "source_query": {
      "type": "string",
      "description": "Optional SQL query (overrides source_table if provided)"
    },
    "target_connection": {
      "type": "string",
      "description": "Connection name for writing data"
    },
    "target_path": {
      "type": "string",
      "description": "Output path or table name"
    },
    "target_format": {
      "type": "string",
      "enum": ["delta", "parquet", "csv", "json"],
      "default": "delta",
      "description": "Output format"
    },
    "keys": {
      "type": "array",
      "items": {"type": "string"},
      "description": "Business key columns (required for most patterns)"
    },
    "tracked_columns": {
      "type": "array",
      "items": {"type": "string"},
      "description": "Columns to track for change detection (SCD2, merge)"
    },
    "transforms": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "function": {"type": "string", "description": "Transformer name (from list_transformers)"},
          "params": {"type": "object", "description": "Transformer parameters"}
        },
        "required": ["function"]
      },
      "description": "Optional transform steps to apply"
    },
    "validation_rules": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "type": {
            "type": "string",
            "enum": ["not_null", "unique", "range", "regex", "in_list", "not_in_list", "custom_sql", "foreign_key", "date_range", "length", "row_count"]
          },
          "column": {"type": "string"},
          "params": {"type": "object"}
        },
        "required": ["type"]
      },
      "description": "Optional data validation rules"
    },
    "incremental": {
      "type": "object",
      "properties": {
        "column": {"type": "string"},
        "mode": {"type": "string", "enum": ["rolling_window", "append", "high_watermark"]},
        "lookback": {"type": "integer"},
        "unit": {"type": "string", "enum": ["day", "hour", "minute"]}
      },
      "description": "Optional incremental loading config"
    }
  },
  "required": ["pattern", "pipeline_name", "source_connection", "source_table", "target_connection", "target_path"]
}
```

**Implementation Strategy:**

1. Sanitize `pipeline_name` and compute node name
2. Build `ReadConfig` Pydantic model from source params
3. Build `WriteConfig` Pydantic model from target params, with pattern-appropriate defaults:
   - `dimension` → `mode: overwrite`
   - `fact` → `mode: append` or `upsert` (if keys provided)
   - `scd2` → uses transformer, not write mode
   - `merge` → `mode: upsert`, requires keys
   - `aggregation` → `mode: overwrite`
   - `date_dimension` → `mode: overwrite`, no source (generated)
4. Build `TransformConfig` if transforms provided, validating each function against `FunctionRegistry`
5. Build `ValidationConfig` if rules provided
6. Build pattern-specific params (`transformer` + `params` on NodeConfig)
7. Construct `NodeConfig` → `PipelineConfig`
8. Serialize to YAML
9. **Round-trip validate:** re-parse the YAML through `PipelineConfig` model
10. Return the YAML + metadata

**Response Contract:**
```json
{
  "yaml": "pipelines:\n  - pipeline: dim_customer\n    ...",
  "pipeline_name": "dim_customer",
  "pattern": "dimension",
  "node_count": 1,
  "validated": true,
  "warnings": [],
  "next_step": "Save this YAML and run: odibi run dim_customer",
  "ready_for": {
    "validate_pipeline": {"yaml_content": "..."}
  }
}
```

### 7.4 Tool: `validate_pipeline`

**Purpose:** Replace the existing `validate_yaml` with a structured Pydantic-powered validator that returns machine-readable errors with fix instructions.

**MCP Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "yaml_content": {
      "type": "string",
      "description": "YAML content to validate"
    },
    "check_connections": {
      "type": "boolean",
      "default": false,
      "description": "Also validate that connection names exist in the project"
    },
    "check_transformers": {
      "type": "boolean",
      "default": true,
      "description": "Validate transformer names against FunctionRegistry"
    }
  },
  "required": ["yaml_content"]
}
```

**Response Contract:**
```json
{
  "valid": true,
  "pipeline_count": 1,
  "node_count": 3,
  "summary": "1 pipeline, 3 nodes, all valid"
}
```

Or on failure:
```json
{
  "valid": false,
  "errors": [
    {
      "field_path": "pipelines[0].nodes[0].read.format",
      "code": "MISSING_REQUIRED",
      "message": "read config missing 'format'",
      "expected_type": "string",
      "allowed_values": ["csv", "parquet", "json", "delta", "sql", "excel", "avro"],
      "fix": "Add 'format: sql' for SQL sources, or 'format: csv' for CSV files"
    },
    {
      "field_path": "pipelines[0].nodes[0].name",
      "code": "INVALID_NODE_NAME",
      "message": "Node name 'my-node' contains invalid characters",
      "fix": "Use 'my_node' (alphanumeric + underscore only)"
    },
    {
      "field_path": "pipelines[0].nodes[1].transform.steps[0].function",
      "code": "UNKNOWN_TRANSFORMER",
      "message": "Transformer 'dedup' not found in FunctionRegistry",
      "fix": "Did you mean 'deduplicate'? Use list_transformers to see all available functions."
    }
  ],
  "warnings": [
    {
      "field_path": "pipelines[0].nodes[0]",
      "code": "MISSING_VALIDATION",
      "message": "Node 'customer_dim' has no validation rules. Consider adding not_null + unique on key columns."
    }
  ]
}
```

**Implementation:** Parse YAML → attempt to construct Pydantic models → catch `ValidationError` → map each error to structured output with field path, code, and fix suggestion. Additionally check transformer names against `FunctionRegistry.has_function()`.

---

## 8. Phase 2: Incremental Builder

**Timeline:** 2–3 weeks after Phase 1  
**Goal:** Handle the 20% of pipelines that don't fit a pattern template—custom multi-node pipelines with complex transform chains, multiple joins, conditional logic.

### 8.1 Stateful Builder Architecture

Phase 2 introduces **server-side state**: an in-memory dictionary of pipeline configurations being built. Each pipeline has a unique session ID.

```python
# Server-side state
_builder_sessions: Dict[str, PipelineBuilderState] = {}

@dataclass
class PipelineBuilderState:
    session_id: str
    pipeline_name: str
    layer: str
    pattern: Optional[str]
    nodes: Dict[str, NodeBuilderState]  # keyed by node name
    created_at: datetime
    last_modified: datetime

@dataclass
class NodeBuilderState:
    name: str
    read: Optional[ReadConfig] = None
    transform: Optional[TransformConfig] = None
    write: Optional[WriteConfig] = None
    validation: Optional[ValidationConfig] = None
    depends_on: List[str] = field(default_factory=list)
    transformer: Optional[str] = None
    params: Optional[Dict] = None
    incremental: Optional[IncrementalConfig] = None
```

### 8.2 Builder Tools

#### `create_pipeline`

Initialize a new pipeline builder session.

**Input:**
```json
{
  "pipeline_name": "silver_orders",
  "layer": "silver",
  "pattern": "dimension"
}
```

**Output:**
```json
{
  "session_id": "bld_a1b2c3d4",
  "pipeline_name": "silver_orders",
  "status": "empty",
  "next_step": "add_node",
  "ready_for": {"add_node": {"session_id": "bld_a1b2c3d4"}}
}
```

#### `add_node`

Add a node skeleton to the pipeline.

**Input:**
```json
{
  "session_id": "bld_a1b2c3d4",
  "node_name": "raw_orders",
  "depends_on": []
}
```

**Output:**
```json
{
  "session_id": "bld_a1b2c3d4",
  "node_name": "raw_orders",
  "status": "skeleton",
  "missing": ["read", "write"],
  "next_step": "configure_read",
  "ready_for": {"configure_read": {"session_id": "bld_a1b2c3d4", "node_name": "raw_orders"}}
}
```

#### `configure_read`

Set the read configuration for a node.

**Input:**
```json
{
  "session_id": "bld_a1b2c3d4",
  "node_name": "raw_orders",
  "connection": "wwi",
  "format": "sql",
  "table": "Sales.Orders",
  "options": {}
}
```

Internally constructs a `ReadConfig` Pydantic model. If validation fails, returns structured errors immediately—the agent doesn't have to wait until `render_pipeline_yaml`.

#### `configure_write`

Set the write configuration for a node.

**Input:**
```json
{
  "session_id": "bld_a1b2c3d4",
  "node_name": "raw_orders",
  "connection": "local",
  "format": "delta",
  "path": "silver/orders",
  "mode": "upsert",
  "keys": ["order_id"]
}
```

`mode` is enum-constrained: `overwrite | append | upsert | append_once | merge`.

#### `configure_transform`

Add transform steps to a node.

**Input:**
```json
{
  "session_id": "bld_a1b2c3d4",
  "node_name": "raw_orders",
  "steps": [
    {"function": "trim_whitespace", "params": {"columns": ["customer_name"]}},
    {"function": "cast_columns", "params": {"mappings": {"order_date": "date"}}}
  ]
}
```

Each `function` is validated against `FunctionRegistry.has_function()`. Each `params` dict is validated against `FunctionRegistry.validate_params()`. Invalid transformer names return the error immediately with the closest matching name (fuzzy match).

#### `configure_validation`

Add validation rules to a node.

**Input:**
```json
{
  "session_id": "bld_a1b2c3d4",
  "node_name": "raw_orders",
  "rules": [
    {"type": "not_null", "column": "order_id"},
    {"type": "unique", "column": "order_id"},
    {"type": "range", "column": "quantity", "min": 0, "max": 100000}
  ],
  "on_fail": "quarantine",
  "quarantine_path": "_quarantine/orders"
}
```

`type` is enum-constrained to the 11 validation types. `on_fail` is constrained to `quarantine | fail | warn`.

#### `get_pipeline_state`

Inspect the current state of a builder session.

**Input:**
```json
{
  "session_id": "bld_a1b2c3d4"
}
```

**Output:** Returns the full pipeline configuration as currently built, plus a `completeness` field indicating what's missing from each node.

```json
{
  "session_id": "bld_a1b2c3d4",
  "pipeline_name": "silver_orders",
  "completeness": {
    "raw_orders": {
      "read": "configured",
      "write": "configured",
      "transform": "configured",
      "validation": "not_configured"
    }
  },
  "ready_to_render": true,
  "config_preview": { ... }
}
```

#### `render_pipeline_yaml`

Serialize the builder state to validated YAML.

**Input:**
```json
{
  "session_id": "bld_a1b2c3d4",
  "include_project": false
}
```

**Behavior:**
1. Collect all node configs from the builder state
2. Construct `PipelineConfig` Pydantic model
3. If `include_project` is true, wrap in `ProjectConfig` (requires connections)
4. Serialize to YAML
5. **Round-trip validate:** re-parse the YAML output through the Pydantic model
6. If round-trip fails, return an error (this is a framework bug, not an agent bug)
7. Clean up the builder session

**Output:**
```json
{
  "yaml": "pipelines:\n  - pipeline: silver_orders\n    ...",
  "validated": true,
  "round_trip_passed": true,
  "node_count": 3,
  "warnings": [],
  "session_closed": true
}
```

### 8.3 Builder Session Management

- Sessions auto-expire after 30 minutes of inactivity
- Maximum 10 concurrent sessions per MCP server instance
- `list_sessions` tool to see active builder sessions
- `discard_pipeline` tool to explicitly close a session without rendering

---

## 9. Phase 3: Smart Chaining & Templates

**Timeline:** 2–3 weeks after Phase 2  
**Goal:** Wire the discovery tools directly into the builder tools for a 2-call onboarding workflow. Add convenience template tools for common patterns.

### 9.1 Smart Chaining: `profile_source` → `apply_pattern_template`

The existing `profile_source` tool returns a `ready_for` dict. Today that dict points to the (disabled) `generate_bronze_node` tool. In Phase 3, it points to `apply_pattern_template`:

```json
{
  "ready_for": {
    "apply_pattern_template": {
      "pattern": "dimension",
      "source_connection": "wwi",
      "source_table": "Dimension.Customer",
      "source_format": "sql",
      "keys": ["CustomerKey"],
      "target_connection": "local",
      "target_path": "gold/dim_customer",
      "target_format": "delta"
    }
  }
}
```

The agent literally copies the `ready_for` dict and passes it to `apply_pattern_template`. Two tool calls: profile → build.

### 9.2 Bulk Ingestion Tool: `create_ingestion_pipeline`

The one template tool worth building (because it handles a genuinely different shape — multiple tables in one call):

```json
{
  "pipeline_name": "bronze_wwi",
  "source_connection": "wwi",
  "target_connection": "local",
  "tables": [
    {"schema": "Sales", "table": "Orders"},
    {"schema": "Sales", "table": "OrderLines"},
    {"schema": "Dimension", "table": "Customer", "primary_key": ["CustomerKey"]}
  ]
}
```

Reuses existing `generate_sql_pipeline` logic from `yaml_builder.py` but with Pydantic model construction.

**No per-pattern template tools** (`create_dimension_pipeline`, `create_scd2_pipeline`, etc.). `apply_pattern_template` already handles all patterns via the `pattern` enum. Adding per-pattern tools scales linearly and provides no value — see Section 18.4.

### 9.3 Auto-Suggestion: `suggest_pipeline`

Takes a `profile_source` result and suggests the best pattern + configuration:

**Input:**
```json
{
  "profile_result": { ... },
  "target_connection": "local",
  "target_base_path": "gold/"
}
```

**Output:**
```json
{
  "suggested_pattern": "scd2",
  "reason": "Table has candidate key 'EmployeeKey', timestamp columns 'ModifiedDate', and high-cardinality text columns that change over time",
  "confidence": 0.85,
  "ready_for": {
    "apply_pattern_template": {
      "pattern": "scd2",
      "keys": ["EmployeeKey"],
      "tracked_columns": ["Title", "Department"],
      ...
    }
  }
}
```

---

## 10. Error Response Contract

All tools follow the same error response format. This is critical for cheap model compatibility—the model doesn't need to parse different error formats.

### 10.1 Validation Error

```json
{
  "valid": false,
  "errors": [
    {
      "field_path": "pipelines[0].nodes[0].write.mode",
      "code": "INVALID_ENUM",
      "message": "'replace' is not a valid WriteMode",
      "expected_type": "WriteMode",
      "allowed_values": ["overwrite", "append", "upsert", "append_once", "merge"],
      "fix": "Change mode to 'overwrite' for dimension patterns"
    }
  ],
  "warnings": []
}
```

### 10.2 Error Codes

| Code | Meaning |
|------|---------|
| `MISSING_REQUIRED` | A required field is missing |
| `INVALID_ENUM` | Value not in allowed enum |
| `INVALID_TYPE` | Wrong type (e.g., string where list expected) |
| `INVALID_NODE_NAME` | Node name has invalid characters |
| `UNKNOWN_TRANSFORMER` | Transformer not in FunctionRegistry |
| `INVALID_TRANSFORMER_PARAMS` | Transformer params don't match model |
| `UNKNOWN_CONNECTION` | Connection name not found in project |
| `WRONG_KEY` | Using wrong YAML key (e.g., `source:` instead of `read:`) |
| `MISSING_FORMAT` | Read or write config missing `format` |
| `PATTERN_REQUIRES` | Pattern requires fields that weren't provided |
| `CIRCULAR_DEPENDENCY` | Node DAG has a cycle |
| `ROUND_TRIP_FAILED` | Generated YAML failed re-validation (framework bug) |

### 10.3 Fix Suggestions

Every error includes a `fix` field with a concrete action:

```json
{"fix": "Change mode to 'overwrite'"}
{"fix": "Add 'format: sql' for SQL sources"}
{"fix": "Use 'my_node' instead of 'my-node' (alphanumeric + underscore only)"}
{"fix": "Did you mean 'deduplicate'? Use list_transformers to see available functions."}
{"fix": "SCD2 pattern requires 'key_columns', 'tracked_columns', 'effective_date_column'"}
```

---

## 11. Cheap Model Compatibility

### 11.1 Design Strategies

| Strategy | What It Does | Why It Helps Cheap Models |
|----------|-------------|--------------------------|
| Enum constraints in MCP input schema | `"enum": ["dimension", "fact", "scd2", ...]` | Model picks from a list, can't hallucinate values |
| `list_*` discovery tools | Return what's available before the model needs to use it | Eliminates guessing |
| `ready_for` chaining | Previous tool returns pre-filled params for next tool | Model copies JSON, no synthesis needed |
| Structured errors with `fix` | Every error says exactly how to fix it | Model follows instructions, doesn't need to reason |
| No YAML handling | Model passes JSON params, gets YAML back | No indentation, no quoting, no YAML syntax |
| Progressive disclosure | `list_transformers` → then `describe_transformer` | Don't overwhelm with 56 transformers at once |
| Required vs optional | MCP schema marks required params | Model knows what's mandatory |

### 11.2 Model Tiers and Expected Behavior

| Tier | Models | Expected Capability |
|------|--------|-------------------|
| **Tier 1 (Strong)** | GPT-4o, Claude Sonnet, Gemini Pro | Full builder workflow, multi-node pipelines |
| **Tier 2 (Medium)** | GPT-4o-mini, Claude Haiku, Gemini Flash | Pattern templates, 1-2 tool call workflows |
| **Tier 3 (Weak)** | Open source 7B models | `list_patterns` → `apply_pattern_template` only |

The Phase 1 tools are designed so that even Tier 3 models can produce valid pipelines: call `apply_pattern_template` with the pattern name and source/target details. One tool call, one valid pipeline.

---

## 12. Agent Workflow Examples

### 12.1 Bronze Ingestion (Phase 1 — 3 tool calls)

```
Agent → map_environment("wwi", "")
      ← {schemas: [{name: "Sales", tables: ["Orders", "OrderLines"]}]}

Agent → profile_source("wwi", "Sales.Orders")
      ← {schema: [...], candidate_keys: ["OrderID"], ready_for: {apply_pattern_template: {...}}}

Agent → apply_pattern_template(pattern="fact", source_connection="wwi", ...)
      ← {yaml: "...", validated: true}
```

### 12.2 Dimension Pipeline (Phase 1 — 2 tool calls)

```
Agent → list_patterns()
      ← {patterns: [{name: "dimension", required_params: {keys: ...}, example_call: {...}}]}

Agent → apply_pattern_template(
          pattern="dimension",
          pipeline_name="dim_customer",
          source_connection="wwi",
          source_table="Dimension.Customer",
          target_connection="local",
          target_path="gold/dim_customer",
          keys=["CustomerKey"]
        )
      ← {yaml: "...", validated: true}
```

### 12.3 Complex Multi-Node Pipeline (Phase 2 — 7 tool calls)

```
Agent → create_pipeline("silver_orders", layer="silver")
      ← {session_id: "bld_abc123"}

Agent → add_node(session_id="bld_abc123", node_name="raw_orders")
Agent → configure_read(session_id="bld_abc123", node_name="raw_orders", connection="wwi", format="sql", table="Sales.Orders")
Agent → configure_transform(session_id="bld_abc123", node_name="raw_orders", steps=[
          {function: "trim_whitespace", params: {columns: ["CustomerName"]}},
          {function: "cast_columns", params: {mappings: {OrderDate: "date"}}}
        ])
Agent → configure_write(session_id="bld_abc123", node_name="raw_orders", connection="local", format="delta", path="silver/orders", mode="upsert", keys=["OrderID"])
Agent → configure_validation(session_id="bld_abc123", node_name="raw_orders", rules=[
          {type: "not_null", column: "OrderID"},
          {type: "unique", column: "OrderID"}
        ], on_fail="quarantine")

Agent → render_pipeline_yaml(session_id="bld_abc123")
      ← {yaml: "...", validated: true, round_trip_passed: true}
```

### 12.4 Auto-Onboarding (Phase 3 — 2 tool calls)

```
Agent → profile_source("wwi", "Dimension.Employee")
      ← {
           suggestions: {suggested_pattern: "scd2", ...},
           ready_for: {apply_pattern_template: {pattern: "scd2", keys: ["EmployeeKey"], ...}}
         }

Agent → apply_pattern_template(**ready_for["apply_pattern_template"])
      ← {yaml: "...", validated: true}
```

---

## 13. Existing Infrastructure to Leverage

### 13.1 Reuse from `odibi/tools/templates.py`

The `TemplateGenerator` class already knows how to:
- Extract field descriptions from Pydantic `Field(description=...)`
- Map enum values to `"option1 | option2"` strings
- Identify required vs. optional fields
- Generate YAML templates from any Pydantic model

This can be directly used in `list_patterns` and `list_transformers` to generate accurate param schemas.

### 13.2 Reuse from `odibi_mcp/tools/yaml_builder.py`

- `_sanitize_node_name()` — node name sanitization logic
- `_build_read_config()` / `_build_write_config()` — dict construction for SQL tables
- `_validate_nodes()` / `_validate_read_config()` / `_validate_write_config()` — pre-Pydantic validation checks for common AI mistakes
- `generate_project_yaml()` — working tool, keep as-is

### 13.3 Reuse from `odibi_mcp/contracts/smart.py`

- `SmartResponse` pattern with `confidence`, `warnings`, `errors`, `next_step`, `ready_for`
- `OdibiSuggestions` with `suggested_pattern`, `suggested_transformers`
- `FixInstruction` dataclass for actionable repair instructions

### 13.4 Reuse from `odibi/registry.py`

- `FunctionRegistry.list_functions()` — enumerate all transformers
- `FunctionRegistry.get_function_info()` — docstring + param info
- `FunctionRegistry.get_param_model()` — Pydantic model for typed validation
- `FunctionRegistry.validate_params()` — param validation
- `FunctionRegistry.has_function()` — existence check

---

## 14. File & Module Layout

### New Files to Create

```
odibi_mcp/
├── tools/
│   ├── builders.py           # Phase 2: create_pipeline, add_node, configure_*, render_*
│   ├── discovery_tools.py    # Phase 1: list_transformers, list_patterns, list_validation_tests
│   ├── pattern_tools.py      # Phase 1: apply_pattern_template
│   │                         # Phase 3: create_dimension_pipeline, create_scd2_pipeline, etc.
│   └── validation_tools.py   # Phase 1: validate_pipeline (replaces validate_yaml)
├── contracts/
│   ├── smart.py              # Existing - keep
│   └── builder.py            # Phase 2: BuilderState, BuilderResponse contracts
└── builder_state.py          # Phase 2: PipelineBuilderState, session management
```

### Files to Modify

```
odibi_mcp/server.py           # Register new tools in list_tools() and call_tool()
odibi_mcp/tools/smart.py      # Phase 3: Update ready_for to point to apply_pattern_template
odibi_mcp/contracts/smart.py  # Add response contracts for new tools if needed
```

### Files NOT to Modify

```
odibi/config.py              # Source of truth - read only
odibi/registry.py            # Source of truth - read only
odibi/patterns/*.py          # Source of truth - read only
odibi/tools/templates.py     # Reuse existing functions, don't modify
```

---

## 15. Testing Strategy

### 15.1 Unit Tests

```
tests/unit/test_mcp_discovery_tools.py     # list_transformers, list_patterns
tests/unit/test_mcp_pattern_tools.py       # apply_pattern_template
tests/unit/test_mcp_validation_tools.py    # validate_pipeline
tests/unit/test_mcp_builders.py            # Phase 2: builder tools
tests/unit/test_mcp_builder_state.py       # Phase 2: session management
```

### 15.2 Key Test Cases

**`list_transformers`:**
- Returns all registered transformers (count ≥ 50)
- Each entry has `name`, `description`, `parameters`
- Category filter works
- Search filter works

**`list_patterns`:**
- Returns exactly 6 patterns
- Each has `required_params` and `example_call`
- `example_call` params match `apply_pattern_template` input schema

**`apply_pattern_template`:**
- ✅ Each pattern produces valid YAML that round-trip validates
- ✅ Missing required params (e.g., no `keys` for dimension) returns structured error
- ✅ Invalid pattern name returns error with allowed values
- ✅ Invalid transformer name in `transforms` returns error with closest match
- ✅ Node name sanitization works (hyphens → underscores)
- ✅ `source_query` overrides `source_table`
- ✅ Incremental config is correctly added
- ✅ Validation rules are correctly added

**`validate_pipeline`:**
- ✅ Valid YAML returns `{valid: true}`
- ✅ Invalid field names return structured errors with field path
- ✅ Wrong YAML keys (`source:` instead of `read:`) caught and reported
- ✅ Unknown transformer names flagged with closest match
- ✅ Missing format on read/write caught
- ✅ Invalid enum values return allowed values list

**Round-trip validation:**
- ✅ Every YAML output from `apply_pattern_template` re-parses through Pydantic
- ✅ If round-trip fails, error includes `ROUND_TRIP_FAILED` code (framework bug indicator)

### 15.3 Integration Tests

```
tests/integration/test_mcp_end_to_end.py
```

- Profile a test CSV → get `ready_for` → pass to `apply_pattern_template` → validate output runs with `PipelineManager`
- Build a multi-node pipeline with Phase 2 builder → render → validate
- Test all 6 patterns end-to-end

---

## 16. Success Criteria

### Phase 1 Complete When:

- [ ] `list_transformers` returns all registered functions with param schemas
- [ ] `list_patterns` returns all 6 patterns with requirements and example calls
- [ ] `apply_pattern_template` produces valid YAML for all 6 patterns
- [ ] `validate_pipeline` returns structured errors with fix suggestions
- [ ] Round-trip validation passes for all generated YAML
- [ ] All 30+ disabled tools in `server.py` can be permanently removed (not just commented out)
- [ ] `CRITICAL_CONTEXT` injection in `knowledge.py` is no longer needed for construction tools
- [ ] Unit tests pass for all 4 new tools

### Phase 2 Complete When:

- [ ] Builder tools can construct any pipeline that the YAML schema supports
- [ ] Each `configure_*` call validates immediately (not deferred to render)
- [ ] `render_pipeline_yaml` round-trip validates
- [ ] Session management works (auto-expire, max sessions, cleanup)
- [ ] Complex multi-node pipeline with joins and dependencies builds correctly

### Phase 3 Complete When:

- [ ] `profile_source` returns `ready_for.apply_pattern_template` instead of `ready_for.generate_bronze_node`
- [ ] `create_dimension_pipeline`, `create_scd2_pipeline`, `create_fact_pipeline` work as 1-call shortcuts
- [ ] `create_ingestion_pipeline` handles bulk table onboarding
- [ ] `suggest_pipeline` auto-picks pattern from profile results
- [ ] A GPT-4o-mini agent can go from `map_environment` → `profile_source` → `apply_pattern_template` → valid YAML with no human intervention

### Overall Success:

> An AI agent (any model tier) can discover data sources, select the right pattern, and produce a production-ready pipeline YAML without ever writing a line of YAML manually. The YAML validates on the first try in >95% of cases.

---

## 17. Anti-Patterns to Avoid

| Anti-Pattern | Why It's Bad | What to Do Instead |
|-------------|-------------|-------------------|
| String template YAML generation | Drifts from Pydantic schema over time | Construct Pydantic models, serialize at the end |
| Prose instructions in tool responses | Cheap models ignore them | Use enum constraints and structured errors |
| Exposing raw Pydantic ValidationError | Huge, hard to parse, includes internal field names | Map to structured error with `field_path`, `code`, `fix` |
| Single mega-tool with 30 params | Models get confused, omit required fields | Progressive disclosure: `list_*` → `apply_*` |
| Client-side YAML manipulation | Agents can't handle indentation/quoting | Server generates all YAML |
| Returning `{"error": "something went wrong"}` | Not actionable | Return `{code, field_path, allowed_values, fix}` |
| Hardcoding transformer lists | Falls out of sync with FunctionRegistry | Always read from `FunctionRegistry.list_functions()` at runtime |
| Trusting agent-provided transformer names | Agents hallucinate function names | Validate every name against `FunctionRegistry.has_function()` |

---

## 18. Zero-Hardcoding Strategy

The #1 maintenance risk is hardcoded lists that drift from the source code. Every piece of data an MCP tool returns must be **dynamically introspected** from the actual Python objects at runtime.

### 18.1 Problem: Pattern Lists

**Bad (from initial design):** A hardcoded dict in `list_patterns` that says "dimension requires natural_key, surrogate_key."

**Why it rots:** Someone adds a 7th pattern or changes what `DimensionPattern.validate()` checks → the MCP tool is wrong.

**Fix:** Read from `odibi.patterns._PATTERNS` dict at runtime:

```python
from odibi.patterns import _PATTERNS

def list_patterns():
    result = []
    for name, cls in _PATTERNS.items():
        result.append({
            "name": name,
            "description": cls.__doc__,  # Already exists on every pattern class
            "required_params": cls.get_required_params(),  # NEW: see below
        })
    return {"patterns": result}
```

**Prerequisite — add structured metadata to Pattern base class:**

Each pattern class already checks its requirements in `validate()`, but via scattered `if not self.params.get(...)` checks. Add a class-level declaration:

```python
# odibi/patterns/base.py
class Pattern(ABC):
    # Subclasses override these
    required_params: ClassVar[Dict[str, str]] = {}    # {param_name: description}
    optional_params: ClassVar[Dict[str, str]] = {}    # {param_name: description}
    default_write_mode: ClassVar[str] = "overwrite"
    description: ClassVar[str] = ""
    use_when: ClassVar[str] = ""

    @classmethod
    def get_required_params(cls) -> Dict[str, str]:
        return cls.required_params

    @classmethod
    def get_optional_params(cls) -> Dict[str, str]:
        return cls.optional_params
```

```python
# odibi/patterns/dimension.py
class DimensionPattern(Pattern):
    required_params = {
        "natural_key": "Business key column(s) that uniquely identify each record",
        "surrogate_key": "Auto-generated primary key column name (e.g., 'customer_sk')",
    }
    optional_params = {
        "scd_type": "SCD type: 0 (static), 1 (overwrite), 2 (history). Default: 1",
        "track_cols": "Columns to track for SCD1/2 changes",
        "target": "Target table path (required for SCD2)",
        "unknown_member": "Insert SK=0 row for orphan FK handling",
        "audit": "Audit config: {load_timestamp: bool, source_system: str}",
    }
    default_write_mode = "overwrite"
    use_when = "Reference/lookup data that changes infrequently (customers, products, locations)"
```

This keeps the metadata **next to the code it describes** — if someone changes `validate()`, they see the `required_params` dict right above it and update both.

### 18.2 Problem: Validation Test Types

**Bad:** Hardcoding `["not_null", "unique", "range", ...]` in the MCP schema.

**Actual truth:** `config.py` line 2045 defines `TestConfig` as a discriminated union:
```python
TestConfig = Annotated[Union[
    NotNullTest, UniqueTest, AcceptedValuesTest, RowCountTest,
    CustomSQLTest, RangeTest, RegexMatchTest, VolumeDropTest,
    SchemaContract, DistributionContract, FreshnessContract,
], Field(discriminator="type")]
```

**Fix:** Introspect the union at runtime:

```python
from typing import get_args, get_origin, Union
from odibi.config import TestConfig

def list_validation_tests():
    # Unwrap Annotated[Union[...], Field(...)]
    inner = get_args(TestConfig)[0]  # Union[NotNullTest, UniqueTest, ...]
    test_classes = get_args(inner)    # (NotNullTest, UniqueTest, ...)

    result = []
    for cls in test_classes:
        # Each test class has a `type` field with a Literal value
        type_value = cls.model_fields["type"].default
        result.append({
            "type": type_value,
            "description": cls.__doc__,
            "fields": {
                name: {
                    "type": str(info.annotation),
                    "required": info.is_required(),
                    "description": info.description or "",
                }
                for name, info in cls.model_fields.items()
                if name != "type"
            }
        })
    return {"tests": result, "count": len(result)}
```

Now if someone adds a 12th validation test to the union, `list_validation_tests` picks it up automatically.

### 18.3 Problem: Enum Values in MCP Input Schemas

**Bad:** `"enum": ["overwrite", "append", "upsert", "append_once", "merge"]` typed by hand.

**Fix:** Generate MCP schemas from the Python enums:

```python
from odibi.config import WriteMode, ConnectionType, EngineType

def _enum_values(enum_cls) -> list[str]:
    return [e.value for e in enum_cls]

# In list_tools():
Tool(
    name="configure_write",
    inputSchema={
        "properties": {
            "mode": {
                "type": "string",
                "enum": _enum_values(WriteMode),  # Always in sync
            }
        }
    }
)
```

Apply this pattern to every enum: `WriteMode`, `ConnectionType`, `EngineType`, `DeleteDetectionMode`, `AlertType`, `ErrorStrategy`.

For format strings (csv, parquet, json, etc.) — extract from `ReadConfig.model_fields["format"]` annotation if it uses Literal, or define a `SupportedFormat` enum in `config.py`.

### 18.4 Problem: Phase 3 Per-Pattern Template Tools

**Bad:** `create_dimension_pipeline`, `create_scd2_pipeline`, `create_fact_pipeline` — one tool per pattern. Add a 7th pattern → write a 7th tool.

**Fix:** Kill them. `apply_pattern_template` already handles all patterns. With `list_patterns` returning good `example_call` blocks (generated dynamically from pattern metadata), agents have everything they need. One tool handles all patterns.

If convenience is still wanted, generate them dynamically at server startup:

```python
# In list_tools(), dynamically register one tool per pattern:
for name, cls in _PATTERNS.items():
    tools.append(Tool(
        name=f"create_{name}_pipeline",
        description=f"Create a {name} pipeline. Shortcut for apply_pattern_template(pattern='{name}').",
        inputSchema=cls.generate_mcp_schema(),  # Derived from required_params/optional_params
    ))
```

But honestly — this adds tools without adding value. `apply_pattern_template` with `pattern` as an enum is cleaner. **Recommend: don't build per-pattern tools.**

### 18.5 Problem: Transformer Categories

**Bad:** The design mentions `"category": "scd" | "merge" | "column" | "filter"` but `FunctionRegistry` doesn't store categories.

**Fix (lightweight):** Infer category from the module path where the transformer is defined:

```python
# When building list_transformers response:
func = FunctionRegistry.get(name)
module = getattr(func, "__module__", "")
# "odibi.transformers.scd" → "scd"
# "odibi.transformers.column_ops" → "column_ops"
category = module.split(".")[-1] if "transformers" in module else "custom"
```

No registration change needed. Categories are derived from the existing module structure.

### 18.6 Summary: What Must Be Dynamic

| Data | Source of Truth | How to Introspect |
|------|----------------|-------------------|
| Pattern names | `odibi.patterns._PATTERNS.keys()` | Import dict at runtime |
| Pattern requirements | `PatternClass.required_params` | **NEW: class attribute to add** |
| Pattern descriptions | `PatternClass.__doc__` | Already exists |
| Transformer names | `FunctionRegistry.list_functions()` | Already exists |
| Transformer params | `FunctionRegistry.get_param_model(name)` | Already exists |
| Transformer categories | `func.__module__` | Derived from module path |
| Validation test types | `get_args(TestConfig)` union members | Introspect Pydantic union |
| Validation test fields | `TestClass.model_fields` | Pydantic introspection |
| Enum values (WriteMode, etc.) | `[e.value for e in EnumClass]` | Standard enum iteration |
| Connection types | `ConnectionType.__members__` | Standard enum |
| Format strings | `ReadConfig` / `WriteConfig` annotations | Pydantic field introspection |

### 18.7 The One Required Code Change

The **only** change to `odibi/` core needed is adding `required_params` / `optional_params` / `default_write_mode` / `use_when` class attributes to each pattern class in `odibi/patterns/`. This is ~5 lines per pattern class, right next to the existing docstring and `validate()` method.

Everything else (transformer info, validation types, enums) is already introspectable from existing code. Zero new registries needed.

---

## Appendix A: Full MCP Tool Registry (Target State)

### Phase 1 Tools (New)

| Tool | Category | Description |
|------|----------|-------------|
| `list_transformers` | Discovery | List all registered transformers with param schemas |
| `list_patterns` | Discovery | List 6 warehouse patterns with requirements |
| `apply_pattern_template` | Construction | Build pipeline YAML from pattern + typed params |
| `validate_pipeline` | Validation | Validate YAML with structured error output |

### Phase 2 Tools (New)

| Tool | Category | Description |
|------|----------|-------------|
| `create_pipeline` | Builder | Start a new pipeline builder session |
| `add_node` | Builder | Add a node to the pipeline |
| `configure_read` | Builder | Set read config for a node |
| `configure_write` | Builder | Set write config for a node |
| `configure_transform` | Builder | Set transform steps for a node |
| `configure_validation` | Builder | Set validation rules for a node |
| `get_pipeline_state` | Builder | Inspect current builder state |
| `render_pipeline_yaml` | Builder | Serialize to validated YAML |
| `list_sessions` | Builder | List active builder sessions |
| `discard_pipeline` | Builder | Close a session without rendering |

### Phase 3 Tools (New)

| Tool | Category | Description |
|------|----------|-------------|
| `create_ingestion_pipeline` | Template | Bulk Bronze layer ingestion (multi-table) |
| `suggest_pipeline` | Smart | Auto-suggest pattern from profile results |

### Existing Tools (Keep)

| Tool | Category | Status |
|------|----------|--------|
| `map_environment` | Discovery | Keep (Phase 3: update `ready_for`) |
| `profile_source` | Discovery | Keep (Phase 3: update `ready_for`) |
| `profile_folder` | Discovery | Keep |
| `download_sql` | Download | Keep |
| `download_table` | Download | Keep |
| `download_file` | Download | Keep |
| `generate_project_yaml` | Construction | Keep |
| `diagnose` | Diagnostics | Keep |
| `diagnose_path` | Diagnostics | Keep |
| `lineage_graph` | Observability | Keep |
| `story_read` | Observability | Keep |
| `node_sample` | Observability | Keep |
| `node_failed_rows` | Observability | Keep |

### Deprecated Tools (Remove)

| Tool | Replacement |
|------|------------|
| `validate_yaml` | `validate_pipeline` |
| All 30+ commented-out tools | Covered by new tools |
| `CRITICAL_CONTEXT` injection | No longer needed |

---

## Appendix B: Config Enum Quick Reference

These are the enum values that MCP input schemas must constrain:

| Enum | Values |
|------|--------|
| `EngineType` | `spark`, `pandas`, `polars` |
| `ConnectionType` | `local`, `azure_blob`, `delta`, `sql_server`, `http` |
| `WriteMode` | `overwrite`, `append`, `upsert`, `append_once`, `merge` |
| `DeleteDetectionMode` | `none`, `snapshot_diff`, `sql_compare` |
| `AlertType` | `webhook`, `slack`, `teams`, `teams_workflow` |
| `ErrorStrategy` | `fail_fast`, `fail_later` |
| Read/Write `format` | `csv`, `parquet`, `json`, `delta`, `sql`, `excel`, `avro` |
| Pipeline `layer` | `bronze`, `silver`, `gold` |
| Pipeline `pattern` | `dimension`, `fact`, `scd2`, `merge`, `aggregation`, `date_dimension` |
| Validation `type` | **Introspect from `get_args(TestConfig)`** — currently: `not_null`, `unique`, `accepted_values`, `row_count`, `custom_sql`, `range`, `regex_match`, `volume_drop`, `schema_contract`, `distribution_contract`, `freshness_contract` |
| Validation `on_fail` | `quarantine`, `fail`, `warn` |
| Incremental `mode` | `rolling_window`, `append`, `high_watermark` |
| Incremental `unit` | `day`, `hour`, `minute` |
