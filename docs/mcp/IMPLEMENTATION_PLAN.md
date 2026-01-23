# Odibi MCP Facade — Implementation Plan

> **Version:** v4.1  
> **Total Effort:** ~35 hours  
> **Approach:** Atomic tasks suitable for AI-assisted implementation (Continue + GPT 5.2)

## How to Use This Plan

1. Work through phases in order (dependencies noted)
2. Each task is atomic: one file, one model, one test
3. Run the verification command after each task
4. Mark the checkbox when complete
5. Commit after each phase

---

## Phase 1: Core Contracts (4 hours)

### 1a. Create TruncatedReason Enum

**File:** `odibi_mcp/contracts/enums.py`

**Task:** Create the TruncatedReason enum for typed truncation responses.

```python
from enum import Enum

class TruncatedReason(str, Enum):
    """Explicit reasons for response truncation."""
    ROW_LIMIT = "row_limit"
    COLUMN_LIMIT = "column_limit"
    BYTE_LIMIT = "byte_limit"
    CELL_LIMIT = "cell_limit"
    POLICY_MASKING = "policy_masking"
    SAMPLING_ONLY = "sampling_only"
    PAGINATION = "pagination"
```

**Verification:**
```bash
python -c "from odibi_mcp.contracts.enums import TruncatedReason; print(TruncatedReason.ROW_LIMIT.value)"
```

**Expected:** `row_limit`

---

### 1b. Create PolicyApplied Model

**File:** `odibi_mcp/contracts/envelope.py`

**Task:** Create the PolicyApplied model to track which policies were applied.

**Dependencies:** None

**Verification:**
```bash
pytest tests/unit/mcp/test_envelope.py::test_policy_applied -v
```

---

### 1c. Create MCPEnvelope Base Model

**File:** `odibi_mcp/contracts/envelope.py` (append)

**Task:** Create MCPEnvelope, MCPSuccessEnvelope, MCPErrorEnvelope.

**Dependencies:** 1a, 1b

**Verification:**
```bash
pytest tests/unit/mcp/test_envelope.py -v
```

---

### 1d. Create RunSelector Type

**File:** `odibi_mcp/contracts/selectors.py`

**Task:** Create RunSelector union type with RunById model.

**Dependencies:** None

**Verification:**
```bash
python -c "from odibi_mcp.contracts.selectors import RunSelector, DEFAULT_RUN_SELECTOR; print(DEFAULT_RUN_SELECTOR)"
```

**Expected:** `latest_successful`

---

### 1e. Create ResourceRef Model

**File:** `odibi_mcp/contracts/resources.py`

**Task:** Create ResourceRef with logical/physical ref gating.

**Dependencies:** None

**Verification:**
```bash
pytest tests/unit/mcp/test_resources.py::test_resource_ref -v
```

---

### 1f. Create ConnectionPolicy Model

**File:** `odibi_mcp/contracts/access.py`

**Task:** Create ConnectionPolicy with deny-by-default path checking.

**Dependencies:** None

**Verification:**
```bash
pytest tests/unit/mcp/test_access.py::test_connection_policy_deny_by_default -v
```

---

### 1g. Create AccessContext Model

**File:** `odibi_mcp/contracts/access.py` (append)

**Task:** Create AccessContext with project/connection/path validation.

**Dependencies:** 1f

**Verification:**
```bash
pytest tests/unit/mcp/test_access.py -v
```

---

### 1h. Create TimeWindow Model

**File:** `odibi_mcp/contracts/time.py`

**Task:** Create TimeWindow with complete validation (ordering, presence, timezone).

**Dependencies:** None

**Verification:**
```bash
pytest tests/unit/mcp/test_time.py -v
```

---

## Phase 2: Typed Response Models (3 hours)

### 2a. Create ColumnSpec Model

**File:** `odibi_mcp/contracts/schema.py`

**Task:** Create ColumnSpec to replace parallel arrays.

**Dependencies:** None

**Verification:**
```bash
python -c "from odibi_mcp.contracts.schema import ColumnSpec; print(ColumnSpec(name='id', dtype='int').model_dump())"
```

---

### 2b. Create SchemaResponse Model

**File:** `odibi_mcp/contracts/schema.py` (append)

**Task:** Create SchemaResponse with List[ColumnSpec].

**Dependencies:** 2a

**Verification:**
```bash
pytest tests/unit/mcp/test_schema.py::test_schema_response -v
```

---

### 2c. Create GraphNode and GraphEdge Models

**File:** `odibi_mcp/contracts/graph.py`

**Task:** Create typed graph models for DAG visualization.

**Dependencies:** None

**Verification:**
```bash
pytest tests/unit/mcp/test_graph.py -v
```

---

### 2d. Create GraphData Model

**File:** `odibi_mcp/contracts/graph.py` (append)

**Task:** Create GraphData container with List[GraphNode], List[GraphEdge].

**Dependencies:** 2c

**Verification:**
```bash
pytest tests/unit/mcp/test_graph.py::test_graph_data -v
```

---

### 2e. Create SchemaChange Model

**File:** `odibi_mcp/contracts/schema.py` (append)

**Task:** Create ColumnChange and SchemaChange models.

**Dependencies:** 2a

**Verification:**
```bash
pytest tests/unit/mcp/test_schema.py::test_schema_change -v
```

---

### 2f. Create DiffSummary Model

**File:** `odibi_mcp/contracts/diff.py`

**Task:** Create DiffSummary with hashed counts by default.

**Dependencies:** 2e

**Verification:**
```bash
pytest tests/unit/mcp/test_diff.py -v
```

---

### 2g. Create NodeStatsResponse Model

**File:** `odibi_mcp/contracts/stats.py`

**Task:** Create StatPoint and NodeStatsResponse models.

**Dependencies:** 1h (TimeWindow)

**Verification:**
```bash
pytest tests/unit/mcp/test_stats.py -v
```

---

### 2h. Create FileInfo and ListFilesResponse Models

**File:** `odibi_mcp/contracts/discovery.py`

**Task:** Create typed file listing responses.

**Dependencies:** 1e (ResourceRef)

**Verification:**
```bash
pytest tests/unit/mcp/test_discovery.py -v
```

---

## Phase 3: Access Enforcement (3 hours)

### 3a. Create Access Context Injection

**File:** `odibi_mcp/access/context.py`

**Task:** Create context manager for injecting AccessContext into layers.

**Dependencies:** 1g

**Verification:**
```bash
pytest tests/unit/mcp/test_access_injection.py -v
```

---

### 3b. Add Project Scoping to CatalogManager

**File:** `odibi/catalog.py` (modify)

**Task:** Add `set_access_context()` and `_apply_project_scope()` methods.

**Dependencies:** 3a

**Verification:**
```bash
pytest tests/unit/test_catalog.py::test_project_scoping -v
```

---

### 3c. Add Access Checks to StoryLoader

**File:** `odibi_mcp/loaders/story.py`

**Task:** Create StoryLoader with project access validation.

**Dependencies:** 3a

**Verification:**
```bash
pytest tests/unit/mcp/test_story_loader.py -v
```

---

### 3d. Create Path Validation for Discovery

**File:** `odibi_mcp/access/path_validator.py`

**Task:** Implement path prefix checking with deny-by-default.

**Dependencies:** 1f

**Verification:**
```bash
pytest tests/unit/mcp/test_path_validator.py -v
```

---

### 3e. Create Physical Ref Gate

**File:** `odibi_mcp/access/physical_gate.py`

**Task:** Implement 3-gate physical ref resolution.

**Dependencies:** 1e, 1g

**Verification:**
```bash
pytest tests/unit/mcp/test_physical_gate.py -v
```

---

## Phase 4: Audit Logger (1 hour)

### 4a. Create AuditEntry Model

**File:** `odibi_mcp/audit/entry.py`

**Task:** Create AuditEntry dataclass with all required fields.

**Dependencies:** None

**Verification:**
```bash
python -c "from odibi_mcp.audit.entry import AuditEntry; print(AuditEntry.__annotations__)"
```

---

### 4b. Create AuditLogger

**File:** `odibi_mcp/audit/logger.py`

**Task:** Create AuditLogger with structured logging and arg redaction.

**Dependencies:** 4a

**Verification:**
```bash
pytest tests/unit/mcp/test_audit.py -v
```

---

### 4c. Integrate Audit Logger with Server

**File:** `odibi_mcp/server.py` (modify)

**Task:** Add audit logging to tool dispatch.

**Dependencies:** 4b

**Verification:**
```bash
pytest tests/unit/mcp/test_server_audit.py -v
```

---

## Phase 5: Story Tools (3 hours)

### 5a. Create story_read Tool

**File:** `odibi_mcp/tools/story.py`

**Task:** Implement story_read with RunSelector support.

**Dependencies:** 1c, 1d, 2d, 2e, 3c

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_story.py::test_story_read -v
```

---

### 5b. Create story_diff Tool

**File:** `odibi_mcp/tools/story.py` (append)

**Task:** Implement story_diff with DiffSummary response.

**Dependencies:** 5a, 2f

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_story.py::test_story_diff -v
```

---

### 5c. Create node_describe Tool

**File:** `odibi_mcp/tools/story.py` (append)

**Task:** Implement node_describe with schema and validation info.

**Dependencies:** 5a, 2b

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_story.py::test_node_describe -v
```

---

## Phase 6: Sample Tools (2 hours)

### 6a. Create Sample Limiter

**File:** `odibi_mcp/utils/limiter.py`

**Task:** Implement limit_sample with row/column/cell caps.

**Dependencies:** 1a

**Verification:**
```bash
pytest tests/unit/mcp/test_limiter.py -v
```

---

### 6b. Create node_sample Tool

**File:** `odibi_mcp/tools/sample.py`

**Task:** Implement node_sample with limiting and RunSelector.

**Dependencies:** 6a, 1d, 3c

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_sample.py::test_node_sample -v
```

---

### 6c. Create node_sample_in Tool

**File:** `odibi_mcp/tools/sample.py` (append)

**Task:** Implement node_sample_in for input samples.

**Dependencies:** 6b

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_sample.py::test_node_sample_in -v
```

---

### 6d. Create node_failed_rows Tool

**File:** `odibi_mcp/tools/sample.py` (append)

**Task:** Implement node_failed_rows with validation filter.

**Dependencies:** 6b

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_sample.py::test_node_failed_rows -v
```

---

## Phase 7: Catalog Tools (3 hours)

### 7a. Create node_stats Tool

**File:** `odibi_mcp/tools/catalog.py`

**Task:** Implement node_stats with TimeWindow and typed response.

**Dependencies:** 1h, 2g, 3b

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_catalog.py::test_node_stats -v
```

---

### 7b. Create pipeline_stats Tool

**File:** `odibi_mcp/tools/catalog.py` (append)

**Task:** Implement pipeline_stats with TimeWindow.

**Dependencies:** 7a

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_catalog.py::test_pipeline_stats -v
```

---

### 7c. Create failure_summary Tool

**File:** `odibi_mcp/tools/catalog.py` (append)

**Task:** Implement failure_summary with optional pipeline filter.

**Dependencies:** 7a

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_catalog.py::test_failure_summary -v
```

---

### 7d. Create schema_history Tool

**File:** `odibi_mcp/tools/catalog.py` (append)

**Task:** Implement schema_history with SchemaChange list.

**Dependencies:** 2e, 3b

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_catalog.py::test_schema_history -v
```

---

## Phase 8: Lineage Tools (2 hours)

### 8a. Create lineage_upstream Tool

**File:** `odibi_mcp/tools/lineage.py`

**Task:** Implement lineage_upstream with ResourceRef responses.

**Dependencies:** 1e, 3e

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_lineage.py::test_lineage_upstream -v
```

---

### 8b. Create lineage_downstream Tool

**File:** `odibi_mcp/tools/lineage.py` (append)

**Task:** Implement lineage_downstream with ResourceRef responses.

**Dependencies:** 8a

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_lineage.py::test_lineage_downstream -v
```

---

### 8c. Create lineage_graph Tool

**File:** `odibi_mcp/tools/lineage.py` (append)

**Task:** Implement lineage_graph with GraphData response.

**Dependencies:** 2d, 8a

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_lineage.py::test_lineage_graph -v
```

---

## Phase 9: Schema Tools (2 hours)

### 9a. Create output_schema Tool

**File:** `odibi_mcp/tools/schema.py`

**Task:** Implement output_schema with SchemaResponse.

**Dependencies:** 2b, 1d, 3b

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_schema.py::test_output_schema -v
```

---

### 9b. Create list_outputs Tool

**File:** `odibi_mcp/tools/schema.py` (append)

**Task:** Implement list_outputs with ResourceRef list and physical gating.

**Dependencies:** 9a, 3e

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_schema.py::test_list_outputs -v
```

---

## Phase 10: Source Discovery Tools (5 hours)

### 10a. Create Discovery Limits Config

**File:** `odibi_mcp/discovery/limits.py`

**Task:** Create DiscoveryLimits with hard caps.

**Dependencies:** None

**Verification:**
```bash
python -c "from odibi_mcp.discovery.limits import DiscoveryLimits; print(DiscoveryLimits().max_files_per_call)"
```

**Expected:** `100`

---

### 10b. Create list_files Tool

**File:** `odibi_mcp/tools/discovery.py`

**Task:** Implement list_files with pagination and path validation.

**Dependencies:** 2h, 3d, 10a

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_discovery.py::test_list_files -v
```

---

### 10c. Create list_tables Tool

**File:** `odibi_mcp/tools/discovery.py` (append)

**Task:** Implement list_tables for SQL connections.

**Dependencies:** 10b

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_discovery.py::test_list_tables -v
```

---

### 10d. Create infer_schema Tool

**File:** `odibi_mcp/tools/discovery.py` (append)

**Task:** Implement infer_schema with byte/row limits.

**Dependencies:** 2b, 10a, 3d

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_discovery.py::test_infer_schema -v
```

---

### 10e. Create describe_table Tool

**File:** `odibi_mcp/tools/discovery.py` (append)

**Task:** Implement describe_table for SQL connections.

**Dependencies:** 2b, 10c

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_discovery.py::test_describe_table -v
```

---

### 10f. Create preview_source Tool

**File:** `odibi_mcp/tools/discovery.py` (append)

**Task:** Implement preview_source with column projection and limits.

**Dependencies:** 6a, 10d

**Verification:**
```bash
pytest tests/unit/mcp/test_tools_discovery.py::test_preview_source -v
```

---

## Phase 11: Error Handling (1 hour)

### 11a. Create Error Response Helper

**File:** `odibi_mcp/utils/errors.py`

**Task:** Create _error_response helper with envelope wrapping.

**Dependencies:** 1c

**Verification:**
```bash
pytest tests/unit/mcp/test_errors.py -v
```

---

### 11b. Integrate Error Handling in Server

**File:** `odibi_mcp/server.py` (modify)

**Task:** Add try/except with typed error responses.

**Dependencies:** 11a

**Verification:**
```bash
pytest tests/unit/mcp/test_server_errors.py -v
```

---

## Phase 12: Integration Tests (6 hours)

### 12a. Create Mock Story Fixtures

**File:** `tests/fixtures/mcp_stories.py`

**Task:** Create realistic Story fixtures for testing.

**Dependencies:** All Phase 5

**Verification:**
```bash
python -c "from tests.fixtures.mcp_stories import get_mock_story; print(get_mock_story().pipeline_name)"
```

---

### 12b. Create Mock Catalog Fixtures

**File:** `tests/fixtures/mcp_catalog.py`

**Task:** Create Catalog data fixtures.

**Dependencies:** All Phase 7

**Verification:**
```bash
python -c "from tests.fixtures.mcp_catalog import get_mock_node_runs; print(len(get_mock_node_runs()))"
```

---

### 12c. Integration Test: Story Tools End-to-End

**File:** `tests/integration/mcp/test_story_e2e.py`

**Task:** Test story_read → node_describe → node_sample flow.

**Dependencies:** 12a, Phase 5, Phase 6

**Verification:**
```bash
pytest tests/integration/mcp/test_story_e2e.py -v
```

---

### 12d. Integration Test: Discovery Tools End-to-End

**File:** `tests/integration/mcp/test_discovery_e2e.py`

**Task:** Test list_files → infer_schema → preview_source flow.

**Dependencies:** 12b, Phase 10

**Verification:**
```bash
pytest tests/integration/mcp/test_discovery_e2e.py -v
```

---

### 12e. Integration Test: Access Enforcement

**File:** `tests/integration/mcp/test_access_e2e.py`

**Task:** Test project scoping, path denial, physical ref gating.

**Dependencies:** Phase 3

**Verification:**
```bash
pytest tests/integration/mcp/test_access_e2e.py -v
```

---

### 12f. Integration Test: Full AI Workflow

**File:** `tests/integration/mcp/test_ai_workflow.py`

**Task:** Simulate: list_files → infer_schema → suggest_pattern → generate_yaml → validate_yaml

**Dependencies:** All phases

**Verification:**
```bash
pytest tests/integration/mcp/test_ai_workflow.py -v
```

---

## Post-Implementation

### Documentation Updates

- [ ] Update `odibi_mcp/README.md` with new tools
- [ ] Add examples to `docs/mcp/examples/`
- [ ] Update `CHANGELOG.md` with v2.11.0 MCP enhancements

### Configuration Template

- [ ] Create `mcp_config.example.yaml` with all options documented
- [ ] Add to `odibi init` scaffolding

---

## Quick Reference: File Structure

```
odibi_mcp/
├── __init__.py
├── server.py                    # Main MCP server (existing, modified)
├── contracts/
│   ├── __init__.py
│   ├── enums.py                 # TruncatedReason
│   ├── envelope.py              # PolicyApplied, MCPEnvelope, Success/Error
│   ├── selectors.py             # RunSelector, RunById
│   ├── resources.py             # ResourceRef
│   ├── access.py                # ConnectionPolicy, AccessContext
│   ├── time.py                  # TimeWindow
│   ├── schema.py                # ColumnSpec, SchemaResponse, SchemaChange
│   ├── graph.py                 # GraphNode, GraphEdge, GraphData
│   ├── diff.py                  # DiffSummary
│   ├── stats.py                 # StatPoint, NodeStatsResponse
│   └── discovery.py             # FileInfo, ListFilesResponse
├── access/
│   ├── __init__.py
│   ├── context.py               # Context injection
│   ├── path_validator.py        # Path prefix checking
│   └── physical_gate.py         # 3-gate physical ref
├── audit/
│   ├── __init__.py
│   ├── entry.py                 # AuditEntry
│   └── logger.py                # AuditLogger
├── loaders/
│   ├── __init__.py
│   └── story.py                 # StoryLoader
├── discovery/
│   ├── __init__.py
│   └── limits.py                # DiscoveryLimits
├── tools/
│   ├── __init__.py
│   ├── story.py                 # story_read, story_diff, node_describe
│   ├── sample.py                # node_sample, node_sample_in, node_failed_rows
│   ├── catalog.py               # node_stats, pipeline_stats, failure_summary, schema_history
│   ├── lineage.py               # lineage_upstream, lineage_downstream, lineage_graph
│   ├── schema.py                # output_schema, list_outputs
│   └── discovery.py             # list_files, list_tables, infer_schema, describe_table, preview_source
└── utils/
    ├── __init__.py
    ├── limiter.py               # Sample limiting
    └── errors.py                # Error response helpers

tests/unit/mcp/
├── test_envelope.py
├── test_resources.py
├── test_access.py
├── test_time.py
├── test_schema.py
├── test_graph.py
├── test_diff.py
├── test_stats.py
├── test_discovery.py
├── test_access_injection.py
├── test_story_loader.py
├── test_path_validator.py
├── test_physical_gate.py
├── test_audit.py
├── test_server_audit.py
├── test_limiter.py
├── test_tools_story.py
├── test_tools_sample.py
├── test_tools_catalog.py
├── test_tools_lineage.py
├── test_tools_schema.py
├── test_tools_discovery.py
├── test_errors.py
└── test_server_errors.py

tests/integration/mcp/
├── test_story_e2e.py
├── test_discovery_e2e.py
├── test_access_e2e.py
└── test_ai_workflow.py

tests/fixtures/
├── mcp_stories.py
└── mcp_catalog.py
```
