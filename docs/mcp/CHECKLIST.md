# Odibi MCP Facade — Implementation Checklist

> **Instructions:** Mark tasks with `[x]` as you complete them. Run the verification command after each task.

---

## Phase 1: Core Contracts (4 hours)

- [x] **1a.** Create `TruncatedReason` enum → `odibi_mcp/contracts/enums.py`
- [x] **1b.** Create `PolicyApplied` model → `odibi_mcp/contracts/envelope.py`
- [x] **1c.** Create `MCPEnvelope` base model → `odibi_mcp/contracts/envelope.py`
- [x] **1d.** Create `RunSelector` type → `odibi_mcp/contracts/selectors.py`
- [x] **1e.** Create `ResourceRef` model → `odibi_mcp/contracts/resources.py`
- [x] **1f.** Create `ConnectionPolicy` model → `odibi_mcp/contracts/access.py`
- [x] **1g.** Create `AccessContext` model → `odibi_mcp/contracts/access.py`
- [x] **1h.** Create `TimeWindow` model → `odibi_mcp/contracts/time.py`

**Phase 1 Complete:** [x]

---

## Phase 2: Typed Response Models (3 hours)

- [x] **2a.** Create `ColumnSpec` model → `odibi_mcp/contracts/schema.py`
- [x] **2b.** Create `SchemaResponse` model → `odibi_mcp/contracts/schema.py`
- [x] **2c.** Create `GraphNode` and `GraphEdge` models → `odibi_mcp/contracts/graph.py`
- [x] **2d.** Create `GraphData` model → `odibi_mcp/contracts/graph.py`
- [x] **2e.** Create `SchemaChange` model → `odibi_mcp/contracts/schema.py`
- [x] **2f.** Create `DiffSummary` model → `odibi_mcp/contracts/diff.py`
- [x] **2g.** Create `NodeStatsResponse` model → `odibi_mcp/contracts/stats.py`
- [x] **2h.** Create `FileInfo` and `ListFilesResponse` models → `odibi_mcp/contracts/discovery.py`

**Phase 2 Complete:** [x]

---

## Phase 3: Access Enforcement (3 hours)

- [x] **3a.** Create access context injection → `odibi_mcp/access/context.py`
- [x] **3b.** Add project scoping to `CatalogManager` → `odibi/catalog.py`
- [x] **3c.** Add access checks to `StoryLoader` → `odibi_mcp/loaders/story.py`
- [x] **3d.** Create path validation for discovery → `odibi_mcp/access/path_validator.py`
- [x] **3e.** Create physical ref gate → `odibi_mcp/access/physical_gate.py`

**Phase 3 Complete:** [x]

---

## Phase 4: Audit Logger (1 hour)

- [x] **4a.** Create `AuditEntry` model → `odibi_mcp/audit/entry.py`
- [x] **4b.** Create `AuditLogger` → `odibi_mcp/audit/logger.py`
- [x] **4c.** Integrate audit logger with server → `odibi_mcp/server.py`

**Phase 4 Complete:** [x]

---

## Phase 5: Story Tools (3 hours)

- [x] **5a.** Create `story_read` tool → `odibi_mcp/tools/story.py`

- [x] **5b.** Create `story_diff` tool → `odibi_mcp/tools/story.py`
- [x] **5c.** Create `node_describe` tool → `odibi_mcp/tools/story.py`

**Phase 5 Complete:** [x]

---

## Phase 6: Sample Tools (2 hours)

- [x] **6a.** Create sample limiter → `odibi_mcp/utils/limiter.py`
- [x] **6b.** Create `node_sample` tool → `odibi_mcp/tools/sample.py`
- [x] **6c.** Create `node_sample_in` tool → `odibi_mcp/tools/sample.py`
- [x] **6d.** Create `node_failed_rows` tool → `odibi_mcp/tools/sample.py`

**Phase 6 Complete:** [x]

---

## Phase 7: Catalog Tools (3 hours)

- [x] **7a.** Create `node_stats` tool → `odibi_mcp/tools/catalog.py`
- [x] **7b.** Create `pipeline_stats` tool → `odibi_mcp/tools/catalog.py`
- [x] **7c.** Create `failure_summary` tool → `odibi_mcp/tools/catalog.py`
- [x] **7d.** Create `schema_history` tool → `odibi_mcp/tools/catalog.py`

**Phase 7 Complete:** [x]

---

## Phase 8: Lineage Tools (2 hours)

- [x] **8a.** Create `lineage_upstream` tool → `odibi_mcp/tools/lineage.py`
- [x] **8b.** Create `lineage_downstream` tool → `odibi_mcp/tools/lineage.py`
- [x] **8c.** Create `lineage_graph` tool → `odibi_mcp/tools/lineage.py`

**Phase 8 Complete:** [x]

---

## Phase 9: Schema Tools (2 hours)

- [x] **9a.** Create `output_schema` tool → `odibi_mcp/tools/schema.py`
- [x] **9b.** Create `list_outputs` tool → `odibi_mcp/tools/schema.py`

**Phase 9 Complete:** [x]

---

## Phase 10: Source Discovery Tools (5 hours)

- [x] **10a.** Create `DiscoveryLimits` config → `odibi_mcp/discovery/limits.py`
- [x] **10b.** Create `list_files` tool → `odibi_mcp/tools/discovery.py`
- [x] **10c.** Create `list_tables` tool → `odibi_mcp/tools/discovery.py`
- [x] **10d.** Create `infer_schema` tool → `odibi_mcp/tools/discovery.py`
- [x] **10e.** Create `describe_table` tool → `odibi_mcp/tools/discovery.py`
- [x] **10f.** Create `preview_source` tool → `odibi_mcp/tools/discovery.py`

**Phase 10 Complete:** [x]

---

## Phase 11: Error Handling (1 hour)

- [x] **11a.** Create error response helper → `odibi_mcp/utils/errors.py`
- [x] **11b.** Integrate error handling in server → `odibi_mcp/server.py`

**Phase 11 Complete:** [x]

---

## Phase 12: Integration Tests (6 hours)

- [x] **12a.** Create mock Story fixtures → `tests/fixtures/mcp_stories.py`
- [x] **12b.** Create mock Catalog fixtures → `tests/fixtures/mcp_catalog.py`
- [x] **12c.** Integration test: Story tools E2E → `tests/integration/mcp/test_story_e2e.py`
- [x] **12d.** Integration test: Discovery tools E2E → `tests/integration/mcp/test_discovery_e2e.py`
- [x] **12e.** Integration test: Access enforcement → `tests/integration/mcp/test_access_e2e.py`
- [x] **12f.** Integration test: Full AI workflow → `tests/integration/mcp/test_ai_workflow.py`

**Phase 12 Complete:** [x]

---

## Post-Implementation

- [x] Update `odibi_mcp/README.md` with new tools
- [x] Add examples to `docs/mcp/examples/`
- [x] Update `CHANGELOG.md` with v2.11.0 MCP enhancements
- [x] Create `mcp_config.example.yaml`
- [x] Add MCP config to `odibi init` scaffolding

**Post-Implementation Complete:** [x]

---

## Summary

| Phase | Tasks | Status |
|-------|-------|--------|
| 1. Core Contracts | 8 | ✅ |
| 2. Typed Models | 8 | ✅ |
| 3. Access Enforcement | 5 | ✅ |
| 4. Audit Logger | 3 | ✅ |
| 5. Story Tools | 3 | ✅ |
| 6. Sample Tools | 4 | ✅ |
| 7. Catalog Tools | 4 | ✅ |
| 8. Lineage Tools | 3 | ✅ |
| 9. Schema Tools | 2 | ✅ |
| 10. Discovery Tools | 6 | ✅ |
| 11. Error Handling | 2 | ✅ |
| 12. Integration Tests | 6 | ✅ |
| Post-Implementation | 5 | ✅ |
| **Total** | **59** | ✅ |

---

## Progress Tracking

**Started:** 2026-01-23  
**Phase 1 Done:** 2026-01-23  
**Phase 6 Done (MVP):** 2026-01-23  
**Phase 12 Done (Complete):** 2026-01-23  
**Post-Implementation Done:** 2026-01-23

---

## Notes

_Use this space to track blockers, decisions, or deviations from the plan:_

```
-
-
-
```
