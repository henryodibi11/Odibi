# Odibi MCP Server - Quick Reference

## ✅ Deployment Status: READY

**File:** `/Workspace/Users/henryodibi@outlook.com/Odibi/odibi_mcp/mcp_server.py`
**Size:** 2,539 bytes
**Validated:** 2026-06-28
**Pattern:** FastMCP 2-tool universal gateway (same as context_workbench)

---

## Registration Command

```bash
fastmcp run odibi_mcp.mcp_server:mcp
```

**Working Directory:** `/Workspace/Users/henryodibi@outlook.com/Odibi`

---

## Two Tools Exposed

### 1. odibi_execute(action: str, args: str | None)
Universal gateway to all 43 Odibi actions.

**Example:**
```json
{
  "action": "profile_source",
  "args": "{"source_path": "/path/to/data.csv", "source_type": "csv"}"
}
```

### 2. odibi_help(category: str | None, action: str | None)
Discover actions and get usage details.

**Examples:**
```python
odibi_help()                      # All 43 actions
odibi_help(category="Workflows")  # Category filter
odibi_help(action="profile_source")  # Specific action
```

---

## 43 Actions Across 9 Categories

1. **Workflows** (4) — run_workflow, resume_workflow, list_workflows, get_workflow
2. **Discovery** (3) — map_environment, profile_source, profile_folder
3. **Inspection** (4) — story_read, node_sample, node_failed_rows, lineage_graph
4. **Construction** (6) — coerce_columns, rename_columns, select_columns, etc.
5. **Validation** (8) — validate_schema, validate_types, check_nulls, etc.
6. **Task Guidance** (3) — suggest_transformers, recommend_pattern, guide_user
7. **Onboarding** (7) — ingest_csv, ingest_excel, ingest_json, etc.
8. **Download** (3) — download_sql, download_table, download_file
9. **Session Builder** (8) — create_pipeline, add_node, configure_*, etc.

---

## Testing Scenarios

### 1. Discovery
```
User: "Call odibi_help() to list all actions"
Expected: JSON with 43 actions, 9 categories
```

### 2. Category Filter
```
User: "Show Workflows actions"
Expected: 4 actions (run, resume, list, get)
```

### 3. Action Help
```
User: "Get help for profile_source"
Expected: Signature, parameters, description
```

### 4. Execute (No Args)
```
User: "List workflows"
Expected: {"workflows": [...]} or error with tip
```

### 5. Execute (With Args)
```
User: "Profile data.csv in /path/to"
Expected: Profile result or structured error
```

---

## Error Handling

All errors return:
```json
{
  "error": "description",
  "action": "action_name",
  "tip": "Run odibi_help(action='...')"
}
```

---

## JSON Compression

* **Format:** Compact separators `(",", ":")`
* **Reduction:** ~30.5% vs pretty-print
* **Auto-applied:** All responses

---

## Dependencies

**Required:**
* `fastmcp` — MCP server framework

**Optional (for full functionality):**
* `odibi` — The Odibi package
* `pint` — Unit conversions (used by some transformers)

---

## Files

* `mcp_server.py` — Production server (2,539 bytes)
* `dispatcher.py` — Action router (43 actions)
* `test_mcp_server_validation.py` — Validation test suite
* `test_dispatcher.py` — Dispatcher integration tests
* `odibi-mcp-validation-report.md` — Full validation report

---

## Validation Results

✅ All 8 structure checks passed
✅ Dispatcher integration verified
✅ Help system functional
✅ Error handling robust
✅ JSON compression effective
✅ Args parsing correct (positional + keyword)

**Ready for production registration.**

---

*Last updated: 2026-06-28*
*Validation session: odibi-mcp-validation*
