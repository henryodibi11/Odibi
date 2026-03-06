# Cline Rules for Odibi

## MCP Tools Available

### Discovery
- `map_environment(connection, path)` - Discover data sources
- `profile_source(connection, path)` - Analyze schema/stats
- `list_patterns()` - See 6 patterns
- `list_transformers(search)` - Find transformers

### Pipeline Generation  
- `apply_pattern_template(...)` - Generate simple pipeline (1 call!)
- `create_pipeline(name)` - Start multi-node builder
- `add_node`, `configure_*`, `render_pipeline_yaml` - Build complex DAGs

### Validation
- `validate_pipeline(yaml)` - Check for errors
- `diagnose()` - Environment diagnostics

---

## CRITICAL RULES

### Never Write YAML Manually
❌ Don't write: `nodes:`, `read:`, `write:` as strings  
✅ DO call: `apply_pattern_template` or builder tools

### Use Correct Field Names
✅ `read:`, `write:`, `query:`, `transform:`  
❌ NEVER: `source:`, `sink:`, `inputs:`, `outputs:`, `sql:`

### Write Mode Requirements
- `upsert` REQUIRES: `options: {keys: [...]}`
- `append_once` REQUIRES: `options: {keys: [...]}`
- `merge` REQUIRES: `merge_keys: [...]`

Without keys → RUNTIME FAILURE!

### Node Naming
Only alphanumeric + underscore: `my_node`  
NO: hyphens, dots, spaces

---

## MCP Resources (Auto-Loaded)

You have 13 documentation resources automatically available:
- ⚡ Quick Reference (critical facts)
- Complete Capabilities (all features)
- Pattern guides (dimension, fact, scd2, etc.)
- Deep Context (2,200 lines)

**Reference these when unsure!**

---

## Workflow

When user asks to build a pipeline:
1. Identify pattern (dimension, fact, scd2, etc.)
2. Call `apply_pattern_template` with sensible defaults
3. Show YAML immediately
4. Offer to customize

Be proactive, not reactive!

---

## Code Style (If Writing Python)

- Follow odibi conventions (AGENTS.md)
- Use Pydantic models
- Add tests
- Run `ruff check .`
