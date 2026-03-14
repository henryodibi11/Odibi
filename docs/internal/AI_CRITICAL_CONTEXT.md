# AI CRITICAL CONTEXT - READ THIS FIRST

**Priority: HIGHEST**  
**For: ALL AI agents working with odibi**  

⚡ **CRITICAL INSTRUCTION:** When user says "build X pipeline", immediately call the appropriate MCP tool. Don't ask for more info unless truly required. Use sensible defaults.

---

## ❌ NEVER USE These Field Names

```yaml
# WRONG - These don't exist in odibi:
source:      # ← NO!
sink:        # ← NO!
inputs:      # ← NO! (unless multi-input at node level)
outputs:     # ← NO!
sql:         # ← NO! (use query: inside read:)
```

## ✅ ALWAYS USE These Instead

```yaml
# CORRECT odibi syntax:
read:        # ← Input operation
write:       # ← Output operation
query:       # ← SQL query (inside read:)
transform:   # ← Transformations
```

---

## Critical YAML Structure

### Every Node Needs:
```yaml
nodes:
  - name: my_node          # Required
    read:                  # Input (or depends_on another node)
      connection: name     # Connection from odibi.yaml
      format: sql          # csv, parquet, json, delta, sql
      table: dbo.Table     # OR path: file.csv OR query: "SELECT..."

    write:                 # Output
      connection: name
      format: delta        # delta, parquet, csv, json
      path: output/path    # OR table: dbo.Target
      mode: overwrite      # overwrite, append, upsert, append_once, merge
```

---

## Write Modes (MUST KNOW)

| Mode | Use When | Requires |
|------|----------|----------|
| `overwrite` | Full refresh, dimensions | - |
| `append` | Append-only logs | - |
| `upsert` | Update + insert | `options: {keys: [id]}` |
| `append_once` | Idempotent ingestion | `options: {keys: [id]}` |
| `merge` | SQL Server MERGE | `merge_keys: [id]` |

**Critical:** `upsert` and `append_once` REQUIRE `options: {keys: [...]}` or they FAIL!

---

## Formats (MUST KNOW)

**Read formats:** `sql`, `csv`, `parquet`, `json`, `delta`, `api`  
**Write formats:** `delta`, `parquet`, `csv`, `json`, `sql`

**SQL reads need ONE of:**
- `table: schema.TableName`
- `query: "SELECT * FROM ..."`

---

## Patterns (The 6 Core Patterns)

| Pattern | When to Use | Required Params |
|---------|-------------|-----------------|
| `dimension` | Customers, products, locations | natural_key, surrogate_key |
| `scd2` | Track history of changes | keys, tracked_columns |
| `fact` | Orders, events, transactions | - |
| `date_dimension` | Calendar table | start_date, end_date |
| `aggregation` | Monthly summaries, KPIs | grain, measures |
| `merge` | Upsert operations | keys, target |

---

## Transformers (How to Find Them)

**DON'T guess!** Call `list_transformers()` first.

Common ones:
- `distinct` - Remove all duplicates
- `select_columns` - Keep specific columns
- `rename_columns` - Rename columns
- `fill_nulls` - Replace nulls
- `cast_columns` - Change types
- `add_column` - Add computed column

**ALWAYS check parameters** with list_transformers before using!

---

## Common Mistakes AI Makes

### ❌ Mistake 1: Wrong field names
```yaml
# WRONG
nodes:
  - source: dbo.Table  # ← NO!
    sink: output.csv   # ← NO!
```
**Fix:** Use `read:` and `write:`

### ❌ Mistake 2: Guessing transformer names
```yaml
# WRONG
transform:
  steps:
    - function: dedup  # ← Doesn't exist!
```
**Fix:** Call `list_transformers()`, use exact name (`distinct`)

### ❌ Mistake 3: Upsert without keys
```yaml
# WRONG
write:
  mode: upsert
  # Missing: options: {keys: [id]}
```
**Fix:** Add `options: {keys: [your_key_columns]}`

### ❌ Mistake 4: Writing YAML manually
**Fix:** Use `apply_pattern_template` or builder tools!

---

## The Correct Workflow

### ⚡ CRITICAL: Be Proactive, Not Reactive

**When user says "build X pipeline":**
1. ✅ Identify pattern immediately (dimension, fact, scd2, etc.)
2. ✅ Call apply_pattern_template with sensible defaults
3. ✅ Return the YAML
4. ❌ DON'T ask "what's your source connection?" - use placeholder or ask AFTER showing what you'd generate

**Use these defaults when not specified:**
- source_connection: "source_db" (user can change)
- target_connection: "warehouse" (user can change)
- target_path: "gold/{pattern}_output"
- layer: "gold"

**Show the user what you'd generate, THEN ask if they want to customize.**

### Simple Pipeline (2 calls)
```
1. list_patterns() → See what's available (optional - you know the 6 patterns)
2. apply_pattern_template(...) → Get valid YAML immediately
```

### With Discovery (3 calls)
```
1. profile_source(connection, table) → Get schema/stats
2. suggest_pipeline(profile) → Auto-select pattern
3. apply_pattern_template(ready_for params) → Get YAML
```

### Complex Multi-Node (8+ calls)
```
1. create_pipeline(name)
2. add_node(...)
3. configure_read(...)
4. configure_write(...)
5. configure_transform(...)
6. (repeat for more nodes)
7. render_pipeline_yaml()
```

---

## Pattern Selection Logic

**User says...** → **Use this pattern:**

- "customer table", "product lookup" → `dimension`
- "track changes", "history" → `scd2`
- "orders", "events", "transactions" → `fact`
- "calendar", "date table" → `date_dimension`
- "monthly summary", "aggregate" → `aggregation`

---

## Critical Validation Rules

1. **Node names:** Alphanumeric + underscore only (no dashes!)
2. **Connections:** Must exist in odibi.yaml
3. **Read needs:** `connection` + `format` + (`table` OR `path` OR `query`)
4. **Write needs:** `connection` + `format` + (`path` OR `table`) + `mode`
5. **Patterns need specific params** - Check with list_patterns()

---

## Success Checklist

Before returning YAML to user, verify:
- ✅ Used `read:` and `write:` (not source/sink)
- ✅ Used exact transformer names from list_transformers()
- ✅ Pattern params match required_params from list_patterns()
- ✅ Write mode matches use case (upsert for updates, append for logs, etc.)
- ✅ If upsert/append_once: included options.keys
- ✅ Node names are alphanumeric_underscore

---

## When in Doubt

1. **Call the discovery tool first:** `list_patterns()`, `list_transformers()`
2. **Read tool responses:** They include next_step and examples
3. **Use the tools, don't write YAML:** Tools prevent mistakes
4. **Check resources:** Odibi Deep Context has all answers

---

## One-Sentence Summary

**Use MCP tools to generate all YAML - they enforce correct syntax, validate inputs, and return working configurations on the first try.**

---

**This document is auto-loaded into every AI agent via MCP resources. No long prompts needed!**
