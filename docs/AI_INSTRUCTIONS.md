# AI Instructions for Odibi MCP Tools

**Read this FIRST. Follow these rules EXACTLY.**

---

## CRITICAL RULES

### 1. NEVER Write YAML Manually
❌ Don't write: `nodes:`, `read:`, `write:` strings  
✅ DO call: `apply_pattern_template` or builder tools

### 2. NEVER Use These Field Names
❌ `source:` `sink:` `inputs:` `outputs:` `sql:`  
✅ `read:` `write:` `query:` `transform:`

### 3. ALWAYS Be Proactive
❌ Don't ask: "What's your source connection?"  
✅ DO: Call tool with defaults, show result, offer to customize

---

## Quick Pattern Reference

| User Says | Pattern | Required Params | Tool Call |
|-----------|---------|-----------------|-----------|
| "customer dimension" | dimension | natural_key, surrogate_key | `apply_pattern_template(pattern="dimension", natural_key="customer_id", surrogate_key="customer_sk", source_table="dbo.Customer", ...)` |
| "track employee changes" | scd2 | keys, tracked_columns | `apply_pattern_template(pattern="scd2", keys=["emp_id"], tracked_columns=["name","dept"], ...)` |
| "daily orders" | fact | keys (optional) | `apply_pattern_template(pattern="fact", keys=["order_id"], ...)` |
| "calendar table" | date_dimension | start_date, end_date | `apply_pattern_template(pattern="date_dimension", start_date="2020-01-01", end_date="2030-12-31", ...)` |
| "monthly summary" | aggregation | grain, measures | `apply_pattern_template(pattern="aggregation", grain=["customer_id","month"], measures=["SUM(amount)"], ...)` |

---

## Default Parameters (Use When Not Specified)

```python
pipeline_name = f"{pattern}_{table_name}"  # e.g., "dimension_customer"
source_connection = "source_db"            # Placeholder
target_connection = "warehouse"            # Placeholder  
target_path = f"gold/{table_name}"        # Sensible default
layer = "gold"                            # Default layer
source_format = "sql"                     # Default for tables
target_format = "delta"                   # Default output
```

**Show user the generated YAML, THEN say:** "I used placeholder connections ('source_db', 'warehouse'). Want me to update with your actual connection names?"

---

## Write Modes - CRITICAL

| Mode | Use When | CRITICAL REQUIREMENT |
|------|----------|---------------------|
| `overwrite` | Full refresh | None |
| `append` | Append-only | None |
| `upsert` | Update+insert | **MUST have:** `options: {keys: [id]}` |
| `append_once` | Idempotent load | **MUST have:** `options: {keys: [id]}` |
| `merge` | SQL Server MERGE | **MUST have:** `merge_keys: [id]` |

**If you use upsert/append_once WITHOUT keys, the YAML will fail at runtime!**

---

## Action Template

**User:** "Build X pipeline"

**You immediately:**

```python
# 1. Identify pattern
pattern = identify_pattern(user_request)  # dimension, fact, scd2, etc.

# 2. Extract table name
table = extract_table(user_request)  # e.g., "dbo.Customer"

# 3. Call tool with defaults
result = apply_pattern_template(
    pattern=pattern,
    pipeline_name=f"{pattern}_{table_name}",
    source_connection="source_db",  # Placeholder
    target_connection="warehouse",  # Placeholder
    target_path=f"gold/{pattern}_{table_name}",
    source_table=table,
    # Pattern-specific required params
    **get_pattern_defaults(pattern, table)
)

# 4. Show YAML
print(result["yaml"])

# 5. Offer to customize
print("I used placeholder connections. Want to update with your actual connection names?")
```

---

## Pattern-Specific Defaults

### Dimension
```python
natural_key = infer_key_from_table_name(table)  # "customer" → "customer_id"
surrogate_key = f"{natural_key}_sk"
```

### SCD2
```python
keys = [infer_key_from_table_name(table)]
tracked_columns = ["name", "email", "status"]  # Common columns
```

### Fact
```python
keys = [infer_key_from_table_name(table)]  # Optional but recommended
```

### Date Dimension
```python
start_date = "2020-01-01"  # Sensible default
end_date = "2030-12-31"    # 10 years
```

### Aggregation
```python
grain = ["id", "month"]  # Example
measures = ["SUM(amount)", "COUNT(*)"]  # Example
```

---

## Transformer Names - EXACT MATCH REQUIRED

**DON'T guess!** If user wants transformations:
1. Call `list_transformers()` OR `list_transformers(search="keyword")`
2. Use EXACT name from response

**Common correct names:**
- `distinct` (NOT "dedup" or "deduplicate")
- `fill_nulls` (NOT "drop_nulls")
- `select_columns` (NOT "select")
- `rename_columns` (NOT "rename")

---

## Example: Perfect Response

**User:** "Build a customer dimension"

**You (AI):**
```
I'll build a customer dimension pipeline using the dimension pattern.

[Calls apply_pattern_template with defaults]

Here's the generated pipeline YAML:

```yaml
pipelines:
  - pipeline: dimension_customer
    layer: gold
    nodes:
      - name: dimension_node
        read:
          connection: source_db
          format: sql
          table: dbo.Customer
        write:
          connection: warehouse
          format: delta
          path: gold/dim_customer
          mode: overwrite
        transformer: dimension
        params:
          natural_key: customer_id
          surrogate_key: customer_sk
```

I used placeholder connections ('source_db', 'warehouse').
What are your actual connection names from odibi.yaml?
```

**Key points:**
✓ Generated YAML immediately
✓ Used correct field names (read/write)
✓ Used defaults for unspecified params
✓ Offered to customize AFTER showing result

---

## Success Checklist (Before Responding)

Every pipeline YAML you generate must have:
- [ ] `read:` and `write:` (NEVER source:/sink:)
- [ ] `connection:` fields reference actual/placeholder connection names
- [ ] `format:` specified for both read and write
- [ ] `mode:` specified in write
- [ ] If mode is upsert/append_once: `options: {keys: [...]}` present
- [ ] Pattern params match requirements (check list_patterns response)
- [ ] Transformer names are exact (check list_transformers if used)
- [ ] Node names are alphanumeric_underscore only

---

## Tool Response Pattern

Every tool response includes `next_step`. USE IT!

```python
result = apply_pattern_template(...)

if result["valid"]:
    print(result["yaml"])
    print(f"\nNext: {result['next_step']}")  # ← Tool tells you!
else:
    print(f"Errors: {result['errors']}")
    # Errors have 'fix' field - show it!
```

---

## One-Sentence Mission

**Generate valid, working odibi pipeline YAML on the first try by calling MCP tools with sensible defaults, never writing YAML strings manually.**

---

**This is your PRIMARY directive when working with odibi!**
