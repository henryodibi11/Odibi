# Prompt for Next Thread: Implement ODIBI Gold Layer Patterns

Copy and paste this into a new Amp thread:

---

## Implement Phase 1: DimensionPattern for ODIBI

### Context

I'm enhancing ODIBI's Gold layer to support declarative dimensional modeling. See the full plan at `docs/plans/gold_layer_implementation_plan.md`.

ODIBI already has:
- `odibi/patterns/scd2.py` - SCD Type 2 logic
- `odibi/patterns/fact.py` - Basic fact pattern (just dedup)
- `odibi/patterns/base.py` - Base Pattern class
- `odibi/transformers/advanced.py` - `generate_surrogate_key` transformer

### Task

Create `odibi/patterns/dimension.py` - a new `DimensionPattern` that builds a complete dimension table.

### Requirements

1. **Surrogate Key Generation**
   - Auto-generate integer surrogate key (e.g., `customer_sk`)
   - Use `MAX(existing_sk) + ROW_NUMBER()` for new rows
   - Handle first run (no existing table)

2. **SCD Support**
   - `scd_type: 0` - Static (never update)
   - `scd_type: 1` - Overwrite (no history)
   - `scd_type: 2` - Track history (reuse existing `scd2.py` logic)

3. **Unknown Member**
   - If `unknown_member: true`, insert a row with SK=0 and "Unknown" values
   - This row is used for orphan FK handling in fact tables

4. **Audit Columns**
   - Add `load_timestamp` (current timestamp)
   - Add `source_system` (from config)

5. **Config Schema**
   ```yaml
   pattern:
     type: dimension
     params:
       natural_key: customer_id         # Required
       surrogate_key: customer_sk       # Required
       scd_type: 2                      # 0, 1, or 2
       track_cols: [name, city]      # For SCD1/2
       target: gold/dim_customer        # For SCD2 (existing table)
       unknown_member: true             # Optional
       audit:
         load_timestamp: true
         source_system: "crm"
   ```

### Implementation Steps

1. Create `odibi/patterns/dimension.py`
2. Create `DimensionPattern` class extending `Pattern`
3. Implement `validate()` method
4. Implement `execute()` method with:
   - SK generation logic
   - SCD routing (0/1/2)
   - Unknown member insertion
   - Audit column injection
5. Register pattern in `odibi/patterns/__init__.py`
6. Add config schema in `odibi/config.py` if needed
7. Create unit tests in `tests/patterns/test_dimension.py`
8. Test with both Pandas and Spark engines

### Reference Files

Look at these for patterns to follow:
- `odibi/patterns/scd2.py` - SCD2 logic to reuse
- `odibi/patterns/fact.py` - Simple pattern structure
- `odibi/patterns/base.py` - Base class
- `odibi/transformers/advanced.py` - `generate_surrogate_key` for reference

### Acceptance Criteria

This config should work:

```yaml
- name: dim_customer
  depends_on: [clean_customers]
  pattern:
    type: dimension
    params:
      natural_key: customer_id
      surrogate_key: customer_sk
      scd_type: 2
      track_cols: [name, email, city]
      target: gold/dim_customer
      unknown_member: true
      audit:
        load_timestamp: true
        source_system: "crm"
  write:
    connection: gold
    path: dim_customer
```

And produce:

| customer_sk | customer_id | name | email | city | valid_from | valid_to | is_current | load_timestamp | source_system |
|-------------|-------------|------|-------|------|------------|----------|------------|----------------|---------------|
| 0 | -1 | Unknown | unknown@unknown.com | Unknown | 1900-01-01 | NULL | true | 2024-01-15 | crm |
| 1 | abc123... | John | john@mail.com | NYC | 2024-01-15 | NULL | true | 2024-01-15 | crm |

---

After Phase 1 is complete, we'll move to Phase 2 (DateDimensionPattern) and Phase 3 (Enhanced FactPattern).
