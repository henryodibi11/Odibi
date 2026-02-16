# Example 3: SCD2 Dimension (Track History)

Build a customer dimension with full history tracking (Slowly Changing Dimension Type 2).

## When to Use

- You need to answer: "What was the customer's tier *last month*?"
- Dimension attributes change slowly (name, address, tier)
- BI reports need point-in-time accuracy

## How It Works

When a tracked column changes:
1. Old row gets `valid_to` = change date, `is_current` = false
2. New row gets `valid_to` = NULL, `is_current` = true
3. All history is preserved

---

## Full Config

```yaml
# odibi.yaml
project: customer_dimension

connections:
  bronze:
    type: local
    base_path: ./data/bronze
  silver:
    type: local
    base_path: ./data/silver

story:
  connection: silver
  path: stories

system:
  connection: silver
  path: _system

pipelines:
  - pipeline: dimensions
    layer: silver
    nodes:
      - name: dim_customer
        read:
          connection: bronze
          format: parquet
          path: customers
        
        pattern:
          type: dimension
          params:
            natural_key: customer_id
            surrogate_key: customer_sk
            scd_type: 2
            track_cols:
              - name
              - email
              - tier
              - city
            unknown_member: true  # Creates SK=0 for orphan handling
        # No write: block needed — SCD2 writes directly to the target table
```

---

## Sample Input (Bronze)

Copy from `docs/examples/canonical/sample_data/customers.csv` to `data/bronze/customers/`:

| customer_id | name | email | tier | city | updated_at |
|-------------|------|-------|------|------|------------|
| 1 | Alice | alice@example.com | Gold | NYC | 2025-01-01 |
| 2 | Bob | bob@example.com | Silver | LA | 2025-01-01 |

---

## Expected Output (Silver)

After first run:

| customer_sk | customer_id | name | tier | city | valid_from | valid_to | is_current |
|-------------|-------------|------|------|------|------------|----------|------------|
| 0 | _UNKNOWN | Unknown | Unknown | Unknown | 1900-01-01 | NULL | true |
| 1 | 1 | Alice | Gold | NYC | 2025-01-01 | NULL | true |
| 2 | 2 | Bob | Silver | LA | 2025-01-01 | NULL | true |

After Alice's tier changes to Platinum:

| customer_sk | customer_id | name | tier | city | valid_from | valid_to | is_current |
|-------------|-------------|------|------|------|------------|----------|------------|
| 0 | _UNKNOWN | Unknown | Unknown | Unknown | 1900-01-01 | NULL | true |
| 1 | 1 | Alice | Gold | NYC | 2025-01-01 | 2025-01-15 | **false** |
| 2 | 2 | Bob | Silver | LA | 2025-01-01 | NULL | true |
| 3 | 1 | Alice | **Platinum** | NYC | 2025-01-15 | NULL | **true** |

---

## Run

```bash
odibi run odibi.yaml
```

---

## Query Current State

```sql
SELECT * FROM dim_customer WHERE is_current = true
```

## Query Historical State

```sql
-- What was Alice's tier on Jan 10?
SELECT * FROM dim_customer 
WHERE customer_id = 'C001' 
  AND '2025-01-10' BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31')
```

---

## Schema Reference

| Key | Docs |
|-----|------|
| `pattern.type: dimension` | [Dimension Pattern](../../patterns/dimension.md) |
| `params.scd_type: 2` | [SCD2 Pattern](../../patterns/scd2.md) |
| `params.unknown_member` | [Dimension Pattern](../../patterns/dimension.md#unknown-member) |

---

## Decision: SCD Type 1 vs 2

| Choose SCD1 if... | Choose SCD2 if... |
|-------------------|-------------------|
| History not needed | "What was the value last month?" |
| Storage is limited | BI needs point-in-time accuracy |
| Simpler queries | Audit requirements |

```yaml
# SCD1: Overwrites, no history
params:
  scd_type: 1
```

[→ Pattern: Dimension](../../patterns/dimension.md)
