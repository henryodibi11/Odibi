# Odibi Cookbook: Recipes for Common Patterns

This guide provides copy-pasteable solutions for real-world Data Engineering problems.

## Recipe 1: The "Unstable API" Ingestion 🌪️

**Problem:**
"My source JSON adds new fields constantly and is deeply nested. My pipeline breaks whenever the schema changes."

**Solution:**
Use `schema_policy: { mode: "evolve" }` to automatically adapt to new columns, and `normalize_json` to flatten the structure.

```yaml
- name: "ingest_unstable_api"
  read:
    connection: "api_source"
    format: "json"
    path: "events/v1/*.json"

  # 1. Handle Drift: Automatically add new columns as NULLable
  schema_policy:
    mode: "evolve"
    on_new_columns: "add_nullable"

  # 2. Flatten: Convert nested JSON into columns (e.g. payload.id -> payload_id)
  transformer: "normalize_json"
  params:
    column: "payload"
    sep: "_"

  write:
    connection: "silver"
    format: "delta"
    table: "events_flat"
```

---

## Recipe 2: The "Privacy-First" Customer Table 🔒

**Problem:**
"I need to ingest customer data but Hash emails and Mask credit card numbers for compliance (GDPR/CCPA)."

**Solution:**
Use the `privacy` block for global anonymization and `sensitive` columns for masking in stories. You can also mix methods using `hash_columns` transformer.

```yaml
- name: "load_secure_customers"
  read:
    connection: "s3_raw"
    format: "parquet"
    path: "customers/"

  # 1. Global Privacy Policy (Applies to PII columns)
  privacy:
    method: "hash"
    salt: "${PRIVACY_SALT}"  # Load from env var

  # 2. Mark Columns as PII (Triggers Privacy Policy)
  columns:
    email:
      pii: true
    phone:
      pii: true

  # 3. Explicit Masking for Credit Cards (Transformers run before Write)
  transform:
    steps:
      # Mask CCNs (keep last 4)
      - function: "regex_replace"
        params:
          column: "credit_card"
          pattern: ".(?=.{4})"  # Regex to match all except last 4
          replacement: "*"

  # 4. Hide from Stories (Documentation)
  sensitive: ["email", "credit_card", "phone"]

  write:
    connection: "silver"
    format: "delta"
    table: "dim_customers_anonymized"
```

---

## Recipe 3: Sessionizing Clickstream Data ⏱️

**Problem:**
"I have raw events. I need to group them into User Sessions (30-minute timeout) and load them incrementally."

**Solution:**
Combine the `sessionize` transformer with `incremental: { mode: "stateful" }` to process only new data while maintaining session logic.

```yaml
- name: "clickstream_sessions"
  read:
    connection: "kafka_landing"
    format: "json"
    path: "clicks/"

    # 1. Incremental Loading (Stateful)
    # Tracks the last processed timestamp to only read new events
    incremental:
      mode: "stateful"
      state_key: "clickstream_hwm"
      watermark_lag: "1h"  # Handle late arriving data

  # 2. Session Logic (30 min timeout)
  transformer: "sessionize"
  params:
    timestamp_col: "event_time"
    user_col: "user_id"
    threshold_seconds: 1800  # 30 minutes
    session_col: "session_id"

  write:
    connection: "gold"
    format: "delta"
    table: "fact_sessions"
    mode: "append"
```
